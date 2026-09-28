"""Node-to-node data consistency verification.

Dispatches to pluggable comparison backends and applies the configured
database/schema/table scope rules before any SQL is issued.

Two entry points:

  run_node_comparison()       -- legacy two-node API, stable return shape
  run_multi_node_comparison() -- N nodes (2 or more), matrix mode

Both share the same engines, the same scope resolution and the same table
discovery; they differ only in how results are aggregated.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db import DatabasePool
from utils.scope import build_matcher
from backends.checksum import compare_table_checksum
from backends.native import compare_table_native
from backends.pg_comparator import run_pg_comparator, check_pg_comparator_installed
from backends.data_diff import run_data_diff, check_data_diff_installed
from core.matrix_comparator import run_multi_node_comparison as _matrix_compare

logger = logging.getLogger("comparator.node")


# Data comparison engines, selectable with `node-compare --backend`.
BACKENDS = {
    "checksum": "Chunked per-key-range checksum comparison (fast, built-in)",
    "native": "Full row-level diff (accurate, built-in)",
    "pg_comparator": "pg_comparator external tool (two nodes only)",
    "data_diff": "data-diff 外部工具，支持跨数据库类型对比（两节点）",
}

# Schema comparison engines, used by `schema-compare`. Kept in the same
# registry so `list-backends` shows everything the tool can drive, but
# reported separately: comparing table definitions is a different question
# from comparing rows, and conflating them would make a schema difference
# look like a data difference.
SCHEMA_BACKENDS = {
    "pgdiff": "pgdiff 外部工具，对比 PostgreSQL/Kingbase 的 schema 差异（Java）",
}

# Backends that can only ever compare one pair of nodes.
PAIRWISE_ONLY = ("pg_comparator", "data_diff", "pgdiff")


def list_backends() -> dict:
    """List every backend the tool can drive, with availability."""
    from backends.pgdiff import check_pgdiff_installed

    def _check(name):
        if name == "pg_comparator":
            return check_pg_comparator_installed()
        if name == "data_diff":
            return check_data_diff_installed()
        if name == "pgdiff":
            return check_pgdiff_installed()
        return True

    result = {}
    for name, desc in BACKENDS.items():
        result[name] = {
            "description": desc,
            "available": _check(name),
            "kind": "data",
            "multi_node": name not in PAIRWISE_ONLY,
        }
    for name, desc in SCHEMA_BACKENDS.items():
        result[name] = {
            "description": desc,
            "available": _check(name),
            "kind": "schema",
            "multi_node": False,
        }
    return result


# ---------------------------------------------------------------------------
# Table discovery through the scope rules
# ---------------------------------------------------------------------------

def discover_tables(pool, config, node, tables=None, matcher=None):
    """Resolve the (schema, table) pairs this run should compare on `node`.

    `tables` is an explicit list of table names, or None to take everything
    the scope allows. Either way the scope rules are applied last, so an
    excluded table stays excluded even when the user names it with -t.
    """
    cmp_cfg = config.get("comparison", {}) or {}
    matcher = matcher or build_matcher(config, node=node)

    dbname = pool.get_database_name(node)
    if not matcher.database_allowed(dbname):
        logger.info("Database '%s' excluded by scope; nothing to compare", dbname)
        return []

    all_schemas = pool.get_schemas(node)
    allowed_schemas = matcher.filter_schemas(dbname, all_schemas)
    if not allowed_schemas:
        allowed_schemas = [cmp_cfg.get("schema") or "public"]

    pairs = []
    if tables:
        # Explicit names may live in any allowed schema.
        for tbl in tables:
            for schema in allowed_schemas:
                if not matcher.table_allowed(dbname, schema, tbl):
                    logger.info("Table %s.%s excluded by scope", schema, tbl)
                    continue
                try:
                    real = pool.resolve_table(node, schema, tbl)
                except Exception:
                    real = None
                if real:
                    pairs.append((schema, real))
                    break
            else:
                logger.warning("Table '%s' not found in any allowed schema on %s",
                               tbl, node)
        # A single explicit name may also arrive as "schema.table".
        return _dedupe(pairs)

    for schema in allowed_schemas:
        try:
            found = pool.get_tables(node, schema)
        except Exception as e:
            logger.warning("Cannot list tables in %s on %s: %s", schema, node, e)
            continue
        for tbl in matcher.filter_tables(dbname, schema, found):
            pairs.append((schema, tbl))
    return _dedupe(pairs)


def _dedupe(pairs):
    seen = set()
    out = []
    for p in pairs:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _split_qualified(tables, default_schema):
    """Accept both 'accounts' and 'public.accounts' in a table list."""
    out = []
    for t in tables or []:
        if "." in t:
            s, _, name = t.partition(".")
            out.append((s, name))
        else:
            out.append((default_schema, t))
    return out


# ---------------------------------------------------------------------------
# Legacy two-node entry point
# ---------------------------------------------------------------------------

def run_node_comparison(pool: DatabasePool, config: dict,
                        node_a: str, node_b: str,
                        backend: str = "checksum",
                        tables: list = None,
                        schema: str = None,
                        args=None,
                        watermark=None) -> dict:
    """Compare two nodes. Return shape is unchanged from the original API."""
    cmp_cfg = config.get("comparison", {}) or {}
    schema = schema or cmp_cfg.get("schema", "public")
    chunk_size = cmp_cfg.get("chunk_size", 10000)
    parallel = cmp_cfg.get("parallel", 4)
    matcher = build_matcher(config, args, node=node_a)

    if tables:
        candidates = _split_qualified(tables, schema)
        selected = [(s, t) for (s, t) in candidates
                    if matcher.table_allowed(pool.get_database_name(node_a), s, t)]
        skipped = [t for (s, t) in candidates if (s, t) not in selected]
        if skipped:
            logger.info("Excluded by scope: %s", ", ".join("%s.%s" % p for p in skipped))
    else:
        selected = discover_tables(pool, config, node_a, tables=None, matcher=matcher)

    if not selected:
        logger.warning("No tables selected for comparison")
        return {
            "success": True, "backend": backend, "tables_checked": 0,
            "tables_diff": 0, "results": [], "total_time": 0.0,
            "conclusion": "CONSISTENT", "exit_code": 0,
        }

    logger.info("Node comparison: %s vs %s, backend=%s, %d tables",
                node_a, node_b, backend, len(selected))
    start_time = time.time()

    if backend in PAIRWISE_ONLY:
        names = [t for _, t in selected]
        # Both external tools need the pool: data-diff is told the key column
        # for each table (without it, it cannot align rows), and
        # pg_comparator needs it to build its per-table connection URLs.
        func = (run_pg_comparator if backend == "pg_comparator"
                else run_data_diff)
        result = func(pool, config, node_a, node_b, names, schema)
        result["backend"] = backend
        result["total_time"] = time.time() - start_time
        # A backend that could not run at all is not the same answer as
        # "the data differs", so it gets its own exit code.
        if not result.get("success"):
            logger.warning("%s 未能完成对比: %s", backend, result.get("error"))
            result["exit_code"] = 2
            result["conclusion"] = "INCONCLUSIVE"
        elif result.get("identical"):
            result["exit_code"] = 0
            result["conclusion"] = "CONSISTENT"
        else:
            result["exit_code"] = 1
            result["conclusion"] = "DIVERGENT"
        return result

    compare_func = (compare_table_checksum if backend == "checksum"
                    else compare_table_native)

    results = []
    tables_diff = 0

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {}
        for s, table in selected:
            if backend == "checksum":
                f = executor.submit(compare_func, pool, node_a, node_b,
                                    s, table, chunk_size, 1, watermark)
            else:
                # defect fix: this used to pass (parallel, 1), which landed
                # on (max_diff_rows=parallel, max_workers=1) and capped the
                # diff sample at 4 rows.
                f = executor.submit(compare_func, pool, node_a, node_b,
                                    s, table, 500, 1)
            futures[f] = (s, table)

        for future in as_completed(futures):
            s, table = futures[future]
            try:
                r = future.result()
                results.append(r)
                if not r.get("identical"):
                    tables_diff += 1
                    logger.warning("Table %s.%s differs!", s, table)
            except Exception as e:
                logger.error("Failed to compare %s.%s: %s", s, table, e)
                results.append({
                    "table": "%s.%s" % (s, table),
                    "error": str(e),
                    "identical": False,
                    "confidence": "unavailable",
                })
                tables_diff += 1

    total_time = time.time() - start_time
    return {
        "success": tables_diff == 0,
        "conclusion": "DIVERGENT" if tables_diff else "CONSISTENT",
        "exit_code": 1 if tables_diff else 0,
        "backend": backend,
        "tables_checked": len(results),
        "tables_diff": tables_diff,
        "total_chunks_diff": sum(r.get("chunks_diff", 0) for r in results),
        "total_time": round(total_time, 2),
        "results": results,
    }


# ---------------------------------------------------------------------------
# Multi-node entry point
# ---------------------------------------------------------------------------

def run_multi_node_comparison(pool: DatabasePool, config: dict, nodes: list,
                              backend: str = "checksum", primary: str = None,
                              tables: list = None, schema: str = None,
                              args=None, watermark=None,
                              max_workers: int = 4) -> dict:
    """Compare two or more nodes in one matrix pass."""
    schema = schema or (config.get("comparison", {}) or {}).get("schema", "public")
    base = primary or nodes[0]
    matcher = build_matcher(config, args, node=base)

    if backend in PAIRWISE_ONLY and len(nodes) > 2:
        raise ValueError(
            "%s 引擎只支持两节点对比，无法做 %d 节点矩阵。\n"
            "  选项 1：用 --backend checksum 做全节点矩阵扫描（扫描次数为 N 而非 N*(N-1)）\n"
            "  选项 2：用 -a/-b 显式指定一对节点"
            % (backend, len(nodes)))

    if tables:
        dbname = pool.get_database_name(base)
        pairs = [(s, t) for (s, t) in _split_qualified(tables, schema)
                 if matcher.table_allowed(dbname, s, t)]
    else:
        pairs = discover_tables(pool, config, base, tables=None, matcher=matcher)

    if not pairs:
        return {
            "success": True, "conclusion": "CONSISTENT", "exit_code": 0,
            "backend": backend, "nodes": list(nodes), "base": base,
            "tables_checked": 0, "tables_diff": 0, "tables_inconclusive": 0,
            "total_chunks": 0, "outlier_summary": [], "warnings": [],
            "results": [], "total_time": 0.0,
        }

    # Group by schema so the matrix comparator keeps its single-schema API.
    by_schema = {}
    for s, t in pairs:
        by_schema.setdefault(s, []).append(t)

    if len(by_schema) == 1:
        only = list(by_schema)[0]
        return _matrix_compare(pool, config, nodes, backend=backend,
                               primary=primary, tables=by_schema[only],
                               schema=only, watermark=watermark,
                               max_workers=max_workers)

    merged = None
    for s, tbls in by_schema.items():
        r = _matrix_compare(pool, config, nodes, backend=backend,
                            primary=primary, tables=tbls, schema=s,
                            watermark=watermark, max_workers=max_workers)
        if merged is None:
            merged = r
        else:
            merged["results"].extend(r["results"])
            merged["tables_checked"] += r["tables_checked"]
            merged["tables_diff"] += r["tables_diff"]
            merged["tables_inconclusive"] += r["tables_inconclusive"]
            merged["total_chunks"] += r["total_chunks"]
            merged["total_time"] = round(merged["total_time"] + r["total_time"], 2)
            merged["warnings"].extend(w for w in r["warnings"]
                                      if w not in merged["warnings"])
            _merge_outliers(merged, r)

    if merged["tables_diff"]:
        merged["conclusion"], merged["exit_code"] = "DIVERGENT", 1
    elif merged["tables_inconclusive"]:
        merged["conclusion"], merged["exit_code"] = "INCONCLUSIVE", 2
    else:
        merged["conclusion"], merged["exit_code"] = "CONSISTENT", 0
    merged["success"] = merged["conclusion"] == "CONSISTENT"
    _recompute_rates(merged)
    return merged


def _merge_outliers(merged, r):
    by_node = {d["node"]: d for d in merged.get("outlier_summary", [])}
    for d in r.get("outlier_summary", []):
        cur = by_node.get(d["node"])
        if cur is None:
            merged.setdefault("outlier_summary", []).append(d)
            by_node[d["node"]] = d
        else:
            cur["diff_chunks"] += d["diff_chunks"]
            cur["tables"] = sorted(set(cur["tables"]) | set(d["tables"]))


def _recompute_rates(merged):
    """Re-derive outlier rates after several schemas were merged.

    The per-schema run computed its rate against that schema's chunk count;
    once several are combined the denominator changes and the "likely just
    replication lag" hint would otherwise be based on a stale ratio.
    """
    total = merged.get("total_chunks") or 0
    for d in merged.get("outlier_summary", []):
        rate = (float(d["diff_chunks"]) / total) if total else 0.0
        d["total_chunks"] = total
        d["rate"] = round(rate, 4)
        if rate <= 0.01:
            d["hint"] = "疑似复制延迟（差异块占比很低），建议在静默窗口复验"
        elif rate >= 0.50:
            d["hint"] = "节点与其余节点大面积不一致"
        else:
            d["hint"] = "需要人工判断：既非明显的复制延迟，也非大面积损坏"
    merged["outlier_summary"].sort(key=lambda x: -x["diff_chunks"])
