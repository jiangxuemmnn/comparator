"""Compare N nodes at once and identify which one is the odd one out.

Comparing every pair costs N*(N-1) table scans. Scanning each node once into
a {table: {chunk: digest}} matrix and comparing the matrix costs N scans --
a factor of N-1. For 3 nodes that is 2x, for 5 nodes 4x.

The bigger win is not speed. In all-pairs mode the scans are spread over
N*(N-1) sequential reads, so the nodes are observed at widely different
moments; in matrix mode they are read in N passes. Since two nodes are
always two independent snapshots, a narrower observation window means fewer
differences that are merely "one side hadn't caught up yet". Matrix mode is
a correctness improvement first and a performance one second.

Voting is reported with its limitation attached. Plurality voting assumes
failures are independent, and in a replication topology they are not: an
error on the primary propagates to every standby, so the tool can end up
naming a single correct node as the outlier. `--primary` is offered as the
more trustworthy alternative whenever the topology is known.

A node that could not be read does NOT vote and is NOT reported as an
outlier. Partial failure is isolated to the affected table.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from backends.checksum import scan_digests, localize_range

logger = logging.getLogger("comparator.matrix")

CONSISTENT = "CONSISTENT"
DIVERGENT = "DIVERGENT"
INCONCLUSIVE = "INCONCLUSIVE"

# A node whose chunk diverges on no more than this fraction of chunks is
# likely showing replication lag rather than a real inconsistency.
LAG_THRESHOLD = 0.01
SEVERE_THRESHOLD = 0.50

VOTING_WARNING = (
    "多数派表决只能定位少数派，不能证明多数派是正确的。"
    "若被测集群存在主从关系，主库的错误会传播到所有从库，此时表决结果可能完全相反。"
    "已知复制拓扑时请用 --primary <节点> 显式指定基准。"
)


def _scan(pool, node, schema, table, cols, types, pk_cols, cuts,
          watermark, retries=2, chunk_size=10000):
    """Scan one node, retrying transient failures before giving up."""
    last = None
    for attempt in range(retries + 1):
        try:
            _, chunks = scan_digests(pool, node, schema, table, cols, types,
                                     pk_cols, cuts=cuts, watermark=watermark,
                                     chunk_size=chunk_size)
            return chunks, None
        except Exception as e:
            last = e
            if attempt < retries:
                logger.debug("scan %s.%s on %s failed (attempt %d): %s",
                             schema, table, node, attempt + 1, e)
                time.sleep(0.5 * (attempt + 1))
    logger.warning("scan %s.%s on %s failed: %s", schema, table, node, last)
    return None, str(last)


def _vote(values_by_node):
    """Group nodes by chunk digest and pick a plurality.

    `values_by_node` contains only nodes that returned data. Returns
    (verdict, groups, outliers) where verdict is CONSISTENT / DIVERGENT /
    'UNRESOLVED'.
    """
    groups = {}
    for node, val in values_by_node.items():
        groups.setdefault(val, []).append(node)

    if len(groups) == 1:
        return CONSISTENT, [sorted(groups.values(), key=len, reverse=True)[0]], []

    ordered = sorted(groups.values(), key=len, reverse=True)
    total = len(values_by_node)

    if total == 2:
        # With two nodes "they differ" is the whole truth: there is no
        # majority to be had, but the divergence itself is certain. Report it
        # without naming a culprit.
        return DIVERGENT, ordered, []

    if len(ordered[0]) * 2 > total:
        outliers = [n for g in ordered[1:] for n in g]
        return DIVERGENT, ordered, outliers

    # No strict majority (e.g. 2:2 across four nodes). Do not guess.
    return "UNRESOLVED", ordered, []


def run_multi_node_comparison(pool, config, nodes, backend="checksum",
                              primary=None, tables=None, schema=None,
                              watermark=None, max_workers=4,
                              max_diff_rows=20):
    """Compare `nodes` (2 or more) and return a single aggregated verdict."""
    cmp_cfg = config.get("comparison", {}) or {}
    schema = schema or cmp_cfg.get("schema", "public")

    nodes = list(nodes)
    if len(nodes) < 2:
        raise ValueError("多节点对比至少需要两个节点")

    base = primary or nodes[0]
    if base not in nodes:
        raise ValueError("基准节点 %s 不在节点列表中" % base)

    if backend != "checksum":
        raise ValueError(
            "矩阵模式仅支持 checksum 引擎，当前指定的是 %s。"
            "native 需要逐行读取双方数据，多节点矩阵下代价过高；"
            "pg_comparator / data_diff 本身就是两两对比工具。"
            "请改用 --backend checksum，或用 -a/-b 显式指定一对节点。"
            % backend)

    chunk_size = (config.get("comparison", {}) or {}).get("chunk_size", 10000)
    started = time.time()
    results = []
    tables_diff = 0
    tables_inconclusive = 0

    # Reachability probe: a node that cannot be reached is reported as
    # UNKNOWN everywhere rather than silently making every table "differ".
    reachable = {}
    for n in nodes:
        try:
            pool.execute(n, "SELECT 1")
            reachable[n] = True
        except Exception as e:
            reachable[n] = False
            logger.error("节点 %s 不可达: %s", n, e)

    live_nodes = [n for n in nodes if reachable[n]]
    if base not in live_nodes:
        # Structured answer rather than an exception: the run could not
        # produce a verdict, which is exit 2, not a traceback and exit 1.
        return {
            "success": False,
            "conclusion": INCONCLUSIVE,
            "exit_code": 2,
            "backend": backend,
            "mode": "primary" if primary else "matrix-vote",
            "base": base,
            "nodes": list(nodes),
            "unreachable": [n for n in nodes if not reachable[n]],
            "schema": schema,
            "tables_checked": 0,
            "tables_diff": 0,
            "tables_inconclusive": 0,
            "total_chunks": 0,
            "outlier_summary": [],
            "warnings": ["基准节点 %s 不可达，本轮无法给出结论" % base],
            "results": [],
            "total_time": round(time.time() - started, 2),
        }

    outlier_chunks = {n: 0 for n in live_nodes}
    outlier_tables = {n: set() for n in live_nodes}
    total_chunks_all = 0

    for table in tables:
        t_result = _compare_one_table(
            pool, base, live_nodes, schema, table, watermark,
            max_workers, max_diff_rows, all_nodes=nodes,
            chunk_size=chunk_size)
        t_result["rows"] = {n: (t_result["rows"].get(n) if
                                t_result["rows"].get(n) is not None else None)
                            for n in nodes}
        results.append(t_result)

        if t_result["conclusion"] == DIVERGENT:
            tables_diff += 1
        elif t_result["conclusion"] == INCONCLUSIVE:
            tables_inconclusive += 1

        for n, cnt in (t_result.get("outliers") or {}).items():
            outlier_chunks[n] = outlier_chunks.get(n, 0) + cnt
            outlier_tables.setdefault(n, set()).add(table)
        total_chunks_all += t_result.get("chunks_total", 0)

    # ---- overall verdict ----
    if tables_diff:
        conclusion = DIVERGENT
        exit_code = 1
    elif tables_inconclusive:
        conclusion = INCONCLUSIVE
        exit_code = 2
    else:
        conclusion = CONSISTENT
        exit_code = 0

    outlier_summary = []
    for n in live_nodes:
        cnt = outlier_chunks.get(n, 0)
        if not cnt:
            continue
        rate = (float(cnt) / total_chunks_all) if total_chunks_all else 0.0
        if rate <= LAG_THRESHOLD:
            hint = "疑似复制延迟（差异块占比很低），建议在静默窗口复验"
        elif rate >= SEVERE_THRESHOLD:
            hint = "节点与其余节点大面积不一致"
        else:
            hint = "需要人工判断：既非明显的复制延迟，也非大面积损坏"
        outlier_summary.append({
            "node": n,
            "diff_chunks": cnt,
            "total_chunks": total_chunks_all,
            "rate": round(rate, 4),
            "tables": sorted(outlier_tables.get(n, [])),
            "hint": hint,
        })
    outlier_summary.sort(key=lambda d: -d["diff_chunks"])

    warnings = []
    if outlier_summary and not primary:
        warnings.append(VOTING_WARNING)
    for n in nodes:
        if not reachable[n]:
            warnings.append("节点 %s 不可达，其数据未参与对比，结论不完整" % n)

    return {
        "success": conclusion == CONSISTENT,
        "conclusion": conclusion,
        "exit_code": exit_code,
        "backend": backend,
        "mode": "primary" if primary else "matrix-vote",
        "base": base,
        "nodes": nodes,
        "unreachable": [n for n in nodes if not reachable[n]],
        "schema": schema,
        "tables_checked": len(results),
        "tables_diff": tables_diff,
        "tables_inconclusive": tables_inconclusive,
        "total_chunks": total_chunks_all,
        "outlier_summary": outlier_summary,
        "warnings": warnings,
        "total_time": round(time.time() - started, 2),
        "results": results,
    }


def _compare_one_table(pool, base, live_nodes, schema, table, watermark,
                       max_workers, max_diff_rows, all_nodes=None,
                       chunk_size=10000):
    """Build the chunk matrix for one table and reduce it to a verdict.

    `all_nodes` is the full requested node list; `live_nodes` is the subset
    that answered the reachability probe. Both are needed: a node that never
    answered still has to be recorded against this table, otherwise a run
    where one node was down would silently report the rest as CONSISTENT
    instead of stating that the verdict is incomplete.
    """
    all_nodes = list(all_nodes or live_nodes)
    entry = {
        "table": "%s.%s" % (schema, table),
        "conclusion": INCONCLUSIVE,
        "rows": {},
        "node_status": {},
        "chunks_total": 0,
        "chunks_diff": 0,
        "chunk_detail": [],
        "outliers": {},
        "diff_details": [],
    }
    for n in all_nodes:
        entry["node_status"][n] = "UNREACHABLE" if n not in live_nodes else "PENDING"

    try:
        real = pool.resolve_table(base, schema, table)
        pk_cols = pool.get_primary_key(base, schema, table)
        col_info = pool.get_column_info(base, schema, table)
    except Exception as e:
        entry["error"] = str(e)
        entry["diff_details"].append("无法读取表元数据: %s" % e)
        return entry

    cols = [c["name"] for c in col_info]
    types = [c["type"] for c in col_info]

    if not cols:
        entry["error"] = "表中没有列"
        return entry

    if not pk_cols:
        # Without a key there is nothing to chunk on; compare row counts and
        # say plainly that content was not checked.
        counts = {}
        for n in live_nodes:
            try:
                counts[n] = pool.get_row_count(n, schema, real)
            except Exception as e:
                entry["node_status"][n] = "UNREACHABLE"
                entry["diff_details"].append("%s: %s" % (n, e))
        entry["rows"] = counts
        entry["method"] = "row_count"
        entry["note"] = "无主键，只比较行数；行数相同不代表内容相同"
        if len(set(counts.values())) > 1:
            entry["conclusion"] = DIVERGENT
            entry["diff_details"].append("行数不一致: %s" % counts)
        else:
            # Equal counts prove nothing about the contents, so this can
            # never be reported as CONSISTENT.
            entry["conclusion"] = INCONCLUSIVE
        return entry

    # Base node defines the chunk boundaries every other node reuses.
    try:
        cuts, base_chunks = scan_digests(
            pool, base, schema, real, cols, types, pk_cols,
            cuts=None, watermark=watermark, chunk_size=chunk_size)
        entry["node_status"][base] = "OK"
        entry["rows"][base] = pool.get_row_count(base, schema, real)
    except Exception as e:
        entry["error"] = "基准节点扫描失败: %s" % e
        entry["diff_details"].append(entry["error"])
        return entry

    matrix = {base: base_chunks}

    others = [n for n in live_nodes if n != base]
    with ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(others) or 1))) as ex:
        futures = {
            ex.submit(_scan, pool, n, schema, real, cols, types, pk_cols,
                      cuts, watermark, chunk_size=chunk_size): n
            for n in others
        }
        for fut in as_completed(futures):
            n = futures[fut]
            chunks, err = fut.result()
            if chunks is None:
                entry["node_status"][n] = "UNREACHABLE"
                entry["diff_details"].append("%s 读取失败: %s" % (n, err))
            else:
                matrix[n] = chunks
                entry["node_status"][n] = "OK"
                try:
                    entry["rows"][n] = pool.get_row_count(n, schema, real)
                except Exception:
                    pass

    ok_nodes = [n for n in live_nodes if n in matrix]
    if len(ok_nodes) < 2:
        entry["conclusion"] = INCONCLUSIVE
        entry["diff_details"].append("可用节点少于 2 个，无法比较")
        return entry

    n_chunks = len(base_chunks)
    entry["chunks_total"] = n_chunks
    outliers = {}
    unresolved = 0

    for i in range(n_chunks):
        values = {}
        rows = {}
        for n in ok_nodes:
            c = matrix[n][i] if i < len(matrix[n]) else {"rows": 0, "acc": None}
            values[n] = (c["rows"], c["acc"])
            rows[n] = c["rows"]

        verdict, groups, out = _vote(values)
        detail = {
            "chunk": i,
            "verdict": verdict,
            "rows": rows,
            "groups": [sorted(g) for g in groups],
        }
        if verdict == DIVERGENT:
            entry["chunks_diff"] += 1
            for n in out:
                outliers[n] = outliers.get(n, 0) + 1
            detail["outliers"] = sorted(out)
        elif verdict == "UNRESOLVED":
            unresolved += 1
            detail["note"] = "无严格多数派，无法定位异类节点"
            entry["diff_details"].append(
                "Chunk %d: 无严格多数派（分组=%s，行数=%s）—— "
                "无法判定哪一组正确，需要用 --primary 指定基准或人工核对"
                % (i, detail["groups"], rows))

        entry["chunk_detail"].append(detail)

    entry["outliers"] = outliers

    if entry["chunks_diff"]:
        entry["conclusion"] = DIVERGENT
        for d in entry["chunk_detail"]:
            if d["verdict"] == DIVERGENT:
                entry["diff_details"].append(
                    "Chunk %d: 分组=%s 异类=%s 行数=%s" % (
                        d["chunk"], d["groups"], d.get("outliers"), d["rows"]))
        if max_diff_rows > 0:
            entry["localized"] = _localize_matrix(
                pool, entry, matrix, schema, real, cols, types, pk_cols,
                cuts, max_diff_rows)
    elif unresolved or len(ok_nodes) < len(all_nodes):
        entry["conclusion"] = INCONCLUSIVE
    else:
        entry["conclusion"] = CONSISTENT

    missing = [n for n in all_nodes if n not in matrix]
    if missing:
        entry["diff_details"].append(
            "以下节点未参与本表对比: %s" % ", ".join(missing))

    return entry


def _localize_matrix(pool, entry, matrix, schema, table, cols, types,
                     pk_cols, cuts, max_diff_rows):
    """Row-level diff between the two largest groups of a differing chunk.

    Picking one representative from each side of the split keeps this from
    turning into all-pairs localization, which would defeat the point of the
    matrix in the first place.
    """
    out = []
    for d in entry["chunk_detail"]:
        if d["verdict"] != DIVERGENT or len(d["groups"]) < 2:
            continue
        i = d["chunk"]
        side_a, side_b = d["groups"][0], d["groups"][1]
        lo = cuts[i - 1] if i > 0 else None
        hi = cuts[i] if i < len(cuts) else None
        try:
            loc = localize_range(pool, side_a[0], side_b[0], schema, table,
                                 cols, types, pk_cols, lo, hi,
                                 limit=max_diff_rows)
            loc["chunk"] = i
            loc["between"] = [side_a[0], side_b[0]]
            out.append(loc)
        except Exception as e:
            logger.warning("localize chunk %d failed: %s", i, e)
        if len(out) >= 10:
            break
    return out
