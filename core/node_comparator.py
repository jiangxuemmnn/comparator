"""Node-to-node data consistency verification.

Compares data across database cluster nodes using pluggable backends.
Supports: checksum (fast, chunked), native (EXCEPT queries),
pg_comparator (external), data-diff (external).
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db import DatabasePool
from backends.checksum import compare_table_checksum
from backends.native import compare_table_native
from backends.pg_comparator import run_pg_comparator, check_pg_comparator_installed
from backends.data_diff import run_data_diff, check_data_diff_installed

logger = logging.getLogger("comparator.node")


BACKENDS = {
    "checksum": "Chunked MD5 checksum comparison (fast, built-in)",
    "native": "EXCEPT-based row comparison (accurate, built-in)",
    "pg_comparator": "pg_comparator external tool",
    "data_diff": "data-diff external tool",
}


def list_backends() -> dict:
    """List available backends and their status."""
    result = {}
    for name, desc in BACKENDS.items():
        available = True
        if name == "pg_comparator":
            available = check_pg_comparator_installed()
        elif name == "data_diff":
            available = check_data_diff_installed()
        result[name] = {"description": desc, "available": available}
    return result


def run_node_comparison(pool: DatabasePool, config: dict,
                        node_a: str, node_b: str,
                        backend: str = "checksum",
                        tables: list[str] = None) -> dict:
    """Compare all (or specified) tables between two database nodes.

    Args:
        pool: DatabasePool instance
        config: Full configuration dict
        node_a: Name of first node (key in config['databases'])
        node_b: Name of second node (key in config['databases'])
        backend: Comparison backend ('checksum', 'native', 'pg_comparator', 'data_diff')
        tables: Optional list of tables to compare (None = all user tables)

    Returns:
        dict with: success, backend, tables_checked, tables_diff,
        total_chunks_diff, total_time, results (list of per-table results).
    """
    cmp_cfg = config.get("comparison", {})
    schema = cmp_cfg.get("schema", "public")
    chunk_size = cmp_cfg.get("chunk_size", 10000)
    parallel = cmp_cfg.get("parallel", 4)
    exclude = set(cmp_cfg.get("exclude_tables", []))

    if tables is None:
        tables = pool.get_tables(node_a, schema)
        tables = [t for t in tables if t not in exclude]

    if not tables:
        logger.warning("No tables found to compare in schema '%s'", schema)
        return {
            "success": True,
            "backend": backend,
            "tables_checked": 0,
            "tables_diff": 0,
            "results": [],
        }

    logger.info("Node comparison: %s vs %s, backend=%s, %d tables",
                node_a, node_b, backend, len(tables))

    start_time = time.time()

    # Use external tools if requested
    if backend == "pg_comparator":
        result = run_pg_comparator(config, node_a, node_b, tables)
        result["backend"] = backend
        result["total_time"] = time.time() - start_time
        return result

    if backend == "data_diff":
        result = run_data_diff(config, node_a, node_b, tables)
        result["backend"] = backend
        result["total_time"] = time.time() - start_time
        return result

    # Built-in backends
    compare_func = compare_table_checksum if backend == "checksum" else compare_table_native

    results = []
    tables_diff = 0
    total_chunks = 0

    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {}
        for table in tables:
            if backend == "checksum":
                f = executor.submit(compare_func, pool, node_a, node_b,
                                    schema, table, chunk_size, 1)
            else:
                f = executor.submit(compare_func, pool, node_a, node_b,
                                    schema, table, parallel, 1)
            futures[f] = table

        for future in as_completed(futures):
            table = futures[future]
            try:
                r = future.result()
                results.append(r)
                if not r["identical"]:
                    tables_diff += 1
                    logger.warning("Table %s differs!", table)
                if "chunks_total" in r:
                    total_chunks += r["chunks_total"]
            except Exception as e:
                logger.error("Failed to compare table %s: %s", table, e)
                results.append({
                    "table": "%s.%s" % (schema, table),
                    "error": str(e),
                    "identical": False,
                })
                tables_diff += 1

    total_time = time.time() - start_time

    return {
        "success": tables_diff == 0,
        "backend": backend,
        "tables_checked": len(results),
        "tables_diff": tables_diff,
        "total_chunks_diff": sum(
            r.get("chunks_diff", 0) for r in results
        ),
        "total_time": round(total_time, 2),
        "results": results,
    }
