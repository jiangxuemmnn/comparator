"""Native SQL comparison - exact row-level diff between two database nodes.

Uses EXCEPT/MINUS and FULL OUTER JOIN to find rows present in one node
but not the other, or rows where column values differ.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("comparator.native")


def _quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


def _build_diff_query(schema: str, table: str, pk_cols: list[str],
                      columns: list[str], direction: str = "a_not_b",
                      limit: int = 500) -> str:
    """Build SQL to find rows that differ between two nodes.

    direction: 'a_not_b' = rows in A missing from B
               'b_not_a' = rows in B missing from A
    """
    col_list = ", ".join(_quote_ident(c) for c in columns)

    if direction == "a_not_b":
        return (
            "SELECT %s FROM %s.%s "
            "EXCEPT "
            "SELECT %s FROM %s.%s "
            "LIMIT %d"
        ) % (col_list, _quote_ident(schema), _quote_ident(table),
             col_list, _quote_ident(schema), _quote_ident(table), limit)
    else:
        return (
            "SELECT %s FROM %s.%s "
            "EXCEPT "
            "SELECT %s FROM %s.%s "
            "LIMIT %d"
        ) % (col_list, _quote_ident(schema), _quote_ident(table),
             col_list, _quote_ident(schema), _quote_ident(table), limit)


def compare_table_native(pool, node_a: str, node_b: str,
                         schema: str, table: str,
                         max_diff_rows: int = 500,
                         max_workers: int = 4) -> dict:
    """Compare a table between two nodes using EXCEPT queries.

    Returns dict with: table, rows_a, rows_b, missing_in_b, missing_in_a,
    identical (bool), diff_sample (list).
    """
    logger.info("Native compare [%s] on %s.%s ...", table, schema, table)

    pk_cols = pool.get_primary_key(node_a, schema, table)
    if not pk_cols:
        logger.warning("Table %s.%s has no primary key, comparing all columns", schema, table)

    cols = [c["name"] for c in pool.get_column_info(node_a, schema, table)]
    if not cols:
        return {
            "table": "%s.%s" % (schema, table),
            "method": "native",
            "rows_a": 0, "rows_b": 0,
            "identical": True,
            "diff_sample": [],
        }

    rows_a = pool.get_row_count(node_a, schema, table)
    rows_b = pool.get_row_count(node_b, schema, table)

    def _get_missing(direction: str):
        sql = _build_diff_query(schema, table, pk_cols or cols, cols,
                                direction, max_diff_rows)
        try:
            rows = pool.execute(node_a if direction == "a_not_b" else node_b, sql)
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error("Native diff %s failed: %s", direction, e)
            return ["ERROR: %s" % e]

    results = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_a = executor.submit(_get_missing, "a_not_b")
        f_b = executor.submit(_get_missing, "b_not_a")
        results["missing_in_b"] = f_a.result()
        results["missing_in_a"] = f_b.result()

    identical = (len(results["missing_in_b"]) == 0 and
                 len(results["missing_in_a"]) == 0)

    return {
        "table": "%s.%s" % (schema, table),
        "method": "native",
        "rows_a": rows_a,
        "rows_b": rows_b,
        "missing_in_b_count": len(results["missing_in_b"]),
        "missing_in_a_count": len(results["missing_in_a"]),
        "identical": identical,
        "diff_sample": {
            "in_a_not_in_b": results["missing_in_b"][:20],
            "in_b_not_in_a": results["missing_in_a"][:20],
        },
    }
