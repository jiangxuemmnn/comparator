"""Exact row-level comparison between two nodes.

This engine used to build a cross-node `EXCEPT` query:

    SELECT cols FROM public.accounts EXCEPT SELECT cols FROM public.accounts

Both sides named the SAME table, and the statement was executed on ONE
connection, so it compared a table with itself and returned an empty set
every time. `identical` was therefore always True -- the engine was a
no-op that reported every pair of divergent tables as consistent.

A real cross-node EXCEPT is not expressible in plain SQL: the two tables
live on different servers, so it would need dblink or postgres_fdw, which
require a superuser-installed extension whose availability varies by
compatibility mode. Rather than take that dependency, this engine now
streams both sides key-ordered and merge-joins them in Python, reusing the
same normalization the checksum engine uses.

The user-visible promise is unchanged -- "show me exactly which rows differ"
-- and it is now actually kept. Use it on tables small enough to read in
full; for large tables screen with the checksum engine first, which already
localizes any chunk it flags.
"""

from __future__ import annotations

import logging

from backends.checksum import localize_range

logger = logging.getLogger("comparator.native")

# Safety valve for a table far larger than row-level diffing is meant for.
DEFAULT_ROW_CAP = 200000


def compare_table_native(pool, node_a: str, node_b: str,
                         schema: str, table: str,
                         max_diff_rows: int = 500,
                         max_workers: int = 4,
                         row_cap: int = DEFAULT_ROW_CAP) -> dict:
    """Compare every row of one table between two nodes.

    Returns the original keys (missing_in_a_count / missing_in_b_count /
    diff_sample) so existing callers keep working, plus explicit counts and
    a confidence marker.
    """
    logger.info("Native compare [%s] on %s.%s ...", table, schema, table)

    real = pool.resolve_table(node_a, schema, table)
    pk_cols = pool.get_primary_key(node_a, schema, table)
    col_info = pool.get_column_info(node_a, schema, table)
    cols = [c["name"] for c in col_info]
    types = [c["type"] for c in col_info]

    rows_a = pool.get_row_count(node_a, schema, real)
    rows_b = pool.get_row_count(node_b, schema, real)

    if not cols:
        return {
            "table": "%s.%s" % (schema, table),
            "method": "native",
            "rows_a": 0, "rows_b": 0,
            "identical": True,
            "confidence": "unavailable",
            "note": "表中没有列",
            "diff_sample": {"in_a_not_in_b": [], "in_b_not_in_a": []},
        }

    if not pk_cols:
        # Without a key there is no way to line rows up between the two
        # sides, so a row-level verdict cannot be given. Say so instead of
        # returning a comparison that only looks meaningful.
        logger.warning("%s.%s has no primary key; row-level diff unavailable",
                       schema, table)
        return {
            "table": "%s.%s" % (schema, table),
            "method": "native",
            "rows_a": rows_a, "rows_b": rows_b,
            "identical": rows_a == rows_b,
            "confidence": "row_count_only",
            "note": "无主键，无法按行对齐；只能比较行数",
            "missing_in_a_count": None,
            "missing_in_b_count": None,
            "diff_sample": {"in_a_not_in_b": [], "in_b_not_in_a": []},
        }

    if max(rows_a, rows_b) > row_cap:
        return {
            "table": "%s.%s" % (schema, table),
            "method": "native",
            "rows_a": rows_a, "rows_b": rows_b,
            "identical": False,
            "confidence": "aborted",
            "note": ("表太大（%d 行 > %d），行级对比会读取全表。"
                     "请改用 --backend checksum 先筛出差异块。" % (
                         max(rows_a, rows_b), row_cap)),
            "diff_sample": {"in_a_not_in_b": [], "in_b_not_in_a": []},
        }

    result = localize_range(pool, node_a, node_b, schema, real,
                            cols, types, pk_cols, lo=None, hi=None,
                            limit=max_diff_rows, hard_limit=row_cap)

    # in_a_not_in_b = rows that exist on A but are missing on B, and so on.
    missing_in_b = result["only_in_a"]
    missing_in_a = result["only_in_b"]
    changed = result["value_mismatch"]
    counts = result["counts"]

    identical = not (counts["only_in_a"] or counts["only_in_b"]
                     or counts["value_mismatch"])

    return {
        "table": "%s.%s" % (schema, table),
        "method": "native",
        "rows_a": rows_a,
        "rows_b": rows_b,
        "identical": identical,
        "confidence": "deterministic" if identical else "snapshot",
        "missing_in_a_count": counts["only_in_b"],
        "missing_in_b_count": counts["only_in_a"],
        "value_mismatch_count": counts["value_mismatch"],
        "diff_sample": {
            "in_a_not_in_b": missing_in_b,
            "in_b_not_in_a": missing_in_a,
            "value_mismatch": changed,
        },
        "truncated": result["truncated"],
    }
