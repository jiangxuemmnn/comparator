"""Checksum-based table comparison - fast block-level consistency check.

Splits tables into chunks by primary key ranges, computes MD5 checksum
for each chunk, and compares across database nodes.
"""

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("comparator.checksum")


def _quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


def _build_chunk_query(schema: str, table: str, pk_cols: list[str],
                       columns: list[str], start: int, size: int) -> str:
    """Build SQL to compute checksum for one chunk."""
    col_exprs = []
    for c in columns:
        col_exprs.append("COALESCE(%s::text, '\\N')" % _quote_ident(c))

    concat = " || '|' || ".join(col_exprs)
    pk_order = ", ".join(_quote_ident(c) for c in pk_cols)

    return (
        "SELECT MD5(string_agg(row_hash, '' ORDER BY %s)) "
        "FROM ("
        "  SELECT MD5(%s) AS row_hash "
        "  FROM %s.%s "
        "  ORDER BY %s "
        "  OFFSET %d LIMIT %d"
        ") AS chunk"
    ) % (pk_order, concat, _quote_ident(schema), _quote_ident(table),
         pk_order, start, size)


def _build_row_count_query(schema: str, table: str) -> str:
    return "SELECT COUNT(*) FROM %s.%s" % (_quote_ident(schema), _quote_ident(table))


def compare_table_checksum(pool, node_a: str, node_b: str,
                           schema: str, table: str,
                           chunk_size: int = 10000,
                           max_workers: int = 4) -> dict:
    """Compare a table between two nodes using checksum chunking.

    Returns dict with: table, rows_a, rows_b, chunks_total, chunks_diff,
    identical (bool), diff_details (list).
    """
    logger.info("Checksum compare [%s] on %s.%s ...", table, schema, table)

    # Get PK and column info from node A
    pk_cols = pool.get_primary_key(node_a, schema, table)
    if not pk_cols:
        logger.warning("Table %s.%s has no primary key, falling back to row-count only", schema, table)
        rows_a = pool.get_row_count(node_a, schema, table)
        rows_b = pool.get_row_count(node_b, schema, table)
        return {
            "table": "%s.%s" % (schema, table),
            "method": "row_count",
            "rows_a": rows_a,
            "rows_b": rows_b,
            "identical": rows_a == rows_b,
            "diff_details": [] if rows_a == rows_b else [
                "Row count mismatch: %d vs %d" % (rows_a, rows_b)
            ],
        }

    cols = [c["name"] for c in pool.get_column_info(node_a, schema, table)]
    rows_a = pool.get_row_count(node_a, schema, table)
    rows_b = pool.get_row_count(node_b, schema, table)

    if rows_a == 0 and rows_b == 0:
        return {
            "table": "%s.%s" % (schema, table),
            "method": "checksum",
            "rows_a": 0, "rows_b": 0,
            "chunks_total": 0, "chunks_diff": 0,
            "identical": True,
            "diff_details": [],
        }

    # Compute checksums in parallel
    total_chunks = (max(rows_a, rows_b) + chunk_size - 1) // chunk_size
    diff_details = []

    def _check_chunk(chunk_idx: int):
        start = chunk_idx * chunk_size
        sql_a = _build_chunk_query(schema, table, pk_cols, cols, start, chunk_size)
        sql_b = _build_chunk_query(schema, table, pk_cols, cols, start, chunk_size)

        try:
            chk_a = pool.execute(node_a, sql_a)[0][0]
        except Exception as e:
            logger.error("Node %s chunk %d failed: %s", node_a, chunk_idx, e)
            chk_a = None

        try:
            chk_b = pool.execute(node_b, sql_b)[0][0]
        except Exception as e:
            logger.error("Node %s chunk %d failed: %s", node_b, chunk_idx, e)
            chk_b = None

        if chk_a != chk_b:
            return {
                "chunk": chunk_idx,
                "start": start,
                "size": chunk_size,
                "checksum_a": chk_a,
                "checksum_b": chk_b,
                "detail": (
                    "Chunk %d (offset %d, limit %d) differs: "
                    "checksum_a=%s, checksum_b=%s"
                ) % (chunk_idx, start, chunk_size,
                     chk_a or "ERROR", chk_b or "ERROR"),
            }
        return None

    diff_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_check_chunk, i): i for i in range(total_chunks)}
        for future in as_completed(futures):
            result = future.result()
            if result:
                diff_count += 1
                diff_details.append(result)

    return {
        "table": "%s.%s" % (schema, table),
        "method": "checksum",
        "rows_a": rows_a,
        "rows_b": rows_b,
        "chunks_total": total_chunks,
        "chunks_diff": diff_count,
        "identical": diff_count == 0,
        "diff_details": [d["detail"] for d in diff_details],
    }
