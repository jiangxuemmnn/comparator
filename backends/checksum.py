"""Chunked table comparison across database nodes.

Chunks are defined by PRIMARY KEY VALUE RANGES, and the row hashes are
computed in Python after normalizing each value (see utils/normalize.py).

Why keyset ranges instead of OFFSET/LIMIT
-----------------------------------------
The previous implementation paged with `ORDER BY pk OFFSET n LIMIT m`, i.e.
"the n-th row onward". If node B is missing a row near the start, every
subsequent page on B shifts by one, so a single missing row makes EVERY
chunk differ and the real location becomes unfindable. Measured on a live
two-node cluster with exactly one differing row, the old code reported
3/3 chunks different and emitted a phantom third chunk whose B-side checksum
was NULL. Deep OFFSET is also O(offset) per query, making whole-table cost
O(n^2 / chunk_size) -- the opposite of what chunking is for.

A chunk here means "the rows whose primary key falls in this value range".
That set is fixed by the key values alone, so a row-count difference lands
in exactly one chunk and stays findable. `WHERE pk > lo` is an index range
scan, so whole-table cost is O(n).

Why the hashing is not done in SQL
----------------------------------
Not because the SQL is unavailable -- KingbaseES V009R001C010 in oracle mode
supports md5(), string_agg(), listagg(), jsonb, plpgsql and more. It is
because the *text* of a value differs between declarations of the same
logical value: 1.0::numeric renders '1.0' while 1::numeric(10,2) renders
'1.00', char(10) pads with spaces while varchar does not, timestamptz renders
in the session time zone. Hashing server-side would therefore report equal
data as different (or worse). Normalizing in Python is what makes the two
sides actually comparable.

Why an order-independent accumulator
------------------------------------
Rows arrive in whatever order the scan produces, which depends on collation
and index direction. Chaining hashes in arrival order would make two
identical tables hash differently when their collations differ. Each chunk
therefore accumulates sum(H(row)) mod 2^128. SUM rather than XOR: with XOR
a duplicated row would cancel itself out.
"""

from __future__ import annotations

import hashlib
import logging

from utils.db import qualified, quote_ident
from utils.normalize import normalize_row

logger = logging.getLogger("comparator.checksum")

_MASK128 = (1 << 128) - 1

# Types whose values cannot be ordered for range chunking.
_UNRANGEABLE = ("bytea", "json", "jsonb", "array", "user-defined")


# ---------------------------------------------------------------------------
# SQL construction
# ---------------------------------------------------------------------------

def _pk_predicate(pk_cols: list[str], lo=None, hi=None):
    """Lexicographic range predicate over a (possibly composite) key.

    Returns (sql_fragment, params). The expanded form
    `a > ? OR (a = ? AND b > ?)` is used instead of row-value comparison
    `(a,b) > (?,?)` because not every compatibility mode accepts the latter.
    """
    parts, params = [], []

    if lo is not None:
        branch = []
        for i, col in enumerate(pk_cols):
            piece = []
            for j in range(i):
                piece.append("%s = %%s" % quote_ident(pk_cols[j]))
                params.append(lo[j])
            piece.append("%s > %%s" % quote_ident(col))
            params.append(lo[i])
            branch.append("(%s)" % " AND ".join(piece))
        parts.append("(%s)" % " OR ".join(branch))

    if hi is not None:
        branch = []
        for i, col in enumerate(pk_cols):
            piece = []
            for j in range(i):
                piece.append("%s = %%s" % quote_ident(pk_cols[j]))
                params.append(hi[j])
            piece.append("%s <= %%s" % quote_ident(col))
            params.append(hi[i])
            branch.append("(%s)" % " AND ".join(piece))
        parts.append("(%s)" % " OR ".join(branch))

    return " AND ".join(parts) if parts else "1=1", params


def _select_sql(schema, table, cols, pk_cols, where, params, watermark_col=None,
                watermark=None, limit=None):
    sel = ", ".join(quote_ident(c) for c in cols)
    order = ", ".join(quote_ident(c) for c in pk_cols)
    sql = "SELECT %s FROM %s" % (sel, qualified(schema, table))
    all_params = list(params)
    if where and where != "1=1":
        sql += " WHERE " + where
        if watermark is not None and watermark_col:
            sql += " AND %s <= %%s" % quote_ident(watermark_col)
            all_params.append(watermark)
    elif watermark is not None and watermark_col:
        sql += " WHERE %s <= %%s" % quote_ident(watermark_col)
        all_params.append(watermark)
    sql += " ORDER BY " + order
    if limit:
        sql += " LIMIT %d" % int(limit)
    return sql, all_params


def _row_digest(values, types) -> int:
    """128-bit digest of one normalized row, as an int."""
    return int.from_bytes(hashlib.md5(normalize_row(values, types)).digest(), "big")


# ---------------------------------------------------------------------------
# One-pass scan producing chunk digests
# ---------------------------------------------------------------------------

def scan_digests(pool, node, schema, table, cols, types, pk_cols,
                 cuts=None, chunk_size=10000, watermark=None, itersize=5000):
    """Stream the table once and produce a digest per chunk.

    `cuts` is the list of chunk boundaries produced by the base node. When it
    is None this is the base node's pass: a chunk is closed every
    `chunk_size` rows and the last key seen becomes its upper bound. When
    `cuts` is given this is the peer's pass and the identical boundaries are
    reused, so both sides compare the same key ranges regardless of how many
    rows each holds.

    Returns (cuts, chunks) where chunks is a list of {index, rows, acc}.
    """
    wm_col = pk_cols[0] if (watermark is not None and len(pk_cols) == 1) else None
    sql, params = _select_sql(schema, table, cols, pk_cols, None, [],
                              watermark_col=wm_col, watermark=watermark)

    generated_cuts = []
    chunks = []

    cur_rows = 0
    cur_acc = 0
    chunk_idx = 0
    cut_pos = 0
    prev_pk = None

    with pool.stream_cursor(node, sql, params, itersize=itersize) as cur:
        for row in cur:
            values = [row[c] for c in cols]
            pk = tuple(row[c] for c in pk_cols)

            if cuts is None:
                # Base node: close a chunk every chunk_size rows.
                if cur_rows and cur_rows % chunk_size == 0:
                    generated_cuts.append(prev_pk)
                    chunks.append({"index": chunk_idx, "rows": cur_rows,
                                   "acc": cur_acc})
                    chunk_idx += 1
                    cur_rows = 0
                    cur_acc = 0
                prev_pk = pk
            else:
                # Peer node: advance to the chunk this key belongs to.
                while cut_pos < len(cuts) and pk > cuts[cut_pos]:
                    chunks.append({"index": chunk_idx, "rows": cur_rows,
                                   "acc": cur_acc})
                    chunk_idx += 1
                    cut_pos += 1
                    cur_rows = 0
                    cur_acc = 0

            cur_acc = (cur_acc + _row_digest(values, types)) & _MASK128
            cur_rows += 1

        # Flush the final partial chunk.
        chunks.append({"index": chunk_idx, "rows": cur_rows, "acc": cur_acc})

    return (generated_cuts if cuts is None else cuts), chunks


# ---------------------------------------------------------------------------
# Row-level localization
# ---------------------------------------------------------------------------

def localize_range(pool, node_a, node_b, schema, table, cols, types, pk_cols,
                   lo=None, hi=None, limit=200, hard_limit=500000):
    """Find the rows that actually differ inside one key range.

    Merge-joins the two nodes' key-ordered streams in Python. The range is
    bounded by the enclosing chunk, so streaming it whole is what makes the
    result correct -- capping the *queries* instead would silently compare
    only the first N rows and then report everything past that as differing.
    `limit` truncates the returned lists only; `hard_limit` is a runaway
    guard for the pathological case of a key range holding far more rows on
    one side than the chunk size suggests.
    """
    where, params = _pk_predicate(pk_cols, lo, hi)
    sql, sparams = _select_sql(schema, table, cols, pk_cols, where, params)

    def stream(node):
        out = []
        with pool.stream_cursor(node, sql, sparams, itersize=5000) as cur:
            for row in cur:
                pk = tuple(row[c] for c in pk_cols)
                out.append((pk, _row_digest([row[c] for c in cols], types)))
                if len(out) > hard_limit:
                    break
        return out

    a = stream(node_a)
    b = stream(node_b)

    only_a, only_b, changed = [], [], []
    i = j = 0
    while i < len(a) and j < len(b):
        if a[i][0] == b[j][0]:
            if a[i][1] != b[j][1]:
                changed.append(a[i][0])
            i += 1
            j += 1
        elif a[i][0] < b[j][0]:
            only_a.append(a[i][0])
            i += 1
        else:
            only_b.append(b[j][0])
            j += 1
    only_a.extend(pk for pk, _ in a[i:])
    only_b.extend(pk for pk, _ in b[j:])

    return {
        "only_in_a": _fmt_pks(only_a, limit),
        "only_in_b": _fmt_pks(only_b, limit),
        "value_mismatch": _fmt_pks(changed, limit),
        "counts": {
            "only_in_a": len(only_a),
            "only_in_b": len(only_b),
            "value_mismatch": len(changed),
        },
        "truncated": max(len(only_a), len(only_b), len(changed)) > limit,
    }


def _fmt_pks(pks, limit):
    """Render key tuples for the report, truncated to `limit` entries."""
    shown = pks[:limit]
    out = [p[0] if len(p) == 1 else p for p in shown]
    if len(pks) > limit:
        out.append("... (+%d more)" % (len(pks) - limit))
    return out


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compare_table_checksum(pool, node_a: str, node_b: str,
                           schema: str, table: str,
                           chunk_size: int = 10000,
                           max_workers: int = 4,
                           watermark=None,
                           localize: bool = True,
                           max_diff_rows: int = 20) -> dict:
    """Compare one table between two nodes.

    Keeps the original return keys (table / rows_a / rows_b / chunks_total /
    chunks_diff / identical / diff_details) so existing callers and the table
    renderer keep working, and adds confidence + localization detail.
    """
    logger.info("Checksum compare [%s] on %s.%s ...", table, schema, table)

    real = pool.resolve_table(node_a, schema, table)
    pk_cols = pool.get_primary_key(node_a, schema, table)
    col_info = pool.get_column_info(node_a, schema, table)
    cols = [c["name"] for c in col_info]
    types = [c["type"] for c in col_info]

    if not cols:
        raise ValueError("No columns found for %s.%s" % (schema, table))

    rows_a = pool.get_row_count(node_a, schema, real)
    rows_b = pool.get_row_count(node_b, schema, real)

    # ---- No primary key: row counts only, and say so plainly ----
    if not pk_cols:
        logger.warning("%s.%s has no primary key; falling back to row-count only",
                       schema, table)
        return {
            "table": "%s.%s" % (schema, table),
            "method": "row_count",
            "rows_a": rows_a,
            "rows_b": rows_b,
            "identical": rows_a == rows_b,
            "chunks_total": 0,
            "chunks_diff": 0,
            "confidence": "row_count_only",
            "note": "无主键，只能比较行数；行数相同不代表内容相同",
            "diff_details": ([] if rows_a == rows_b else
                             ["Row count mismatch: %d vs %d" % (rows_a, rows_b)]),
        }

    if rows_a == 0 and rows_b == 0:
        return {
            "table": "%s.%s" % (schema, table),
            "method": "checksum",
            "rows_a": 0, "rows_b": 0,
            "chunks_total": 0, "chunks_diff": 0,
            "identical": True,
            "confidence": "deterministic",
            "diff_details": [],
        }

    # ---- Range chunking is only meaningful for orderable keys ----
    type_map = {c["name"]: c["type"] for c in col_info}
    pk_type = (type_map.get(pk_cols[0]) or "").lower()
    rangeable = not any(k in pk_type for k in _UNRANGEABLE)

    if watermark is not None and len(pk_cols) != 1:
        logger.warning("watermark ignored: requires a single-column primary key")

    if rangeable:
        cuts, chunks_a = scan_digests(
            pool, node_a, schema, real, cols, types, pk_cols,
            cuts=None, chunk_size=chunk_size, watermark=watermark)
        _, chunks_b = scan_digests(
            pool, node_b, schema, real, cols, types, pk_cols,
            cuts=cuts, chunk_size=chunk_size, watermark=watermark)
    else:
        # Unorderable key (bytea/json): one accumulator over the whole table.
        cuts = []
        _, chunks_a = scan_digests(
            pool, node_a, schema, real, cols, types, pk_cols, cuts=[])
        _, chunks_b = scan_digests(
            pool, node_b, schema, real, cols, types, pk_cols, cuts=[])

    # Align by index; pad the shorter side so a missing tail still surfaces.
    n = max(len(chunks_a), len(chunks_b))
    while len(chunks_a) < n:
        chunks_a.append({"index": len(chunks_a), "rows": 0, "acc": None})
    while len(chunks_b) < n:
        chunks_b.append({"index": len(chunks_b), "rows": 0, "acc": None})

    diff_details = []
    chunk_detail = []
    for i in range(n):
        ca, cb = chunks_a[i], chunks_b[i]
        same = (ca["rows"] == cb["rows"] and ca["acc"] == cb["acc"])
        chunk_detail.append({
            "chunk": i,
            "rows_a": ca["rows"],
            "rows_b": cb["rows"],
            "identical": same,
        })
        if not same:
            diff_details.append(
                "Chunk %d: rows_a=%d rows_b=%d, checksum_a=%s checksum_b=%s" % (
                    i, ca["rows"], cb["rows"],
                    _hex(ca["acc"]), _hex(cb["acc"])))

    chunks_diff = len(diff_details)

    result = {
        "table": "%s.%s" % (schema, table),
        "method": "checksum" if rangeable else "checksum_whole_table",
        "rows_a": rows_a,
        "rows_b": rows_b,
        "chunks_total": n,
        "chunks_diff": chunks_diff,
        "identical": chunks_diff == 0,
        "confidence": "deterministic" if watermark is not None else "snapshot",
        "chunk_detail": chunk_detail,
        "diff_details": diff_details,
    }
    if not rangeable:
        result["note"] = ("主键类型 %s 不可做范围分块，已退化为整表单累加器" % pk_type)

    # ---- Localize the differing chunks to concrete rows ----
    if chunks_diff and localize and rangeable:
        result["localized"] = _localize_chunks(
            pool, node_a, node_b, schema, real, cols, types, pk_cols,
            cuts, chunk_detail, max_diff_rows)

    return result


def _localize_chunks(pool, node_a, node_b, schema, table, cols, types, pk_cols,
                     cuts, chunk_detail, max_diff_rows):
    out = []
    for detail in chunk_detail:
        if detail["identical"]:
            continue
        i = detail["chunk"]
        lo = cuts[i - 1] if i > 0 else None
        hi = cuts[i] if i < len(cuts) else None
        try:
            d = localize_range(pool, node_a, node_b, schema, table,
                               cols, types, pk_cols, lo, hi, limit=max_diff_rows)
            d["chunk"] = i
            out.append(d)
        except Exception as e:
            logger.warning("localize chunk %d failed: %s", i, e)
        if len(out) >= 20:
            break
    return out


def _hex(value):
    if value is None:
        return "EMPTY"
    return "%032x" % value
