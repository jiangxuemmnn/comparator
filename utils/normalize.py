"""Cross-node value normalization.

Two database nodes can hold the *same logical value* while psycopg2 hands
back different Python objects, or the same object renders differently.
Comparing raw values therefore produces false mismatches. This module maps
a value + its declared column type to a canonical byte string so that
"same bytes" really does mean "same value".

Traps this module exists to handle (all confirmed by实测 on
KingbaseES V009R001C010, database_mode=oracle -- see deepseek/baseline_evidence.md):

  numeric scale     1.0::numeric       -> Decimal('1.0')  -> '1.0'
                    1::numeric(10,2)   -> Decimal('1.00') -> '1.00'
                    Same logical value, different ::text. Never round-trip
                    numbers through SQL text or hash them server-side.

  char() padding    'abc'::char(10)    -> 'abc       ' (7 trailing spaces)
                    'abc'::varchar(10) -> 'abc'         (trailing spaces kept)
                    Padding is an artifact of the type, not the data. Only
                    `character` pads, so only it gets rstripped.

  empty vs NULL     In Oracle mode SELECT ''::text returns NULL -- the two are
                    indistinguishable at the client. Other modes keep them
                    distinct. Both are handled, but a comparison that depends
                    on telling them apart cannot be trusted in Oracle mode.

  date type         DATE is handed back as datetime, not date.

  timestamptz       Rendered in the session time zone. Two nodes with
                    different TimeZone settings would disagree on the same
                    instant, so everything is converted to UTC first.

  bytea             Comes back as memoryview / bytes.

Usage:

    from utils.normalize import normalize_row, values_equal

    cols  = [c["name"] for c in pool.get_column_info(node, schema, table)]
    types = [c["type"] for c in pool.get_column_info(node, schema, table)]

    digest = normalize_row(row_values, types)     # canonical bytes
    same   = values_equal(a, b, "character varying")
"""

from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation

# Canonical NULL marker. A byte that cannot appear inside the length-prefixed
# encoding of a real value, so NULL can never collide with any non-NULL value.
NULL_TAG = b"\x00"


# ---------------------------------------------------------------------------
# 列类型判定
# ---------------------------------------------------------------------------

def _t(data_type: str) -> str:
    """Normalize an information_schema `data_type` string for matching."""
    return (data_type or "").strip().lower()


def is_char_padded(data_type: str) -> bool:
    """True for blank-padded fixed-length types (CHAR(n) / CHARACTER(n)).

    information_schema reports these as `character`, `char`, or -- on
    KingbaseES -- `bpchar`. Deliberately excludes `character varying`,
    whose trailing spaces are real data.
    """
    t = _t(data_type)
    if t.startswith("character varying") or t.startswith("varchar"):
        return False
    return t in ("character", "char", "bpchar") or t.startswith("character(")


def is_float(data_type: str) -> bool:
    """True for approximate numeric types, whose comparison cannot be exact."""
    t = _t(data_type)
    return any(k in t for k in ("double precision", "real", "float"))


def is_temporal(data_type: str) -> bool:
    t = _t(data_type)
    return any(k in t for k in ("timestamp", "date", "time"))


def is_boolean(data_type: str) -> bool:
    t = _t(data_type)
    return t in ("boolean", "bool")


def is_integral(data_type: str) -> bool:
    t = _t(data_type)
    return any(k in t for k in ("integer", "int", "bigint", "smallint", "serial"))


def is_exact_numeric(data_type: str) -> bool:
    """True for exact numeric types (NUMERIC / DECIMAL / NUMBER)."""
    t = _t(data_type)
    return any(k in t for k in ("numeric", "decimal", "number")) and "double" not in t


def _coerce_numeric(value, data_type):
    """Align a JSON-sourced number with the column's declared type.

    Values recorded by the application arrive as JSON, so an amount written
    as 174306.15 comes back from json.loads as a float, while the same column
    read from the database yields Decimal('174306.15'). Comparing them
    unchanged would report every numeric column as a mismatch.

    Decimal(str(x)) rather than Decimal(x): the repr of a float carries
    binary rounding artifacts that Decimal would faithfully preserve.
    """
    if not is_exact_numeric(data_type):
        return value
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return value
    if isinstance(value, str):
        # JSON has no way to say "numeric", so a client may send the value
        # as a string. The column type settles the question.
        try:
            return Decimal(value.strip())
        except (InvalidOperation, ValueError, AttributeError):
            return value
    return value


# Timestamp formats a client is likely to put in a JSON payload. Ordered
# most-specific first so a value with fractional seconds is not truncated.
_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
)


def _coerce_temporal(value, data_type):
    """Parse a JSON timestamp string into a datetime.

    JSON has no date type, so an application recording `created_at` hands
    over a string while the database returns a datetime. Without this the
    two never match and every timestamped column is reported as a mismatch.
    """
    if not is_temporal(data_type) or not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    for fmt in _TIME_FORMATS:
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        return value


# ---------------------------------------------------------------------------
# 标量归一化
# ---------------------------------------------------------------------------

def _decimal_bytes(value: Decimal) -> bytes:
    """Canonical text for a Decimal.

    Strips scale differences (1.0 / 1.00 / 1 -> '1') and collapses -0 to 0,
    while never falling back to float (which would lose precision).
    """
    if not value.is_finite():
        return ("N" + str(value)).encode("utf-8")
    try:
        # normalize() strips trailing zeros but may introduce an exponent
        # (Decimal('100').normalize() -> Decimal('1E+2')); 'f' formatting
        # renders it back without one.
        text = format(value.normalize(), "f")
    except (InvalidOperation, ValueError):
        text = str(value)
    if text in ("-0", "-0.0"):
        text = "0"
    return text.encode("utf-8")


def _temporal_bytes(value) -> bytes:
    """Canonical text for date/time values, always in UTC when tz-aware."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc)
            # isoformat() renders '+00:00'; make it uniform and explicit.
            return ("T" + value.isoformat()).encode("utf-8")
        return ("T" + value.isoformat()).encode("utf-8")
    if isinstance(value, datetime.date):
        return ("T" + value.isoformat()).encode("utf-8")
    if isinstance(value, datetime.timedelta):
        # Oracle mode maps DATE to datetime, but intervals can still show up.
        return ("T" + str(value)).encode("utf-8")
    return ("T" + str(value)).encode("utf-8")


def normalize_value(value, data_type: str = None) -> bytes:
    """Map a single value to its canonical byte string.

    `data_type` comes from information_schema.columns.data_type and is what
    lets us tell blank-padded CHAR from space-preserving VARCHAR. When it is
    unavailable the value is normalized structurally, which is correct for
    everything except the CHAR-padding case.
    """
    if value is None:
        return NULL_TAG

    # --- bool before int: bool is a subclass of int in Python ---
    if isinstance(value, bool):
        return b"B1" if value else b"B0"

    if data_type:
        value = _coerce_numeric(value, data_type)
        value = _coerce_temporal(value, data_type)

    if isinstance(value, int):
        return ("I" + str(value)).encode("utf-8")

    if isinstance(value, Decimal):
        return _decimal_bytes(value)

    if isinstance(value, float):
        # Approximate types cannot be compared exactly; repr() gives the
        # shortest string that round-trips, so identical binary values still
        # compare equal.
        return ("F" + repr(value)).encode("utf-8")

    if isinstance(value, str):
        if data_type and is_char_padded(data_type):
            value = value.rstrip(" ")
        return b"S" + value.encode("utf-8")

    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("X" + bytes(value).hex()).encode("utf-8")

    if isinstance(value, (datetime.datetime, datetime.date, datetime.timedelta)):
        return _temporal_bytes(value)

    if isinstance(value, (list, tuple)):
        # Arrays / composites: normalize element-wise with their own types.
        parts = [normalize_value(v) for v in value]
        return b"A" + b"".join(_length_prefix(p) for p in parts)

    if isinstance(value, dict):
        # jsonb comes back already parsed. Sort keys so key order in the
        # stored document cannot cause a false difference.
        parts = []
        for k in sorted(value.keys()):
            parts.append(normalize_value(k))
            parts.append(normalize_value(value[k]))
        return b"J" + b"".join(_length_prefix(p) for p in parts)

    # Fallback: str() is lossy in principle, but reaching here means an
    # unhandled type -- better to compare something than to crash.
    return b"?" + str(value).encode("utf-8")


def _length_prefix(payload: bytes) -> bytes:
    """Length-prefix a payload so concatenation cannot be ambiguous.

    The old checksum engine joined column values with a '|' separator, which
    collides: ('a|b', 'c') and ('a', 'b|c') produced identical strings. A
    length prefix removes the possibility at the byte level.
    """
    return str(len(payload)).encode("ascii") + b":" + payload


# ---------------------------------------------------------------------------
# 行级 API
# ---------------------------------------------------------------------------

def normalize_row(values, data_types=None) -> bytes:
    """Canonical encoding of a whole row, order-sensitive.

    Each value is length-prefixed, so no two distinct row shapes can encode
    to the same bytes.
    """
    types = data_types or []
    out = []
    for i, v in enumerate(values):
        dt = types[i] if i < len(types) else None
        out.append(_length_prefix(normalize_value(v, dt)))
    return b"".join(out)


def values_equal(a, b, data_type: str = None) -> bool:
    """Cross-node equality for a single column value."""
    return normalize_value(a, data_type) == normalize_value(b, data_type)


def compare_rows(expected: dict, actual: dict, data_types: dict = None):
    """Column-by-column comparison of two row dicts.

    Returns a list of human-readable difference strings -- empty means equal.
    Used by the app-DB verifier, whose previous str()-based comparison
    reported Decimal('100.00') vs Decimal('100.0') and bool vs 1 as
    mismatches.

    Columns present in `actual` but absent from `expected` are not reported:
    the tracking record only carries the columns the application claimed to
    write, and untracked columns carry no expectation.
    """
    types = data_types or {}
    diffs = []
    for key in expected:
        if key not in actual:
            diffs.append("%s: missing in actual" % key)
            continue
        dt = types.get(key)
        if not values_equal(expected[key], actual[key], dt):
            diffs.append("%s: expected=%r, actual=%r" % (
                key, expected[key], actual[key],
            ))
    return diffs
