"""Application-to-database data consistency verification.

Two integration modes:

  1. JMeter mode (recommended for performance testing):
     - Tool creates a tracking table + stored function on the database
     - JMeter calls the function after each business write via JDBC PostProcessor
     - Tool reads the tracking table and verifies all entries
     - No Python coding needed

  2. Python API mode (for custom automation):
     - Use AppDBTracker class in Python scripts
     - Call tracker.record_*() after each business operation
     - Call tracker.verify_all() to check
"""

from __future__ import annotations

import logging
import json
import uuid
from datetime import datetime

from utils.db import DatabasePool, qualified
from utils.normalize import compare_rows

logger = logging.getLogger("comparator.appdb")

CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS {schema}._app_db_tracking (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    txn_id VARCHAR(64),
    request_id VARCHAR(128),
    operation VARCHAR(16) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    pk_values JSONB NOT NULL,
    row_data JSONB,
    created_at {ts} DEFAULT CURRENT_TIMESTAMP,
    verified BOOLEAN DEFAULT FALSE,
    verify_result VARCHAR(512)
);
"""

# Columns added after the first release. Listed as (name, type) rather than
# as ready-made ALTER statements: the statements need `ADD COLUMN IF NOT
# EXISTS`, and Kingbase's sqlserver compatibility mode rejects the word
# COLUMN entirely (`ALTER TABLE t ADD c type` is accepted everywhere).
MIGRATE_TRACKING_COLUMNS = [
    ("txn_id", "VARCHAR(64)"),
    ("request_id", "VARCHAR(128)"),
]

# Stored function that JMeter calls after each business write.
#
# txn_id groups several writes that belong to one business transaction, so a
# transaction that only partly landed can be detected. request_id identifies
# one client request, so a failed JMeter sample can be matched against what
# actually reached the database.
#
# Returns VOID, not the new row id: the caller (a JDBC PostProcessor) never
# reads it, and plpgsql's `INTO` assignment is rejected by the sqlserver
# compatibility mode.
#
# NOT created on sqlserver-mode nodes -- see INSERT_TRACK_ROW_TEMPLATE.
CREATE_TRACK_FUNCTION = """
CREATE OR REPLACE FUNCTION {schema}.sp_track_write(
    p_batch_id VARCHAR,
    p_operation VARCHAR,
    p_table_name VARCHAR,
    p_pk_values VARCHAR,
    p_row_data VARCHAR DEFAULT NULL,
    p_txn_id VARCHAR DEFAULT NULL,
    p_request_id VARCHAR DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    INSERT INTO {schema}._app_db_tracking
        (batch_id, txn_id, request_id, operation, table_name, pk_values, row_data)
    VALUES (p_batch_id, p_txn_id, p_request_id, p_operation, p_table_name,
            p_pk_values::jsonb, p_row_data::jsonb);
END;
$$ LANGUAGE plpgsql;
"""

# Portable alternative to the stored function, used where the function
# cannot exist. Kingbase's sqlserver compatibility mode rejects ANY INSERT
# inside a plpgsql function body (PERFORM works, DML does not), so a tracked
# write there has to be a plain INSERT issued by the client:
#
#   INSERT INTO {schema}._app_db_tracking
#       (batch_id, txn_id, request_id, operation, table_name, pk_values, row_data)
#   VALUES (?, ?, ?, ?, ?, ?, ?)
#
# The jsonb columns accept the bound string values without an explicit cast
# in all four modes (verified).
INSERT_TRACK_ROW_TEMPLATE = """INSERT INTO {schema}._app_db_tracking
    (batch_id, txn_id, request_id, operation, table_name, pk_values, row_data)
VALUES (%s, %s, %s, %s, %s, %s, %s)"""

# All historical signatures, so a table created by an older release can be
# torn down cleanly.
DROP_TRACK_FUNCTION = """
DROP FUNCTION IF EXISTS {schema}.sp_track_write(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
DROP FUNCTION IF EXISTS {schema}.sp_track_write(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
"""


def setup_tracking(pool: DatabasePool, node: str, schema: str = "public",
                   with_function: bool = None):
    """Create the tracking table (and, where possible, the stored function).

    Returns a dict describing what the client should call:
        {"function": bool, "insert_sql": str, "mode": str}

    `with_function=None` decides automatically: the function is skipped on
    sqlserver-mode nodes, where an INSERT inside plpgsql cannot execute.
    """
    if with_function is None:
        with_function = not pool.is_sqlserver(node)

    pool.execute_ddl(node, CREATE_TRACKING_TABLE.format(
        schema=schema, ts=pool.timestamp_type(node)))

    # Add columns an older install may lack. `ADD <col> <type>` without the
    # COLUMN keyword is the only form every mode accepts.
    try:
        existing = {c["name"] for c in pool.get_column_info(
            node, schema, "_app_db_tracking")}
    except Exception:
        existing = set()
    for col, coltype in MIGRATE_TRACKING_COLUMNS:
        if col in existing:
            continue
        try:
            pool.execute_ddl(
                node,
                "ALTER TABLE %s ADD %s %s"
                % (qualified(schema, "_app_db_tracking"), col, coltype))
        except Exception as e:
            logger.warning("Adding column %s to _app_db_tracking failed: %s", col, e)

    if with_function:
        # Dropped first because CREATE OR REPLACE cannot change a function's
        # return type, and an older install created this one returning
        # INTEGER. Dropping by argument types also works on that version.
        try:
            pool.execute_ddl(node, DROP_TRACK_FUNCTION.format(schema=schema))
        except Exception as e:
            logger.debug("drop of old sp_track_write skipped: %s", e)
        pool.execute_ddl(node, CREATE_TRACK_FUNCTION.format(schema=schema))

    logger.info("Tracking infrastructure created on node '%s' (table%s)",
                node, " + function" if with_function else ", no function")
    return {
        "function": with_function,
        "mode": pool._mode_cache.get(node, ""),
        "insert_sql": INSERT_TRACK_ROW_TEMPLATE.format(schema=schema),
    }


def teardown_tracking(pool: DatabasePool, node: str, schema: str = "public"):
    """Drop tracking table and stored function."""
    pool.execute_ddl(node, DROP_TRACK_FUNCTION.format(schema=schema))
    pool.execute_ddl(node, "DROP TABLE IF EXISTS %s._app_db_tracking" % schema)
    logger.info("Tracking infrastructure dropped from node '%s'", node)


def verify_batch(pool: DatabasePool, node: str,
                 batch_id: str = None, schema: str = "public",
                 mark_verified: bool = True) -> dict:
    """Verify tracked operations against actual database state.

    Reads _app_db_tracking records, checks each against the actual table.

    Args:
        pool: DatabasePool
        node: Node name
        batch_id: Specific batch ID to verify, or None = all unverified
        schema: Database schema
        mark_verified: If True, update tracking table verified=true for passed checks

    Returns dict with: total, verified, missing, mismatch, errors, success, errors list.
    """
    logger.info("Verifying app-db consistency for batch '%s' on node '%s'",
                batch_id or "ALL", node)

    # A batch id that was never recorded (a typo, or JMeter never ran) must
    # not report PASS: "nothing to check" and "everything checked out" are
    # different answers, and only the second deserves a green light.
    if batch_id:
        try:
            found = pool.execute(
                node,
                "SELECT COUNT(*) FROM %s WHERE batch_id = %%s"
                % qualified(schema, "_app_db_tracking"),
                (batch_id,),
            )
        except Exception as e:
            return {
                "total": 0, "verified": 0, "missing": 0, "mismatch": 0,
                "errors_count": 0, "success": False, "errors": [],
                "torn_transactions": [],
                "error": "无法读取追踪表: %s" % e,
            }
        if not found or not found[0][0]:
            return {
                "total": 0, "verified": 0, "missing": 0, "mismatch": 0,
                "errors_count": 0, "success": False, "errors": [],
                "torn_transactions": [],
                "error": ("批次 '%s' 在节点 %s 上不存在（追踪表中没有该批次的任何记录）。"
                          "请确认 batch_id 是否正确，或确认 JMeter 是否已写入追踪记录。"
                          % (batch_id, node)),
            }

    if batch_id:
        records = pool.execute(
            node,
            "SELECT id, batch_id, txn_id, request_id, operation, table_name, "
            "pk_values, row_data "
            "FROM %s._app_db_tracking "
            "WHERE batch_id = %%s AND NOT verified "
            "ORDER BY id" % schema,
            (batch_id,),
        )
    else:
        records = pool.execute(
            node,
            "SELECT id, batch_id, txn_id, request_id, operation, table_name, "
            "pk_values, row_data "
            "FROM %s._app_db_tracking "
            "WHERE NOT verified "
            "ORDER BY id" % schema,
        )

    total = len(records)
    if total == 0:
        logger.info("No unverified records found")
        return {
            "total": 0, "verified": 0, "missing": 0,
            "mismatch": 0, "errors_count": 0,
            "success": True, "errors": [],
            "torn_transactions": [],
        }

    last_ids = _build_last_record_map(
        pool, node, schema, {r.get("batch_id") for r in records if r.get("batch_id")})

    verified = 0
    superseded = 0
    missing = 0
    mismatch = 0
    errors = []
    ok_ids = []
    # id -> (verified flag, result text) for one batched write-back.
    outcomes = []
    type_cache = {}

    for rec in records:
        try:
            # An older record for the same key has been overtaken; its value
            # snapshot no longer describes the row. Count it as superseded
            # rather than comparing stale values against the current row.
            is_last = last_ids.get(_record_key(rec), rec["id"]) == rec["id"]
            if not is_last:
                superseded += 1
                outcomes.append((rec["id"], True, "superseded by a later record"))
                continue

            result = _verify_one_record(pool, node, schema, rec, type_cache)
            status = result["status"]
            if status == "ok":
                verified += 1
                ok_ids.append(rec["id"])
                outcomes.append((rec["id"], True, "ok"))
            else:
                if status == "missing":
                    missing += 1
                elif status == "mismatch":
                    mismatch += 1
                errors.append(result)
                outcomes.append((rec["id"], False, (result.get("detail") or status)[:500]))
        except Exception as e:
            logger.error("Verify record %d failed: %s", rec["id"], e)
            errors.append({"id": rec["id"], "status": "error", "detail": str(e)})
            outcomes.append((rec["id"], False, ("error: %s" % e)[:500]))

    if mark_verified:
        _write_back(pool, node, schema, outcomes)

    torn = _find_torn_transactions(pool, node, schema, batch_id, outcomes)

    return {
        "total": total,
        "verified": verified,
        "superseded": superseded,
        "missing": missing,
        "mismatch": mismatch,
        "errors_count": len(errors),
        "success": len(errors) == 0 and not torn,
        "errors": errors[:50],
        "torn_transactions": torn,
    }


def _build_last_record_map(pool, node, schema, batch_ids):
    """{(batch_id, table, pk_json): newest_record_id} over the given batches.

    A key can be written more than once in a run -- an INSERT followed by an
    UPDATE, or an update and a later delete. Only the newest record describes
    the state the row should be in now; comparing an older record's snapshot
    against the current row reports a mismatch for a perfectly legal update.
    """
    if not batch_ids:
        return {}
    # `IN (...)` rather than `= ANY(array)`: the sqlserver compatibility
    # mode does not accept the ARRAY constructor that psycopg2 emits for a
    # Python list.
    ids = list(batch_ids)
    placeholders = ", ".join(["%s"] * len(ids))
    sql = ("SELECT id, batch_id, table_name, pk_values FROM "
           + qualified(schema, "_app_db_tracking")
           + " WHERE batch_id IN (" + placeholders + ") ORDER BY id")
    rows = pool.execute(node, sql, tuple(ids))
    last = {}
    for r in rows:
        pk = r["pk_values"]
        if isinstance(pk, str):
            try:
                pk = json.loads(pk)
            except Exception:
                continue
        key = (r["batch_id"], r["table_name"], json.dumps(pk, sort_keys=True))
        last[key] = r["id"]          # rows are ordered by id, so last wins
    return last


def _record_key(rec):
    pk = rec["pk_values"]
    if isinstance(pk, str):
        try:
            pk = json.loads(pk)
        except Exception:
            pk = {}
    return (rec.get("batch_id"), rec["table_name"],
            json.dumps(pk, sort_keys=True))


def _write_back(pool, node, schema, outcomes):
    """Persist verification outcomes in batches.

    Was one UPDATE per record, which dominates the run time once a tracking
    table holds tens of thousands of rows.
    """
    ok_ids = [oid for oid, ok, _ in outcomes if ok]
    failed = [(oid, txt) for oid, ok, txt in outcomes if not ok]
    try:
        if ok_ids:
            # Same reason as above: no ANY(array) on sqlserver-mode nodes.
            placeholders = ", ".join(["%s"] * len(ok_ids))
            sql = ("UPDATE " + qualified(schema, "_app_db_tracking")
                   + " SET verified = TRUE, verify_result = 'ok'"
                   + " WHERE id IN (" + placeholders + ")")
            pool.execute(node, sql, tuple(ok_ids), fetch=False)
        for oid, txt in failed:
            pool.execute(
                node,
                "UPDATE %s._app_db_tracking SET verify_result = %%s "
                "WHERE id = %%s" % schema,
                (txt, oid),
                fetch=False,
            )
    except Exception as e:
        logger.warning("写入校验结果失败（不影响本次结论）: %s", e)


def _find_torn_transactions(pool, node, schema, batch_id, outcomes):
    """Transactions whose writes only partly reached the database.

    A business transaction that inserts an order and updates a balance must
    leave both changes or neither. If one landed and the other did not, the
    database committed a partial transaction -- something a per-row check
    cannot see, because each row on its own looks fine or plainly missing.

    Grouping uses the WHOLE batch, not just the records verified in this
    run. Only unverified rows are re-checked (that is what makes repeated
    verification cheap), so after a first pass marked the successful halves
    as verified, a group built from the remaining rows would contain only
    the failures -- every transaction would look uniformly failed and the
    tear would vanish. Rows verified in an earlier run are taken as passed.

    Only possible when the client attached a txn_id; without one there is
    nothing to group by.
    """
    verdict = {oid: ok for oid, ok, _ in outcomes}
    try:
        if batch_id:
            rows = pool.execute(
                node,
                "SELECT id, txn_id, verified FROM %s "
                "WHERE batch_id = %%s AND txn_id IS NOT NULL"
                % qualified(schema, "_app_db_tracking"),
                (batch_id,))
        else:
            rows = pool.execute(
                node,
                "SELECT id, txn_id, verified FROM %s WHERE txn_id IS NOT NULL"
                % qualified(schema, "_app_db_tracking"))
    except Exception as e:
        logger.debug("torn-transaction query skipped: %s", e)
        return []

    groups = {}
    for r in rows:
        oid = r["id"]
        ok = verdict[oid] if oid in verdict else bool(r["verified"])
        groups.setdefault(r["txn_id"], []).append(ok)

    torn = []
    for txn, results in groups.items():
        if len(results) < 2:
            continue
        if all(results) or not any(results):
            continue
        torn.append({
            "txn_id": txn,
            "total_ops": len(results),
            "ok_ops": sum(1 for r in results if r),
            "failed_ops": sum(1 for r in results if not r),
            "detail": "同一事务的部分写操作已落库、部分没有 —— 事务被撕裂",
        })
    torn.sort(key=lambda d: d["txn_id"])
    return torn


def _verify_one_record(pool, node, schema, record, type_cache=None) -> dict:
    """Verify a single tracking record against the database."""
    op = record["operation"]
    tbl = record["table_name"]
    pk = record["pk_values"]
    if isinstance(pk, str):
        pk = json.loads(pk)
    expected = record["row_data"]
    if isinstance(expected, str):
        expected = json.loads(expected)

    where = " AND ".join("%s = %%s" % _quote_ident(k) for k in pk.keys())
    values = list(pk.values())
    target = qualified(schema, tbl)

    if op == "DELETE":
        rows = pool.execute(node, "SELECT COUNT(*) FROM %s WHERE %s" % (target, where), values)
        if rows[0][0] > 0:
            return {
                "id": record["id"], "status": "mismatch",
                "operation": op, "table": tbl, "pk": pk,
                "detail": "Row should have been deleted but still exists",
            }
        return {
            "id": record["id"], "status": "ok",
            "operation": op, "table": tbl, "pk": pk,
        }

    # INSERT or UPDATE
    rows = pool.execute(node, "SELECT * FROM %s WHERE %s" % (target, where), values)

    if not rows:
        return {
            "id": record["id"], "status": "missing",
            "operation": op, "table": tbl, "pk": pk,
            "detail": "Row not found in database — data likely lost",
        }

    if expected:
        actual = dict(rows[0])
        if type_cache is not None:
            if tbl not in type_cache:
                try:
                    type_cache[tbl] = {
                        c["name"]: c["type"]
                        for c in pool.get_column_info(node, schema, tbl)}
                except Exception:
                    type_cache[tbl] = {}
            types = type_cache[tbl]
        else:
            types = {}
        diffs = compare_rows(expected, actual, types)
        if diffs:
            return {
                "id": record["id"], "status": "mismatch",
                "operation": op, "table": tbl, "pk": pk,
                "mismatches": diffs,
                "detail": "列值不一致: %s" % "; ".join(diffs),
            }

    return {
        "id": record["id"], "status": "ok",
        "operation": op, "table": tbl, "pk": pk,
    }


def get_batch_summary(pool: DatabasePool, node: str,
                      schema: str = "public") -> list:
    """List all batches and their status."""
    rows = pool.execute(
        node,
        "SELECT batch_id, operation, table_name, "
        "COUNT(*) AS total, "
        "SUM(CASE WHEN verified THEN 1 ELSE 0 END) AS verified_count, "
        "MIN(created_at) AS first_op, MAX(created_at) AS last_op "
        "FROM %s._app_db_tracking "
        "GROUP BY batch_id, operation, table_name "
        "ORDER BY batch_id, table_name" % schema,
    )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Python API mode — for custom Python automation scripts
# ---------------------------------------------------------------------------

class AppDBTracker:
    """Programmatic API: record writes and verify in Python scripts.

    Usage:
        setup_tracking(pool, "node1")
        tracker = AppDBTracker(pool, "node1")
        tracker.start_batch()
        tracker.record_insert("accounts", {"id": 1, "balance": 100})
        tracker.record_update("accounts", {"id": 1},
                              new_row={"id": 1, "balance": 200})
        results = tracker.verify_all()
    """

    def __init__(self, pool: DatabasePool, node: str, schema: str = "public"):
        self.pool = pool
        self.node = node
        self.schema = schema
        self.batch_id = None

    def start_batch(self, batch_id: str = None):
        self.batch_id = batch_id or uuid.uuid4().hex[:16]
        logger.info("Started tracking batch: %s", self.batch_id)
        return self.batch_id

    def record_insert(self, table_name: str, full_row: dict,
                      pk_values: dict = None):
        if not pk_values:
            pk_values = _extract_pk(self.pool, self.node, self.schema,
                                    table_name, full_row)
        self._save("INSERT", table_name, pk_values, full_row)

    def record_update(self, table_name: str, new_row: dict,
                      pk_values: dict = None):
        if not pk_values:
            pk_values = _extract_pk(self.pool, self.node, self.schema,
                                    table_name, new_row)
        self._save("UPDATE", table_name, pk_values, new_row)

    def record_delete(self, table_name: str, pk_values: dict):
        self._save("DELETE", table_name, pk_values, None)

    def _save(self, operation: str, table_name: str,
              pk_values: dict, row_data: dict):
        if not self.batch_id:
            raise RuntimeError("Must call start_batch() first")
        self.pool.execute(
            self.node,
            "INSERT INTO %s._app_db_tracking "
            "(batch_id, operation, table_name, pk_values, row_data) "
            "VALUES (%%s, %%s, %%s, %%s, %%s)" % self.schema,
            (self.batch_id, operation, table_name,
             json.dumps(pk_values), json.dumps(row_data) if row_data else None),
            fetch=False,
        )

    def verify_all(self) -> dict:
        if not self.batch_id:
            raise RuntimeError("Must call start_batch() first")
        return verify_batch(self.pool, self.node, self.batch_id, self.schema)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


def _extract_pk(pool, node, schema, table, row: dict) -> dict:
    pk_cols = pool.get_primary_key(node, schema, table)
    return {c: row.get(c) for c in pk_cols}


def _diff_rows(expected: dict, actual: dict, types: dict = None) -> list:
    """Deprecated shim kept for callers outside this module.

    The previous implementation compared with str(), which reported
    Decimal('100.00') vs Decimal('100.0') and True vs 1 as mismatches. The
    comparison now goes through utils.normalize.compare_rows.
    """
    return compare_rows(expected, actual, types)
