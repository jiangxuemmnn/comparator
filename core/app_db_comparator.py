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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified BOOLEAN DEFAULT FALSE,
    verify_result VARCHAR(512)
);
"""

# Upgrades for tracking tables created by an earlier version. Added
# separately because CREATE TABLE IF NOT EXISTS leaves an existing table
# untouched, so a run against an old schema would otherwise fail on the
# missing columns much later, with a confusing error.
MIGRATE_TRACKING_TABLE = [
    "ALTER TABLE {schema}._app_db_tracking ADD COLUMN IF NOT EXISTS txn_id VARCHAR(64)",
    "ALTER TABLE {schema}._app_db_tracking ADD COLUMN IF NOT EXISTS request_id VARCHAR(128)",
    "ALTER TABLE {schema}._app_db_tracking ALTER COLUMN verify_result TYPE VARCHAR(512)",
]

# Stored function that JMeter calls after each business write.
#
# txn_id groups several writes that belong to one business transaction, so a
# transaction that only partly landed can be detected. request_id identifies
# one client request, so a failed JMeter sample can be matched against what
# actually reached the database.
#
# JMeter JDBC PostProcessor example:
#   SELECT {schema}.sp_track_write('batch1', 'INSERT', 'accounts',
#                                  '{"id":123}', '{"id":123,"balance":100}')
CREATE_TRACK_FUNCTION = """
CREATE OR REPLACE FUNCTION {schema}.sp_track_write(
    p_batch_id VARCHAR,
    p_operation VARCHAR,
    p_table_name VARCHAR,
    p_pk_values VARCHAR,
    p_row_data VARCHAR DEFAULT NULL,
    p_txn_id VARCHAR DEFAULT NULL,
    p_request_id VARCHAR DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO {schema}._app_db_tracking
        (batch_id, txn_id, request_id, operation, table_name, pk_values, row_data)
    VALUES (p_batch_id, p_txn_id, p_request_id, p_operation, p_table_name,
            p_pk_values::jsonb, p_row_data::jsonb)
    RETURNING id INTO v_id;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;
"""

# All historical signatures, so a table created by an older release can be
# torn down cleanly.
DROP_TRACK_FUNCTION = """
DROP FUNCTION IF EXISTS {schema}.sp_track_write(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
DROP FUNCTION IF EXISTS {schema}.sp_track_write(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
"""


def setup_tracking(pool: DatabasePool, node: str, schema: str = "public",
                   with_function: bool = True):
    """Create the tracking table (and stored function) on the target node."""
    pool.execute_ddl(node, CREATE_TRACKING_TABLE.format(schema=schema))
    for stmt in MIGRATE_TRACKING_TABLE:
        try:
            pool.execute_ddl(node, stmt.format(schema=schema))
        except Exception as e:
            logger.warning("Tracking table migration skipped: %s", e)
    if with_function:
        pool.execute_ddl(node, CREATE_TRACK_FUNCTION.format(schema=schema))
    logger.info("Tracking infrastructure created on node '%s' (table%s)",
                node, " + function" if with_function else "")


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

    verified = 0
    missing = 0
    mismatch = 0
    errors = []
    ok_ids = []
    # id -> (verified flag, result text) for one batched write-back.
    outcomes = []
    type_cache = {}

    for rec in records:
        try:
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

    torn = _find_torn_transactions(records, outcomes)

    return {
        "total": total,
        "verified": verified,
        "missing": missing,
        "mismatch": mismatch,
        "errors_count": len(errors),
        "success": len(errors) == 0 and not torn,
        "errors": errors[:50],
        "torn_transactions": torn,
    }


def _write_back(pool, node, schema, outcomes):
    """Persist verification outcomes in batches.

    Was one UPDATE per record, which dominates the run time once a tracking
    table holds tens of thousands of rows.
    """
    ok_ids = [oid for oid, ok, _ in outcomes if ok]
    failed = [(oid, txt) for oid, ok, txt in outcomes if not ok]
    try:
        if ok_ids:
            pool.execute(
                node,
                "UPDATE %s._app_db_tracking SET verified = TRUE, "
                "verify_result = 'ok' WHERE id = ANY(%%s)" % schema,
                (ok_ids,),
                fetch=False,
            )
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


def _find_torn_transactions(records, outcomes):
    """Transactions whose writes only partly reached the database.

    A business transaction that inserts an order and updates a balance must
    leave both changes or neither. If one landed and the other did not, the
    database committed a partial transaction -- something a per-row check
    cannot see, because each row on its own looks fine or plainly missing.

    Only possible when the client attached a txn_id; without one there is
    nothing to group by.
    """
    verdict = {oid: ok for oid, ok, _ in outcomes}
    groups = {}
    for rec in records:
        txn = rec.get("txn_id")
        if not txn:
            continue
        groups.setdefault(txn, []).append(rec["id"])

    torn = []
    for txn, ids in groups.items():
        if len(ids) < 2:
            continue
        results = [verdict.get(i) for i in ids]
        if all(r is True for r in results) or all(r is False for r in results):
            continue
        torn.append({
            "txn_id": txn,
            "total_ops": len(ids),
            "ok_ops": sum(1 for r in results if r is True),
            "failed_ops": sum(1 for r in results if r is not True),
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
