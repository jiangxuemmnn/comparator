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

import logging
import json
import uuid
from datetime import datetime

from utils.db import DatabasePool

logger = logging.getLogger("comparator.appdb")

CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS {schema}._app_db_tracking (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    pk_values JSONB NOT NULL,
    row_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified BOOLEAN DEFAULT FALSE,
    verify_result TEXT
);
"""

# Stored function that JMeter calls via SELECT after each business write.
# JMeter JDBC PostProcessor example:
#   SELECT {schema}.sp_track_write('batch1', 'INSERT', 'accounts',
#                                  '{"id":123}', '{"id":123,"balance":100}')
CREATE_TRACK_FUNCTION = """
CREATE OR REPLACE FUNCTION {schema}.sp_track_write(
    p_batch_id VARCHAR,
    p_operation VARCHAR,
    p_table_name VARCHAR,
    p_pk_values VARCHAR,
    p_row_data VARCHAR DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO {schema}._app_db_tracking
        (batch_id, operation, table_name, pk_values, row_data)
    VALUES (p_batch_id, p_operation, p_table_name,
            p_pk_values::jsonb, p_row_data::jsonb)
    RETURNING id INTO v_id;
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;
"""

DROP_TRACK_FUNCTION = """
DROP FUNCTION IF EXISTS {schema}.sp_track_write(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
"""


def setup_tracking(pool: DatabasePool, node: str, schema: str = "public"):
    """Create tracking table + stored function on the target node.

    After this, JMeter can call sp_track_write() to record operations.
    """
    pool.execute_ddl(node, CREATE_TRACKING_TABLE.format(schema=schema))
    pool.execute_ddl(node, CREATE_TRACK_FUNCTION.format(schema=schema))
    logger.info("Tracking infrastructure created on node '%s' (table + function)", node)


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
            "SELECT id, operation, table_name, pk_values, row_data "
            "FROM %s._app_db_tracking "
            "WHERE batch_id = %%s AND NOT verified "
            "ORDER BY id" % schema,
            (batch_id,),
        )
    else:
        records = pool.execute(
            node,
            "SELECT id, operation, table_name, pk_values, row_data "
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
        }

    verified = 0
    missing = 0
    mismatch = 0
    errors = []

    for rec in records:
        try:
            result = _verify_one_record(pool, node, schema, rec)
            if result["status"] == "ok":
                verified += 1
                if mark_verified:
                    pool.execute(
                        node,
                        "UPDATE %s._app_db_tracking SET verified = TRUE "
                        "WHERE id = %%s" % schema,
                        (rec["id"],),
                        fetch=False,
                    )
            elif result["status"] == "missing":
                missing += 1
                errors.append(result)
            elif result["status"] == "mismatch":
                mismatch += 1
                errors.append(result)
        except Exception as e:
            logger.error("Verify record %d failed: %s", rec["id"], e)
            errors.append({
                "id": rec["id"], "status": "error",
                "detail": str(e),
            })

    return {
        "total": total,
        "verified": verified,
        "missing": missing,
        "mismatch": mismatch,
        "errors_count": len(errors),
        "success": len(errors) == 0,
        "errors": errors[:50],
    }


def _verify_one_record(pool, node, schema, record) -> dict:
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

    if op == "DELETE":
        rows = pool.execute(
            node,
            "SELECT COUNT(*) FROM %s.%s WHERE %s" % (
                schema, _quote_ident(tbl), where
            ),
            values,
        )
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
    rows = pool.execute(
        node,
        "SELECT * FROM %s.%s WHERE %s" % (
            schema, _quote_ident(tbl), where
        ),
        values,
    )

    if not rows:
        return {
            "id": record["id"], "status": "missing",
            "operation": op, "table": tbl, "pk": pk,
            "detail": "Row not found in database — data likely lost",
        }

    if expected:
        actual = dict(rows[0])
        diffs = _diff_rows(expected, actual)
        if diffs:
            return {
                "id": record["id"], "status": "mismatch",
                "operation": op, "table": tbl, "pk": pk,
                "mismatches": diffs,
                "detail": "Column value mismatch: %s" % diffs,
            }

    return {
        "id": record["id"], "status": "ok",
        "operation": op, "table": tbl, "pk": pk,
    }


def get_batch_summary(pool: DatabasePool, node: str,
                      schema: str = "public") -> dict:
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


def _diff_rows(expected: dict, actual: dict) -> list[str]:
    diffs = []
    for key in expected:
        if key not in actual:
            diffs.append("%s: missing in actual" % key)
        elif str(expected[key]) != str(actual[key]):
            diffs.append("%s: expected=%s, actual=%s" % (
                key, expected[key], actual[key]
            ))
    return diffs
