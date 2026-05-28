"""RPO (Recovery Point Objective) data loss detection.

Detects data loss in fault/failover scenarios by:
1. Inserting marker records before a fault event
2. After recovery, verifying those markers exist
3. Comparing row counts before and after the event
4. Checking for gaps in sequential data (e.g., missing serial IDs)

Typical workflow:
  rpo = RPOChecker(pool, "node1")
  rpo.plant_markers()          # Plant markers before fault
  # --- User triggers fault/failover ---
  rpo.check_markers()          # After recovery, verify markers
  rpo.check_row_counts()       # Compare counts
  rpo.check_sequence_gaps()    # Look for gaps in sequential columns
"""

from __future__ import annotations

import logging
import time
import uuid
import json
from datetime import datetime

from utils.db import DatabasePool

logger = logging.getLogger("comparator.rpo")


def _quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')

CREATE_MARKER_TABLE = """
CREATE TABLE IF NOT EXISTS {schema}._rpo_markers (
    id SERIAL PRIMARY KEY,
    marker_id VARCHAR(64) NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(256),
    phase VARCHAR(32) NOT NULL,  -- 'before_fault', 'after_fault'
    extra JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SNAPSHOT_TABLE = """
CREATE TABLE IF NOT EXISTS {schema}._rpo_snapshots (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    row_count BIGINT NOT NULL,
    max_id BIGINT,
    snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class RPOChecker:
    """RPO data loss detection utility.

    Usage:
        rpo = RPOChecker(pool, "node1", schema="public")

        # Phase 1: Before fault
        batch_id = rpo.plant_markers(tables=["accounts", "orders"])

        # --- User triggers fault/failover ---

        # Phase 2: After recovery
        results = rpo.check_all()
        print(results)
    """

    def __init__(self, pool: DatabasePool, node: str, schema: str = "public"):
        self.pool = pool
        self.node = node
        self.schema = schema
        self._ensure_tables()

    def _ensure_tables(self):
        """Create RPO tracking tables if not exist."""
        pool = self.pool
        pool.execute_ddl(self.node, CREATE_MARKER_TABLE.format(schema=self.schema))
        pool.execute_ddl(self.node, CREATE_SNAPSHOT_TABLE.format(schema=self.schema))

    def plant_markers(self, tables: list[str] = None,
                      marker_count: int = 10) -> str:
        """Plant marker records before a fault event.

        Records row counts and inserts visible markers on the target node.

        Args:
            tables: Tables to snapshot (None = all user tables)
            marker_count: Number of marker rows to insert per table

        Returns:
            batch_id for later verification
        """
        batch_id = uuid.uuid4().hex[:16]

        if tables is None:
            tables = self.pool.get_tables(self.node, self.schema)

        for table in tables:
            # Snapshot row count
            count = self.pool.get_row_count(self.node, self.schema, table)
            self.pool.execute(
                self.node,
                "INSERT INTO %s._rpo_snapshots (batch_id, table_name, row_count) "
                "VALUES (%%s, %%s, %%s)" % self.schema,
                (batch_id, table, count),
                fetch=False,
            )

            # Try to get max ID for sequential gap detection
            pk_cols = self.pool.get_primary_key(self.node, self.schema, table)
            max_id = None
            if pk_cols and len(pk_cols) == 1:
                pk = pk_cols[0]
                try:
                    rows = self.pool.execute(
                        self.node,
                        "SELECT MAX(%s) FROM %s.%s" % (
                            _quote_ident(pk), _quote_ident(self.schema), _quote_ident(table)),
                    )
                    max_id = rows[0][0] if rows else None
                except Exception:
                    pass

            # Update max_id in snapshot
            if max_id is not None:
                self.pool.execute(
                    self.node,
                    "UPDATE %s._rpo_snapshots SET max_id = %%s "
                    "WHERE batch_id = %%s AND table_name = %%s" % self.schema,
                    (max_id, batch_id, table),
                    fetch=False,
                )

            # Insert marker rows
            for i in range(marker_count):
                marker_id = "%s_%s_%d" % (batch_id, table, i)
                self.pool.execute(
                    self.node,
                    "INSERT INTO %s._rpo_markers "
                    "(marker_id, batch_id, table_name, phase, extra) "
                    "VALUES (%%s, %%s, %%s, 'before_fault', %%s)" % self.schema,
                    (
                        marker_id, batch_id, table,
                        json.dumps({
                            "seq": i,
                            "timestamp": datetime.now().isoformat(),
                        }),
                    ),
                    fetch=False,
                )

        logger.info("Planted %d markers per table, batch=%s on node '%s'",
                    marker_count, batch_id, self.node)
        return batch_id

    def check_markers(self, batch_id: str) -> dict:
        """Verify that markers planted before fault still exist after recovery.

        Returns dict with: total, found, missing, missing_details.
        """
        markers = self.pool.execute(
            self.node,
            "SELECT marker_id, table_name FROM %s._rpo_markers "
            "WHERE batch_id = %%s AND phase = 'before_fault'" % self.schema,
            (batch_id,),
        )

        total = len(markers)
        found = 0
        missing = []

        for m in markers:
            table = m["table_name"]
            marker_id = m["marker_id"]

            rows = self.pool.execute(
                self.node,
                "SELECT COUNT(*) FROM %s._rpo_markers "
                "WHERE marker_id = %%s" % self.schema,
                (marker_id,),
            )
            if rows and rows[0][0] > 0:
                found += 1
            else:
                missing.append({
                    "marker_id": marker_id,
                    "table": table,
                    "detail": "Marker lost after fault",
                })

        logger.info("Marker check: %d/%d found, %d missing (batch=%s)",
                    found, total, len(missing), batch_id)

        return {
            "batch_id": batch_id,
            "total": total,
            "found": found,
            "missing": len(missing),
            "missing_details": missing,
            "success": len(missing) == 0,
        }

    def check_row_counts(self, batch_id: str) -> dict:
        """Compare current row counts with pre-fault snapshots.

        Returns dict with per-table before/after counts and diffs.
        """
        snapshots = self.pool.execute(
            self.node,
            "SELECT table_name, row_count, max_id "
            "FROM %s._rpo_snapshots "
            "WHERE batch_id = %%s "
            "ORDER BY table_name" % self.schema,
            (batch_id,),
        )

        results = []
        total_before = 0
        total_after = 0
        tables_with_loss = []

        for snap in snapshots:
            table = snap["table_name"]
            before = snap["row_count"]
            after = self.pool.get_row_count(self.node, self.schema, table)
            delta = after - before

            total_before += before
            total_after += after

            entry = {
                "table": table,
                "before_count": before,
                "after_count": after,
                "delta": delta,
            }

            if delta < 0:
                tables_with_loss.append(table)
                entry["status"] = "DATA_LOSS"
                logger.warning("Table %s: %d rows lost (before=%d, after=%d)",
                               table, -delta, before, after)
            elif delta > 0:
                entry["status"] = "DATA_GAIN"
            else:
                entry["status"] = "ok"

            results.append(entry)

        return {
            "batch_id": batch_id,
            "total_before": total_before,
            "total_after": total_after,
            "total_delta": total_after - total_before,
            "tables_with_loss": tables_with_loss,
            "tables_checked": len(results),
            "success": len(tables_with_loss) == 0,
            "details": results,
        }

    def check_sequence_gaps(self, batch_id: str,
                            min_gap_size: int = 1) -> dict:
        """Check for gaps in sequential PK columns after recovery.

        Compares current MAX(id) against pre-fault max_id.
        A gap indicates data was written before fault but missing after.
        """
        snapshots = self.pool.execute(
            self.node,
            "SELECT table_name, max_id FROM %s._rpo_snapshots "
            "WHERE batch_id = %%s AND max_id IS NOT NULL" % self.schema,
            (batch_id,),
        )

        gaps_found = []
        results = []

        for snap in snapshots:
            table = snap["table_name"]
            old_max = snap["max_id"]

            pk_cols = self.pool.get_primary_key(self.node, self.schema, table)
            if not pk_cols or len(pk_cols) != 1:
                continue

            pk = pk_cols[0]

            # Check current max
            rows = self.pool.execute(
                self.node,
                "SELECT MAX(%s) FROM %s.%s" % (
                    _quote_ident(pk), _quote_ident(self.schema), _quote_ident(table)),
            )
            new_max = rows[0][0] if rows else None

            # Check for missing IDs between old_min and old_max
            # This detects: rows inserted before fault, committed, but lost after
            cols_info = self.pool.get_column_info(self.node, self.schema, table)
            col_types = {c["name"]: c["type"] for c in cols_info}
            col_type = col_types.get(pk, "").lower()

            # Only check integer-type PKs
            if any(t in col_type for t in ("int", "serial", "numeric")):
                # Use generate_series to find gaps
                q_pk = _quote_ident(pk)
                q_schema = _quote_ident(self.schema)
                q_table = _quote_ident(table)
                gap_sql = (
                    "SELECT s.i AS missing_id FROM generate_series(1, "
                    "(SELECT MAX(%s) FROM %s.%s)) AS s(i) "
                    "LEFT JOIN %s.%s t ON t.%s = s.i "
                    "WHERE t.%s IS NULL AND s.i <= %%s "
                    "ORDER BY s.i LIMIT 100"
                ) % (q_pk, q_schema, q_table, q_schema, q_table, q_pk, q_pk)

                gaps = self.pool.execute(self.node, gap_sql, (old_max,))

                if gaps:
                    gap_ids = [g[0] for g in gaps]
                    gaps_found.append({
                        "table": table,
                        "old_max": old_max,
                        "new_max": new_max,
                        "missing_ids": gap_ids[:20],
                        "missing_count": len(gap_ids),
                    })

            entry = {
                "table": table,
                "pk_column": pk,
                "old_max": old_max,
                "new_max": new_max,
            }
            results.append(entry)

        return {
            "batch_id": batch_id,
            "tables_checked": len(results),
            "gaps_found": len(gaps_found),
            "success": len(gaps_found) == 0,
            "details": results,
            "gaps": gaps_found,
        }

    def check_all(self, batch_id: str) -> dict:
        """Run all RPO checks: markers + row counts + sequence gaps.

        Returns comprehensive RPO assessment.
        """
        logger.info("Running full RPO check (batch=%s) ...", batch_id)

        markers = self.check_markers(batch_id)
        counts = self.check_row_counts(batch_id)
        gaps = self.check_sequence_gaps(batch_id)

        data_loss = (
            not markers["success"] or
            not counts["success"] or
            not gaps["success"]
        )

        return {
            "batch_id": batch_id,
            "node": self.node,
            "rpo_success": not data_loss,
            "data_loss_detected": data_loss,
            "markers": markers,
            "row_counts": counts,
            "sequence_gaps": gaps,
            "summary": {
                "markers_lost": markers["missing"],
                "rows_lost": -counts["total_delta"] if counts["total_delta"] < 0 else 0,
                "tables_with_gaps": [g["table"] for g in gaps.get("gaps", [])],
            },
        }

    def teardown(self):
        """Drop RPO tracking tables."""
        self.pool.execute_ddl(
            self.node,
            "DROP TABLE IF EXISTS %s._rpo_markers" % self.schema,
        )
        self.pool.execute_ddl(
            self.node,
            "DROP TABLE IF EXISTS %s._rpo_snapshots" % self.schema,
        )
