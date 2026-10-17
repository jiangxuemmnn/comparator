"""RPO (Recovery Point Objective) data loss detection.

Detects data loss in fault/failover scenarios by:
1. Snapshotting per-table row counts and maximum keys before a fault
2. Planting marker rows that ride the same replication stream
3. After recovery, verifying the markers still exist
4. Comparing row counts and maximum keys against the snapshots

Typical workflow:
  rpo = RPOChecker(pool, "node1")
  rpo.plant_markers()          # Snapshot state + plant markers, before fault
  # --- User triggers fault/failover ---
  rpo.check_all(batch_id)      # After recovery: markers + conservation
"""

from __future__ import annotations

import logging
import time
import uuid
import json
from datetime import datetime

from utils.db import DatabasePool
from utils.scope import build_matcher

logger = logging.getLogger("comparator.rpo")

# Tables the tool itself owns. They grow while a check runs -- _rpo_snapshots
# gains a row per table, _rpo_markers gains one per marker -- so snapshotting
# them would report a made-up delta and bury the real signal. Excluded
# unconditionally, independent of configuration.
TOOL_TABLES = ("_rpo_markers", "_rpo_snapshots", "_app_db_tracking")


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
    created_at {ts} DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SNAPSHOT_TABLE = """
CREATE TABLE IF NOT EXISTS {schema}._rpo_snapshots (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    row_count BIGINT NOT NULL,
    max_id BIGINT,
    snapshot_at {ts} DEFAULT CURRENT_TIMESTAMP
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

    def __init__(self, pool: DatabasePool, node: str, schema: str = "public",
                 config: dict = None):
        self.pool = pool
        self.node = node
        self.schema = schema
        self.matcher = build_matcher(config or {}, node=node) if config is not None else None
        self._ensure_tables()

    def _select_tables(self, tables):
        """Resolve the table list to snapshot, honouring scope and tool tables."""
        if tables is None:
            tables = self.pool.get_tables(self.node, self.schema)
            if self.matcher is not None:
                dbname = self.pool.get_database_name(self.node)
                tables = self.matcher.filter_tables(dbname, self.schema, tables)
        return [t for t in tables if t not in TOOL_TABLES]

    def _ensure_tables(self):
        """Create RPO tracking tables if not exist."""
        pool = self.pool
        pool.execute_ddl(self.node, CREATE_MARKER_TABLE.format(
            schema=self.schema, ts=pool.timestamp_type(self.node)))
        pool.execute_ddl(self.node, CREATE_SNAPSHOT_TABLE.format(
            schema=self.schema, ts=pool.timestamp_type(self.node)))

    def plant_markers(self, tables: list = None,
                      marker_count: int = 10) -> str:
        """Record the pre-fault state and plant markers. Returns batch_id.

        Two separate things are captured, and it matters which is which:

        * Per-table snapshots (row count + maximum key). These are what
          attribute any loss to a specific business table.

        * A handful of marker rows, appended to _rpo_markers. The marker
          INSERT travels the same replication stream as business writes, so
          whether the last markers survived says whether the stream itself
          carried the final writes before the fault. They are NOT written
          into the business tables -- doing so would pollute the data under
          test and collide with NOT NULL / FK constraints -- so a missing
          marker is evidence about replication, not about any particular
          business table. The previous version stamped each marker with a
          business table name it never actually touched, which made the
          report attribute loss to the wrong place.
        """
        batch_id = uuid.uuid4().hex[:16]
        tables = self._select_tables(tables)

        for table in tables:
            count = self.pool.get_row_count(self.node, self.schema, table)

            # MAX(pk) is stored in a BIGINT column (`max_id`), so it is only
            # meaningful for integer keys. A varchar or uuid key makes that
            # INSERT fail outright ("invalid input syntax for type bigint"),
            # which aborted the whole plant and left a half-written snapshot.
            # Such tables simply skip the max-key regression check; the row
            # count still covers them.
            max_id = None
            pk_cols = self.pool.get_primary_key(self.node, self.schema, table)
            if pk_cols and len(pk_cols) == 1:
                pk_type = ""
                try:
                    for col in self.pool.get_column_info(
                            self.node, self.schema, table):
                        if col["name"] == pk_cols[0]:
                            pk_type = (col.get("type") or "").lower()
                            break
                except Exception:
                    pass
                if any(t in pk_type for t in ("int", "serial", "number")):
                    try:
                        rows = self.pool.execute(
                            self.node,
                            "SELECT MAX(%s) FROM %s.%s" % (
                                _quote_ident(pk_cols[0]),
                                _quote_ident(self.schema),
                                _quote_ident(table)),
                        )
                        value = rows[0][0] if rows else None
                        # NUMERIC can carry a fraction; only a plain int can
                        # be stored as BIGINT without loss.
                        if isinstance(value, int) and not isinstance(value, bool):
                            max_id = value
                    except Exception as e:
                        logger.debug("MAX(%s) unavailable on %s.%s: %s",
                                     pk_cols[0], self.schema, table, e)

            self.pool.execute(
                self.node,
                "INSERT INTO %s._rpo_snapshots "
                "(batch_id, table_name, row_count, max_id) "
                "VALUES (%%s, %%s, %%s, %%s)" % self.schema,
                (batch_id, table, count, max_id),
                fetch=False,
            )

        # Markers are one batch-wide probe, not one per business table.
        #
        # Each marker records the batch's planted total, not just its own
        # sequence number. Without that, a check can only compare the
        # markers it finds against each other: deleting the last few would
        # simply lower the count and the loss would go unreported.
        now = datetime.now().isoformat()
        self.pool.execute(
            self.node,
            "INSERT INTO %s._rpo_markers "
            "(marker_id, batch_id, table_name, phase, extra) "
            # concat() rather than `||`: the sqlserver compatibility mode
            # does not accept `||` at all, and no single operator works in
            # all four modes (`+` fails in oracle/pg). Two-argument concat,
            # nested, is understood everywhere.
            "SELECT concat(concat(%%s, '_'), g), %%s, '(replication-probe)', 'before_fault', "
            "       json_build_object('seq', g, 'total', %%s, "
            "                         'timestamp', %%s)::jsonb "
            "FROM generate_series(1, %%s) g" % self.schema,
            (batch_id, batch_id, marker_count, now, marker_count),
            fetch=False,
        )

        logger.info("Planted %d replication-probe markers over %d tables, batch=%s on %s",
                    marker_count, len(tables), batch_id, self.node)
        return batch_id

    def check_markers(self, batch_id: str) -> dict:
        """Check whether the markers planted before the fault survived.

        A marker that is gone means writes committed on the primary just
        before the fault never reached this node. Because the markers live
        in one table, that verdict is about the replication stream as a
        whole; per-table loss is reported by check_conservation instead.
        """
        markers = self.pool.execute(
            self.node,
            "SELECT marker_id, extra FROM %s._rpo_markers "
            "WHERE batch_id = %%s AND phase = 'before_fault' "
            "ORDER BY marker_id" % self.schema,
            (batch_id,),
        )

        # The denominator is the number PLANTED, read back from the markers
        # that survived, not the number currently present. Taking it from
        # the survivors would make every deletion shrink the total and no
        # loss would ever be reported.
        present = {}
        planted_total = 0
        for m in markers:
            extra = m["extra"]
            if isinstance(extra, str):
                try:
                    extra = json.loads(extra)
                except Exception:
                    extra = {}
            extra = extra or {}
            seq = extra.get("seq")
            if seq is not None:
                present[seq] = m["marker_id"]
            planted_total = max(planted_total, extra.get("total") or 0)
        if not planted_total:
            # Legacy batch planted by an older version, or a batch whose
            # markers all vanished. Fall back to the surviving count.
            planted_total = len(present) or len(markers)

        seqs = sorted(set(range(1, planted_total + 1)) - set(present))
        missing = [{
            "marker_id": "%s_%d" % (batch_id, s),
            "seq": s,
            "detail": "故障前写入的标记在恢复后不存在 —— 复制流未送达最后的写入",
        } for s in seqs]

        logger.info("Marker check: %d/%d found, %d missing (batch=%s)",
                    planted_total - len(missing), planted_total,
                    len(missing), batch_id)

        return {
            "batch_id": batch_id,
            "total": planted_total,
            "found": planted_total - len(missing),
            "missing": len(missing),
            "missing_details": missing,
            "missing_seqs": seqs,
            "success": len(missing) == 0,
            "note": ("标记是整批次的复制流探针，不针对某张业务表；"
                     "逐表丢失情况见 conservation 检查"),
        }

    def check_conservation(self, batch_id: str) -> dict:
        """Detect loss by comparing row counts and maximum keys.

        This replaces the previous generate_series-based gap scan, which had
        two fatal flaws:

          * it reported every normal DELETE as data loss, and every sequence
            jump (nextval is not reclaimed on rollback) as data loss too, so
            false positives were guaranteed on any live system;
          * it materialised `generate_series(1, MAX(pk))` and left-joined it,
            so a table whose keys start at 100 million would build a row per
            integer in between before answering.

        A row count that went down, or a maximum key that went backwards,
        cannot happen through normal growth and is genuine evidence. Both
        checks are O(1) or index-only.

        One thing this deliberately does NOT do is distinguish "rows deleted
        by the application" from "rows lost in the fault". Nothing available
        server-side can tell those apart -- the only reliable discriminator
        is an application-side record of what was written, which is what the
        app-db tracking mode provides.
        """
        snapshots = self.pool.execute(
            self.node,
            "SELECT table_name, row_count, max_id FROM %s._rpo_snapshots "
            "WHERE batch_id = %%s ORDER BY table_name" % self.schema,
            (batch_id,),
        )

        losses = []
        results = []

        for snap in snapshots:
            table = snap["table_name"]
            if table in TOOL_TABLES:
                # A batch planted by an older version may have snapshotted
                # the tool's own tables; skipping them keeps their growth
                # out of the report.
                continue
            old_count = snap["row_count"]
            old_max = snap["max_id"]

            try:
                new_count = self.pool.get_row_count(self.node, self.schema, table)
            except Exception as e:
                results.append({"table": table, "error": str(e),
                                "status": "UNKNOWN"})
                continue

            entry = {
                "table": table,
                "before_count": old_count,
                "after_count": new_count,
                "count_delta": new_count - old_count,
                "old_max": old_max,
                "new_max": None,
                "status": "ok",
            }

            pk_cols = self.pool.get_primary_key(self.node, self.schema, table)
            if old_max is not None and pk_cols and len(pk_cols) == 1:
                try:
                    rows = self.pool.execute(
                        self.node,
                        "SELECT MAX(%s) FROM %s.%s" % (
                            _quote_ident(pk_cols[0]),
                            _quote_ident(self.schema),
                            _quote_ident(table)),
                    )
                    new_max = rows[0][0] if rows else None
                    entry["new_max"] = new_max
                    if new_max is not None and new_max < old_max:
                        entry["status"] = "MAX_REGRESSION"
                        entry["detail"] = (
                            "最大主键从 %s 回退到 %s —— 故障前的尾部数据未恢复"
                            % (old_max, new_max))
                        losses.append(entry)
                except Exception as e:
                    entry["max_error"] = str(e)

            if entry["count_delta"] < 0:
                entry["status"] = ("MAX_REGRESSION" if entry["status"] == "MAX_REGRESSION"
                                   else "ROW_LOSS")
                entry.setdefault("detail", "行数减少 %d 行" % -entry["count_delta"])
                if entry not in losses:
                    losses.append(entry)

            results.append(entry)

        return {
            "batch_id": batch_id,
            "tables_checked": len(results),
            "tables_with_loss": len(losses),
            "success": len(losses) == 0,
            "details": results,
            "losses": losses,
            "note": ("行数减少或最大主键回退都是确定的丢失证据。"
                     "但本检查无法区分「业务正常删除」与「故障丢失」——"
                     "唯一可靠的区分手段是应用侧记录（app-db 追踪模式）。"),
        }

    def batch_exists(self, batch_id: str) -> bool:
        """True if this batch left any snapshot or marker behind."""
        for table in ("_rpo_snapshots", "_rpo_markers"):
            try:
                rows = self.pool.execute(
                    self.node,
                    "SELECT COUNT(*) FROM %s.%s WHERE batch_id = %%s"
                    % (_quote_ident(self.schema), _quote_ident(table)),
                    (batch_id,),
                )
                if rows and rows[0][0] > 0:
                    return True
            except Exception:
                continue
        return False

    def check_all(self, batch_id: str) -> dict:
        """Run the full RPO assessment: markers + count/key conservation."""
        logger.info("Running full RPO check (batch=%s) ...", batch_id)

        # A batch id that was never planted (a typo, or a plant that failed)
        # must not roll up to "PASS - no data loss": no evidence is not the
        # same as no loss, and that is the most dangerous way to be wrong
        # about an RPO result.
        if not self.batch_exists(batch_id):
            return {
                "batch_id": batch_id,
                "node": self.node,
                "rpo_success": False,
                "data_loss_detected": None,
                "error": ("批次 '%s' 在节点 %s 上不存在（没有对应的快照或探针）。"
                          "请确认 batch_id 是否正确，或重新执行 rpo-plant。"
                          % (batch_id, self.node)),
                "markers": {"total": 0, "found": 0, "missing": 0,
                            "missing_details": [], "success": False},
                "conservation": {"tables_checked": 0, "tables_with_loss": 0,
                                 "success": False, "details": [], "losses": []},
                "summary": {"markers_lost": 0, "tables_with_loss": 0,
                            "rows_lost": 0},
            }

        markers = self.check_markers(batch_id)
        conservation = self.check_conservation(batch_id)

        data_loss = not markers["success"] or not conservation["success"]

        return {
            "batch_id": batch_id,
            "node": self.node,
            "rpo_success": not data_loss,
            "data_loss_detected": data_loss,
            "markers": markers,
            "conservation": conservation,
            "summary": {
                "markers_lost": markers["missing"],
                "tables_with_loss": conservation["tables_with_loss"],
                "rows_lost": sum(
                    -d["count_delta"] for d in conservation["details"]
                    if d.get("count_delta", 0) < 0),
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
