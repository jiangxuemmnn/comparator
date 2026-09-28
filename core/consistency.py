"""Decide how much a comparison result can actually be trusted.

Two nodes are two independent connections, therefore two independent
snapshots taken at different moments. While the application keeps writing,
that time gap alone produces differences. This module exists so the tool
can say WHICH of the following a result is, instead of collapsing
everything into PASS/FAIL:

  deterministic -- the two sides were read at a point where no writes were
                   in flight, so a difference is real and so is equality
  snapshot      -- reads happened at different moments; equality is
                   meaningful for append-only tables, inequality may be lag
  weak          -- repeated agreement only; no evidence about any particular
                   instant
  inconclusive  -- the environment cannot support a verdict at all

What each mechanism can and cannot establish:

  watermark (compare only pk <= W)
      INSERT  yes   -- rows above W are excluded from both sides
      UPDATE  NO    -- the row keeps its key, so it stays inside the window
                       and is still changing
      DELETE  NO    -- same reason
      Requires a monotonic single-column key.

  quiesce (wait until the tables stop changing)
      INSERT / UPDATE / DELETE all yes. The only mechanism that covers all
      three, because it removes the time gap itself rather than working
      around it. Cost: coverage -- writes during the wait are not checked,
      and a busy system may never reach a quiet window.

  LSN catch-up (primary LSN vs standby replay LSN)
      Establishes that the standby replayed everything committed on the
      primary up to a point. It does NOT by itself anchor the reads to the
      same instant, because the primary keeps moving while it is read.
      Applies only to physical streaming replication: logical replication
      can apply out of order, multi-master has no single LSN ordering, and
      a shared-storage cluster (RAC) has no replay at all.

  read-time zero-write check (L_a2 == L_a)
      Take the LSN as the FIRST statement of a REPEATABLE READ transaction
      and again before committing. If they are equal, nothing was written
      on the primary for the whole duration of the read -- an extremely
      short quiesce window obtained for the price of two function calls.
      This is cheap enough to always attempt.

  convergence (N rounds agree)
      Separates a transient difference from a persistent one. It says
      nothing about any specific instant, and two nodes that are wrong in
      the same way agree forever.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger("comparator.consistency")

DETERMINISTIC = "deterministic"
SNAPSHOT = "snapshot"
WEAK = "weak"
INCONCLUSIVE = "inconclusive"

DEFAULT_QUIESCE_SECONDS = 5
DEFAULT_LSN_TIMEOUT = 30


def parse_lsn(lsn):
    """Parse '0/C05EC60' into a comparable integer."""
    if lsn is None:
        return None
    if isinstance(lsn, int):
        return lsn
    try:
        hi, _, lo = str(lsn).partition("/")
        return (int(hi, 16) << 32) | int(lo, 16)
    except (ValueError, TypeError):
        return None


class ConsistencySession:
    """Runs the pre-flight checks, gates the comparison, then grades it."""

    def __init__(self, pool, config, nodes, base, mode=None):
        cfg = (config.get("consistency") or {})
        self.pool = pool
        self.nodes = list(nodes)
        self.base = base
        self.mode = mode or cfg.get("mode", "auto")
        self.quiesce_seconds = int(cfg.get("quiesce_seconds",
                                           DEFAULT_QUIESCE_SECONDS))
        self.lsn_timeout = int(cfg.get("lsn_timeout", DEFAULT_LSN_TIMEOUT))
        self.capabilities = {}
        self.notes = []

    # -- Level 0: what is this environment even capable of? ----------------

    def preflight(self, schema: str, tables) -> dict:
        """Probe the environment once and record what it supports."""
        caps = {
            "lsn": False,
            "change_counter": False,
            "standbys": [],
            "primary": None,
            "error": None,
        }
        try:
            for n in self.nodes:
                if self.pool.is_in_recovery(n):
                    if self.pool.last_wal_replay_lsn(n) is not None:
                        caps["standbys"].append(n)
                else:
                    if self.pool.current_wal_lsn(n) is not None:
                        caps["primary"] = n
            caps["lsn"] = bool(caps["primary"]) and bool(caps["standbys"])
            total, _ = self.pool.table_change_counter(self.base, schema, list(tables))
            caps["change_counter"] = total is not None
        except Exception as e:
            caps["error"] = str(e)

        if not caps["lsn"] and caps["standbys"]:
            self.notes.append("缺少可用的 LSN 函数或没有主库连接，无法确认复制追平状态")
        if not caps["standbys"]:
            self.notes.append("未检测到备库（节点均在主库模式），LSN 追平检查不适用")
        if not caps["change_counter"]:
            self.notes.append("pg_stat_user_tables 不可用，无法判定静默窗口")

        self.capabilities = caps
        return caps

    # -- choosing a level --------------------------------------------------

    def choose_mode(self) -> str:
        if self.mode != "auto":
            return self.mode
        caps = self.capabilities
        if caps.get("change_counter"):
            return "quiesce"
        if caps.get("lsn"):
            return "watermark"
        return "loose"

    # -- Level 1/2: get the run ready --------------------------------------

    def begin(self, schema: str, tables, watermark=None) -> dict:
        """Wait for a usable window, then record the state the run starts in."""
        mode = self.choose_mode()
        tables = list(tables)
        ctx = {
            "mode": mode,
            "schema": schema,
            "tables": tables,
            "quiesced": False,
            "lsn_start": None,
            "counter_start": None,
            "watermark": watermark,
            "notes": [],
        }

        if mode in ("quiesce", "auto"):
            ok = self.wait_quiesce(schema, tables)
            ctx["quiesced"] = ok
            if not ok:
                ctx["notes"].append(
                    "等待 %ds 未获得静默窗口（业务持续写入），"
                    "本轮对 UPDATE/DELETE 的差异无法与复制延迟区分"
                    % self.quiesce_seconds)

        counter, _ = self.pool.table_change_counter(self.base, schema, tables)
        ctx["counter_start"] = counter
        if self.capabilities.get("lsn") and self.capabilities.get("primary"):
            ctx["lsn_start"] = self.pool.current_wal_lsn(self.capabilities["primary"])

        return ctx

    def end(self, ctx: dict) -> dict:
        """Grade the run that just finished.

        The evidence that the read spanned no writes comes from bracketing
        it -- LSN before and after, change counters before and after. The
        initial quiesce only makes that bracket likely to succeed; on its own
        it says nothing about what happened while the comparison ran, so it
        must never override a positive detection of writes.
        """
        mode = ctx.get("mode", "loose")
        notes = list(ctx.get("notes", []))
        lagging = []

        quiet = None      # None = no evidence either way
        basis = None

        # Signal 1: LSN bracketing. Authoritative when available.
        if ctx.get("lsn_start") is not None:
            primary = self.capabilities.get("primary")
            lsn_end = self.pool.current_wal_lsn(primary)
            if lsn_end is not None:
                if lsn_end == ctx["lsn_start"]:
                    quiet, basis = True, "读取期间主库 LSN 未变化（%s）" % lsn_end
                else:
                    quiet, basis = False, "读取期间主库 LSN 由 %s 推进到 %s" % (
                        ctx["lsn_start"], lsn_end)

        # Signal 2: per-table change counters. Catches writes the LSN check
        # cannot see when no primary connection is configured.
        if quiet is None and ctx.get("counter_start") is not None:
            counter, _ = self.pool.table_change_counter(
                self.base, ctx["schema"], ctx["tables"])
            if counter is not None:
                if counter == ctx["counter_start"]:
                    quiet, basis = True, "读取期间被测表变更计数未变化"
                else:
                    quiet, basis = False, "读取期间被测表变更计数 %s -> %s" % (
                        ctx["counter_start"], counter)

        if quiet is True:
            confidence = DETERMINISTIC
            notes.append(basis)
        elif quiet is False:
            confidence = SNAPSHOT
            notes.append(basis)
            if ctx.get("lsn_start") is not None:
                lagging = self.wait_lsn_all(ctx["lsn_start"])
                if not lagging:
                    notes.append("备库已回放至读取起始点 %s；"
                                 "期间有新写入，结论仅对 INSERT 类差异成立"
                                 % ctx["lsn_start"])
                else:
                    notes.append("以下备库在本轮结束前未回放到读取起始点: %s"
                                 % ", ".join(lagging))
            else:
                notes.append("无法确认复制是否追平，差异可能是复制延迟")
        else:
            # No LSN and no counters: fall back to the quiesce gate alone.
            confidence = DETERMINISTIC if ctx.get("quiesced") else WEAK
            if not ctx.get("quiesced"):
                notes.append("环境不提供 LSN 与变更计数，结论建立在两个独立快照上")
            else:
                notes.append("环境不提供读取期间的无写入证据，"
                             "结论仅依据开始前的静默窗口")

        return {
            "mode": mode,
            "confidence": confidence,
            "read_was_quiet": quiet,
            "quiesced": ctx.get("quiesced", False),
            "lagging_nodes": lagging,
            "notes": notes,
            "capabilities": self.capabilities,
        }

    # -- primitives --------------------------------------------------------

    def wait_quiesce(self, schema: str, tables, poll: float = 1.0,
                     max_wait: float = None) -> bool:
        """True once the tables have been unchanged for `quiesce_seconds`.

        Uses pg_stat_user_tables' insert/update/delete counters rather than
        COUNT(*) or MAX(pk): those miss UPDATEs entirely, so a table being
        updated in place would look quiet forever. Reads the counters for
        the tables actually being compared, so unrelated activity elsewhere
        on the instance cannot prevent a window from being found.
        """
        tables = list(tables)
        if not tables:
            return False
        max_wait = max_wait if max_wait is not None else (self.quiesce_seconds * 6 + 10)

        start = time.time()
        last, _ = self.pool.table_change_counter(self.base, schema, tables)
        if last is None:
            return False
        last_change_at = time.time()

        while time.time() - start < max_wait:
            time.sleep(poll)
            cur, _ = self.pool.table_change_counter(self.base, schema, tables)
            if cur is None:
                return False
            if cur != last:
                last = cur
                last_change_at = time.time()
                continue
            if time.time() - last_change_at >= self.quiesce_seconds:
                logger.info("静默窗口达成：%d 秒内被测表无变更", self.quiesce_seconds)
                return True

        logger.warning("等待 %ds 未获得静默窗口", self.quiesce_seconds)
        return False

    def wait_lsn_all(self, target_lsn, timeout=None):
        """Wait for every standby to replay past `target_lsn`.

        Returns the list of standbys that did NOT catch up in time.
        """
        target = parse_lsn(target_lsn)
        if target is None:
            return list(self.capabilities.get("standbys", []))
        timeout = timeout if timeout is not None else self.lsn_timeout
        pending = set(self.capabilities.get("standbys", []))
        deadline = time.time() + timeout

        while pending and time.time() < deadline:
            for n in list(pending):
                cur = parse_lsn(self.pool.last_wal_replay_lsn(n))
                if cur is not None and cur >= target:
                    pending.discard(n)
            if pending:
                time.sleep(0.5)
        return sorted(pending)


def grade_rounds(rounds):
    """Reduce N convergence rounds to a verdict.

    A round only counts as evidence if the tables were not being written
    during it -- otherwise "agreed three times running" can just mean the
    sampling always landed in the same kind of gap.
    """
    if not rounds:
        return {"confidence": INCONCLUSIVE, "conclusion": INCONCLUSIVE,
                "note": "没有可用轮次"}

    quiet = [r for r in rounds if r.get("quiesced")]
    noisy = [r for r in rounds if not r.get("quiesced")]

    if quiet:
        if all(r.get("identical") for r in quiet):
            return {"confidence": DETERMINISTIC, "conclusion": "CONSISTENT",
                    "note": "在 %d 个零变更轮次中全部一致" % len(quiet)}
        return {"confidence": DETERMINISTIC, "conclusion": "DIVERGENT",
                "note": "零变更轮次中也出现不一致 —— 这是最强的结论形式"}

    if noisy and all(r.get("identical") for r in noisy):
        return {"confidence": WEAK, "conclusion": "CONSISTENT",
                "note": "%d 轮均在变更中采样，一致只是弱证据" % len(noisy)}

    if noisy:
        return {"confidence": INCONCLUSIVE, "conclusion": INCONCLUSIVE,
                "note": "无零变更轮次且存在不一致，无法区分真实差异与复制延迟"}

    return {"confidence": INCONCLUSIVE, "conclusion": INCONCLUSIVE,
            "note": "无可用轮次"}
