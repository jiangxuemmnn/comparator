"""Reconcile a JMeter result file against what the database actually holds.

This answers the question that a JMeter report on its own cannot: when the
client says N writes failed, how many of them actually landed?

The classic RPO test discrepancy -- the client has FEWER records than the
database -- happens when a write commits and the response is lost on the way
back (failover, connection reset, client crash). JMeter records that sample
as a failure, but the row is in the database. Read naively, that looks like
data loss when it is the opposite.

Matching the two sides needs a key that exists on both: JMeter must emit a
per-request identifier into the JTL (sample_variables=REQ_ID) and pass the
same value to sp_track_write as p_request_id. With that, every request falls
into one of four cells:

                    row in database      row absent
    client success  normal               REAL DATA LOSS
    client failure  lost response        normal failure
                    (false alarm)

The top-right cell is the only one that is an actual defect. The
bottom-left cell is the phenomenon worth explaining rather than alarming
about.

Two effects cannot be attributed per request and are reported separately:

  * A request whose response was lost AND whose tracking call never ran.
    Nothing in the JTL or the tracking table mentions it, but the row is in
    the database. Detected in aggregate by scanning the business tables for
    rows with no matching tracking record.

  * JMeter rows carrying no request id at all (sample_variables not
    configured). Counted and reported rather than silently skipped.
"""

from __future__ import annotations

import csv
import itertools
import json
import logging

from utils.db import DatabasePool, qualified, quote_ident

logger = logging.getLogger("comparator.jtl")

# Column name JMeter writes for sample_variables=REQ_ID. Configurable,
# because the variable can be named anything in the test plan.
DEFAULT_ID_COLUMNS = ("REQ_ID", "REQUEST_ID", "request_id", "req_id")

REAL_LOSS = "REAL_DATA_LOSS"
LOST_RESPONSE = "LOST_RESPONSE"
NORMAL_OK = "OK"
NORMAL_FAILURE = "FAILURE"
UNKNOWN = "UNKNOWN"


def parse_jtl(path, id_column=None, limit=None):
    """Read a JTL file into {request_id: success_bool} plus parse statistics.

    Rows carrying no request id cannot be matched to anything, so they are
    counted in stats rather than dropped without mention.
    """
    by_request = {}
    stats = {"rows": 0, "no_id": 0, "duplicate_id": 0, "id_column": None,
             "columns": [], "error": None}

    with open(path, "r", encoding="utf-8", newline="") as fh:
        # JMeter can prepend comment lines; the first real line is the header.
        body = (line for line in fh if line.strip() and not line.startswith("#"))
        try:
            header_line = next(body)
        except StopIteration:
            stats["error"] = "文件为空"
            return by_request, stats

        reader = csv.DictReader(itertools.chain([header_line], body))
        stats["columns"] = list(reader.fieldnames or [])
        col = _pick_id_column(stats["columns"], id_column)
        stats["id_column"] = col
        if col is None:
            return by_request, stats

        for row in reader:
            if limit and stats["rows"] >= limit:
                break
            stats["rows"] += 1
            rid = (row.get(col) or "").strip()
            if not rid:
                stats["no_id"] += 1
                continue
            success = str(row.get("success", "")).strip().lower() in ("true", "1", "yes")
            if rid in by_request:
                stats["duplicate_id"] += 1
                # A request id should be unique. If it repeats, AND the
                # results so a lost response cannot be masked by a success.
                by_request[rid] = by_request[rid] and success
            else:
                by_request[rid] = success

    return by_request, stats


def _pick_id_column(header, requested):
    if requested:
        return requested if requested in header else None
    for cand in DEFAULT_ID_COLUMNS:
        if cand in header:
            return cand
    return None


def reconcile(pool: DatabasePool, node, jtl_path, batch_id=None,
              schema="public", id_column=None, check_orphans=True,
              orphan_limit=200):
    """Compare a JTL result file with the database."""
    try:
        by_request, stats = parse_jtl(jtl_path, id_column=id_column)
    except FileNotFoundError:
        return {
            "success": False,
            "error": "JTL 文件不存在: %s" % jtl_path,
            "jtl_stats": {},
        }
    except OSError as e:
        return {
            "success": False,
            "error": "无法读取 JTL 文件 %s: %s" % (jtl_path, e),
            "jtl_stats": {},
        }

    if stats["id_column"] is None:
        return {
            "success": False,
            "error": ("JTL 中没有找到请求标识列（尝试过 %s）。"
                      "请在 JMeter 的 user.properties 中设置 "
                      "sample_variables=REQ_ID，并确保测试计划为每个请求"
                      "生成唯一的请求标识。" % ", ".join(DEFAULT_ID_COLUMNS)),
            "jtl_stats": stats,
        }

    tracked = _load_tracking(pool, node, schema, batch_id)
    outcomes = {}
    for rid, success in by_request.items():
        rec = tracked.get(rid)
        if rec is None:
            # The client reported on a request the database has no record
            # of. Whether the row exists cannot be decided from here; that
            # is what the orphan scan is for.
            outcomes[rid] = {
                "request_id": rid, "client_success": success,
                "verdict": UNKNOWN,
                "detail": "追踪表中没有该请求的记录，无法判定其数据库状态",
            }
            continue
        exists, detail = _row_state(pool, node, schema, rec)
        if success and exists:
            verdict = NORMAL_OK
        elif success and not exists:
            verdict = REAL_LOSS
        elif not success and exists:
            verdict = LOST_RESPONSE
        else:
            verdict = NORMAL_FAILURE
        outcomes[rid] = {
            "request_id": rid,
            "client_success": success,
            "row_in_db": exists,
            "verdict": verdict,
            "table": rec["table_name"],
            "operation": rec["operation"],
            "pk": _pk_display(rec),
            "detail": detail,
        }

    # Tracking records whose request never appears in the JTL.
    orphan_requests = [r for r in tracked if r not in by_request]

    result = _summarize(outcomes, stats, orphan_requests, batch_id)
    if check_orphans:
        existing_ids = {rid for rid, o in outcomes.items() if o.get("row_in_db")}
        # Requests with no tracking record at all have an unknown row state;
        # include them so their keys can still bound the scan, but only if
        # the record itself was found.
        existing_ids |= {rid for rid in orphan_requests}
        result["untracked_rows"] = find_untracked_rows(
            pool, node, schema, tracked, existing_ids=existing_ids,
            limit=orphan_limit)
    return result


def _pk_display(rec):
    pk = rec.get("pk_values")
    if isinstance(pk, str):
        try:
            pk = json.loads(pk)
        except Exception:
            return pk
    return pk


def _load_tracking(pool, node, schema, batch_id):
    """{request_id: record} for the batch (or the whole table)."""
    sql = ("SELECT request_id, table_name, operation, pk_values, row_data, "
           "created_at FROM %s._app_db_tracking WHERE request_id IS NOT NULL"
           % quote_ident(schema))
    params = ()
    if batch_id:
        sql += " AND batch_id = %s"
        params = (batch_id,)
    rows = pool.execute(node, sql, params)
    out = {}
    for r in rows:
        out[r["request_id"]] = r
    return out


def _row_state(pool, node, schema, rec):
    """Does the tracked operation's row exist in the database right now?"""
    op = rec["operation"]
    pk = rec["pk_values"]
    if isinstance(pk, str):
        pk = json.loads(pk)
    if not pk:
        return False, "追踪记录没有主键，无法判定"
    where = " AND ".join("%s = %%s" % quote_ident(k) for k in pk.keys())
    target = qualified(schema, rec["table_name"])
    try:
        rows = pool.execute(node, "SELECT COUNT(*) FROM %s WHERE %s" % (target, where),
                            list(pk.values()))
    except Exception as e:
        return False, "查询失败: %s" % e
    count = rows[0][0] if rows else 0
    if op == "DELETE":
        return count == 0, "删除操作，行已不存在" if count == 0 else "删除操作，行仍然存在"
    return count > 0, "行存在" if count > 0 else "行不存在"


def _summarize(outcomes, stats, orphan_requests, batch_id):
    cells = {
        NORMAL_OK: [], NORMAL_FAILURE: [], LOST_RESPONSE: [],
        REAL_LOSS: [], UNKNOWN: [],
    }
    for o in outcomes.values():
        cells[o["verdict"]].append(o)

    lost = cells[LOST_RESPONSE]
    real = cells[REAL_LOSS]
    unknown = cells[UNKNOWN]

    notes = []
    if lost:
        notes.append(
            "有 %d 个请求在 JMeter 中被记为失败，但数据其实已经落库 —— "
            "这是提交成功、响应在返回路径上丢失导致的『假失败』，"
            "不是数据丢失。RPO 统计中客户端记录少于数据库记录，"
            "通常正是这一项造成的。" % len(lost))
    if real:
        notes.append(
            "有 %d 个请求在 JMeter 中记为成功，但数据不在库里 —— "
            "这才是真正的数据丢失，需要重点排查。" % len(real))
    if unknown:
        notes.append(
            "有 %d 个请求在追踪表中没有记录（PostProcessor 未执行或未提交），"
            "其数据库状态无法逐个判定；"
            "请参考 untracked_rows 的汇总结果。" % len(unknown))
    if stats.get("no_id"):
        notes.append(
            "JTL 中有 %d 行没有请求标识，已跳过。"
            "请在 JMeter 中配置 sample_variables=REQ_ID。" % stats["no_id"])
    if orphan_requests:
        notes.append(
            "追踪表中有 %d 条记录的请求标识未出现在 JTL 中，"
            "可能是结果文件不完整。" % len(orphan_requests))

    return {
        "success": not real and not unknown,
        "batch_id": batch_id,
        "jtl_stats": stats,
        "total_requests": len(outcomes),
        "cells": {
            "client_ok_db_ok": len(cells[NORMAL_OK]),
            "client_ok_db_missing": len(real),
            "client_failed_db_ok": len(lost),
            "client_failed_db_missing": len(cells[NORMAL_FAILURE]),
            "undetermined": len(unknown),
        },
        "real_data_loss": real[:50],
        "lost_response": lost[:50],
        "undetermined": unknown[:50],
        "orphan_request_ids": orphan_requests[:50],
        "notes": notes,
    }


# The untracked scan needs a window that brackets the run. A time window is
# preferred: a key range can only bracket rows BETWEEN tracked keys, so a row
# written just past the highest tracked key stays invisible to it.
_MIN_TRACKED_KEYS = 2
_MAX_SPARSITY = 100

# Columns that plausibly record when a row was written, so the batch's own
# tracking timestamps can be used as the scan window.
_TIME_COLUMN_HINTS = ("created_at", "create_time", "created_time", "ctime",
                      "insert_time", "inserted_at", "create_date", "ts",
                      "created", "gmt_create")


def find_untracked_rows(pool, node, schema, tracked, existing_ids=None,
                        limit=200):
    """Rows in the business tables with no matching tracking record.

    Catches the case the per-request comparison cannot: a write that
    committed while the client was cut off before its tracking call ran.
    Nothing links such a row to a request, so the only way to find it is to
    look at the key range the run actually touched.

    That range is derived ONLY from tracked keys that really exist in the
    database. A tracked record for a row that was never written (a genuine
    data loss) would otherwise stretch the range to its key and sweep in
    every unrelated row between -- a false alarm thousands of rows wide.
    A sparse range is reported as inconclusive rather than scanned.
    """
    by_table = {}
    for rec in tracked.values():
        pk = rec["pk_values"]
        if isinstance(pk, str):
            try:
                pk = json.loads(pk)
            except Exception:
                continue
        if not pk or len(pk) != 1:
            continue
        col, val = list(pk.items())[0]
        entry = by_table.setdefault(rec["table_name"], {
            "col": col, "values": set(), "times": []})

        # The timestamp says when the request was made, which is true
        # whether or not the row it names turned out to exist -- a request
        # whose row went missing still happened during the run.
        if rec.get("created_at") is not None:
            entry["times"].append(rec["created_at"])

        # The key only bounds a range if the row is actually in the database.
        if existing_ids is not None and rec["request_id"] not in existing_ids:
            continue
        entry["values"].add(str(val))

    out = []
    skipped = []
    for table, info in sorted(by_table.items()):
        col = info["col"]
        wanted = info["values"]
        time_col = _pick_time_column(pool, node, schema, table)
        times = [t for t in info["times"] if t is not None]

        if time_col and times:
            lo_t, hi_t = min(times), max(times)
            try:
                found = pool.execute(
                    node,
                    "SELECT %s FROM %s WHERE %s >= %%s AND %s <= %%s" % (
                        quote_ident(col), qualified(schema, table),
                        quote_ident(time_col), quote_ident(time_col)),
                    (lo_t, hi_t))
                untracked = [r[0] for r in found if str(r[0]) not in wanted]
            except Exception as e:
                logger.warning("untracked time-window scan failed on %s: %s", table, e)
                skipped.append({"table": table, "reason": "扫描失败: %s" % e})
                continue
            window = {"kind": "时间窗口", "column": time_col,
                      "range": [str(lo_t), str(hi_t)]}
        elif (len(wanted) >= _MIN_TRACKED_KEYS
              and all(_numeric(v) for v in wanted)):
            vals = sorted(float(v) for v in wanted)
            lo_bound, hi_bound = vals[0], vals[-1]
            span = hi_bound - lo_bound + 1
            if span > len(wanted) * _MAX_SPARSITY:
                skipped.append({
                    "table": table,
                    "reason": ("追踪主键过于稀疏（%d 个键跨越 %d 的区间），"
                               "且该表无可用时间列，无法界定覆盖范围"
                               % (len(wanted), int(span))),
                })
                continue
            try:
                found = pool.execute(
                    node,
                    "SELECT %s FROM %s WHERE %s >= %%s AND %s <= %%s" % (
                        quote_ident(col), qualified(schema, table),
                        quote_ident(col), quote_ident(col)),
                    (lo_bound, hi_bound))
                untracked = [r[0] for r in found if str(r[0]) not in wanted]
            except Exception as e:
                logger.warning("untracked key-range scan failed on %s: %s", table, e)
                skipped.append({"table": table, "reason": "扫描失败: %s" % e})
                continue
            window = {"kind": "主键区间", "column": col,
                      "range": [str(lo_bound), str(hi_bound)]}
            skipped.append({
                "table": table,
                "reason": ("无时间列，回退到主键区间扫描：只能发现区间内部"
                           "未追踪的行，落在区间两端之外的行不会被发现"),
            })
        elif not time_col:
            skipped.append({
                "table": table,
                "reason": "该表没有可识别的时间列，且主键不足以界定范围"})
            continue
        else:
            skipped.append({
                "table": table,
                "reason": "追踪记录中没有可用时间戳，且主键不足以界定范围"})
            continue

        if untracked:
            out.append({
                "table": table,
                "column": col,
                "window": window,
                "range": window["range"],
                "untracked_count": len(untracked),
                "sample": untracked[:20],
            })

    return {"rows": out, "skipped": skipped}


def _pick_time_column(pool, node, schema, table):
    """Find a column on `table` that records when a row was written."""
    try:
        cols = pool.get_column_info(node, schema, table)
    except Exception:
        return None
    by_name = {c["name"].lower(): c["name"] for c in cols}
    for hint in _TIME_COLUMN_HINTS:
        if hint in by_name:
            return by_name[hint]
    for c in cols:
        t = (c["type"] or "").lower()
        if "timestamp" in t or t == "date":
            return c["name"]
    return None


def _numeric(v):
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


def format_report(result):
    """Human-readable rendering of reconcile()."""
    lines = []
    if not result.get("success") and result.get("error"):
        return result["error"]
    if result.get("error"):
        return result["error"]

    cells = result["cells"]
    lines.append("JTL 对账结果" + ("（批次 %s）" % result["batch_id"]
                                   if result.get("batch_id") else ""))
    lines.append("=" * 62)
    lines.append("")
    lines.append("                    DB 中有数据      DB 中无数据")
    lines.append("  JMeter 报成功     %-16d %d" % (
        cells["client_ok_db_ok"], cells["client_ok_db_missing"]))
    lines.append("  JMeter 报失败     %-16d %d" % (
        cells["client_failed_db_ok"], cells["client_failed_db_missing"]))
    lines.append("")
    lines.append("  未判定: %d" % cells["undetermined"])
    lines.append("")

    if cells["client_failed_db_ok"]:
        lines.append("[应答丢失 / 假失败] %d 条 —— 数据其实已落库"
                     % cells["client_failed_db_ok"])
        for d in result["lost_response"][:10]:
            lines.append("    req=%s %s.%s pk=%s" % (
                d["request_id"], d.get("table"), d.get("operation"), d.get("pk")))
        lines.append("")

    if cells["client_ok_db_missing"]:
        lines.append("[真数据丢失] %d 条 —— 需要重点排查"
                     % cells["client_ok_db_missing"])
        for d in result["real_data_loss"][:10]:
            lines.append("    req=%s %s.%s pk=%s" % (
                d["request_id"], d.get("table"), d.get("operation"), d.get("pk")))
        lines.append("")

    untracked = result.get("untracked_rows") or {}
    rows = untracked.get("rows") or []
    if rows:
        total = sum(u["untracked_count"] for u in rows)
        lines.append("[未被追踪的行] 共 %d 行 —— "
                     "这些行的写操作没有留下追踪记录" % total)
        for u in rows[:10]:
            w = u.get("window") or {}
            lines.append("    %s.%s [%s %s..%s] 共 %d 行，样例 %s" % (
                u["table"], u["column"], w.get("kind", "范围"),
                u["range"][0], u["range"][1],
                u["untracked_count"], u["sample"][:5]))
        lines.append("")
    for sk in (untracked.get("skipped") or []):
        lines.append("反向扫描 %s: %s" % (sk["table"], sk["reason"]))
    if untracked.get("skipped"):
        lines.append("")

    for n in result.get("notes", []):
        lines.append("说明: %s" % n)

    lines.append("")
    lines.append("JTL 行数=%d  请求标识列=%s  无标识行=%d" % (
        result["jtl_stats"]["rows"], result["jtl_stats"]["id_column"],
        result["jtl_stats"]["no_id"]))
    return "\n".join(lines)
