#!/usr/bin/env python3
"""Database Consistency Comparator - CLI entry point.

Tool for database cluster consistency verification, supporting:
  1. Node-to-node data comparison
  2. Application-to-database consistency
  3. RPO data loss detection
  4. Custom workload execution

Usage:
  python cli.py -c config.yaml node-compare -a node1 -b node2
  python cli.py -c config.yaml node-compare -a node1 -b node2 --backend native
  python cli.py -c config.yaml rpo-plant -n node1
  python cli.py -c config.yaml rpo-check -n node1 -bid abc123
  python cli.py -c config.yaml app-db-setup -n node1      # JMeter tracking setup
  python cli.py -c config.yaml app-db-verify -n node1      # Verify JMeter writes
  python cli.py -c config.yaml run-workload -n node1 -w templates/transfer.yaml
  python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2
"""

import argparse
import contextlib
import logging
import io
import sys
import os
import time
import signal
import json
from datetime import datetime

__version__ = "2.0.0"

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config import load_config, generate_template
from utils.logger import setup_logger
from utils.db import DatabasePool
from core.node_comparator import (
    run_node_comparison, run_multi_node_comparison, list_backends,
)
from core.app_db_comparator import (
    AppDBTracker, setup_tracking, teardown_tracking,
    verify_batch, get_batch_summary,
)
from core.rpo_checker import RPOChecker
from core.jtl_reconciler import reconcile as jtl_reconcile, format_report as jtl_format
from core.consistency import ConsistencySession
from backends.pgdiff import SCHEMA_TYPES
from workload.generator import DataGenerator
from workload.runner import WorkloadRunner


def _print_result(result: dict, fmt: str = "table"):
    """Pretty-print a comparison result."""
    if fmt == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return

    # Table format
    if "results" in result:
        from tabulate import tabulate
        rows = []
        for r in result["results"]:
            row = [
                r.get("table", ""),
                r.get("rows_a", ""),
                r.get("rows_b", ""),
            ]
            if "chunks_total" in r:
                row.append("%d/%d" % (r.get("chunks_diff", 0), r.get("chunks_total", 0)))
            else:
                row.append("-")
            row.append("OK" if r.get("identical") else "DIFF")
            rows.append(row)

        headers = ["Table", "Rows(A)", "Rows(B)", "Chunks", "Status"]
        print(tabulate(rows, headers=headers, tablefmt="grid"))
        print()

    # External backends report a different shape; surface the reason plainly
    # instead of printing a summary full of zeros.
    if result.get("error"):
        print("对比未能完成: %s" % result["error"])
        if result.get("skipped"):
            for sk in result["skipped"][:10]:
                print("  %s: %s" % (sk.get("table"), sk.get("reason")))
        return

    if result.get("per_table"):
        from tabulate import tabulate
        rows = [[t.get("table"), t.get("key", ""),
                 "OK" if t.get("identical") else
                 ("FAIL" if t.get("identical") is False else "N/A")]
                for t in result["per_table"]]
        print(tabulate(rows, headers=["Table", "Key", "Status"], tablefmt="grid"))
        print()
        if result.get("diff_count", 0) > 0:
            print("Diff details (前 40 行):")
            for line in (result.get("details") or [])[:40]:
                print("    %s" % line)
            print()

    # Summary
    print("Summary:")
    print("  Backend:      %s" % result.get("backend", "N/A"))
    print("  Tables checked: %s" % result.get("tables_checked", 0))
    print("  Tables diff:   %s" % result.get("tables_diff", 0))
    if result.get("tables_inconclusive"):
        print("  Inconclusive:  %s" % result.get("tables_inconclusive"))
    print("  Total time:    %ss" % result.get("total_time", "N/A"))
    print("  Result:        %s" % result.get(
        "conclusion", "PASS" if result.get("success") else "FAIL"))

    # Diff details
    if result.get("tables_diff", 0) > 0 and "results" in result:
        print("\nDiff details:")
        for r in result["results"]:
            if r.get("identical"):
                continue
            print("  [%s]" % r.get("table"))
            if r.get("note"):
                print("    注意: %s" % r["note"])
            for d in (r.get("diff_details") or [])[:10]:
                print("    - %s" % d)
            for loc in (r.get("localized") or [])[:5]:
                print("    chunk %d 行级定位:" % loc.get("chunk"))
                print("       仅A有  : %s" % (loc.get("only_in_a"),))
                print("       仅B有  : %s" % (loc.get("only_in_b"),))
                print("       值不同 : %s" % (loc.get("value_mismatch"),))
                if loc.get("truncated"):
                    print("       （列表已截断）")

    # Anything the run could not decide is stated, not folded into PASS/FAIL.
    inconclusive = [r for r in result.get("results", [])
                    if r.get("confidence") in ("row_count_only", "unavailable",
                                               "aborted")]
    if inconclusive:
        print("\n以下表的结论不完整:")
        for r in inconclusive:
            reason = (r.get("error") or r.get("note")
                      or r.get("confidence") or "未知原因")
            print("  [%s] %s" % (r.get("table"), reason))


def cmd_node_compare(args, config, pool):
    """Compare data across database nodes."""
    logger = setup_logger(config)
    tables = args.tables.split(",") if args.tables else None

    nodes = getattr(args, "nodes", None)
    if nodes:
        node_list = [n.strip() for n in nodes.split(",") if n.strip()]

        # Pairwise-only external engines have no matrix mode; if the user
        # asked for exactly two nodes, honour that instead of failing.
        if args.backend in ("pg_comparator", "data_diff") and len(node_list) == 2:
            logger.info("Node comparison: %s vs %s, backend=%s (pairwise)",
                        node_list[0], node_list[1], args.backend)
            result, assessment = _with_consistency(
                pool, config, args, node_list, node_list[0], tables,
                lambda: run_node_comparison(
                    pool, config, node_a=node_list[0], node_b=node_list[1],
                    backend=args.backend, tables=tables,
                    schema=args.schema, args=args))
            _print_result(result, args.format)
            _print_consistency(assessment, args.format)
            return result.get("exit_code", 0 if result["success"] else 1)

        logger.info("Multi-node comparison: %s, backend=%s, primary=%s",
                    ",".join(node_list), args.backend, args.primary or "-")
        result, assessment = _with_consistency(
            pool, config, args, node_list, args.primary or node_list[0], tables,
            lambda: run_multi_node_comparison(
                pool, config, node_list,
                backend=args.backend,
                primary=args.primary,
                tables=tables,
                schema=args.schema,
                args=args,
                watermark=getattr(args, "watermark", None)))
        _print_multi_result(result, args.format)
        _print_consistency(assessment, args.format)
        return result.get("exit_code", 1)

    if not args.node_a or not args.node_b:
        print("Error: 需要 -a/-b 指定两个节点，或用 --nodes 指定多个节点")
        return 1

    logger.info("Node comparison: %s vs %s, backend=%s",
                args.node_a, args.node_b, args.backend)

    result, assessment = _with_consistency(
        pool, config, args, [args.node_a, args.node_b], args.node_a, tables,
        lambda: run_node_comparison(
            pool, config,
            node_a=args.node_a,
            node_b=args.node_b,
            backend=args.backend,
            tables=tables,
            schema=args.schema,
            args=args,
            watermark=getattr(args, "watermark", None)))
    _print_result(result, args.format)
    _print_consistency(assessment, args.format)
    return result.get("exit_code", 0 if result["success"] else 1)


def _with_consistency(pool, config, args, nodes, base, tables, fn):
    """Run `fn` inside a consistency session and grade what it produced.

    The session establishes what the environment can prove (quiet window,
    replica catch-up) and then measures whether any writes landed while the
    comparison was reading. Without it a result is just "two snapshots taken
    at different moments happened to agree or not", which is not the same
    claim and should not be printed as one.
    """
    if getattr(args, "no_consistency", False):
        return fn(), None

    schema = args.schema or config["comparison"]["schema"]
    watch = tables
    if not watch:
        try:
            watch = pool.get_tables(base, schema)
        except Exception:
            watch = []

    mode = getattr(args, "consistency_mode", None)
    watermark = getattr(args, "watermark", None)
    if mode == "watermark" and not watermark:
        print("警告: --consistency-mode watermark 需要同时提供 --watermark <值>；"
              "未提供时水位线不生效，实际比较的是整表。", file=sys.stderr)

    session = ConsistencySession(
        pool, config, nodes, base, mode=mode)
    if getattr(args, "wait_quiesce", None):
        session.quiesce_seconds = int(args.wait_quiesce)
    session.preflight(schema, watch)
    ctx = session.begin(schema, watch,
                        watermark=getattr(args, "watermark", None))
    result = fn()
    assessment = session.end(ctx)
    result["consistency"] = assessment
    return result, assessment


def _print_consistency(assessment, fmt="table"):
    if not assessment:
        return
    if fmt == "json":
        return
    print("\n一致性判定:")
    print("  置信级别: %s   方式: %s" % (assessment["confidence"],
                                         assessment["mode"]))
    if assessment.get("lagging_nodes"):
        print("  未追平的备库: %s" % ", ".join(assessment["lagging_nodes"]))
    for n in assessment.get("notes", []):
        print("    - %s" % n)
    conf = assessment["confidence"]
    if conf in ("snapshot", "weak", "inconclusive"):
        print("  提示: 本轮结论不是确定性的。若需要确定结论，"
              "请在业务停顿的窗口重跑，或增大 --wait-quiesce。")


def _print_multi_result(result: dict, fmt: str = "table"):
    """Render an N-node matrix comparison."""
    if fmt == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return

    from tabulate import tabulate

    nodes = result.get("nodes", [])
    rows = []
    for r in result.get("results", []):
        node_status = r.get("node_status", {})
        row = [r.get("table", "")]
        for n in nodes:
            if n in (r.get("outliers") or {}):
                cell = "DIFF"
            else:
                st = node_status.get(n)
                cell = {"OK": "OK", "UNREACHABLE": "UNREACH"}.get(st, "-")
            row.append(cell)
        row.append(r.get("conclusion", ""))
        rows.append(row)

    headers = ["Table"] + nodes + ["结论"]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    print()

    if result.get("outlier_summary"):
        print("异类节点:")
        for d in result["outlier_summary"]:
            print("  %-8s %d/%d chunks (%.1f%%)  表: %s" % (
                d["node"], d["diff_chunks"], d["total_chunks"],
                d["rate"] * 100, ", ".join(d["tables"])))
            print("           %s" % d["hint"])
        print()

    for r in result.get("results", []):
        if r.get("conclusion") == "CONSISTENT":
            continue
        print("[%s] %s" % (r.get("conclusion"), r.get("table")))
        if r.get("note"):
            print("    %s" % r["note"])
        for d in (r.get("diff_details") or [])[:10]:
            print("    - %s" % d)
        for loc in (r.get("localized") or [])[:5]:
            print("    chunk %d 对比 %s:" % (loc.get("chunk"), loc.get("between")))
            print("       仅前者有: %s" % loc.get("only_in_a"))
            print("       仅后者有: %s" % loc.get("only_in_b"))
            print("       值不同  : %s" % loc.get("value_mismatch"))
        print()

    print("Summary:")
    print("  Backend:        %s" % result.get("backend", "N/A"))
    print("  Mode:           %s (base=%s)" % (result.get("mode"), result.get("base")))
    print("  Tables checked: %s" % result.get("tables_checked", 0))
    print("  Tables diff:    %s" % result.get("tables_diff", 0))
    print("  Inconclusive:   %s" % result.get("tables_inconclusive", 0))
    print("  Total time:     %ss" % result.get("total_time", "N/A"))
    print("  Result:         %s" % result.get("conclusion", "?"))

    for w in result.get("warnings", []):
        print("\n  警告: %s" % w)


def cmd_app_db_setup(args, config, pool):
    """Create tracking table (and stored function where supported)."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]
    info = setup_tracking(pool, args.node, schema)
    print("Tracking infrastructure created on node '%s' (mode: %s)"
          % (args.node, info.get("mode") or "unknown"))
    print()

    if info.get("function"):
        print("JMeter JDBC PostProcessor SQL:")
        print("  SELECT %s.sp_track_write(" % schema)
        print("      '${batchId}',")
        print("      'INSERT',")
        print("      '<table_name>',")
        print("      '{\"<pk_col>\": ${<pk_var>}}',")
        print("      '{\"<col1>\": ${<var1>}, ...}',")
        print("      '${txnId}',")
        print("      '${REQ_ID}'")
        print("  )")
    else:
        print("该节点是 sqlserver 兼容模式：存储函数体内的 INSERT 不受支持，")
        print("因此没有创建 sp_track_write。JMeter 请直接使用普通 INSERT")
        print("（参数从左到右：batch_id, txn_id, request_id, operation,")
        print("  table_name, pk_values, row_data）：")
        print()
        print("  " + " ".join(info["insert_sql"].split()))
    print()
    print("After JMeter finishes, verify with:")
    print("  python cli.py -c %s app-db-verify -n %s" % (args.config, args.node))
    return 0


def cmd_app_db_verify(args, config, pool):
    """Verify all tracked operations against actual database state."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    result = verify_batch(
        pool, args.node,
        batch_id=args.batch_id or None,
        schema=schema,
        mark_verified=not args.no_mark,
    )

    if result.get("error"):
        if args.format == "json":
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        else:
            print("App-DB 校验未能完成: %s" % result["error"])
        return 2

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print("\nApp-DB Consistency Verification:")
        print("  Total operations tracked: %d" % result["total"])
        print("  Verified OK:             %d" % result["verified"])
        if result.get("superseded"):
            print("  Superseded (older records):  %d" % result["superseded"])
        print("  Missing (data lost):     %d" % result["missing"])
        print("  Mismatch (wrong value):  %d" % result["mismatch"])
        print("  Result:  %s" % ("PASS" if result["success"] else "FAIL"))

        if result.get("torn_transactions"):
            print("\n  撕裂事务（同一事务的部分写操作落库、部分没有）:")
            for t in result["torn_transactions"][:20]:
                print("    txn=%s  %d/%d 成功  %s" % (
                    t["txn_id"], t["ok_ops"], t["total_ops"], t["detail"]))

        if result["errors"]:
            print("\n  Error details (first 20):")
            for e in result["errors"][:20]:
                print("    [#%s] [%s] %s.%s — %s" % (
                    e.get("id", "?"),
                    e.get("operation", "?"),
                    e.get("table", "?"),
                    e.get("pk", {}),
                    e.get("detail", ""),
                ))

    return 0 if result["success"] else 1


def cmd_app_db_status(args, config, pool):
    """Show summary of all tracked batches."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    rows = get_batch_summary(pool, args.node, schema)

    if not rows:
        print("No batches found. Run 'app-db-setup' first, then run JMeter.")
        return 0

    if args.format == "json":
        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
    else:
        from tabulate import tabulate
        table = []
        for r in rows:
            table.append([
                r["batch_id"], r["table_name"], r["operation"],
                r["total"], r["verified_count"],
                r["total"] - r["verified_count"],
            ])
        print(tabulate(table,
                       headers=["Batch ID", "Table", "Op", "Total", "Verified", "Pending"],
                       tablefmt="grid"))
    return 0


def cmd_app_db_teardown(args, config, pool):
    """Drop tracking table and stored function."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]
    teardown_tracking(pool, args.node, schema)
    print("Tracking infrastructure removed from node '%s'" % args.node)
    return 0


def cmd_jtl_reconcile(args, config, pool):
    """Reconcile a JMeter .jtl result file against the database."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    result = jtl_reconcile(
        pool, args.node, args.jtl,
        batch_id=args.batch_id,
        schema=schema,
        id_column=args.request_id_column,
        check_orphans=not args.no_orphans,
    )

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print(jtl_format(result))

    if result.get("error"):
        return 2
    return 0 if result.get("success") else 1


def cmd_rpo_plant(args, config, pool):
    """Plant RPO markers before a fault event."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    rpo = RPOChecker(pool, args.node, schema, config=config)
    tables = args.tables.split(",") if args.tables else None

    batch_id = rpo.plant_markers(tables=tables, marker_count=args.marker_count)
    print("RPO markers planted!")
    print("  Node:     %s" % args.node)
    print("  Batch ID: %s" % batch_id)
    print("  Markers:  %d per table" % args.marker_count)
    print()
    print("Now trigger your fault/failover scenario.")
    print("After recovery, run:")
    print("  python cli.py -c <config> rpo-check -n %s -bid %s" % (args.node, batch_id))
    return 0


def cmd_rpo_check(args, config, pool):
    """Check RPO data loss after fault recovery."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    rpo = RPOChecker(pool, args.node, schema, config=config)
    result = rpo.check_all(args.batch_id)

    if result.get("error"):
        if args.format == "json":
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        else:
            print("RPO 检查未能完成: %s" % result["error"])
        return 2

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print("\nRPO Check Results (batch=%s):" % args.batch_id)
        print("=" * 60)

        # Markers -- a whole-batch probe of the replication stream.
        m = result["markers"]
        print("\n[复制流探针 / Marker Check]")
        print("  Total:   %d" % m["total"])
        print("  Found:   %d" % m["found"])
        print("  Missing: %d" % m["missing"])
        if m["missing_details"]:
            print("  丢失的标记序号: %s" % (m.get("missing_seqs") or []))
            for d in m["missing_details"][:10]:
                print("    LOST: %s" % d["marker_id"])
        if m.get("note"):
            print("  说明: %s" % m["note"])

        # Per-table count / max-key conservation.
        c = result["conservation"]
        print("\n[行数与主键守恒 / Conservation]")
        print("  Tables checked: %d" % c["tables_checked"])
        print("  Tables with loss: %d" % c["tables_with_loss"])
        for d in c["details"]:
            if d.get("error"):
                print("    %s: 无法检查 (%s)" % (d["table"], d["error"]))
                continue
            mark = "" if d["status"] == "ok" else "  <<< %s" % d["status"]
            print("    %-20s %d -> %d (delta %+d)  max %s -> %s%s" % (
                d["table"], d["before_count"], d["after_count"],
                d["count_delta"], d["old_max"], d["new_max"], mark))
            if d.get("detail"):
                print("        %s" % d["detail"])
        if c.get("note"):
            print("  说明: %s" % c["note"])

        # Overall
        print("\n" + "=" * 60)
        print("RPO Result: %s" % ("PASS - No data loss" if result["rpo_success"] else "FAIL - Data loss detected!"))
        if result.get("summary"):
            s = result["summary"]
            print("  Markers lost: %d" % s["markers_lost"])
            print("  Rows lost:    %d" % s["rows_lost"])

    if args.teardown:
        rpo.teardown()
        logger.info("RPO tracking tables dropped")

    return 0 if result["rpo_success"] else 1


def cmd_gen_data(args, config, pool):
    """Generate test data on a node."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    gen = DataGenerator(pool, args.node, schema)

    if args.teardown:
        gen.teardown()
        logger.info("Test data tables dropped")
        return 0

    if args.truncate:
        gen.truncate_all()

    gen.generate_all(
        accounts=args.accounts,
        products=args.products,
        orders=args.orders,
        transactions=args.transactions,
    )
    print("Test data generated on node '%s'" % args.node)
    return 0


def cmd_run_workload(args, config, pool):
    """Run a custom workload from YAML file."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    runner = WorkloadRunner(pool, args.node, schema)
    print("Running workload: %s" % args.workload)
    stats = runner.run_file(args.workload)

    if args.format == "json":
        print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))
    else:
        print("\nWorkload Results:")
        print("  Total ops:    %d" % stats["total_ops"])
        print("  Success ops:  %d" % stats["success_ops"])
        print("  Error ops:    %d" % stats["error_ops"])
        print("  Elapsed:      %ss" % stats["elapsed_seconds"])
        print("  TPS:          %.2f" % stats["tps"])

        if stats["errors"]:
            print("\n  Errors (sample):")
            for e in stats["errors"][:5]:
                print("    [%s] %s" % (e["txn"], e["error"]))

    return 0 if stats["error_ops"] == 0 else 1


def cmd_schema_compare(args, config, pool):
    """Compare database SCHEMAS (DDL) between two nodes via the pgdiff tool.

    Separate from node-compare on purpose: pgdiff reports differences in
    table definitions, columns, indexes and constraints. Folding it into
    --backend would make a schema difference read like a data difference.
    """
    logger = setup_logger(config)
    from backends.pgdiff import run_pgdiff

    schemas = ([x.strip() for x in args.schemas.split(",") if x.strip()]
               if args.schemas else None)
    result = run_pgdiff(pool, config, args.node_a, args.node_b,
                        schemas=schemas, schema_type=args.schema_type)

    # 0 = 一致, 1 = 存在差异, 2 = 未能完成（工具不可用等）
    if result.get("identical") is True:
        exit_code = 0
    elif result.get("identical") is False:
        exit_code = 1
    else:
        exit_code = 2

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return exit_code

    if not result.get("success"):
        print("Schema 对比失败: %s" % result.get("error"))
        return exit_code

    print("Schema 对比: %s vs %s" % (args.node_a, args.node_b))
    print("  对比对象: %s" % result.get("schema_type", "ALL"))
    if result.get("schemas"):
        print("  schema:   %s" % ", ".join(result["schemas"]))
    else:
        print("  schema:   全部非系统 schema")
    if result.get("identical"):
        print("  结论:     两侧 schema 一致")
    else:
        print("  结论:     存在差异（pgdiff 生成 %d 条 DDL 语句）"
              % result.get("diff_count"))
        print()
        print("  以下 SQL 表示让第二个库追上第一个库需要执行的语句：")
        for line in (result.get("details") or [])[:80]:
            print("    %s" % line)

    return exit_code


def cmd_list_backends(args, config, pool):
    """List available comparison backends."""
    backends = list_backends()
    groups = (("数据对比引擎  (node-compare --backend)", "data"),
              ("结构对比引擎  (schema-compare)", "schema"))
    for title, kind in groups:
        items = [(n, i) for n, i in backends.items() if i.get("kind") == kind]
        if not items:
            continue
        print("%s:" % title)
        for name, info in items:
            status = "available" if info["available"] else "NOT INSTALLED"
            extra = "" if info.get("multi_node") else "  [仅两节点]"
            print("  %-16s %s%s" % (name, status, extra))
            print("      %s" % info["description"])
        print()
    return 0


class _Tee(object):
    """Write to the terminal and to a buffer at the same time.

    The scheduler needs the subcommand's output to build its report, but the
    user still wants to watch the run live.
    """

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            try:
                s.write(data)
            except Exception:
                pass

    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass


_VOLATILE_KEYS = ("total_time", "elapsed_seconds", "timestamp", "checked_at")


def _extract_payload(captured):
    """Pull a JSON object out of a round's output, if there is one."""
    start = captured.find("{")
    if start < 0:
        return None
    try:
        return json.loads(captured[start:])
    except ValueError:
        return None


def _round_fingerprint(exit_code, captured):
    """What counts as "the same result as last round".

    Falling back to the exit code alone would make every pair of distinct
    failures look identical -- two different tables diverging both exit 1 --
    and the convergence streak would advance on a false premise. So when
    there is no JSON payload, the text output is used instead, with the
    volatile lines (timestamps, durations) removed.
    """
    payload = _extract_payload(captured)
    if payload is not None:
        for key in _VOLATILE_KEYS:
            payload.pop(key, None)
        # The consistency assessment is deliberately excluded: it describes
        # the observation conditions of this round, not the data.
        payload.pop("consistency", None)
        return (exit_code, json.dumps(payload, sort_keys=True, default=str))

    cleaned = []
    for line in captured.splitlines():
        stripped = line.strip()
        if not stripped or "Total time" in stripped:
            continue
        if "Next run in" in stripped or "Scheduled run" in stripped:
            continue
        cleaned.append(stripped)
    return (exit_code, "\n".join(cleaned))


def cmd_schedule(args, config, pool):
    """Run a subcommand repeatedly and record what each round concluded."""
    logger = setup_logger(config)
    sched_cfg = config.get("schedule", {}) or {}

    interval = args.interval if args.interval is not None else sched_cfg.get("interval")
    interval = int(interval or 0)
    report_dir = args.report_dir or sched_cfg.get("report_dir") or "./reports"
    converge = int(args.converge or 0)
    max_rounds = int(args.rounds or 0)

    if interval <= 0:
        print("Error: 需要 --interval（或在 config 的 schedule.interval 中设置）为正整数秒")
        return 1

    sub_args = list(args.sub_args)

    # Strip leading '--' if present (argparse REMAINDER passes it through)
    if sub_args and sub_args[0] == "--":
        sub_args = sub_args[1:]

    if not sub_args:
        print("Error: Must specify a subcommand after --, e.g.:")
        print("  python cli.py -c config.yaml schedule --interval 300 -- node-compare --nodes node1,node2,node3")
        return 1


    if report_dir:
        try:
            os.makedirs(report_dir, exist_ok=True)
        except OSError as e:
            logger.warning("无法创建报告目录 %s: %s", report_dir, e)
            report_dir = None

    logger.info("Scheduled mode: running '%s' every %d seconds", " ".join(sub_args), interval)
    if report_dir:
        logger.info("每轮报告写入 %s", report_dir)
    if converge:
        logger.info("收敛判定：连续 %d 轮结果一致即判定收敛", converge)

    iteration = 0
    running = True
    streak = 0
    prev_fp = None
    history = []
    final_record = None

    def _handle_signal(sig, frame):
        nonlocal running
        logger.info("Received signal %s, stopping scheduler...", sig)
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    while running:
        iteration += 1
        logger.info("=== Scheduled run #%d ===", iteration)
        started = time.time()
        exit_code = None
        captured = ""

        try:
            parser = _build_parser()
            full_argv = []
            if args.config:
                full_argv.extend(["-c", args.config])
            if args.schema:
                full_argv.extend(["--schema", args.schema])
            full_argv.extend(sub_args)

            parsed = parser.parse_args(full_argv)
            cmd_map = _build_cmd_map()
            cmd_func = cmd_map.get(parsed.command.replace("-", "_"))
            if not cmd_func:
                logger.error("Scheduled run #%d: unknown command '%s'", iteration, parsed.command)
                break

            buf = io.StringIO()
            with contextlib.redirect_stdout(_Tee(sys.stdout, buf)):
                exit_code = cmd_func(parsed, config, pool)
            captured = buf.getvalue()
            logger.info("Scheduled run #%d completed (exit=%s)", iteration, exit_code)
        except SystemExit:
            logger.warning("Scheduled run #%d: subcommand parser exited", iteration)
            exit_code = 2
        except Exception as e:
            logger.error("Scheduled run #%d failed: %s", iteration, e)
            exit_code = 2

        elapsed = time.time() - started
        fp = _round_fingerprint(exit_code, captured)
        streak = streak + 1 if fp == prev_fp else 1
        prev_fp = fp

        # A round that ran longer than the interval means the next one starts
        # immediately, so consecutive rounds can overlap in their coverage.
        # Saying so is better than silently measuring a window that moved.
        overran = elapsed > interval
        if overran:
            logger.warning("第 %d 轮耗时 %.1fs 超过间隔 %ds —— 本轮与下一轮之间没有留白",
                           iteration, elapsed, interval)

        record = {
            "round": iteration,
            "timestamp": datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed, 2),
            "exit_code": exit_code,
            "overran_interval": overran,
            "identical_streak": streak,
            "converged": bool(converge and streak >= converge),
        }
        # Attach the parsed result to the record itself, not to a copy of
        # it: the summary keeps references to these dicts, so anything added
        # later to a copy would be missing from the summary.
        parsed = _extract_payload(captured)
        if parsed is not None:
            record["result"] = parsed
        history.append(record)
        final_record = record

        if report_dir:
            _write_round_report(report_dir, iteration, record, captured)
            _write_summary(report_dir, sub_args, interval, converge, history)

        if record["converged"]:
            logger.info("连续 %d 轮结果一致，判定收敛。", streak)
            running = False

        if max_rounds and iteration >= max_rounds:
            logger.info("已达到 --rounds %d，停止。", max_rounds)
            running = False

        if running:
            remaining = interval - elapsed
            if remaining > 0:
                logger.info("Next run in %.1f seconds...", remaining)
                time.sleep(remaining)
            else:
                logger.info("间隔已耗尽，立即开始下一轮")

    logger.info("Scheduler stopped after %d iterations", iteration)
    if converge and final_record and not final_record["converged"]:
        logger.warning("调度结束时仍未连续 %d 轮结果一致（当前连续 %d 轮）——"
                       "结果在轮次之间发生变化，说明数据仍在校验过程中变动",
                       converge, streak)
    return 0 if (final_record and final_record["exit_code"] == 0) else 1


def _write_round_report(report_dir, iteration, record, captured):
    """Persist one round's outcome next to its raw output."""
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = dict(record)
    payload["raw_output"] = captured
    path = os.path.join(report_dir, "round_%04d_%s.json" % (iteration, stamp))
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
        record["report"] = path
    except OSError as e:
        logging.getLogger("comparator").warning("写入回合报告失败: %s", e)


def _write_summary(report_dir, sub_args, interval, converge, history):
    """Maintain a rolling summary so the whole run can be reviewed at once."""
    ok = sum(1 for h in history if h["exit_code"] == 0)
    bad = sum(1 for h in history if h["exit_code"] == 1)
    unknown = sum(1 for h in history if h["exit_code"] not in (0, 1))
    summary = {
        "command": " ".join(sub_args),
        "interval_seconds": interval,
        "converge_target": converge or None,
        "rounds": len(history),
        "rounds_passed": ok,
        "rounds_failed": bad,
        "rounds_inconclusive": unknown,
        "rounds_overran_interval": sum(1 for h in history if h["overran_interval"]),
        "current_identical_streak": history[-1]["identical_streak"] if history else 0,
        "last_result": history[-1] if history else None,
        "history": history,
    }
    path = os.path.join(report_dir, "summary.json")
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=2, default=str)
    except OSError as e:
        logging.getLogger("comparator").warning("写入汇总报告失败: %s", e)
    return 0


def cmd_init_config(args, config, pool):
    """Generate a config file template."""
    path = args.path or "config.yaml"
    if os.path.exists(path) and not args.force:
        print("Config file already exists: %s" % path)
        print("Use --force to overwrite")
        return 1
    generate_template(path)
    print("Config template written to: %s" % path)
    print("Edit the file to configure your database nodes, then run:")
    print("  python cli.py -c %s node-compare -a node1 -b node2" % path)
    return 0


def _validate_node_args(args, config):
    """Turn a mistyped node name into a usable message.

    Without this, `-a nodeX` surfaces as a bare KeyError from deep inside
    the pool, with a traceback and no hint about which names are valid.
    """
    known = set(config.get("databases", {}) or {})
    used = []
    for attr in ("node", "node_a", "node_b"):
        value = getattr(args, attr, None)
        if value:
            used.append(value)
    nodes = getattr(args, "nodes", None)
    if nodes:
        used.extend(n.strip() for n in nodes.split(",") if n.strip())

    missing = [n for n in used if n not in known]
    if missing:
        raise ValueError(
            "节点 %s 未在配置中定义。可用节点: %s"
            % (", ".join("'%s'" % n for n in missing),
               ", ".join(sorted(known)) or "(配置中没有任何节点)"))


def _build_cmd_map():
    """Build mapping of command names to functions."""
    return {
        "node_compare": cmd_node_compare,
        "node-compare": cmd_node_compare,
        "app_db_setup": cmd_app_db_setup,
        "app-db-setup": cmd_app_db_setup,
        "app_db_verify": cmd_app_db_verify,
        "app-db-verify": cmd_app_db_verify,
        "app_db_status": cmd_app_db_status,
        "app-db-status": cmd_app_db_status,
        "app_db_teardown": cmd_app_db_teardown,
        "app-db-teardown": cmd_app_db_teardown,
        "rpo_plant": cmd_rpo_plant,
        "rpo-plant": cmd_rpo_plant,
        "jtl_reconcile": cmd_jtl_reconcile,
        "jtl-reconcile": cmd_jtl_reconcile,
        "rpo_check": cmd_rpo_check,
        "rpo-check": cmd_rpo_check,
        "gen_data": cmd_gen_data,
        "gen-data": cmd_gen_data,
        "run_workload": cmd_run_workload,
        "run-workload": cmd_run_workload,
        "schema_compare": cmd_schema_compare,
        "schema-compare": cmd_schema_compare,
        "list_backends": cmd_list_backends,
        "list-backends": cmd_list_backends,
        "schedule": cmd_schedule,
        "init_config": cmd_init_config,
        "init-config": cmd_init_config,
    }


def _build_parser():
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        description="Database Consistency Comparator - cluster data verification tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare two nodes with checksum method
  python cli.py -c config.yaml node-compare -a node1 -b node2

  # Compare with native EXCEPT method, specific tables
  python cli.py -c config.yaml node-compare -a node1 -b node2 --backend native -t accounts,orders

  # JMeter: setup tracking, then JMeter calls sp_track_write(), then verify
  python cli.py -c config.yaml app-db-setup -n node1
  python cli.py -c config.yaml app-db-verify -n node1 --batch-id myrun001

  # RPO: plant markers before fault
  python cli.py -c config.yaml rpo-plant -n node1

  # RPO: check after fault recovery
  python cli.py -c config.yaml rpo-check -n node1 -bid abc123

  # Generate test data
  python cli.py -c config.yaml gen-data -n node1 --accounts 5000

  # Run a custom workload
  python cli.py -c config.yaml run-workload -n node1 -w workload/templates/transfer.yaml

  # Schedule periodic comparison (every 5 minutes)
  python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2

  # Generate a config template
  python cli.py init-config
        """,
    )

    parser.add_argument("--version", action="version",
                        version="comparator %s" % __version__)
    parser.add_argument("-c", "--config", default="config.yaml",
                        help="Path to YAML config file (default: config.yaml)")
    parser.add_argument("--schema", default=None,
                        help="Database schema (overrides config)")
    parser.add_argument("-f", "--format", choices=["table", "json"],
                        default="table", help="Output format")

    # The same three options are accepted after the subcommand as well.
    # USAGE.md documents `node-compare -a node1 -b node2 -f json`, and
    # argparse would otherwise reject -f there because it was declared only
    # on the top-level parser.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-c", "--config", default=argparse.SUPPRESS)
    common.add_argument("--schema", default=argparse.SUPPRESS)
    common.add_argument("-f", "--format", choices=["table", "json"],
                        default=argparse.SUPPRESS)

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # node-compare
    p_cmp = sub.add_parser("node-compare", parents=[common], help="Compare data between database nodes")
    p_cmp.add_argument("-a", "--node-a", default=None, help="Source node name (from config)")
    p_cmp.add_argument("-b", "--node-b", default=None, help="Target node name (from config)")
    p_cmp.add_argument("--nodes", default=None,
                       help="逗号分隔的节点列表，做任意多节点矩阵对比，例如 node1,node2,node3")
    p_cmp.add_argument("--primary", default=None,
                       help="以指定节点为基准逐一对其他节点，替代多数派表决")
    p_cmp.add_argument("--backend", default="checksum",
                       choices=["checksum", "native", "pg_comparator", "data_diff"],
                       help="Comparison backend (default: checksum)")
    p_cmp.add_argument("-t", "--tables", default=None,
                       help="Comma-separated list of tables (default: all user tables)")
    # Scope overrides -- these narrow the set further and still apply when
    # -t names a table explicitly.
    p_cmp.add_argument("--exclude-tables", default=None,
                       help="额外排除的表，支持通配符（如 tmp_*,*_bak）")
    p_cmp.add_argument("--exclude-schemas", default=None,
                       help="额外排除的 schema，支持通配符")
    p_cmp.add_argument("--exclude-databases", default=None,
                       help="额外排除的 database，支持通配符")
    p_cmp.add_argument("--watermark", default=None,
                       help="水位线：只比较主键不超过该值的行（需单列主键）")
    p_cmp.add_argument("--wait-quiesce", type=int, default=None,
                       help="校验前等待连续 N 秒无数据变更，再开始比较（默认 5）")
    p_cmp.add_argument("--consistency-mode", default=None,
                       choices=["auto", "quiesce", "watermark", "loose"],
                       help="一致性判定方式（默认 auto，按环境能力自动选择）")
    p_cmp.add_argument("--no-consistency", action="store_true",
                       help="跳过一致性判定，直接比较（输出不含置信级别）")

    # app-db-setup — JMeter integration: create tracking infrastructure
    p_adbs = sub.add_parser("app-db-setup", parents=[common],
        help="Setup tracking table + stored function for JMeter")
    p_adbs.add_argument("-n", "--node", required=True, help="Database node name")

    # app-db-verify — JMeter integration: verify tracked writes
    p_adbv = sub.add_parser("app-db-verify", parents=[common],
        help="Verify JMeter-tracked writes against actual data")
    p_adbv.add_argument("-n", "--node", required=True, help="Database node name")
    p_adbv.add_argument("--batch-id", default=None,
                        help="Verify specific batch ID (default: all unverified)")
    p_adbv.add_argument("--no-mark", action="store_true",
                        help="Do not mark verified records (re-verify next time)")

    # app-db-status — show batch summaries
    p_adbst = sub.add_parser("app-db-status", parents=[common],
        help="Show summary of all tracking batches")
    p_adbst.add_argument("-n", "--node", required=True, help="Database node name")

    # app-db-teardown — cleanup
    p_adbt = sub.add_parser("app-db-teardown", parents=[common],
        help="Remove tracking table and stored function")
    p_adbt.add_argument("-n", "--node", required=True, help="Database node name")

    # jtl-reconcile — JMeter 结果文件对账
    p_jr = sub.add_parser("jtl-reconcile", parents=[common],
        help="把 JMeter 的 .jtl 结果与数据库实际状态对账")
    p_jr.add_argument("-n", "--node", required=True, help="Database node name")
    p_jr.add_argument("--jtl", required=True, help="JMeter 结果文件路径 (.jtl/.csv)")
    p_jr.add_argument("--batch-id", default=None,
                      help="只对账指定批次的追踪记录（默认全部）")
    p_jr.add_argument("--request-id-column", default=None,
                      help="JTL 中请求标识的列名（默认自动识别 REQ_ID 等）")
    p_jr.add_argument("--no-orphans", action="store_true",
                      help="跳过「数据库中有、追踪表中没有」的反向扫描")

    # rpo-plant
    p_rp = sub.add_parser("rpo-plant", parents=[common], help="Plant RPO markers before fault event")
    p_rp.add_argument("-n", "--node", required=True, help="Database node name")
    p_rp.add_argument("-t", "--tables", default=None,
                      help="Comma-separated list of tables to track")
    p_rp.add_argument("--marker-count", type=int, default=10,
                      help="Number of markers per table (default: 10)")

    # rpo-check
    p_rc = sub.add_parser("rpo-check", parents=[common], help="Check RPO data loss after fault recovery")
    p_rc.add_argument("-n", "--node", required=True, help="Database node name")
    p_rc.add_argument("-bid", "--batch-id", required=True, help="Batch ID from rpo-plant")
    p_rc.add_argument("--teardown", action="store_true",
                      help="Drop RPO tracking tables after check")

    # gen-data
    p_gd = sub.add_parser("gen-data", parents=[common], help="Generate test data on a node")
    p_gd.add_argument("-n", "--node", required=True, help="Database node name")
    p_gd.add_argument("--accounts", type=int, default=1000)
    p_gd.add_argument("--products", type=int, default=200)
    p_gd.add_argument("--orders", type=int, default=5000)
    p_gd.add_argument("--transactions", type=int, default=10000)
    p_gd.add_argument("--teardown", action="store_true", help="Drop generated tables")
    p_gd.add_argument("--truncate", action="store_true", help="Truncate tables before generating")

    # run-workload
    p_rw = sub.add_parser("run-workload", parents=[common], help="Run custom workload from YAML file")
    p_rw.add_argument("-n", "--node", required=True, help="Database node name")
    p_rw.add_argument("-w", "--workload", required=True, help="Path to workload YAML file")

    # schema-compare — 表结构对比（pgdiff 外部工具）
    p_scp = sub.add_parser("schema-compare", parents=[common],
        help="对比两节点的 schema 差异（pgdiff 外部工具）")
    p_scp.add_argument("-a", "--node-a", required=True, help="Source node name")
    p_scp.add_argument("-b", "--node-b", required=True, help="Target node name")
    p_scp.add_argument("--schemas", default=None,
                       help="schema 名称（pgdiff 一次只对比一个；不填则对比全部非系统 schema）")
    p_scp.add_argument("--schema-type", default="ALL",
                       choices=list(SCHEMA_TYPES) + ["ALL"],
                       help="要对比的对象类型（默认 ALL，按依赖顺序全部对比）")

    # list-backends
    sub.add_parser("list-backends", parents=[common], help="List available comparison backends")

    # schedule
    p_sc = sub.add_parser("schedule", parents=[common], help="Run a subcommand on a schedule")
    p_sc.add_argument("--interval", type=int, default=None,
                      help="巡检间隔秒数（默认取 config 的 schedule.interval）")
    p_sc.add_argument("--report-dir", default=None,
                      help="每轮报告的存放目录（默认取 config 的 schedule.report_dir）")
    p_sc.add_argument("--converge", type=int, default=0,
                      help="连续 N 轮结果一致即判定收敛并停止（0=不启用）")
    p_sc.add_argument("--rounds", type=int, default=0,
                      help="最多执行 N 轮后停止（0=不限）")
    p_sc.add_argument("sub_args", nargs=argparse.REMAINDER,
                      help="Subcommand and its arguments (after --)")

    # init-config
    p_ic = sub.add_parser("init-config", parents=[common], help="Generate a config file template")
    p_ic.add_argument("--path", default="config.yaml", help="Output path")
    p_ic.add_argument("--force", action="store_true", help="Overwrite existing file")

    return parser


def main():
    parser = _build_parser()

    # Handle schedule subcommand specially (it has its own sub-args)
    if "--" in sys.argv:
        # Allow: python cli.py -c cfg schedule --interval 300 -- node-compare ...
        pass

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    cmd_map = _build_cmd_map()
    cmd_func = cmd_map.get(args.command.replace("-", "_"))

    if not cmd_func:
        print("Unknown command: %s" % args.command)
        return 1

    # Load config for commands that need it
    config = {}
    pool = None

    try:
        if args.command not in ("init-config",):
            config = load_config(args.config)
            _validate_node_args(args, config)
            pool = DatabasePool(config)

        return cmd_func(args, config, pool)
    except Exception as e:
        # An uncaught error means the command could not produce an answer;
        # that is exit 2 ("inconclusive"), not 1 ("the data differs").
        print("Error: %s" % e, file=sys.stderr)
        if not isinstance(e, (ValueError, FileNotFoundError)):
            import traceback
            traceback.print_exc()
        return 2
    finally:
        if pool:
            pool.close_all()


if __name__ == "__main__":
    sys.exit(main())
