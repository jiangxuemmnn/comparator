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
import sys
import os
import time
import signal
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config import load_config, generate_template
from utils.logger import setup_logger
from utils.db import DatabasePool
from core.node_comparator import run_node_comparison, list_backends
from core.app_db_comparator import (
    AppDBTracker, setup_tracking, teardown_tracking,
    verify_batch, get_batch_summary,
)
from core.rpo_checker import RPOChecker
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

    # Summary
    print("Summary:")
    print("  Backend:      %s" % result.get("backend", "N/A"))
    print("  Tables checked: %s" % result.get("tables_checked", 0))
    print("  Tables diff:   %s" % result.get("tables_diff", 0))
    print("  Total time:    %ss" % result.get("total_time", "N/A"))
    print("  Result:        %s" % ("PASS" if result.get("success") else "FAIL"))

    # Diff details
    if result.get("tables_diff", 0) > 0 and "results" in result:
        print("\nDiff details:")
        for r in result["results"]:
            if not r.get("identical") and "diff_details" in r:
                print("  [%s]" % r.get("table"))
                for d in r["diff_details"][:10]:
                    print("    - %s" % d)


def cmd_node_compare(args, config, pool):
    """Compare data between two database nodes."""
    logger = setup_logger(config)
    logger.info("Node comparison: %s vs %s, backend=%s",
                args.node_a, args.node_b, args.backend)

    tables = args.tables.split(",") if args.tables else None

    result = run_node_comparison(
        pool, config,
        node_a=args.node_a,
        node_b=args.node_b,
        backend=args.backend,
        tables=tables,
    )
    _print_result(result, args.format)
    return 0 if result["success"] else 1


def cmd_app_db_setup(args, config, pool):
    """Create tracking table + stored function for JMeter integration.

    After running this, JMeter can call:
        SELECT public.sp_track_write('batch1', 'INSERT', 'accounts',
                                      '{"id":123}', '{"id":123,"bal":100}')
    """
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]
    setup_tracking(pool, args.node, schema)
    print("Tracking infrastructure created on node '%s'" % args.node)
    print()
    print("JMeter JDBC PostProcessor SQL:")
    print("  SELECT %s.sp_track_write(" % schema)
    print("      '${batchId}',")
    print("      'INSERT',")
    print("      '<table_name>',")
    print("      '{\"<pk_col>\": ${<pk_var>}}',")
    print("      '{\"<col1>\": ${<var1>}, ...}'")
    print("  )")
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

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print("\nApp-DB Consistency Verification:")
        print("  Total operations tracked: %d" % result["total"])
        print("  Verified OK:             %d" % result["verified"])
        print("  Missing (data lost):     %d" % result["missing"])
        print("  Mismatch (wrong value):  %d" % result["mismatch"])
        print("  Result:  %s" % ("PASS" if result["success"] else "FAIL"))

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


def cmd_rpo_plant(args, config, pool):
    """Plant RPO markers before a fault event."""
    logger = setup_logger(config)
    schema = args.schema or config["comparison"]["schema"]

    rpo = RPOChecker(pool, args.node, schema)
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

    rpo = RPOChecker(pool, args.node, schema)
    result = rpo.check_all(args.batch_id)

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print("\nRPO Check Results (batch=%s):" % args.batch_id)
        print("=" * 60)

        # Markers
        m = result["markers"]
        print("\n[Marker Check]")
        print("  Total:  %d" % m["total"])
        print("  Found:  %d" % m["found"])
        print("  Missing: %d" % m["missing"])
        if m["missing_details"]:
            for d in m["missing_details"][:10]:
                print("    LOST: %s (table: %s)" % (d["marker_id"], d["table"]))

        # Row counts
        rc = result["row_counts"]
        print("\n[Row Count Check]")
        print("  Total before: %d" % rc["total_before"])
        print("  Total after:  %d" % rc["total_after"])
        print("  Delta:        %d" % rc["total_delta"])
        if rc["tables_with_loss"]:
            print("  Tables with data loss: %s" % ", ".join(rc["tables_with_loss"]))
            for d in rc["details"]:
                if d.get("status") == "DATA_LOSS":
                    print("    %s: %d rows lost" % (d["table"], -d["delta"]))

        # Gaps
        sg = result["sequence_gaps"]
        print("\n[Sequence Gap Check]")
        print("  Tables checked: %d" % sg["tables_checked"])
        print("  Gaps found:     %d" % sg["gaps_found"])
        for g in sg.get("gaps", []):
            print("    %s: %d missing IDs (sample: %s)" % (
                g["table"], g["missing_count"], g["missing_ids"][:5]
            ))

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


def cmd_list_backends(args, config, pool):
    """List available comparison backends."""
    backends = list_backends()
    print("Available backends:")
    for name, info in backends.items():
        status = "available" if info["available"] else "NOT INSTALLED"
        print("  %-20s %s" % (name, status))
        print("    %s" % info["description"])
    return 0


def cmd_schedule(args, config, pool):
    """Run in scheduled mode, executing a subcommand at intervals."""
    logger = setup_logger(config)
    interval = args.interval

    if interval <= 0:
        print("Error: --interval must be a positive number of seconds")
        return 1

    sub_args = list(args.sub_args)

    # Strip leading '--' if present (argparse REMAINDER passes it through)
    if sub_args and sub_args[0] == "--":
        sub_args = sub_args[1:]

    if not sub_args:
        print("Error: Must specify a subcommand after --, e.g.:")
        print("  python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2")
        return 1

    cmd_name = sub_args[0].replace("-", "_")

    logger.info("Scheduled mode: running '%s' every %d seconds",
                " ".join(sub_args), interval)

    iteration = 0
    running = True

    def _handle_signal(sig, frame):
        nonlocal running
        logger.info("Received signal %s, stopping scheduler...", sig)
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    while running:
        iteration += 1
        logger.info("=== Scheduled run #%d ===", iteration)

        try:
            parser = _build_parser()
            full_argv = []
            if args.config:
                full_argv.extend(["-c", args.config])
            if args.schema:
                full_argv.extend(["--schema", args.schema])
            full_argv.extend(sub_args)

            parsed = parser.parse_args(full_argv)
            exit_code = parsed.func(parsed, config, pool)
            logger.info("Scheduled run #%d completed (exit=%d)", iteration, exit_code)
        except SystemExit:
            logger.warning("Scheduled run #%d: subcommand parser exited", iteration)
        except Exception as e:
            logger.error("Scheduled run #%d failed: %s", iteration, e)

        if running and interval > 0:
            logger.info("Next run in %d seconds...", interval)
            time.sleep(interval)

    logger.info("Scheduler stopped after %d iterations", iteration)
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
        "rpo_check": cmd_rpo_check,
        "rpo-check": cmd_rpo_check,
        "gen_data": cmd_gen_data,
        "gen-data": cmd_gen_data,
        "run_workload": cmd_run_workload,
        "run-workload": cmd_run_workload,
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

    parser.add_argument("-c", "--config", default="config.yaml",
                        help="Path to YAML config file (default: config.yaml)")
    parser.add_argument("--schema", default=None,
                        help="Database schema (overrides config)")
    parser.add_argument("-f", "--format", choices=["table", "json"],
                        default="table", help="Output format")

    sub = parser.add_subparsers(dest="command", help="Available commands")

    # node-compare
    p_cmp = sub.add_parser("node-compare", help="Compare data between two database nodes")
    p_cmp.add_argument("-a", "--node-a", required=True, help="Source node name (from config)")
    p_cmp.add_argument("-b", "--node-b", required=True, help="Target node name (from config)")
    p_cmp.add_argument("--backend", default="checksum",
                       choices=["checksum", "native", "pg_comparator", "data_diff"],
                       help="Comparison backend (default: checksum)")
    p_cmp.add_argument("-t", "--tables", default=None,
                       help="Comma-separated list of tables (default: all user tables)")

    # app-db-setup — JMeter integration: create tracking infrastructure
    p_adbs = sub.add_parser("app-db-setup",
        help="Setup tracking table + stored function for JMeter")
    p_adbs.add_argument("-n", "--node", required=True, help="Database node name")

    # app-db-verify — JMeter integration: verify tracked writes
    p_adbv = sub.add_parser("app-db-verify",
        help="Verify JMeter-tracked writes against actual data")
    p_adbv.add_argument("-n", "--node", required=True, help="Database node name")
    p_adbv.add_argument("--batch-id", default=None,
                        help="Verify specific batch ID (default: all unverified)")
    p_adbv.add_argument("--no-mark", action="store_true",
                        help="Do not mark verified records (re-verify next time)")

    # app-db-status — show batch summaries
    p_adbst = sub.add_parser("app-db-status",
        help="Show summary of all tracking batches")
    p_adbst.add_argument("-n", "--node", required=True, help="Database node name")

    # app-db-teardown — cleanup
    p_adbt = sub.add_parser("app-db-teardown",
        help="Remove tracking table and stored function")
    p_adbt.add_argument("-n", "--node", required=True, help="Database node name")

    # rpo-plant
    p_rp = sub.add_parser("rpo-plant", help="Plant RPO markers before fault event")
    p_rp.add_argument("-n", "--node", required=True, help="Database node name")
    p_rp.add_argument("-t", "--tables", default=None,
                      help="Comma-separated list of tables to track")
    p_rp.add_argument("--marker-count", type=int, default=10,
                      help="Number of markers per table (default: 10)")

    # rpo-check
    p_rc = sub.add_parser("rpo-check", help="Check RPO data loss after fault recovery")
    p_rc.add_argument("-n", "--node", required=True, help="Database node name")
    p_rc.add_argument("-bid", "--batch-id", required=True, help="Batch ID from rpo-plant")
    p_rc.add_argument("--teardown", action="store_true",
                      help="Drop RPO tracking tables after check")

    # gen-data
    p_gd = sub.add_parser("gen-data", help="Generate test data on a node")
    p_gd.add_argument("-n", "--node", required=True, help="Database node name")
    p_gd.add_argument("--accounts", type=int, default=1000)
    p_gd.add_argument("--products", type=int, default=200)
    p_gd.add_argument("--orders", type=int, default=5000)
    p_gd.add_argument("--transactions", type=int, default=10000)
    p_gd.add_argument("--teardown", action="store_true", help="Drop generated tables")

    # run-workload
    p_rw = sub.add_parser("run-workload", help="Run custom workload from YAML file")
    p_rw.add_argument("-n", "--node", required=True, help="Database node name")
    p_rw.add_argument("-w", "--workload", required=True, help="Path to workload YAML file")

    # list-backends
    sub.add_parser("list-backends", help="List available comparison backends")

    # schedule
    p_sc = sub.add_parser("schedule", help="Run a subcommand on a schedule")
    p_sc.add_argument("--interval", type=int, required=True,
                      help="Interval in seconds between runs")
    p_sc.add_argument("sub_args", nargs=argparse.REMAINDER,
                      help="Subcommand and its arguments (after --)")

    # init-config
    p_ic = sub.add_parser("init-config", help="Generate a config file template")
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
            pool = DatabasePool(config)

        return cmd_func(args, config, pool)
    except Exception as e:
        print("Error: %s" % e, file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if pool:
            pool.close_all()


if __name__ == "__main__":
    sys.exit(main())
