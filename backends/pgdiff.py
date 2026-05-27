"""Integration with pgdiff external tool.

pgdiff: https://github.com/joncrlsn/pgdiff
Compares PostgreSQL database schemas and data.

Requires pgdiff to be installed on the system (Java-based tool).
"""

import logging
import subprocess
import tempfile
import os

logger = logging.getLogger("comparator.pgdiff")


def _dump_schema(pool, name: str, schema: str, output_path: str):
    """Dump schema via pg_dump to file."""
    conn = pool.get_conn(name)
    dsn = conn.get_dsn_parameters()
    cmd = [
        "pg_dump",
        "-h", dsn.get("host", "localhost"),
        "-p", dsn.get("port", "54321"),
        "-U", dsn.get("user", ""),
        "-d", dsn["dbname"],
        "-n", schema,
        "--schema-only",
        "-f", output_path,
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = dsn.get("password", "")
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=60)
    if result.returncode != 0:
        raise RuntimeError("pg_dump schema failed: %s" % result.stderr)


def check_pgdiff_installed() -> bool:
    """Check if pgdiff is installed."""
    try:
        result = subprocess.run(
            ["pgdiff", "--version"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except Exception:
        return False


def run_pgdiff(pool, config: dict, node_a: str, node_b: str) -> dict:
    """Run pgdiff to compare schemas between two nodes.

    Returns dict with: success, output, diff_count, details.
    """
    if not check_pgdiff_installed():
        return {
            "success": False,
            "error": "pgdiff is not installed or not in PATH",
            "output": "",
            "diff_count": -1,
        }

    cmp_cfg = config.get("comparison", {})
    schema = cmp_cfg.get("schema", "public")

    tmp_a = tempfile.mktemp(suffix=".sql", prefix="schema_a_")
    tmp_b = tempfile.mktemp(suffix=".sql", prefix="schema_b_")

    try:
        logger.info("Dumping schema from node %s ...", node_a)
        _dump_schema(pool, node_a, schema, tmp_a)

        logger.info("Dumping schema from node %s ...", node_b)
        _dump_schema(pool, node_b, schema, tmp_b)

        logger.info("Running pgdiff ...")
        result = subprocess.run(
            ["pgdiff", tmp_a, tmp_b],
            capture_output=True, text=True, timeout=120,
        )
        output = result.stdout

        diff_count = output.count("ALTER") + output.count("CREATE") + output.count("DROP")

        return {
            "success": True,
            "output": output,
            "diff_count": diff_count,
            "details": output.splitlines(),
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": "pgdiff timed out",
            "output": "",
            "diff_count": -1,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "output": "",
            "diff_count": -1,
        }
    finally:
        for p in [tmp_a, tmp_b]:
            if os.path.exists(p):
                os.unlink(p)
