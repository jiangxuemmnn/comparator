"""Integration with data-diff external tool.

data-diff: https://github.com/datafold/data-diff
Cross-database data comparison tool supporting PostgreSQL and others.

Requires data-diff to be installed (pip install data-diff).
"""

from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger("comparator.data_diff")


def check_data_diff_installed() -> bool:
    """Check if data-diff is installed."""
    try:
        result = subprocess.run(
            ["data-diff", "--version"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except Exception:
        return False


def _build_data_diff_url(db_config: dict, schema: str) -> str:
    """Build a data-diff database URL from config."""
    host = db_config["host"]
    port = db_config.get("port", 54321)
    dbname = db_config["dbname"]
    user = db_config["user"]
    password = db_config.get("password", "")

    # data-diff URL format: postgresql://user:pass@host:port/dbname
    return "postgresql://%s:%s@%s:%d/%s" % (user, password, host, port, dbname)


def run_data_diff(config: dict, node_a: str, node_b: str,
                  tables: list[str] = None) -> dict:
    """Run data-diff to compare tables between two database nodes.

    Returns dict with: success, output, diff_count, details.
    """
    if not check_data_diff_installed():
        return {
            "success": False,
            "error": "data-diff is not installed (pip install data-diff)",
            "output": "",
            "diff_count": -1,
        }

    db_cfg = config["databases"]
    cmp_cfg = config.get("comparison", {})
    schema = cmp_cfg.get("schema", "public")
    tables = tables or cmp_cfg.get("tables", [])

    url_a = _build_data_diff_url(db_cfg[node_a], schema)
    url_b = _build_data_diff_url(db_cfg[node_b], schema)

    cmd = ["data-diff", url_a, url_b]

    if tables:
        for t in tables:
            cmd.extend(["--table", "%s.%s" % (schema, t)])

    logger.info("Running data-diff: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=600,
        )
        output = result.stdout + result.stderr

        diff_count = 0
        for line in output.splitlines():
            if "diff" in line.lower() or "mismatch" in line.lower():
                diff_count += 1

        return {
            "success": result.returncode == 0,
            "output": output,
            "diff_count": diff_count,
            "details": output.splitlines()[-100:],
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": "data-diff timed out after 600s",
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
