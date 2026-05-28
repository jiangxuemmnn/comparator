"""Integration with pg_comparator external tool.

pg_comparator: https://github.com/credativ/pg_comparator
Supports PostgreSQL / Kingbase.

Requires pg_comparator to be installed on the system.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
import os

logger = logging.getLogger("comparator.pg_comparator")


def _build_pg_comparator_config(node_a: dict, node_b: dict,
                                schema: str, tables: list[str]) -> str:
    """Build pg_comparator config YAML content."""
    lines = [
        "---",
        "source:",
        "  host: %s" % node_a["host"],
        "  port: %d" % node_a.get("port", 54321),
        "  dbname: %s" % node_a["dbname"],
        "  user: %s" % node_a["user"],
        "  password: %s" % node_a.get("password", ""),
        "target:",
        "  host: %s" % node_b["host"],
        "  port: %d" % node_b.get("port", 54321),
        "  dbname: %s" % node_b["dbname"],
        "  user: %s" % node_b["user"],
        "  password: %s" % node_b.get("password", ""),
        "schema: %s" % schema,
    ]
    if tables:
        lines.append("tables:")
        for t in tables:
            lines.append("  - %s" % t)
    return "\n".join(lines)


def check_pg_comparator_installed() -> bool:
    """Check if pg_comparator is installed."""
    try:
        result = subprocess.run(
            ["pg_comparator", "--version"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except Exception:
        return False


def run_pg_comparator(config: dict, node_a: str, node_b: str,
                      tables: list[str] = None) -> dict:
    """Run pg_comparator to compare two database nodes.

    Returns dict with: success, output, diff_count, details.
    """
    if not check_pg_comparator_installed():
        return {
            "success": False,
            "error": "pg_comparator is not installed or not in PATH",
            "output": "",
            "diff_count": -1,
        }

    db_cfg = config["databases"]
    cmp_cfg = config.get("comparison", {})
    schema = cmp_cfg.get("schema", "public")
    tables = tables or cmp_cfg.get("tables", [])

    cfg_content = _build_pg_comparator_config(
        db_cfg[node_a], db_cfg[node_b], schema, tables
    )

    # Write temp config
    fd, cfg_path = tempfile.mkstemp(suffix=".yml", prefix="pgcmp_")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(cfg_content)

        logger.info("Running pg_comparator with config: %s", cfg_path)
        result = subprocess.run(
            ["pg_comparator", "-c", cfg_path],
            capture_output=True, text=True, timeout=300,
        )
        output = result.stdout + result.stderr

        # Parse for diff count
        diff_count = 0
        for line in output.splitlines():
            if "DIFF" in line or "diff" in line.lower():
                diff_count += 1

        return {
            "success": result.returncode == 0,
            "output": output,
            "diff_count": diff_count,
            "details": output.splitlines()[-50:],
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": "pg_comparator timed out after 300s",
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
        if os.path.exists(cfg_path):
            os.unlink(cfg_path)
