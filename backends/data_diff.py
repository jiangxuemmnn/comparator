"""Integration with the data-diff external tool.

data-diff: https://github.com/datafold/data-diff
Compares table CONTENTS, including across different database engines.
Install with the postgres adapter:

    pip install 'data-diff[postgresql]'

**Invocation shape.** data-diff takes the two tables as POSITIONAL arguments
interleaved with their connection URIs:

    data-diff <src_uri> <src_table> <tgt_uri> <tgt_table> [options]

There is no `--table` option; an earlier version of this module passed
`--table schema.name` and put the two URIs side by side, so data-diff would
have read the second URI as a table name. It also omitted `-k`, without
which data-diff cannot line rows up between the two sides -- it needs the
key column to decide what "the same row" means.

Because the table is part of the argument list, one invocation covers one
table. That is why this runs a subprocess per table rather than once for all
of them.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from urllib.parse import quote

logger = logging.getLogger("comparator.data_diff")


def check_data_diff_installed() -> bool:
    """True when the data-diff executable is on PATH.

    `shutil.which` is used rather than running `data-diff --version`: it is
    cheaper, and it distinguishes "not installed" from "installed but
    erroring on --version", which the returncode check conflates.
    """
    return shutil.which("data-diff") is not None


def _build_url(db_config: dict) -> str:
    """Build a data-diff connection URI.

    Credentials and the database name are percent-encoded. Without that, a
    password containing '@', ':' or '/' silently reshapes the URI into a
    different host or an unparsable string -- and the resulting error from
    the SQLAlchemy URL parser does not point back at the password.
    """
    user = quote(str(db_config["user"]), safe="")
    password = quote(str(db_config.get("password", "")), safe="")
    host = db_config["host"]
    port = db_config.get("port", 54321)
    dbname = quote(str(db_config["dbname"]), safe="")
    auth = "%s:%s@" % (user, password) if password else "%s@" % user
    return "postgresql://%s%s:%s/%s" % (auth, host, port, dbname)


# Lines data-diff emits when the two tables match.
_NO_DIFF = re.compile(r"no\s+(differences|rows)", re.IGNORECASE)


def run_data_diff(pool, config: dict, node_a: str, node_b: str,
                  tables=None, schema: str = None) -> dict:
    """Compare table contents between two nodes using data-diff.

    Returns dict with: success, identical, diff_count, output, details.
    `identical` is None when the comparison could not be run, so an
    unavailable tool is never mistaken for a detected difference.
    """
    if not check_data_diff_installed():
        return {
            "success": False,
            "identical": None,
            "error": ("data-diff 未安装或不在 PATH 中。安装方式：\n"
                      "  pip install 'data-diff[postgresql]'"),
            "output": "", "diff_count": -1, "details": [],
        }

    db_cfg = config["databases"]
    cmp_cfg = config.get("comparison", {}) or {}
    schema = schema or cmp_cfg.get("schema", "public")
    tables = list(tables or cmp_cfg.get("tables") or [])
    if not tables:
        return {
            "success": False, "identical": None,
            "error": "未指定要对比的表。data-diff 每次只对比一张表，"
                     "请用 -t 指定表名。",
            "output": "", "diff_count": -1, "details": [],
        }

    try:
        url_a = _build_url(db_cfg[node_a])
        url_b = _build_url(db_cfg[node_b])
    except KeyError as e:
        return {
            "success": False, "identical": None,
            "error": "节点配置缺少字段: %s" % e,
            "output": "", "diff_count": -1, "details": [],
        }

    per_table = []
    skipped = []
    total_diffs = 0
    all_output = []

    for table in tables:
        # data-diff cannot align rows without a key.
        try:
            pk_cols = pool.get_primary_key(node_a, schema, table)
        except Exception as e:
            skipped.append({"table": table, "reason": "读取主键失败: %s" % e})
            continue
        if not pk_cols:
            skipped.append({
                "table": table,
                "reason": "该表无主键，data-diff 无法对齐行；请改用 checksum 引擎",
            })
            continue

        qualified_table = "%s.%s" % (schema, table)
        cmd = ["data-diff",
               url_a, qualified_table,
               url_b, qualified_table,
               "-k", pk_cols[0]]
        safe_cmd = [c.replace(url_a, url_a.split("@")[-1]).replace(
            url_b, url_b.split("@")[-1]) for c in cmd]
        logger.info("Running data-diff: %s", " ".join(safe_cmd))

        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=600)
        except subprocess.TimeoutExpired:
            per_table.append({"table": table, "error": "超时（600s）"})
            skipped.append({"table": table, "reason": "执行超时"})
            continue
        except Exception as e:
            per_table.append({"table": table, "error": str(e)})
            skipped.append({"table": table, "reason": str(e)})
            continue

        output = (result.stdout or "") + (result.stderr or "")
        all_output.append("### %s\n%s" % (qualified_table, output))

        if _NO_DIFF.search(output):
            identical = True
            rows = 0
        elif result.returncode != 0:
            identical = None
            rows = -1
            skipped.append({
                "table": table,
                "reason": "data-diff 退出码 %d: %s" % (
                    result.returncode,
                    (result.stderr or "").strip().splitlines()[-1][:160]
                    if result.stderr else ""),
            })
        else:
            identical = False
            # data-diff prints one line per differing row (or a summary);
            # count the non-empty, non-header lines.
            rows = sum(1 for ln in output.splitlines()
                       if ln.strip() and not ln.startswith(("INFO", "WARNING")))
            total_diffs += rows

        per_table.append({
            "table": qualified_table,
            "key": pk_cols[0],
            "identical": identical,
            "diff_rows": rows,
        })

    ran = [t for t in per_table if t.get("identical") is not None]
    if not ran:
        return {
            "success": False, "identical": None,
            "error": "没有任何表完成对比：%s" % "; ".join(
                "%s(%s)" % (s["table"], s["reason"]) for s in skipped),
            "output": "\n".join(all_output),
            "diff_count": -1, "details": [], "skipped": skipped,
        }

    identical = all(t["identical"] for t in ran)
    return {
        "success": True,
        "identical": identical,
        "diff_count": total_diffs,
        "tables_compared": len(ran),
        "per_table": per_table,
        "skipped": skipped,
        "output": "\n".join(all_output),
        "details": [ln for ln in "\n".join(all_output).splitlines()
                    if ln.strip()][-100:],
    }
