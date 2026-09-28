"""Integration with the pg_comparator external tool.

pg_comparator: https://github.com/credativ/pg_comparator
Efficient table content comparison for PostgreSQL / Kingbase, by
Fabien Coelho. Install the distro package (`postgresql-comparator`) or
build from source.

**Invocation shape.** pg_comparator takes two POSITIONAL, URL-shaped
arguments describing the source and target table:

    [driver://][login[:pass]@][host][:port]/database/[[schema.]table[?key[:cols...]]]

    pg_comparator pgsql://user:pw@host1:5432/db/public.accounts?id \\
                   pgsql://user:pw@host2:5432/db/public.accounts?id

There is no `--config` option -- an earlier version of this module wrote a
YAML file with invented `source:`/`target:` keys and passed it via `-c`,
which pg_comparator has never accepted. Because the table is part of the
argument, one invocation covers one table.

**This module never passes `-S`/`--synchronize` or `-D`/`--do-it`.**
Together those make pg_comparator WRITE to the target database, which is
the opposite of what a verification tool should do. They are excluded here
deliberately; do not add them.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from urllib.parse import quote

logger = logging.getLogger("comparator.pg_comparator")

# Flags that would let the tool modify the target database. Listed so the
# exclusion is explicit and greppable rather than an absence.
_DESTRUCTIVE_FLAGS = ("-S", "--synchronize", "-D", "--do-it")


def check_pg_comparator_installed() -> bool:
    """True when the pg_comparator executable is on PATH."""
    return shutil.which("pg_comparator") is not None


def _build_url(db_config: dict, schema: str, table: str, key: str) -> str:
    """Build one side's URL-shaped argument.

    Format:
        [driver://][login[:pass]@][host][:port]/database/[[schema.]table[?key[:cols...]]]

    Credentials are percent-encoded: a password containing '@' or '/' would
    otherwise split the URL at the wrong place and point the comparison at a
    different host or database without any error being raised.
    """
    user = quote(str(db_config["user"]), safe="")
    password = quote(str(db_config.get("password", "")), safe="")
    host = db_config["host"]
    port = db_config.get("port", 54321)
    dbname = quote(str(db_config["dbname"]), safe="")
    auth = "%s:%s@" % (user, password) if password else "%s@" % user
    url = "pgsql://%s%s:%s/%s/%s.%s" % (auth, host, port, dbname, schema, table)
    if key:
        url += "?%s" % key
    return url


# pg_comparator reports differences in a few shapes depending on options;
# these are the ones that indicate the tables actually differ.
_DIFF_HINTS = (
    re.compile(r"\bDIFFER", re.IGNORECASE),
    re.compile(r"\bdiff(?:erence)?s?\b", re.IGNORECASE),
    re.compile(r"^\s*[<>]\s", re.MULTILINE),
)


def _looks_different(output: str) -> bool:
    return any(rx.search(output) for rx in _DIFF_HINTS)


def run_pg_comparator(pool, config: dict, node_a: str, node_b: str,
                      tables=None, schema: str = None) -> dict:
    """Compare table contents between two nodes using pg_comparator.

    Returns dict with: success, identical, diff_count, output, details.
    `identical` is None when the comparison could not be run, so an
    unavailable tool is never mistaken for a detected difference.
    """
    if not check_pg_comparator_installed():
        return {
            "success": False,
            "identical": None,
            "error": ("pg_comparator 未安装或不在 PATH 中。"
                      "Debian/Ubuntu 可装 postgresql-comparator 包，"
                      "或从 https://github.com/credativ/pg_comparator 构建。"),
            "output": "", "diff_count": -1, "details": [],
        }

    db_cfg = config["databases"]
    cmp_cfg = config.get("comparison", {}) or {}
    schema = schema or cmp_cfg.get("schema", "public")
    tables = list(tables or cmp_cfg.get("tables") or [])
    if not tables:
        return {
            "success": False, "identical": None,
            "error": "未指定要对比的表。pg_comparator 每次只对比一张表，"
                     "请用 -t 指定表名。",
            "output": "", "diff_count": -1, "details": [],
        }

    per_table = []
    skipped = []
    all_output = []

    for table in tables:
        try:
            pk_cols = pool.get_primary_key(node_a, schema, table)
        except Exception as e:
            skipped.append({"table": table, "reason": "读取主键失败: %s" % e})
            continue
        if not pk_cols:
            skipped.append({
                "table": table,
                "reason": "该表无主键；pg_comparator 的 URL 需要指定 key 列",
            })
            continue
        key = pk_cols[0]

        try:
            src = _build_url(db_cfg[node_a], schema, table, key)
            tgt = _build_url(db_cfg[node_b], schema, table, key)
        except KeyError as e:
            return {
                "success": False, "identical": None,
                "error": "节点配置缺少字段: %s" % e,
                "output": "", "diff_count": -1, "details": [],
            }

        cmd = ["pg_comparator", src, tgt]

        # Belt and braces: assert no destructive flag slipped in.
        bad = [f for f in _DESTRUCTIVE_FLAGS if f in cmd]
        if bad:
            raise AssertionError(
                "pg_comparator 的破坏性参数不得使用: %s（会修改目标库）" % bad)

        safe = [c.replace(src, src.split("@")[-1]).replace(tgt, tgt.split("@")[-1])
                for c in cmd]
        logger.info("Running pg_comparator: %s", " ".join(safe))

        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=300)
        except subprocess.TimeoutExpired:
            skipped.append({"table": table, "reason": "执行超时（300s）"})
            continue
        except Exception as e:
            skipped.append({"table": table, "reason": str(e)})
            continue

        output = (result.stdout or "") + (result.stderr or "")
        all_output.append("### %s.%s\n%s" % (schema, table, output))

        if result.returncode != 0:
            skipped.append({
                "table": table,
                "reason": "退出码 %d: %s" % (
                    result.returncode,
                    (result.stderr or "").strip().splitlines()[-1][:160]
                    if result.stderr else ""),
            })
            per_table.append({"table": "%s.%s" % (schema, table),
                              "key": key, "identical": None})
            continue

        differs = _looks_different(output)
        per_table.append({
            "table": "%s.%s" % (schema, table),
            "key": key,
            "identical": not differs,
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

    differing = [t for t in ran if not t["identical"]]
    return {
        "success": True,
        "identical": not differing,
        "diff_count": len(differing),
        "tables_compared": len(ran),
        "per_table": per_table,
        "skipped": skipped,
        "output": "\n".join(all_output),
        "details": [ln for ln in "\n".join(all_output).splitlines()
                    if ln.strip()][-100:],
    }
