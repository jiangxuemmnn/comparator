"""Integration with the pgdiff external tool.

pgdiff: https://github.com/joncrlsn/pgdiff
Compares the SCHEMA of two PostgreSQL/Kingbase databases and emits the
ALTER/CREATE/DROP statements that would make the second one match the first.
It never modifies a database itself -- you review the generated SQL.

This compares table definitions, not rows. It is wired as its own command
(`schema-compare`) rather than as a `node-compare --backend`, because
folding the two together would make a schema difference read like a data
difference.

**Invocation shape.** pgdiff does NOT take schema dump files. It connects to
two databases directly and takes each side's connection parameters as a
paired set of options -- uppercase for the first database, lowercase for the
second:

    pgdiff -U user -H host -P port -D db -S schema \
           -u user -h host -p port -d db -s schema \
           <SCHEMATYPE>

It also compares ONE KIND of object per run (TABLE, COLUMN, INDEX, ...).
`ALL` runs every kind in dependency order.

An earlier version of this module passed two `pg_dump` output files as
positional arguments, which pgdiff has never accepted, and relied on
`pg_dump` being present. Neither is needed: pgdiff talks to the databases
itself.
"""

from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger("comparator.pgdiff")

# Object kinds pgdiff can compare, in the dependency order its own docs
# recommend. "ALL" is offered as a convenience and is the default.
SCHEMA_TYPES = (
    "SCHEMA", "ROLE", "SEQUENCE", "TABLE", "COLUMN", "INDEX", "VIEW",
    "FOREIGN_KEY", "FUNCTION", "TRIGGER", "OWNER",
    "GRANT_RELATIONSHIP", "GRANT_ATTRIBUTE",
)
DEFAULT_SCHEMA_TYPE = "ALL"


def check_pgdiff_installed() -> bool:
    """Check if pgdiff is installed."""
    try:
        result = subprocess.run(
            ["pgdiff", "--version"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def _side_args(dbcfg, schema, upper):
    """Connection options for one side.

    pgdiff distinguishes the two databases purely by the case of its option
    letters, which is easy to get wrong when building them programmatically:
    the flag names are chosen from a single table here so the two sides can
    never drift into using the same case.
    """
    f = (lambda ch: "-" + ch.upper()) if upper else (lambda ch: "-" + ch.lower())
    args = [f("h"), str(dbcfg["host"])]
    if dbcfg.get("port"):
        args += [f("p"), str(dbcfg["port"])]
    args += [f("d"), str(dbcfg["dbname"])]
    args += [f("u"), str(dbcfg["user"])]
    if schema:
        args += [f("s"), schema]
    return args


def build_command(dbcfg_a, dbcfg_b, schema=None,
                  schema_type=DEFAULT_SCHEMA_TYPE):
    """Build the pgdiff argv for comparing two databases.

    Passwords: when both sides use the same password it is supplied through
    PGPASSWORD rather than -W/-w, because command-line arguments are visible
    to any user who can run `ps`. Only when the two sides genuinely differ is
    the flag form used, and that fact is logged.
    """
    cmd = ["pgdiff"]
    cmd += _side_args(dbcfg_a, schema, upper=True)
    cmd += _side_args(dbcfg_b, schema, upper=False)

    pwd_a = dbcfg_a.get("password") or ""
    pwd_b = dbcfg_b.get("password") or ""
    env_extra = {}
    if pwd_a == pwd_b:
        if pwd_a:
            env_extra["PGPASSWORD"] = pwd_a
    else:
        logger.info("两侧口令不同，改用 -W/-w 传参（会出现在进程列表中）")
        cmd += ["-W", pwd_a, "-w", pwd_b]

    cmd.append(schema_type)
    return cmd, env_extra


def _count_statements(output):
    """Number of generated SQL statements in pgdiff's output.

    pgdiff prints the SQL needed to reconcile the two schemas, so a
    non-empty result IS the difference. Blank lines, SQL comments and the
    tool's own chatter are not statements.
    """
    count = 0
    for raw in output.splitlines():
        line = raw.strip()
        if not line or line.startswith("--") or line.startswith("/*"):
            continue
        if line.endswith(";"):
            count += 1
    return count


def run_pgdiff(pool, config: dict, node_a: str, node_b: str,
               schemas=None, schema_type=DEFAULT_SCHEMA_TYPE) -> dict:
    """Compare database SCHEMAS between two nodes using pgdiff.

    Returns dict with: success, identical, diff_count, output, details.
    `identical` is None (not False) when the comparison could not be run,
    so an unavailable tool is never mistaken for a detected difference.
    """
    if not check_pgdiff_installed():
        return {
            "success": False,
            "identical": None,
            "error": ("pgdiff 未安装或不在 PATH 中。"
                      "pgdiff 是外部工具，需先安装："
                      "https://github.com/joncrlsn/pgdiff"),
            "output": "",
            "diff_count": -1,
            "details": [],
        }

    db_cfg = config["databases"]
    schema = None
    if schemas:
        if isinstance(schemas, (list, tuple)):
            if len(schemas) > 1:
                return {
                    "success": False, "identical": None,
                    "error": ("pgdiff 每次只能对比一个 schema（收到 %d 个：%s）。"
                              "请用 --schemas 指定单个 schema，或分别对比。"
                              % (len(schemas), ", ".join(schemas))),
                    "output": "", "diff_count": -1, "details": [],
                }
            schema = schemas[0]
        else:
            schema = schemas

    try:
        cmd, env_extra = build_command(db_cfg[node_a], db_cfg[node_b],
                                       schema, schema_type)
    except KeyError as e:
        return {
            "success": False, "identical": None,
            "error": "节点配置缺少字段: %s" % e,
            "output": "", "diff_count": -1, "details": [],
        }

    import os
    env = os.environ.copy()
    env.update(env_extra)

    # Never show the password if it ended up on the command line.
    shown = list(cmd)
    for flag in ("-W", "-w"):
        if flag in shown:
            shown[shown.index(flag) + 1] = "***"
    logger.info("Running pgdiff (%s): %s", schema_type, " ".join(shown))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                env=env, timeout=180)
    except subprocess.TimeoutExpired:
        return {
            "success": False, "identical": None,
            "error": "pgdiff 执行超时（180s）",
            "output": "", "diff_count": -1, "details": [],
        }
    except FileNotFoundError:
        return {
            "success": False, "identical": None,
            "error": "找不到 pgdiff 可执行文件，请确认它在 PATH 中",
            "output": "", "diff_count": -1, "details": [],
        }
    except Exception as e:
        return {
            "success": False, "identical": None,
            "error": str(e), "output": "", "diff_count": -1, "details": [],
        }

    output = (result.stdout or "") + (result.stderr or "")
    details = [ln for ln in output.splitlines() if ln.strip()]

    if result.returncode != 0 and not result.stdout:
        return {
            "success": False, "identical": None,
            "error": "pgdiff 退出码 %d: %s" % (
                result.returncode, (result.stderr or "").strip()[:300]),
            "output": output, "diff_count": -1, "details": details,
        }

    diff_count = _count_statements(result.stdout or "")
    return {
        "success": True,
        "identical": diff_count == 0,
        "diff_count": diff_count,
        "schema_type": schema_type,
        "schemas": [schema] if schema else [],
        "output": output,
        "details": details,
    }
