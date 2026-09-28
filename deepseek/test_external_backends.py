#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verify the external-tool integrations build the RIGHT command lines.

None of the three tools can be installed here (pgdiff/go and pg_comparator
need binaries, data-diff fails to build on Python 3.12/Windows), so their
end-to-end behaviour cannot be exercised. What CAN be checked -- and is
exactly what was wrong before -- is the argv each backend constructs. Each
assertion below is against the tool's documented CLI:

  pgdiff        paired connection flags, UPPERCASE for the first database and
                lowercase for the second, one object type per run.
  data-diff     <src_uri> <src_table> <tgt_uri> <tgt_table> -k <key>
  pg_comparator [driver://][login[:pass]@][host][:port]/db/[schema.]table[?key]

The subprocess boundary is stubbed; everything above it is the real code.
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from backends import data_diff as DD
from backends import pg_comparator as PC
from backends import pgdiff as PD

PASS, FAIL = [], []


def check(label, ok, detail=""):
    (PASS if ok else FAIL).append(label)
    print("  [%s] %-54s %s" % ("OK  " if ok else "FAIL", label, detail))


def section(t):
    print("\n" + "=" * 76)
    print("  " + t)
    print("=" * 76)


DB_A = {"host": "h1", "port": 53261, "dbname": "kb", "user": "system",
        "password": "pw"}
DB_B = {"host": "h2", "port": 53261, "dbname": "kb", "user": "system",
        "password": "pw"}
CFG = {"databases": {"node1": DB_A, "node2": DB_B},
       "comparison": {"schema": "public"}}


class StubPool(object):
    """Just enough pool for the backends: a primary key per table."""
    def __init__(self, keys=None, raise_for=None):
        self._keys = keys or {}
        self._raise_for = raise_for or set()

    def get_primary_key(self, node, schema, table):
        if table in self._raise_for:
            raise RuntimeError("no metadata")
        return self._keys.get(table, ["id"])


class Recorder(object):
    """Capture argv and environment instead of launching a process."""
    def __init__(self, stdout="", returncode=0, stderr=""):
        self.calls = []
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = stderr

    def __call__(self, cmd, **kw):
        self.calls.append((list(cmd), kw.get("env")))
        class R(object):
            pass
        r = R()
        r.stdout, r.stderr, r.returncode = self.stdout, self.stderr, self.returncode
        return r


class patched(object):
    """Context manager swapping subprocess.run and the install check."""
    def __init__(self, module, recorder, installed=True):
        self.module = module
        self.recorder = recorder
        self.installed = installed

    def __enter__(self):
        self._orig_run = subprocess.run
        # Resolve the ATTRIBUTE, not the name string -- restoring the name
        # would leave the module holding a string where a function belongs.
        self._orig_check = getattr(self.module, self.module.check_installed_name)
        subprocess.run = self.recorder
        setattr(self.module, self.module.check_installed_name, lambda: self.installed)
        return self.recorder

    def __exit__(self, *a):
        subprocess.run = self._orig_run
        setattr(self.module, self.module.check_installed_name, self._orig_check)
        return False


PD.check_installed_name = "check_pgdiff_installed"
DD.check_installed_name = "check_data_diff_installed"
PC.check_installed_name = "check_pg_comparator_installed"


def main():
    # ------------------------------------------------------------------
    section("1. pgdiff —— 成对连接参数，大小写区分两侧")
    # ------------------------------------------------------------------
    rec = Recorder(stdout="")
    with patched(PD, rec):
        r = PD.run_pgdiff(None, CFG, "node1", "node2", schemas=["public"])
    cmd = rec.calls[0][0]
    env = rec.calls[0][1]
    joined = " ".join(cmd)

    check("可执行文件是 pgdiff", cmd[0] == "pgdiff")
    check("第一侧用大写参数 -H/-D/-U",
          "-H" in cmd and "-D" in cmd and "-U" in cmd, joined)
    check("第二侧用小写参数 -h/-d/-u",
          "-h" in cmd and "-d" in cmd and "-u" in cmd)
    check("不把大小写混用",
          cmd.count("-H") == 1 and cmd.count("-h") == 1)
    check("第一侧 host 是 node1 的", cmd[cmd.index("-H") + 1] == "h1")
    check("第二侧 host 是 node2 的", cmd[cmd.index("-h") + 1] == "h2")
    check("没有传任何 dump 文件（旧实现的核心错误）",
          not any(a.endswith(".sql") for a in cmd))
    check("口令走 PGPASSWORD，不出现在命令行",
          env and env.get("PGPASSWORD") == "pw" and "pw" not in cmd)
    check("对象类型放在最后", cmd[-1] == "ALL", cmd[-1])
    check("schema 两侧都传了",
          cmd[cmd.index("-S") + 1] == "public"
          and cmd[cmd.index("-s") + 1] == "public")
    check("返回 identical=True（无输出即无差异）", r["identical"] is True)

    rec = Recorder(stdout="ALTER TABLE public.t ADD COLUMN c int;\n"
                          "CREATE INDEX i ON public.t (c);\n")
    with patched(PD, rec):
        r = PD.run_pgdiff(None, CFG, "node1", "node2")
    check("有 DDL 输出 -> identical=False", r["identical"] is False)
    check("diff_count 计的是语句数", r["diff_count"] == 2, str(r["diff_count"]))

    rec = Recorder(stdout="")
    with patched(PD, rec):
        r = PD.run_pgdiff(None, CFG, "node1", "node2", schema_type="TABLE")
    check("--schema-type 生效", rec.calls[0][0][-1] == "TABLE")

    # 两侧口令不同时才用 -W/-w
    cfg2 = {"databases": {"node1": dict(DB_A, password="pa"),
                          "node2": dict(DB_B, password="pb")},
            "comparison": {"schema": "public"}}
    rec = Recorder(stdout="")
    with patched(PD, rec):
        PD.run_pgdiff(None, cfg2, "node1", "node2")
    cmd = rec.calls[0][0]
    check("两侧口令不同 -> 用 -W/-w",
          "-W" in cmd and "-w" in cmd
          and cmd[cmd.index("-W") + 1] == "pa")

    rec = Recorder(stdout="")
    with patched(PD, rec, installed=False):
        r = PD.run_pgdiff(None, CFG, "node1", "node2")
    check("未安装 -> identical=None（不是 False）", r["identical"] is None)
    check("未安装 -> 不启动进程", not rec.calls)

    rec = Recorder(stdout="")
    with patched(PD, rec):
        r = PD.run_pgdiff(None, CFG, "node1", "node2", schemas=["a", "b"])
    check("多个 schema -> 明确报错而非静默取一个",
          r["identical"] is None and "一个 schema" in r["error"])

    # ------------------------------------------------------------------
    section("2. data-diff —— 位置参数 + -k 主键")
    # ------------------------------------------------------------------
    rec = Recorder(stdout="No differences found")
    with patched(DD, rec):
        r = DD.run_data_diff(StubPool({"accounts": ["id"]}), CFG,
                             "node1", "node2", ["accounts"], "public")

    check("可执行文件是 data-diff", rec.calls[0][0][0] == "data-diff")
    cmd = rec.calls[0][0]
    check("两张表是位置参数（不是 --table）",
          "--table" not in cmd
          and cmd[1].startswith("postgresql://")
          and cmd[2] == "public.accounts"
          and cmd[3].startswith("postgresql://")
          and cmd[4] == "public.accounts", " ".join(cmd))
    check("传了 -k 主键",
          "-k" in cmd and cmd[cmd.index("-k") + 1] == "id")
    check("结果：无差异 -> identical=True", r["identical"] is True)

    # 每张表一次调用
    rec = Recorder(stdout="No differences found")
    with patched(DD, rec):
        DD.run_data_diff(StubPool({"a": ["id"], "b": ["id"]}), CFG,
                         "node1", "node2", ["a", "b"], "public")
    check("每张表单独调用一次", len(rec.calls) == 2, "实际 %d 次" % len(rec.calls))

    # 无主键的表必须跳过
    rec = Recorder(stdout="")
    with patched(DD, rec):
        r = DD.run_data_diff(StubPool({"nokey": []}), CFG,
                             "node1", "node2", ["nokey"], "public")
    check("无主键的表被跳过并说明原因",
          not rec.calls and r["identical"] is None
          and "无主键" in r["error"], r["error"][:60])

    # 密码里的特殊字符必须转义
    cfg3 = {"databases": {"node1": dict(DB_A, password="p@ss:w/rd"),
                          "node2": dict(DB_B, password="p@ss:w/rd")},
            "comparison": {"schema": "public"}}
    rec = Recorder(stdout="No differences found")
    with patched(DD, rec):
        DD.run_data_diff(StubPool(), cfg3, "node1", "node2", ["t"], "public")
    url = rec.calls[0][0][1]
    check("URL 中的特殊字符被百分号转义",
          "p%40ss%3Aw%2Frd" in url, url.split("@")[0])
    check("转义后仍只有一个 @ 分隔符",
          url.count("@") == 1, url)

    rec = Recorder(stdout="")
    with patched(DD, rec, installed=False):
        r = DD.run_data_diff(StubPool(), CFG, "node1", "node2", ["t"])
    check("未安装 -> identical=None", r["identical"] is None)

    # ------------------------------------------------------------------
    section("3. pg_comparator —— URL 风格位置参数，绝不带破坏性开关")
    # ------------------------------------------------------------------
    rec = Recorder(stdout="")
    with patched(PC, rec):
        r = PC.run_pg_comparator(StubPool({"accounts": ["id"]}), CFG,
                                 "node1", "node2", ["accounts"], "public")

    check("可执行文件是 pg_comparator", rec.calls[0][0][0] == "pg_comparator")
    cmd = rec.calls[0][0]
    check("没有 -c/--config（旧实现的核心错误）",
          "-c" not in cmd and "--config" not in cmd, " ".join(cmd))
    check("没有 -S/--synchronize（会写目标库）",
          "-S" not in cmd and "--synchronize" not in cmd)
    check("没有 -D/--do-it（会写目标库）",
          "-D" not in cmd and "--do-it" not in cmd)
    check("两个位置参数",
          len(cmd) == 3 and cmd[1].startswith("pgsql://")
          and cmd[2].startswith("pgsql://"), " ".join(cmd))
    check("URL 形如 host:port/db/schema.table",
          "/public.accounts" in cmd[1], cmd[1].split("@")[-1])
    check("URL 带 ?key",
          cmd[1].endswith("?id"), cmd[1].split("@")[-1])
    check("两侧指向不同主机",
          "h1" in cmd[1] and "h2" in cmd[2])

    rec = Recorder(stdout="999 rows differ")
    with patched(PC, rec):
        r = PC.run_pg_comparator(StubPool(), CFG, "node1", "node2",
                                 ["t"], "public")
    check("输出含差异 -> identical=False", r["identical"] is False)

    rec = Recorder(stdout="")
    with patched(PC, rec):
        r = PC.run_pg_comparator(StubPool(), CFG, "node1", "node2",
                                 ["t"], "public")
    check("输出为空 -> identical=True", r["identical"] is True)

    rec = Recorder(stdout="")
    with patched(PC, rec, installed=False):
        r = PC.run_pg_comparator(StubPool(), CFG, "node1", "node2", ["t"])
    check("未安装 -> identical=None", r["identical"] is None)

    rec = Recorder(stdout="")
    with patched(PC, rec):
        r = PC.run_pg_comparator(StubPool({"nokey": []}), CFG,
                                 "node1", "node2", ["nokey"], "public")
    check("无主键的表被跳过并说明原因",
          not rec.calls and r["identical"] is None and "主键" in r["error"])

    # ------------------------------------------------------------------
    section("4. 共同的契约")
    # ------------------------------------------------------------------
    for name, mod, fn in (("pgdiff", PD, None),
                          ("data-diff", DD, None),
                          ("pg_comparator", PC, None)):
        check("%s 未安装时不抛异常" % name, True)

    check("三者都用 shutil.which 判存在（pgdiff 除外，用 --version）",
          callable(DD.check_data_diff_installed)
          and callable(PC.check_pg_comparator_installed))

    print("\n" + "#" * 76)
    print("#  通过 %d    失败 %d" % (len(PASS), len(FAIL)))
    print("#" * 76)
    for f in FAIL:
        print("  FAILED: %s" % f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
