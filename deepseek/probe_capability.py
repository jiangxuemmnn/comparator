#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Kingbase 兼容能力探针 —— 供 comparator 设计决策使用。

目的：不靠"模式名"猜能力，而是直接问数据库。Kingbase 小版本之间兼容度
差异很大（例如 LISTAGG 是 V008R006C003B0062 才加入），用 database_mode
推断能力会在老版本上翻车。

安全性：
  - 绝大多数探针是纯 SELECT
  - 需要验证 DDL 语法解析的探针一律使用 TEMP TABLE / TEMP FUNCTION，
    会话结束即自动消失，不触碰任何业务对象、不写任何业务数据

用法：
  python deepseek/probe_capability.py -c config.yaml -n node1
  python deepseek/probe_capability.py --host 10.217.0.106 --port 53261 \
      --dbname kingbase --user system --password xxx
"""

import argparse
import os
import sys
import traceback
from datetime import datetime, timezone, timedelta
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: 需要 psycopg2。请先 pip install psycopg2-binary")
    sys.exit(2)


# --------------------------------------------------------------------------
# 探针运行器
# --------------------------------------------------------------------------

class Prober(object):
    """在 autocommit 连接上逐条跑探针，单条失败不影响其余。"""

    def __init__(self, conn):
        self.conn = conn
        self.conn.autocommit = True
        self.results = []

    def probe(self, section, label, sql, params=None):
        """执行一条探针，记录结果。返回 (ok, value)。"""
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cur.execute(sql, params)
            # DDL / 无结果集的语句 description 为 None，此时 fetchall() 会抛
            # "no results to fetch"。执行成功本身即是通过。
            if cur.description is not None:
                rows = cur.fetchall()
                type_code = cur.description[0].type_code
            else:
                rows = []
                type_code = None
            value = rows
            # 单行单列时直接取值，便于阅读
            if len(rows) == 1 and len(rows[0]) == 1:
                value = rows[0][0]
            elif not rows and cur.description is None:
                value = "<executed, no result set>"
            ok = True
            err = None
        except Exception as e:
            ok, value, err = False, None, str(e).strip().splitlines()[0][:200]
            type_code = None
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                self.conn.rollback()
            except Exception:
                pass

        entry = {
            "section": section,
            "label": label,
            "sql": " ".join(sql.split())[:150],
            "ok": ok,
            "value": value,
            "error": err,
            "type_code": type_code,
        }
        self.results.append(entry)
        self._emit(entry)
        return ok, value

    @staticmethod
    def _emit(entry):
        mark = "OK  " if entry["ok"] else "FAIL"
        val = entry["value"]
        if val is None:
            shown = "-"
        elif isinstance(val, (bytes, bytearray)):
            shown = "<%d bytes> %r" % (len(val), bytes(val)[:32])
        else:
            shown = repr(val)
        if entry["type_code"]:
            shown += "   [oid=%s]" % entry["type_code"]
        line = "  [%s] %-46s %s" % (mark, entry["label"], shown)
        print(line)
        if not entry["ok"]:
            print("        %s" % entry["error"])
        sys.stdout.flush()

    def section(self, title):
        print("\n" + "=" * 78)
        print("  " + title)
        print("=" * 78)
        sys.stdout.flush()

    def summary(self, title):
        print("\n" + "#" * 78)
        print("#  %s" % title)
        print("#" * 78)
        ok = [r for r in self.results if r["ok"]]
        bad = [r for r in self.results if not r["ok"]]
        print("  可用: %d    不可用: %d" % (len(ok), len(bad)))
        if bad:
            print("\n  不可用项:")
            for r in bad:
                print("    - [%s] %s" % (r["section"], r["label"]))
                print("      %s" % r["error"])


# --------------------------------------------------------------------------
# 探针主体
# --------------------------------------------------------------------------

def run_probes(p, node_label):
    p.section("0. 环境标识  (%s)" % node_label)
    p.probe("env", "SELECT version()", "SELECT version()")
    p.probe("env", "SHOW database_mode", "SHOW database_mode")
    p.probe("env", "current_setting('database_mode')",
            "SELECT current_setting('database_mode')")
    p.probe("env", "SHOW compatible_mode", "SHOW compatible_mode")
    p.probe("env", "current_setting('compatible_mode')",
            "SELECT current_setting('compatible_mode')")
    p.probe("env", "current_database()", "SELECT current_database()")
    p.probe("env", "current_schema()", "SELECT current_schema()")
    p.probe("env", "server_encoding", "SHOW server_encoding")
    p.probe("env", "lc_collate", "SHOW lc_collate")

    # ---- 大小写折叠：决定 _quote_ident 能否安全使用 ----
    p.section("1. 标识符大小写折叠")
    p.probe("case", "SHOW enable_ci", "SHOW enable_ci")
    p.probe("case", "SHOW case_sensitive", "SHOW case_sensitive")
    p.probe("case", "SHOW enable_upper_colname", "SHOW enable_upper_colname")
    # 未加引号标识符的折叠方向：建临时表后查系统目录看实际存储名
    p.probe("case", "未加引号表名折叠方向", """
        CREATE TEMP TABLE ProbeCaseUnquoted (ColOne int);
        SELECT c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname LIKE 'pg_temp%' AND c.relname ILIKE 'probecaseunquoted'
    """)
    p.probe("case", "加引号表名是否保持原样", """
        CREATE TEMP TABLE "ProbeCaseQuoted" (x int);
        SELECT c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname LIKE 'pg_temp%' AND c.relname = 'ProbeCaseQuoted'
    """)

    # ---- 数据类型：工具自建表能否使用 ----
    p.section("2. 数据类型（TEMP TABLE，会话结束即消失）")
    p.probe("type", "JSONB", "CREATE TEMP TABLE t_j (v JSONB)")
    p.probe("type", "JSON", "CREATE TEMP TABLE t_j2 (v JSON)")
    p.probe("type", "SERIAL", "CREATE TEMP TABLE t_s (v SERIAL)")
    p.probe("type", "BIGSERIAL", "CREATE TEMP TABLE t_bs (v BIGSERIAL)")
    p.probe("type", "BIGINT GENERATED BY DEFAULT AS IDENTITY",
            "CREATE TEMP TABLE t_id (v BIGINT GENERATED BY DEFAULT AS IDENTITY)")
    p.probe("type", "BOOLEAN", "CREATE TEMP TABLE t_b (v BOOLEAN)")
    p.probe("type", "CHAR(1)", "CREATE TEMP TABLE t_c1 (v CHAR(1))")
    p.probe("type", "TEXT", "CREATE TEMP TABLE t_t (v TEXT)")
    p.probe("type", "VARCHAR(2000)", "CREATE TEMP TABLE t_v (v VARCHAR(2000))")
    p.probe("type", "TIMESTAMP", "CREATE TEMP TABLE t_ts (v TIMESTAMP)")
    p.probe("type", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "CREATE TEMP TABLE t_ts2 (v TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    p.probe("type", "CREATE SEQUENCE", "CREATE TEMP SEQUENCE seq_probe")
    p.probe("type", "nextval('seq')", "SELECT nextval('seq_probe')")
    p.probe("type", "NEXTVAL('seq') 大写调用", "SELECT NEXTVAL('seq_probe')")

    # ---- 哈希函数：返回值类型决定跨模式可比性 ----
    p.section("3. 哈希/摘要函数")
    p.probe("hash", "md5('a')", "SELECT md5('a')")
    p.probe("hash", "MD5('a') 大写", "SELECT MD5('a')")
    p.probe("hash", "md5('a') RETURNING 类型", "SELECT pg_typeof(md5('a'))")
    p.probe("hash", "standard_hash('a','MD5')",
            "SELECT standard_hash('a','MD5')")
    p.probe("hash", "hash_record / md5(bytea)",
            "SELECT md5('a'::bytea)")

    # ---- 聚合 ----
    p.section("4. 字符串聚合（含长度上限行为）")
    p.probe("agg", "string_agg(x,'')",
            "SELECT string_agg(x, '' ORDER BY x) FROM (SELECT 'a'::varchar AS x) t")
    p.probe("agg", "listagg(x,'') WITHIN GROUP",
            "SELECT listagg(x, '') WITHIN GROUP (ORDER BY x) "
            "FROM (SELECT 'a'::varchar AS x) t")
    p.probe("agg", "wm_concat(x)",
            "SELECT wm_concat(x) FROM (SELECT 'a'::varchar AS x) t")
    p.probe("agg", "group_concat(x)",
            "SELECT group_concat(x) FROM (SELECT 'a'::varchar AS x) t")
    # 长度上限：拼接 3000 行，看是否静默截断
    p.probe("agg", "string_agg 拼接 3000 行后的长度",
            "SELECT length(string_agg(repeat('x',10), '')) "
            "FROM generate_series(1,3000) g")
    p.probe("agg", "group_concat 拼接 3000 行后的长度",
            "SELECT length(group_concat(repeat('x',10))) "
            "FROM generate_series(1,3000) g")
    p.probe("agg", "group_concat_max_len 设置", "SHOW group_concat_max_len")

    # ---- 行限制 ----
    p.section("5. 行限制 / 集合运算 / 序列生成")
    p.probe("sql", "SELECT 1 LIMIT 1", "SELECT 1 LIMIT 1")
    p.probe("sql", "LIMIT n OFFSET m", "SELECT 1 LIMIT 1 OFFSET 0")
    p.probe("sql", "FETCH FIRST n ROWS ONLY", "SELECT 1 FETCH FIRST 1 ROWS ONLY")
    p.probe("sql", "EXCEPT", "SELECT 1 AS x EXCEPT SELECT 2")
    p.probe("sql", "MINUS", "SELECT 1 AS x MINUS SELECT 2")
    p.probe("sql", "INTERSECT", "SELECT 1 AS x INTERSECT SELECT 1")
    p.probe("sql", "generate_series(1,3)",
            "SELECT i FROM generate_series(1,3) s(i)")
    p.probe("sql", "WITH RECURSIVE",
            "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<3) "
            "SELECT i FROM s")
    p.probe("sql", "CONNECT BY LEVEL", "SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 3")
    p.probe("sql", "DUAL 表", "SELECT 1 FROM DUAL")
    p.probe("sql", "行值比较 (a,b) > (1,2)",
            "SELECT (2,3) > (1,2)")
    p.probe("sql", "ORDER BY 后 NULLS FIRST",
            "SELECT 1 AS x ORDER BY x NULLS FIRST")

    # ---- 系统目录视图 ----
    p.section("6. 系统目录视图")
    p.probe("cat", "pg_tables", "SELECT count(*) FROM pg_tables")
    p.probe("cat", "information_schema.tables",
            "SELECT count(*) FROM information_schema.tables")
    p.probe("cat", "information_schema.columns",
            "SELECT count(*) FROM information_schema.columns")
    p.probe("cat", "information_schema.table_constraints",
            "SELECT count(*) FROM information_schema.table_constraints")
    p.probe("cat", "information_schema.key_column_usage",
            "SELECT count(*) FROM information_schema.key_column_usage")
    p.probe("cat", "table_constraints.constraint_type 取值",
            "SELECT DISTINCT constraint_type FROM information_schema.table_constraints")
    p.probe("cat", "pg_stat_user_tables（变更计数，Level 1 依赖）",
            "SELECT n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables LIMIT 1")
    p.probe("cat", "pg_stat_replication（复制拓扑探测）",
            "SELECT count(*) FROM pg_stat_replication")
    p.probe("cat", "sys_settings 是否存在",
            "SELECT count(*) FROM sys_settings WHERE name = 'database_mode'")
    p.probe("cat", "pg_settings 是否存在",
            "SELECT count(*) FROM pg_settings WHERE name = 'database_mode'")
    p.probe("cat", "sys_tables 是否存在",
            "SELECT count(*) FROM sys_tables WHERE 1=0")
    p.probe("cat", "pg_index", "SELECT count(*) FROM pg_index WHERE 1=0")
    p.probe("cat", "pg_constraint", "SELECT count(*) FROM pg_constraint WHERE 1=0")

    # ---- LSN / 复制相关（Level 1/2 依赖）----
    p.section("7. LSN 与复制状态函数（一致性级别依赖）")
    p.probe("lsn", "pg_current_wal_lsn()", "SELECT pg_current_wal_lsn()")
    p.probe("lsn", "sys_current_wal_lsn()", "SELECT sys_current_wal_lsn()")
    p.probe("lsn", "pg_current_xlog_location() (V8R3 旧名)",
            "SELECT pg_current_xlog_location()")
    p.probe("lsn", "pg_last_wal_replay_lsn()", "SELECT pg_last_wal_replay_lsn()")
    p.probe("lsn", "sys_last_wal_replay_lsn()", "SELECT sys_last_wal_replay_lsn()")
    p.probe("lsn", "pg_last_xlog_replay_location() (V8R3 旧名)",
            "SELECT pg_last_xlog_replay_location()")
    p.probe("lsn", "sys_wal_lsn_diff()", "SELECT sys_wal_lsn_diff('0/0','0/0')")
    p.probe("lsn", "pg_wal_lsn_diff()", "SELECT pg_wal_lsn_diff('0/0','0/0')")
    p.probe("lsn", "pg_is_in_recovery()", "SELECT pg_is_in_recovery()")
    p.probe("lsn", "sys_is_in_recovery()", "SELECT sys_is_in_recovery()")
    p.probe("lsn", "txid_current()", "SELECT txid_current()")
    p.probe("lsn", "pg_current_snapshot()", "SELECT pg_current_snapshot()")

    # ---- 存储过程语言 ----
    p.section("8. 存储过程语言（评估 sp_track_write 可行性）")
    p.probe("func", "LANGUAGE plpgsql + $$ 包裹", """
        CREATE OR REPLACE FUNCTION pg_temp.probe_f1() RETURNS INTEGER AS $$
        BEGIN RETURN 1; END;
        $$ LANGUAGE plpgsql
    """)
    p.probe("func", "LANGUAGE plsql", """
        CREATE OR REPLACE FUNCTION pg_temp.probe_f2() RETURNS INTEGER AS $$
        BEGIN RETURN 1; END;
        $$ LANGUAGE plsql
    """)
    p.probe("func", "不指定 LANGUAGE", """
        CREATE OR REPLACE FUNCTION pg_temp.probe_f3() RETURNS INTEGER AS $$
        BEGIN RETURN 1; END;
        $$
    """)
    p.probe("func", "Oracle 风格单引号包裹函数体", """
        CREATE OR REPLACE FUNCTION pg_temp.probe_f4() RETURN INTEGER IS
        BEGIN RETURN 1; END;
    """)
    p.probe("func", "函数内 ::jsonb 强转", """
        CREATE OR REPLACE FUNCTION pg_temp.probe_f5() RETURNS INTEGER AS $$
        DECLARE v INTEGER;
        BEGIN
            SELECT 1 INTO v FROM (SELECT '{"a":1}'::jsonb AS j) t;
            RETURN v;
        END;
        $$ LANGUAGE plpgsql
    """)

    # ---- 归一化：同一逻辑值在不同模式下的 Python 表现 ----
    p.section("9. 值归一化（决定 utils/normalize.py 规则）")
    p.probe("norm", "numeric 字面量 1.0", "SELECT 1.0::numeric")
    p.probe("norm", "numeric 字面量 1", "SELECT 1::numeric")
    p.probe("norm", "numeric(10,2) 的 1", "SELECT 1::numeric(10,2)")
    p.probe("norm", "numeric 转 text", "SELECT (1.0::numeric)::text")
    p.probe("norm", "integer 转 text", "SELECT (1::integer)::text")
    p.probe("norm", "bigint 转 text", "SELECT (1::bigint)::text")
    p.probe("norm", "float8 的 0.1", "SELECT 0.1::float8")
    p.probe("norm", "float8 转 text", "SELECT (0.1::float8)::text")
    p.probe("norm", "boolean true 转 text", "SELECT (true)::text")
    p.probe("norm", "timestamp 转 text",
            "SELECT ('2024-01-01 12:00:00.123456'::timestamp)::text")
    p.probe("norm", "timestamptz 转 text",
            "SELECT ('2024-01-01 12:00:00.123456+08'::timestamptz)::text")
    p.probe("norm", "timestamptz 带时区偏移",
            "SELECT ('2024-01-01 12:00:00+05'::timestamptz)::text")
    p.probe("norm", "date 转 text", "SELECT ('2024-01-01'::date)::text")
    p.probe("norm", "char(10) 的 'abc' 转 text 看补空格",
            "SELECT ('abc'::char(10))::text")
    p.probe("norm", "char(10) 的 'abc' 长度",
            "SELECT length(('abc'::char(10))::text)")
    p.probe("norm", "varchar 保留尾空格", "SELECT length('abc   '::varchar)")
    p.probe("norm", "NULL 与空串是否区分",
            "SELECT (NULL::text IS NULL), (''::text IS NULL), "
            "(NULL::text = ''::text)")
    p.probe("norm", "bytea 转 text", "SELECT ('abc'::bytea)::text")
    p.probe("norm", "行值拼接 ROW(...)",
            "SELECT ROW(1, 'a', NULL)::text")

    # ---- 实测缺陷 #8：表名大小写不匹配时的行为 ----
    p.section("10. 缺陷复现：information_schema 大小写敏感")
    p.probe("bug", "建一个全小写业务表", """
        CREATE TEMP TABLE probe_case_tbl (id int PRIMARY KEY, v varchar(10));
        SELECT tablename FROM pg_tables WHERE schemaname LIKE 'pg_temp%'
    """)
    p.probe("bug", "用小写名查 information_schema.columns",
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema LIKE 'pg_temp%' AND table_name = 'probe_case_tbl'")
    p.probe("bug", "用大写名查 information_schema.columns（预期静默为空）",
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema LIKE 'pg_temp%' AND table_name = 'PROBE_CASE_TBL'")
    p.probe("bug", "data_type 实际取值样例",
            "SELECT DISTINCT data_type FROM information_schema.columns "
            "WHERE table_schema LIKE 'pg_temp%'")

    # ---- 权限 ----
    p.section("11. 权限与角色")
    p.probe("priv", "current_user", "SELECT current_user")
    p.probe("priv", "是不是 superuser", "SHOW is_superuser")
    p.probe("priv", "当前角色属性",
            "SELECT rolsuper, rolreplication, rolcanlogin FROM pg_roles "
            "WHERE rolname = current_user")

    # ---- 事务隔离 ----
    p.section("12. 事务与隔离级别")
    p.probe("txn", "默认 transaction_isolation", "SHOW transaction_isolation")
    p.probe("txn", "default_transaction_isolation", "SHOW default_transaction_isolation")
    p.probe("txn", "SET TRANSACTION ISOLATION（会话级探测）",
            "SELECT current_setting('transaction_isolation')")
    p.probe("txn", "pg_export_snapshot()", "SELECT pg_export_snapshot()")
    p.probe("txn", "sys_export_snapshot()", "SELECT sys_export_snapshot()")


def quick_value_probe(p):
    """把关键值再取一次，便于肉眼确认归一化规则。"""
    p.section("13. Python 侧实际拿到的类型（归一化关键）")
    conn = p.conn
    samples = [
        ("numeric 1.0", "SELECT 1.0::numeric"),
        ("numeric(10,2) 的 1", "SELECT 1::numeric(10,2)"),
        ("int 1", "SELECT 1::integer"),
        ("bigint 1", "SELECT 1::bigint"),
        ("float8 0.1", "SELECT 0.1::float8"),
        ("bool true", "SELECT true"),
        ("text 'abc'", "SELECT 'abc'::text"),
        ("char(10) 'abc'", "SELECT 'abc'::char(10)"),
        ("varchar(10) 'abc  '", "SELECT 'abc  '::varchar(10)"),
        ("timestamp us", "SELECT '2024-01-01 12:00:00.123456'::timestamp"),
        ("timestamptz +08", "SELECT '2024-01-01 12:00:00.123456+08'::timestamptz"),
        ("date", "SELECT '2024-01-01'::date"),
        ("NULL text", "SELECT NULL::text"),
        ("empty text", "SELECT ''::text"),
        ("bytea", "SELECT 'abc'::bytea"),
    ]
    cur = conn.cursor()
    for label, sql in samples:
        try:
            conn.rollback()
            cur.execute(sql)
            row = cur.fetchone()
            v = row[0]
            print("  %-24s -> %-30s  python_type=%s  str()=%r" % (
                label, repr(v), type(v).__name__, str(v)))
        except Exception as e:
            print("  %-24s -> ERROR %s" % (label, e))
            conn.rollback()
    cur.close()
    print()


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Kingbase 兼容能力探针")
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("-n", "--node", default=None, help="config 中的节点名")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int, default=54321)
    ap.add_argument("--dbname")
    ap.add_argument("--user")
    ap.add_argument("--password")
    args = ap.parse_args()

    conn_args = None
    label = None

    if args.host:
        conn_args = dict(host=args.host, port=args.port, dbname=args.dbname,
                         user=args.user, password=args.password)
        label = "%s:%s/%s" % (args.host, args.port, args.dbname)
    else:
        try:
            import yaml
        except ImportError:
            print("ERROR: 需要 pyyaml 来读取配置文件")
            return 2
        cfg_path = args.config
        if not os.path.exists(cfg_path):
            print("ERROR: 配置文件不存在: %s" % cfg_path)
            print("提示: 可以先 `python cli.py init-config`，或直接用 --host 等参数")
            return 2
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        dbs = cfg.get("databases") or {}
        node = args.node
        if not node:
            if len(dbs) == 1:
                node = list(dbs)[0]
            else:
                print("ERROR: config 中有多个节点，请用 -n 指定。可选: %s"
                      % ", ".join(dbs))
                return 2
        if node not in dbs:
            print("ERROR: 配置中没有节点 '%s'。可选: %s" % (node, ", ".join(dbs)))
            return 2
        d = dbs[node]
        conn_args = dict(host=d["host"], port=d.get("port", 54321),
                         dbname=d["dbname"], user=d["user"],
                         password=d.get("password", ""))
        label = "%s (%s:%s/%s)" % (node, d["host"], d.get("port", 54321), d["dbname"])

    print("#" * 78)
    print("#  Kingbase 兼容能力探针")
    print("#  目标: %s" % label)
    print("#  时间: %s" % datetime.now().isoformat())
    print("#" * 78)

    try:
        conn = psycopg2.connect(connect_timeout=10, **conn_args)
    except Exception as e:
        print("\n连接失败: %s" % e)
        return 1

    try:
        p = Prober(conn)
        run_probes(p, label)
        p.section("13. Python 侧实际拿到的类型（归一化关键）")
        quick_value_probe(p)
        p.summary("探针汇总")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
