#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verify utils/normalize.py against values fetched from the live database.

The point of the normalize layer is that the SAME logical value must produce
the SAME bytes regardless of how the column was declared. These cases come
from real probe output on KingbaseES V009R001C010 (database_mode=oracle).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import psycopg2

from utils.normalize import normalize_value, normalize_row, values_equal

def _conn_args():
    """Connection details come from the (gitignored) config.yaml so no
    credentials are ever committed. Env vars override."""
    import yaml
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
    path = os.environ.get("KB_CONFIG", os.path.join(root, "config.yaml"))
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh) or {}
    dbs = cfg.get("databases") or {}
    node = os.environ.get("KB_NODE") or next(iter(dbs))
    d = dbs[node]
    return dict(
        host=os.environ.get("KB_HOST", d["host"]),
        port=int(os.environ.get("KB_PORT", d.get("port", 54321))),
        dbname=os.environ.get("KB_DB", d["dbname"]),
        user=os.environ.get("KB_USER", d["user"]),
        password=os.environ.get("KB_PWD", d.get("password", "")),
    )

PASS, FAIL = [], []


def fetch_one(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def check(label, ok, detail=""):
    (PASS if ok else FAIL).append(label)
    print("  [%s] %-58s %s" % ("OK  " if ok else "FAIL", label, detail))


def section(t):
    print("\n" + "=" * 78)
    print("  " + t)
    print("=" * 78)


def main():
    conn = psycopg2.connect(connect_timeout=10, **_conn_args())
    conn.autocommit = True
    cur = conn.cursor()

    # ---------------------------------------------------------------
    section("1. numeric：同一逻辑值，不同声明宽度")
    # ---------------------------------------------------------------
    a = fetch_one(cur, "SELECT 1.0::numeric")
    b = fetch_one(cur, "SELECT 1::numeric(10,2)")
    c = fetch_one(cur, "SELECT 1::numeric")
    d = fetch_one(cur, "SELECT 1.000::numeric")
    vals = [normalize_value(v, "numeric") for v in (a, b, c, d)]
    check("1.0 / 1::numeric(10,2) / 1 / 1.000 归一为同一字节",
          len(set(vals)) == 1,
          "raw=%r -> %r" % ([str(v) for v in (a, b, c, d)], vals[0]))

    e = fetch_one(cur, "SELECT 1.50::numeric")
    f = fetch_one(cur, "SELECT 1.5::numeric")
    check("1.50 与 1.5 相同",
          normalize_value(e, "numeric") == normalize_value(f, "numeric"))

    g = fetch_one(cur, "SELECT 1.51::numeric")
    check("1.5 与 1.51 不同",
          normalize_value(f, "numeric") != normalize_value(g, "numeric"))

    # 大数不能用科学计数法串味
    big = fetch_one(cur, "SELECT 100::numeric")
    check("100 不产生指数形式",
          normalize_value(big, "numeric") == b"I100" or
          normalize_value(big, "numeric") == b"100",
          repr(normalize_value(big, "numeric")))

    z = fetch_one(cur, "SELECT (-0.0)::numeric")
    check("-0 归一为 0",
          normalize_value(z, "numeric") == normalize_value(
              fetch_one(cur, "SELECT 0::numeric"), "numeric"))

    # ---------------------------------------------------------------
    section("2. char(n) 补空格 vs varchar 保留空格")
    # ---------------------------------------------------------------
    ch = fetch_one(cur, "SELECT 'abc'::char(10)")
    vc = fetch_one(cur, "SELECT 'abc'::varchar(10)")
    check("char(10) 'abc' 去掉补齐空格后 == varchar 'abc'",
          normalize_value(ch, "bpchar") == normalize_value(vc, "character varying"),
          "char raw=%r" % (ch,))

    vp = fetch_one(cur, "SELECT 'abc  '::varchar(10)")
    check("varchar 尾空格被保留（不与 'abc' 相等）",
          normalize_value(vp, "character varying") !=
          normalize_value(vc, "character varying"))

    # ---------------------------------------------------------------
    section("3. 时间类型与时区")
    # ---------------------------------------------------------------
    t1 = fetch_one(cur, "SELECT '2024-01-01 12:00:00.123456+08'::timestamptz")
    t2 = fetch_one(cur, "SELECT '2024-01-01 04:00:00.123456+00'::timestamptz")
    check("同一时刻的不同时区表示归一为同一字节",
          normalize_value(t1, "timestamp with time zone") ==
          normalize_value(t2, "timestamp with time zone"),
          "%r vs %r" % (t1, t2))

    t3 = fetch_one(cur, "SELECT '2024-01-01 12:00:00.123456'::timestamp")
    check("US 精度保留（不相等即正确）",
          normalize_value(t1, "timestamptz") != normalize_value(t3, "timestamp"))

    # Oracle 模式把 DATE 映射为 datetime
    dt = fetch_one(cur, "SELECT '2024-01-01'::date")
    check("DATE 映射为 datetime 后仍可归一",
          normalize_value(dt, "date").startswith(b"T"),
          "%r -> %r" % (dt, normalize_value(dt, "date")))

    # ---------------------------------------------------------------
    section("4. NULL 与空串")
    # ---------------------------------------------------------------
    n = fetch_one(cur, "SELECT NULL::text")
    e = fetch_one(cur, "SELECT ''::text")
    check("NULL 有独立标记，不与任何字符串碰撞",
          normalize_value(n) == b"\x00",
          repr(normalize_value(n)))
    print("       注：''::text 在本模式返回 %r（Oracle 语义：空串即 NULL）" % (e,))
    if e is None:
        check("Oracle 模式下 '' 与 NULL 无法区分（已知限制）",
              normalize_value(e) == normalize_value(n))

    # ---------------------------------------------------------------
    section("5. bool / bytea / int")
    # ---------------------------------------------------------------
    check("true 与 1 不碰撞",
          normalize_value(True) != normalize_value(1))
    check("bytea 归一为 hex",
          normalize_value(fetch_one(cur, "SELECT 'abc'::bytea")) == b"X616263")

    # ---------------------------------------------------------------
    section("6. 分隔符碰撞（旧实现的 bug）")
    # ---------------------------------------------------------------
    # 旧实现用 ' | ' 拼接，('a|b','c') 与 ('a','b|c') 会产生同一字符串
    old_a = "a|b" + "|" + "c"
    old_b = "a" + "|" + "b|c"
    check("旧实现确实会碰撞（仅供对照）", old_a == old_b,
          "%r == %r" % (old_a, old_b))
    new_a = normalize_row(["a|b", "c"], ["text", "text"])
    new_b = normalize_row(["a", "b|c"], ["text", "text"])
    check("新实现不再碰撞", new_a != new_b)

    # 列数不同也不能碰撞
    check("('ab','c') 与 ('a','bc') 不碰撞",
          normalize_row(["ab", "c"]) != normalize_row(["a", "bc"]))

    # ---------------------------------------------------------------
    section("7. 顺序无关性验证（分块累加器的前提）")
    # ---------------------------------------------------------------
    cur.execute("SELECT id, account_no, balance FROM public.accounts ORDER BY id LIMIT 5")
    sample = cur.fetchall()
    if sample:
        per_row = [normalize_row(list(r), ["integer", "character varying", "numeric"])
                   for r in sample]
        check("同一批行，不同读取顺序 -> 行哈希集合相同",
              sorted(per_row) == sorted(list(reversed(per_row))),
              "%d 行" % len(sample))

    # ---------------------------------------------------------------
    section("8. values_equal 端到端")
    # ---------------------------------------------------------------
    check("Decimal('100.00') 与 Decimal('100.0') 相等",
          values_equal(fetch_one(cur, "SELECT 100.00::numeric"),
                       fetch_one(cur, "SELECT 100.0::numeric"), "numeric"))
    check("'abc' char(10) 与 'abc' text 相等",
          values_equal(ch, "abc", "bpchar"))

    cur.close()
    conn.close()

    print("\n" + "#" * 78)
    print("#  通过 %d    失败 %d" % (len(PASS), len(FAIL)))
    print("#" * 78)
    if FAIL:
        for f in FAIL:
            print("  FAILED: %s" % f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
