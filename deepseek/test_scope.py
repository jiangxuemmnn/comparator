#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unit tests for utils/scope.py -- pure logic, no database needed."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from utils.scope import ScopeMatcher, build_matcher

PASS, FAIL = [], []


def check(label, ok):
    (PASS if ok else FAIL).append(label)
    print("  [%s] %s" % ("OK  " if ok else "FAIL", label))


def section(t):
    print("\n" + "=" * 78)
    print("  " + t)
    print("=" * 78)


class Args(object):
    def __init__(self, **kw):
        self.exclude_tables = kw.get("exclude_tables")
        self.exclude_schemas = kw.get("exclude_schemas")
        self.exclude_databases = kw.get("exclude_databases")


def main():
    # ---------------------------------------------------------------
    section("1. 旧配置兼容（无 scopes，回落到 comparison.*）")
    # ---------------------------------------------------------------
    legacy = {
        "comparison": {
            "schema": "public",
            "exclude_tables": ["_rpo_markers", "_app_db_tracking"],
        }
    }
    m = ScopeMatcher(legacy)
    check("public 下的 accounts 保留",
          m.table_allowed("db", "public", "accounts"))
    check("public 下的 _rpo_markers 排除",
          not m.table_allowed("db", "public", "_rpo_markers"))
    check("非 public schema 排除",
          not m.table_allowed("db", "other", "accounts"))

    # ---------------------------------------------------------------
    section("2. 三层规则：database / schema / table")
    # ---------------------------------------------------------------
    cfg = {
        "scopes": [
            {
                "database": "test_db",
                "include_schemas": ["public", "biz"],
                "exclude_schemas": ["tmp_*"],
                "exclude_tables": ["tmp_*", "*_bak", "_rpo_*"],
            },
            {
                "database": "audit_db",
                "exclude_tables": ["*"],       # 整库跳过
            },
        ]
    }
    m = ScopeMatcher(cfg)

    check("test_db.public.accounts 保留",
          m.table_allowed("test_db", "public", "accounts"))
    check("test_db.biz.orders 保留",
          m.table_allowed("test_db", "biz", "orders"))
    check("test_db 的白名单外 schema 排除",
          not m.table_allowed("test_db", "other", "accounts"))
    check("test_db.tmp_stage 被 exclude_schemas 排除",
          not m.table_allowed("test_db", "tmp_stage", "accounts"))
    check("test_db.public.tmp_x 被 exclude_tables 排除",
          not m.table_allowed("test_db", "public", "tmp_x"))
    check("test_db.public.orders_bak 被 *_bak 排除",
          not m.table_allowed("test_db", "public", "orders_bak"))
    check("test_db.public._rpo_markers 被排除",
          not m.table_allowed("test_db", "public", "_rpo_markers"))

    check("audit_db 整库被排除",
          not m.table_allowed("audit_db", "public", "anything"))

    # 没被任何 scope 覆盖的库：默认放行
    check("未配置的库默认放行",
          m.table_allowed("other_db", "public", "accounts"))

    # ---------------------------------------------------------------
    section("3. 核心修复：显式 -t 时排除规则仍然生效")
    # ---------------------------------------------------------------
    args = Args(exclude_tables="tmp_x,secret_tbl")
    m2 = build_matcher(cfg, args)
    # 模拟用户显式指定 -t accounts,tmp_x,secret_tbl
    requested = ["accounts", "tmp_x", "secret_tbl"]
    kept = m2.filter_tables("test_db", "public", requested)
    check("显式指定的 tmp_x 仍被排除", "tmp_x" not in kept)
    check("显式指定的 secret_tbl 仍被排除", "secret_tbl" not in kept)
    check("未被排除的 accounts 保留", kept == ["accounts"])
    print("        filter_tables(%r) -> %r" % (requested, kept))

    # ---------------------------------------------------------------
    section("4. CLI 覆盖不会变成白名单")
    # ---------------------------------------------------------------
    m3 = build_matcher(legacy, Args())     # 空的 CLI 参数
    check("空 CLI 参数不影响判断",
          m3.table_allowed("db", "public", "accounts"))
    check("空 CLI 参数下排除依然有效",
          not m3.table_allowed("db", "public", "_rpo_markers"))

    # 只有 exclude_schemas 的 CLI 覆盖，不应把表也限制住
    m4 = build_matcher(legacy, Args(exclude_schemas="tmp_*"))
    check("CLI 只排 schema 时，public 的表仍保留",
          m4.table_allowed("db", "public", "accounts"))
    check("CLI 排 schema 生效",
          not m4.table_allowed("db", "tmp_zone", "accounts"))

    # ---------------------------------------------------------------
    section("5. 大小写宽容")
    # ---------------------------------------------------------------
    m5 = ScopeMatcher({"scopes": [{"exclude_tables": ["TMP_*"]}]})
    check("大写模式匹配小写表名",
          not m5.table_allowed("db", "public", "tmp_x"))
    check("小写模式匹配大写表名",
          not ScopeMatcher({"scopes": [{"exclude_tables": ["tmp_*"]}]})
              .table_allowed("db", "public", "TMP_X"))

    # ---------------------------------------------------------------
    section("6. database 通配与 exclude_databases")
    # ---------------------------------------------------------------
    m6 = ScopeMatcher({"scopes": [{"database": "prod_*",
                                   "exclude_tables": ["*"]}]})
    check("prod_a 被通配匹配后整库排除",
          not m6.table_allowed("prod_a", "public", "t"))
    check("dev_a 不受影响", m6.table_allowed("dev_a", "public", "t"))

    m7 = build_matcher({}, Args(exclude_databases="audit_db"))
    check("CLI --exclude-databases 生效",
          not m7.table_allowed("audit_db", "public", "t"))
    check("其余库不受影响", m7.table_allowed("test_db", "public", "t"))

    print("\n" + "#" * 78)
    print("#  通过 %d    失败 %d" % (len(PASS), len(FAIL)))
    print("#" * 78)
    for f in FAIL:
        print("  FAILED: %s" % f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
