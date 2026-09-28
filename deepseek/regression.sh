#!/usr/bin/env bash
# Full command-surface regression against the live three-node cluster.
# Run from the project root:  bash deepseek/regression.sh
set -u
cd "$(dirname "$0")/.." || exit 1

PASS=0
FAIL=0
declare -a FAILED

run() {
  local name="$1"; shift
  local expect="$1"; shift
  local out
  out=$(PYTHONIOENCODING=utf-8 python cli.py -c config.yaml "$@" 2>&1)
  local code=$?
  if [ "$code" -eq "$expect" ]; then
    printf '  [OK  ] %-52s exit=%s\n' "$name" "$code"
    PASS=$((PASS+1))
  else
    printf '  [FAIL] %-52s exit=%s (expected %s)\n' "$name" "$code" "$expect"
    FAIL=$((FAIL+1))
    FAILED+=("$name")
    echo "$out" | tail -12 | sed 's/^/         /'
  fi
}

section() { printf '\n=== %s ===\n' "$1"; }

section "基础命令"
run "list-backends"                     0 list-backends
run "init-config (已存在，应拒绝)"       1 init-config

section "节点对比"
run "node-compare 两节点 checksum"       0 node-compare -a node1 -b node2
run "node-compare 两节点 native"         0 node-compare -a node1 -b node2 --backend native
run "node-compare 两节点 -f json"        0 node-compare -a node1 -b node2 -f json
run "node-compare 多节点矩阵"            0 node-compare --nodes node1,node2,node3
run "node-compare 多节点 --primary"      0 node-compare --nodes node1,node2,node3 --primary node1
run "node-compare 指定表"                0 node-compare -a node1 -b node2 -t accounts
run "node-compare 水位线"                0 node-compare -a node1 -b node2 --watermark 1000
run "node-compare 快速静默窗口"          0 node-compare -a node1 -b node2 --wait-quiesce 1
run "node-compare 跳过一致性判定"        0 node-compare -a node1 -b node2 --no-consistency
run "node-compare 外部引擎多节点应报错"  1 node-compare --nodes node1,node2,node3 --backend pg_comparator

section "排除规则（核心修复）"
run "显式 -t 时排除规则仍生效（无差异）"  0 node-compare -a node1 -b node2 -t accounts,orders
run "CLI --exclude-tables"               0 node-compare -a node1 -b node2 --exclude-tables 'probe_*'
run "CLI --exclude-schemas"              0 node-compare -a node1 -b node2 --exclude-schemas 'sys*'
run "不存在的表应报错（不再静默）"        1 node-compare -a node1 -b node2 -t no_such_table

section "外部工具（均未安装，应给出明确提示且退出码为 2）"
run "schema-compare (pgdiff)"            2 schema-compare -a node1 -b node2
run "node-compare --backend pg_comparator" 2 node-compare -a node1 -b node2 --backend pg_comparator -t accounts
run "node-compare --backend data_diff"   2 node-compare -a node1 -b node2 --backend data_diff -t accounts

section "应用-DB 一致性"
run "app-db-setup"                       0 app-db-setup -n node1
run "app-db-status"                      0 app-db-status -n node1
run "app-db-verify"                      0 app-db-verify -n node1

section "RPO"
run "rpo-plant"                          0 rpo-plant -n node1 --marker-count 5

section "测试数据"
run "gen-data --truncate"                0 gen-data -n node1 --accounts 100 --products 20 --orders 100 --transactions 100 --truncate

section "回归后校验"
run "gen-data 后节点仍一致"               0 node-compare -a node1 -b node2 --no-consistency

section "清理"
run "app-db-teardown"                    0 app-db-teardown -n node1

printf '\n%s\n' "=================================================="
printf '  通过 %d   失败 %d\n' "$PASS" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
  printf '  失败项:\n'
  for f in "${FAILED[@]}"; do printf '    - %s\n' "$f"; done
fi
printf '%s\n' "=================================================="
exit $((FAIL > 0))
