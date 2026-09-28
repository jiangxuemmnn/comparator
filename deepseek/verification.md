# 验证记录

实测环境：KingbaseES V009R001C010，`database_mode = oracle`，一主两备 RWC 集群
（node1 10.217.0.106 主库，node2 10.217.0.105 / node3 10.217.0.104 备库）。

---

## 一、缺陷修复的前后对比

每条都用同一场景在修复前后各跑一次，而不是只看修复后的结果。

### 1. native 引擎假通过（缺陷 #1）

场景：node2 暂停 WAL 回放，node1 插入一行 `accounts.id = 0`。

| 引擎 | 修复前 | 修复后 |
|---|---|---|
| checksum | DIFF（正确） | DIFF |
| **native** | **OK / PASS** ← 行数 2001 vs 2000 却报通过 | **DIFF** |

修复后 native 能给出具体差异行。构造"删 1 行 + 改 1 行"的场景再测：

```
identical=False  rows=1999/2000
  仅B有 = [3]    ← node1 删了 id=3，node2 还有
  值不同 = [42]  ← id=42 被改过
```

而修复前这两个差异都不会被发现。

### 2. checksum 块错位（缺陷 #2）

场景：1 行真实差异（`id=0`，按主键排序位于最前），`chunk_size=1000`，2001 行。

| | 修复前 | 修复后 |
|---|---|---|
| chunk 差异 | **3/3（100%）** | **1/3** |
| 幻影 chunk | 有（`checksum_b=ERROR`） | 无 |
| 行级定位 | 无 | `仅A有: [0]` |

修复前每个无辜的块都被报成差异，且完全无法定位真实差异所在。

UPDATE 场景（行数不变、值改变）：

```
chunk 0: rows 1000/1000  DIFF     其余块 OK
行级定位 chunk 0:  仅A有=[] 仅B有=[] 值不同=[500]
```

### 3. 排除规则在 `-t` 时失效（缺陷 #3）

```
filter_tables('test_db', 'public', ['accounts','tmp_x','secret_tbl']) -> ['accounts']
```

显式指定的 `tmp_x` / `secret_tbl` 仍被排除规则拦下（旧版本会照单全收）。

### 4. 表名大小写不匹配时静默返回空（缺陷 #8）

```
accounts   -> accounts
Accounts   -> accounts      ← 大小写容错
ACCOUNTS   -> accounts
no_such_table -> TableNotFound   ← 明确报错，不再静默降级
```

### 5. RPO 空洞误报（缺陷 #6）

修复前 `generate_series(1, MAX(pk))` 把业务正常删除与序列跳号全报成丢失。
修复后改为行数 / 最大主键守恒：

```
accounts   2004 -> 1999 (delta -5)  max 999999 -> 2000  <<< MAX_REGRESSION
            最大主键从 999999 回退到 2000 —— 故障前的尾部数据未恢复
```

### 6. RPO 探针计数（实现过程中发现的新 bug）

修复前：删掉 5 个探针（含序号最大的 19、20），报告 `Total: 17, Found: 17, Missing: 0`
——因为分母取自"当前还存在的探针"。

修复后：`Total: 20, Found: 15, Missing: 5`，丢失序号 `[15,16,17,19,20]` 全部列出。

### 7. 应用-DB 值比较（缺陷 #4）

6 条追踪记录（含 1 个撕裂事务、1 条缺失、1 条值错）：

| | 修复前 | 修复后 |
|---|---|---|
| 结果 | ok=0，**5 条全部误报** | ok=3, missing=1, mismatch=1 |
| 撕裂事务 | 未检出 | **T2 检出（1/2 成功）** |

修复前的误报来自 `str()` 比较（`Decimal('100.00')` vs `Decimal('100.0')`），
以及 JSON 往返导致的类型丢失（JSON 数字 → float，JSON 时间 → str）。

### 8. 节点失联的结论（实现过程中发现的新 bug）

node3 指向不存在的端口：

| | 修复前 | 修复后 |
|---|---|---|
| 每张表结论 | **CONSISTENT** | **INCONCLUSIVE** |
| 退出码 | 0 | **2** |

根因是判断基准用错了——对着"可达节点集合"比而不是"全部节点"，于是
"可用节点 < 可达节点"永远不成立。节点全挂时会误报为一致。

---

## 二、归一化层的实测验证

`deepseek/test_normalize.py`，20 项全部通过，包括：

```
1.0 / 1::numeric(10,2) / 1 / 1.000 归一为同一字节   raw=['1.0','1.00','1','1.000'] -> b'1'
char(10) 'abc' 去补齐空格后 == varchar 'abc'
同一时刻的不同时区表示归一为同一字节
NULL 有独立标记，不与任何字符串碰撞
旧实现确实会碰撞（对照）'a|b|c' == 'a|b|c'
新实现不再碰撞
Decimal('100.00') 与 Decimal('100.0') 相等
```

并确认了 Oracle 模式的语义陷阱：`SELECT ''::text` 返回 `None`（空串即 NULL）。

---

## 三、能力实测（Phase 0）

`deepseek/probe_capability.py` 在真机上的结论：**104 项可用 / 9 项不可用**，
不可用的全部是 V8R3 旧函数名与 Oracle 专有函数。

关键发现（详见 `baseline_evidence.md`）：

- Oracle 模式下 `JSONB` / `SERIAL` / `BOOLEAN` / `plpgsql` / `$$` / `::jsonb`
  **全部可用**——计划中"删除存储过程以规避兼容风险"的三条理由被实测推翻，
  因此改为**保留 `sp_track_write`、只加参数**，不做无谓改动
- `enable_ci = off`（区分大小写），未加引号标识符折叠为**小写**
- `pg_stat_user_tables`、`pg_current_wal_lsn()`、`pg_last_wal_replay_lsn()` 可用
  → 一致性级别方案成立
- `pg_current_wal_lsn()` 在备库上报错，实现中已限定只在主库调用

---

## 四、命令面回归

`bash deepseek/regression.sh`，**23 项全部通过**，覆盖：

- 13 个子命令的基本调用与退出码
- 多节点矩阵、`--primary`、水位线、静默窗口、跳过一致性判定
- 三层排除规则（`-t` 场景、CLI 覆盖、不存在的表应报错）
- 外部引擎在多节点下应明确报错（退出码 1）
- `schedule` 的定时、报告落盘、收敛判定、超时告警

一致性判定的三种场景也分别验证过：

| 场景 | 判定 |
|---|---|
| 读取期间无写入 | `deterministic`（LSN 未变化） |
| 读取期间有写入 | `snapshot`（LSN 推进 + 备库追平说明） |
| 无 LSN 无计数器 | 回退到变更计数，并说明证据来源 |

---

## 五、环境恢复

所有验证均在 three-node 集群上完成，测试产生的工具表
（`_rpo_markers` / `_rpo_snapshots` / `_app_db_tracking`）已删除，
节点数据已收敛，`public` schema 仅剩 `accounts` / `orders` / `products` / `transactions`
四张测试表。所有暂停 WAL 回放的操作均已恢复。
