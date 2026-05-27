# 使用说明

## 环境要求

- Python 3.8+
- Kingbase 或 PostgreSQL 数据库集群
- 操作系统：Linux

## 安装（内网环境可用）

### 基础安装（仅内置引擎，无需外网）

本工具内置 `checksum` 和 `native` 两种对比引擎，覆盖日常所有场景。**内部对比引擎不需要安装任何外部工具**。

```bash
# 1. 在有网络的机器上下载依赖包
pip download -d ./offline_pkgs -r requirements.txt

# 2. 将整个 comparator 目录 + offline_pkgs 拷到内网机器

# 3. 在内网机器上离线安装
pip install --no-index --find-links=./offline_pkgs -r requirements.txt
```

### （可选）外部对比工具

以下工具为可选项，仅在你需要用 `--backend pg_comparator` 或 `--backend data_diff` 时才需要安装。日常使用**完全不需要**：

| 工具 | 安装方式 | 说明 |
|------|---------|------|
| pg_comparator | 参考 [pg_comparator](https://github.com/credativ/pg_comparator) | 第三方对比工具，需编译安装 |
| data-diff | `pip install data-diff` | Python 包，跨数据库对比 |
| pgdiff | 参考 [pgdiff](https://github.com/joncrlsn/pgdiff) | Java 工具，表结构对比 |

```bash
# 查看已安装的对比引擎
python cli.py -c config.yaml list-backends
```

---

## 快速开始

### 1. 生成配置文件

```bash
python cli.py init-config
# 生成 config.yaml
```

### 2. 编辑配置

编辑 `config.yaml`，至少填写数据库连接信息（详见配置文件内的注释）：

```yaml
databases:
  node1:
    host: 192.168.1.101
    port: 54321
    dbname: testdb
    user: system
    password: "your_password"

  node2:
    host: 192.168.1.102
    port: 54321
    dbname: testdb
    user: system
    password: "your_password"
```

### 3. 快速验证 — 生成数据并对比

```bash
# 在 node1 生成测试数据
python cli.py -c config.yaml gen-data -n node1 --accounts 1000

# 对比 node1 和 node2
python cli.py -c config.yaml node-compare -a node1 -b node2
```

---

## 核心场景

### 场景 A：JMeter 持续发压 + 工具实时监控数据一致性

这是最常用的高可用测试场景。JMeter 通过 JDBC 持续下发业务，工具在旁路监控。

```
JMeter (JDBC Sampler)  ──持续写入──▶  Kingbase 集群
       │
       │  (每个写操作后调用 sp_track_write 记录)
       ▼
  _app_db_tracking 表  ◀── 工具读取验证 ── comparator
```

#### 步骤：

**1) 初始化追踪基础设施**

```bash
python cli.py -c config.yaml app-db-setup -n node1
```

执行后数据库会自动创建：
- `_app_db_tracking` 表 — 记录每个写操作
- `sp_track_write()` 函数 — 供 JMeter 调用

**2) 配置 JMeter 测试计划**

在 JMeter 的 JDBC 请求中，每个写操作的后面添加一个 JDBC PostProcessor，调用追踪函数：

```
JMeter 线程组：
  ├── JDBC Sampler: 业务 INSERT/UPDATE/DELETE
  │    SQL: INSERT INTO accounts (account_no, balance) VALUES (?, ?)
  │
  └── JDBC PostProcessor: 记录操作到追踪表
       SQL: SELECT public.sp_track_write(
                '${__UUID()}',
                'INSERT',
                'accounts',
                '{"account_no": "${account_no}"}',
                '{"account_no": "${account_no}", "balance": ${balance}}'
            )
```

**sp_track_write 参数说明：**

```sql
SELECT {schema}.sp_track_write(
    batch_id,       -- 批次标识，建议用 JMeter 变量如 ${__UUID()} 或固定字符串如 'jmeter_run_001'
    operation,      -- 'INSERT' / 'UPDATE' / 'DELETE'
    table_name,     -- 表名，如 'accounts'
    pk_values,      -- 主键值，JSON 格式：'{"id": 123}' 或 '{"account_no": "ACC001"}'
    row_data        -- 完整行数据（INSERT/UPDATE 时），JSON 格式。DELETE 可传 NULL
)
```

**3) 启动 JMeter 发压的同时，启动巡检**

```bash
# 终端1：JMeter 持续发压
jmeter -n -t my_test_plan.jmx -Jduration=3600

# 终端2：工具定时巡检节点一致性（每5分钟一次）
python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2

# 终端3（可选）：定时验证应用-DB一致性（每2分钟）
python cli.py -c config.yaml schedule --interval 120 -- app-db-verify -n node1
```

**4) 压测结束后全量验证**

```bash
# 一次性验证所有未检查的追踪记录
python cli.py -c config.yaml app-db-verify -n node1

# 验证特定批次
python cli.py -c config.yaml app-db-verify -n node1 --batch-id jmeter_run_001

# 查看所有批次状态
python cli.py -c config.yaml app-db-status -n node1
```

**5) 清理**

```bash
python cli.py -c config.yaml app-db-teardown -n node1
```

---

### 场景 B：RPO 数据丢失检测

适用于手动触发故障的场景（kill 主节点、断网、主备切换等）。

```bash
# === 阶段1：故障前准备 ===

# 可选：启动业务负载（另一个终端）
python cli.py -c config.yaml run-workload -n node1 -w workload/templates/transfer.yaml &

# 种植 RPO 标记
python cli.py -c config.yaml rpo-plant -n node1
# → 记录 Batch ID，例如：a1b2c3d4e5f6g7h8


# === 阶段2：手动触发故障 ===
# 执行 kill、断网、切换等操作


# === 阶段3：恢复后检测 ===
python cli.py -c config.yaml rpo-check -n node1 -bid a1b2c3d4e5f6g7h8
```

**报告解读：**
- Marker Check：标记丢失数 > 0 → 故障期间有数据丢失
- Row Count Check：delta < 0 → 丢失了对应行数
- Sequence Gap Check：有 gap → 自增序列不连续，丢失了具体 ID 区间的数据

---

### 场景 C：JMeter + RPO 组合（推荐）

最完整的高可用验证流程：

```bash
# 终端1：设置追踪 + 启动 JMeter
python cli.py -c config.yaml app-db-setup -n node1
# JMeter 配置好 sp_track_write 调用后启动
jmeter -n -t my_test.jmx &

# 终端2：种植 RPO 标记
python cli.py -c config.yaml rpo-plant -n node1
# → 记录 batch_id 为 rpo_001

# 终端3：启动巡检监控
python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2

# === 触发故障 ===

# 故障后：
python cli.py -c config.yaml rpo-check -n node1 -bid rpo_001
python cli.py -c config.yaml app-db-verify -n node1
python cli.py -c config.yaml node-compare -a node1 -b node2 --backend native
```

---

## 命令参考

### 通用参数

| 参数 | 说明 |
|------|------|
| `-c, --config` | 配置文件路径，默认 `config.yaml` |
| `--schema` | 数据库 schema（覆盖配置） |
| `-f, --format` | `table`（默认）或 `json` |

---

### node-compare — 节点间数据对比

```bash
python cli.py -c config.yaml node-compare -a <节点A> -b <节点B> [选项]
```

| 选项 | 说明 |
|------|------|
| `-a, --node-a` | 源节点名称（必填） |
| `-b, --node-b` | 目标节点名称（必填） |
| `--backend` | 引擎：`checksum`（默认）、`native`、`pg_comparator`、`data_diff` |
| `-t, --tables` | 逗号分隔的表名，默认全部用户表 |

**引擎选择建议：**

| 场景 | 引擎 |
|------|------|
| 日常巡检、百万行+大表快速扫 | `checksum` |
| 发现问题后定位具体差异行 | `native` |
| 第三方成熟工具 | `pg_comparator` |

**示例：**

```bash
# 日常巡检
python cli.py -c config.yaml node-compare -a node1 -b node2

# 问题排查
python cli.py -c config.yaml node-compare -a node1 -b node2 --backend native -t accounts

# JSON 输出（便于采集）
python cli.py -c config.yaml node-compare -a node1 -b node2 -f json
```

---

### app-db-setup / verify / status / teardown — 应用-DB一致性（JMeter集成）

```bash
# 创建追踪表+存储过程
python cli.py -c config.yaml app-db-setup -n <节点>

# 验证追踪记录（JMeter 运行中或结束后）
python cli.py -c config.yaml app-db-verify -n <节点> [--batch-id <id>] [--no-mark]

# 查看批次摘要
python cli.py -c config.yaml app-db-status -n <节点>

# 清理
python cli.py -c config.yaml app-db-teardown -n <节点>
```

| 选项 | 说明 |
|------|------|
| `-n, --node` | 数据库节点 |
| `--batch-id` | 只验证指定批次（不指定=所有未验证的） |
| `--no-mark` | 验证后不标记为已完成（下次还会验证） |

---

### rpo-plant / rpo-check — RPO 数据丢失检测

```bash
# 故障前
python cli.py -c config.yaml rpo-plant -n <节点> [-t tables] [--marker-count 20]

# 故障后
python cli.py -c config.yaml rpo-check -n <节点> -bid <batch_id> [--teardown]
```

| 选项 | 说明 |
|------|------|
| `-n, --node` | 数据库节点 |
| `-t, --tables` | 追踪的表（默认全部） |
| `--marker-count` | 每表插入的标记数（默认 10） |
| `-bid, --batch-id` | rpo-plant 返回的 batch_id |
| `--teardown` | 检测后删除 RPO 追踪表 |

---

### gen-data — 生成测试数据

```bash
python cli.py -c config.yaml gen-data -n <节点> [--accounts N] [--orders N] [--teardown]
```

| 选项 | 说明 |
|------|------|
| `-n, --node` | 目标节点 |
| `--accounts` | accounts 表行数（默认 1000） |
| `--products` | products 表行数（默认 200） |
| `--orders` | orders 表行数（默认 5000） |
| `--transactions` | transactions 表行数（默认 10000） |
| `--teardown` | 清理模式 |

---

### run-workload — Python 内置负载生成器

```bash
python cli.py -c config.yaml run-workload -n <节点> -w <YAML模板>
```

| 选项 | 说明 |
|------|------|
| `-n, --node` | 目标节点 |
| `-w, --workload` | workload YAML 文件路径 |

---

### schedule — 定时巡检

```bash
python cli.py -c config.yaml schedule --interval <秒> -- <子命令及其参数>
```

**示例：**

```bash
# 每5分钟检查节点一致性
python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2

# 每2分钟验证应用-DB一致性
python cli.py -c config.yaml schedule --interval 120 -- app-db-verify -n node1

# 每1分钟快速巡检（JSON输出到文件）
python cli.py -c config.yaml schedule --interval 60 -- node-compare -a node1 -b node2 -f json >> reports/check.log
```

按 `Ctrl+C` 停止巡检。

---

## 自定义 workload YAML

如果 JMeter 不方便部署，也可以用内置的 workload runner 模拟业务：

```yaml
name: "my_scenario"
description: "我的业务场景"
concurrency: 15              # 并发线程数
duration: 600                # 执行时长（秒）
throttle_ms: 10              # 事务间隔（毫秒，0=无间隔）

tables:                      # 自动建表（可选）
  - name: accounts
    ddl: |
      CREATE TABLE IF NOT EXISTS {schema}.accounts (
          id SERIAL PRIMARY KEY,
          balance NUMERIC(18,2) DEFAULT 0
      )

seed:                        # 初始化数据（可选）
  - |
    INSERT INTO {schema}.accounts (id, balance)
    SELECT g, 10000.00 FROM generate_series(1, 100) AS g

transactions:                # 事务定义
  - name: "transfer"
    weight: 10               # 权重（越大越频繁）
    sql: |
      UPDATE {schema}.accounts SET balance = balance - %(random_float:0.01-1000.00)s WHERE id = %(random_int:1-100)s;
      UPDATE {schema}.accounts SET balance = balance + %(random_float:0.01-1000.00)s WHERE id = %(random_int:1-100)s

  - name: "deposit"
    weight: 3
    sql: |
      UPDATE {schema}.accounts SET balance = balance + %(random_float:0.01-5000.00)s WHERE id = %(random_int:1-100)s
```

**SQL 占位符：**

| 占位符 | 替换结果 |
|--------|---------|
| `%(random_int:N-M)s` | N~M 的随机整数 |
| `%(random_str:N)s` | N 位随机字符串 |
| `%(random_float:N-M)s` | N~M 的随机浮点数 |
| `%(uuid)s` | UUID |
| `%(timestamp)s` | 当前时间戳 |

注意：`{schema}` 会在运行时被替换为 config 中配置的 schema。

---

## 典型测试流程总结

### 主备切换验证

```
JMeter发压 ─────────────────────────────────────────────▶
                │                           │
     app-db-setup                    app-db-verify
     rpo-plant                       rpo-check
                │                           │
                ├── 触发主备切换 ──────────┤
                │                           │
     schedule ──┤── node-compare 每5min ────────────────▶
```

### 故障恢复验证

```bash
# 1. 准备
python cli.py -c config.yaml app-db-setup -n node1

# 2. 启动 JMeter（已配置 sp_track_write）

# 3. 启动巡检
python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2 &

# 4. 种 RPO 标记 + 记 batch_id
python cli.py -c config.yaml rpo-plant -n node1

# 5. 手动触发故障

# 6. 恢复后验证
python cli.py -c config.yaml rpo-check -n node1 -bid <batch_id>
python cli.py -c config.yaml app-db-verify -n node1
python cli.py -c config.yaml node-compare -a node1 -b node2 --backend native

# 7. 对比三个报告结果，形成结论
```

---

## 配置文件参考

配置文件 `config.yaml` 已包含详细的中文注释，直接打开查看即可。主要包括：

- `databases` — 集群各节点连接信息
- `comparison` — 对比参数（schema、表范围、分块大小、并行度）
- `rpo` — RPO 检测参数
- `workload` — 内置负载生成器默认参数
- `schedule` — 定时巡检参数
- `logging` — 日志配置

---

## 常见问题

**Q: 工具必须要联网才能用吗？**
A: 不需要。内置的 checksum 和 native 引擎不需要任何外部工具。只需 `pip install` 安装 psycopg2、pyyaml、tabulate 三个纯 Python 包即可。可以离线安装（见上方安装说明）。

**Q: JMeter 怎么调用 sp_track_write？**
A: 在 JMeter 的 JDBC 请求后添加一个 JDBC PostProcessor，Query Type 选 `Select Statement`，SQL 写 `SELECT public.sp_track_write(...)`。参数用 JMeter 变量 `${var}` 传递。

**Q: 对比大表很慢怎么办？**
A: 使用 `checksum` 引擎（默认），它分块计算 MD5 比全量扫描快很多。可调整 `chunk_size`（加大=减少查询次数）和 `parallel`（加大=提高并发）优化。

**Q: 定时巡检的日志在哪？**
A: 默认输出到 `./logs/comparator.log`，可在 config.yaml 中配置。

**Q: 多个节点怎么对比？**
A: 两两对比，例如一主两备：
```bash
python cli.py -c config.yaml node-compare -a node1 -b node2
python cli.py -c config.yaml node-compare -a node1 -b node3
```
