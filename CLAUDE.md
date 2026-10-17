# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

数据库集群（Kingbase/PostgreSQL）一致性校验 CLI 工具，用于高可用测试场景中的数据一致性验证。支持 Kingbase 的 Oracle / PostgreSQL / MySQL / SQLServer 四种兼容模式。

## 常用命令

```bash
# 生成配置文件
python cli.py init-config

# 配置与环境自检（模式 / 主备 / 可达性 / 表数 / LSN）
python cli.py -c config.yaml check-config

# 表结构对比（pgdiff 外部工具，与数据对比分开）
python cli.py -c config.yaml schema-compare -a node1 -b node2

# 列出全部引擎及其可用性（数据对比 + 结构对比）
python cli.py -c config.yaml list-backends

# 两节点对比
python cli.py -c config.yaml node-compare -a node1 -b node2

# 任意多节点矩阵对比（自动定位异类节点）
python cli.py -c config.yaml node-compare --nodes node1,node2,node3
python cli.py -c config.yaml node-compare --nodes node1,node2,node3 --primary node1

# 对比时排除表 / schema / 库（显式 -t 时同样生效）
python cli.py -c config.yaml node-compare -a node1 -b node2 --exclude-tables 'tmp_*'

# 应用-DB一致性追踪（JMeter 集成）
python cli.py -c config.yaml app-db-setup -n node1
python cli.py -c config.yaml app-db-verify -n node1

# JMeter 结果对账（解释"客户端比数据库少"）
python cli.py -c config.yaml jtl-reconcile -n node1 --jtl results.jtl

# RPO 数据丢失检测
python cli.py -c config.yaml rpo-plant -n node1
python cli.py -c config.yaml rpo-check -n node1 -bid <batch_id>

# 生成测试数据
python cli.py -c config.yaml gen-data -n node1 --accounts 5000

# 定时巡检
python cli.py -c config.yaml schedule --interval 300 -- node-compare --nodes node1,node2,node3
python cli.py -c config.yaml schedule --interval 60 --converge 3 -- node-compare -a node1 -b node2

# 离线安装依赖（内网 Linux 服务器）
pip install --no-index --find-links=./offline_pkgs -r requirements.txt
```

项目没有测试套件和 lint 配置。临时的验证脚本放在 `deepseek/`，不与工具代码混合。

## 核心架构

### 分层结构

```
cli.py (argparse 子命令路由)
  ├── core/         业务逻辑（节点对比、矩阵、一致性判定、应用-DB、JTL 对账、RPO）
  ├── backends/     可插拔对比引擎（checksum、native、pg_comparator、data_diff）
  ├── workload/     测试数据生成 + 自定义负载执行
  └── utils/        配置、连接池、值归一化、范围规则、日志
```

### 子命令路由模式

`cli.py` 使用 `_build_cmd_map()` 返回命令名→函数的字典，同时支持 `node_compare` 和 `node-compare` 两种格式。`main()` 和 `cmd_schedule()` 都通过 `cmd_map.get(parsed.command.replace("-", "_"))` 查找处理器。

**关键细节：** `cmd_schedule()` 使用 `argparse.REMAINDER` 捕获 `--` 后的子命令参数，然后重新调用 `_build_parser().parse_args()` 解析。注意 REMAINDER 会原样传入 `--`，需要先 strip 掉。**不能**使用 `parsed.func`（argparse Namespace 没有这个属性）。

通用参数（`-c` / `--schema` / `-f`）通过一个 `add_help=False` 的父解析器同时挂在顶层与每个子命令上，因此在子命令前后都能写。

### 退出码三态

`node-compare` 用退出码区分三种结论，自动化脚本需要据此分流：

| 码 | 含义 |
|---|---|
| 0 | `CONSISTENT` — 所有参与节点读到相同数据 |
| 1 | `DIVERGENT` — 确认不一致 |
| 2 | `INCONCLUSIVE` — 结论不完整（有节点读不到，或差异块无法定位多数派） |

### 数据库连接池 (`utils/db.py`)

`DatabasePool` 的核心行为：

- **`cursor(name, autocommit=False)`** — 上下文管理器。`autocommit=True` 时在退出时调用 `conn.commit()`；默认 `False` 时**不提交**。**写事务必须传 `autocommit=True`。**
- **`execute(name, sql, params, fetch=True)`** — `params` 是 list-of-tuples 时调用 `executemany()`。`autocommit=not fetch`。
- **`execute_ddl(name, sql)`** — 临时设置 `conn.autocommit = True`，执行完恢复。
- **`stream_cursor(name, sql, params, itersize)`** — 服务端（命名）游标，用于流式读取大表。**使用独占连接**，因为命名游标必须活在事务里，多线程通过共享连接并发流式读取会互相回滚对方的事务。
- **`resolve_table(name, schema, table)`** — 返回表的真实存储名，按原名/小写/大写依次尝试；找不到抛 `TableNotFound`。`get_primary_key` / `get_column_info` 内部都先调用它。
- **`get_key_bounds`** / **`table_change_counter`** / **`current_wal_lsn`** / **`last_wal_replay_lsn`** / **`is_in_recovery`** — 矩阵分块与一致性判定使用。
- **`_connect(name, retries=2)`** — 打开连接；可重试的 OperationalError 会自动重试 2 次（认证/配置类错误不重试）。集群故障测试中网络抖动是常态，单次失败不应中断整轮。
- **`get_dsn_parameters()` 不存在**（`psycopg2` 的 connection 对象上有同名方法，但 `DatabasePool` 没有暴露）。需要密码传给外部工具时从 `pool._config["databases"][name]["password"]` 读取。

### Kingbase 兼容模式差异（实测，V009R001C010）

四套集群（oracle 53261 / mysql 53262 / sqlserver 53263 / pg 53264）实测得到。
**sqlserver 模式与其余三种差异最大**，以下每一条都已在代码中适配：

| 差异 | 影响 | 处理方式 |
|---|---|---|
| `autocommit=False` 时 psycopg2 发出裸 `BEGIN`，被拒绝（`syntax error at end of input`） | 所有命令在第一个语句就失败 | `get_conn` 探测 `SHOW database_mode`，sqlserver 模式设 `autocommit=True` |
| psycopg2 命名游标不可用（它按 `conn.autocommit` 判断） | `stream_cursor` 全挂 | 改用 `DECLARE ... WITH HOLD` + `COMMIT` + 分批 `FETCH`（`_HoldCursor`） |
| **运行时错误被静默吞掉**：不存在的表、除零、主键冲突都不抛异常 | 查询失败会被当成"零行"，结论完全不可信 | `_check_statement` 用空的 `statusmessage` 判定失败；`@@ERROR` 只能拿到数字码，没有错误文本 |
| `TIMESTAMP` 是 T-SQL 的 rowversion，不能带 `DEFAULT` | 所有带 `DEFAULT CURRENT_TIMESTAMP` 的建表失败 | `pool.timestamp_type(node)` 返回 `DATETIME` |
| 不支持 `\|\|` 字符串拼接 | RPO 探针 INSERT 失败 | 改用 `concat(concat(a,b),c)`（四模式通用；`+` 在 oracle/pg 不通用） |
| `ALTER TABLE ... ADD COLUMN` 语法错误 | 追踪表迁移失败 | 用 `ALTER TABLE t ADD <col> <type>`（不带 `COLUMN`），先查列是否存在 |
| **plpgsql 函数体内任何 INSERT 都失败** | `sp_track_write` 不可用 | sqlserver 模式不创建该函数，`app-db-setup` 打印普通 INSERT 模板 |
| plpgsql `INTO` 赋值不支持（`SELECT INTO` / `RETURNING INTO`） | 函数无法返回值 | `sp_track_write` 改为 `RETURNS VOID` |
| `= ANY(array)` 不支持 | 批量查询/更新失败 | 改用 `IN (...)` 动态占位符 |

**其余三种模式**：`TIMESTAMP`/`SERIAL`/`JSONB`/`BOOLEAN`/`plpgsql`/`EXCEPT`/`LIMIT`/
`string_agg` 均可用；pg 模式只有 `pg_*` 函数（无 `sys_*`、无 `listagg`/`wm_concat`）；
mysql 模式无 `plsql`；oracle 模式 `''` 与 NULL 不可区分。

### 值归一化 (`utils/normalize.py`)

**跨节点比较必须先过这一层**，否则会产生大量假差异。归一化把「值 + 列类型」映射成规范字节串，同时被 checksum 引擎和 app-DB 校验共用。

实测确认的陷阱（详见文件头部注释）：numeric 精度（`1.0` vs `1.00`）、`char(n)` 补空格、timestamptz 时区、Oracle 模式下 DATE 映射为 `datetime`、以及 **Oracle 模式下 `''` 与 `NULL` 无法区分**。

编码用**长度前缀**而非分隔符拼接——旧实现用 `'|'` 拼接，值里含 `|` 时会产生碰撞。

### 范围规则 (`utils/scope.py`)

三层排除：database → schema → table，支持 fnmatch 通配符，**排除优先于包含**。

**关键不变式：排除规则必须无条件生效。** 无论 `tables`（即 `-t`）是否为 None，都要走 `matcher.filter_tables()`。旧实现只在 `tables is None` 时应用排除，导致显式 `-t` 时排除规则完全失效。

工具自用表 `_rpo_markers` / `_rpo_snapshots` / `_app_db_tracking` 在 RPO 路径中被**无条件排除**（不看配置），因为它们在校验过程中本来就在增长。

### 对比引擎选择

**数据对比**（`node-compare --backend`）：

| 引擎 | 文件 | 适用场景 | 多节点 |
|------|------|---------|--------|
| `checksum` | `backends/checksum.py` | 大表快速扫描，按**主键区间**分块，Python 侧哈希 | 支持 |
| `native` | `backends/native.py` | 小表全表逐行对比，Python 侧归并连接 | 支持 |
| `pg_comparator` | `backends/pg_comparator.py` | 外部工具，两两对比 | 不支持 |
| `data_diff` | `backends/data_diff.py` | 外部工具，**跨数据库类型**对比，两两 | 不支持 |

**结构对比**（`schema-compare`）：

| 引擎 | 文件 | 说明 | 多节点 |
|------|------|------|--------|
| `pgdiff` | `backends/pgdiff.py` | 外部工具（Go 二进制，直连数据库取元数据，不依赖 `pg_dump`），对比表定义 / 列 / 索引 / 约束 | 不支持 |

**`pgdiff` 独立成 `schema-compare` 命令，不是 `node-compare --backend` 的一员。** 它回答的是另一个问题（表结构 vs 数据），混在一起会让"表结构不同"看起来像"数据不同"。三个外部工具都由 `node_comparator.BACKENDS` / `SCHEMA_BACKENDS` 注册，`list-backends` 会按类型分组列出并显示是否已安装。

**外部工具的调用方式容易写错，改这些文件前先核对官方 CLI：**

- `pgdiff` 用**大小写成对**的参数区分两侧（第一侧 `-H/-P/-D/-U/-S`，第二侧 `-h/-p/-d/-u/-s`），且**每种对象类型单独调用一次**（`TABLE` / `COLUMN` / … / `ALL`）。它不接受 schema dump 文件。
- `data-diff` 的表名是**位置参数**（`<src_uri> <表> <tgt_uri> <表> -k 主键`），没有 `--table` 选项；不传 `-k` 无法对齐行，所以无主键的表会被跳过。
- `pg_comparator` 接受两个 **URL 风格的位置参数**（`[driver://][login[:pass]@][host][:port]/db/[schema.]table[?key]`），**没有 `-c` 选项**。它的 `-S`/`--synchronize` 与 `-D`/`--do-it` 会**写入目标库**，代码中显式排除，不要添加。

三者的连接 URL 都要对凭据做百分号转义，否则密码里的 `@` / `:` / `/` 会改变 URL 结构。

新增引擎时需要同时改三处：`BACKENDS`（或 `SCHEMA_BACKENDS`）字典、`run_node_comparison` / `run_multi_node_comparison` 的 dispatch 分支、以及 `cli.py` 的 `--backend choices`。

**checksum 的两条关键设计约束：**

1. **分块边界必须由基准节点统一产生，所有节点复用同一组边界。** 绝不能让各节点按自己的数据切分。块语义是"主键落在某个值区间内的行"，与行号无关。
2. **哈希在 Python 侧算。** 不是因为数据库不支持（实测 Oracle 模式下 md5/string_agg 都可用），而是因为同一逻辑值在不同声明下文本不同（`1.0::numeric` → `'1.0'`，`1::numeric(10,2)` → `'1.00'`），服务端哈希会把相同数据报成不同。

块内用**顺序无关累加器**（`Σ H(row) mod 2^128`），因为行到达顺序取决于 collation。用求和而非异或：异或下重复行会自我抵消。

无主键的表降级为 row-count-only 对比，并在结果中标注 `confidence: row_count_only`。不可排序的主键类型（bytea/json）退化为整表单累加器。

### SQL 标识符引用

多个模块使用 `_quote_ident(name)` / `utils.db.quote_ident(name)`：

```python
def quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')
```

**所有用户提供的 schema/table/column 名都必须经过它**；值一律走绑定参数。

### 一致性判定 (`core/consistency.py`)

两个节点是两条独立连接 = 两个不同时刻的快照。持续写入下"两个查询结果相同/不同"的证据力很弱，因此工具给出置信级别而非笼统的 PASS/FAIL：

| 级别 | 含义 |
|---|---|
| `deterministic` | 读取期间确认无写入，两侧看到同一状态 |
| `snapshot` | 两侧读取时刻不同 |
| `weak` | 仅凭多轮一致，对具体时刻无证明力 |
| `inconclusive` | 环境不支持给出结论 |

**读取期零写入检查**是最有价值的判据：读取前后各取一次主库 LSN，相等即证明整个读取期间主库无写入。代码上注意，`quiesced`（开始时拿到静默窗口）**不能覆盖**读取期间检测到写入这一更强反证。

各手段能力边界：水位线对 UPDATE/DELETE **无效**（主键不变，行仍在水位线内且在变）；静默窗口是唯一覆盖增删改三种操作的手段；**共享存储集群（RAC）不适用 LSN 判据**。

### 矩阵对比 (`core/matrix_comparator.py`)

每节点只扫描一次（N 次而非 N(N-1) 次），得 `{table: {chunk: 摘要}}` 矩阵后横向比对。**首先是正确性改进**：观测窗口从 N(N-1) 次读取的跨度缩到 N 次，减少了"一侧还没追上"造成的差异。

**多数派表决的边界：** 表决假设各节点错误相互独立，但复制拓扑下主库的错误会传播到所有从库，工具可能把唯一正确的节点报成异类。因此输出必须附带这条警告，并提供 `--primary` 作为更可靠的替代。偶数节点平票（2:2）时**不猜测**，报 `UNRESOLVED`。

**失败节点不参与表决**，标记 `UNREACH`，只让受影响的表降级为 `INCONCLUSIVE`。判断"是否所有节点都参与"时要对着**全部节点**比，而不是对着可达节点比——否则节点全挂时光看可达集合会误判成 CONSISTENT。

### 应用-DB 追踪 (`core/app_db_comparator.py`)

追踪表 `_app_db_tracking` + 存储函数 `sp_track_write`。

**关键不变式：业务写入与追踪记录必须在同一个事务内。** JMeter 侧把 JDBC Connection Configuration 的 autocommit 关掉，业务 SQL 后跟追踪调用，最后统一 COMMIT/ROLLBACK。业务 SQL 本身不需要改动。

**注意纠正一个常见误解：PostProcessor 不是独立线程**，它在业务线程内、Sampler 之后顺序执行，并复用同一连接池的连接。旧方案的问题不是"线程分开"，而是 autocommit 开着。

`sp_track_write` 支持 `p_txn_id`（同一业务事务的多个写操作填同一个值）与 `p_request_id`（一个请求一个唯一值）。前者用于**撕裂事务检测**，后者用于 JTL 对账。两者都可为 NULL。

值比较走 `normalize.compare_rows`，不用 `str()`——后者会把 `Decimal('100.00')` 与 `Decimal('100.0')`、`True` 与 `1` 判成不一致。

### JTL 对账 (`core/jtl_reconciler.py`)

按 `request_id` 把 JMeter 结果与数据库状态关联成四象限。**只有"客户端报成功但数据不在"才是需要告警的缺陷**；"客户端报失败但数据已落库"是提交成功、响应丢失导致的假失败，正是"客户端记录少于数据库"这一现象的成因。

反向孤儿扫描的时间窗口优先用业务表的时间戳列界定；没有时间列时回退到主键区间，并且**必须说明其局限**——主键区间只能发现区间内部的未追踪行。窗口只用真实存在于数据库中的追踪主键来定界，否则一条指向虚构主键的记录会把范围拉到整张表。

### RPO 检查 (`core/rpo_checker.py`)

两种手段：复制流探针（标记）+ 行数与主键守恒。

探针写在工具自己的表里，**不写业务表**（会污染被测数据、撞 NOT NULL/外键约束），因此探针丢失是**关于复制流的结论，不针对某张业务表**。每个探针记录本批次的总数——只记序号的话，删掉最后几个只会让"总数"一起变小，缺失永远报不出来。

`check_conservation` 取代了旧的 `generate_series` 空洞扫描。旧做法把每次正常删除、每次序列跳号（`nextval` 回滚不回收）都报成数据丢失，且主键从一亿开始时先生成一亿行再关联。

**工具无法区分「业务正常删除」与「故障丢失」**，这一点必须在输出中说明，不能含糊。

### 关键注意事项

- `os.makedirs("")` 在 Python 3.8 上会抛出 `FileNotFoundError`，创建目录前需检查路径非空。
- `logger.py` 中 `logging.FileHandler` 不存在时会自动创建文件，但不会自动创建目录。
- `workload/runner.py` 的 `_worker` 多线程模型共享同一个数据库连接 — 已知限制，不适合超高并发。
- JMeter 集成：`sp_track_write()` 通过 JDBC PostProcessor 调用，需使用独立的 JDBC Connection Configuration 避免连接池争用。
- **日志走 stderr，命令结果走 stdout**（`utils/logger.py` 的 console handler 绑定 `sys.stderr`）。这样 `-f json` 的输出可以直接管道给 `jq`；若把日志改回 stdout，JSON 流会被日志行污染而无法解析。
- `cli.py` 的 `cmd_schedule` 在轮次之间用 `contextlib.redirect_stdout` 捕获子命令输出以生成报告；收敛指纹**不能用退出码充数**——两个不同的失败都返回 1，只看退出码会让收敛判定建立在错误的前提上。
