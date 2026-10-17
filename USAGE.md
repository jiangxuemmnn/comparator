# 使用说明

## 环境要求

- Python 3.11+
- Kingbase（Oracle / PostgreSQL / MySQL / SQLServer 任一兼容模式）或 PostgreSQL
- 操作系统：Linux

## Kingbase 兼容模式

四种模式（Oracle / PostgreSQL / MySQL / SQLServer）均已完成实测，命令用法完全相同，
差异只在内部实现。工具会在连接时自动探测数据库模式，不需要额外配置。

| 能力 | oracle | mysql | sqlserver | pg |
|---|---|---|---|---|
| 节点对比（checksum / native） | ✅ | ✅ | ✅ | ✅ |
| 多节点矩阵 + 异常定位 | ✅ | ✅ | ✅ | ✅ |
| 一致性判定（LSN / 变更计数） | ✅ | ✅ | ✅ | ✅ |
| 应用-DB 追踪 | ✅ 存储函数 | ✅ 存储函数 | ✅ **普通 INSERT** | ✅ 存储函数 |
| RPO 检测 | ✅ | ✅ | ✅ | ✅ |
| 定时巡检 | ✅ | ✅ | ✅ | ✅ |

**sqlserver 模式的两点特别说明**：

1. `app-db-setup` 不会创建 `sp_track_write`，而是打印一条等价的普通 INSERT
   语句给 JMeter 使用——该模式下 Kingbase 不支持 plpgsql 函数体内的 INSERT。
2. 该模式不返回运行时错误的详细信息（只给数字错误码）。工具会检测到失败
   并报错，但错误原因需要结合数据库日志排查。

## 安装（内网环境可用）

内置的 `checksum` 与 `native` 两种引擎不需要任何外部工具。

```bash
# 1. 在有网络的机器上下载依赖包
pip download -d ./offline_pkgs -r requirements.txt \
    --platform manylinux2014_x86_64 \
    --python-version 311 \
    --only-binary=:all:

# 2. 将整个 comparator 目录 + offline_pkgs 拷到内网机器

#    离线包必须与目标机的 Python 版本和平台一致。本仓库 offline_pkgs/
#    内是 cp311（Python 3.11）+ manylinux2014_x86_64 的 wheel。
#    目标机版本不同时，用相同的 --python-version / --platform 重新下载。

# 3. 在内网机器上离线安装
pip install --no-index --find-links=./offline_pkgs -r requirements.txt
```

---

## 快速开始

```bash
# 1. 生成配置模板
python cli.py init-config

# 2. 编辑 config.yaml，至少填写 databases 下的节点连接信息

# 3. 生成一点测试数据（在可写的节点上）
python cli.py -c config.yaml gen-data -n node1 --accounts 5000

# 4. 对比（一主两备一次比完）
python cli.py -c config.yaml node-compare --nodes node1,node2,node3
```

---

## 一、节点间数据对比

### 1.1 任意多节点（没有数量上限）

工具对节点数量**没有上限**，只要求至少 2 个。主机数量、是否一主多备、
备库有几个都不影响；节点只需在 `config.yaml` 的 `databases` 下各配一项。
实测过 2 / 3 / 4 / 5 个节点，命令行与输出形式一致。

矩阵模式每个节点只扫描一次，所以节点越多相对收益越大（N 个节点是 N 次扫描，
逐对比则是 N×(N-1) 次）。

```bash
# 两两对比（传统用法，输出为两列）
python cli.py -c config.yaml node-compare -a node1 -b node2

# 任意多节点矩阵对比（N 个节点只扫描 N 次，而不是 N*(N-1) 次）
python cli.py -c config.yaml node-compare --nodes node1,node2,node3

# 指定基准节点，其余节点与它逐一比对
python cli.py -c config.yaml node-compare --nodes node1,node2,node3 --primary node1
```

矩阵模式的输出会直接指出**哪个节点是异类**：

```
+---------------------+---------+---------+---------+------------+
| Table               | node1   | node2   | node3   | 结论       |
+=====================+=========+=========+=========+============+
| public.accounts     | OK      | OK      | DIFF    | DIVERGENT  |
| public.orders       | OK      | OK      | OK      | CONSISTENT |
+---------------------+---------+---------+---------+------------+

异类节点:
  node3    1/4 chunks (25.0%)  表: public.accounts
           需要人工判断：既非明显的复制延迟，也非大面积损坏
```

**多数派表决的适用边界（重要）**：表决假设各节点的错误是**相互独立**的。而在复制拓扑下并非如此——主库写错会传播到所有从库，此时工具可能反过来把唯一正确的节点报成异类。因此：

- 已知复制拓扑时，请用 `--primary <节点>` 显式指定基准，这比表决可靠
- 使用表决时，工具会在输出末尾附带这条警告
- 节点数为偶数出现平票（如 4 节点的 2:2）时，工具**不会猜测**，而是报告"无严格多数派"

### 1.2 三种结论状态与退出码

| 结论 | 含义 | 退出码 |
|---|---|---|
| `CONSISTENT` | 所有参与的节点都读到了相同数据 | 0 |
| `DIVERGENT` | 节点之间确实不一致 | 1 |
| `INCONCLUSIVE` | **结论不完整**——有节点没读到，或有 chunk 无法定位多数派 | 2 |

退出码 2 与 1 分开，是为了让 CI / 自动化脚本能区分「数据不一致」与「环境有问题」，这两者的处理方式完全不同。

**单个节点失联不会让整批结果变成"不一致"**：失联节点标记为 `UNREACH`、不参与表决，其影响的表结论降级为 `INCONCLUSIVE`。任何时刻单节点失败都会先重试 2 次。

### 1.3 对比引擎

```bash
python cli.py -c config.yaml list-backends
```

**内置的两个引擎覆盖日常全部场景，正常情况下不需要外部工具。** 下面两个是备选，
只在有特殊对比需求时才用得上：

| 引擎 | 说明 | 来源 | 多节点 |
|---|---|---|---|
| `checksum`（默认） | 按主键区间分块算哈希，快；发现差异后会**自动定位到具体行** | 内置 | 支持 |
| `native` | 全表逐行精确对比，适合小表深度排查 | 内置 | **不支持**（用 `-a/-b`） |
| `pg_comparator` | 成熟的行级对比工具，可作独立交叉验证 | 外部，可选 | **不支持** |
| `data_diff` | 支持**跨数据库类型**对比（如 Kingbase 对 MySQL） | 外部，可选 | **不支持** |

外部引擎是两两对比工具，用 `--nodes` 指定 3 个以上节点时工具会**明确报错**并给出替代方案，而不会静默地跑 N*(N-1) 次。未安装时退出码为 `2`（未能完成），与"发现不一致"（`1`）区分开。

未安装的外部工具在 `list-backends` 中显示为 `NOT INSTALLED`，调用时返回可读提示。

### 1.4 表结构对比（schema-compare）

`pgdiff` 对比的是**表结构**（表定义、列、索引、约束），不是数据，因此单独成命令：

```bash
python cli.py -c config.yaml schema-compare -a node1 -b node2
python cli.py -c config.yaml schema-compare -a node1 -b node2 --schemas public
python cli.py -c config.yaml schema-compare -a node1 -b node2 --schema-type TABLE
```

**这也是可选的备选手段**——只在需要独立核对表结构时才用。`pgdiff` 是外部工具，
需先安装（<https://github.com/joncrlsn/pgdiff>）。它**直连两个数据库**自己取元数据，
不依赖 `pg_dump`，也不是 Python 包。未安装时会给出明确的错误提示，退出码为 `2`。

`pgdiff` 每次只对比**一个 schema**、**一类对象**。默认 `--schema-type ALL` 按依赖顺序
把表、列、索引、视图、外键、函数等都对比一遍；只关心某一类时（比如排查看表结构有没
有同步）可以单独指定，快得多。

**不要把 schema 差异当成数据差异**：一次主备切换如果 DDL 没有同步，
`schema-compare` 会报出来，而 `node-compare` 的数据对比可能因为两边列不同
而失败或给出误导性结果。建议先做 schema 对比。

```bash
python cli.py -c config.yaml list-backends   # 列出全部引擎及其可用性
```

### 1.4 差异定位

`checksum` 引擎在报告差异块的同时，会把块内的行逐行比对，直接给出：

```
[DIVERGENT] public.accounts
    - Chunk 0: 分组=[['node1', 'node2'], ['node3']] 异类=['node3'] 行数={'node1': 1000, ...}
    chunk 0 对比 ['node1', 'node3']:
       仅前者有: [9999]
       仅后者有: []
       值不同  : [42]
```

- `仅前者有`：该行只在前一个节点存在（如 node1 新增了一行，node2 还没复制到）
- `仅后者有`：反向
- `值不同`：两侧都有这一行，但内容不同（UPDATE 场景）

---

## 二、校验范围：排除库 / schema / 表

### 2.1 配置方式

在 `config.yaml` 里用 `scopes` 段描述规则，支持 shell 通配符：

```yaml
scopes:
  # test_db 里 10 张表，排除其中 3 张
  - database: test_db
    include_schemas: [public, biz]
    exclude_schemas: [tmp_*]
    exclude_tables: [tmp_*, "*_bak", _rpo_*, _app_db_*]

  # 整个库跳过
  - database: audit_db
    exclude_tables: ["*"]
```

规则：

- 三层依次判断：**database → schema → table**
- **排除优先于包含**
- 包含列表留空 = 全部包含（再减去排除的）
- `database` 留空 = 适用于所有库
- 模式不区分大小写（`-t Accounts` 能匹配到实际存为 `accounts` 的表）

未配置 `scopes` 时，会自动用旧的 `comparison.schema` + `comparison.exclude_tables` 合成一条默认规则，**老配置文件无需改动**。

### 2.2 命令行覆盖

```bash
python cli.py -c config.yaml node-compare -a node1 -b node2 \
    --exclude-tables 'tmp_*' --exclude-schemas 'tmp_*' --exclude-databases 'archive_*'
```

### 2.3 一个容易踩的坑（已修复）

排除规则**在显式指定 `-t` 时同样生效**。也就是说：

```bash
python cli.py -c config.yaml node-compare -a node1 -b node2 -t accounts,tmp_x
```

其中 `tmp_x` 若命中排除规则，会被跳过。旧版本在这种情况下会忽略排除规则，把临时表也一并比对。

### 2.4 工具自用表的处理

`_rpo_markers` / `_rpo_snapshots` / `_app_db_tracking` 是工具自己创建的表，会被**无条件排除**，不看配置。它们在校验过程中本来就在增长，纳入对比会产生无意义的差异。

---

## 三、结论的置信级别（任务关键）

### 3.1 为什么需要它

两个节点是两条独立连接，也就是**两个不同时刻的快照**。业务在持续写入时，这个时间差本身就会制造差异。所以"两个查询结果相同/不同"这件事，在持续写入下**证据力很弱**——可能只是各自读到了不同的时刻。

工具不会把这件事掩盖成一个 PASS/FAIL，而是明确给出本轮结论属于哪一级：

| 级别 | 含义 |
|---|---|
| `deterministic` | 读取期间确认**无写入发生**，两侧看到的是同一状态。差异是真实的，相等也是真实的 |
| `snapshot` | 两侧读取时刻不同。对只增不改的表，相等有意义；不等可能是复制延迟 |
| `weak` | 仅凭"连续几轮结果一致"，对任何具体时刻都没有证明力 |
| `inconclusive` | 环境不支持给出结论 |

命令输出末尾会直接打印：

```
一致性判定:
  置信级别: deterministic   方式: quiesce
    - 读取期间主库 LSN 未变化（0/C4D78C8）
```

### 3.2 工具是如何取得"可比较窗口"的

| 手段 | 对 INSERT | 对 UPDATE | 对 DELETE | 失效条件 |
|---|---|---|---|---|
| 静默窗口（等业务停顿） | 有效 | 有效 | 有效 | 业务无法停顿 |
| 水位线（只比 `pk <= W`） | 有效 | **无效** | **无效** | 主键非单调 / 无主键 / 复合主键 |
| LSN 追平 | 有效 | 有效 | 有效 | 逻辑复制 / 多主 / RAC / 无主库连接 |
| 收敛判定 | 弱 | 弱 | 弱 | 两节点以同样方式都错 |

**最关键的判据是"读取期零写入检查"**：工具在读取前后各取一次主库 LSN，若两者相同，说明整个读取过程期间主库没有写入——这等价于一个极短的静默窗口，且几乎不花代价。这也是 `deterministic` 级别的来源。

水位线对 UPDATE 和 DELETE **无效**：UPDATE 不改主键，被改的行仍在水位线以内且仍在变化；DELETE 同理。要覆盖增删改三种操作，只能用静默窗口。

### 3.3 相关参数

```yaml
consistency:
  mode: auto            # auto | quiesce | watermark | loose
  quiesce_seconds: 5    # 连续 N 秒无变更才认为进入静默窗口
  lsn_timeout: 30       # 等待备库追平的超时
```

```bash
--wait-quiesce 10         # 覆盖静默窗口秒数
--consistency-mode auto   # 覆盖判定方式
--no-consistency          # 跳过判定（输出中不含置信级别）
--watermark 100000        # 水位线：只比较主键不超过该值的行
```

### 3.4 工具会明确报告"无法判定"的场景

以下场景工具**不会给出 PASS/FAIL**，而是标记为结论不完整并说明原因：

| 场景 | 工具输出 |
|---|---|
| 表无主键 | `确定性: 无（无主键，仅行数比对；行数相同不代表内容相同）` |
| 主键非单调（UUID / 随机） | 分块仍正确，但水位线不适用 |
| 复合主键 | 水位线不适用 |
| 业务持续写入，未等到静默窗口 | `置信级别: snapshot`，并说明 UPDATE/DELETE 的差异无法与复制延迟区分 |
| LSN 不可用 / 无主库连接 | 降级并说明 |
| 共享存储集群（RAC） | LSN 判据**不适用**（无 WAL 重放概念），工具会说明 |
| 校验期间检测到变更 | 置信级别降为 `snapshot`，并指出 LSN 推进 |
| 单节点连接失败 | 该表 `INCONCLUSIVE`，退出码 2 |
| 表结构不一致 | 报错，不进行比较 |

**`pg_export_snapshot()` 跨节点不可用**——快照标识符是单个实例内存中的注册项，且导出会话必须存活，而故障测试中连接中断是常态。不要指望用导出的快照对齐两个实例。

---

## 四、应用-DB 一致性校验（JMeter 集成）

### 4.1 为什么原方案会错位

旧方案 `业务 SQL → PostProcessor → sp_track_write` 中，业务写入与追踪写入是**两个独立事务**（JDBC 默认 autocommit），中间有一个故障可以插进去的缝：

| 时序 | 业务 | 追踪记录 | 工具判断 | 真相 |
|---|---|---|---|---|
| 业务提交 → 故障 → 追踪未写 | 已落库 | 缺失 | 静默通过 | 数据一致，虚惊 |
| 业务未提交 → 追踪却写了 | 未落库 | 有 | **误报"数据丢失"** | 数据一致，误判 |
| 客户端整体崩溃 | 可能已落库 | 整批缺失 | 静默通过 | 客户端记录 < DB |

**关键澄清：PostProcessor 不是独立线程。** 它在当前业务线程内、Sampler 之后顺序执行，并且复用同一个 JDBC Connection Configuration 的连接（JMeter 的连接池按线程分配）。所以"业务线程和追踪线程分开"并没有发生——真正的问题是 **autocommit 开着**。

### 4.2 改造方式（业务 SQL 一行都不用改）

**先澄清一个容易写错的点：JMeter 没有叫 "JDBC PostProcessor" 的元件。**
后置处理器里只有 JSR223 / 正则提取 / JSON 提取等，没有 JDBC。要在业务 SQL
之后写追踪记录，用**第二个 JDBC Request Sampler**（或 JSR223 PostProcessor
直接调 JDBC API）。下面的写法是实测通过的。

一个线程内按顺序放三个 Sampler，共用同一个 JDBC 连接：

```
JDBC Connection Configuration:  Autocommit = false,  Max Number of Connections = 线程数

Thread Group
├── JDBC Request "business INSERT"        ← 你自己的业务 SQL，原样不动
│   └── JSR223 PreProcessor "generate REQ_ID"   ← 只挂在这个 Sampler 下
├── JDBC Request "track write"            ← SELECT sp_track_write(...)
└── JDBC Request "commit"                 ← Query Type = Commit
```

这样业务变更与追踪记录**在同一个事务里**，要么都成功要么都失败。业务 SQL 的
定制能力完全保留：JMeter 变量、函数、参数化、任意 SQL，一切照旧。

**两个实测踩过的坑：**

1. **`REQ_ID` 必须挂在业务 Sampler 下面，不能放在线程组层级。**
   放在线程组上的 PreProcessor 会在**每个 Sampler 之前**都执行一次，
   结果业务 SQL 用 ID `A`、追踪写入用重新生成的 ID `B`，两者永远对不上
   （实测表现为 `app-db-verify` 全部报 Missing）。
2. **`row_data` 必须与业务 SQL 实际写入的值一致。** 追踪记录里的值是
   "应用声称写了什么"，不是"随便填什么"。实测中把 `v` 写死为 1，
   而业务写入用的是随机值，10 条记录全部判为 mismatch。

可直接使用的模板见 `examples/jmeter_tracking.jmx`（已在 oracle 模式下实测：
2 线程 × 5 循环，10 条业务写入、10 条追踪记录、校验全部通过、JTL 对账四象限正确）。

**sqlserver 模式**：把 `track write` 换成 `app-db-setup` 打印的普通 INSERT 即可。

### 4.3 使用步骤

```bash
# 1) 初始化追踪基础设施（建表 + 建存储函数）
python cli.py -c config.yaml app-db-setup -n node1

# 2) 在 JMeter 中配置（见 4.2），建议同时设置：
#    user.properties:  sample_variables=REQ_ID
#    测试计划中为每个请求生成唯一 REQ_ID，并传给 sp_track_write 的 p_request_id
#    （这样后续才能用 jtl-reconcile 对账）

# 3) JMeter 运行期间或结束后，验证追踪记录
python cli.py -c config.yaml app-db-verify -n node1
python cli.py -c config.yaml app-db-verify -n node1 --batch-id myrun001

# 4) 查看批次摘要
python cli.py -c config.yaml app-db-status -n node1

# 5) 清理
python cli.py -c config.yaml app-db-teardown -n node1
```

**sp_track_write 参数：**

```sql
SELECT public.sp_track_write(
    p_batch_id,      -- 批次标识。注意：不要用 '${__UUID()}'，那会让每次调用都成为新批次
    p_operation,     -- 'INSERT' / 'UPDATE' / 'DELETE'
    p_table_name,    -- 表名，如 'accounts'
    p_pk_values,     -- 主键值，JSON：'{"id": 123}'
    p_row_data,      -- 完整行数据，JSON。DELETE 传 NULL
    p_txn_id,        -- 事务标识：同一业务事务的多个写操作填同一个值
    p_request_id     -- 请求标识：一个 JMeter 请求填一个唯一值
)
```

**sqlserver 兼容模式的差异（重要）**：该模式下 Kingbase 不接受 plpgsql 函数体内的
任何 INSERT，因此 `sp_track_write` 不会被创建。`app-db-setup` 会直接打印可用的
普通 INSERT 模板：

```sql
INSERT INTO public._app_db_tracking
    (batch_id, txn_id, request_id, operation, table_name, pk_values, row_data)
VALUES (?, ?, ?, ?, ?, ?, ?)
```

其余用法完全相同（同样放在业务 SQL 之后、同一事务内、最后统一提交）。
其它三种模式仍然使用 `sp_track_write`，两者行为一致。

### 4.4 撕裂事务检测

若为同一业务事务的多个写操作填了相同的 `p_txn_id`，校验时工具会按事务分组检查：**一个事务的部分写操作落库了、部分没有**，这种"事务被撕裂"的情况是逐行检查看不出来的（每行单独看，要么正常要么就是缺失）。

```
撕裂事务检测:
   txn=T2  1/2 成功  同一事务的部分写操作已落库、部分没有 —— 事务被撕裂
```

> 这是事务一致性校验的第一期能力。第二期计划基于 `xmin` 做事务边界比对，但它在 Oracle / MySQL / SQLServer 兼容模式下不一定可用，且逻辑复制下跨节点不可比。

### 4.5 值比较的归一化

追踪记录里的值来自 JSON，而数据库列有自己的类型。直接按字符串比较会把大量正常数据判成不一致，例如：

- `1.0` 与 `1.00` 是同一逻辑值但文本不同
- `char(10)` 会补空格，`varchar` 不会
- JSON 里的时间是字符串，数据库返回的是时间类型
- JSON 里的数字是浮点，`numeric` 列返回的是 `Decimal`

工具会读取列的实际类型再做归一化比较。**已知限制**：在 Kingbase 的 Oracle 兼容模式下，`''` 与 `NULL` 在客户端无法区分（`SELECT ''::text` 返回的就是 NULL），因此任何依赖区分二者的比较在该模式下都不可靠。

---

## 五、JMeter 结果对账（解释"客户端比数据库少"）

### 5.1 现象与原因

压测中常见：JMeter 报告里成功的数量**少于**数据库实际行数。原因是**提交成功、但响应在返回路径上丢失**（主备切换、连接重置、客户端崩溃）。JMeter 把这次采样记为失败，但数据其实已经落库了。

### 5.2 对账

前提：JMeter 为每个请求生成唯一 `REQ_ID`，既写入 JTL（`sample_variables=REQ_ID`），也作为 `p_request_id` 传给 `sp_track_write`。

```bash
python cli.py -c config.yaml jtl-reconcile -n node1 --jtl results.jtl
python cli.py -c config.yaml jtl-reconcile -n node1 --jtl results.jtl --batch-id myrun001
```

输出是一个四象限表：

```
                    DB 中有数据      DB 中无数据
  JMeter 报成功     3                1
  JMeter 报失败     1                1

[应答丢失 / 假失败] 1 条 —— 数据其实已落库
    req=R4 accounts.UPDATE pk={'id': 13}

[真数据丢失] 1 条 —— 需要重点排查
    req=R5 accounts.INSERT pk={'id': 999998}
```

| | DB 中存在 | DB 中不存在 |
|---|---|---|
| JMeter 报成功 | 正常 | **真数据丢失**（唯一需要告警的格子） |
| JMeter 报失败 | **应答丢失的假失败** | 正常失败，无数据 |

### 5.3 反向孤儿扫描

还有一类情况无法逐请求归因：**写操作已提交，但客户端在追踪调用执行前就断了**。此时数据库里有一行，而 JTL 和追踪表都没有它的任何记录。

工具会扫描业务表中**本次运行覆盖的时间窗口内**、却没有对应追踪记录的行：

```
[未被追踪的行] 共 2 行 —— 这些行的写操作没有留下追踪记录
    accounts.id [时间窗口 2026-09-28 18:32:00.014..18:32:00.130] 共 2 行，样例 [3001, 3002]
```

窗口优先用业务表的时间戳列（`created_at` 等）界定；没有时间列时回退到主键区间，并在输出里说明——**主键区间只能发现区间内部的未追踪行，落在两端之外的发现不了**。

---

## 六、RPO 数据丢失检测

```bash
# 故障前：记录各表状态 + 种下复制流探针
python cli.py -c config.yaml rpo-plant -n node1
# → 记下 Batch ID

# === 手动触发故障：kill 主节点 / 断网 / 主备切换 ===

# 故障后
python cli.py -c config.yaml rpo-check -n node1 -bid <batch_id>
python cli.py -c config.yaml rpo-check -n node1 -bid <batch_id> --teardown
```

### 6.1 两种检测手段

**复制流探针（Marker）**：故障前往追踪表里插若干标记行，它们和业务写入走**同一条复制流**。故障后如果标记丢了，说明复制流没有送达最后的写入。

标记写在工具自己的表里，**不写业务表**（那样会污染被测数据，还会撞上 NOT NULL / 外键约束）。因此标记丢失是**关于复制流的结论，不针对某张业务表**——逐表的情况由下面这项给出。

每个标记记录了自己在本批次中的序号和本批次的总数，所以即使序号最大的几个被删掉，也能发现缺失：

```
[复制流探针 / Marker Check]
  Total:   20
  Found:   15
  Missing: 5
  丢失的标记序号: [15, 16, 17, 19, 20]
```

**行数与主键守恒（Conservation）**：对比故障前后的行数和最大主键。

- 行数减少 → 丢失了对应行数
- 最大主键回退 → 故障前的尾部数据未恢复

两项都是确定的丢失证据，且开销很低。

```
[行数与主键守恒 / Conservation]
  Tables checked: 4
  Tables with loss: 1
    accounts   2004 -> 1999 (delta -5)  max 999999 -> 2000  <<< MAX_REGRESSION
        最大主键从 999999 回退到 2000 —— 故障前的尾部数据未恢复
```

### 6.2 一项必须说明的限制

**工具无法区分「业务正常删除的行」与「故障丢失的行」。** 行数减少既可能是故障导致，也可能是应用自己删了数据。服务端没有任何信息能把两者分开——唯一可靠的区分手段是应用侧记录（即第四节的应用-DB 追踪模式）。

旧版本用 `generate_series` 扫描主键空洞，那个做法有两个致命问题，已移除：它会把每一次正常删除、每一次序列跳号（`nextval` 回滚后不回收）都报成数据丢失；而且若主键从一亿开始，它会先生成一亿行再关联查询。

---

## 七、定时巡检

```bash
# 每 5 分钟巡检一次（间隔也可写在 config 的 schedule.interval）
python cli.py -c config.yaml schedule --interval 300 -- \
    node-compare --nodes node1,node2,node3

# 连续 3 轮结果一致即判定收敛并停止
python cli.py -c config.yaml schedule --interval 60 --converge 3 -- \
    node-compare --nodes node1,node2,node3

# 最多跑 10 轮
python cli.py -c config.yaml schedule --interval 60 --rounds 10 -- \
    node-compare --nodes node1,node2,node3 -f json
```

| 参数 | 说明 |
|---|---|
| `--interval` | 轮询间隔秒数（默认取 `config.schedule.interval`） |
| `--report-dir` | 报告目录（默认取 `config.schedule.report_dir`） |
| `--converge N` | 连续 N 轮结果一致即判定收敛并停止 |
| `--rounds N` | 最多执行 N 轮 |

**每轮都会落盘**：`round_0001_<时间>.json` 记录该轮的结论、耗时、退出码与完整输出；`summary.json` 汇总历次结果。

**耗时超过间隔时会告警**：

```
[WARNING] 第 1 轮耗时 13.1s 超过间隔 5s —— 本轮与下一轮之间没有留白
```

记录为 `overran_interval: true`。两轮之间没有留白意味着它们的观测窗口可能重叠，这个信息会保留在报告里而不是被吞掉。

**收敛判定的注意点**：判定依据是各轮结果的**实质内容**（结论、差异块、差异行），不是耗时或时间戳，也不是"退出码相同"——两个不同的失败都返回退出码 1，只看退出码会让收敛判定建立在错误的前提上。收敛只证明"在这 N 个采样点上一致"，**不证明任何具体时刻的一致性**。

---

## 八、命令参考

### 通用参数

| 参数 | 说明 |
|---|---|
| `-c, --config` | 配置文件路径，默认 `config.yaml` |
| `--schema` | 数据库 schema（覆盖配置） |
| `-f, --format` | `table`（默认）或 `json` |

以上参数在子命令**前后都可以写**：`cli.py -c cfg node-compare ... -f json` 与 `cli.py -c cfg -f json node-compare ...` 等价。

**输出流约定**：日志走 **stderr**，命令结果（表格或 JSON）走 **stdout**。因此
`-f json` 的输出可以直接接到 `jq` 等工具：

```bash
python cli.py -c config.yaml node-compare --nodes node1,node2,node3 -f json | jq .conclusion
```

### node-compare

| 选项 | 说明 |
|---|---|
| `-a` / `-b` | 两个节点（传统用法） |
| `--nodes` | 逗号分隔的节点列表，任意多节点矩阵对比 |
| `--primary` | 指定基准节点，替代多数派表决 |
| `--backend` | `checksum`（默认）/ `native` / `pg_comparator` / `data_diff` |
| `-t, --tables` | 逗号分隔的表名，支持 `schema.table` 写法 |
| `--exclude-tables` / `--exclude-schemas` / `--exclude-databases` | 额外排除，支持通配符 |
| `--watermark` | 水位线：只比较主键不超过该值的行 |
| `--wait-quiesce` | 校验前等待连续 N 秒无数据变更 |
| `--consistency-mode` | `auto` / `quiesce` / `watermark` / `loose` |
| `--no-consistency` | 跳过一致性判定 |

### app-db-*

| 命令 | 说明 |
|---|---|
| `app-db-setup -n <节点>` | 创建追踪表与存储函数 |
| `app-db-verify -n <节点> [--batch-id <id>] [--no-mark]` | 校验追踪记录，含撕裂事务检测 |
| `app-db-status -n <节点>` | 批次摘要 |
| `app-db-teardown -n <节点>` | 清理 |

### schema-compare

| 选项 | 说明 |
|---|---|
| `-a` / `-b` | 两个节点 |
| `--schemas` | schema 名称（pgdiff 一次只对比一个；不填则对比全部非系统 schema） |
| `--schema-type` | 要对比的对象类型，默认 `ALL`；可选 `TABLE` / `COLUMN` / `INDEX` / `VIEW` / `FOREIGN_KEY` / `FUNCTION` 等 |

退出码：`0` 两侧结构一致，`1` 存在差异，`2` 未能完成（如工具未安装）。

### jtl-reconcile

| 选项 | 说明 |
|---|---|
| `-n, --node` | 节点 |
| `--jtl` | JMeter 结果文件路径 |
| `--batch-id` | 只对账指定批次 |
| `--request-id-column` | JTL 中请求标识的列名（默认自动识别 `REQ_ID` 等） |
| `--no-orphans` | 跳过反向孤儿扫描 |

### rpo-plant / rpo-check

| 选项 | 说明 |
|---|---|
| `-n, --node` | 节点 |
| `-t, --tables` | 追踪的表（默认全部，自动排除工具自用表） |
| `--marker-count` | 探针数量（默认 10） |
| `-bid, --batch-id` | `rpo-plant` 返回的 batch_id |
| `--teardown` | 检测后删除 RPO 追踪表 |

### gen-data

| 选项 | 说明 |
|---|---|
| `-n, --node` | 目标节点 |
| `--accounts` / `--products` / `--orders` / `--transactions` | 各表行数 |
| `--truncate` | 生成前先清空 |
| `--teardown` | 删除所有测试表 |

### run-workload / schedule / init-config / list-backends

```bash
python cli.py -c config.yaml run-workload -n node1 -w workload/templates/transfer.yaml
python cli.py -c config.yaml schedule --interval 300 -- node-compare --nodes node1,node2,node3
python cli.py init-config
python cli.py -c config.yaml list-backends
```

---

## 九、自定义 workload YAML

```yaml
name: "my_scenario"
concurrency: 15              # 并发线程数
duration: 600                # 执行时长（秒）
throttle_ms: 10              # 事务间隔（毫秒）

tables:
  - name: accounts
    ddl: |
      CREATE TABLE IF NOT EXISTS {schema}.accounts (
          id SERIAL PRIMARY KEY,
          balance NUMERIC(18,2) DEFAULT 0
      )

transactions:
  - name: "transfer"
    weight: 10
    sql: |
      UPDATE {schema}.accounts SET balance = balance - %(random_float:0.01-1000.00)s WHERE id = %(random_int:1-100)s;
      UPDATE {schema}.accounts SET balance = balance + %(random_float:0.01-1000.00)s WHERE id = %(random_int:1-100)s
```

> `run-workload` 的多线程共享同一个数据库连接，不适合高并发场景。需要真实并发时请用 JMeter。

**SQL 占位符：**

| 占位符 | 替换结果 |
|---|---|
| `%(random_int:N-M)s` | N~M 的随机整数 |
| `%(random_str:N)s` | N 位随机字符串 |
| `%(random_float:N-M)s` | N~M 的随机浮点数 |
| `%(uuid)s` | UUID |
| `%(timestamp)s` | 当前时间戳 |

`{schema}` 会在运行时替换为配置中的 schema。

---

## 十、典型测试流程

### 主备切换验证

```bash
# 1. 准备
python cli.py -c config.yaml app-db-setup -n node1
python cli.py -c config.yaml rpo-plant -n node1        # 记下 batch_id

# 2. 启动 JMeter（已配置 sample_variables=REQ_ID 与 sp_track_write）

# 3. 旁路巡检
python cli.py -c config.yaml schedule --interval 300 -- \
    node-compare --nodes node1,node2,node3 &

# 4. 触发主备切换

# 5. 恢复后验证
python cli.py -c config.yaml rpo-check -n node1 -bid <batch_id>
python cli.py -c config.yaml app-db-verify -n node1
python cli.py -c config.yaml jtl-reconcile -n node1 --jtl results.jtl
python cli.py -c config.yaml node-compare --nodes node1,node2,node3 --primary node1

# 6. 对比几份报告形成结论：
#    - rpo-check     : 故障期间丢了多少
#    - app-db-verify : 有没有事务被撕裂
#    - jtl-reconcile : 客户端报告的失败里有多少是假失败
#    - node-compare  : 节点之间最终是否收敛
```

---

## 十一、常见问题

**Q：工具必须要联网才能用吗？**
A：不需要。内置引擎不依赖任何外部工具，只需 `psycopg2`、`pyyaml`、`tabulate` 三个包，可离线安装。

**Q：支持 Kingbase 的哪些兼容模式？**
A：Oracle / PostgreSQL / MySQL / SQLServer 四种都支持。工具不按模式名做判断，而是**直接探测数据库的实际能力**（LSN 函数是否可用、变更计数器是否可读、时间戳列是否存在等），因此小版本差异不会导致误判。

**Q：一主两备怎么对比？能支持更多节点吗？**
A：`python cli.py -c config.yaml node-compare --nodes node1,node2,node3`。工具会直接指出哪个节点是异类。
**节点数量没有上限**，`--nodes` 后面按逗号列出即可（如 5 个、10 个），
前提是每个节点都在 `config.yaml` 里配置过。

**Q：报出了差异，怎么定位到具体行？**
A：默认就会定位。`checksum` 引擎发现差异块后会在块内逐行比对，直接给出「仅前者有 / 仅后者有 / 值不同」的具体主键值。

**Q：结论显示 `snapshot` 而不是 `deterministic`，怎么办？**
A：说明校验期间业务还在写入。若需要确定性结论，请在业务停顿的窗口重跑，或加大 `--wait-quiesce`。业务完全无法停顿的表，可以考虑改用应用-DB 追踪模式。

**Q：`INCONCLUSIVE` 和 `DIVERGENT` 有什么区别？**
A：`DIVERGENT` 是"确认不一致"（退出码 1）；`INCONCLUSIVE` 是"没能得出结论"（退出码 2），通常因为某节点读不到，或差异块无法定位多数派。自动化脚本中这两者应当分流处理。

**Q：定时巡检的日志和报告在哪？**
A：日志默认在 `./logs/comparator.log`，同时输出到终端（stderr）；报告默认在
`./reports/`，含每轮的 `round_*.json` 与汇总 `summary.json`。

**Q：把输出重定向到文件后看不到日志了？**
A：这是有意为之。日志走 stderr、结果走 stdout，所以 `> out.json` 只会写结果，
日志仍显示在终端。两者都要留时用 `> out.json 2> run.log`。

**Q：大表对比很慢怎么办？**
A：用 `checksum` 引擎（默认），它按主键区间分块。可调大 `comparison.chunk_size`（减少块数）和 `parallel`（提高并发）。分块边界由主键值决定，因此两侧行数不同也不会错位。
