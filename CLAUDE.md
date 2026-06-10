# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

数据库集群（Kingbase/PostgreSQL）一致性校验 CLI 工具，用于高可用测试场景中的数据一致性验证。

## 常用命令

```bash
# 生成配置文件
python cli.py init-config

# 节点间数据对比（内置 checksum 引擎，默认）
python cli.py -c config.yaml node-compare -a node1 -b node2

# 节点对比 — native 引擎（精确行级对比）
python cli.py -c config.yaml node-compare -a node1 -b node2 --backend native -t accounts,orders

# 应用-DB一致性追踪（JMeter 集成）
python cli.py -c config.yaml app-db-setup -n node1
python cli.py -c config.yaml app-db-verify -n node1

# RPO 数据丢失检测
python cli.py -c config.yaml rpo-plant -n node1
python cli.py -c config.yaml rpo-check -n node1 -bid <batch_id>

# 生成测试数据
python cli.py -c config.yaml gen-data -n node1 --accounts 5000

# 内置负载生成器
python cli.py -c config.yaml run-workload -n node1 -w workload/templates/transfer.yaml

# 定时巡检（每5分钟）
python cli.py -c config.yaml schedule --interval 300 -- node-compare -a node1 -b node2

# 列出可用对比引擎
python cli.py -c config.yaml list-backends

# 离线安装依赖（内网 Linux 服务器）
pip install --no-index --find-links=./offline_pkgs -r requirements.txt
```

项目没有测试套件和 lint 配置。

## 核心架构

### 分层结构

```
cli.py (argparse 子命令路由)
  ├── core/         业务逻辑（节点对比、应用-DB校验、RPO检测）
  ├── backends/     可插拔对比引擎（checksum、native、pg_comparator、data_diff）
  ├── workload/     测试数据生成 + 自定义负载执行
  └── utils/        配置加载、数据库连接池、日志
```

### 子命令路由模式

`cli.py` 使用 `_build_cmd_map()` 返回命令名→函数的字典，同时支持 `node_compare` 和 `node-compare` 两种格式。`main()` 和 `cmd_schedule()` 都通过 `cmd_map.get(parsed.command.replace("-", "_"))` 查找处理器。

**关键细节：** `cmd_schedule()` 使用 `argparse.REMAINDER` 捕获 `--` 后的子命令参数，然后重新调用 `_build_parser().parse_args()` 解析。注意 REMAINDER 会原样传入 `--`，需要先 strip 掉。**不能**使用 `parsed.func`（argparse Namespace 没有这个属性）。

### 数据库连接池 (`utils/db.py`)

`DatabasePool` 的核心行为：

- **`cursor(name, autocommit=False)`** — 上下文管理器。`autocommit=True` 时在退出时调用 `conn.commit()`；默认 `False` 时**不提交**，写操作会丢失。**写事务必须传 `autocommit=True`。**
- **`execute(name, sql, params, fetch=True)`** — 自动判断单条/批量：`params` 是 list-of-tuples 时调用 `executemany()`，否则 `execute()`。`autocommit=not fetch` 意味着写操作自动提交、读操作不提交。
- **`execute_ddl(name, sql)`** — 临时设置 `conn.autocommit = True`，执行完恢复。
- **`get_dsn_parameters()`** — **不包含密码**。需要密码传给外部工具（pg_dump 等）时，必须从 `pool._config["databases"][name]["password"]` 读取。
- **`get_primary_key()`** — 查询 `information_schema.table_constraints` + `key_column_usage`，返回 `list[str]`（列名列表）。

### Python 3.8 兼容性

目标环境是 Linux + Python 3.8.8。所有使用 PEP 585 语法（`list[str]`、`dict[str, int]` 等）的模块**必须**在文件顶部添加：
```python
from __future__ import annotations
```

当前已有该声明的文件：`checksum.py`, `native.py`, `data_diff.py`, `rpo_checker.py`, `app_db_comparator.py`, `node_comparator.py`, `runner.py`, `db.py`, `pg_comparator.py`。新文件如需使用 PEP 585 语法也同样需要。

### SQL 标识符引用

多个模块使用 `_quote_ident(name)` 对用户提供的表名/列名做安全引用：
```python
def _quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')
```
出现在：`checksum.py`, `native.py`, `app_db_comparator.py`, `rpo_checker.py`。处理用户输入的 schema/table/column 时必须使用。

### 对比引擎选择

| 引擎 | 文件 | 适用场景 |
|------|------|---------|
| `checksum` | `backends/checksum.py` | 大表快速扫描，分块 MD5，需主键 |
| `native` | `backends/native.py` | 小表精确行级对比，EXCEPT 查询 |
| `pg_comparator` | `backends/pg_comparator.py` | 外部工具，临时 YAML 配置 |
| `data_diff` | `backends/data_diff.py` | 跨数据库外部工具 |

checksum 引擎无主键时降级为 row-count-only 对比（仅比较行数）。

### 配置加载

`utils/config.py` 的 `load_config()` 使用 `_deep_merge(DEFAULT_CONFIG, user_config)` 合并默认值，空字段自动填充。`config.yaml` 被 `.gitignore` 排除（含密码），模板文件是 `config.yaml.example`。`generate_template()` 优先复制 `.example` 模板（保留中文注释），不存在时才从 `DEFAULT_CONFIG` 生成。

### 关键注意事项

- `os.makedirs("")` 在 Python 3.8 上会抛出 `FileNotFoundError`，创建目录前需检查路径非空。
- `logger.py` 中 `logging.FileHandler` 不存在时会自动创建文件，但不会自动创建目录。
- `workload/runner.py` 的 `_worker` 多线程模型共享同一个数据库连接 — 这是已知限制，不适合超高并发场景。
- JMeter 集成：`sp_track_write()` 存储过程通过 JDBC PostProcessor 调用，需使用独立的 JDBC Connection Configuration 避免连接池争用。
