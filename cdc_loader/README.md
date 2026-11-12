# 离线 CDC 装载器

把供应商交付的**一致性快照**与后续**变更日志（JSONL）**合成为可查询的
**设备主数据**，全部状态保存在一个 SQLite 数据库中。仅依赖 Python 3.11
标准库与内置 `sqlite3`，不对接真实数据库复制协议，也不需要任何第三方包。

---

## 1. 目录结构

```
cdc_loader/
├── cdc/
│   ├── protocol.py     # 字段类型、清单与事件协议校验、确定性序列化（冲突判定）
│   ├── errors.py       # 统一错误类型与退出码
│   ├── store.py        # SQLite 连接、建表、meta 键值
│   ├── fault.py        # 三个确定性故障注入点（os._exit(42)）
│   ├── snapshot.py     # 阶段一：分片校验/暂存、完整性门禁、原子发布
│   ├── applier.py      # INSERT/UPDATE/主键迁移/DELETE 行变更应用
│   ├── log_ingest.py   # 阶段二：日志流式暂存、去重冲突、按提交顺序应用
│   ├── reference.py    # 独立参考实现（不 import 其它装载器模块）
│   └── cli.py          # import-snapshot / consume / dump / status / verify
├── examples/
│   ├── generate_sample.py  # 确定性样例生成器（含期望结果 expected.json）
│   └── demo.py             # 端到端演示（25 项断言，含真实崩溃子进程）
├── tests/
│   └── test_loader.py      # 21 个 unittest 单元测试
├── run_cli.py              # 隔离模式(-I)启动器
└── README.md
```

---

## 2. 输入协议（项目自定义）

### 2.1 字段类型

固定字段在快照清单 `fields` 中声明，顺序即列顺序。支持三种标量类型：

| 类型      | Python 表示                 | 说明                                   |
|-----------|----------------------------|----------------------------------------|
| `TEXT`    | `str`（`device_id` 非空）  | `device_id` 必须是首个 TEXT 主键字段   |
| `INTEGER` | `int`，**拒绝布尔**        | `True/False` 不被当作 1/0              |
| `REAL`    | `int`/`float`，存为 float  | 同样拒绝布尔                           |

`null`（Python `None`）对任意类型合法。行必须恰好包含清单声明的字段，
缺字段或出现未定义字段都按协议错误拒绝。

### 2.2 快照清单 `manifest.json`

```json
{
  "cut_point_lsn": 2000,
  "fields": [
    {"name": "device_id", "type": "TEXT"},
    {"name": "vendor", "type": "TEXT"},
    {"name": "temperature", "type": "REAL"},
    {"name": "cycle_count", "type": "INTEGER"}
  ],
  "shards": [
    {"file": "shard_0.jsonl", "rows": 4, "sha256": "<64位小写十六进制>"},
    {"file": "shard_1.jsonl", "rows": 4, "sha256": "..."}
  ]
}
```

* `cut_point_lsn`（切点 S，非负整数）：快照保证包含截至 S **已提交**的全部状态。
* 每个分片给出文件名、行数与整文件 SHA-256。分片为 UTF-8 JSONL，每行一行。

### 2.3 变更日志事件（JSONL）

每个事件都有 `event` 与 `txn_id`；除 `BEGIN` 外都有事务内从 **1** 开始
严格递增的 `seq`；`COMMIT` 另带非负整数 `commit_lsn`。

| 事件   | 关键字段                                   |
|--------|--------------------------------------------|
| BEGIN  | `txn_id`（无 seq）                          |
| INSERT | `seq`，`row`（完整行，按字段定义校验）      |
| UPDATE | `seq`，`before`/`after`（均为完整行，含主键，允许更换设备编号） |
| DELETE | `seq`，`device_id`                          |
| COMMIT | `seq`，`commit_lsn`                         |
| ABORT  | `seq`                                      |

协议规则（违反即拒绝，不会污染主表）：

1. 行事件必须先有 `BEGIN`；事务进入 COMMITTED/ABORTED 终态后不得再有事件。
2. 同一事务内 `seq` 严格递增（重传旧序号除外，见第 4 点）。
3. **首次出现**的提交位置必须递增：新 COMMIT 的 `commit_lsn` 必须大于
   此前见过的任一已提交 LSN。不同事务的行事件允许交错。
4. **事件身份**为 `(txn_id, seq)`（BEGIN 记为 `txn_id#BEGIN`）。同一身份
   重复出现时，对载荷做键序无关的规范化（`json.dumps(..., sort_keys=True)`）：
   * 完全相同 → 视为重传，无副作用；
   * 载荷不同 → `PayloadConflict`（退出码 3），拒绝并保持现状。
5. UPDATE 应用时校验 `before` 与库中当前行完全一致（前镜像不匹配即拒绝）；
   主键迁移要求新编号不与现存设备冲突。

---

## 3. 两阶段装载与持久化设计

### 阶段一：快照暂存与发布

* 分片可**乱序、可重传**到达。每片独立校验 SHA-256 与行数，通过后写入
  `snapshot_staging` 并在 `snapshot_shards` 登记；设备编号跨分片全局唯一。
* **完整性门禁**：清单中任一分片缺失或摘要不符，就不会触碰 `device_master`，
  业务查询（`dump`）直接返回“快照尚未发布”。因此不可能看见半份快照。
* 到齐后在**单条 SQLite 事务**内完成：清空主表 → 灌入全部暂存行 →
  写入清单元数据/发布标志 → 检查点初始化为切点 S → 清空暂存表。原子可见。

### 阶段二：日志追赶

* **流式落库，不靠内存字典**：事件逐行校验后写入 `log_events`，事务状态写入
  `log_txn`（OPEN/COMMITTED/ABORTED）。进程退出后状态完整保留，重开同一
  SQLite 库即可继续。BEGIN 无序号，在表内以哨兵 `seq=0` 存储
  （`WITHOUT ROWID` 表主键列不允许 NULL，真实 seq 从 1 起，零不会冲突）。
* 仅应用 `commit_lsn > S` 且状态为 COMMITTED 的事务；`commit_lsn <= S`
  （切点及之前）的事务视为其效果已包含在快照中，跳过；ABORTED 与只有
  BEGIN 的 OPEN 事务永不进入主表。
* 应用顺序按 `commit_lsn` 升序（与首次提交次序一致）。**一笔事务的多次
  更改、主键迁移以及检查点推进，在同一个 SQLite 事务内落定**：任一事件
  应用失败则整体回滚，主表与检查点都保持旧值。
* 检查点 `checkpoint_lsn` 即最后成功应用事务的 `commit_lsn`，只进不退。

### SQLite 表

| 表                 | 用途                                       |
|--------------------|--------------------------------------------|
| `meta`             | 发布标志、快照元数据、检查点、提交次序计数 |
| `snapshot_shards`  | 分片到达/校验登记                          |
| `snapshot_staging` | 发布前的快照暂存行                         |
| `device_master`    | 已发布设备主表（业务查询唯一入口）         |
| `log_events`       | 暂存日志事件（身份去重 + 冲突检测）        |
| `log_txn`          | 事务状态、commit_lsn、首次提交次序         |

数据库以 WAL + `synchronous=FULL` 打开，已提交事务在 `os._exit` 崩溃后
重开连接仍可恢复。

---

## 4. 运行方式

所有处理建议以隔离模式运行（`-I` 忽略环境变量与用户 site 包，数据目录
不进入导入路径，避免数据中被植入的同名模块如 `json.py` 被加载）。
数据目录与代码目录保持分离，路径一律以参数传入。

```bash
cd /workspace/cdc_loader

# (0) 生成一套自洽样例（确定性；输出目录必须为空或不存在）
python3 -I examples/generate_sample.py /tmp/cdc_sample

DB=/tmp/cdc/state.db

# (1) 导入快照：分片可乱序、可重复投递；不完整则拒绝发布
python3 -I run_cli.py import-snapshot --db "$DB" \
    --manifest /tmp/cdc_sample/manifest.json \
    --shards /tmp/cdc_sample/shard_2.jsonl /tmp/cdc_sample/shard_0.jsonl /tmp/cdc_sample/shard_1.jsonl

# (2) 继续消费日志（可多次调用，断点续传、幂等）
python3 -I run_cli.py consume --db "$DB" \
    --logs /tmp/cdc_sample/log_1.jsonl /tmp/cdc_sample/log_2.jsonl

# (3) 读取当前主表与检查点（json / jsonl / table）
python3 -I run_cli.py dump --db "$DB" --format json

# (4) 查看状态（分片到达、未闭合事务、暂存事件数）
python3 -I run_cli.py status --db "$DB" --manifest /tmp/cdc_sample/manifest.json

# (5) 独立参考实现对照最终行集与检查点（ok=true 即一致）
python3 -I run_cli.py verify --db "$DB" \
    --manifest /tmp/cdc_sample/manifest.json \
    --shards /tmp/cdc_sample/shard_*.jsonl \
    --logs /tmp/cdc_sample/log_1.jsonl /tmp/cdc_sample/log_2.jsonl
```

### 故障注入点（确定性）

通过 `--fault` 指定，命中后以 `os._exit(42)` 真实终止进程（跳过缓冲刷新
与 `finally`，最接近真实崩溃），每个注入点在一次进程生命周期内只触发一次：

| 注入点                                  | 卡住的位置                                   |
|-----------------------------------------|----------------------------------------------|
| `before_snapshot_publish`               | 分片已全部暂存、快照发布之前                 |
| `before_txn_commit`                     | 事务行变更已执行、SQLite 提交之前            |
| `after_txn_commit_before_return`        | SQLite 已提交成功、向调用方返回之前          |

```bash
python3 -I run_cli.py consume --db "$DB" --logs log_2.jsonl \
    --fault before_txn_commit      # 进程退出码 42
# 修复/重启后用完全相同的命令继续即可（不传 --fault）
python3 -I run_cli.py consume --db "$DB" --logs log_2.jsonl
```

### 一键演示与单元测试

```bash
python3 -I examples/demo.py                       # 25 项端到端断言（含 3 个真实崩溃）
python3 -m unittest discover -s tests -v          # 21 个单元测试
```

演示覆盖：分片乱序到达、切点两侧提交的交错事务、未提交事务尾部、
同批先改后删、主键改变、损坏分片、事件身份冲突，以及三个注入点的
崩溃后重放；最后用独立参考实现交叉核对行集与检查点。

---

## 5. 输入假设

1. 清单与分片/日志均为可信供应商通道交付的**格式正确的 UTF-8 JSON/JSONL**；
   装载器负责校验结构、类型、摘要与协议顺序，但不处理字节级传输错误。
2. 切点 S 的语义由供应商保证：快照包含所有 `commit_lsn <= S` 的已提交效果；
   装载器不再回放这部分事务（样例中 T10 即如此：其更新已体现在快照行里）。
3. `commit_lsn` 为非负整数，首次出现的提交位置全局递增；`seq` 在事务内
   从 1 起递增。重传只能重复**完全相同**的旧记录。
4. 一个设备编号在任一时刻只属于一行；快照内与跨分片的设备编号唯一。
5. 日志可以分多个文件、多次投喂，文件按投喂顺序拼接解释；尚未闭合
   （只有 BEGIN）的事务允许跨文件、跨进程等待后续事件。
6. 同一个 SQLite 状态库不做并发多进程写入（崩溃恢复场景是串行重开）。

---

## 6. 失败后的操作方式

| 失败情形                     | 现象/退出码                     | 处置                                                                 |
|------------------------------|---------------------------------|----------------------------------------------------------------------|
| 分片缺失                     | `SnapshotIncomplete`，退出码 4  | 补齐缺失分片后重跑同一 `import-snapshot`；主表始终不可见直至完整     |
| 分片 SHA-256/行数不符        | `ProtocolError`，退出码 2       | 向供应商索取正确分片替换后重投；坏片不会写入暂存表                   |
| 事件协议/顺序/镜像错误       | `ProtocolError`，退出码 2       | 修正日志后重投；事务级回滚保证主表与检查点不变                       |
| 同事件身份不同载荷           | `PayloadConflict`，退出码 3     | 这是数据正确性红线，需人工核对供应商日志；装载器拒绝并保持现状       |
| 快照发布前崩溃               | 退出码 42                       | 重开库重跑导入：分片已暂存，直接发布                                 |
| 事务提交前崩溃               | 退出码 42                       | 重开库重放日志：事务从 `log_events` 完整回放，不丢不重               |
| 提交后返回前崩溃             | 退出码 42                       | 提交已落盘；重放仅命中去重，检查点不回退、无副作用                   |
| 普通进程退出（正常/异常）    | —                               | 所有状态在 SQLite，重开同一 `--db` 继续即可，无需内存重建            |

幂等性保证：分片重传、旧日志重放都不会产生重复行或重复应用；检查点
单调递增。任何不确定时，运行 `verify` 用独立参考实现对照最终行集与检查点。

---

## 7. 参考实现如何对照

`cdc/reference.py` 不 import 装载器的任何其它子模块，只用字典/列表独立地：
装载并校验快照 → 扫描全部日志做相同的去重/冲突/顺序检查 → 按 `commit_lsn`
升序、事务内按 `seq` 升序回放 `commit_lsn > S` 的已提交事务 → 输出最终
行集、检查点、已应用/中止/悬空事务清单。`verify` 子命令逐行比对装载器
的 `device_master` 与参考结果，并核对检查点是否一致。
