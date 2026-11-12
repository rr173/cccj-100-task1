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
├── entity/
│   ├── protocol.py     # 对照资料行格式（7 字段）与修订身份/内容规则
│   ├── errors.py       # 归并层错误类型与退出码
│   ├── store.py        # 关系台账 SQLite（独立于装载器状态库）
│   ├── fault.py        # 故障注入点 after_ledger_before_decision
│   ├── engine.py       # 决策引擎：连通分量/冲突路径/实体标识/变动分类
│   ├── ledger.py       # 批次摄入、设备在场刷新、版本落库、查询
│   ├── recompute.py    # 独立全量重算器（不 import 本包其它模块）
│   └── cli.py          # ingest / refresh / status / explain / history /
│                       # show-version / changes / verify
├── reading/
│   ├── protocol.py     # 读数行格式（8 字段）/有限十进制/带时区时间归 UTC
│   ├── errors.py       # 读数仓错误类型与退出码
│   ├── store.py        # 读数事实仓 + 按日指标投影 SQLite（第三个独立库）
│   ├── ingest.py       # 逐稿件不可变落仓、同稿篡改拒收、批次原子性
│   ├── grouping.py     # 只读复用归并层已保存的分组成员（不重算连通分量）
│   ├── fault.py        # 故障注入点 after_detail_before_publish
│   ├── build.py        # 口径冻结选版/归属/Decimal 聚合/原子发布/同步门
│   ├── query.py        # 格子查询、下钻、待归属清单、稿件审计
│   ├── reconcile.py    # 独立离线核算器（不 import 本包其它模块）
│   └── cli.py          # ingest / build / runs / cells / drill /
│                       # unassigned / audit / status / verify
├── coverage/
│   ├── protocol.py     # 契约/豁免行格式（13/5 字段）、不可变稿号规则
│   ├── errors.py       # 覆盖层错误类型与退出码（含 AuditStale=8）
│   ├── timeutil.py     # IANA 时区、左闭右开、DST gap/fold、步长锚定
│   ├── store.py        # 契约/豁免/审计明细 SQLite（第四个独立库）
│   ├── ingest.py       # 契约稿与豁免批次摄入、同稿篡改整批拒收
│   ├── snapshot.py     # 按批次固定契约/豁免/读数选版/成员版本快照
│   ├── scheduler.py    # 日程展开：交集、豁免并集扣除、DST 处理
│   ├── matcher.py      # 容差配对：最近优先/同距取早/别名归并/重复诊断
│   ├── gaps.py         # 连续缺采缺口段（豁免切断）与附近读数诊断
│   ├── fault.py        # 故障注入点 after_detail_before_confirm
│   ├── audit.py        # 口径冻结/两阶段原子发布/同步门/残档重做
│   ├── query.py        # runs/report/missing/duplicates/unmatched/explain
│   ├── verifier.py     # 独立核对（不 import scheduler/matcher/gaps/audit）
│   └── cli.py          # ingest-contracts / ingest-exemptions / audit /
│                       # report / missing / duplicates / unmatched /
│                       # explain / runs / status / verify
├── examples/
│   ├── generate_sample.py        # 装载器样例生成器（含 expected.json）
│   ├── demo.py                   # 装载器端到端演示（25 项断言）
│   ├── generate_entity_sample.py # 归并层确定性样例（批次 + 场景期望）
│   ├── demo_entity.py            # 归并层端到端演示（19 项断言，含真实崩溃）
│   ├── generate_reading_sample.py# 读数层确定性样例（上游/归并/读数批次）
│   ├── demo_reading.py           # 读数层端到端演示（32 项断言，含真实崩溃）
│   ├── generate_coverage_sample.py # 覆盖审计样例（契约/豁免/DST/边界）
│   └── demo_coverage.py          # 覆盖审计端到端演示（48 项断言，含真实崩溃）
├── tests/
│   ├── test_loader.py            # 装载器单元测试
│   ├── test_entity.py            # 归并层单元测试
│   ├── test_reading.py           # 读数事实仓与投影单元测试
│   └── test_coverage.py          # 覆盖审计单元测试（22 个）
├── run_cli.py                    # 装载器隔离模式(-I)启动器
├── run_entity.py                 # 归并层隔离模式(-I)启动器
├── run_reading.py                # 读数层隔离模式(-I)启动器
└── run_coverage.py               # 覆盖审计隔离模式(-I)启动器
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

---

## 8. 设备实体归并层（entity/）

采购、维修、供应商各自使用不同的设备编号。归并层在 `device_master`
之上建立可追溯的实体归并：**直接只读**装载器状态库的设备主表，另建
**独立的 SQLite 关系台账**，不修改原装载入口的任何行为。设备不会因为
“型号相同”被自动认作一台——归并依据完全由外部对照资料显式提供。

### 8.1 对照资料格式（严格）

一次投递为一个 JSONL 批次文件（可多个文件合成一批），每行**恰好** 7 个字段：

| 字段          | 约束                                                         |
|---------------|--------------------------------------------------------------|
| `source`      | 非空字符串，资料来源（如 procurement / maintenance / supplier）|
| `relation_id` | 非空字符串，来源内部的关系编号                                |
| `revision`    | 整数 ≥ 1（拒绝布尔），同一 `(source, relation_id)` 的修订号   |
| `endpoint_a`  | 非空字符串，一端设备编号                                      |
| `endpoint_b`  | 非空字符串，另一端设备编号，不得与 `endpoint_a` 相同          |
| `category`    | `same_entity`（同一实体）或 `must_differ`（必须不同）         |
| `effective`   | 布尔；`false` 表示自该修订起撤销此关系                        |

缺字段、多字段、类型不符一律按协议错误拒绝（退出码 2）。

### 8.2 资料修订规则

* **身份与内容**：修订身份为 `(source, relation_id, revision)`；内容为
  `(endpoint_a, endpoint_b, category, effective)`。台账按身份**持久保留
  每一条修订的完整内容**，永不改写、永不删除。
* **连续递增**：每个 `(source, relation_id)` 的修订号从 1 开始、每次 +1。
  新修订必须恰好接续台账当前最大号，跳号拒绝。
* **取代**：更高修订号可以撤销（`effective=false`）、更换端点或改变类别；
  关系的当前状态 = 最大修订号的内容。
* **重传与篡改**：修订号 ≤ 当前最大号的行是旧版本重投——内容与台账完全
  一致则幂等无效果；**任何内容字段被改写即整批拒绝**（`RevisionConflict`，
  退出码 3）。批内每一行都与台账逐字比对，不只看各关系的最新版本，
  已被取代的旧修订被改写同样拒绝。
* **批内重复**：同一身份在批内出现两次，内容一致去重，不一致即拒绝。

### 8.3 决策计算与实体身份规则

* **顶点**：当前在场的全部设备（`refresh` 时从 `device_master` 同步）。
* **边**：当前生效且两端都在场的 `same_entity` 关系。任一端不在场的
  关系**暂扣**（数据保留、不参与计算），设备重现后自动重新参与。
* **实体**：`same_entity` 边的无向连通分量；连通可传递（A-B、B-C ⇒
  A 与 C 同属一个实体）。无关系的设备自成单设备实体。
* **约束**：生效且两端在场的 `must_differ` 作用于**整个分量**：两端落在
  同一分量即冲突（包括经链式传递后的跨端冲突）。
* **实体标识（确定且公开）**：`E-` + 分量内字典序最小的设备编号。
  成员集合不变则标识不变；不同分量的最小编号必然不同，标识不会碰撞。
* **分组哈希（公开）**：`sha256(canonical_json([[entity_id, 成员...], ...]))`，
  按 entity_id 升序、成员字典序；`canonical_json` 即
  `json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`。
* **冲突路径（确定且可复核）**：从约束两端中字典序较小端出发做广度优先
  搜索；每层邻居按设备编号字典序展开；同一对设备间的多条平行关系按
  `(source, relation_id, revision)` 字典序取第一条。错误报告列出每条冲突
  约束及其完整关系路径，不只报“不一致”。
* **版本**：决策分组与当前版本不同才落新版本（`refresh` 与 `ingest`
  共用同一套判定）；分组不变不出版本，因此重读相同资料不会凭空增加
  历史版本。仅约束变化（不改变分组）也不产生新版本。

### 8.4 批次原子性与故障注入

一次投递是**不可分割的决策批次**：修订对照与写入 → 批次登记 → 最终候选
图统一判定 → 版本落库 → 进度推进，全部在**同一条 SQLite 事务**内完成。
同一批中“撤销旧连接 + 加入新连接”按最终候选图统一判定（不按事件先后
顺序），拆分与合并在同一版本内完成。任何矛盾（格式 / 篡改 / 约束冲突）
或崩溃都使整批回滚：修订台账、实体成员、批次与处理进度维持投递前状态。

故障注入点（`--fault after_ledger_before_decision`，`os._exit(42)`）位于
**关系台账已写入（事务内）、决策尚未落定**的位置。崩溃使未提交事务整体
回滚，重开后成员、修订台账与历史版本均无半批状态，用同一批次文件重投
即可。

### 8.5 刷新：上游删除与重现

`refresh` 重新读取 `device_master` 并同步在场设备：上游删除的设备退出
活动成员（其关联关系暂扣，数据保留）；设备重新出现后自动重新参与约束
判定。若重现使既有约束重新生效并产生矛盾，本次刷新整体拒绝、不落版本，
需先修正资料再刷新。

### 8.6 运行方式

```bash
cd /workspace/cdc_loader

# (0) 准备样例：装载器样例 + 归并层样例
python3 -I examples/generate_sample.py /tmp/cdc_sample
python3 -I examples/generate_entity_sample.py /tmp/entity_sample
DB=/tmp/cdc/state.db; LEDGER=/tmp/cdc/ledger.db

# (1) 装载器正常跑（行为不变）
python3 -I run_cli.py import-snapshot --db "$DB" \
    --manifest /tmp/cdc_sample/manifest.json \
    --shards /tmp/cdc_sample/shard_0.jsonl /tmp/cdc_sample/shard_1.jsonl /tmp/cdc_sample/shard_2.jsonl
python3 -I run_cli.py consume --db "$DB" \
    --logs /tmp/cdc_sample/log_1.jsonl /tmp/cdc_sample/log_2.jsonl

# (2) 同步在场设备（首次必须，建立版本 1）
python3 -I run_entity.py refresh --ledger-db "$LEDGER" --loader-db "$DB"

# (3) 摄入对照资料批次（原子决策批次）
python3 -I run_entity.py ingest --ledger-db "$LEDGER" \
    --batch /tmp/entity_sample/b1_init.jsonl

# (4) 查询：状态 / 按设备解释 / 历史 / 某版本分组 / 相邻版本变动
python3 -I run_entity.py status   --ledger-db "$LEDGER"
python3 -I run_entity.py explain  --ledger-db "$LEDGER" --device D001 [--version N]
python3 -I run_entity.py history  --ledger-db "$LEDGER"
python3 -I run_entity.py show-version --ledger-db "$LEDGER" --version 3
python3 -I run_entity.py changes  --ledger-db "$LEDGER" --version 4

# (5) 独立全量重算器：逐版本对照完整分组与冲突结论
python3 -I run_entity.py verify   --ledger-db "$LEDGER"
```

`explain` 输出设备所在实体、成员、依据关系（边）、从该设备到各成员的
确定性路径、作用于该实体的“必须不同”约束，以及因端点缺席而暂扣的
关系。成员取自版本快照；依据按该版本的台账水位重算（当前版本用台账
头部水位——后续未产生新版本的批次可能改变了依据，如撤销冗余关系）。
`changes --version N` 给出 N-1 → N 的合并 / 拆分 / 出现 / 消失 /
成员增减（含设备级出现与消失），全部按确定顺序输出。

### 8.7 独立全量重算器

`entity/recompute.py` 不 import 归并层的任何其它模块，只按本 README
公开的规则独立实现，对台账中**每一个被接受的版本**：以该版本的台账
水位重放修订 → 以该版本记录的活跃设备为顶点重算连通分量与实体标识 →
比对**完整分组**（实体标识 + 全部成员集合，而非实体数量）与分组哈希 →
重算“必须不同”约束结论（被接受版本必须零冲突）。另做头部检查：以台账
当前水位 + 当前在场设备重算，结果必须与当前版本一致（证明没有漏建
版本）。任何不一致逐条列出并以退出码 6 结束。

### 8.8 失败后如何重投

| 失败情形                       | 现象/退出码                  | 处置                                                         |
|--------------------------------|------------------------------|--------------------------------------------------------------|
| 资料格式/修订号不连续          | `RelationProtocolError`，2   | 修正批次文件后整批重投；台账无残留                           |
| 旧修订内容被改写（含已被取代） | `RevisionConflict`，3        | 数据正确性红线，人工与来源方核对；归并层拒绝并保持现状       |
| 约束与连通关系矛盾             | `ConstraintConflict`，5      | 按报告中的冲突约束与关系路径修正资料后整批重投；台账无残留   |
| 未 refresh / 快照未发布        | `UpstreamNotReady`，4        | 先让装载器发布快照，再运行 `refresh`                         |
| 决策落定前崩溃（故障注入）     | 退出码 42                    | 重开台账用**同一批次文件**重投；无半批状态                   |
| 普通进程退出                   | —                            | 全部状态在 SQLite，重开同一 `--ledger-db` 继续即可           |

幂等性保证：批次重投（内容一致）只命中去重，不产生重复修订、不产生
新版本；任何不确定时运行 `verify` 用独立重算器全量对照。

### 8.9 归并层演示与测试

```bash
python3 -I examples/demo_entity.py                # 19 项端到端断言（含真实崩溃）
python3 -m unittest discover -s tests -v          # 装载器 + 归并层全部单元测试
```

演示覆盖：两条独立连接构成链后触发跨端冲突（报告 D010→D011→D012 关系
路径）；同一批撤销桥边并新增另一连接产生拆分与合并（只出一个新版本）；
已被新修订取代的旧修订被篡改（拒绝且整批回滚）；上游删除并重新出现
设备（暂扣与重新参与）；两个来源对同一对端点提供依据后撤销其中一条
（另一条仍然有效，分组不变不出版本）；`after_ledger_before_decision`
崩溃后重投；全部批次重投幂等；独立重算器逐版本对照。

---

## 9. 振动读数事实仓与按日指标投影（reading/）

同一台机器的振动读数会散落在不同设备编号下，而维修人员会持续纠正哪些
编号属于同一实体。本层在归并层**已经保存的分组成员**之上，建立不可变的
逐稿件读数事实仓，以及按**固定口径**冻结的按日指标投影，让分析人员可以
切换设备归属口径、复查旧日报“为什么是当时那个数字”。它**只读**归并层
台账（`version_entities` / `version_devices`），不重算连通分量、不改写
归并层，也不改变装载器与归并层的任何既有入口。本次不含网页，只提供
命令行可运行入口。

### 9.1 读数逐行资料格式（严格）

一次投递为一个 JSONL 批次文件（可多个文件合成一批），每行**恰好** 8 个
字段：

| 字段          | 约束                                                          |
|---------------|---------------------------------------------------------------|
| `origin`      | 非空字符串，读数来源；不同来源的 `reading_key` 互相独立计数   |
| `reading_key` | 非空字符串，来源内部的观测编号                                |
| `edition`     | 正整数（≥1，拒绝布尔），同一观测的稿件号                      |
| `device`      | 非空字符串，稿件声称的设备编号（后续稿件可改）                |
| `occurred_at` | **必须带时区**的 ISO-8601 时间（`Z` 或 `±HH:MM`），归一到 UTC |
| `metric`      | 非空字符串，指标名（如 `vibration_rms`）                      |
| `amount`      | **有限十进制文本**（`Decimal` 解析，拒绝 NaN/Infinity）       |
| `withdrawn`   | 布尔；`true` 表示该稿件撤回此观测                             |

缺字段、多字段、类型不符、朴素（无时区）时间、非有限十进制都按协议错误
拒绝（退出码 2）。`amount` 全程以 `Decimal` 处理、以文本落库，求和绝不
经过二进制浮点（因此 `0.1 + 0.2` 精确得到 `0.3`，而不是
`0.30000000000000004`）。`occurred_at` 归一到 UTC 后再确定日期，
`2026-11-12T00:30:00+08:00` 的 UTC 日期是 `2026-11-11`。

### 9.2 稿件不可变、选版与篡改红线

* **稿件身份** = `(origin, reading_key, edition)`；事实仓
  `reading_editions` 只追加、永不改写、永不删除，每条稿件原文
  （含到达批次、内容哈希）都可经 `audit` 复查。
* **观测身份** = `(origin, reading_key)`。新稿件号（更大 `edition`）可以
  改读数、发生时间、设备、指标，也可以撤回；选版永远取**截止范围内的
  最大 edition**，因此迟来的旧稿件无法盖过新版。
* 同一稿件身份重投且内容逐字一致 → 幂等无操作；**同稿件换了任何内容
  字段** → `EditionConflict`（退出码 3）整批拒收，事实仓批次不前进。
* 一次投递是不可分割的确认批次：稿件对照与写入、批次登记在同一条 SQLite
  事务内落定，任何矛盾整体回滚。

### 9.3 统计口径：两个坐标同时冻结

一次统计明确固定两个口径坐标，落在 `stats_runs` 一行：

1. **截止批次 `cutoff_batch_id`**：读数只取随批次号 `<= 截止批次` 收到的
   稿件。构建开始即把口径登记为 `building` 并提交，之后即使新批次到达也
   不混入——已声明截止批次的统计不可能掺入后来才收到的内容。
2. **设备分组版本 `grouping_version`**：设备归属只查归并层**该版本快照**
   的成员（`version_entities`），绝不套用“今天”的成员。

在该范围内：每条观测选最大 edition → 最新稿 `withdrawn=true` 的排除
（仍留在明细与撤回清单）→ 其余按选中稿件声称的 `device` 在该分组版本中
找实体。设备不在那一版成员中时进入**可查询的待归属清单**
（`outcome=unassigned`），不静默丢弃、不强行归类。产出按
**实体 × UTC 日期 × 指标** 划分的条数、精确和、最小值、最大值
（`stats_cells`）。

### 9.4 下钻、归属依据与口径切换

* `drill` 从一个格子下钻到构成它的每条读数：读数身份 `(origin,
  reading_key)`、采用的 `chosen_edition`、稿件载荷、到达批次，以及归属
  依据（分组版本、设备→实体、判定原因）。
* 从旧分组切到新分组：**合并**把若干格子合到同一实体，**拆分**则把各条
  读数重新分配到不同实体；跨天/跨设备更正后，原格子的计数与极值随之
  修正，撤回项从格子移除但保留在撤回清单。
* 每个口径独立持久化，可指定任意历史口径查看；重复生成同一对
  `(cutoff_batch, grouping_version)` 直接复用已发布结果（`status=reused`）。

### 9.5 原子发布、构建中可读与崩溃恢复

* 明细（`stats_observations` / `stats_attribution`）与格子
  （`stats_cells`）在一条事务内写就，最后才把 `stats_runs` 从
  `building` 翻成 `published`。在线查询只认 `published`，因此构建较慢时
  分析人员仍读到上一份完整日报，**不会先清空在线结果**。
* 故障注入点 `after_detail_before_publish`（`os._exit(42)`）位于明细已写、
  尚未发布处：未提交事务整体回滚，只留下阶段一已提交的 `building` 残档。
  重开后旧日报照常可读，`building` 口径不对外可见（明确报错而非半空
  结果），用同一对口径重建即安全清理残档并发布。
* 资料送达与统计生成可交错：`build --gate-dir` 提供基于文件的跨进程同步
  门，构建进程在门点等待时，另一进程可继续 `ingest` 新批次；放行后选版
  查询仍带 `ingest_batch_id <= cutoff`，截止范围保持一致。构建另有
  `flock` 跨进程互斥锁，避免并发构建互相踩踏。

### 9.6 SQLite 表（第三个独立数据库，WAL + synchronous=FULL）

| 表                     | 用途                                                  |
|------------------------|-------------------------------------------------------|
| `meta`                 | `last_batch_id`、`schema_version`                     |
| `ingest_batches`       | 每个被确认的读数投递批次（截止批次的锚点）            |
| `reading_editions`     | 不可变逐稿件事实仓（含到达批次、UTC 日期、内容哈希）  |
| `stats_runs`           | 口径登记：building / published 与各项计数、输入哈希   |
| `stats_observations`   | 口径下每条观测选中的最新版（下钻事实）                |
| `stats_attribution`    | 每条选中观测的归属依据 / 待归属 / 撤回原因            |
| `stats_cells`          | 按日指标投影：实体 × UTC 日期 × 指标的精确聚合        |

### 9.7 运行方式

```bash
cd /workspace/cdc_loader

# (0) 准备样例：最小上游快照 + 归并批次（合并/拆开）+ 五个读数批次
python3 -I examples/generate_reading_sample.py /tmp/reading_sample

# (1) 上游快照 + 归并层版本（版本1 单设备 / 版本2 合并 DA-DB / 版本3 拆开）
python3 -I run_cli.py import-snapshot --db /tmp/rd/loader.db \
    --manifest /tmp/reading_sample/loader/manifest.json \
    --shards /tmp/reading_sample/loader/shard_0.jsonl
python3 -I run_entity.py refresh --ledger-db /tmp/rd/ledger.db --loader-db /tmp/rd/loader.db
python3 -I run_entity.py ingest  --ledger-db /tmp/rd/ledger.db --batch /tmp/reading_sample/entity/g1_merge.jsonl
python3 -I run_entity.py ingest  --ledger-db /tmp/rd/ledger.db --batch /tmp/reading_sample/entity/g2_split.jsonl

RDB=/tmp/rd/reading.db; LDB=/tmp/rd/ledger.db

# (2) 接收读数（可多文件合成一批，可多次调用，断点幂等）
python3 -I run_reading.py ingest --reading-db "$RDB" \
    --batch /tmp/reading_sample/reading/b1_base.jsonl /tmp/reading_sample/reading/b2_unknown.jsonl

# (3) 按固定口径生成并发布统计（截止批次 + 分组版本）
python3 -I run_reading.py build --reading-db "$RDB" --ledger-db "$LDB" \
    --cutoff-batch 1 --grouping-version 2     # 合并口径
python3 -I run_reading.py build --reading-db "$RDB" --ledger-db "$LDB" \
    --cutoff-batch 1 --grouping-version 3     # 拆分口径（同一截止、不同归属）

# (4) 按格子查询 / 从格子下钻 / 待归属清单 / 稿件审计 / 口径登记
python3 -I run_reading.py cells      --reading-db "$RDB" --cutoff-batch 1 --grouping-version 2
python3 -I run_reading.py drill      --reading-db "$RDB" --cutoff-batch 1 --grouping-version 2 \
    --entity E-DA --date 2026-11-11 --metric vibration_rms
python3 -I run_reading.py unassigned --reading-db "$RDB" --cutoff-batch 2 --grouping-version 2
python3 -I run_reading.py audit      --reading-db "$RDB" --origin plant-a --reading-key r1
python3 -I run_reading.py runs       --reading-db "$RDB"
python3 -I run_reading.py status     --reading-db "$RDB"

# (5) 独立离线核算器：逐格 + 逐条下钻 + 逐条待归属全量对照
python3 -I run_reading.py verify --reading-db "$RDB" --ledger-db "$LDB"
```

### 9.8 独立离线核算器

`reading/reconcile.py` 不 import 读数层的任何其它模块，只用标准库独立地：
按口径截止批次重新扫描 `reading_editions` 自行选版 → 只读打开归并层台账
取该分组版本成员做归属 → 用 `Decimal` 独立聚合 → 与 `stats_cells`
**逐格**、与 `stats_observations` **逐条下钻明细**、与
`stats_attribution` 的**每一条待归属/撤回记录**比对，并复核五项计数。
不是只核对总行数；任何不一致逐条列出并以退出码 6 结束。`building`
残档不参与对照，但会在报告的 `building_run_ids` 中单独列出。

### 9.9 失败后如何处置

| 失败情形                     | 现象/退出码                | 处置                                                     |
|------------------------------|----------------------------|----------------------------------------------------------|
| 读数行字段/时间/十进制非法   | `ReadingProtocolError`，2  | 修正批次文件后整批重投；事实仓无残留                     |
| 同稿件身份内容被改写         | `EditionConflict`，3       | 数据红线，人工核对来源；拒收且批次不前进                 |
| 截止批次/分组版本不存在      | `CaliberNotFound`，7       | 先确认读数批次与归并层版本，再用合法坐标构建             |
| 明细已写未发布时崩溃         | 退出码 42                  | 重开后旧日报可读；同口径重建清理 building 残档并发布     |
| 普通进程退出                 | —                          | 全部状态在 SQLite，重开同一 `--reading-db` 继续          |

### 9.10 读数层演示与测试

```bash
python3 -I examples/demo_reading.py               # 32 项端到端断言（含真实崩溃与进程间同步）
python3 -m unittest discover -s tests -v          # 装载器 + 归并层 + 读数层全部单元测试
```

演示覆盖：设备链归并后又拆开，两种归属口径下完整明细与指标对比；把当前
最小读数改到次日另一设备、再撤回当前最大值后原格子计数与极值的修正；
未知设备在新分组版本中仍进待归属清单的变化；带时差的午夜读数归 UTC
日期与十进制 `0.1 + 0.2 = 0.3` 精确求和；同稿篡改拒收、迟来旧稿不盖
新版、重传幂等；用进程间同步门在构建期间再收新稿件以证明截止范围一致；
明细已写未启用阶段直接结束进程后旧日报可读、新口径安全重建；最后由独立
离线核算器对所有格子、全部下钻明细与待归属记录做全量对照。

---

## 10. 采样覆盖审计（coverage/）

工厂要区分“设备没有按计划上报”和“当天本来就不要求采样”。本层在既有
读数事实仓与归并层成员台账之上，只做**数据质量计算与命令行入口**（不做
可视化界面），把外部声明的采样契约展开为应到采样时刻，再与固定批次内的
读数配对，产出覆盖/缺采/重复/未匹配明细。它**不把读数层的指标合计当成
覆盖率**：覆盖率永远是“被覆盖的应到项 / 非豁免应到项”。本层是第四个
独立 SQLite 库，对读数仓与归并层台账只读打开，不改写任何既有入口。

### 10.1 采样契约与豁免资料格式（严格）

契约批次（UTF-8 JSONL）每行**恰好** 13 个字段：

| 字段                | 约束 |
|---------------------|------|
| `contract_no`       | 非空字符串，契约编号 |
| `draft_no`          | 正整数（≥1，拒绝布尔），稿号 |
| `active`            | 布尔；`false` 表示自该稿起停用 |
| `origin`            | 非空字符串，读数来源（与读数台账 origin 对齐） |
| `entity_id`         | 非空字符串，设备实体（与归并层实体标识对齐） |
| `metric`            | 非空字符串，指标名 |
| `effective_start` / `effective_end` | 带时区 ISO-8601，生效起止，左闭右开，终点更晚 |
| `timezone`          | IANA 时区名（如 `America/New_York`），裸偏移拒收 |
| `weekdays`          | 非空整数数组 1..7（周一=1），按日段**当地起点日期**判定 |
| `segments`          | 非空数组，每项 `{"start":"HH:MM","end":"HH:MM"}`；`end<=start` 表示跨午夜，自动拆成两个明确日段 |
| `step_seconds`      | 正整数，步长秒数，**以该日段起点为锚** |
| `tolerance_seconds` | 非负整数，配对容差秒数，超容差不能补洞 |

豁免批次每行**恰好** 5 个字段：`exemption_id`、`contract_no`、`start`、
`end`（均带时区、左闭右开）、`reason`。豁免区间允许相交和包含，按
**并集**扣除，重叠位置只扣一次但保留全部命中豁免编号。

**不可变稿件**：契约身份是 `(contract_no, draft_no)`，豁免身份是
`exemption_id`，事实表只追加、永不改写。同稿号重投且内容逐字一致 ->
幂等无操作（仍登记一个确认批次）；同稿号换任何内容字段 ->
`DraftConflict`（退出码 3）整批拒收，批次水位不前进。新稿号可以修订
日程或停用（`active=false`），审计按截止批次内每个契约的**最大稿号**
解释，契约数据因此能说明“何时生效、哪一稿在生效”。

### 10.2 时间语义（IANA 时区、左闭右开、夏令时）

* 所有生效、查询、豁免区间一律左闭右开 `[start, end)`；恰好落在生效
  终点、豁免终点、查询终点的时刻都不成为应到项。
* 日程逐日在契约 IANA 时区内展开：先与生效范围、查询范围取交集，再
  删除豁免并集覆盖的时刻。步长以**日段起点为锚**计步（`step_index`
  从 0 起），绝不从查询起点重新计步；跨午夜班次拆成两个日段后各自锚定。
* 春季跳时（gap）：不存在的当地挂钟时刻不制造应到项（如纽约
  2026-03-08 的 02:00 被跳过，段内只剩 00/01/03/04）。
* 秋季回拨（fold）：同一当地时刻出现两次时，两个真实时刻都产生应到项，
  分别用 `fold_occurrence=0/1` 与各自 UTC 偏移（如 `-05:00`/`-04:00`）
  记录。每个应到项都保存时区名、当地日期/挂钟时间与 UTC 偏移，可解释
  应到项从何而来。

### 10.3 一次审计固定的四个口径坐标

`audit` 固定：查询时段、契约截止批次、读数截止批次、设备成员版本
（豁免截止批次随审计一并登记，缺省取当前最大豁免批次）。

1. 契约只取 `ingest_batch_id <= 截止` 的最大稿号；停用稿不展开。
2. 读数从读数仓固定范围选各观测的**最高稿**，撤回稿不参与。
3. 按 `(origin, entity_id, metric)` 归到契约实体；设备 -> 实体只查
   归并层**指定成员版本**快照，同实体的多个设备编号（别名）都能满足
   同一计划，绝不套用今天的成员改写历史归属。
4. 口径在阶段一就登记为 `building` 并提交；同步门等待期间即使有新批次
   到达，后续查询全部带批次上界，固定资料批次不会漂移。

### 10.4 配对规则、重复处理与覆盖率口径

* 每条读数在容差内（`|Δ| <= tolerance_seconds`，含边界）选时间距离
  **最近**的应到项；距离相同选**更早**的应到项；超容差不补洞，进
  未匹配清单并保留最近候选与距离依据。每条读数最多匹配一个应到项。
* 同一应到项可被多条读数命中：**只算一次覆盖**，但全部命中记录与重复
  上报诊断（`duplicate_hit=1`）都保留。被豁免位置不接受覆盖。
* 覆盖率只返回**分子（已覆盖非豁免应到项数）与分母（非豁免应到项数）**，
  绝不拿实际条数除以预期条数掩盖空洞；分母为零时 `ratio=null` 并附
  `zero_expected_note`，不除零、不称为全量正常。
* 连续缺采在同一契约内合为缺口段；被豁免的计划位置把缺口切断。每个缺口
  记录切断它的豁免编号，以及附近未匹配读数及其未匹配原因，可追查每个
  缺口对应哪份日程、哪些豁免、附近读数为何没补上。

### 10.5 原子发布、崩溃恢复与旧审计固定

明细（`audit_expected` / `audit_matches` / `audit_gaps` /
`audit_unmatched`）与发布位在**同一条 SQLite 事务**内写就，最后才把
`audit_runs` 从 `building` 翻成 `published`。故障注入点
`after_detail_before_confirm` 以 `os._exit(42)` 在明细已写、对外确认前
真实终止进程：事务整体回滚，只留下阶段一已提交的 `building` 残档；残档
对外读取直接报 `AuditStale`（退出码 8），不返回半份结果，用同一口径
重做即安全清理并发布。旧 `published` 审计永不改写——契约或读数事后更正
只影响新审计。

### 10.6 SQLite 表（第四个独立库，WAL + synchronous=FULL）

| 表 | 用途 |
|----|------|
| `contract_batches` / `exemption_batches` | 契约/豁免确认批次（截止水位锚点） |
| `contract_drafts` | 不可变契约稿件（含时区、星期、日段、步长、容差） |
| `exemptions` | 不可变停机豁免区间 |
| `audit_runs` | 审计口径登记：building/published、五项计数、四份输入哈希 |
| `audit_expected` | 应到采样时刻（时区名/当地时间/UTC 偏移/fold/豁免依据） |
| `audit_matches` | 全部读数-应到项配对依据（含重复命中诊断） |
| `audit_gaps` | 连续缺采缺口段、切断豁免、附近未匹配读数 |
| `audit_unmatched` | 未匹配读数及原因与候选距离依据 |

### 10.7 运行方式

```bash
cd /workspace/cdc_loader

# (0) 准备样例（最小上游快照 + 归并批次 + 读数批次 + 契约/豁免批次）
python3 -I examples/generate_coverage_sample.py /tmp/cov_sample

# (1) 上游快照 + 归并层版本1/版本2 + 两个读数批次（同既有各层入口）
python3 -I run_cli.py import-snapshot --db /tmp/cov/loader.db \
    --manifest /tmp/cov_sample/loader/manifest.json \
    --shards /tmp/cov_sample/loader/shard_0.jsonl
python3 -I run_entity.py refresh --ledger-db /tmp/cov/ledger.db --loader-db /tmp/cov/loader.db
python3 -I run_entity.py ingest  --ledger-db /tmp/cov/ledger.db --batch /tmp/cov_sample/entity/g1_merge.jsonl
python3 -I run_reading.py ingest --reading-db /tmp/cov/reading.db \
    --batch /tmp/cov_sample/reading/b1_base.jsonl /tmp/cov_sample/reading/b2_correct.jsonl

CVDB=/tmp/cov/coverage.db; RDB=/tmp/cov/reading.db; LDB=/tmp/cov/ledger.db

# (2) 登记契约稿件（每个文件一个确认批次）与停机豁免
for f in c1_draft1 c2_spring c3_fall c4_night c5_outside c6_effedge; do
  python3 -I run_coverage.py ingest-contracts --coverage-db "$CVDB" \
      --batch /tmp/cov_sample/coverage/$f.jsonl
done
python3 -I run_coverage.py ingest-exemptions --coverage-db "$CVDB" \
    --batch /tmp/cov_sample/coverage/e1_overlap.jsonl

# (3) 固定四坐标执行审计并原子发布
python3 -I run_coverage.py audit --coverage-db "$CVDB" \
    --reading-db "$RDB" --ledger-db "$LDB" \
    --query-start 2026-03-01T00:00:00Z --query-end 2026-12-31T00:00:00Z \
    --contract-cutoff 6 --reading-cutoff 1 --membership-version 2

# (4) 五类明细与溯源
python3 -I run_coverage.py report     --coverage-db "$CVDB" --run-id 1
python3 -I run_coverage.py missing    --coverage-db "$CVDB" --run-id 1
python3 -I run_coverage.py duplicates --coverage-db "$CVDB" --run-id 1
python3 -I run_coverage.py unmatched  --coverage-db "$CVDB" --run-id 1
python3 -I run_coverage.py explain    --coverage-db "$CVDB" --run-id 1 --item-id 12
python3 -I run_coverage.py runs       --coverage-db "$CVDB"

# (5) 独立核对程序：不调用正式展开/配对函数，全量逐条对照
python3 -I run_coverage.py verify --coverage-db "$CVDB" \
    --reading-db "$RDB" --ledger-db "$LDB"
```

### 10.8 独立核对程序如何对照

`coverage/verifier.py` 不 import `scheduler` / `matcher` / `gaps` /
`audit`，只用标准库与本包协议/存储基础，独立重写日程展开（逐日逐段、
段起点锚定、逐个真实时刻做生效/查询/豁免判定）与读数配对（对每个
读数×应到项做完整 O(n·m) 比较，独立复算容差、最近选择、同距取早、
重复命中、缺口分段），再与已发布审计的每张明细表**逐条**比对应到项、
配对集合与距离、未匹配原因、缺口段和七项计数；任何不一致逐条列出并以
退出码 6 结束。`building` 残档不参与对照，单独在 `stale_run_ids` 列出。

### 10.9 失败后如何重做

| 失败情形 | 现象/退出码 | 处置 |
|----------|-------------|------|
| 契约/豁免字段或时间/时区非法 | `CoverageProtocolError`，2 | 修正批次后整批重投；库内无残留 |
| 同稿号内容被改写 | `DraftConflict`，3 | 数据红线，人工核对；新日程请用新稿号 |
| 截止批次/成员版本不存在 | `CaliberNotFound`，7 | 先确认契约/读数批次与归并层版本再审计 |
| 明细已写未确认时崩溃 | 退出码 42 | 残档拒读（`AuditStale`，8）；同口径重做安全发布 |
| 普通进程退出 | — | 全部状态在 SQLite，重开同一 `--coverage-db` 继续 |

### 10.10 覆盖审计演示与测试

```bash
python3 -I examples/demo_coverage.py               # 48 项端到端断言（含真实崩溃与同步门）
python3 -m unittest discover -s tests -v           # 全部四层共 84 个单元测试
```

演示覆盖：普通班次中条数足够但集中同一时刻仍有缺采；设备别名归并后
共同满足计划；读数改时间/撤回导致覆盖变化（旧审计固定不变）；重叠豁免
只扣一次且切断缺口；真实 IANA 时区的春季跳时与秋季回拨；恰好落在生效
终点、豁免终点与容差边界的记录；跨午夜拆段与零应到不除零；同步门证明
固定批次不漂移；明细已写、确认前真实退出后不出现半份结果；最后由独立
核对程序对每个应到时刻与读数配对做完整比较。
