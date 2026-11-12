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
├── examples/
│   ├── generate_sample.py        # 装载器样例生成器（含 expected.json）
│   ├── demo.py                   # 装载器端到端演示（25 项断言）
│   ├── generate_entity_sample.py # 归并层确定性样例（批次 + 场景期望）
│   └── demo_entity.py            # 归并层端到端演示（19 项断言，含真实崩溃）
├── tests/
│   ├── test_loader.py            # 装载器单元测试
│   └── test_entity.py            # 归并层单元测试
├── run_cli.py                    # 装载器隔离模式(-I)启动器
├── run_entity.py                 # 归并层隔离模式(-I)启动器
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
