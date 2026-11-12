"""读数事实仓与按日指标投影的 SQLite 持久层（本项目第三个独立数据库）。

设计要点
========

* 事实仓 ``reading_editions`` 是**逐稿件追加、永不改写**的不可变台账：
  同一 (origin, reading_key, edition) 的每条稿件原文都可查，迟来的旧
  稿件天然无法盖过新版（选版永远取最大 edition）。
* 统计以**口径** (cutoff_batch, grouping_version) 冻结：读数只取截止
  批次前已确认收到的稿件，设备归属只看该分组版本快照（归并层
  ``version_entities`` 已保存的成员），不套用今天的成员。
* 生成结果分两层落库：``stats_runs`` 一行登记一次口径（building ->
  published），明细/格子/归属写在独立的 ``stats_*`` 表里。发布是单条
  事务里把状态翻成 published，因此在线查询永远读到上一份完整日报，
  构建中途崩溃只留下 building 残档，重开后可安全重建。

表总览
======

* ``meta``              —— 键值状态（last_batch_id、schema_version）
* ``ingest_batches``    —— 每个被确认的读数投递批次（截止批次的锚点）
* ``reading_editions``  —— 不可变逐稿件事实仓
* ``stats_runs``        —— 统计口径登记（cutoff_batch + grouping_version）
* ``stats_observations``—— 口径下每条观测选中的最新版（下钻事实）
* ``stats_attribution`` —— 每条选中观测的设备归属依据/待归属原因
* ``stats_cells``       —— 按日指标投影：实体 × UTC 日期 × 指标
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id     INTEGER PRIMARY KEY,
    row_count    INTEGER NOT NULL,     -- 批内有效行数（去重后）
    new_editions INTEGER NOT NULL,     -- 其中真正新增的稿件条数
    content_hash TEXT NOT NULL         -- 全批规范化行排序后的 SHA-256
);

CREATE TABLE IF NOT EXISTS reading_editions (
    origin          TEXT NOT NULL,
    reading_key     TEXT NOT NULL,
    edition         INTEGER NOT NULL,
    device          TEXT NOT NULL,
    occurred_at     TEXT NOT NULL,      -- 归一到 UTC 的 ISO-8601
    occurred_date   TEXT NOT NULL,      -- UTC 日期 YYYY-MM-DD
    metric          TEXT NOT NULL,
    amount          TEXT NOT NULL,      -- 十进制文本，绝不使用 float
    withdrawn       INTEGER NOT NULL,   -- 1 撤回 / 0 有效
    ingest_batch_id INTEGER NOT NULL,   -- 该稿件随哪个确认批次收到
    ingest_seq      INTEGER NOT NULL,   -- 批内行序（稳定排序/审计）
    content_hash    TEXT NOT NULL,
    PRIMARY KEY (origin, reading_key, edition)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_editions_observation
    ON reading_editions(origin, reading_key, edition);
CREATE INDEX IF NOT EXISTS idx_editions_batch
    ON reading_editions(ingest_batch_id);

CREATE TABLE IF NOT EXISTS stats_runs (
    run_id             INTEGER PRIMARY KEY,
    cutoff_batch_id    INTEGER NOT NULL,
    grouping_version   INTEGER NOT NULL,
    status             TEXT NOT NULL,   -- building / published
    created_at         TEXT NOT NULL,
    published_at       TEXT,
    observation_count  INTEGER,         -- 选中观测数（含撤回排除前的候选口径）
    included_count     INTEGER,         -- 实际计入格子的有效观测数
    withdrawn_count    INTEGER,         -- 最新版撤回而排除的观测数
    unassigned_count   INTEGER,
    cell_count         INTEGER,
    input_hash         TEXT,            -- 口径输入的确定性摘要（幂等佐证）
    UNIQUE (cutoff_batch_id, grouping_version)
);

CREATE TABLE IF NOT EXISTS stats_observations (
    run_id            INTEGER NOT NULL,
    origin            TEXT NOT NULL,
    reading_key       TEXT NOT NULL,
    chosen_edition    INTEGER NOT NULL,
    device            TEXT NOT NULL,    -- 选中稿件声称的设备
    occurred_at       TEXT NOT NULL,
    occurred_date     TEXT NOT NULL,
    metric            TEXT NOT NULL,
    amount            TEXT NOT NULL,
    withdrawn         INTEGER NOT NULL,
    ingest_batch_id   INTEGER NOT NULL, -- 选中稿件实际到达批次
    PRIMARY KEY (run_id, origin, reading_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS stats_attribution (
    run_id            INTEGER NOT NULL,
    origin            TEXT NOT NULL,
    reading_key       TEXT NOT NULL,
    device            TEXT NOT NULL,
    assigned          INTEGER NOT NULL, -- 1 已归属实体 / 0 待归属
    entity_id         TEXT,             -- assigned=1 时的实体标识
    basis             TEXT NOT NULL,    -- JSON：归属依据或待归属原因
    PRIMARY KEY (run_id, origin, reading_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS stats_cells (
    run_id      INTEGER NOT NULL,
    entity_id   TEXT NOT NULL,
    utc_date    TEXT NOT NULL,
    metric      TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    amount_sum  TEXT NOT NULL,          -- Decimal 精确和的文本
    amount_min  TEXT NOT NULL,
    amount_max  TEXT NOT NULL,
    PRIMARY KEY (run_id, entity_id, utc_date, metric)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_cells_run
    ON stats_cells(run_id, entity_id, utc_date, metric);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开（必要时创建）读数仓并初始化表结构（WAL + FULL 同步）。"""
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    if get_meta(conn, "schema_version") is None:
        set_meta(conn, "schema_version", SCHEMA_VERSION)
        conn.commit()
    return conn


def get_meta(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    return json.loads(row["value"])


def set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value, ensure_ascii=False, sort_keys=True)),
    )
