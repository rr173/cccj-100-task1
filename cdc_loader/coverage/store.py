"""采样覆盖审计层的 SQLite 持久层（本项目第四个独立数据库，WAL + FULL）。

库内保存三类数据：

1. **不可变契约稿件与豁免**：``contract_drafts`` / ``exemptions`` 只追加，
   同稿号内容永不改写；批次登记在 ``contract_batches`` /
   ``exemption_batches``，审计口径按批次号固定“契约资料接收到哪一批”。
2. **审计口径与明细**：``audit_runs`` 一行登记一次审计（building ->
   published），应到项/匹配/缺口/未匹配读数分表保存。旧审计固定不变，
   契约或读数事后更正只影响新审计。
3. **输入摘要**：每个 published 审计保存契约/豁免/读数/成员四份输入
   哈希，证明固定资料批次不会漂移。

读数事实仓与归并层台账始终以**只读 URI** 打开，本库不写它们。
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

-- 契约投递批次（口径锚点：契约资料接收到哪一批）
CREATE TABLE IF NOT EXISTS contract_batches (
    batch_id      INTEGER PRIMARY KEY,
    row_count     INTEGER NOT NULL,
    new_drafts    INTEGER NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contract_drafts (
    contract_no        TEXT NOT NULL,
    draft_no           INTEGER NOT NULL,
    active             INTEGER NOT NULL,        -- 1 启用 / 0 停用
    origin             TEXT NOT NULL,
    entity_id          TEXT NOT NULL,
    metric             TEXT NOT NULL,
    effective_start_utc TEXT NOT NULL,
    effective_end_utc  TEXT NOT NULL,
    timezone           TEXT NOT NULL,
    weekdays           TEXT NOT NULL,           -- JSON 数组 [1..7]
    segments           TEXT NOT NULL,           -- JSON [{start,end}]
    step_seconds       INTEGER NOT NULL,
    tolerance_seconds  INTEGER NOT NULL,
    ingest_batch_id    INTEGER NOT NULL,
    ingest_seq         INTEGER NOT NULL,
    content_hash       TEXT NOT NULL,
    PRIMARY KEY (contract_no, draft_no)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_drafts_contract
    ON contract_drafts(contract_no, draft_no);

-- 豁免投递批次
CREATE TABLE IF NOT EXISTS exemption_batches (
    batch_id      INTEGER PRIMARY KEY,
    row_count     INTEGER NOT NULL,
    new_rows      INTEGER NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exemptions (
    exemption_id     TEXT PRIMARY KEY,
    contract_no      TEXT NOT NULL,
    start_utc        TEXT NOT NULL,            -- 左闭
    end_utc          TEXT NOT NULL,            -- 右开
    reason           TEXT NOT NULL,
    ingest_batch_id  INTEGER NOT NULL,
    ingest_seq       INTEGER NOT NULL,
    content_hash     TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_exemptions_contract
    ON exemptions(contract_no);

-- 审计口径登记
CREATE TABLE IF NOT EXISTS audit_runs (
    run_id                INTEGER PRIMARY KEY,
    query_start_utc       TEXT NOT NULL,
    query_end_utc         TEXT NOT NULL,
    contract_cutoff_batch INTEGER NOT NULL,
    exemption_cutoff_batch INTEGER NOT NULL,
    reading_cutoff_batch  INTEGER NOT NULL,
    membership_version    INTEGER NOT NULL,
    status                TEXT NOT NULL,       -- building / published / aborted
    created_at            TEXT NOT NULL,
    published_at          TEXT,
    expected_count        INTEGER,
    covered_count         INTEGER,
    missing_count         INTEGER,
    exempt_count          INTEGER,
    duplicate_count       INTEGER,
    unmatched_count       INTEGER,
    zero_expected_note    TEXT,
    input_hash            TEXT
);

-- 应到采样时刻（含被豁免项，exempt=1；跳时 gap 不产生行）
CREATE TABLE IF NOT EXISTS audit_expected (
    run_id              INTEGER NOT NULL,
    item_id             INTEGER NOT NULL,      -- 审计内稳定序号
    contract_no         TEXT NOT NULL,
    draft_no            INTEGER NOT NULL,
    origin              TEXT NOT NULL,
    entity_id           TEXT NOT NULL,
    metric              TEXT NOT NULL,
    scheduled_utc       TEXT NOT NULL,
    tz_name             TEXT NOT NULL,
    local_time          TEXT NOT NULL,         -- 当地挂钟 HH:MM:SS
    local_date          TEXT NOT NULL,
    utc_offset          TEXT NOT NULL,         -- +08:00
    fold_occurrence     INTEGER NOT NULL,      -- 0 唯一/首次, 1 回拨第二次
    segment_date        TEXT NOT NULL,
    segment_start       TEXT NOT NULL,
    step_index          INTEGER NOT NULL,      -- 相对日段起点的步序
    exempt              INTEGER NOT NULL,
    exemption_ids       TEXT NOT NULL,         -- JSON 命中豁免编号（可多个，重叠只扣一次）
    covered             INTEGER NOT NULL,
    PRIMARY KEY (run_id, item_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_expected_run
    ON audit_expected(run_id, contract_no, scheduled_utc);

-- 读数-应到项配对（全部匹配依据；同项多次命中保留重复诊断）
CREATE TABLE IF NOT EXISTS audit_matches (
    run_id            INTEGER NOT NULL,
    item_id           INTEGER NOT NULL,
    origin            TEXT NOT NULL,
    reading_key       TEXT NOT NULL,
    chosen_edition    INTEGER NOT NULL,
    device            TEXT NOT NULL,
    entity_id         TEXT NOT NULL,
    occurred_at_utc   TEXT NOT NULL,
    distance_seconds  INTEGER NOT NULL,
    is_primary        INTEGER NOT NULL,        -- 1 该读数唯一选中的应到项
    duplicate_hit     INTEGER NOT NULL,        -- 1 应到项已被别的读数先命中
    ingest_batch_id   INTEGER NOT NULL,
    PRIMARY KEY (run_id, origin, reading_key, item_id)
) WITHOUT ROWID;

-- 连续缺采缺口段（被豁免位置切断；同契约内合并）
CREATE TABLE IF NOT EXISTS audit_gaps (
    run_id            INTEGER NOT NULL,
    gap_id            INTEGER NOT NULL,
    contract_no       TEXT NOT NULL,
    draft_no          INTEGER NOT NULL,
    start_item_id     INTEGER NOT NULL,
    end_item_id       INTEGER NOT NULL,
    start_utc         TEXT NOT NULL,
    end_utc           TEXT NOT NULL,            -- 最后一个缺采时刻（含）
    missing_item_ids  TEXT NOT NULL,           -- JSON 项序号列表
    split_exemptions  TEXT NOT NULL,           -- JSON 切断缺口的豁免编号
    nearby_readings   TEXT NOT NULL,           -- JSON 附近未匹配读数及原因
    PRIMARY KEY (run_id, gap_id)
) WITHOUT ROWID;

-- 未匹配读数（超容差/实体不在成员版本/无契约等，全部保留原因）
CREATE TABLE IF NOT EXISTS audit_unmatched (
    run_id            INTEGER NOT NULL,
    origin            TEXT NOT NULL,
    reading_key       TEXT NOT NULL,
    chosen_edition    INTEGER NOT NULL,
    device            TEXT NOT NULL,
    entity_id         TEXT,                    -- 可能为空：成员版本中无此设备
    metric            TEXT NOT NULL,
    occurred_at_utc   TEXT NOT NULL,
    reason            TEXT NOT NULL,
    candidates        TEXT NOT NULL,           -- JSON 容差内最近候选及距离依据
    ingest_batch_id   INTEGER NOT NULL,
    PRIMARY KEY (run_id, origin, reading_key)
) WITHOUT ROWID;
"""


def connect(db_path: str) -> sqlite3.Connection:
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
