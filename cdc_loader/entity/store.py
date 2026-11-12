"""实体归并层的 SQLite 关系台账（独立于装载器状态库的第二个数据库）。

表总览：

* ``meta``              —— 键值状态（last_batch_id、current_version、
                           presence_initialized、schema_version）
* ``batches``           —— 每一次被接受的投递（批次即原子决策单位）
* ``revisions``         —— 修订台账，不可变，按 (source, relation_id,
                           revision) 持久保留全部修订内容
* ``presence``          —— 当前在场设备（refresh 时从 device_master 同步）
* ``decision_versions`` —— 被接受的决策版本（分组变化才产生新版本）
* ``version_entities``  —— 每个版本的实体成员
* ``version_devices``   —— 每个版本参与计算的活跃设备（顶点集）

被拒绝的批次（协议错误 / 修订篡改 / 约束冲突 / 崩溃）不留任何痕迹：
修订、批次、版本、meta 进度全部维持投递前状态。
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

CREATE TABLE IF NOT EXISTS batches (
    batch_id       INTEGER PRIMARY KEY,
    row_count      INTEGER NOT NULL,     -- 批内有效行数（去重后）
    new_revisions  INTEGER NOT NULL,     -- 其中真正新增的修订条数
    content_hash   TEXT NOT NULL         -- 全批规范化行排序后的 SHA-256
);

CREATE TABLE IF NOT EXISTS revisions (
    source       TEXT NOT NULL,
    relation_id  TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    endpoint_a   TEXT NOT NULL,
    endpoint_b   TEXT NOT NULL,
    category     TEXT NOT NULL,          -- same_entity / must_differ
    effective    INTEGER NOT NULL,       -- 1 生效 / 0 撤销
    batch_id     INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    PRIMARY KEY (source, relation_id, revision)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS presence (
    device_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS decision_versions (
    version       INTEGER PRIMARY KEY,
    cause         TEXT NOT NULL,         -- ingest / refresh
    batch_id      INTEGER,               -- cause=ingest 时为本批 id
    ledger_seq    INTEGER NOT NULL,      -- 计算本版本时的台账水位（批次号）
    entity_count  INTEGER NOT NULL,
    device_count  INTEGER NOT NULL,
    grouping_hash TEXT NOT NULL,         -- 完整分组的确定性哈希（规则见 README）
    presence_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS version_entities (
    version   INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    PRIMARY KEY (version, entity_id, device_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS version_devices (
    version   INTEGER NOT NULL,
    device_id TEXT NOT NULL,
    PRIMARY KEY (version, device_id)
) WITHOUT ROWID;
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开（必要时创建）台账库并初始化表结构（WAL + FULL 同步）。"""
    conn = sqlite3.connect(db_path)
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
