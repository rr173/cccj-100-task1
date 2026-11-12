"""SQLite 持久层。

装载过程中的全部状态都保存在同一个 SQLite 数据库里，进程退出后重启
可以原样继续；暂存日志按事件落表，而不是放在无限增长的内存字典中。

表总览：

* ``meta``                 —— 通用键值状态（发布标志、检查点、计数器）
* ``snapshot_shards``      —— 快照分片到达/校验情况
* ``snapshot_staging``     —— 快照暂存行（发布前沿设备编号唯一）
* ``device_master``        —— 已发布的设备主表（业务查询唯一入口）
* ``log_events``           —— 暂存的日志事件（重传去重 + 冲突检测）
* ``log_txn``              —— 每个事务的状态（OPEN/COMMITTED/ABORTED）
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_shards (
    shard_file TEXT PRIMARY KEY,
    row_count  INTEGER NOT NULL,
    sha256     TEXT NOT NULL,
    staged     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS snapshot_staging (
    device_id TEXT PRIMARY KEY,
    payload   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_master (
    device_id TEXT PRIMARY KEY,
    payload   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS log_events (
    txn_id      TEXT NOT NULL,
    seq         INTEGER NOT NULL,          -- BEGIN 用哨兵序号 0，行/提交事件从 1 起
    event_type  TEXT NOT NULL,
    payload     TEXT NOT NULL,
    source_file TEXT NOT NULL,
    line_no     INTEGER NOT NULL,
    PRIMARY KEY (txn_id, seq)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS log_txn (
    txn_id        TEXT PRIMARY KEY,
    state         TEXT NOT NULL,           -- OPEN / COMMITTED / ABORTED
    commit_lsn    INTEGER,
    first_seen    INTEGER NOT NULL,        -- COMMIT 在日志流中首次出现的次序
    payload_begin TEXT NOT NULL
) WITHOUT ROWID;
"""


def connect(db_path: str) -> sqlite3.Connection:
    """打开（必要时创建）状态库并初始化表结构。

    显式开启 WAL：写事务提交后即使进程被 ``os._exit`` 杀死，已落盘的
    提交也能在重开连接时恢复。
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# meta 键值
# ---------------------------------------------------------------------------

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
