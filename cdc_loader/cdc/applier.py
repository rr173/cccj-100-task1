"""把已提交事务的行事件按事务内序号应用到 ``device_master``。

所有调用都运行在调用方开启的同一个 SQLite 事务内，从而保证“一笔事务
的多次更改、主键迁移以及消费位置在同一个事务中落定”。
"""

from __future__ import annotations

import json
import sqlite3

from . import protocol
from .errors import ProtocolError


def _load_row(conn: sqlite3.Connection, device_id: str) -> dict | None:
    row = conn.execute(
        "SELECT payload FROM device_master WHERE device_id=?", (device_id,)
    ).fetchone()
    if row is None:
        return None
    return json.loads(row["payload"])


def _upsert(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        "INSERT INTO device_master(device_id, payload) VALUES(?, ?) "
        "ON CONFLICT(device_id) DO UPDATE SET payload=excluded.payload",
        (row["device_id"], protocol.canonical_json(row)),
    )


def apply_event(conn: sqlite3.Connection, event: dict) -> None:
    """应用单条行事件。"""
    etype = event["event"]
    if etype == "INSERT":
        row = event["row"]
        existing = _load_row(conn, row["device_id"])
        if existing is not None:
            raise ProtocolError(
                f"INSERT 目标设备已存在: {row['device_id']}（txn={event['txn_id']} "
                f"seq={event['seq']}）"
            )
        _upsert(conn, row)
        return

    if etype == "DELETE":
        pk = event["device_id"]
        existing = _load_row(conn, pk)
        if existing is None:
            raise ProtocolError(
                f"DELETE 目标设备不存在: {pk}（txn={event['txn_id']} seq={event['seq']}）"
            )
        conn.execute("DELETE FROM device_master WHERE device_id=?", (pk,))
        return

    if etype == "UPDATE":
        old_pk = event["old_pk"]
        new_pk = event["new_pk"]
        before = event["before"]
        after = event["after"]
        existing = _load_row(conn, old_pk)
        if existing is None:
            raise ProtocolError(
                f"UPDATE 目标设备不存在: {old_pk}（txn={event['txn_id']} seq={event['seq']}）"
            )
        if existing != before:
            raise ProtocolError(
                f"UPDATE 前镜像不匹配（txn={event['txn_id']} seq={event['seq']} "
                f"device={old_pk}）: 日志={protocol.canonical_json(before)} "
                f"库中={protocol.canonical_json(existing)}"
            )
        if old_pk == new_pk:
            _upsert(conn, after)
        else:
            # 主键迁移：新编号不得与现存设备冲突。
            if _load_row(conn, new_pk) is not None:
                raise ProtocolError(
                    f"主键迁移目标设备已存在: {new_pk}（txn={event['txn_id']} "
                    f"seq={event['seq']}）"
                )
            conn.execute("DELETE FROM device_master WHERE device_id=?", (old_pk,))
            _upsert(conn, after)
        return

    raise ProtocolError(f"不是行事件，无法应用: {etype}")  # pragma: no cover


def apply_txn(conn: sqlite3.Connection, txn_id: str) -> int:
    """按事务内序号回放某事务的全部已暂存行事件，返回事件数。"""
    events = conn.execute(
        "SELECT payload FROM log_events WHERE txn_id=? AND event_type != 'BEGIN' "
        "ORDER BY seq ASC",
        (txn_id,),
    ).fetchall()
    count = 0
    for rec in events:
        event = json.loads(rec["payload"])
        if event["event"] in ("COMMIT", "ABORT"):
            continue
        apply_event(conn, event)
        count += 1
    return count
