"""阶段一：快照暂存与发布。

分片可以乱序到达：每个分片独立校验行数与 SHA-256，通过后才把行写入
``snapshot_staging`` 并登记到 ``snapshot_shards``。全部清单分片都到齐
且校验通过之前，``device_master`` 不会被触碰——业务查询不可能看见
半份快照。发布动作是单条 SQLite 事务（清空主表 -> 灌入暂存行 ->
写入清单元数据与发布标志 -> 清理暂存表），原子可见。
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from . import protocol, store
from .errors import ProtocolError, SnapshotIncomplete
from .fault import FAULT_BEFORE_SNAPSHOT_PUBLISH, FaultInjector


def load_manifest(manifest_path: str) -> dict:
    """读取并校验快照清单。"""
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        raise ProtocolError(f"快照清单不存在: {manifest_path}")
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"快照清单不是合法 JSON: {exc}") from exc
    return protocol.validate_manifest(raw)


def is_published(conn: sqlite3.Connection) -> bool:
    return bool(store.get_meta(conn, "snapshot_published", False))


def stage_shard(conn: sqlite3.Connection, manifest: dict, shard_path: str) -> dict:
    """校验并暂存一个分片（幂等）。

    返回统计字典 ``{"file", "rows", "status"}``，status 取值
    ``staged``（新暂存）或 ``duplicate``（此前已暂存，重新校验通过）。
    """
    fname = os.path.basename(shard_path)
    entries = {s["file"]: s for s in manifest["shards"]}
    if fname not in entries:
        raise ProtocolError(f"分片 {fname} 不在快照清单中")
    entry = entries[fname]

    existing = conn.execute(
        "SELECT shard_file FROM snapshot_shards WHERE shard_file=?",
        (fname,),
    ).fetchone()
    if existing is not None:
        # 重传：重新跑一遍摘要，确认内容没有变化即视为无副作用。
        digest = protocol.sha256_file(shard_path)
        if digest != entry["sha256"]:
            raise ProtocolError(
                f"重传分片 {fname} 摘要与清单不符（可能内容已被替换）"
            )
        return {"file": fname, "rows": entry["rows"], "status": "duplicate"}

    digest = protocol.sha256_file(shard_path)
    if digest != entry["sha256"]:
        raise ProtocolError(
            f"分片 {fname} SHA-256 不符: 期望 {entry['sha256']}，实际 {digest}"
        )

    field_map = manifest["field_map"]
    rows: list[dict] = []
    seen_ids: set[str] = set()
    for lineno, raw_row in protocol.iter_jsonl(shard_path):
        row = protocol.validate_row(raw_row, field_map)
        if row["device_id"] in seen_ids:
            raise ProtocolError(f"{fname}:{lineno} 设备编号重复: {row['device_id']}")
        seen_ids.add(row["device_id"])
        rows.append(row)

    if len(rows) != entry["rows"]:
        raise ProtocolError(
            f"分片 {fname} 行数不符: 清单声明 {entry['rows']}，实际 {len(rows)}"
        )

    # 与暂存表中来自其它分片的行再做一次全局唯一性检查。
    placeholders = ",".join("?" for _ in seen_ids)
    dup = conn.execute(
        f"SELECT device_id FROM snapshot_staging WHERE device_id IN ({placeholders})",
        tuple(seen_ids),
    ).fetchone()
    if dup is not None:
        raise ProtocolError(f"分片 {fname} 与已暂存分片存在重复设备: {dup['device_id']}")

    conn.execute("BEGIN")
    try:
        conn.executemany(
            "INSERT INTO snapshot_staging(device_id, payload) VALUES(?, ?)",
            [(r["device_id"], protocol.canonical_json(r)) for r in rows],
        )
        conn.execute(
            "INSERT INTO snapshot_shards(shard_file, row_count, sha256, staged) "
            "VALUES(?, ?, ?, 1)",
            (fname, entry["rows"], digest),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"file": fname, "rows": len(rows), "status": "staged"}


def shard_status(conn: sqlite3.Connection, manifest: dict) -> list[dict]:
    """对照清单返回每个分片的到达情况。"""
    rows = {
        r["shard_file"]: r
        for r in conn.execute("SELECT * FROM snapshot_shards").fetchall()
    }
    out = []
    for entry in manifest["shards"]:
        rec = rows.get(entry["file"])
        out.append(
            {
                "file": entry["file"],
                "expected_rows": entry["rows"],
                "expected_sha256": entry["sha256"],
                "staged": rec is not None,
                "actual_sha256": rec["sha256"] if rec else None,
            }
        )
    return out


def publish(conn: sqlite3.Connection, manifest: dict, fault: FaultInjector | None = None) -> dict:
    """全部分片到齐后把暂存快照原子发布到主表。

    已发布时为无操作（幂等）。任一分片缺失或已登记摘要与清单不一致时
    抛出 :class:`SnapshotIncomplete`，主表保持原样。
    """
    if is_published(conn):
        return {"status": "already_published", "cut_point_lsn": _published_cut(conn)}

    status = shard_status(conn, manifest)
    missing = [s["file"] for s in status if not s["staged"]]
    if missing:
        raise SnapshotIncomplete(f"快照不完整，缺少分片: {missing}")
    mismatched = [
        s["file"]
        for s in status
        if s["actual_sha256"] != s["expected_sha256"]
    ]
    if mismatched:
        raise SnapshotIncomplete(f"快照摘要不符的分片: {mismatched}")

    total = sum(s["expected_rows"] for s in status)
    staged_count = conn.execute("SELECT COUNT(*) AS c FROM snapshot_staging").fetchone()["c"]
    if staged_count != total:
        raise SnapshotIncomplete(
            f"暂存行数 {staged_count} 与清单声明总行数 {total} 不一致"
        )

    # 注入点：发布前崩溃。重启后分片仍登记在库，重跑 import 即可继续。
    if fault is not None:
        fault.hit(FAULT_BEFORE_SNAPSHOT_PUBLISH)

    snapshot_meta: dict[str, Any] = {
        "cut_point_lsn": manifest["cut_point_lsn"],
        "fields": manifest["fields"],
        "shards": manifest["shards"],
    }
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM device_master")
        conn.execute(
            "INSERT INTO device_master(device_id, payload) "
            "SELECT device_id, payload FROM snapshot_staging"
        )
        store.set_meta(conn, "snapshot_published", True)
        store.set_meta(conn, "snapshot_meta", snapshot_meta)
        # 检查点初始化为切点 S：只应用 commit_lsn > S 的事务。
        store.set_meta(conn, "checkpoint_lsn", manifest["cut_point_lsn"])
        conn.execute("DELETE FROM snapshot_staging")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"status": "published", "cut_point_lsn": manifest["cut_point_lsn"], "rows": total}


def _published_cut(conn: sqlite3.Connection) -> int:
    meta = store.get_meta(conn, "snapshot_meta")
    return int(meta["cut_point_lsn"]) if meta else -1


def import_snapshot(
    conn: sqlite3.Connection,
    manifest_path: str,
    shard_paths: list[str],
    fault: FaultInjector | None = None,
) -> dict:
    """导入入口：读清单 -> 逐分片暂存（允许乱序/重传）-> 满足条件即发布。"""
    manifest = load_manifest(manifest_path)
    results = [stage_shard(conn, manifest, p) for p in shard_paths]
    pub = publish(conn, manifest, fault)
    return {"shards": results, "publish": pub, "cut_point_lsn": manifest["cut_point_lsn"]}
