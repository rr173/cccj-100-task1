"""读数批次摄入：逐行资料 -> 不可变逐稿件事实仓。

核心不变量
==========

* **稿件不可变**：``reading_editions`` 只追加。身份
  (origin, reading_key, edition) 已存在时，内容必须逐字一致（幂等
  重传）；任何字段被改写都按 :class:`EditionConflict` 整批拒收。
* **批次原子**：一次投递的全部稿件与批次登记在同一条 SQLite 事务内
  落定，任何矛盾或崩溃都使整批回滚，事实仓维持投递前状态。
* **只追加不改写**：新稿件号可以改读数/时间/设备/指标，也可以撤回；
  选版时永远取最大 edition，迟来的旧稿件无法盖过新版。
"""

from __future__ import annotations

import hashlib
import sqlite3

from . import protocol, store
from .errors import EditionConflict, ReadingProtocolError


def _read_batch_rows(batch_files: list[str]) -> list[dict]:
    """读取并逐行校验全部批次文件，返回规范化行（保持投递顺序）。"""
    rows: list[dict] = []
    for path in batch_files:
        for lineno, raw in protocol.iter_jsonl(path):
            rows.append(
                protocol.validate_reading_row(raw, f"{path}:{lineno}")
            )
    if not rows:
        raise ReadingProtocolError("批次为空：没有任何读数行")
    return rows


def _dedupe_batch(rows: list[dict]) -> list[dict]:
    """批内去重：同一稿件身份内容一致去重，内容不同即整批拒绝。"""
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = protocol.identity_of(row)
        if key in seen:
            if protocol.content_of(seen[key]) != protocol.content_of(row):
                raise EditionConflict(
                    f"批内同一稿件身份 {key} 出现两种内容: "
                    f"{protocol.canonical_json(protocol.content_of(seen[key]))} 与 "
                    f"{protocol.canonical_json(protocol.content_of(row))}"
                )
            continue
        seen[key] = row
    return list(seen.values())


def _check_against_fact(
    conn: sqlite3.Connection, rows: list[dict]
) -> tuple[list[dict], int]:
    """逐行对照事实仓：旧稿件内容必须逐字一致；返回 (新增, 重传数)。

    对批内每一行做内容比对——同稿件身份换了任何内容字段即拒收，
    不允许“同稿件换内容”。
    """
    new_rows: list[dict] = []
    replayed = 0
    for row in rows:
        origin, reading_key, edition = protocol.identity_of(row)
        existing = conn.execute(
            "SELECT device, occurred_at, metric, amount, withdrawn "
            "FROM reading_editions WHERE origin=? AND reading_key=? AND edition=?",
            (origin, reading_key, edition),
        ).fetchone()
        if existing is not None:
            stored = {
                "device": existing["device"],
                "occurred_at": existing["occurred_at"],
                "metric": existing["metric"],
                "amount": existing["amount"],
                "withdrawn": bool(existing["withdrawn"]),
            }
            if stored != protocol.content_of(row):
                raise EditionConflict(
                    f"稿件 {(origin, reading_key, edition)} 的内容与事实仓已登记"
                    "版本不一致（同稿件不得换内容）: "
                    f"事实仓={protocol.canonical_json(stored)} "
                    f"投递={protocol.canonical_json(protocol.content_of(row))}"
                )
            replayed += 1
        else:
            new_rows.append(row)
    return new_rows, replayed


def ingest_batch(conn: sqlite3.Connection, batch_files: list[str]) -> dict:
    """摄入一次读数投递（一个不可分割的确认批次）。

    单条 SQLite 事务内完成：稿件对照与写入 -> 批次登记 -> meta 进度。
    任何协议/篡改错误整体回滚，事实仓维持投递前状态。
    """
    rows = _dedupe_batch(_read_batch_rows(batch_files))

    conn.execute("BEGIN IMMEDIATE")
    try:
        new_rows, replayed = _check_against_fact(conn, rows)

        batch_id = int(store.get_meta(conn, "last_batch_id", 0)) + 1
        batch_hash = hashlib.sha256(
            "\n".join(
                sorted(protocol.canonical_json(r) for r in rows)
            ).encode("utf-8")
        ).hexdigest()
        conn.execute(
            "INSERT INTO ingest_batches(batch_id, row_count, new_editions, "
            "content_hash) VALUES(?, ?, ?, ?)",
            (batch_id, len(rows), len(new_rows), batch_hash),
        )
        conn.executemany(
            "INSERT INTO reading_editions(origin, reading_key, edition, device, "
            "occurred_at, occurred_date, metric, amount, withdrawn, "
            "ingest_batch_id, ingest_seq, content_hash) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["origin"],
                    r["reading_key"],
                    r["edition"],
                    r["device"],
                    r["occurred_at"],
                    r["occurred_date"],
                    r["metric"],
                    r["amount"],
                    1 if r["withdrawn"] else 0,
                    batch_id,
                    idx,
                    protocol.content_hash(r),
                )
                for idx, r in enumerate(new_rows)
            ],
        )
        store.set_meta(conn, "last_batch_id", batch_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "status": "accepted",
        "batch_id": batch_id,
        "rows": len(rows),
        "new_editions": len(new_rows),
        "replayed_editions": replayed,
    }


def max_batch_id(conn: sqlite3.Connection) -> int:
    return int(store.get_meta(conn, "last_batch_id", 0))
