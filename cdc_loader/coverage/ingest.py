"""契约与豁免批次摄入：不可变稿件登记、同稿篡改整批拒收、批次原子性。

与读数仓同一套纪律：

* 契约稿号身份 ``(contract_no, draft_no)``、豁免身份 ``exemption_id``
  只追加，永不改写；
* 身份已存在时内容必须逐字一致（幂等重传），任何字段被改写即
  :class:`DraftConflict` 整批拒收，批次号不前进；
* 一次投递的全部行与批次登记在同一条 SQLite 事务内落定。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3

from . import protocol, store
from .errors import CoverageProtocolError, DraftConflict


def _read_rows(batch_files: list[str], kind: str) -> list[dict]:
    rows: list[dict] = []
    validator = (
        protocol.validate_contract_row
        if kind == "contract"
        else protocol.validate_exemption_row
    )
    for path in batch_files:
        for lineno, raw in protocol.iter_jsonl(path):
            rows.append(validator(raw, f"{path}:{lineno}"))
    if not rows:
        raise CoverageProtocolError("批次为空：没有任何有效行")
    return rows


def _dedupe(rows: list[dict], identity_fn) -> list[dict]:
    seen: dict = {}
    for row in rows:
        key = identity_fn(row)
        if key in seen and seen[key] != row:
            raise DraftConflict(f"批内同一身份 {key!r} 出现两种内容")
        seen[key] = row
    return list(seen.values())


def _batch_hash(rows: list[dict]) -> str:
    return hashlib.sha256(
        "\n".join(sorted(protocol.canonical_json(r) for r in rows)).encode("utf-8")
    ).hexdigest()


def ingest_contract_batch(conn: sqlite3.Connection, batch_files: list[str]) -> dict:
    """摄入一次契约投递（不可分割的确认批次）。"""
    rows = _dedupe(
        _read_rows(batch_files, "contract"), protocol.contract_identity
    )

    conn.execute("BEGIN IMMEDIATE")
    try:
        new_rows = []
        replayed = 0
        for row in rows:
            contract_no, draft_no = protocol.contract_identity(row)
            existing = conn.execute(
                "SELECT active, origin, entity_id, metric, effective_start_utc, "
                "effective_end_utc, timezone, weekdays, segments, step_seconds, "
                "tolerance_seconds FROM contract_drafts "
                "WHERE contract_no=? AND draft_no=?",
                (contract_no, draft_no),
            ).fetchone()
            if existing is not None:
                stored = {
                    "active": bool(existing["active"]),
                    "origin": existing["origin"],
                    "entity_id": existing["entity_id"],
                    "metric": existing["metric"],
                    "effective_start_utc": existing["effective_start_utc"],
                    "effective_end_utc": existing["effective_end_utc"],
                    "timezone": existing["timezone"],
                    "weekdays": json.loads(existing["weekdays"]),
                    "segments": json.loads(existing["segments"]),
                    "step_seconds": existing["step_seconds"],
                    "tolerance_seconds": existing["tolerance_seconds"],
                }
                if stored != protocol.contract_content(row):
                    raise DraftConflict(
                        f"契约稿 {(contract_no, draft_no)} 内容与已登记稿件不一致"
                        "（同稿号不得换内容，新日程请用新稿号）: "
                        f"已登记={protocol.canonical_json(stored)} "
                        f"投递={protocol.canonical_json(protocol.contract_content(row))}"
                    )
                replayed += 1
            else:
                new_rows.append(row)

        batch_id = int(store.get_meta(conn, "last_contract_batch_id", 0)) + 1
        conn.execute(
            "INSERT INTO contract_batches(batch_id, row_count, new_drafts, "
            "content_hash) VALUES(?, ?, ?, ?)",
            (batch_id, len(rows), len(new_rows), _batch_hash(rows)),
        )
        conn.executemany(
            "INSERT INTO contract_drafts(contract_no, draft_no, active, origin, "
            "entity_id, metric, effective_start_utc, effective_end_utc, timezone, "
            "weekdays, segments, step_seconds, tolerance_seconds, ingest_batch_id, "
            "ingest_seq, content_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    r["contract_no"],
                    r["draft_no"],
                    1 if r["active"] else 0,
                    r["origin"],
                    r["entity_id"],
                    r["metric"],
                    r["effective_start_utc"],
                    r["effective_end_utc"],
                    r["timezone"],
                    protocol.canonical_json(r["weekdays"]),
                    protocol.canonical_json(r["segments"]),
                    r["step_seconds"],
                    r["tolerance_seconds"],
                    batch_id,
                    idx,
                    protocol.content_hash(r),
                )
                for idx, r in enumerate(new_rows)
            ],
        )
        store.set_meta(conn, "last_contract_batch_id", batch_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "status": "accepted",
        "kind": "contract",
        "batch_id": batch_id,
        "rows": len(rows),
        "new_drafts": len(new_rows),
        "replayed_drafts": replayed,
    }


def ingest_exemption_batch(conn: sqlite3.Connection, batch_files: list[str]) -> dict:
    """摄入一次停机豁免投递（不可分割的确认批次）。"""
    rows = _dedupe(_read_rows(batch_files, "exemption"), protocol.exemption_identity)

    conn.execute("BEGIN IMMEDIATE")
    try:
        new_rows = []
        replayed = 0
        for row in rows:
            ex_id = protocol.exemption_identity(row)
            existing = conn.execute(
                "SELECT contract_no, start_utc, end_utc, reason FROM exemptions "
                "WHERE exemption_id=?",
                (ex_id,),
            ).fetchone()
            if existing is not None:
                stored = {
                    "contract_no": existing["contract_no"],
                    "start_utc": existing["start_utc"],
                    "end_utc": existing["end_utc"],
                    "reason": existing["reason"],
                }
                if stored != protocol.exemption_content(row):
                    raise DraftConflict(
                        f"豁免 {ex_id!r} 内容与已登记记录不一致（豁免不可变）: "
                        f"已登记={protocol.canonical_json(stored)} "
                        f"投递={protocol.canonical_json(protocol.exemption_content(row))}"
                    )
                replayed += 1
            else:
                new_rows.append(row)

        batch_id = int(store.get_meta(conn, "last_exemption_batch_id", 0)) + 1
        conn.execute(
            "INSERT INTO exemption_batches(batch_id, row_count, new_rows, "
            "content_hash) VALUES(?, ?, ?, ?)",
            (batch_id, len(rows), len(new_rows), _batch_hash(rows)),
        )
        conn.executemany(
            "INSERT INTO exemptions(exemption_id, contract_no, start_utc, end_utc, "
            "reason, ingest_batch_id, ingest_seq, content_hash) "
            "VALUES(?,?,?,?,?,?,?,?)",
            [
                (
                    r["exemption_id"],
                    r["contract_no"],
                    r["start_utc"],
                    r["end_utc"],
                    r["reason"],
                    batch_id,
                    idx,
                    protocol.content_hash(r),
                )
                for idx, r in enumerate(new_rows)
            ],
        )
        store.set_meta(conn, "last_exemption_batch_id", batch_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "status": "accepted",
        "kind": "exemption",
        "batch_id": batch_id,
        "rows": len(rows),
        "new_rows": len(new_rows),
        "replayed_rows": replayed,
    }
