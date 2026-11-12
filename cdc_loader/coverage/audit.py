"""审计编排引擎：固定四坐标 -> 展开 -> 配对 -> 缺口 -> 原子发布。

一次审计固定：查询时段、契约资料截止批次、读数资料截止批次、设备成员
版本（豁免截止批次随契约批次一起固定，见参数说明）。全程两阶段：

* 阶段一（独立事务提交）：登记 ``building`` 口径，四个坐标就此冻结；
  同步门等待期间即使有新契约/新读数到达，后续查询全部带批次上界，
  固定资料批次不会漂移。
* 阶段二（单条事务）：写全部明细，最后才把状态翻成 ``published``。
  崩溃（含 ``os._exit``）使阶段二整体回滚，只留 building 残档；残档
  不对外可读，用同一口径重做即安全替换。旧 published 审计永不改写。
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import time
from datetime import datetime, timezone

from . import gaps as gap_mod
from . import matcher, protocol, scheduler, snapshot, store
from .errors import AuditStale, CaliberNotFound
from .fault import FAULT_AFTER_DETAIL_BEFORE_CONFIRM, FaultInjector

_GATE_POLL = 0.05
_DEFAULT_GATE_TIMEOUT = 60.0


class SyncGate:
    """基于文件的跨进程同步门（与读数层同一约定，仅测试编排使用）。"""

    def __init__(self, gate_dir: str | None, timeout: float = _DEFAULT_GATE_TIMEOUT) -> None:
        self.gate_dir = gate_dir
        self.timeout = timeout
        self.waiting_path = os.path.join(gate_dir, "waiting") if gate_dir else ""
        self.release_path = os.path.join(gate_dir, "release") if gate_dir else ""

    def wait_if_configured(self, label: str) -> None:
        if not self.gate_dir:
            return
        os.makedirs(self.gate_dir, exist_ok=True)
        with open(self.waiting_path, "w", encoding="utf-8") as fh:
            fh.write(label + "\n")
        deadline = time.monotonic() + self.timeout
        while not os.path.exists(self.release_path):
            if time.monotonic() > deadline:
                raise TimeoutError(f"同步门 {label!r} 等待超时（{self.timeout}s）")
            time.sleep(_GATE_POLL)


def _acquire_lock(db_path: str):
    lock_path = db_path + ".audit.lock"
    fh = open(lock_path, "a+", encoding="utf-8")
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
    return fh


def _db_file_path(conn) -> str:
    return conn.execute("PRAGMA database_list").fetchone()[2]


def _input_hash(
    query_start: datetime,
    query_end: datetime,
    contract_cutoff: int,
    exemption_cutoff: int,
    reading_cutoff: int,
    membership_version: int,
    contracts: list[dict],
    exemptions: list[dict],
    readings: list[dict],
) -> str:
    payload = {
        "query_start_utc": query_start.isoformat(),
        "query_end_utc": query_end.isoformat(),
        "contract_cutoff_batch": contract_cutoff,
        "exemption_cutoff_batch": exemption_cutoff,
        "reading_cutoff_batch": reading_cutoff,
        "membership_version": membership_version,
        "contracts": [
            [c["contract_no"], c["draft_no"], protocol.content_hash(c)]
            for c in contracts
        ],
        "exemptions": sorted(e["exemption_id"] for e in exemptions),
        "readings": [
            [r["origin"], r["reading_key"], r["edition"], r["device"],
             r["occurred_at"], r["metric"], r["ingest_batch_id"]]
            for r in readings
        ],
    }
    return hashlib.sha256(
        protocol.canonical_json(payload).encode("utf-8")
    ).hexdigest()


def run_audit(
    conn,
    reading_db_path: str,
    ledger_db_path: str,
    query_start_raw: str,
    query_end_raw: str,
    contract_cutoff_batch: int,
    reading_cutoff_batch: int,
    membership_version: int,
    exemption_cutoff_batch: int | None = None,
    gate: SyncGate | None = None,
    fault: FaultInjector | None = None,
) -> dict:
    """执行一次覆盖审计（或复用同口径已发布结果）。"""
    lock_fh = _acquire_lock(_db_file_path(conn))
    try:
        return _run_locked(
            conn, reading_db_path, ledger_db_path, query_start_raw, query_end_raw,
            contract_cutoff_batch, reading_cutoff_batch, membership_version,
            exemption_cutoff_batch, gate, fault,
        )
    finally:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        lock_fh.close()


def _run_locked(
    conn, reading_db_path, ledger_db_path, query_start_raw, query_end_raw,
    contract_cutoff_batch, reading_cutoff_batch, membership_version,
    exemption_cutoff_batch, gate, fault,
) -> dict:
    query_start, query_end = snapshot.parse_query_window(query_start_raw, query_end_raw)
    if exemption_cutoff_batch is None:
        exemption_cutoff_batch = int(
            store.get_meta(conn, "last_exemption_batch_id", 0)
        )
    # 豁免水位可以为 0：仅当存在活跃契约而库中没有任何豁免批次时才报错；
    # 若本审计根本没有需要豁免的契约（如全部停用），零水位是合法口径。
    if exemption_cutoff_batch < 0:
        raise CaliberNotFound("豁免截止批次不能为负数")

    # 幂等：同口径已发布审计直接复用，绝不重算改写
    existing = conn.execute(
        "SELECT * FROM audit_runs WHERE query_start_utc=? AND query_end_utc=? "
        "AND contract_cutoff_batch=? AND exemption_cutoff_batch=? "
        "AND reading_cutoff_batch=? "
        "AND membership_version=? AND status='published'",
        (query_start.isoformat(), query_end.isoformat(), contract_cutoff_batch,
         exemption_cutoff_batch, reading_cutoff_batch, membership_version),
    ).fetchone()
    if existing is not None:
        return {"status": "reused", "run_id": existing["run_id"]}

    # 清理同口径 building 残档（崩溃后安全重做）
    stale = conn.execute(
        "SELECT run_id FROM audit_runs WHERE query_start_utc=? AND query_end_utc=? "
        "AND contract_cutoff_batch=? AND exemption_cutoff_batch=? "
        "AND reading_cutoff_batch=? "
        "AND membership_version=? AND status='building'",
        (query_start.isoformat(), query_end.isoformat(), contract_cutoff_batch,
         exemption_cutoff_batch, reading_cutoff_batch, membership_version),
    ).fetchall()
    for row in stale:
        _delete_run_details(conn, row["run_id"])
        conn.execute("DELETE FROM audit_runs WHERE run_id=?", (row["run_id"],))
    conn.commit()

    # 阶段一：口径登记并提交，坐标冻结
    created_at = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO audit_runs(query_start_utc, query_end_utc, "
        "contract_cutoff_batch, exemption_cutoff_batch, reading_cutoff_batch, "
        "membership_version, status, created_at) "
        "VALUES(?, ?, ?, ?, ?, ?, 'building', ?)",
        (query_start.isoformat(), query_end.isoformat(), contract_cutoff_batch,
         exemption_cutoff_batch, reading_cutoff_batch, membership_version,
         created_at),
    )
    run_id = int(cur.lastrowid)
    conn.commit()

    # 同步门：此期间另一进程可投递新批次，但下方查询都带批次上界
    if gate is not None:
        gate.wait_if_configured(
            f"audit={run_id};contract_cutoff={contract_cutoff_batch};"
            f"reading_cutoff={reading_cutoff_batch};membership={membership_version}"
        )

    # ---- 固定快照（全部带显式批次上界，不受门后新数据影响）----
    contracts = snapshot.load_current_contracts(conn, contract_cutoff_batch)
    active_contracts = [c for c in contracts if c["active"]]
    exemptions = snapshot.load_exemptions(
        conn, [c["contract_no"] for c in active_contracts], exemption_cutoff_batch
    )
    readings_raw = snapshot.load_readings(reading_db_path, reading_cutoff_batch)
    membership = snapshot.load_membership(ledger_db_path, membership_version)

    exemptions_by_contract: dict[str, list[dict]] = {}
    for ex in exemptions:
        exemptions_by_contract.setdefault(ex["contract_no"], []).append(ex)

    items = scheduler.expand_all(
        active_contracts, query_start, query_end, exemptions_by_contract
    )
    readings = matcher.parse_reading_rows(readings_raw, membership["entity_of"])
    decisions = matcher.match_all(readings, items, active_contracts)
    summary = matcher.apply_coverage(items, decisions)
    gap_rows = gap_mod.build_gaps(
        items, summary["missing_items"], summary["unmatched"]
    )
    input_hash = _input_hash(
        query_start, query_end, contract_cutoff_batch, exemption_cutoff_batch,
        reading_cutoff_batch, membership_version, contracts, exemptions, readings_raw,
    )

    zero_note = None
    if summary["denominator"] == 0:
        zero_note = (
            "查询窗口与生效日程交集内无任何非豁免应到项；覆盖比例未定义，"
            "不得记为 100% 或称为全量正常"
        )

    # 阶段二：明细 + 发布在单条事务内落定
    conn.execute("BEGIN IMMEDIATE")
    try:
        _persist_expected(conn, run_id, items)
        _persist_matches(conn, run_id, summary["match_rows"])
        _persist_unmatched(conn, run_id, summary["unmatched"])
        _persist_gaps(conn, run_id, gap_rows)

        if fault is not None:
            fault.hit(FAULT_AFTER_DETAIL_BEFORE_CONFIRM)

        conn.execute(
            "UPDATE audit_runs SET status='published', published_at=?, "
            "expected_count=?, covered_count=?, missing_count=?, exempt_count=?, "
            "duplicate_count=?, unmatched_count=?, zero_expected_note=?, "
            "input_hash=? WHERE run_id=?",
            (
                datetime.now(timezone.utc).isoformat(),
                summary["expected_total"],
                summary["covered_count"],
                len(summary["missing_items"]),
                summary["exempt_count"],
                summary["duplicate_count"],
                len(summary["unmatched"]),
                zero_note,
                input_hash,
                run_id,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "status": "published",
        "run_id": run_id,
        "expected_count": summary["expected_total"],
        "denominator_non_exempt": summary["denominator"],
        "covered_count": summary["covered_count"],
        "missing_count": len(summary["missing_items"]),
        "exempt_count": summary["exempt_count"],
        "duplicate_count": summary["duplicate_count"],
        "unmatched_count": len(summary["unmatched"]),
        "gap_count": len(gap_rows),
        "zero_expected_note": zero_note,
        "input_hash": input_hash,
    }


def _delete_run_details(conn, run_id: int) -> None:
    conn.execute("DELETE FROM audit_expected WHERE run_id=?", (run_id,))
    conn.execute("DELETE FROM audit_matches WHERE run_id=?", (run_id,))
    conn.execute("DELETE FROM audit_gaps WHERE run_id=?", (run_id,))
    conn.execute("DELETE FROM audit_unmatched WHERE run_id=?", (run_id,))


def _persist_expected(conn, run_id: int, items: list[scheduler.ExpectedItem]) -> None:
    conn.executemany(
        "INSERT INTO audit_expected(run_id, item_id, contract_no, draft_no, origin, "
        "entity_id, metric, scheduled_utc, tz_name, local_time, local_date, "
        "utc_offset, fold_occurrence, segment_date, segment_start, step_index, "
        "exempt, exemption_ids, covered) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                run_id, it.item_id, it.contract_no, it.draft_no, it.origin,
                it.entity_id, it.metric, it.scheduled_utc.isoformat(), it.tz_name,
                it.local_time, it.local_date, it.utc_offset, it.fold_occurrence,
                it.segment_date, it.segment_start, it.step_index,
                1 if it.exempt else 0, protocol.canonical_json(it.exemption_ids),
                1 if it.covered else 0,
            )
            for it in items
        ],
    )


def _persist_matches(conn, run_id: int, match_rows: list[dict]) -> None:
    conn.executemany(
        "INSERT INTO audit_matches(run_id, item_id, origin, reading_key, "
        "chosen_edition, device, entity_id, occurred_at_utc, distance_seconds, "
        "is_primary, duplicate_hit, ingest_batch_id) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                run_id, m["item_id"], m["reading"].origin, m["reading"].reading_key,
                m["reading"].chosen_edition, m["reading"].device,
                m["reading"].entity_id, m["reading"].occurred_at_utc.isoformat(),
                m["distance_seconds"], 1, m["duplicate_hit"],
                m["reading"].ingest_batch_id,
            )
            for m in match_rows
        ],
    )


def _persist_unmatched(conn, run_id: int, unmatched: list[matcher.MatchDecision]) -> None:
    conn.executemany(
        "INSERT INTO audit_unmatched(run_id, origin, reading_key, chosen_edition, "
        "device, entity_id, metric, occurred_at_utc, reason, candidates, "
        "ingest_batch_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                run_id, d.reading.origin, d.reading.reading_key,
                d.reading.chosen_edition, d.reading.device, d.reading.entity_id,
                d.reading.metric, d.reading.occurred_at_utc.isoformat(), d.reason,
                protocol.canonical_json(d.candidates), d.reading.ingest_batch_id,
            )
            for d in unmatched
        ],
    )


def _persist_gaps(conn, run_id: int, gap_rows: list[dict]) -> None:
    conn.executemany(
        "INSERT INTO audit_gaps(run_id, gap_id, contract_no, draft_no, "
        "start_item_id, end_item_id, start_utc, end_utc, missing_item_ids, "
        "split_exemptions, nearby_readings) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                run_id, g["gap_id"], g["contract_no"], g["draft_no"],
                g["start_item_id"], g["end_item_id"], g["start_utc"], g["end_utc"],
                protocol.canonical_json(g["missing_item_ids"]),
                protocol.canonical_json(g["split_exemptions"]),
                protocol.canonical_json(g["nearby_readings"]),
            )
            for g in gap_rows
        ],
    )


def require_published(conn, run_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM audit_runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        raise CaliberNotFound(f"审计 {run_id} 不存在")
    if row["status"] != "published":
        raise AuditStale(
            f"审计 {run_id} 状态为 {row['status']}（明细未对外确认），"
            "不返回半份结果；请用同一口径重做"
        )
    return row
