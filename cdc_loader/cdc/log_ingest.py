"""阶段二：变更日志追赶。

设计要点
========

1. **流式落库，不靠内存字典。** 日志逐行读取、逐事件校验后直接写入
   ``log_events`` / ``log_txn`` 表；进程退出后暂存事件仍在 SQLite 中，
   重启继续消费即可。
2. **事件身份去重与冲突检测。** 身份为 ``(txn_id, seq)``（BEGIN 用
   空序号）。同一身份重复出现且规范化载荷完全相同 => 重传，无副作用；
   载荷不同 => :class:`PayloadConflict`，拒绝继续。
3. **只应用切点之后的完整事务。** 快照发布后检查点为切点 S，仅
   ``commit_lsn > S`` 且状态为 COMMITTED 的事务可进入主表；ABORTED、
   只有 BEGIN 的 OPEN 事务永远不应用。
4. **按提交顺序应用。** 首次出现的提交位置（流水中的先后次序）递增，
   应用时再按 ``commit_lsn`` 升序，二者一致；每个事务在**同一个 SQLite
   事务**内完成行变更、检查点推进与已应用标记。
5. **旧日志重放幂等。** 已应用事务的事件再次送入只会命中去重；已推进
   的检查点不会回退。
"""

from __future__ import annotations

import json
import sqlite3

from . import applier, protocol, snapshot, store
from .errors import PayloadConflict, ProtocolError, SnapshotIncomplete
from .fault import (
    FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN,
    FAULT_BEFORE_TXN_COMMIT,
    FaultInjector,
)

# log_txn.state 取值
STATE_OPEN = "OPEN"
STATE_COMMITTED = "COMMITTED"
STATE_ABORTED = "ABORTED"

# BEGIN 没有事务内序号，在 log_events 中以哨兵 0 存储（真实 seq 从 1 起，
# 主键 (txn_id, seq) 因此仍能对 BEGIN 去重——WITHOUT ROWID 表不允许 NULL 主键）。
BEGIN_SEQ = 0

_TERMINAL_STATES = frozenset({STATE_COMMITTED, STATE_ABORTED})


class LogIngester:
    """日志消费器；绑定一个数据库连接与故障注入器。"""

    def __init__(self, conn: sqlite3.Connection, fault: FaultInjector | None = None) -> None:
        self.conn = conn
        self.fault = fault
        self.counters = {
            "files": 0,
            "events_seen": 0,
            "events_staged": 0,
            "events_duplicate": 0,
            "txn_applied": 0,
            "row_changes": 0,
        }

    # ------------------------------------------------------------------
    # 日志文件摄入（暂存）
    # ------------------------------------------------------------------

    def require_published(self) -> int:
        if not snapshot.is_published(self.conn):
            raise SnapshotIncomplete(
                "快照尚未完整发布，拒绝追赶日志（业务查询不能看见半份快照）"
            )
        return int(store.get_meta(self.conn, "checkpoint_lsn"))

    def ingest_file(self, path: str) -> dict:
        """把一个 JSONL 日志文件的全部事件暂存入库。"""
        self.require_published()
        field_map = store.get_meta(self.conn, "snapshot_meta")["fields"]
        field_map = {f["name"]: f["type"] for f in field_map}

        staged = duplicated = seen = 0
        for lineno, raw in protocol.iter_jsonl(path):
            seen += 1
            event = protocol.validate_event(raw, field_map)
            if self._stage_event(event, path, lineno):
                staged += 1
            else:
                duplicated += 1

        self.counters["files"] += 1
        self.counters["events_seen"] += seen
        self.counters["events_staged"] += staged
        self.counters["events_duplicate"] += duplicated

        applied, changes = self.apply_ready()
        self.counters["txn_applied"] += applied
        self.counters["row_changes"] += changes
        return {
            "file": path,
            "events_seen": seen,
            "events_staged": staged,
            "events_duplicate": duplicated,
            "txn_applied_this_file": applied,
            "row_changes_this_file": changes,
        }

    def _stage_event(self, event: dict, source_file: str, line_no: int) -> bool:
        """暂存单事件，返回 True 表示新写入、False 表示重传。

        BEGIN 没有 seq，在 log_events 中以 NULL 表示；其余事件 seq 必填。
        """
        conn = self.conn
        txn_id = event["txn_id"]
        etype = event["event"]
        seq = event.get("seq")  # BEGIN -> None
        stored_seq = BEGIN_SEQ if seq is None else seq
        identity = protocol.event_identity(txn_id, seq)
        payload = protocol.canonical_json(event)

        txn = conn.execute(
            "SELECT state, payload_begin FROM log_txn WHERE txn_id=?", (txn_id,)
        ).fetchone()

        existing = conn.execute(
            "SELECT payload FROM log_events WHERE txn_id=? AND seq=?",
            (txn_id, stored_seq),
        ).fetchone()
        if existing is not None:
            if existing["payload"] != payload:
                raise PayloadConflict(
                    f"事件身份 {identity} 出现不同载荷："
                    f"已存={existing['payload']} 新到={payload}"
                    f"（来源 {source_file}:{line_no}）"
                )
            return False  # 完全相同的重传，无副作用

        if etype == "BEGIN":
            if txn is not None:
                # 事务已存在却没有 BEGIN 行，说明此前的 BEGIN 载荷不同——
                # 同一身份冲突已在上面拦截，这里只防御状态异常。
                raise ProtocolError(f"事务 {txn_id} 的 BEGIN 与既有状态冲突")
            conn.execute("BEGIN")
            try:
                conn.execute(
                    "INSERT INTO log_events(txn_id, seq, event_type, payload, "
                    "source_file, line_no) VALUES(?, ?, 'BEGIN', ?, ?, ?)",
                    (txn_id, BEGIN_SEQ, payload, source_file, line_no),
                )
                conn.execute(
                    "INSERT INTO log_txn(txn_id, state, first_seen, payload_begin) "
                    "VALUES(?, ?, ?, ?)",
                    (txn_id, STATE_OPEN, -1, payload),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            return True

        # 非 BEGIN 事件必须先有 BEGIN。
        if txn is None:
            raise ProtocolError(
                f"事件 {identity} 在缺少 BEGIN 的情况下出现（{source_file}:{line_no}）"
            )
        if txn["state"] in _TERMINAL_STATES:
            raise ProtocolError(
                f"事务 {txn_id} 已处于终态 {txn['state']}，又收到 {etype} 事件"
            )

        # 事务内序号递增校验（允许重传，重传已在上方去重返回）。
        prev = conn.execute(
            "SELECT MAX(seq) AS m FROM log_events WHERE txn_id=? AND seq IS NOT NULL",
            (txn_id,),
        ).fetchone()["m"]
        if prev is not None and seq <= prev:
            raise ProtocolError(
                f"事务 {txn_id} 的序号未递增：已有最大 seq={prev}，新事件 seq={seq}"
            )

        conn.execute("BEGIN")
        try:
            conn.execute(
                "INSERT INTO log_events(txn_id, seq, event_type, payload, "
                "source_file, line_no) VALUES(?, ?, ?, ?, ?, ?)",
                (txn_id, seq, etype, payload, source_file, line_no),
            )

            if etype == "COMMIT":
                order = int(store.get_meta(conn, "txn_commit_order", 0)) + 1
                # 首次出现的提交位置必须递增：新 commit_lsn 不得小于此前
                # 见过的任一提交（重传的同一事务在上方已去重，走不到这里）。
                max_lsn = conn.execute(
                    "SELECT MAX(commit_lsn) AS m FROM log_txn WHERE state=?",
                    (STATE_COMMITTED,),
                ).fetchone()["m"]
                if max_lsn is not None and event["commit_lsn"] <= max_lsn:
                    raise ProtocolError(
                        f"首次出现的提交位置未递增：已有最大 commit_lsn={max_lsn}，"
                        f"新事务 {txn_id} commit_lsn={event['commit_lsn']}"
                    )
                conn.execute(
                    "UPDATE log_txn SET state=?, commit_lsn=?, first_seen=? "
                    "WHERE txn_id=?",
                    (STATE_COMMITTED, event["commit_lsn"], order, txn_id),
                )
                store.set_meta(conn, "txn_commit_order", order)
            elif etype == "ABORT":
                conn.execute(
                    "UPDATE log_txn SET state=? WHERE txn_id=?",
                    (STATE_ABORTED, txn_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return True

    # ------------------------------------------------------------------
    # 应用已提交事务
    # ------------------------------------------------------------------

    def apply_ready(self) -> tuple[int, int]:
        """把所有可应用的已提交事务按 commit_lsn 顺序落定。

        返回 ``(应用事务数, 行事件数)``。已应用（commit_lsn 不大于当前
        检查点）的事务跳过；中止/未闭合事务不参与。
        """
        checkpoint = self.require_published()
        conn = self.conn
        rows = conn.execute(
            "SELECT txn_id, commit_lsn, first_seen FROM log_txn "
            "WHERE state=? AND commit_lsn > ? ORDER BY commit_lsn ASC, first_seen ASC",
            (STATE_COMMITTED, checkpoint),
        ).fetchall()

        applied = changes = 0
        for rec in rows:
            self._apply_one(rec["txn_id"], int(rec["commit_lsn"]))
            applied += 1
            changes += self._last_change_count
        return applied, changes

    def _apply_one(self, txn_id: str, commit_lsn: int) -> None:
        conn = self.conn
        # 注入点：事务提交前崩溃。行变更与检查点都还没提交，
        # 主表与检查点维持旧值；重启后该事务仍可从暂存表完整回放。
        if self.fault is not None:
            self.fault.hit(FAULT_BEFORE_TXN_COMMIT)

        conn.execute("BEGIN IMMEDIATE")
        try:
            n_changes = applier.apply_txn(conn, txn_id)
            store.set_meta(conn, "checkpoint_lsn", commit_lsn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        self._last_change_count = n_changes

        # 注入点：提交成功、尚未返回。重启后检查点已包含该事务，
        # 重放日志只会命中去重，不会重复应用。
        if self.fault is not None:
            self.fault.hit(FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN)


def ingest_logs(
    conn: sqlite3.Connection,
    log_paths: list[str],
    fault: FaultInjector | None = None,
) -> dict:
    """继续消费入口：按给定文件顺序摄入日志并应用就绪事务。"""
    ingester = LogIngester(conn, fault)
    file_reports = [ingester.ingest_file(p) for p in log_paths]
    return {
        "files": file_reports,
        "counters": ingester.counters,
        "checkpoint_lsn": store.get_meta(conn, "checkpoint_lsn"),
    }


def open_transactions(conn: sqlite3.Connection) -> list[dict]:
    """返回仍处于 OPEN（只有 BEGIN 未见 COMMIT/ABORT）的事务。"""
    return [
        {"txn_id": r["txn_id"]}
        for r in conn.execute(
            "SELECT txn_id FROM log_txn WHERE state=? ORDER BY txn_id", (STATE_OPEN,)
        ).fetchall()
    ]
