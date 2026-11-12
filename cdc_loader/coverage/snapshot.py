"""固定资料批次的快照读取：契约稿、豁免、读数选版、成员版本。

一次审计的四个口径坐标在审计开始即冻结，后续全部计算只基于这些快照：

1. ``contract_cutoff_batch``：契约只取随批次号 <= 截止批次登记的稿件，
   每个契约取截止范围内**最大稿号**（停用稿 active=0 即不展开日程）；
2. 豁免只取随豁免批次 <= 截止批次登记的记录（豁免批次与契约批次各自
   编号，审计同时固定两个豁免水位——这里统一用“豁免截止批次”，其值
   取自豁免批次表）；
3. ``reading_cutoff_batch``：读数只取读数仓中 ``ingest_batch_id <= 截止``
   的稿件，每条观测选最大 edition，撤回稿不参与；
4. ``membership_version``：设备 -> 实体只查归并层该版本快照，绝不套用
   今天的成员。
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime

from .errors import CaliberNotFound
from .timeutil import to_utc


def _open_readonly(db_path: str, label: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise CaliberNotFound(f"{label}数据库不存在: {db_path}")
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError as exc:
        raise CaliberNotFound(f"无法只读打开{label}数据库 {db_path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# 契约与豁免（本库）
# ---------------------------------------------------------------------------


def load_current_contracts(conn: sqlite3.Connection, cutoff_batch: int) -> list[dict]:
    """取截止契约批次内每个契约编号的最大稿号稿件（含停用稿标记）。"""
    last = int(conn.execute(
        "SELECT COALESCE(MAX(batch_id), 0) AS m FROM contract_batches"
    ).fetchone()["m"])
    if cutoff_batch < 1 or cutoff_batch > last:
        raise CaliberNotFound(
            f"契约截止批次 {cutoff_batch} 不在已确认范围 1..{last} 内"
        )
    rows = conn.execute(
        """
        SELECT d.*
        FROM contract_drafts d
        JOIN (
            SELECT contract_no, MAX(draft_no) AS max_draft
            FROM contract_drafts
            WHERE ingest_batch_id <= ?
            GROUP BY contract_no
        ) t ON t.contract_no = d.contract_no AND t.max_draft = d.draft_no
        WHERE d.ingest_batch_id <= ?
        ORDER BY d.contract_no
        """,
        (cutoff_batch, cutoff_batch),
    ).fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "contract_no": r["contract_no"],
                "draft_no": r["draft_no"],
                "active": bool(r["active"]),
                "origin": r["origin"],
                "entity_id": r["entity_id"],
                "metric": r["metric"],
                "effective_start_utc": r["effective_start_utc"],
                "effective_end_utc": r["effective_end_utc"],
                "timezone": r["timezone"],
                "weekdays": json.loads(r["weekdays"]),
                "segments": json.loads(r["segments"]),
                "step_seconds": r["step_seconds"],
                "tolerance_seconds": r["tolerance_seconds"],
            }
        )
    return out


def load_exemptions(
    conn: sqlite3.Connection, contract_nos: list[str], cutoff_batch: int
) -> list[dict]:
    """取截止豁免批次内、与给定契约关联的豁免区间（UTC，左闭右开）。

    截止批次 0 表示“本审计不固定任何豁免资料”（库中尚无豁免批次时的
    合法口径），返回空列表。
    """
    if cutoff_batch == 0:
        return []
    last = int(conn.execute(
        "SELECT COALESCE(MAX(batch_id), 0) AS m FROM exemption_batches"
    ).fetchone()["m"])
    if cutoff_batch < 1 or cutoff_batch > last:
        raise CaliberNotFound(
            f"豁免截止批次 {cutoff_batch} 不在已确认范围 1..{last} 内"
        )
    if not contract_nos:
        return []
    placeholders = ",".join("?" for _ in contract_nos)
    rows = conn.execute(
        f"SELECT * FROM exemptions WHERE ingest_batch_id <= ? "
        f"AND contract_no IN ({placeholders}) ORDER BY exemption_id",
        [cutoff_batch, *contract_nos],
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 读数事实仓（只读）
# ---------------------------------------------------------------------------


def load_readings(reading_db_path: str, cutoff_batch: int) -> list[dict]:
    """在读数仓截止批次内选版：每观测取最大 edition，撤回稿不参与。

    与读数仓 build 的选版纪律一致，但本函数独立实现且只读：
    先取截止范围内每观测最大 edition，再排除该最新稿 withdrawn=1 的观测。
    """
    conn = _open_readonly(reading_db_path, "读数事实仓")
    try:
        try:
            last = int(conn.execute(
                "SELECT COALESCE(MAX(batch_id), 0) AS m FROM ingest_batches"
            ).fetchone()["m"])
        except sqlite3.OperationalError as exc:
            raise CaliberNotFound(
                f"读数库缺少 ingest_batches 表（不是有效的读数事实仓）: {exc}"
            ) from exc
        if cutoff_batch < 1 or cutoff_batch > last:
            raise CaliberNotFound(
                f"读数截止批次 {cutoff_batch} 不在读数仓已确认范围 1..{last} 内"
            )
        rows = conn.execute(
            """
            SELECT e.origin, e.reading_key, e.edition, e.device,
                   e.occurred_at, e.metric, e.withdrawn, e.ingest_batch_id
            FROM reading_editions e
            JOIN (
                SELECT origin, reading_key, MAX(edition) AS max_edition
                FROM reading_editions
                WHERE ingest_batch_id <= ?
                GROUP BY origin, reading_key
            ) t ON t.origin = e.origin AND t.reading_key = e.reading_key
               AND t.max_edition = e.edition
            WHERE e.ingest_batch_id <= ? AND e.withdrawn = 0
            ORDER BY e.origin, e.reading_key
            """,
            (cutoff_batch, cutoff_batch),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 归并层成员版本（只读）
# ---------------------------------------------------------------------------


def load_membership(ledger_db_path: str, version: int) -> dict:
    """取归并层某分组版本的 设备->实体 映射快照。"""
    conn = _open_readonly(ledger_db_path, "归并层台账")
    try:
        try:
            vrow = conn.execute(
                "SELECT version FROM decision_versions WHERE version=?", (version,)
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise CaliberNotFound(
                f"归并层台账缺少 decision_versions 表: {exc}"
            ) from exc
        if vrow is None:
            available = [
                r["version"]
                for r in conn.execute(
                    "SELECT version FROM decision_versions ORDER BY version"
                )
            ]
            raise CaliberNotFound(
                f"归并层成员版本 {version} 不存在；可用版本: {available}"
            )
        entity_of: dict[str, str] = {}
        for r in conn.execute(
            "SELECT device_id, entity_id FROM version_entities WHERE version=?",
            (version,),
        ):
            entity_of[r["device_id"]] = r["entity_id"]
        return {"version": version, "entity_of": entity_of}
    finally:
        conn.close()


def parse_query_window(start_raw: str, end_raw: str) -> tuple[datetime, datetime]:
    """解析审计固定查询时段（左闭右开，必须带时区）。"""
    from .timeutil import parse_aware_iso

    lo = to_utc(parse_aware_iso(start_raw, "查询起点"))
    hi = to_utc(parse_aware_iso(end_raw, "查询终点"))
    if not hi > lo:
        raise CaliberNotFound(
            f"查询区间必须左闭右开且终点晚于起点: {start_raw} .. {end_raw}"
        )
    return lo, hi
