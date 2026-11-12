"""只读复用归并层已保存的成员结果（不重算连通分量、不改写归并层）。

归并层把每个决策版本的实体成员持久保存在独立台账库的
``decision_versions`` / ``version_entities`` / ``version_devices`` 中。
本模块以只读 URI 打开该库，按**分组版本号**取当时的成员快照——
统计口径因此可以冻结在任意历史分组上，复查旧日报时不会套用今天的
成员。
"""

from __future__ import annotations

import os
import sqlite3

from .errors import CaliberNotFound


def open_ledger_readonly(ledger_db_path: str) -> sqlite3.Connection:
    if not os.path.exists(ledger_db_path):
        raise CaliberNotFound(f"归并层台账库不存在: {ledger_db_path}")
    try:
        conn = sqlite3.connect(f"file:{ledger_db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError as exc:
        raise CaliberNotFound(f"无法只读打开归并层台账库: {ledger_db_path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    return conn


def available_versions(ledger_conn: sqlite3.Connection) -> list[int]:
    try:
        rows = ledger_conn.execute(
            "SELECT version FROM decision_versions ORDER BY version"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise CaliberNotFound(
            f"归并层台账缺少 decision_versions 表（不是有效的归并层库）: {exc}"
        ) from exc
    return [int(r["version"]) for r in rows]


def load_grouping(
    ledger_conn: sqlite3.Connection, version: int
) -> dict:
    """取某分组版本的成员快照。

    返回::

        {
          "version": int,
          "devices": [设备...],            # 该版本参与计算的全部顶点
          "entity_of": {设备: 实体标识},
          "entities": {实体标识: [成员...]},
        }

    设备不在该版本成员中时，``entity_of`` 查不到 -> 进入待归属清单。
    """
    vrow = ledger_conn.execute(
        "SELECT version FROM decision_versions WHERE version=?", (version,)
    ).fetchone()
    if vrow is None:
        raise CaliberNotFound(
            f"归并层分组版本 {version} 不存在；可用版本: "
            f"{available_versions(ledger_conn)}"
        )

    devices = [
        r["device_id"]
        for r in ledger_conn.execute(
            "SELECT device_id FROM version_devices WHERE version=? ORDER BY device_id",
            (version,),
        )
    ]
    entities: dict[str, list[str]] = {}
    entity_of: dict[str, str] = {}
    for r in ledger_conn.execute(
        "SELECT entity_id, device_id FROM version_entities "
        "WHERE version=? ORDER BY entity_id, device_id",
        (version,),
    ):
        entities.setdefault(r["entity_id"], []).append(r["device_id"])
        entity_of[r["device_id"]] = r["entity_id"]

    return {
        "version": version,
        "devices": devices,
        "entity_of": entity_of,
        "entities": entities,
    }
