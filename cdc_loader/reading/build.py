"""统计构建引擎：在冻结口径上生成按日指标投影并原子发布。

口径 = (``cutoff_batch_id``, ``grouping_version``)
===============================================

* ``cutoff_batch_id``：读数只取**随批次号 <= 截止批次**收到的稿件。
  构建开始即把口径登记为 building 并提交，之后即使新批次到达也不混入。
* ``grouping_version``：设备归属只查归并层**该分组版本快照**的成员；
  设备不在那一版成员中就进入可查询的待归属清单，绝不套用今天的成员。

选版规则（在截止范围内）
========================

1. 对每条观测 (origin, reading_key) 取最大 edition 的稿件；
2. 该稿件 ``withdrawn=true`` -> 排除（计入 withdrawn_count，明细仍可查）；
3. 否则按选中稿件声称的 device 在分组版本中找实体：
   找到 -> 归属实体；找不到 -> 待归属（unassigned，不进格子但可查）；
4. 按 (实体, UTC 日期, 指标) 用 :class:`decimal.Decimal` 精确聚合
   条数、精确和、最小值、最大值（全程不使用 float）。

发布与恢复
==========

明细与格子在一条事务内写就，状态翻成 published 才对在线查询可见；
构建中途崩溃只留下 building 残档，上一份 published 日报始终可读，
重开后用同一对口径可安全重建。重复生成同一对已发布口径直接复用。
"""

from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

from . import grouping, protocol, store
from .errors import CaliberNotFound
from .fault import FAULT_AFTER_DETAIL_BEFORE_PUBLISH, FaultInjector

# 同步门轮询间隔与默认超时（秒）。
_GATE_POLL = 0.05
_DEFAULT_GATE_TIMEOUT = 60.0


# ---------------------------------------------------------------------------
# 进程间同步门
# ---------------------------------------------------------------------------

class SyncGate:
    """基于文件的跨进程同步门（演示“构建期间再收新稿件”）。

    构建进程到达门点后写出 ``waiting`` 并阻塞等待 ``release`` 文件；
    另一个进程在此期间投递新批次，再创建 ``release`` 放行。门只用于
    测试编排，生产路径不传 gate_dir 即完全空操作。
    """

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


def _acquire_build_lock(db_path: str):
    """跨进程构建互斥锁，避免同一读数仓并发构建互相踩踏。"""
    import fcntl

    lock_path = db_path + ".build.lock"
    fh = open(lock_path, "a+", encoding="utf-8")
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
    return fh


# ---------------------------------------------------------------------------
# 口径输入读取（截止批次内的选版）
# ---------------------------------------------------------------------------

def _latest_editions(conn, cutoff_batch_id: int) -> list[dict]:
    """在 <= 截止批次收到的稿件中，为每条观测选最大 edition 的稿件。"""
    rows = conn.execute(
        """
        SELECT e.origin, e.reading_key, e.edition, e.device, e.occurred_at,
               e.occurred_date, e.metric, e.amount, e.withdrawn,
               e.ingest_batch_id, e.content_hash
        FROM reading_editions e
        JOIN (
            SELECT origin, reading_key, MAX(edition) AS max_edition
            FROM reading_editions
            WHERE ingest_batch_id <= ?
            GROUP BY origin, reading_key
        ) t ON t.origin = e.origin AND t.reading_key = e.reading_key
           AND t.max_edition = e.edition
        WHERE e.ingest_batch_id <= ?
        ORDER BY e.origin, e.reading_key
        """,
        (cutoff_batch_id, cutoff_batch_id),
    ).fetchall()
    return [dict(r) for r in rows]


def _input_hash(
    cutoff_batch_id: int, grouping_version: int, chosen: list[dict]
) -> str:
    """口径输入的确定性摘要：口径坐标 + 每条选中稿件身份与内容哈希。"""
    payload = {
        "cutoff_batch_id": cutoff_batch_id,
        "grouping_version": grouping_version,
        "editions": [
            [r["origin"], r["reading_key"], r["edition"], r["content_hash"]]
            for r in chosen
        ],
    }
    return hashlib.sha256(
        protocol.canonical_json(payload).encode("utf-8")
    ).hexdigest()


# ---------------------------------------------------------------------------
# 构建主流程
# ---------------------------------------------------------------------------

def build_run(
    conn,
    ledger_db_path: str,
    cutoff_batch_id: int,
    grouping_version: int,
    gate: SyncGate | None = None,
    fault: FaultInjector | None = None,
) -> dict:
    """为一对固定口径生成（或复用）统计日报。"""
    lock_fh = _acquire_build_lock(_db_file_path(conn))
    try:
        return _build_locked(
            conn, ledger_db_path, cutoff_batch_id, grouping_version, gate, fault
        )
    finally:
        import fcntl

        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        lock_fh.close()


def _db_file_path(conn) -> str:
    row = conn.execute("PRAGMA database_list").fetchone()
    return row[2]


def _build_locked(
    conn,
    ledger_db_path: str,
    cutoff_batch_id: int,
    grouping_version: int,
    gate: SyncGate | None,
    fault: FaultInjector | None,
) -> dict:
    # --- 口径合法性：截止批次必须已确认；分组版本必须存在 ---
    last_batch = int(store.get_meta(conn, "last_batch_id", 0))
    if cutoff_batch_id < 1 or cutoff_batch_id > last_batch:
        raise CaliberNotFound(
            f"截止批次 {cutoff_batch_id} 不在已确认批次范围 1..{last_batch} 内"
        )
    ledger_conn = grouping.open_ledger_readonly(ledger_db_path)
    try:
        snap = grouping.load_grouping(ledger_conn, grouping_version)
    finally:
        ledger_conn.close()
    entity_of = snap["entity_of"]

    # --- 幂等：同一对已发布口径直接复用，不重算 ---
    existing = conn.execute(
        "SELECT * FROM stats_runs WHERE cutoff_batch_id=? AND grouping_version=?",
        (cutoff_batch_id, grouping_version),
    ).fetchone()
    if existing is not None and existing["status"] == "published":
        return {
            "status": "reused",
            "run_id": existing["run_id"],
            "cutoff_batch_id": cutoff_batch_id,
            "grouping_version": grouping_version,
            "cell_count": existing["cell_count"],
            "included_count": existing["included_count"],
            "unassigned_count": existing["unassigned_count"],
        }

    # --- 重建：清掉同口径的 building 残档（崩溃后安全重建）---
    if existing is not None:
        run_id = existing["run_id"]
        conn.execute("DELETE FROM stats_cells WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM stats_attribution WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM stats_observations WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM stats_runs WHERE run_id=?", (run_id,))
        conn.commit()

    # --- 阶段一：登记 building 口径并提交（截止坐标就此冻结）---
    created_at = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        "INSERT INTO stats_runs(cutoff_batch_id, grouping_version, status, "
        "created_at) VALUES(?, ?, 'building', ?)",
        (cutoff_batch_id, grouping_version, created_at),
    )
    run_id = int(cur.lastrowid)
    conn.commit()

    # --- 同步门：明细尚未计算。此期间另一个进程可投递新批次，
    #     但选版查询带 ingest_batch_id <= cutoff，截止范围保持一致。---
    if gate is not None:
        gate.wait_if_configured(
            f"run={run_id};cutoff={cutoff_batch_id};grouping={grouping_version}"
        )

    # --- 阶段二：在截止范围内选最新版（快照读，不再依赖门之后的状态）---
    chosen = _latest_editions(conn, cutoff_batch_id)
    input_hash = _input_hash(cutoff_batch_id, grouping_version, chosen)

    observations: list[dict] = []
    attributions: list[dict] = []
    # 格子累加器: (entity, date, metric) -> 聚合状态
    agg: dict[tuple, dict] = {}

    withdrawn_count = 0
    unassigned_count = 0

    for r in chosen:
        withdrawn = bool(r["withdrawn"])
        observations.append(
            {
                "run_id": run_id,
                "origin": r["origin"],
                "reading_key": r["reading_key"],
                "chosen_edition": r["edition"],
                "device": r["device"],
                "occurred_at": r["occurred_at"],
                "occurred_date": r["occurred_date"],
                "metric": r["metric"],
                "amount": r["amount"],
                "withdrawn": 1 if withdrawn else 0,
                "ingest_batch_id": r["ingest_batch_id"],
            }
        )

        if withdrawn:
            # 撤回：排除出格子，明细保留，归属表记录排除原因以便复查。
            withdrawn_count += 1
            attributions.append(
                {
                    "run_id": run_id,
                    "origin": r["origin"],
                    "reading_key": r["reading_key"],
                    "device": r["device"],
                    "assigned": 0,
                    "entity_id": None,
                    "basis": protocol.canonical_json(
                        {
                            "outcome": "withdrawn",
                            "grouping_version": grouping_version,
                            "chosen_edition": r["edition"],
                            "reason": "选中的最新稿件已撤回，不参与统计",
                        }
                    ),
                }
            )
            continue

        entity_id = entity_of.get(r["device"])
        if entity_id is None:
            # 设备不在该分组版本成员中：待归属，绝不静默丢弃，也不套用今天成员。
            unassigned_count += 1
            attributions.append(
                {
                    "run_id": run_id,
                    "origin": r["origin"],
                    "reading_key": r["reading_key"],
                    "device": r["device"],
                    "assigned": 0,
                    "entity_id": None,
                    "basis": protocol.canonical_json(
                        {
                            "outcome": "unassigned",
                            "grouping_version": grouping_version,
                            "chosen_edition": r["edition"],
                            "device": r["device"],
                            "reason": "设备不在该分组版本的成员快照中",
                        }
                    ),
                }
            )
            continue

        attributions.append(
            {
                "run_id": run_id,
                "origin": r["origin"],
                "reading_key": r["reading_key"],
                "device": r["device"],
                "assigned": 1,
                "entity_id": entity_id,
                "basis": protocol.canonical_json(
                    {
                        "outcome": "assigned",
                        "grouping_version": grouping_version,
                        "chosen_edition": r["edition"],
                        "device": r["device"],
                        "entity_id": entity_id,
                        "reason": "设备属于该分组版本快照中的实体成员",
                    }
                ),
            }
        )

        key = (entity_id, r["occurred_date"], r["metric"])
        amount = Decimal(r["amount"])
        state = agg.get(key)
        if state is None:
            agg[key] = {
                "row_count": 1,
                "amount_sum": amount,
                "amount_min": amount,
                "amount_max": amount,
            }
        else:
            state["row_count"] += 1
            state["amount_sum"] += amount
            if amount < state["amount_min"]:
                state["amount_min"] = amount
            if amount > state["amount_max"]:
                state["amount_max"] = amount

    cell_rows = [
        (
            run_id,
            entity_id,
            utc_date,
            metric,
            state["row_count"],
            str(state["amount_sum"]),
            str(state["amount_min"]),
            str(state["amount_max"]),
        )
        for (entity_id, utc_date, metric), state in sorted(agg.items())
    ]
    included_count = sum(state["row_count"] for state in agg.values())

    # --- 阶段三：明细/归属/格子在一条事务内写就，再原子发布 ---
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.executemany(
            "INSERT INTO stats_observations(run_id, origin, reading_key, "
            "chosen_edition, device, occurred_at, occurred_date, metric, amount, "
            "withdrawn, ingest_batch_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [tuple(o[k] for k in (
                "run_id", "origin", "reading_key", "chosen_edition", "device",
                "occurred_at", "occurred_date", "metric", "amount", "withdrawn",
                "ingest_batch_id")) for o in observations],
        )
        conn.executemany(
            "INSERT INTO stats_attribution(run_id, origin, reading_key, device, "
            "assigned, entity_id, basis) VALUES(?,?,?,?,?,?,?)",
            [
                (
                    a["run_id"], a["origin"], a["reading_key"], a["device"],
                    a["assigned"], a["entity_id"], a["basis"],
                )
                for a in attributions
            ],
        )
        conn.executemany(
            "INSERT INTO stats_cells(run_id, entity_id, utc_date, metric, "
            "row_count, amount_sum, amount_min, amount_max) "
            "VALUES(?,?,?,?,?,?,?,?)",
            cell_rows,
        )

        # 故障注入点：明细已写、尚未发布。os._exit 使本事务整体回滚，
        # building 残档保留（在阶段一已提交），在线日报仍指向上一份。
        if fault is not None:
            fault.hit(FAULT_AFTER_DETAIL_BEFORE_PUBLISH)

        conn.execute(
            "UPDATE stats_runs SET status='published', published_at=?, "
            "observation_count=?, included_count=?, withdrawn_count=?, "
            "unassigned_count=?, cell_count=?, input_hash=? WHERE run_id=?",
            (
                datetime.now(timezone.utc).isoformat(),
                len(chosen),
                included_count,
                withdrawn_count,
                unassigned_count,
                len(cell_rows),
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
        "cutoff_batch_id": cutoff_batch_id,
        "grouping_version": grouping_version,
        "observation_count": len(chosen),
        "included_count": included_count,
        "withdrawn_count": withdrawn_count,
        "unassigned_count": unassigned_count,
        "cell_count": len(cell_rows),
        "input_hash": input_hash,
    }
