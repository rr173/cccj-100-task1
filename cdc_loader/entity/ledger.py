"""台账编排：批次摄入、设备在场刷新、版本落库与查询。

核心不变量
==========

* **批次原子**：一次投递的全部修订、批次登记、决策版本与 meta 进度在
  同一条 SQLite 事务内落定；任何矛盾（格式、篡改、约束冲突）或崩溃
  都使整批回滚，台账维持投递前状态。
* **统一判定**：一批中撤销旧连接与加入新连接不逐条演进，而是先落到
  最终候选图再一次性判定，因此同一批的拆分与合并只产生一个新版本。
* **幂等**：重投内容一致的旧修订不改变决策；分组未变时不产生新版本。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from typing import Any

from . import engine, protocol, store
from .errors import (
    ConstraintConflict,
    RelationProtocolError,
    RevisionConflict,
    UpstreamNotReady,
    VersionNotFound,
)
from .fault import FAULT_AFTER_LEDGER_BEFORE_DECISION, FaultInjector


# ---------------------------------------------------------------------------
# 读取台账
# ---------------------------------------------------------------------------

def load_active_relations(conn: sqlite3.Connection, ledger_seq: int | None = None) -> list[dict]:
    """取每个 (source, relation_id) 在指定台账水位下的当前生效关系。

    当前状态 = 不超过 ``ledger_seq``（批次号）的最大修订号；effective=0
    表示已撤销，不返回。``ledger_seq`` 缺省为台账头部（last_batch_id）。
    """
    if ledger_seq is None:
        ledger_seq = int(store.get_meta(conn, "last_batch_id", 0))
    rows = conn.execute(
        """
        SELECT r.source, r.relation_id, r.revision, r.endpoint_a, r.endpoint_b,
               r.category
        FROM revisions r
        JOIN (
            SELECT source, relation_id, MAX(revision) AS max_rev
            FROM revisions WHERE batch_id <= ?
            GROUP BY source, relation_id
        ) t ON t.source = r.source AND t.relation_id = r.relation_id
           AND t.max_rev = r.revision
        WHERE r.effective = 1
        ORDER BY r.source, r.relation_id
        """,
        (ledger_seq,),
    ).fetchall()
    return [dict(r) for r in rows]


def presence_devices(conn: sqlite3.Connection) -> list[str]:
    return [
        r["device_id"]
        for r in conn.execute("SELECT device_id FROM presence ORDER BY device_id")
    ]


def current_version(conn: sqlite3.Connection) -> int | None:
    value = store.get_meta(conn, "current_version")
    return int(value) if value is not None else None


def version_entities(conn: sqlite3.Connection, version: int) -> dict[str, list[str]]:
    """某版本的实体 -> 成员映射（entity_id -> 排序后的成员列表）。"""
    rows = conn.execute(
        "SELECT entity_id, device_id FROM version_entities WHERE version=? "
        "ORDER BY entity_id, device_id",
        (version,),
    ).fetchall()
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["entity_id"], []).append(r["device_id"])
    return out


def version_devices(conn: sqlite3.Connection, version: int) -> list[str]:
    return [
        r["device_id"]
        for r in conn.execute(
            "SELECT device_id FROM version_devices WHERE version=? ORDER BY device_id",
            (version,),
        )
    ]


def get_version_row(conn: sqlite3.Connection, version: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM decision_versions WHERE version=?", (version,)
    ).fetchone()
    if row is None:
        raise VersionNotFound(f"决策版本不存在: {version}")
    return row


# ---------------------------------------------------------------------------
# 版本落库
# ---------------------------------------------------------------------------

def _presence_hash(devices: list[str]) -> str:
    return hashlib.sha256(
        protocol.canonical_json(sorted(devices)).encode("utf-8")
    ).hexdigest()


def _maybe_create_version(
    conn: sqlite3.Connection,
    cause: str,
    batch_id: int | None,
    decision: dict,
    devices: list[str],
) -> int | None:
    """分组与当前版本不同（或尚无任何版本）时落库新版本，返回版本号。"""
    current = current_version(conn)
    if current is not None:
        row = conn.execute(
            "SELECT grouping_hash FROM decision_versions WHERE version=?",
            (current,),
        ).fetchone()
        if row is not None and row["grouping_hash"] == decision["grouping_hash"]:
            return None  # 成员集合未变：不产生新版本（幂等）

    version = (current or 0) + 1
    ledger_seq = int(store.get_meta(conn, "last_batch_id", 0))
    entities = decision["entities"]
    conn.execute(
        "INSERT INTO decision_versions(version, cause, batch_id, ledger_seq, "
        "entity_count, device_count, grouping_hash, presence_hash) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        (
            version,
            cause,
            batch_id,
            ledger_seq,
            len(entities),
            len(devices),
            decision["grouping_hash"],
            _presence_hash(devices),
        ),
    )
    conn.executemany(
        "INSERT INTO version_entities(version, entity_id, device_id) VALUES(?, ?, ?)",
        [
            (version, e["entity_id"], member)
            for e in entities
            for member in e["members"]
        ],
    )
    conn.executemany(
        "INSERT INTO version_devices(version, device_id) VALUES(?, ?)",
        [(version, d) for d in sorted(devices)],
    )
    store.set_meta(conn, "current_version", version)
    return version


# ---------------------------------------------------------------------------
# 批次摄入
# ---------------------------------------------------------------------------

def _read_batch_rows(batch_files: list[str]) -> list[dict]:
    """读取并逐行校验全部批次文件，返回规范化行（保持投递顺序）。"""
    rows: list[dict] = []
    for path in batch_files:
        for lineno, raw in protocol.iter_jsonl(path):
            rows.append(protocol.validate_relation_row(raw, f"{path}:{lineno}"))
    if not rows:
        raise RelationProtocolError("批次为空：没有任何对照资料行")
    return rows


def _dedupe_batch(rows: list[dict]) -> list[dict]:
    """批内去重：同一身份内容一致去重，内容不同即整批拒绝。"""
    seen: dict[tuple, dict] = {}
    for row in rows:
        key = protocol.identity_of(row)
        if key in seen:
            if protocol.content_of(seen[key]) != protocol.content_of(row):
                raise RevisionConflict(
                    f"批内同一修订身份 {key} 出现两种内容: "
                    f"{protocol.canonical_json(protocol.content_of(seen[key]))} 与 "
                    f"{protocol.canonical_json(protocol.content_of(row))}"
                )
            continue
        seen[key] = row
    return list(seen.values())


def _check_against_ledger(
    conn: sqlite3.Connection, rows: list[dict]
) -> tuple[list[dict], int]:
    """逐行对照台账：旧修订内容必须逐字一致；新修订号必须连续接续。

    返回 (待插入新修订, 重传行数)。对批内**每一行**做内容比对——
    不只看各关系的最新版本，被取代的旧修订被改写同样拒绝。
    """
    new_rows: list[dict] = []
    replayed = 0
    for row in rows:
        key = protocol.identity_of(row)
        existing = conn.execute(
            "SELECT endpoint_a, endpoint_b, category, effective FROM revisions "
            "WHERE source=? AND relation_id=? AND revision=?",
            key,
        ).fetchone()
        if existing is not None:
            stored = {
                "endpoint_a": existing["endpoint_a"],
                "endpoint_b": existing["endpoint_b"],
                "category": existing["category"],
                "effective": bool(existing["effective"]),
            }
            if stored != protocol.content_of(row):
                raise RevisionConflict(
                    f"修订 {key} 的内容与台账已登记版本不一致（疑似改写旧版本）: "
                    f"台账={protocol.canonical_json(stored)} "
                    f"投递={protocol.canonical_json(protocol.content_of(row))}"
                )
            replayed += 1
        else:
            new_rows.append(row)

    # 修订链连续性：每个 (source, relation_id) 的新修订必须恰好从
    # 台账当前最大号 +1 开始连续递增，不允许跳号。
    by_relation: dict[tuple[str, str], list[int]] = {}
    for row in new_rows:
        by_relation.setdefault((row["source"], row["relation_id"]), []).append(
            row["revision"]
        )
    for (source, relation_id), revs in sorted(by_relation.items()):
        current_max = conn.execute(
            "SELECT MAX(revision) AS m FROM revisions WHERE source=? AND relation_id=?",
            (source, relation_id),
        ).fetchone()["m"]
        base = int(current_max) if current_max is not None else 0
        expected = list(range(base + 1, base + 1 + len(revs)))
        if sorted(revs) != expected:
            raise RelationProtocolError(
                f"关系 ({source!r}, {relation_id!r}) 的新修订号不连续: "
                f"台账已到 {base}，批内新修订为 {sorted(revs)}，应为 {expected}"
            )
    return new_rows, replayed


def ingest_batch(
    conn: sqlite3.Connection,
    batch_files: list[str],
    fault: FaultInjector | None = None,
) -> dict:
    """摄入一次投递（一个不可分割的决策批次）。

    单条 SQLite 事务内完成：修订对照与写入 -> 批次登记 -> 故障注入点
    -> 最终候选图统一判定 -> 版本落库 -> meta 进度。任何一步失败
    （含约束冲突）整体回滚，台账维持投递前状态。
    """
    if not store.get_meta(conn, "presence_initialized", False):
        raise UpstreamNotReady(
            "设备在场状态尚未初始化：请先运行 refresh 从 device_master 同步"
        )

    rows = _dedupe_batch(_read_batch_rows(batch_files))

    conn.execute("BEGIN IMMEDIATE")
    try:
        new_rows, replayed = _check_against_ledger(conn, rows)

        batch_id = int(store.get_meta(conn, "last_batch_id", 0)) + 1
        batch_hash = hashlib.sha256(
            "\n".join(
                sorted(protocol.canonical_json(r) for r in rows)
            ).encode("utf-8")
        ).hexdigest()
        conn.execute(
            "INSERT INTO batches(batch_id, row_count, new_revisions, content_hash) "
            "VALUES(?, ?, ?, ?)",
            (batch_id, len(rows), len(new_rows), batch_hash),
        )
        conn.executemany(
            "INSERT INTO revisions(source, relation_id, revision, endpoint_a, "
            "endpoint_b, category, effective, batch_id, content_hash) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["source"],
                    r["relation_id"],
                    r["revision"],
                    r["endpoint_a"],
                    r["endpoint_b"],
                    r["category"],
                    1 if r["effective"] else 0,
                    batch_id,
                    protocol.content_hash(r),
                )
                for r in new_rows
            ],
        )
        store.set_meta(conn, "last_batch_id", batch_id)

        # 故障注入点：关系台账已写入（本事务内）、决策尚未落定。
        # os._exit(42) 使未提交事务整体回滚，重开后无半批状态。
        if fault is not None:
            fault.hit(FAULT_AFTER_LEDGER_BEFORE_DECISION)

        # 对最终候选图统一判定（同一批的撤销与新增不区分先后）。
        devices = presence_devices(conn)
        decision = engine.compute_decision(
            devices, load_active_relations(conn, batch_id)
        )
        if decision["conflicts"]:
            raise ConstraintConflict(
                f"批次 {batch_id} 存在 {len(decision['conflicts'])} 条冲突约束，"
                "整批拒绝，台账维持投递前状态",
                report={
                    "status": "rejected",
                    "reason": "constraint_conflict",
                    "conflicts": decision["conflicts"],
                },
            )

        version = _maybe_create_version(conn, "ingest", batch_id, decision, devices)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "status": "accepted",
        "batch_id": batch_id,
        "rows": len(rows),
        "new_revisions": len(new_rows),
        "replayed_revisions": replayed,
        "version_created": version,
        "current_version": current_version(conn),
        "entity_count": len(decision["entities"]),
        "device_count": len(devices),
        "parked_relations": len(decision["parked"]),
    }


# ---------------------------------------------------------------------------
# 设备在场刷新
# ---------------------------------------------------------------------------

def _read_master_devices(loader_db_path: str) -> list[str]:
    """直接读取装载器状态库的 device_master（只读，不修改上游任何东西）。"""
    if not os.path.exists(loader_db_path):
        raise UpstreamNotReady(f"装载器状态库不存在: {loader_db_path}")
    try:
        upstream = sqlite3.connect(f"file:{loader_db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError as exc:
        raise UpstreamNotReady(f"无法打开装载器状态库: {loader_db_path}: {exc}") from exc
    try:
        try:
            row = upstream.execute(
                "SELECT value FROM meta WHERE key='snapshot_published'"
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise UpstreamNotReady(
                f"装载器状态库缺少 meta 表（不是有效的装载器库）: {exc}"
            ) from exc
        published = bool(json.loads(row[0])) if row is not None else False
        if not published:
            raise UpstreamNotReady("装载器快照尚未发布，device_master 不可信，拒绝同步")
        return sorted(
            r[0] for r in upstream.execute("SELECT device_id FROM device_master")
        )
    finally:
        upstream.close()


def refresh_presence(conn: sqlite3.Connection, loader_db_path: str) -> dict:
    """从 device_master 同步在场设备并重算决策。

    上游删除的设备退出活动成员，其关联关系暂扣（数据保留，不参与计算）；
    设备重新出现后自动重新参与约束判定。分组未变时不产生新版本。
    """
    devices = _read_master_devices(loader_db_path)

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM presence")
        conn.executemany(
            "INSERT INTO presence(device_id) VALUES(?)", [(d,) for d in devices]
        )
        store.set_meta(conn, "presence_initialized", True)

        decision = engine.compute_decision(devices, load_active_relations(conn))
        if decision["conflicts"]:
            # 设备重现可能让既有约束重新生效并冲突：不落版本，明确报错。
            raise ConstraintConflict(
                "刷新后在场设备使“必须不同”约束与连通关系矛盾，未产生新版本",
                report={
                    "status": "rejected",
                    "reason": "constraint_conflict_on_refresh",
                    "conflicts": decision["conflicts"],
                },
            )
        version = _maybe_create_version(conn, "refresh", None, decision, devices)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "status": "refreshed",
        "device_count": len(devices),
        "version_created": version,
        "current_version": current_version(conn),
        "entity_count": len(decision["entities"]),
        "parked_relations": len(decision["parked"]),
    }


# ---------------------------------------------------------------------------
# 查询：解释 / 历史 / 变动
# ---------------------------------------------------------------------------

def explain_device(conn: sqlite3.Connection, device: str, version: int | None) -> dict:
    """从某设备解释其在指定（缺省当前）版本的实体成员及依据。

    成员取自版本快照；依据（关系路径与约束）按该版本的台账水位重算，
    唯当前版本使用台账头部水位——因为后续批次既然未产生新版本，
    其修订就不改变成员，但可能改变依据（如撤销冗余关系）。
    """
    current = current_version(conn)
    if version is None:
        if current is None:
            raise VersionNotFound("尚无任何决策版本，请先 refresh / ingest")
        version = current
    vrow = get_version_row(conn, version)
    devices = version_devices(conn, version)
    ledger_seq = (
        int(store.get_meta(conn, "last_batch_id", 0))
        if version == current
        else int(vrow["ledger_seq"])
    )
    relations = load_active_relations(conn, ledger_seq)
    decision = engine.compute_decision(devices, relations)

    result: dict[str, Any] = {
        "device": device,
        "version": version,
        "ledger_seq": ledger_seq,
        "present": device in set(devices),
    }
    if device not in set(devices):
        result["note"] = "该设备在此版本不在场（上游不存在），不参与计算"
        result["parked_relations"] = [
            _rel_desc(r) for r in decision["parked"]
            if device in (r["endpoint_a"], r["endpoint_b"])
        ]
        return result

    entity = next(e for e in decision["entities"] if device in e["members"])
    members = entity["members"]
    member_set = set(members)
    inner_edges = [
        r for r in decision["edges"]
        if r["endpoint_a"] in member_set and r["endpoint_b"] in member_set
    ]
    adj = engine.build_adjacency(inner_edges)
    paths = {
        other: engine.path_between(adj, device, other)
        for other in members
        if other != device
    }
    result.update(
        {
            "entity_id": entity["entity_id"],
            "members": members,
            "basis_edges": [_rel_desc(r) for r in inner_edges],
            "paths_from_device": paths,
            "constraints_on_entity": [
                _rel_desc(r) for r in decision["constraints"]
                if r["endpoint_a"] in member_set or r["endpoint_b"] in member_set
            ],
            "parked_relations": [
                _rel_desc(r) for r in decision["parked"]
                if device in (r["endpoint_a"], r["endpoint_b"])
            ],
        }
    )
    return result


def _rel_desc(rel: dict) -> dict:
    lo, hi = sorted([rel["endpoint_a"], rel["endpoint_b"]])
    return {
        "source": rel["source"],
        "relation_id": rel["relation_id"],
        "revision": rel["revision"],
        "category": rel["category"],
        "endpoints": [lo, hi],
    }


def list_versions(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT version, cause, batch_id, ledger_seq, entity_count, "
            "device_count, grouping_hash FROM decision_versions ORDER BY version"
        )
    ]


def changes_into(conn: sqlite3.Connection, version: int) -> dict:
    """相邻版本变动：从 version-1 到 version。"""
    get_version_row(conn, version)
    if version <= 1:
        raise VersionNotFound("版本 1 没有前驱版本，无法计算相邻变动")
    get_version_row(conn, version - 1)
    old = version_entities(conn, version - 1)
    new = version_entities(conn, version)
    out = engine.classify_changes(old, new)
    out["from_version"] = version - 1
    out["to_version"] = version
    return out
