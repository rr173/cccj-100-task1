"""独立全量重算器：只依赖标准库，**不 import 本包的任何其它模块**。

按 README 中公开的规则独立重算，对台账中**每一个被接受的版本**：
  1. 以该版本的 ``ledger_seq`` 为水位重放修订台账，取每关系最大修订号；
  2. 以该版本记录的活跃设备为顶点重算连通分量与实体标识；
  3. 比较**完整分组**（实体标识 + 全部成员集合，而非实体数量）；
  4. 重算“必须不同”约束结论——被接受的版本必须零冲突；
  5. 比对分组哈希与设备集哈希。

另做头部检查：以台账当前水位 + 当前 presence 重算，结果必须与当前
版本完全一致（证明没有漏建版本，重读相同资料也不会多出历史）。

任何不一致逐条列出并以退出码 6 结束。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

ENTITY_PREFIX = "E-"


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


class _UF:
    def __init__(self, items: list[str]) -> None:
        self.parent = {x: x for x in items}

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _active_relations(conn: sqlite3.Connection, ledger_seq: int) -> list[dict]:
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


def _recompute(devices: list[str], relations: list[dict]) -> dict:
    """独立重算：返回实体映射、分组哈希与冲突约束清单。"""
    vset = set(devices)
    uf = _UF(sorted(vset))
    for rel in relations:
        if (
            rel["category"] == "same_entity"
            and rel["endpoint_a"] in vset
            and rel["endpoint_b"] in vset
        ):
            uf.union(rel["endpoint_a"], rel["endpoint_b"])

    comp: dict[str, list[str]] = {}
    for d in sorted(vset):
        comp.setdefault(uf.find(d), []).append(d)
    entities: dict[str, list[str]] = {}
    for members in comp.values():
        members.sort()
        entities[ENTITY_PREFIX + members[0]] = members

    ordered = [[eid, entities[eid]] for eid in sorted(entities)]
    conflicts = []
    for rel in relations:
        if rel["category"] != "must_differ":
            continue
        a, b = rel["endpoint_a"], rel["endpoint_b"]
        if a in vset and b in vset and uf.find(a) == uf.find(b):
            conflicts.append(
                {
                    "source": rel["source"],
                    "relation_id": rel["relation_id"],
                    "revision": rel["revision"],
                    "endpoints": sorted([a, b]),
                }
            )
    conflicts.sort(key=lambda c: (c["endpoints"], c["source"], c["relation_id"]))
    return {
        "entities": entities,
        "grouping_hash": _hash(ordered),
        "conflicts": conflicts,
    }


def run_verify(ledger_db_path: str) -> dict:
    """全量重算并对照，返回结构化报告（ok=True 表示完全一致）。"""
    conn = sqlite3.connect(ledger_db_path)
    conn.row_factory = sqlite3.Row
    try:
        problems: list[str] = []
        version_reports: list[dict] = []

        versions = conn.execute(
            "SELECT * FROM decision_versions ORDER BY version"
        ).fetchall()

        for vrow in versions:
            version = int(vrow["version"])
            v_problems: list[str] = []

            devices = sorted(
                r["device_id"]
                for r in conn.execute(
                    "SELECT device_id FROM version_devices WHERE version=?",
                    (version,),
                )
            )
            stored: dict[str, list[str]] = {}
            for r in conn.execute(
                "SELECT entity_id, device_id FROM version_entities "
                "WHERE version=? ORDER BY entity_id, device_id",
                (version,),
            ):
                stored.setdefault(r["entity_id"], []).append(r["device_id"])

            recomputed = _recompute(devices, _active_relations(conn, int(vrow["ledger_seq"])))

            # 1) 完整分组比对（实体标识 + 全部成员，而非数量）。
            if recomputed["entities"] != stored:
                only_stored = sorted(set(stored) - set(recomputed["entities"]))
                only_recomp = sorted(set(recomputed["entities"]) - set(stored))
                v_problems.append(
                    f"版本 {version} 分组不一致: 仅台账={only_stored} "
                    f"仅重算={only_recomp}"
                )
                for eid in sorted(set(stored) & set(recomputed["entities"])):
                    if stored[eid] != recomputed["entities"][eid]:
                        v_problems.append(
                            f"版本 {version} 实体 {eid} 成员不一致: "
                            f"台账={stored[eid]} 重算={recomputed['entities'][eid]}"
                        )

            # 2) 冲突结论比对：被接受的版本必须零冲突。
            if recomputed["conflicts"]:
                v_problems.append(
                    f"版本 {version} 重算发现 {len(recomputed['conflicts'])} 条"
                    f"冲突约束（被接受版本必须零冲突）: {recomputed['conflicts']}"
                )

            # 3) 哈希比对（公开规则）。
            if recomputed["grouping_hash"] != vrow["grouping_hash"]:
                v_problems.append(
                    f"版本 {version} 分组哈希不一致: 台账={vrow['grouping_hash']} "
                    f"重算={recomputed['grouping_hash']}"
                )
            if _hash(devices) != vrow["presence_hash"]:
                v_problems.append(f"版本 {version} 活跃设备哈希不一致")

            problems.extend(v_problems)
            version_reports.append(
                {
                    "version": version,
                    "ok": not v_problems,
                    "entity_count": len(recomputed["entities"]),
                    "conflict_count": len(recomputed["conflicts"]),
                    "problems": v_problems,
                }
            )

        # 4) 头部检查：台账当前水位 + 当前 presence 必须等于当前版本。
        meta = {
            r["key"]: json.loads(r["value"])
            for r in conn.execute("SELECT key, value FROM meta")
        }
        head_seq = int(meta.get("last_batch_id", 0))
        current = meta.get("current_version")
        presence = sorted(
            r["device_id"] for r in conn.execute("SELECT device_id FROM presence")
        )
        head = _recompute(presence, _active_relations(conn, head_seq))
        head_ok = True
        if current is None:
            if versions:
                problems.append("meta 缺少 current_version 但存在版本记录")
                head_ok = False
        else:
            cur_row = conn.execute(
                "SELECT grouping_hash FROM decision_versions WHERE version=?",
                (int(current),),
            ).fetchone()
            if cur_row is None:
                problems.append(f"current_version={current} 没有对应版本记录")
                head_ok = False
            elif cur_row["grouping_hash"] != head["grouping_hash"]:
                problems.append(
                    "头部重算分组与当前版本不一致（存在未落版本的分组变化）"
                )
                head_ok = False
        if head["conflicts"]:
            problems.append(f"头部状态存在冲突约束: {head['conflicts']}")
            head_ok = False

        return {
            "ok": not problems,
            "versions_checked": len(versions),
            "versions": version_reports,
            "head_check_ok": head_ok,
            "problems": problems,
        }
    finally:
        conn.close()
