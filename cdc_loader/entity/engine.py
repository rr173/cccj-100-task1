"""实体决策引擎：纯函数，不触碰 SQLite，便于独立重算器对照。

公开且确定的规则（README 中有同样文字，修改必须同步）：

1. **顶点**：当前在场的全部设备（presence 表内容）。
2. **边**：当前生效（取每关系最大修订号且 effective=true）且两端都在场
   的 ``same_entity`` 关系；任一端不在场的关系**暂扣**（parked），不参与
   计算，设备重现后自动重新参与。
3. **实体**：same_entity 边构成的无向连通分量；连通关系可传递
   （A-B、B-C => A 与 C 同属一个实体）。
4. **实体标识**：``E-`` + 分量内字典序最小的设备编号。成员集合不变则
   标识不变；两个不同分量的最小编号必然不同，标识不会碰撞。
5. **约束**：生效且两端在场的 ``must_differ`` 关系；两端落在同一分量
   即冲突，整批拒绝。
6. **冲突路径**：从约束两端中字典序较小端出发做广度优先搜索；每一层
   邻居按设备编号字典序展开；同一对设备间的多条平行关系按
   ``(source, relation_id, revision)`` 字典序取第一条。多次运行结果
   可复核。
7. **分组哈希**：``sha256(canonical_json([[entity_id, 成员...], ...]))``，
   列表按 entity_id 升序，成员按字典序升序；canonical_json 为
   ``json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)``。
"""

from __future__ import annotations

import hashlib
from collections import deque
from typing import Any

from .protocol import CATEGORY_SAME, canonical_json

ENTITY_PREFIX = "E-"


def entity_id_of(members: list[str]) -> str:
    """实体标识规则：E- + 成员中最小设备编号（members 必须已排序）。"""
    return ENTITY_PREFIX + members[0]


def _edge_key(rel: dict) -> tuple:
    return (rel["source"], rel["relation_id"], rel["revision"])


def _constraint_desc(rel: dict) -> dict:
    lo, hi = sorted([rel["endpoint_a"], rel["endpoint_b"]])
    return {
        "source": rel["source"],
        "relation_id": rel["relation_id"],
        "revision": rel["revision"],
        "endpoints": [lo, hi],
    }


def grouping_hash_of(entities: list[dict]) -> str:
    """完整分组的确定性哈希（公开规则，见模块 docstring 第 7 条）。"""
    payload = [[e["entity_id"], e["members"]] for e in entities]
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


class _UnionFind:
    def __init__(self, items: list[str]) -> None:
        self.parent = {x: x for x in items}

    def find(self, x: str) -> str:
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # 路径压缩
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # 确定性：字典序小者作为根（不影响分量结果，只影响调试可读性）。
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra


def build_adjacency(edges: list[dict]) -> dict[str, list[tuple[str, dict]]]:
    """构造确定性邻接表：邻居升序；平行边取 (source, relation_id, revision) 最小者。"""
    best: dict[str, dict[str, tuple[tuple, dict]]] = {}
    for rel in edges:
        desc = {
            "source": rel["source"],
            "relation_id": rel["relation_id"],
            "revision": rel["revision"],
        }
        for x, y in ((rel["endpoint_a"], rel["endpoint_b"]),
                     (rel["endpoint_b"], rel["endpoint_a"])):
            slot = best.setdefault(x, {})
            key = _edge_key(rel)
            if y not in slot or key < slot[y][0]:
                slot[y] = (key, desc)
    return {
        x: sorted(((y, desc) for y, (_, desc) in slot.items()), key=lambda t: t[0])
        for x, slot in best.items()
    }


def path_between(adj: dict[str, list[tuple[str, dict]]], start: str, goal: str) -> list[dict]:
    """按公开规则求 start 到 goal 的确定性路径，返回关系步骤列表。"""
    parent: dict[str, tuple[str, dict] | None] = {start: None}
    queue: deque[str] = deque([start])
    while queue:
        x = queue.popleft()
        if x == goal:
            break
        for neighbor, desc in adj.get(x, []):
            if neighbor not in parent:
                parent[neighbor] = (x, desc)
                queue.append(neighbor)
    if goal not in parent:
        return []
    steps: list[dict] = []
    cur = goal
    while cur != start:
        prev, desc = parent[cur]  # type: ignore[misc]
        steps.append(
            {
                "from": prev,
                "to": cur,
                "source": desc["source"],
                "relation_id": desc["relation_id"],
                "revision": desc["revision"],
            }
        )
        cur = prev
    steps.reverse()
    return steps


def compute_decision(devices: list[str], relations: list[dict]) -> dict[str, Any]:
    """由在场设备与生效关系计算候选决策。

    ``relations`` 为当前生效（最大修订号且 effective）的关系行。
    返回实体分组、参与计算的边/约束、暂扣关系与冲突列表；调用方
    在 ``conflicts`` 非空时必须拒绝整批。
    """
    vertices = sorted(set(devices))
    vset = set(vertices)

    edges: list[dict] = []
    constraints: list[dict] = []
    parked: list[dict] = []
    for rel in relations:
        a, b = rel["endpoint_a"], rel["endpoint_b"]
        if a in vset and b in vset:
            (edges if rel["category"] == CATEGORY_SAME else constraints).append(rel)
        else:
            parked.append(rel)

    uf = _UnionFind(vertices)
    for rel in sorted(edges, key=_edge_key):
        uf.union(rel["endpoint_a"], rel["endpoint_b"])

    comp: dict[str, list[str]] = {}
    for d in vertices:
        comp.setdefault(uf.find(d), []).append(d)
    entities = [
        {"entity_id": entity_id_of(members), "members": members}
        for members in (sorted(m) for m in comp.values())
    ]
    entities.sort(key=lambda e: e["entity_id"])

    adj = build_adjacency(edges)
    conflicts: list[dict] = []
    for rel in sorted(
        constraints,
        key=lambda r: (
            min(r["endpoint_a"], r["endpoint_b"]),
            max(r["endpoint_a"], r["endpoint_b"]),
            r["source"],
            r["relation_id"],
        ),
    ):
        u, v = rel["endpoint_a"], rel["endpoint_b"]
        if uf.find(u) == uf.find(v):
            lo, hi = sorted([u, v])
            conflicts.append(
                {
                    "constraint": _constraint_desc(rel),
                    "path": path_between(adj, lo, hi),
                }
            )

    return {
        "entities": entities,
        "grouping_hash": grouping_hash_of(entities),
        "edges": sorted(edges, key=_edge_key),
        "constraints": sorted(constraints, key=_edge_key),
        "parked": sorted(parked, key=_edge_key),
        "conflicts": conflicts,
    }


def classify_changes(old_entities: dict[str, list[str]],
                     new_entities: dict[str, list[str]]) -> dict[str, Any]:
    """相邻版本的变动分类（确定性，全部输出按标识排序）。

    分类规则（按实体成员重叠关系）：

    * ``merge``      多个旧实体并入同一个新实体（新实体视角）
    * ``split``      一个旧实体拆成多个新实体（旧实体视角）
    * ``appeared``   新实体与任何旧实体无成员重叠
    * ``disappeared`` 旧实体与任何新实体无成员重叠
    * ``changed``    一对一对应但成员有增减（设备加入/退出）
    * 另给出设备级的 ``devices_appeared`` / ``devices_disappeared``
    """
    old_map = {eid: set(m) for eid, m in old_entities.items()}
    new_map = {eid: set(m) for eid, m in new_entities.items()}
    old_devs: set[str] = set().union(*old_map.values()) if old_map else set()
    new_devs: set[str] = set().union(*new_map.values()) if new_map else set()

    merges: list[dict] = []
    splits: list[dict] = []
    appeared: list[dict] = []
    disappeared: list[dict] = []
    changed: list[dict] = []
    unchanged = 0

    for nid in sorted(new_map):
        nmem = new_map[nid]
        src = sorted(oid for oid, omem in old_map.items() if omem & nmem)
        if not src:
            appeared.append({"entity": nid, "devices": sorted(nmem)})
        elif len(src) == 1:
            omem = old_map[src[0]]
            if omem == nmem:
                unchanged += 1
            else:
                changed.append(
                    {
                        "old_entity": src[0],
                        "new_entity": nid,
                        "added_devices": sorted(nmem - omem),
                        "removed_devices": sorted(omem - nmem),
                    }
                )
        else:
            union_old: set[str] = set().union(*(old_map[s] for s in src))
            merges.append(
                {
                    "entity": nid,
                    "from_entities": src,
                    "new_devices": sorted(nmem - union_old),
                }
            )

    for oid in sorted(old_map):
        omem = old_map[oid]
        dst = sorted(nid for nid, nmem in new_map.items() if nmem & omem)
        if not dst:
            disappeared.append({"entity": oid, "devices": sorted(omem)})
        elif len(dst) > 1:
            union_new: set[str] = set().union(*(new_map[d] for d in dst))
            splits.append(
                {
                    "entity": oid,
                    "into_entities": dst,
                    "lost_devices": sorted(omem - union_new),
                }
            )

    return {
        "merges": merges,
        "splits": splits,
        "appeared": appeared,
        "disappeared": disappeared,
        "changed": changed,
        "unchanged_entities": unchanged,
        "devices_appeared": sorted(new_devs - old_devs),
        "devices_disappeared": sorted(old_devs - new_devs),
    }
