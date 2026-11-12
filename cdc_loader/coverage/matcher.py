"""读数-应到项配对引擎。

配对规则（全部可复核，每条判定依据都落库）
==========================================

1. 读数先在**固定成员版本**中按 device 找实体；找不到实体的读数不参与
   配对，进未匹配清单（原因 ``device_not_in_membership_version``），
   绝不套用今天的成员改写历史归属。
2. 读数按 ``(origin, entity_id, metric)`` 归到同键的契约应到项；同实体
   的多个设备编号因此都能满足同一份计划（别名归并）。没有对应契约的
   读数进未匹配清单（原因 ``no_contract_for_origin_entity_metric``）。
3. 每条读数在容差 ``tolerance_seconds`` 内（含边界，``|Δ| <= tol``）
   选时间距离**最近**的应到项；距离相同选**更早**的应到项；超出容差
   不能补洞（原因 ``outside_tolerance``，保留最近候选与距离依据）。
4. 每条读数最多匹配一个应到项；同一个应到项可被多条读数命中——只算
   一次覆盖，但保留全部命中记录与重复上报诊断
   （``duplicate_hit=1``）。被豁免的应到项不接受覆盖（豁免位置本来就
   不要求采样），读数仍按规则找最近的非豁免项。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .scheduler import ExpectedItem


@dataclass
class Reading:
    origin: str
    reading_key: str
    chosen_edition: int
    device: str
    occurred_at_utc: datetime
    metric: str
    ingest_batch_id: int
    entity_id: str | None = None


@dataclass
class MatchDecision:
    reading: Reading
    item: ExpectedItem | None
    distance_seconds: int | None
    reason: str                       # matched / 各类未匹配原因
    candidates: list[dict] = field(default_factory=list)


def parse_reading_rows(rows: list[dict], entity_of: dict[str, str]) -> list[Reading]:
    """把读数仓选版结果转成 Reading，并按固定成员版本标注实体。"""
    out = []
    for r in rows:
        out.append(
            Reading(
                origin=r["origin"],
                reading_key=r["reading_key"],
                chosen_edition=r["edition"],
                device=r["device"],
                occurred_at_utc=datetime.fromisoformat(r["occurred_at"]),
                metric=r["metric"],
                ingest_batch_id=r["ingest_batch_id"],
                entity_id=entity_of.get(r["device"]),
            )
        )
    return out


def match_all(
    readings: list[Reading], items: list[ExpectedItem], contracts: list[dict]
) -> list[MatchDecision]:
    """对每条读数独立做完整配对判定（不依赖其它读数的选择）。"""
    tolerance = {
        c["contract_no"]: int(c["tolerance_seconds"]) for c in contracts
    }
    # 应到项按 (origin, entity, metric) 分组；被豁免项不参与配对
    bucket: dict[tuple[str, str, str], list[ExpectedItem]] = {}
    contract_keys = set()
    for it in items:
        contract_keys.add((it.origin, it.entity_id, it.metric))
        if it.exempt:
            continue
        bucket.setdefault(
            (it.origin, it.entity_id, it.metric), []
        ).append(it)
    for lst in bucket.values():
        lst.sort(key=lambda x: (x.scheduled_utc, x.item_id))

    decisions: list[MatchDecision] = []
    # 稳定顺序：读数身份字典序，保证输出可重现
    for rd in sorted(readings, key=lambda r: (r.origin, r.reading_key)):
        if rd.entity_id is None:
            decisions.append(
                MatchDecision(
                    rd, None, None, "device_not_in_membership_version"
                )
            )
            continue
        key = (rd.origin, rd.entity_id, rd.metric)
        if key not in contract_keys:
            decisions.append(
                MatchDecision(
                    rd, None, None, "no_contract_for_origin_entity_metric"
                )
            )
            continue
        candidates = bucket.get(key, [])
        scored = [
            (
                int(abs((rd.occurred_at_utc - it.scheduled_utc).total_seconds())),
                it,
            )
            for it in candidates
        ]
        scored.sort(key=lambda pair: (pair[0], pair[1].scheduled_utc, pair[1].item_id))
        candidate_basis = [
            {
                "item_id": it.item_id,
                "contract_no": it.contract_no,
                "scheduled_utc": it.scheduled_utc.isoformat(),
                "distance_seconds": dist,
                "tolerance_seconds": tolerance[it.contract_no],
            }
            for dist, it in scored[:5]
        ]
        if not scored:
            # 契约存在但窗口内只剩豁免项
            decisions.append(
                MatchDecision(
                    rd, None, None, "all_scheduled_positions_exempt", candidate_basis
                )
            )
            continue
        best_dist, best_item = scored[0]
        tol = tolerance[best_item.contract_no]
        if best_dist > tol:
            decisions.append(
                MatchDecision(
                    rd, None, best_dist, "outside_tolerance", candidate_basis
                )
            )
            continue
        decisions.append(
            MatchDecision(rd, best_item, best_dist, "matched", candidate_basis)
        )
    return decisions


def apply_coverage(
    items: list[ExpectedItem], decisions: list[MatchDecision]
) -> dict:
    """把配对结论落到应到项上，统计覆盖并识别重复上报。

    返回汇总字典；``match_rows`` 给出全部命中记录（含重复），
    ``unmatched`` 给出未匹配读数判定。
    """
    item_by_id = {it.item_id: it for it in items}
    hit_counts: dict[int, int] = {}
    match_rows = []
    unmatched = []

    for dec in decisions:
        if dec.reason != "matched" or dec.item is None:
            unmatched.append(dec)
            continue
        it = dec.item
        prior = hit_counts.get(it.item_id, 0)
        hit_counts[it.item_id] = prior + 1
        item_by_id[it.item_id].covered = True
        match_rows.append(
            {
                "item_id": it.item_id,
                "reading": dec.reading,
                "distance_seconds": dec.distance_seconds,
                "duplicate_hit": 1 if prior > 0 else 0,
            }
        )

    non_exempt = [it for it in items if not it.exempt]
    covered = sum(1 for it in non_exempt if it.covered)
    missing = [it for it in non_exempt if not it.covered]
    exempt_count = sum(1 for it in items if it.exempt)
    duplicate_count = sum(max(0, c - 1) for c in hit_counts.values())

    return {
        "expected_total": len(items),
        "denominator": len(non_exempt),
        "covered_count": covered,
        "missing_items": missing,
        "exempt_count": exempt_count,
        "duplicate_count": duplicate_count,
        "match_rows": match_rows,
        "unmatched": unmatched,
    }
