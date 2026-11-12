"""缺口段分析：连续缺采合并，被豁免的计划位置把缺口分开。

只在**同一契约内部**合并连续缺采项；一旦中间出现被豁免位置或已覆盖
位置，缺口即切断。每个缺口保存：

* 起止应到项与 UTC 时刻、全部缺采 item_id；
* 切断缺口的豁免编号（缺口前后紧邻的豁免位置，便于追查是哪段停机
  把本来连续的缺采切开）；
* 附近未匹配读数及其未匹配原因与候选距离依据（解释“附近明明有读数
  为什么没补上”）。
"""

from __future__ import annotations

from datetime import datetime

from .matcher import MatchDecision
from .scheduler import ExpectedItem

_NEARBY_WINDOW = 3  # 缺口两端各取最近若干条未匹配读数作为诊断


def build_gaps(
    items: list[ExpectedItem],
    missing_items: list[ExpectedItem],
    unmatched: list[MatchDecision],
) -> list[dict]:
    missing_ids = {it.item_id for it in missing_items}
    by_contract: dict[str, list[ExpectedItem]] = {}
    for it in items:
        by_contract.setdefault(it.contract_no, []).append(it)

    gaps: list[dict] = []
    gap_seq = 0
    for contract_no in sorted(by_contract):
        chain = sorted(
            by_contract[contract_no],
            key=lambda x: (x.scheduled_utc, x.fold_occurrence, x.item_id),
        )
        run: list[ExpectedItem] = []

        def flush(seq: int, run_items: list[ExpectedItem]) -> int:
            if not run_items:
                return seq
            seq += 1
            split_ex = sorted(
                {
                    ex_id
                    for it in run_items
                    for ex_id in _adjacent_exemptions(chain, it)
                }
            )
            gaps.append(
                {
                    "gap_id": seq,
                    "contract_no": contract_no,
                    "draft_no": run_items[0].draft_no,
                    "start_item_id": run_items[0].item_id,
                    "end_item_id": run_items[-1].item_id,
                    "start_utc": run_items[0].scheduled_utc.isoformat(),
                    "end_utc": run_items[-1].scheduled_utc.isoformat(),
                    "missing_item_ids": [it.item_id for it in run_items],
                    "split_exemptions": split_ex,
                    "nearby_readings": _nearby(run_items, unmatched),
                }
            )
            return seq

        for it in chain:
            if it.item_id in missing_ids:
                run.append(it)
            else:
                gap_seq = flush(gap_seq, run)
                run = []
        gap_seq = flush(gap_seq, run)
    return gaps


def _adjacent_exemptions(chain: list[ExpectedItem], target: ExpectedItem) -> list[str]:
    """缺口边界处紧邻的豁免位置携带的豁免编号。"""
    idx = next(i for i, x in enumerate(chain) if x.item_id == target.item_id)
    ids: set[str] = set()
    for j in (idx - 1, idx + 1):
        if 0 <= j < len(chain) and chain[j].exempt:
            ids.update(chain[j].exemption_ids)
    return sorted(ids)


def _nearby(run_items: list[ExpectedItem], unmatched: list[MatchDecision]) -> list[dict]:
    lo = run_items[0].scheduled_utc
    hi = run_items[-1].scheduled_utc
    basis: list[dict] = []
    for dec in unmatched:
        rd = dec.reading
        if (rd.origin, rd.metric) != (
            run_items[0].origin,
            run_items[0].metric,
        ):
            continue
        # 同实体（别名设备）或本契约实体的读数才算“附近”
        if rd.entity_id != run_items[0].entity_id:
            continue
        dist_to_window = _distance_to_window(rd.occurred_at_utc, lo, hi)
        basis.append(
            {
                "origin": rd.origin,
                "reading_key": rd.reading_key,
                "chosen_edition": rd.chosen_edition,
                "device": rd.device,
                "entity_id": rd.entity_id,
                "occurred_at_utc": rd.occurred_at_utc.isoformat(),
                "distance_to_gap_seconds": dist_to_window,
                "reason": dec.reason,
                "candidates": dec.candidates,
            }
        )
    basis.sort(key=lambda b: (b["distance_to_gap_seconds"], b["reading_key"]))
    return basis[:_NEARBY_WINDOW]


def _distance_to_window(ts: datetime, lo: datetime, hi: datetime) -> int:
    if ts < lo:
        return int((lo - ts).total_seconds())
    if ts > hi:
        return int((ts - hi).total_seconds())
    return 0
