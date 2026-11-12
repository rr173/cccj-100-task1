"""日程展开引擎：把契约日程展开为带完整溯源信息的应到采样时刻。

展开顺序（与需求逐条对应）：

1. 只展开截止批次内**最大稿号且 active=true** 的契约；停用稿不产生应到项。
2. 逐日（契约 IANA 时区下的当地日）枚举：日期落在生效区间与查询区间
   的交集所覆盖的当地日范围内，按日段**当地起点日期**的 ISO 星期过滤。
3. 每个日段定义先经 :func:`timeutil.split_segments` 处理跨午夜拆分，
   再以**该日段起点为锚**按步长枚举挂钟时刻——步序 step_index 从 0
   计，绝不从查询起点重新计步。
4. 挂钟时刻经 :func:`timeutil.real_instants` 展开：春季跳时 gap 不产生
   应到项；秋季回拨 fold 产生两个真实时刻（fold_occurrence=0/1），
   各记各的 UTC 偏移。
5. 真实时刻先与 ``[生效起点, 生效终点)`` 和 ``[查询起点, 查询终点)``
   同时取交集（左闭右开），再用停机豁免**并集**扣除；重叠/包含豁免
   只扣一次，但保留全部命中豁免编号以便溯源。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from . import timeutil
from .timeutil import (
    enumerate_segment_instants,
    format_offset,
    get_zone,
    in_range,
    parse_hhmm,
    split_segments,
)


@dataclass
class ExpectedItem:
    contract_no: str
    draft_no: int
    origin: str
    entity_id: str
    metric: str
    scheduled_utc: datetime          # 真实时刻（UTC）
    tz_name: str
    local_date: str                  # 当地挂钟日期
    local_time: str                  # 当地挂钟 HH:MM:SS
    utc_offset: str                  # 该真实时刻的偏移
    fold_occurrence: int             # 0 唯一/回拨首次, 1 回拨第二次
    segment_date: str
    segment_start: str
    step_index: int
    exempt: bool = False
    exemption_ids: list[str] = field(default_factory=list)
    covered: bool = False
    item_id: int = -1


def _parse_utc(raw: str) -> datetime:
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def _merge_exemptions_with_ids(
    exemptions: list[dict],
) -> list[tuple[datetime, datetime, list[str]]]:
    """合并相交/包含的豁免区间为并集，每段保留当时生效的豁免编号。

    扫描线：起点事件 +1、终点事件 -1，同一时刻**先减后加**（左闭右开：
    在 t 结束的豁免不再覆盖 t 时刻）。相邻的并集段允许拆开记录，因此
    每个应到时刻能准确查到是哪些豁免覆盖了它；重叠豁免对同一时刻只扣
    一次（成员判定只看是否落在任一段内）。
    """
    events: list[tuple[datetime, int, str]] = []
    for ex in exemptions:
        lo = _parse_utc(ex["start_utc"])
        hi = _parse_utc(ex["end_utc"])
        events.append((lo, 1, ex["exemption_id"]))
        events.append((hi, -1, ex["exemption_id"]))
    events.sort(key=lambda e: (e[0], e[1]))

    merged: list[tuple[datetime, datetime, list[str]]] = []
    active: set[str] = set()
    seg_start: datetime | None = None
    seg_ids: set[str] = set()
    i = 0
    while i < len(events):
        ts = events[i][0]
        if seg_start is not None and ts > seg_start:
            merged.append((seg_start, ts, sorted(seg_ids)))
            seg_start = None
        j = i
        while j < len(events) and events[j][0] == ts:
            _, kind, ex_id = events[j]
            if kind == 1:
                active.add(ex_id)
            else:
                active.discard(ex_id)
            j += 1
        if active:
            seg_start = ts
            seg_ids = set(active)
        i = j
    return merged


def expand_contract(
    contract: dict,
    query_start: datetime,
    query_end: datetime,
    exemptions: list[dict],
) -> list[ExpectedItem]:
    """展开单个契约（其最大稿号稿件）的全部应到项（含被豁免项）。"""
    if not contract["active"]:
        return []

    zone = get_zone(contract["timezone"])
    eff_start = _parse_utc(contract["effective_start_utc"])
    eff_end = _parse_utc(contract["effective_end_utc"])

    # 生效范围与查询范围的交集；不相交则无应到项
    win_start = max(eff_start, query_start)
    win_end = min(eff_end, query_end)
    if not win_start < win_end:
        return []

    weekdays = set(contract["weekdays"])
    step_seconds = contract["step_seconds"]

    # 当地日迭代范围：交集两端转到契约时区取当地日
    first_day = win_start.astimezone(zone).date()
    last_day = win_end.astimezone(zone).date()

    exemption_union = _merge_exemptions_with_ids(exemptions)

    items: list[ExpectedItem] = []
    day = first_day
    one_day = timedelta(days=1)
    while day <= last_day:
        for seg_def in contract["segments"]:
            st = parse_hhmm(seg_def["start"], "segment.start")
            et = parse_hhmm(seg_def["end"], "segment.end")
            for segment in split_segments(day, st, et):
                # 适用星期按该日段当地起点日期判定
                if segment.day.isoweekday() not in weekdays:
                    continue
                instants = enumerate_segment_instants(zone, segment, step_seconds)
                for step_index, instant in enumerate(instants):
                    instant_utc = instant.astimezone(timezone.utc)
                    # 先与生效范围及查询范围取交集（左闭右开）
                    if not (
                        in_range(instant_utc, eff_start, eff_end)
                        and in_range(instant_utc, query_start, query_end)
                    ):
                        continue
                    local_dt = instant_utc.astimezone(zone)
                    hit_ids = [
                        ex_id
                        for lo, hi, ids in exemption_union
                        if in_range(instant_utc, lo, hi)
                        for ex_id in ids
                    ]
                    items.append(
                        ExpectedItem(
                            contract_no=contract["contract_no"],
                            draft_no=contract["draft_no"],
                            origin=contract["origin"],
                            entity_id=contract["entity_id"],
                            metric=contract["metric"],
                            scheduled_utc=instant_utc,
                            tz_name=contract["timezone"],
                            local_date=local_dt.date().isoformat(),
                            local_time=local_dt.time().isoformat(),
                            utc_offset=format_offset(local_dt),
                            fold_occurrence=_fold_occurrence(zone, local_dt),
                            segment_date=segment.day.isoformat(),
                            segment_start=segment.start_time.isoformat(),
                            step_index=step_index,
                            exempt=bool(hit_ids),
                            exemption_ids=sorted(set(hit_ids)),
                        )
                    )
        day += one_day

    items.sort(
        key=lambda it: (
            it.contract_no,
            it.scheduled_utc,
            it.fold_occurrence,
            it.segment_start,
            it.step_index,
        )
    )
    return items


def _fold_occurrence(zone, local_dt) -> int:
    """回拨时区分第一次（fold=0）与第二次（fold=1）真实时刻。"""
    kind = timeutil.classify_wall_time(
        zone, local_dt.replace(tzinfo=None)
    )
    if kind != "fold":
        return 0
    return int(local_dt.fold)


def expand_all(
    contracts: list[dict],
    query_start: datetime,
    query_end: datetime,
    exemptions_by_contract: dict[str, list[dict]],
) -> list[ExpectedItem]:
    """展开全部契约并赋予审计内稳定 item_id（按确定顺序）。"""
    all_items: list[ExpectedItem] = []
    for contract in sorted(contracts, key=lambda c: c["contract_no"]):
        exs = exemptions_by_contract.get(contract["contract_no"], [])
        all_items.extend(
            expand_contract(contract, query_start, query_end, exs)
        )
    all_items.sort(
        key=lambda it: (
            it.contract_no,
            it.scheduled_utc,
            it.fold_occurrence,
            it.segment_start,
            it.step_index,
        )
    )
    for idx, item in enumerate(all_items):
        item.item_id = idx + 1
    return all_items
