"""时间语义工具：IANA 时区、左闭右开区间、夏令时跳时/回拨的显式处理。

本模块只依赖标准库 :mod:`zoneinfo`。所有生效区间、查询区间、豁免区间
一律**左闭右开** ``[start, end)``。

核心概念
========

* 契约日程按 **IANA 时区名**（如 ``America/New_York``、``Asia/Shanghai``）
  解释，不接受裸偏移（裸偏移无法表达夏令时规则）。
* 每个日段以**该日段当地起点为锚**按步长枚举当地挂钟时刻；步长计数
  绝不从查询起点重新起算。
* 春季跳时（gap）：某当地挂钟时刻在该时区根本不存在（如美国东部
  02:00–03:00 被跳过），``fold=0`` 与 ``fold=1`` 解析到同一真实时刻且
  两侧偏移发生跳变——此时**不制造应到项**。
* 秋季回拨（fold）：同一当地挂钟时刻出现两次（如 01:00 第一次在 EDT、
  第二次在 EST），``fold=0`` 与 ``fold=1`` 解析到**两个不同真实时刻**
  ——两个真实时刻都应采样，各产生一个应到项，分别记录各自 UTC 偏移。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# 解析与规范化
# ---------------------------------------------------------------------------


def parse_aware_iso(raw: str, where: str = "时间") -> datetime:
    """解析必须显式携带时区的 ISO-8601 字符串，返回 aware datetime。

    朴素（无时区）时间一律拒收，避免隐式按本机时区解释。
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{where} 必须是非空时间字符串: {raw!r}")
    try:
        dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{where} 不是合法 ISO-8601 时间: {raw!r}") from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{where} 必须显式携带时区（Z 或 ±HH:MM）: {raw!r}")
    return dt


def to_utc(dt: datetime) -> datetime:
    """归一到 UTC（保留微秒）。"""
    return dt.astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    """UTC ISO 字符串。"""
    return to_utc(dt).isoformat()


def get_zone(tz_name: str) -> ZoneInfo:
    """按 IANA 名取时区；裸偏移/未知时区名直接报错。

    ``ZoneInfoNotFoundError`` 是 :class:`KeyError` 子类，统一转成
    :class:`ValueError` 以便上层按协议错误处理。
    """
    if not isinstance(tz_name, str) or not tz_name.strip():
        raise ValueError(f"时区名必须是非空 IANA 字符串: {tz_name!r}")
    try:
        return ZoneInfo(tz_name)
    except Exception as exc:
        raise ValueError(f"未知或不可用的 IANA 时区名 {tz_name!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# 夏令时判定
# ---------------------------------------------------------------------------


def classify_wall_time(zone: ZoneInfo, naive_dt: datetime) -> str:
    """判定一个朴素当地挂钟时刻在该时区的存在性。

    返回：

    * ``"normal"``：唯一真实时刻；
    * ``"gap"``：春季跳时中不存在的当地时刻（不制造应到项）；
    * ``"fold"``：秋季回拨中出现两次的当地时刻（两个真实时刻都采样）。

    PEP 495 判定：分别以 ``fold=0``/``fold=1`` 附加时区后比较归一 UTC
    的真实时刻。gap 与 fold 下两次解析都得到不同真实时刻，区别在顺序：

    * fold（回拨歧义）：fold=0 落在**更早**的真实时刻（``u0 < u1``）；
    * gap（跳时间隙）：fold=0 落在**更晚**的真实时刻（``u0 > u1``）。
    """
    dt0 = naive_dt.replace(tzinfo=zone, fold=0)
    dt1 = naive_dt.replace(tzinfo=zone, fold=1)
    u0, u1 = dt0.astimezone(timezone.utc), dt1.astimezone(timezone.utc)
    if u0 == u1:
        return "normal"
    return "fold" if u0 < u1 else "gap"


def real_instants(zone: ZoneInfo, naive_dt: datetime) -> list[datetime]:
    """把一个朴素当地挂钟时刻展开为 1 个或 2 个真实（aware）时刻。

    normal -> 1 个；fold -> fold=0、fold=1 两个（按真实时间先后）；
    gap -> 空列表。
    """
    kind = classify_wall_time(zone, naive_dt)
    if kind == "gap":
        return []
    if kind == "fold":
        instants = [
            naive_dt.replace(tzinfo=zone, fold=0),
            naive_dt.replace(tzinfo=zone, fold=1),
        ]
        return sorted(instants, key=lambda d: d.astimezone(timezone.utc))
    return [naive_dt.replace(tzinfo=zone, fold=0)]


def offset_minutes(dt_aware: datetime) -> int:
    """该真实时刻相对 UTC 的偏移分钟数（东正西负）。"""
    return int(dt_aware.utcoffset().total_seconds() // 60)


def format_offset(dt_aware: datetime) -> str:
    """形如 ``+08:00`` / ``-05:00`` 的 UTC 偏移文本。"""
    total = int(dt_aware.utcoffset().total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    return f"{sign}{total // 3600:02d}:{total % 3600 // 60:02d}"


# ---------------------------------------------------------------------------
# 日段与步长枚举（锚定在日段起点）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LocalSegment:
    """一个明确的当地日段（跨午夜班次已拆成两段中的一段）。

    ``day``        该段挂钟所属当地日期
    ``start_time`` 当地起点 ``HH:MM[:SS]``
    ``end_time``   当地终点（不含）；同日内必须晚于起点
    """

    day: date
    start_time: time
    end_time: time


def parse_hhmm(raw: str, where: str) -> time:
    if not isinstance(raw, str):
        raise ValueError(f"{where} 必须是 HH:MM 字符串: {raw!r}")
    text = raw.strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"{where} 不是合法 HH:MM[:SS] 时间: {raw!r}")


def split_segments(
    day: date, start_time: time, end_time: time
) -> list[LocalSegment]:
    """把一个日段定义规范化为明确日段；跨午夜（end<=start）拆成两个日段。

    例如 22:00–02:00 的班次拆为：当天 [22:00,24:00) 与次日 [00:00,02:00)。
    适用星期按**每段各自的当地起点日期**判定。
    """
    if end_time > start_time:
        return [LocalSegment(day, start_time, end_time)]
    midnight = time(0, 0)
    end_of_day = time(23, 59, 59, 999999)
    return [
        LocalSegment(day, start_time, end_of_day),
        LocalSegment(day + timedelta(days=1), midnight, end_time),
    ]


def enumerate_segment_instants(
    zone: ZoneInfo, segment: LocalSegment, step_seconds: int
) -> list[datetime]:
    """以**该日段起点为锚**枚举段内全部当地挂钟时刻并展开为真实时刻。

    挂钟枚举本身按朴素时间 + 步长推进（步长是契约定义的挂钟节拍），
    再逐个经 :func:`real_instants` 处理跳时/回拨：

    * gap 时刻跳过（不产生应到项）；
    * fold 时刻产生两个按真实时间排序的真实时刻。
    """
    naive_start = datetime.combine(segment.day, segment.start_time)
    naive_end = datetime.combine(segment.day, segment.end_time)
    # end=23:59:59.999999 表示当日结束（24:00 排他边界）
    if segment.end_time == time(23, 59, 59, 999999):
        naive_end = datetime.combine(segment.day, time(0, 0)) + timedelta(days=1)
    out: list[datetime] = []
    cur = naive_start
    step = timedelta(seconds=step_seconds)
    while cur < naive_end:
        out.extend(real_instants(zone, cur))
        cur = cur + step
    return out


# ---------------------------------------------------------------------------
# 左闭右开区间工具
# ---------------------------------------------------------------------------


def intersects(lo_a: datetime, hi_a: datetime, lo_b: datetime, hi_b: datetime) -> bool:
    """两个左闭右开区间是否相交。"""
    return lo_a < hi_b and lo_b < hi_a


def in_range(ts: datetime, lo: datetime, hi: datetime) -> bool:
    """左闭右开成员判定 ``lo <= ts < hi``。"""
    return lo <= ts < hi


def merge_intervals(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """把若干左闭右开区间合并为互不相交的并集（允许相交和包含）。"""
    if not intervals:
        return []
    ordered = sorted(intervals, key=lambda iv: (iv[0], iv[1]))
    merged: list[list[datetime]] = [list(ordered[0])]
    for lo, hi in ordered[1:]:
        if lo <= merged[-1][1]:  # 相交或相邻都并入并集
            if hi > merged[-1][1]:
                merged[-1][1] = hi
        else:
            merged.append([lo, hi])
    return [(lo, hi) for lo, hi in merged]


def covered_by_union(
    ts: datetime, union: list[tuple[datetime, datetime]]
) -> tuple[bool, list[int]]:
    """时刻是否落在豁免并集内；返回 (是否豁免, 命中的并集段下标列表)。"""
    hits = [i for i, (lo, hi) in enumerate(union) if in_range(ts, lo, hi)]
    return (bool(hits), hits)
