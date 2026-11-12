"""采样契约与停机豁免的行格式协议、不可变稿号规则与确定性序列化。

契约批次（UTF-8 JSONL，每行一个对象）字段
================================================

================  ==========================================================
字段              约束
================  ==========================================================
``contract_no``   非空字符串，契约编号
``draft_no``      正整数（>=1，拒绝布尔），同一契约的稿号
``active``        布尔；false 表示自该稿起停用此契约
``origin``        非空字符串，读数来源（与读数台账 origin 对齐）
``entity_id``     非空字符串，设备实体标识（与归并层实体标识对齐）
``metric``        非空字符串，指标名
``effective_start`` 带时区 ISO-8601，生效起点（含）
``effective_end``   带时区 ISO-8601，生效终点（不含），必须晚于起点
``timezone``      IANA 时区名（如 America/New_York）
``weekdays``      非空整数数组，取值 1..7（周一=1 … 周日=7），按日段
                  当地起点日期的 ISO 星期判定
``segments``      非空数组，每项 {"start": "HH:MM", "end": "HH:MM"}；
                  end<=start 表示跨午夜班次，展开时自动拆为两个日段
``step_seconds``  正整数（>=1），步长秒数；以日段起点为锚
``tolerance_seconds`` 非负整数，匹配容差秒数；超出容差不能补洞
================  ==========================================================

豁免批次字段（每行一个对象）
============================

``exemption_id``  非空字符串，豁免编号（全局身份）
``contract_no``   非空字符串，关联契约编号（可为空串表示对全部契约生效，
                  本项目要求显式给出关联契约）
``start``         带时区 ISO-8601，豁免起点（含）
``end``           带时区 ISO-8601，豁免终点（不含），必须晚于起点
``reason``        字符串（允许空串），停机原因

**不可变稿件**：契约身份为 ``(contract_no, draft_no)``。同稿号重投且
内容逐字一致 -> 幂等无操作；同稿号换任何内容字段 -> :class:`DraftConflict`
整批拒收。新稿号可修订日程或停用（active=false）。豁免身份为
``exemption_id``，同样不可变。
"""

from __future__ import annotations

import hashlib
import json
from datetime import timezone
from typing import Any, Iterable

from .errors import CoverageProtocolError
from .timeutil import get_zone, parse_aware_iso, parse_hhmm

CONTRACT_FIELDS = frozenset(
    {
        "contract_no",
        "draft_no",
        "active",
        "origin",
        "entity_id",
        "metric",
        "effective_start",
        "effective_end",
        "timezone",
        "weekdays",
        "segments",
        "step_seconds",
        "tolerance_seconds",
    }
)

EXEMPTION_FIELDS = frozenset(
    {"exemption_id", "contract_no", "start", "end", "reason"}
)


def canonical_json(obj: Any) -> str:
    """键序确定的紧凑 JSON（与装载器/归并层/读数层同一约定）。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def iter_jsonl(path: str) -> Iterable[tuple[int, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                yield lineno, json.loads(text)
            except json.JSONDecodeError as exc:
                raise CoverageProtocolError(
                    f"{path}:{lineno} 不是合法 JSON: {exc.msg}"
                ) from exc


def _require_non_empty_str(row: dict, field: str, where: str) -> str:
    value = row[field]
    if not isinstance(value, str) or not value:
        raise CoverageProtocolError(
            f"{where} 字段 {field} 必须是非空字符串: {value!r}"
        )
    return value


def _require_positive_int(row: dict, field: str, where: str, minimum: int = 1) -> int:
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CoverageProtocolError(
            f"{where} 字段 {field} 必须是 >= {minimum} 的整数: {value!r}"
        )
    return value


def validate_contract_row(raw: Any, where: str = "契约行") -> dict:
    """校验一行契约并返回规范化字典（字段集合必须恰好匹配）。"""
    if not isinstance(raw, dict):
        raise CoverageProtocolError(
            f"{where} 必须是 JSON 对象，实际为: {type(raw).__name__}"
        )
    missing = CONTRACT_FIELDS - set(raw)
    extra = set(raw) - CONTRACT_FIELDS
    if missing:
        raise CoverageProtocolError(f"{where} 缺少字段: {sorted(missing)}")
    if extra:
        raise CoverageProtocolError(f"{where} 出现未定义字段: {sorted(extra)}")

    contract_no = _require_non_empty_str(raw, "contract_no", where)
    origin = _require_non_empty_str(raw, "origin", where)
    entity_id = _require_non_empty_str(raw, "entity_id", where)
    metric = _require_non_empty_str(raw, "metric", where)
    draft_no = _require_positive_int(raw, "draft_no", where)
    active = raw["active"]
    if not isinstance(active, bool):
        raise CoverageProtocolError(
            f"{where} 字段 active 必须是布尔值: {active!r}"
        )

    try:
        start = parse_aware_iso(raw["effective_start"], f"{where} effective_start")
        end = parse_aware_iso(raw["effective_end"], f"{where} effective_end")
    except ValueError as exc:
        raise CoverageProtocolError(str(exc)) from exc
    if not end > start:
        raise CoverageProtocolError(
            f"{where} 生效区间必须左闭右开且终点晚于起点: "
            f"{raw['effective_start']} .. {raw['effective_end']}"
        )

    tz_name = raw["timezone"]
    if not isinstance(tz_name, str) or not tz_name.strip():
        raise CoverageProtocolError(f"{where} 字段 timezone 必须是非空 IANA 名")
    try:
        get_zone(tz_name)  # 未知时区名在此即拒
    except ValueError as exc:
        raise CoverageProtocolError(str(exc)) from exc

    weekdays = raw["weekdays"]
    if (
        not isinstance(weekdays, list)
        or not weekdays
        or any(isinstance(v, bool) or not isinstance(v, int) for v in weekdays)
        or any(v < 1 or v > 7 for v in weekdays)
    ):
        raise CoverageProtocolError(
            f"{where} 字段 weekdays 必须是 1..7 的非空整数数组: {weekdays!r}"
        )
    weekdays = sorted(set(weekdays))

    segments_raw = raw["segments"]
    if not isinstance(segments_raw, list) or not segments_raw:
        raise CoverageProtocolError(
            f"{where} 字段 segments 必须是非空数组: {segments_raw!r}"
        )
    segments = []
    for idx, seg in enumerate(segments_raw):
        sw = f"{where} segments[{idx}]"
        if not isinstance(seg, dict) or set(seg) != {"start", "end"}:
            raise CoverageProtocolError(
                f"{sw} 必须是恰好含 start/end 的对象: {seg!r}"
            )
        try:
            st = parse_hhmm(seg["start"], f"{sw}.start")
            et = parse_hhmm(seg["end"], f"{sw}.end")
        except ValueError as exc:
            raise CoverageProtocolError(str(exc)) from exc
        if st == et:
            raise CoverageProtocolError(
                f"{sw} 起点与终点相同（零长度日段不合法）: {seg!r}"
            )
        segments.append({"start": st.strftime("%H:%M:%S"), "end": et.strftime("%H:%M:%S")})

    step_seconds = _require_positive_int(raw, "step_seconds", where)
    tolerance_seconds = _require_positive_int(
        raw, "tolerance_seconds", where, minimum=0
    )

    return {
        "contract_no": contract_no,
        "draft_no": draft_no,
        "active": active,
        "origin": origin,
        "entity_id": entity_id,
        "metric": metric,
        "effective_start_utc": start.astimezone(timezone.utc).isoformat(),
        "effective_end_utc": end.astimezone(timezone.utc).isoformat(),
        "timezone": tz_name,
        "weekdays": weekdays,
        "segments": segments,
        "step_seconds": step_seconds,
        "tolerance_seconds": tolerance_seconds,
    }


def validate_exemption_row(raw: Any, where: str = "豁免行") -> dict:
    if not isinstance(raw, dict):
        raise CoverageProtocolError(
            f"{where} 必须是 JSON 对象，实际为: {type(raw).__name__}"
        )
    missing = EXEMPTION_FIELDS - set(raw)
    extra = set(raw) - EXEMPTION_FIELDS
    if missing:
        raise CoverageProtocolError(f"{where} 缺少字段: {sorted(missing)}")
    if extra:
        raise CoverageProtocolError(f"{where} 出现未定义字段: {sorted(extra)}")

    exemption_id = _require_non_empty_str(raw, "exemption_id", where)
    contract_no = _require_non_empty_str(raw, "contract_no", where)
    reason = raw["reason"]
    if not isinstance(reason, str):
        raise CoverageProtocolError(f"{where} 字段 reason 必须是字符串")
    try:
        start = parse_aware_iso(raw["start"], f"{where} start")
        end = parse_aware_iso(raw["end"], f"{where} end")
    except ValueError as exc:
        raise CoverageProtocolError(str(exc)) from exc
    if not end > start:
        raise CoverageProtocolError(
            f"{where} 豁免区间必须左闭右开且终点晚于起点: "
            f"{raw['start']} .. {raw['end']}"
        )
    return {
        "exemption_id": exemption_id,
        "contract_no": contract_no,
        "start_utc": start.astimezone(timezone.utc).isoformat(),
        "end_utc": end.astimezone(timezone.utc).isoformat(),
        "reason": reason,
    }


def contract_identity(row: dict) -> tuple[str, int]:
    return (row["contract_no"], row["draft_no"])


def contract_content(row: dict) -> dict:
    """稿号身份之外、决定契约语义的全部字段（规范化值）。"""
    return {
        "active": row["active"],
        "origin": row["origin"],
        "entity_id": row["entity_id"],
        "metric": row["metric"],
        "effective_start_utc": row["effective_start_utc"],
        "effective_end_utc": row["effective_end_utc"],
        "timezone": row["timezone"],
        "weekdays": row["weekdays"],
        "segments": row["segments"],
        "step_seconds": row["step_seconds"],
        "tolerance_seconds": row["tolerance_seconds"],
    }


def exemption_identity(row: dict) -> str:
    return row["exemption_id"]


def exemption_content(row: dict) -> dict:
    return {
        "contract_no": row["contract_no"],
        "start_utc": row["start_utc"],
        "end_utc": row["end_utc"],
        "reason": row["reason"],
    }


def content_hash(row: dict) -> str:
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()
