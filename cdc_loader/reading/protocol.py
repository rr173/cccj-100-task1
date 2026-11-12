"""振动读数逐行资料的格式协议与确定性序列化。

每行一个 JSON 对象（UTF-8 JSONL，空行忽略），**恰好**包含 8 个字段：

================  =====================================================
字段              约束
================  =====================================================
``origin``        非空字符串，读数来源；不同来源的 reading_key 互相独立
``reading_key``   非空字符串，来源内部的观测编号
``edition``       正整数（>=1，不接受布尔），同一观测的稿件号
``device``        非空字符串，稿件声称的设备编号（后续稿件可改）
``occurred_at``   带时区的 ISO-8601 时间字符串（必须含 UTC 偏移或 Z）
``metric``        非空字符串，指标名（如 vibration_rms）
``amount``        有限十进制文本（Decimal 可解析，禁止 NaN/Infinity）
``withdrawn``     布尔；true 表示该稿件撤回此观测
================  =====================================================

缺字段、多字段、类型不符都按协议错误拒绝。

**稿件身份与内容**：身份为 ``(origin, reading_key, edition)``；内容为
``(device, occurred_at, metric, amount, withdrawn)``。事实仓按身份持久
保留每一条稿件原文，永不改写、永不删除。重投内容一致的稿件是幂等
无操作；同一稿件身份换了任何内容字段都按篡改拒收。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .errors import ReadingProtocolError

ROW_FIELDS = frozenset(
    {
        "origin",
        "reading_key",
        "edition",
        "device",
        "occurred_at",
        "metric",
        "amount",
        "withdrawn",
    }
)


def canonical_json(obj: Any) -> str:
    """把 JSON 值转成键顺序确定的紧凑字符串（与装载器/归并层同一约定）。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def iter_jsonl(path: str) -> Iterable[tuple[int, Any]]:
    """逐行读取 JSONL，产出 ``(行号, 对象)``，空行跳过。"""
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                yield lineno, json.loads(text)
            except json.JSONDecodeError as exc:
                raise ReadingProtocolError(
                    f"{path}:{lineno} 不是合法 JSON: {exc.msg}"
                ) from exc


def _require_non_empty_str(row: dict, field: str, where: str) -> str:
    value = row[field]
    if not isinstance(value, str) or not value:
        raise ReadingProtocolError(
            f"{where} 字段 {field} 必须是非空字符串: {value!r}"
        )
    return value


def parse_amount(raw: Any, where: str) -> str:
    """校验 amount 为有限十进制文本，返回规范化的十进制字符串。

    用 :class:`decimal.Decimal` 解析，拒绝 NaN / Infinity / 科学计数以外
    的非法输入；保留十进制精度（绝不转 float）。规范化采用
    ``Decimal(str)`` 的标准字符串形式，去掉正负号上的等价差异。
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ReadingProtocolError(
            f"{where} 字段 amount 必须是非空十进制文本字符串: {raw!r}"
        )
    text = raw.strip()
    try:
        dec = Decimal(text)
    except InvalidOperation as exc:
        raise ReadingProtocolError(
            f"{where} 字段 amount 不是合法十进制: {raw!r}"
        ) from exc
    if not dec.is_finite():
        raise ReadingProtocolError(
            f"{where} 字段 amount 必须是有限数，禁止 NaN/Infinity: {raw!r}"
        )
    return str(dec)


def parse_occurred_at(raw: Any, where: str) -> tuple[datetime, str]:
    """解析带时区的时间，返回 ``(UTC datetime, 归一化后的 ISO 字符串)``。

    必须显式携带时区（``Z`` 或形如 ``+08:00`` 的偏移）；朴素时间一律
    拒收，避免隐式按本机时区解释。归一到 UTC 后用
    ``YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00`` 形式保存，日期按 UTC 取。
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ReadingProtocolError(
            f"{where} 字段 occurred_at 必须是非空时间字符串: {raw!r}"
        )
    text = raw.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ReadingProtocolError(
            f"{where} 字段 occurred_at 不是合法 ISO-8601 时间: {raw!r}"
        ) from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ReadingProtocolError(
            f"{where} 字段 occurred_at 必须显式携带时区（Z 或 ±HH:MM）: {raw!r}"
        )
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc, dt_utc.isoformat()


def utc_date(dt_utc: datetime) -> str:
    """UTC 日期，形如 ``2026-11-12``。"""
    return dt_utc.date().isoformat()


def validate_reading_row(raw: Any, where: str = "读数行") -> dict:
    """校验一行读数并返回规范化字典（字段集合必须恰好匹配）。"""
    if not isinstance(raw, dict):
        raise ReadingProtocolError(
            f"{where} 必须是 JSON 对象，实际为: {type(raw).__name__}"
        )
    missing = ROW_FIELDS - set(raw)
    extra = set(raw) - ROW_FIELDS
    if missing:
        raise ReadingProtocolError(f"{where} 缺少字段: {sorted(missing)}")
    if extra:
        raise ReadingProtocolError(f"{where} 出现未定义字段: {sorted(extra)}")

    origin = _require_non_empty_str(raw, "origin", where)
    reading_key = _require_non_empty_str(raw, "reading_key", where)
    device = _require_non_empty_str(raw, "device", where)
    metric = _require_non_empty_str(raw, "metric", where)

    edition = raw["edition"]
    if isinstance(edition, bool) or not isinstance(edition, int) or edition < 1:
        raise ReadingProtocolError(
            f"{where} 字段 edition 必须是 >=1 的整数: {edition!r}"
        )

    withdrawn = raw["withdrawn"]
    if not isinstance(withdrawn, bool):
        raise ReadingProtocolError(
            f"{where} 字段 withdrawn 必须是布尔值: {withdrawn!r}"
        )

    amount_norm = parse_amount(raw["amount"], where)
    dt_utc, occurred_at_utc = parse_occurred_at(raw["occurred_at"], where)

    return {
        "origin": origin,
        "reading_key": reading_key,
        "edition": edition,
        "device": device,
        "occurred_at": occurred_at_utc,
        "occurred_date": utc_date(dt_utc),
        "metric": metric,
        "amount": amount_norm,
        "withdrawn": withdrawn,
    }


def identity_of(row: dict) -> tuple[str, str, int]:
    """稿件身份：(来源, 来源内观测编号, 稿件号)。"""
    return (row["origin"], row["reading_key"], row["edition"])


def observation_key(row: dict) -> tuple[str, str]:
    """观测身份：(来源, 来源内观测编号)。不同来源的 reading_key 独立计数。"""
    return (row["origin"], row["reading_key"])


def content_of(row: dict) -> dict:
    """稿件内容：身份之外、决定读数语义的五个字段（均为规范化值）。"""
    return {
        "device": row["device"],
        "occurred_at": row["occurred_at"],
        "metric": row["metric"],
        "amount": row["amount"],
        "withdrawn": row["withdrawn"],
    }


def content_hash(row: dict) -> str:
    """整行规范化内容的 SHA-256（十六进制小写），用于审计与批次摘要。"""
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()
