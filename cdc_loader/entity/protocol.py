"""对照资料（关系修订）的行格式协议与确定性序列化。

每行一个 JSON 对象（UTF-8 JSONL，空行忽略），**恰好**包含 7 个字段：

==============  =====================================================
字段            约束
==============  =====================================================
``source``      非空字符串，资料来源（如 procurement/maintenance/supplier）
``relation_id`` 非空字符串，来源内部的关系编号
``revision``    正整数（>=1，不接受布尔），同一 (source, relation_id) 的修订号
``endpoint_a``  非空字符串，一端设备编号
``endpoint_b``  非空字符串，另一端设备编号，不得与 endpoint_a 相同
``category``    ``same_entity``（同一实体）或 ``must_differ``（必须不同）
``effective``   布尔；false 表示自该修订起撤销此关系
==============  =====================================================

缺字段、多字段、类型不符都按协议错误拒绝。

**修订身份与内容**：身份为 ``(source, relation_id, revision)``，内容为
``(endpoint_a, endpoint_b, category, effective)``。台账按身份持久保留
每一条修订的完整内容；重投内容一致的旧修订是幂等无操作，改写任何
旧修订内容（哪怕该修订已被更高修订号取代）都会被拒绝。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from .errors import RelationProtocolError

CATEGORY_SAME = "same_entity"
CATEGORY_DIFFER = "must_differ"
CATEGORIES = frozenset({CATEGORY_SAME, CATEGORY_DIFFER})

ROW_FIELDS = frozenset(
    {
        "source",
        "relation_id",
        "revision",
        "endpoint_a",
        "endpoint_b",
        "category",
        "effective",
    }
)


def canonical_json(obj: Any) -> str:
    """把 JSON 值转成键顺序确定的紧凑字符串（与装载器同一约定）。"""
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
                raise RelationProtocolError(
                    f"{path}:{lineno} 不是合法 JSON: {exc.msg}"
                ) from exc


def _require_non_empty_str(row: dict, field: str, where: str) -> str:
    value = row[field]
    if not isinstance(value, str) or not value:
        raise RelationProtocolError(
            f"{where} 字段 {field} 必须是非空字符串: {value!r}"
        )
    return value


def validate_relation_row(raw: Any, where: str = "对照资料行") -> dict:
    """校验一行对照资料并返回规范化字典（字段集合必须恰好匹配）。"""
    if not isinstance(raw, dict):
        raise RelationProtocolError(
            f"{where} 必须是 JSON 对象，实际为: {type(raw).__name__}"
        )
    missing = ROW_FIELDS - set(raw)
    extra = set(raw) - ROW_FIELDS
    if missing:
        raise RelationProtocolError(f"{where} 缺少字段: {sorted(missing)}")
    if extra:
        raise RelationProtocolError(f"{where} 出现未定义字段: {sorted(extra)}")

    source = _require_non_empty_str(raw, "source", where)
    relation_id = _require_non_empty_str(raw, "relation_id", where)
    endpoint_a = _require_non_empty_str(raw, "endpoint_a", where)
    endpoint_b = _require_non_empty_str(raw, "endpoint_b", where)

    revision = raw["revision"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise RelationProtocolError(
            f"{where} 字段 revision 必须是 >=1 的整数: {revision!r}"
        )

    if endpoint_a == endpoint_b:
        raise RelationProtocolError(
            f"{where} 两端设备编号不能相同: {endpoint_a!r}"
        )

    category = raw["category"]
    if category not in CATEGORIES:
        raise RelationProtocolError(
            f"{where} 字段 category 必须是 {sorted(CATEGORIES)} 之一: {category!r}"
        )

    effective = raw["effective"]
    if not isinstance(effective, bool):
        raise RelationProtocolError(
            f"{where} 字段 effective 必须是布尔值: {effective!r}"
        )

    return {
        "source": source,
        "relation_id": relation_id,
        "revision": revision,
        "endpoint_a": endpoint_a,
        "endpoint_b": endpoint_b,
        "category": category,
        "effective": effective,
    }


def identity_of(row: dict) -> tuple[str, str, int]:
    """修订身份：(来源, 关系编号, 修订号)。"""
    return (row["source"], row["relation_id"], row["revision"])


def content_of(row: dict) -> dict:
    """修订内容：身份之外、决定关系语义的四个字段。"""
    return {
        "endpoint_a": row["endpoint_a"],
        "endpoint_b": row["endpoint_b"],
        "category": row["category"],
        "effective": row["effective"],
    }


def content_hash(row: dict) -> str:
    """整行规范化内容的 SHA-256（十六进制小写），用于审计与批次摘要。"""
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()
