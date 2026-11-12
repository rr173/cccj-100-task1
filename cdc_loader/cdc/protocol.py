"""项目自定义的输入协议：字段类型、快照清单与变更日志事件的校验。

输入约定（详见 README）：

* 快照清单是一个 JSON 对象，给出切点 ``cut_point_lsn``、固定字段定义、
  分片列表（每片的文件名、行数与 SHA-256）。
* 分片与日志均为 UTF-8 JSONL，每行一个 JSON 对象，空行允许出现并被忽略。
* 日志事件类型为 BEGIN / INSERT / UPDATE / DELETE / COMMIT / ABORT，
  每个事务带 ``txn_id``，事务内事件带从 1 开始的递增序号 ``seq``，
  COMMIT 事件另带整数 ``commit_lsn``。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from .errors import ProtocolError

# 支持的字段类型。INTEGER/REAL 不接受布尔值（Python 中 bool 是 int 子类，
# 必须显式排除），NULL 用 None 表示。
TEXT = "TEXT"
INTEGER = "INTEGER"
REAL = "REAL"
SUPPORTED_TYPES = frozenset({TEXT, INTEGER, REAL})


# ---------------------------------------------------------------------------
# 确定性序列化：用于“同一事件身份不同载荷”的冲突判定，保证与键顺序无关。
# ---------------------------------------------------------------------------

def canonical_json(obj: Any) -> str:
    """把 JSON 值转成键顺序确定的紧凑字符串。

    输入必须是 JSON 可表示的值（协议校验已先行保证），因此直接用
    ``json.dumps`` 的 sort_keys 即可得到稳定结果。
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# 清单与字段
# ---------------------------------------------------------------------------

def validate_field_def(field: Any) -> dict:
    """校验单个字段定义，返回规范化后的定义。"""
    if not isinstance(field, dict):
        raise ProtocolError(f"字段定义必须是对象，实际为: {type(field).__name__}")
    name = field.get("name")
    if not isinstance(name, str) or not name:
        raise ProtocolError(f"字段名必须是非空字符串: {field!r}")
    ftype = field.get("type")
    if ftype not in SUPPORTED_TYPES:
        raise ProtocolError(
            f"字段 {name!r} 的类型 {ftype!r} 不受支持，允许: {sorted(SUPPORTED_TYPES)}"
        )
    if ftype == TEXT and not isinstance(ftype, str):  # pragma: no cover - 防御
        raise ProtocolError(f"字段 {name!r} 类型非法")
    return {"name": name, "type": ftype}


def validate_manifest(manifest: Any) -> dict:
    """校验快照清单并返回规范化结构。"""
    if not isinstance(manifest, dict):
        raise ProtocolError("快照清单必须是 JSON 对象")

    cut = manifest.get("cut_point_lsn")
    if not isinstance(cut, int) or isinstance(cut, bool) or cut < 0:
        raise ProtocolError("cut_point_lsn 必须是非负整数")

    fields_raw = manifest.get("fields")
    if not isinstance(fields_raw, list) or not fields_raw:
        raise ProtocolError("fields 必须是非空数组")
    fields = [validate_field_def(f) for f in fields_raw]

    names = [f["name"] for f in fields]
    if len(set(names)) != len(names):
        raise ProtocolError(f"字段名存在重复: {names}")
    if "device_id" not in names:
        raise ProtocolError("固定字段中必须包含主键字段 device_id")
    if fields[0]["name"] != "device_id" or fields[0]["type"] != TEXT:
        raise ProtocolError("device_id 必须是首个 TEXT 字段（主键）")
    for f in fields:
        if f["name"] == "device_id":
            continue
        # 其余字段允许为任意受支持类型。
    field_map = {f["name"]: f["type"] for f in fields}

    shards_raw = manifest.get("shards")
    if not isinstance(shards_raw, list) or not shards_raw:
        raise ProtocolError("shards 必须是非空数组")
    shards = []
    seen_files: set[str] = set()
    for idx, sh in enumerate(shards_raw):
        if not isinstance(sh, dict):
            raise ProtocolError(f"第 {idx} 个分片定义必须是对象")
        fname = sh.get("file")
        if not isinstance(fname, str) or not fname:
            raise ProtocolError(f"第 {idx} 个分片缺少 file")
        if fname in seen_files:
            raise ProtocolError(f"分片文件名重复: {fname}")
        seen_files.add(fname)
        rows = sh.get("rows")
        if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
            raise ProtocolError(f"分片 {fname} 的 rows 必须是非负整数")
        digest = sh.get("sha256")
        if not isinstance(digest, str) or len(digest) != 64:
            raise ProtocolError(f"分片 {fname} 的 sha256 必须是 64 位十六进制字符串")
        try:
            int(digest, 16)
        except ValueError:
            raise ProtocolError(f"分片 {fname} 的 sha256 不是合法十六进制")
        shards.append({"file": fname, "rows": rows, "sha256": digest.lower()})

    return {
        "cut_point_lsn": cut,
        "fields": fields,
        "field_map": field_map,
        "shards": shards,
    }


def validate_row(row: Any, field_map: dict[str, str]) -> dict:
    """按字段定义校验一行数据，返回规范化后的行（按字段顺序）。"""
    if not isinstance(row, dict):
        raise ProtocolError(f"数据行必须是对象，实际为: {type(row).__name__}")
    missing = set(field_map) - set(row)
    extra = set(row) - set(field_map)
    if missing:
        raise ProtocolError(f"数据行缺少字段: {sorted(missing)}")
    if extra:
        raise ProtocolError(f"数据行出现未定义字段: {sorted(extra)}")
    out: dict[str, Any] = {}
    for name, ftype in field_map.items():
        out[name] = _check_value(name, row[name], ftype)
    if not isinstance(out["device_id"], str) or not out["device_id"]:
        raise ProtocolError("device_id 必须是非空字符串")
    return out


def _check_value(name: str, value: Any, ftype: str) -> Any:
    if value is None:
        return None
    if ftype == TEXT:
        if not isinstance(value, str):
            raise ProtocolError(f"字段 {name} 期望 TEXT，实际为 {type(value).__name__}")
        return value
    if ftype == INTEGER:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ProtocolError(f"字段 {name} 期望 INTEGER，实际为 {type(value).__name__}")
        return value
    if ftype == REAL:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ProtocolError(f"字段 {name} 期望 REAL，实际为 {type(value).__name__}")
        return float(value)
    raise ProtocolError(f"字段 {name} 类型未知: {ftype}")  # pragma: no cover


# ---------------------------------------------------------------------------
# JSONL 读取
# ---------------------------------------------------------------------------

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
                raise ProtocolError(f"{path}:{lineno} 不是合法 JSON: {exc.msg}") from exc


def sha256_file(path: str) -> str:
    """计算文件的 SHA-256（十六进制小写），与分片摘要比对用。"""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 变更日志事件
# ---------------------------------------------------------------------------

EVENT_TYPES = frozenset({"BEGIN", "INSERT", "UPDATE", "DELETE", "COMMIT", "ABORT"})
_ROW_EVENTS = frozenset({"INSERT", "UPDATE", "DELETE"})


def validate_event(event: Any, field_map: dict[str, str]) -> dict:
    """校验一条日志事件并返回规范化结构。

    只做单事件的结构/类型校验；事务顺序、重复载荷等跨事件规则由
    :mod:`cdc.log_ingest` 负责。
    """
    if not isinstance(event, dict):
        raise ProtocolError(f"日志事件必须是对象，实际为: {type(event).__name__}")
    etype = event.get("event")
    if etype not in EVENT_TYPES:
        raise ProtocolError(f"未知事件类型: {etype!r}")
    txn = event.get("txn_id")
    if not isinstance(txn, str) or not txn:
        raise ProtocolError(f"{etype} 事件缺少非空 txn_id: {event!r}")

    out: dict[str, Any] = {"event": etype, "txn_id": txn}

    if etype in _ROW_EVENTS or etype == "COMMIT" or etype == "ABORT":
        seq = event.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq <= 0:
            raise ProtocolError(f"{etype} 事件的 seq 必须是正整数: {event!r}")
        out["seq"] = seq

    if etype == "INSERT":
        out["row"] = validate_row(event.get("row"), field_map)
    elif etype == "DELETE":
        out["device_id"] = _check_pk(event)
    elif etype == "UPDATE":
        before = event.get("before")
        after = event.get("after")
        out["before"] = validate_row(before, field_map)
        out["after"] = validate_row(after, field_map)
        if out["before"]["device_id"] == out["after"]["device_id"]:
            out["old_pk"] = out["before"]["device_id"]
            out["new_pk"] = out["after"]["device_id"]
        else:
            # 允许更换设备编号（主键迁移）。
            out["old_pk"] = out["before"]["device_id"]
            out["new_pk"] = out["after"]["device_id"]
    elif etype == "COMMIT":
        lsn = event.get("commit_lsn")
        if not isinstance(lsn, int) or isinstance(lsn, bool) or lsn < 0:
            raise ProtocolError(f"COMMIT 事件的 commit_lsn 必须是非负整数: {event!r}")
        out["commit_lsn"] = lsn
    return out


def _check_pk(event: dict) -> str:
    pk = event.get("device_id")
    if not isinstance(pk, str) or not pk:
        raise ProtocolError(f"DELETE 事件缺少非空 device_id: {event!r}")
    return pk


def event_identity(txn_id: str, seq: int | None) -> str:
    """事件身份：事务标识 + 事务内序号。BEGIN 没有序号，单独命名。"""
    return f"{txn_id}#{'BEGIN' if seq is None else seq}"
