"""独立参考实现（只读、不依赖装载器其它模块）。

用最直白的字典/列表方式重新解释一遍快照与日志：

1. 校验并装载全部快照分片，得到 ``device_id -> 行`` 的字典；
2. 顺序扫描所有日志，按事务收集事件，做与装载器相同的身份去重、
   载荷冲突、序号递增、提交位置递增检查；
3. 只保留 ``commit_lsn > 切点 S`` 的已提交事务，按 commit_lsn 升序、
   事务内按 seq 升序回放；
4. 输出最终行集与检查点（最后应用的 commit_lsn，无事务时为 S）。

该模块不写 SQLite，不 import 装载器的任何其它子模块，便于在验证时
作为“第二套大脑”交叉核对。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .errors import PayloadConflict, ProtocolError


def _read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            text = line.strip()
            if text:
                try:
                    yield lineno, json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ProtocolError(f"{path}:{lineno} JSON 解析失败: {exc.msg}")


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _canon(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass
class RefTxn:
    txn_id: str
    events: dict[int | None, dict] = field(default_factory=dict)
    state: str = "OPEN"  # OPEN / COMMITTED / ABORTED
    commit_lsn: int | None = None


class ReferenceRunner:
    def __init__(self, manifest_path: str, shard_paths: list[str], log_paths: list[str]):
        self.manifest_path = manifest_path
        self.shard_paths = shard_paths
        self.log_paths = log_paths

    # ------------------------------------------------------------------
    def _load_manifest(self) -> dict:
        manifest = _read_json(self.manifest_path)
        if not isinstance(manifest, dict) or not isinstance(
            manifest.get("cut_point_lsn"), int
        ):
            raise ProtocolError("清单缺少 cut_point_lsn")
        fields = manifest.get("fields")
        if not isinstance(fields, list) or not fields:
            raise ProtocolError("清单 fields 非法")
        names = [f["name"] for f in fields]
        if "device_id" not in names:
            raise ProtocolError("字段中必须包含 device_id")
        if not isinstance(manifest.get("shards"), list) or not manifest["shards"]:
            raise ProtocolError("清单 shards 非法")
        self.field_names = names
        self.field_set = set(names)
        return manifest

    def _check_row(self, row: Any) -> dict:
        if not isinstance(row, dict) or set(row) != self.field_set:
            raise ProtocolError(f"行字段集合与清单不符: {row!r}")
        if not isinstance(row.get("device_id"), str) or not row["device_id"]:
            raise ProtocolError("device_id 必须是非空字符串")
        return row

    def _load_snapshot(self, manifest: dict) -> dict[str, dict]:
        by_name = {s["file"]: s for s in manifest["shards"]}
        rows: dict[str, dict] = {}
        used: set[str] = set()
        for path in self.shard_paths:
            import os

            fname = os.path.basename(path)
            if fname not in by_name:
                raise ProtocolError(f"分片不在清单中: {fname}")
            entry = by_name[fname]
            if _sha256(path) != entry["sha256"]:
                raise ProtocolError(f"分片 {fname} 摘要不符")
            count = 0
            for _, raw in _iter_jsonl(path):
                row = self._check_row(raw)
                if row["device_id"] in rows:
                    raise ProtocolError(f"设备编号重复: {row['device_id']}")
                rows[row["device_id"]] = row
                count += 1
            if count != entry["rows"]:
                raise ProtocolError(f"分片 {fname} 行数不符")
            used.add(fname)
        missing = set(by_name) - used
        if missing:
            raise ProtocolError(f"缺少分片: {sorted(missing)}")
        return rows

    # ------------------------------------------------------------------
    def _load_logs(self) -> dict[str, RefTxn]:
        txns: dict[str, RefTxn] = {}
        max_commit_lsn: int | None = None
        for path in self.log_paths:
            for lineno, event in _iter_jsonl(path):
                if not isinstance(event, dict) or "event" not in event or "txn_id" not in event:
                    raise ProtocolError(f"{path}:{lineno} 事件结构非法")
                txn_id = event["txn_id"]
                etype = event["event"]
                seq = event.get("seq") if etype != "BEGIN" else None
                txn = txns.get(txn_id)
                if etype == "BEGIN":
                    if txn is not None:
                        self._conflict_check(txn, seq, event, txn_id)
                        continue
                    txn = RefTxn(txn_id)
                    txns[txn_id] = txn
                else:
                    if txn is None:
                        raise ProtocolError(f"{txn_id} 缺少 BEGIN 就出现 {etype}")
                    if txn.state != "OPEN":
                        raise ProtocolError(f"事务 {txn_id} 终态后又出现事件")
                self._conflict_check(txn, seq, event, txn_id)
                if seq is None:
                    txn.events[None] = event
                    continue
                prev = max((s for s in txn.events if s is not None), default=None)
                if prev is not None and seq <= prev:
                    raise ProtocolError(f"事务 {txn_id} 序号未递增")
                txn.events[seq] = event
                if etype == "COMMIT":
                    lsn = event["commit_lsn"]
                    if not isinstance(lsn, int) or isinstance(lsn, bool):
                        raise ProtocolError("commit_lsn 必须是整数")
                    if max_commit_lsn is not None and lsn <= max_commit_lsn:
                        raise ProtocolError("首次出现的提交位置未递增")
                    max_commit_lsn = lsn
                    txn.state = "COMMITTED"
                    txn.commit_lsn = lsn
                elif etype == "ABORT":
                    txn.state = "ABORTED"
        return txns

    @staticmethod
    def _conflict_check(txn: RefTxn, seq: int | None, event: dict, txn_id: str) -> None:
        identity = f"{txn_id}#{'BEGIN' if seq is None else seq}"
        if seq in txn.events and _canon(txn.events[seq]) != _canon(event):
            raise PayloadConflict(f"事件身份 {identity} 载荷冲突")

    # ------------------------------------------------------------------
    def _apply_event(self, rows: dict[str, dict], event: dict) -> None:
        etype = event["event"]
        if etype == "INSERT":
            row = self._check_row(event["row"])
            if row["device_id"] in rows:
                raise ProtocolError(f"INSERT 设备已存在: {row['device_id']}")
            rows[row["device_id"]] = row
        elif etype == "DELETE":
            pk = event["device_id"]
            if pk not in rows:
                raise ProtocolError(f"DELETE 设备不存在: {pk}")
            del rows[pk]
        elif etype == "UPDATE":
            before = self._check_row(event["before"])
            after = self._check_row(event["after"])
            old_pk, new_pk = before["device_id"], after["device_id"]
            if old_pk not in rows:
                raise ProtocolError(f"UPDATE 设备不存在: {old_pk}")
            if rows[old_pk] != before:
                raise ProtocolError(f"UPDATE 前镜像不匹配: {old_pk}")
            if old_pk != new_pk and new_pk in rows:
                raise ProtocolError(f"主键迁移目标已存在: {new_pk}")
            if old_pk != new_pk:
                del rows[old_pk]
            rows[new_pk] = after
        else:
            raise ProtocolError(f"参考实现收到非行事件: {etype}")

    def run(self) -> dict:
        manifest = self._load_manifest()
        cut = manifest["cut_point_lsn"]
        rows = self._load_snapshot(manifest)
        txns = self._load_logs()

        committed = sorted(
            (t for t in txns.values() if t.state == "COMMITTED" and t.commit_lsn > cut),
            key=lambda t: t.commit_lsn,
        )
        checkpoint = cut
        applied_txns: list[str] = []
        for txn in committed:
            for seq in sorted(s for s in txn.events if s is not None):
                event = txn.events[seq]
                if event["event"] in ("COMMIT", "ABORT"):
                    continue
                self._apply_event(rows, event)
            checkpoint = txn.commit_lsn  # type: ignore[assignment]
            applied_txns.append(txn.txn_id)

        return {
            "cut_point_lsn": cut,
            "checkpoint_lsn": checkpoint,
            "rows": rows,
            "applied_txn_ids": applied_txns,
            "open_txn_ids": sorted(t.txn_id for t in txns.values() if t.state == "OPEN"),
            "aborted_txn_ids": sorted(
                t.txn_id for t in txns.values() if t.state == "ABORTED"
            ),
        }


def run_reference(
    manifest_path: str, shard_paths: list[str], log_paths: list[str]
) -> dict:
    """便捷函数：返回参考实现的最终行集与检查点。"""
    return ReferenceRunner(manifest_path, shard_paths, log_paths).run()
