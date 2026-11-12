#!/usr/bin/env python3
"""样例数据生成器（确定性，无随机数）。

生成一套自洽的供应商交付物到输出目录：

    manifest.json            快照清单（切点 S、字段定义、分片行数与 SHA-256）
    shard_0.jsonl ...        快照分片（生成后可乱序投喂）
    log_1.jsonl / log_2.jsonl 切点两侧交错的事务日志
    log_replay.jsonl         已提交旧日志的重传（含一条损坏重传样本的原料）
    conflict_event.jsonl     与 log_1 中事件身份相同但载荷不同的冲突事件
    expected.json            该数据集在无故障情况下的期望最终行集与检查点

设备主数据固定字段：
    device_id   TEXT  主键（设备编号）
    vendor      TEXT  供应商
    model       TEXT  型号
    firmware    TEXT  固件版本
    temperature REAL  最近测点温度（可为 NULL）
    cycle_count INTEGER 循环次数
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

# 让脚本在 examples/ 下直接运行时也能 import 到项目包。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc.reference import run_reference  # noqa: E402

CUT_POINT_LSN = 2000

FIELDS = [
    {"name": "device_id", "type": "TEXT"},
    {"name": "vendor", "type": "TEXT"},
    {"name": "model", "type": "TEXT"},
    {"name": "firmware", "type": "TEXT"},
    {"name": "temperature", "type": "REAL"},
    {"name": "cycle_count", "type": "INTEGER"},
]


def row(device_id: str, vendor: str, model: str, firmware: str,
        temperature, cycle_count: int) -> dict:
    return {
        "device_id": device_id,
        "vendor": vendor,
        "model": model,
        "firmware": firmware,
        "temperature": temperature,
        "cycle_count": cycle_count,
    }


# 快照内容：T10 在切点前的更新（firmware 1.4.0 -> 1.4.1）已包含在内。
SNAPSHOT_ROWS = [
    row("D001", "Acme", "X100", "2.1.0", 36.5, 120),
    row("D002", "Acme", "X100", "2.1.0", 37.1, 132),
    row("D003", "Beta", "M200", "1.0.7", None, 9),
    row("D004", "Beta", "M200", "1.0.8", 41.2, 540),
    row("D005", "Cobalt", "S50", "3.0.0", 29.8, 980),
    row("D006", "Cobalt", "S50", "3.0.1", 30.0, 1011),
    row("D007", "Acme", "X200", "0.9.0", 33.3, 15),
    row("D008", "Beta", "M210", "1.2.0", 38.6, 233),
    row("D009", "Cobalt", "S51", "3.1.0", 28.4, 410),
    # T10（commit_lsn=1900 <= S）的更新结果已在快照中。
    row("D010", "Acme", "X100", "1.4.1", 35.9, 77),
    row("D011", "Beta", "M200", "1.0.8", 40.4, 612),
    row("D012", "Cobalt", "S50", "3.0.1", None, 3),
]

SHARD_SPLITS = [("shard_0.jsonl", 4), ("shard_1.jsonl", 4), ("shard_2.jsonl", 4)]


def write_jsonl(path: str, objects: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for obj in objects:
            fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        h.update(fh.read())
    return h.hexdigest()


def build_logs() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """构造日志，返回 (log_1, log_2, replay, conflict)。"""
    # ---- log_1：切点两侧事务交错；T10 在 S 前提交，T11 在 S 后提交 ----
    log1: list[dict] = []
    log1.append({"event": "BEGIN", "txn_id": "T10"})
    log1.append({"event": "BEGIN", "txn_id": "T11"})  # 两事务交错
    log1.append({
        "event": "UPDATE", "txn_id": "T10", "seq": 1,
        "before": row("D010", "Acme", "X100", "1.4.0", 35.9, 77),
        "after": row("D010", "Acme", "X100", "1.4.1", 35.9, 77),
    })
    log1.append({
        "event": "INSERT", "txn_id": "T11", "seq": 1,
        "row": row("D013", "Delta", "L9", "0.1.0", 25.0, 1),
    })
    log1.append({"event": "COMMIT", "txn_id": "T10", "seq": 2, "commit_lsn": 1900})
    log1.append({"event": "COMMIT", "txn_id": "T11", "seq": 2, "commit_lsn": 2100})

    # ---- log_2：主键迁移、同批先改后删、未提交尾部、删除、中止 ----
    log2: list[dict] = []
    log2.append({"event": "BEGIN", "txn_id": "T12"})
    log2.append({"event": "BEGIN", "txn_id": "T13"})
    log2.append({"event": "BEGIN", "txn_id": "T14"})
    log2.append({"event": "BEGIN", "txn_id": "T15"})  # 只有 BEGIN 的事务
    # T12：主键迁移 D005 -> D050
    log2.append({
        "event": "UPDATE", "txn_id": "T12", "seq": 1,
        "before": row("D005", "Cobalt", "S50", "3.0.0", 29.8, 980),
        "after": row("D050", "Cobalt", "S50", "3.0.0", 29.8, 980),
    })
    # T13：同事务先插入后删除（同批先改后删的一种形态），净效果为零
    log2.append({
        "event": "INSERT", "txn_id": "T13", "seq": 1,
        "row": row("D014", "Delta", "L9", "0.1.0", 26.1, 2),
    })
    log2.append({"event": "DELETE", "txn_id": "T13", "seq": 2, "device_id": "D014"})
    # T14：删除 D003
    log2.append({"event": "DELETE", "txn_id": "T14", "seq": 1, "device_id": "D003"})
    log2.append({"event": "COMMIT", "txn_id": "T12", "seq": 2, "commit_lsn": 2200})
    log2.append({"event": "COMMIT", "txn_id": "T13", "seq": 3, "commit_lsn": 2300})
    log2.append({"event": "ABORT", "txn_id": "T14", "seq": 2})
    # 未提交事务尾部：T15 永远没有 COMMIT/ABORT。

    # ---- 重传：旧日志再次送入，事件身份与载荷完全一致（无副作用）----
    replay = [
        {"event": "BEGIN", "txn_id": "T11"},
        {
            "event": "INSERT", "txn_id": "T11", "seq": 1,
            "row": row("D013", "Delta", "L9", "0.1.0", 25.0, 1),
        },
        {"event": "COMMIT", "txn_id": "T11", "seq": 2, "commit_lsn": 2100},
    ]

    # ---- 冲突：同一事件身份 T11#1，但温度载荷不同 ----
    conflict = [
        {
            "event": "INSERT", "txn_id": "T11", "seq": 1,
            "row": row("D013", "Delta", "L9", "0.1.0", 99.9, 1),
        }
    ]
    return log1, log2, replay, conflict


def generate(out_dir: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    if os.listdir(out_dir):
        raise SystemExit(f"输出目录非空，拒绝覆盖: {out_dir}")

    # 分片
    shard_entries = []
    offset = 0
    for fname, count in SHARD_SPLITS:
        part = SNAPSHOT_ROWS[offset:offset + count]
        path = os.path.join(out_dir, fname)
        write_jsonl(path, part)
        shard_entries.append(
            {"file": fname, "rows": count, "sha256": sha256_of(path)}
        )
        offset += count

    manifest = {
        "cut_point_lsn": CUT_POINT_LSN,
        "fields": FIELDS,
        "shards": shard_entries,
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")

    log1, log2, replay, conflict = build_logs()
    write_jsonl(os.path.join(out_dir, "log_1.jsonl"), log1)
    write_jsonl(os.path.join(out_dir, "log_2.jsonl"), log2)
    write_jsonl(os.path.join(out_dir, "log_replay.jsonl"), replay)
    write_jsonl(os.path.join(out_dir, "conflict_event.jsonl"), conflict)

    # 用独立参考实现直接算出期望结果，避免手工维护。
    ref = run_reference(
        os.path.join(out_dir, "manifest.json"),
        [os.path.join(out_dir, f) for f, _ in SHARD_SPLITS],
        [os.path.join(out_dir, "log_1.jsonl"), os.path.join(out_dir, "log_2.jsonl")],
    )
    expected = {
        "cut_point_lsn": ref["cut_point_lsn"],
        "checkpoint_lsn": ref["checkpoint_lsn"],
        "applied_txn_ids": ref["applied_txn_ids"],
        "open_txn_ids": ref["open_txn_ids"],
        "aborted_txn_ids": ref["aborted_txn_ids"],
        "rows": [ref["rows"][pk] for pk in sorted(ref["rows"])],
    }
    with open(os.path.join(out_dir, "expected.json"), "w", encoding="utf-8") as fh:
        json.dump(expected, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    return expected


def main() -> int:
    ap = argparse.ArgumentParser(description="生成离线 CDC 样例数据")
    ap.add_argument("out_dir", help="输出目录（必须为空或不存在）")
    args = ap.parse_args()
    expected = generate(args.out_dir)
    print(
        f"已生成样例到 {args.out_dir}：期望 {len(expected['rows'])} 行，"
        f"检查点 {expected['checkpoint_lsn']}，应用事务 {expected['applied_txn_ids']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
