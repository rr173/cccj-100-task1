#!/usr/bin/env python3
"""读数事实仓确定性样例生成器（无随机数）。

生成端到端演示所需的全部原料到一个输出目录：

上游装载器（最小快照，设备 DA DB DC DD DE）
    loader/manifest.json
    loader/shard_0.jsonl

归并层对照资料批次（演示“链归并后又拆开”）
    entity/g1_merge.jsonl     DA-DB 同一实体 -> 分组版本 2（合并）
    entity/g2_split.jsonl     撤销该关系      -> 分组版本 3（拆开）

读数逐行资料批次
    reading/b1_base.jsonl     基础振动读数（含 r1 最小、r2 最大）
    reading/b2_unknown.jsonl  未知设备 DZ 的读数（触发待归属清单）
    reading/b3_decimal.jsonl  带时差的午夜读数 + 十进制 0.1/0.2
    reading/b4_correct.jsonl  跨天/跨设备更正 r1 + 撤回当前最大值 r2
    reading/b5_late.jsonl     截止后才到的新批次（验证截止范围保持）

时间口径：r_tz 给 2026-11-12T00:30:00+08:00（UTC 为 2026-11-11T16:30），
用于证明按 UTC 确定日期；十进制 0.1 + 0.2 必须精确等于 0.3。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEVICES = ["DA", "DB", "DC", "DD", "DE"]
METRIC = "vibration_rms"
D1 = "2026-11-11"
D2 = "2026-11-12"


def _write_jsonl(path: str, objects: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for obj in objects:
            fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        h.update(fh.read())
    return h.hexdigest()


def reading(origin: str, key: str, edition: int, device: str,
            occurred_at: str, metric: str, amount: str,
            withdrawn: bool = False) -> dict:
    return {
        "origin": origin,
        "reading_key": key,
        "edition": edition,
        "device": device,
        "occurred_at": occurred_at,
        "metric": metric,
        "amount": amount,
        "withdrawn": withdrawn,
    }


def relation(source: str, relation_id: str, revision: int, a: str, b: str,
             category: str, effective: bool) -> dict:
    return {
        "source": source,
        "relation_id": relation_id,
        "revision": revision,
        "endpoint_a": a,
        "endpoint_b": b,
        "category": category,
        "effective": effective,
    }


# ---------------------------------------------------------------------------
# 读数批次
# ---------------------------------------------------------------------------

# b1：基础读数。r1=1.0 是 E-DA/D1 格子的最小，r2=9.0 是最大。
B1_BASE = [
    reading("plant-a", "r1", 1, "DA", f"{D1}T08:00:00+00:00", METRIC, "1.0"),
    reading("plant-a", "r2", 1, "DA", f"{D1}T09:00:00+00:00", METRIC, "9.0"),
    reading("plant-a", "r3", 1, "DA", f"{D1}T10:00:00+00:00", METRIC, "5.0"),
    reading("plant-b", "r4", 1, "DB", f"{D1}T08:30:00+00:00", METRIC, "2.0"),
    reading("plant-b", "r5", 1, "DC", f"{D2}T08:30:00+00:00", METRIC, "7.0"),
]

# b2：设备 DZ 不在任何分组版本成员中 -> 待归属。
B2_UNKNOWN = [
    reading("plant-a", "r6", 1, "DZ", f"{D2}T11:00:00+00:00", METRIC, "4.0"),
]

# b3：午夜 +08:00 读数（UTC 落在前一天），以及精确十进制 0.1 / 0.2。
B3_DECIMAL = [
    reading("plant-a", "r_tz", 1, "DA", "2026-11-12T00:30:00+08:00", METRIC, "0.1"),
    reading("plant-a", "r_dec", 1, "DA", f"{D1}T18:00:00+00:00", METRIC, "0.2"),
]

# b4：把当前最小 r1 改到次日的 DB；撤回当前最大 r2。
B4_CORRECT = [
    reading("plant-a", "r1", 2, "DB", f"{D2}T07:15:00+00:00", METRIC, "1.0"),
    reading("plant-a", "r2", 2, "DA", f"{D1}T09:00:00+00:00", METRIC, "9.0",
            withdrawn=True),
]

# b5：声明截止批次之后才收到的新稿件（截止范围一致性测试用）。
B5_LATE = [
    reading("plant-a", "r_late", 1, "DA", f"{D2}T12:00:00+00:00", METRIC, "3.0"),
]

G1_MERGE = [relation("maintenance", "M100", 1, "DA", "DB", "same_entity", True)]
G2_SPLIT = [relation("maintenance", "M100", 2, "DA", "DB", "same_entity", False)]


def _write_loader_snapshot(out_dir: str) -> None:
    loader_dir = os.path.join(out_dir, "loader")
    os.makedirs(loader_dir, exist_ok=True)
    fields = [
        {"name": "device_id", "type": "TEXT"},
        {"name": "vendor", "type": "TEXT"},
    ]
    rows = [{"device_id": d, "vendor": "Acme"} for d in DEVICES]
    shard_path = os.path.join(loader_dir, "shard_0.jsonl")
    with open(shard_path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    manifest = {
        "cut_point_lsn": 1000,
        "fields": fields,
        "shards": [
            {
                "file": "shard_0.jsonl",
                "rows": len(rows),
                "sha256": _sha256_file(shard_path),
            }
        ],
    }
    with open(os.path.join(loader_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)


def generate(out_dir: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    _write_loader_snapshot(out_dir)

    _write_jsonl(os.path.join(out_dir, "entity", "g1_merge.jsonl"), G1_MERGE)
    _write_jsonl(os.path.join(out_dir, "entity", "g2_split.jsonl"), G2_SPLIT)

    reading_batches = {
        "b1_base.jsonl": B1_BASE,
        "b2_unknown.jsonl": B2_UNKNOWN,
        "b3_decimal.jsonl": B3_DECIMAL,
        "b4_correct.jsonl": B4_CORRECT,
        "b5_late.jsonl": B5_LATE,
    }
    for name, objs in reading_batches.items():
        _write_jsonl(os.path.join(out_dir, "reading", name), objs)

    return {
        "devices": DEVICES,
        "reading_batches": list(reading_batches),
        "grouping_versions": {"split_v1": 1, "merged_v2": 2, "split_v3": 3},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="生成读数事实仓演示样例")
    parser.add_argument("out_dir", help="输出目录（必须为空或不存在）")
    args = parser.parse_args()
    if os.path.exists(args.out_dir) and os.listdir(args.out_dir):
        parser.error(f"输出目录非空: {args.out_dir}")
    info = generate(args.out_dir)
    print(json.dumps({"status": "generated", "out_dir": args.out_dir, **info},
                     ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
