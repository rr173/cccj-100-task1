#!/usr/bin/env python3
"""采样覆盖审计层确定性样例生成器（无随机数）。

在输出目录中准备四类原料，供 demo_coverage.py 端到端演示：

loader/      最小上游快照（设备 DA DB DC）
entity/      归并对照资料（g1 合并 DA-DB -> 分组版本 2；v1 为各自独立）
reading/     两个读数批次：b1 基础、b2 更正/撤回/迟来补洞
coverage/    契约批次 c1..c5、豁免批次 e1、以及“同稿篡改/新稿停用”拒收样本

场景索引（详细断言见 demo_coverage.py）
========================================

* C1 Asia/Shanghai 普通小时班次：读数条数足够但集中同一时刻仍有缺采、
  设备别名 DB/DA 共同满足同一实体计划、超容差读数不能补洞；
* E1/E2 两段相交豁免：重叠位置只扣一次（保留两个豁免编号）并把缺口
  切成两段；豁免终点边界 13:00 不豁免；
* b2 把读数 r2 改时间、撤回 r3、再补一个 15:00 的新观测 -> 覆盖随
  稿件选版变化；
* C2/C3 America/New_York 春季跳时（02:00 不产生应到项）与秋季回拨
  （01:00 两个真实时刻各一项，分别记录 -05:00/-04:00 偏移）；
* C4 UTC 跨午夜 22:00-02:00 班次，拆成两个明确日段；
* C5 生效区间完全落在查询窗口外 -> 零应到（分母为零单独说明）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEVICES = ["DA", "DB", "DC"]
METRIC = "vibration_rms"
ORIGIN = "plant-a"


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


def reading(key: str, edition: int, device: str, occurred_at: str,
            amount: str = "1.0", withdrawn: bool = False) -> dict:
    return {
        "origin": ORIGIN,
        "reading_key": key,
        "edition": edition,
        "device": device,
        "occurred_at": occurred_at,
        "metric": METRIC,
        "amount": amount,
        "withdrawn": withdrawn,
    }


def relation(relation_id: str, revision: int, a: str, b: str, effective: bool) -> dict:
    return {
        "source": "maintenance",
        "relation_id": relation_id,
        "revision": revision,
        "endpoint_a": a,
        "endpoint_b": b,
        "category": "same_entity",
        "effective": effective,
    }


def contract(contract_no: str, draft_no: int, entity_id: str,
             eff_start: str, eff_end: str, tz_name: str, weekdays: list[int],
             segments: list[dict], step: int, tolerance: int,
             active: bool = True) -> dict:
    return {
        "contract_no": contract_no,
        "draft_no": draft_no,
        "active": active,
        "origin": ORIGIN,
        "entity_id": entity_id,
        "metric": METRIC,
        "effective_start": eff_start,
        "effective_end": eff_end,
        "timezone": tz_name,
        "weekdays": weekdays,
        "segments": segments,
        "step_seconds": step,
        "tolerance_seconds": tolerance,
    }


def exemption(ex_id: str, contract_no: str, start: str, end: str,
              reason: str) -> dict:
    return {
        "exemption_id": ex_id,
        "contract_no": contract_no,
        "start": start,
        "end": end,
        "reason": reason,
    }


# ---------------------------------------------------------------------------
# 读数批次
# ---------------------------------------------------------------------------

# b1 基础批次（审计截止 1 使用）
B1_BASE = [
    # 同一时刻 08:00 两条：item 08:00 只覆盖一次，另一条计重复上报
    reading("r1", 1, "DA", "2026-11-12T08:00:00+08:00"),
    reading("r2", 1, "DA", "2026-11-12T08:00:00+08:00", "2.5"),
    # 别名设备 DB 与 DA 在成员版本 2 同属 E-DA：09:00 由 DB 满足
    reading("r3", 1, "DB", "2026-11-12T09:00:00+08:00"),
    # 容差边界：07:55 距 08:00 恰好 300 秒 = tolerance，允许补 08:00
    reading("r_early", 1, "DA", "2026-11-12T07:55:00+08:00"),
    # 超容差：10:45 距最近计划时刻 10:00/11:00 均 > 300 秒，不能补洞
    reading("r_far", 1, "DA", "2026-11-12T10:45:00+08:00"),
    # 豁免终点边界：13:00 恰为 E2 终点（右开），不豁免，正常覆盖 13:00 项
    reading("r_exedge", 1, "DA", "2026-11-12T13:00:00+08:00"),
    # 未知设备 DC 在成员版本 2 中自成 E-DC，无契约 -> 未匹配
    reading("r_unknown", 1, "DC", "2026-11-12T10:00:00+08:00"),
    # 春季跳时：覆盖 03:00；另有一条落在不存在的 02:00（无法对应应到项）
    reading("r_spring_03", 1, "DA", "2026-03-08T03:00:00-04:00"),
    reading("r_spring_gap", 1, "DA", "2026-03-08T02:30:00-05:00"),
    # 秋季回拨：01:00 两个真实时刻各来一条（偏移不同）
    reading("r_fall_01a", 1, "DA", "2026-11-01T01:00:00-04:00"),
    reading("r_fall_01b", 1, "DA", "2026-11-01T01:00:00-05:00"),
    # 跨午夜班次：覆盖 22:00
    reading("r_night_22", 1, "DA", "2026-11-12T22:00:00+00:00"),
]

# b2 更正批次（审计截止 2 使用）：改时间、撤回、迟来补洞
B2_CORRECT = [
    # r2 改到 10:00（原 08:00 的重复消失，10:00 获得覆盖）
    reading("r2", 2, "DA", "2026-11-12T10:00:00+08:00", "2.5"),
    # r3 撤回：09:00 失去覆盖（r3 最新稿不参与选版）
    reading("r3", 2, "DB", "2026-11-12T09:00:00+08:00", withdrawn=True),
    # 迟来的新观测补 15:00
    reading("r_late", 1, "DB", "2026-11-12T15:00:00+08:00"),
]

G1_MERGE = [relation("M100", 1, "DA", "DB", True)]


# ---------------------------------------------------------------------------
# 契约批次
# ---------------------------------------------------------------------------

# c1：C1 普通班次（上海，小时步长，容差 5 分钟）
C1_DRAFT1 = [
    contract(
        "C1", 1, "E-DA",
        "2026-11-11T16:00:00Z", "2026-11-12T16:00:00Z",
        "Asia/Shanghai", [1, 2, 3, 4, 5, 6, 7],
        [{"start": "08:00", "end": "20:00"}], 3600, 300,
    )
]

# c1b：C1 新稿号修订日程（停用演示 + 日程修订演示），稿号 2 先停用
C1_DRAFT2_DISABLE = [
    contract(
        "C1", 2, "E-DA",
        "2026-11-11T16:00:00Z", "2026-11-12T16:00:00Z",
        "Asia/Shanghai", [1, 2, 3, 4, 5, 6, 7],
        [{"start": "08:00", "end": "20:00"}], 3600, 300, active=False,
    )
]

# c1c：同稿号 1 换内容（必须整批拒收 DraftConflict）
C1_DRAFT1_TAMPERED = [
    contract(
        "C1", 1, "E-DA",
        "2026-11-11T16:00:00Z", "2026-11-12T16:00:00Z",
        "Asia/Shanghai", [1, 2, 3, 4, 5, 6, 7],
        [{"start": "09:00", "end": "20:00"}], 3600, 300,  # 起点被改
    )
]

# c2：春季跳时契约（纽约，2026-03-08 全天小时段；02:00 为 gap）
C2_SPRING = [
    contract(
        "C2", 1, "E-DA",
        "2026-03-08T05:00:00Z", "2026-03-08T10:00:00Z",
        "America/New_York", [7],
        [{"start": "00:00", "end": "05:00"}], 3600, 300,
    )
]

# c3：秋季回拨契约（纽约，2026-11-01；01:00 两个真实时刻）
C3_FALL = [
    contract(
        "C3", 1, "E-DA",
        "2026-11-01T04:00:00Z", "2026-11-01T08:00:00Z",
        "America/New_York", [7],
        [{"start": "00:00", "end": "03:00"}], 3600, 300,
    )
]

# c4：跨午夜班次（UTC，22:00-02:00，两小时一步；锚定各日段起点）
C4_NIGHT = [
    contract(
        "C4", 1, "E-DA",
        "2026-11-12T22:00:00Z", "2026-11-13T02:00:00Z",
        "UTC", [4, 5],
        [{"start": "22:00", "end": "02:00"}], 7200, 300,
    )
]

# c5：生效区间完全在查询窗口之外 -> 零应到
C5_OUTSIDE = [
    contract(
        "C5", 1, "E-DA",
        "2026-12-01T00:00:00Z", "2026-12-02T00:00:00Z",
        "UTC", [1, 2, 3, 4, 5, 6, 7],
        [{"start": "00:00", "end": "06:00"}], 3600, 300,
    )
]

# c6：生效终点边界契约——计划时刻恰好落在生效终点（右开）不得成为应到项
C6_EFFEDGE = [
    contract(
        "C6", 1, "E-DA",
        "2026-11-12T00:00:00Z", "2026-11-12T02:00:00Z",
        "UTC", [4],
        [{"start": "00:00", "end": "03:00"}], 3600, 300,
    )
]

# ---------------------------------------------------------------------------
# 豁免批次（两段相交/包含，按并集扣除）
# ---------------------------------------------------------------------------

E1_OVERLAP = [
    # 11:00（含）-12:30（不含）
    exemption("E1", "C1", "2026-11-12T03:00:00Z", "2026-11-12T04:30:00Z",
             "计划停机检修 A"),
    # 12:00（含）-13:00（不含），与 E1 在 12:00-12:30 相交
    exemption("E2", "C1", "2026-11-12T04:00:00Z", "2026-11-12T05:00:00Z",
             "计划停机检修 B（与 A 重叠）"),
]


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
            {"file": "shard_0.jsonl", "rows": len(rows),
             "sha256": _sha256_file(shard_path)}
        ],
    }
    with open(os.path.join(loader_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)


def generate(out_dir: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    _write_loader_snapshot(out_dir)
    _write_jsonl(os.path.join(out_dir, "entity", "g1_merge.jsonl"), G1_MERGE)

    for name, objs in {
        "b1_base.jsonl": B1_BASE,
        "b2_correct.jsonl": B2_CORRECT,
    }.items():
        _write_jsonl(os.path.join(out_dir, "reading", name), objs)

    for name, objs in {
        "c1_draft1.jsonl": C1_DRAFT1,
        "c1_draft2_disable.jsonl": C1_DRAFT2_DISABLE,
        "c1_draft1_tampered.jsonl": C1_DRAFT1_TAMPERED,
        "c2_spring.jsonl": C2_SPRING,
        "c3_fall.jsonl": C3_FALL,
        "c4_night.jsonl": C4_NIGHT,
        "c5_outside.jsonl": C5_OUTSIDE,
        "c6_effedge.jsonl": C6_EFFEDGE,
        "e1_overlap.jsonl": E1_OVERLAP,
    }.items():
        _write_jsonl(os.path.join(out_dir, "coverage", name), objs)

    return {
        "devices": DEVICES,
        "contract_batches": [
            "c1_draft1.jsonl", "c1_draft2_disable.jsonl", "c1_draft1_tampered.jsonl",
            "c2_spring.jsonl", "c3_fall.jsonl", "c4_night.jsonl",
            "c5_outside.jsonl", "c6_effedge.jsonl",
        ],
        "exemption_batches": ["e1_overlap.jsonl"],
        "reading_batches": ["b1_base.jsonl", "b2_correct.jsonl"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="生成采样覆盖审计演示样例")
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
