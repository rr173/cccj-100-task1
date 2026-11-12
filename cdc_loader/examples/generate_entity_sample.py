#!/usr/bin/env python3
"""实体归并层确定性样例生成器（无随机数）。

配合装载器样例（examples/generate_sample.py）使用：装载器样例跑完
快照 + log_1/log_2 后，device_master 中的设备为
D001 D002 D003 D004 D006 D007 D008 D009 D010 D011 D012 D013 D050。

本生成器输出对照资料批次与上游变更日志：

    b1_init.jsonl             四对基础“同一实体”
    b2_chain_conflict.jsonl   两条独立连接构成链 + 跨端“必须不同”（应拒绝）
    b3_chain_ok.jsonl         修正后的链 + 归并桥边（b2 的重投修正版）
    b4_split_merge.jsonl      同批撤销桥边 + 新增连接 => 拆分与合并
    b5_tamper.jsonl           已被取代的旧修订内容被改写（应拒绝）
    b5r_replay_and_new.jsonl  旧修订真实重传（一致）+ 一条新约束
    b6_two_sources.jsonl      两个来源对同一对端点提供依据
    b7_revoke_one.jsonl       撤销其中一条，另一条仍然有效
    b8_crash.jsonl            用于崩溃注入演示的批次
    upstream_del_d008.jsonl   装载器日志：删除 D008（演示上游删除）
    upstream_ins_d008.jsonl   装载器日志：重新插入 D008（演示重现）
    scenario.json             逐步骤的期望结果（供演示脚本对照）
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def rel(source: str, relation_id: str, revision: int, a: str, b: str,
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


def write_jsonl(path: str, objects: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for obj in objects:
            fh.write(json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n")


# ---------------------------------------------------------------------------
# 对照资料批次
# ---------------------------------------------------------------------------

B1_INIT = [
    rel("procurement", "R001", 1, "D001", "D006", "same_entity", True),
    rel("maintenance", "R101", 1, "D002", "D007", "same_entity", True),
    rel("maintenance", "R102", 1, "D003", "D008", "same_entity", True),
    rel("supplier", "R201", 1, "D004", "D009", "same_entity", True),
]

# 两条独立连接构成链 D010-D011-D012，再叠加跨端“必须不同”=> 矛盾。
B2_CHAIN_CONFLICT = [
    rel("procurement", "R002", 1, "D010", "D011", "same_entity", True),
    rel("maintenance", "R103", 1, "D011", "D012", "same_entity", True),
    rel("supplier", "R202", 1, "D010", "D012", "must_differ", True),
]

# 修正重投：保留链，约束改为 D010 与 D013 必须不同；另加归并桥边。
B3_CHAIN_OK = [
    rel("procurement", "R002", 1, "D010", "D011", "same_entity", True),
    rel("maintenance", "R103", 1, "D011", "D012", "same_entity", True),
    rel("supplier", "R202", 1, "D010", "D013", "must_differ", True),
    rel("procurement", "R003", 1, "D006", "D002", "same_entity", True),
    rel("procurement", "R004", 1, "D007", "D003", "same_entity", True),  # 桥边
]

# 同一批：撤销桥边 R004（effective=false）+ 新增 D008-D004 => 拆分与合并。
B4_SPLIT_MERGE = [
    rel("procurement", "R004", 2, "D007", "D003", "same_entity", False),
    rel("maintenance", "R105", 1, "D008", "D004", "same_entity", True),
]

# 篡改：R004 修订 1 已被修订 2 取代，此处改写其端点（D007-D003 -> D007-D004）。
B5_TAMPER = [
    rel("procurement", "R004", 1, "D007", "D004", "same_entity", True),
    rel("supplier", "R205", 1, "D011", "D013", "must_differ", True),
]

# 真实重传（内容与台账完全一致）+ 一条新约束。
B5R_REPLAY_AND_NEW = [
    rel("procurement", "R004", 1, "D007", "D003", "same_entity", True),
    rel("supplier", "R205", 1, "D011", "D013", "must_differ", True),
]

# 两个来源对同一对端点提供依据。
B6_TWO_SOURCES = [
    rel("procurement", "R006", 1, "D050", "D013", "same_entity", True),
    rel("supplier", "R203", 1, "D050", "D013", "same_entity", True),
]

# 撤销其中一条（procurement），supplier 的依据仍然有效。
B7_REVOKE_ONE = [
    rel("procurement", "R006", 2, "D050", "D013", "same_entity", False),
]

# 崩溃注入演示批次：一条跨实体约束 + 一条归并。
B8_CRASH = [
    rel("procurement", "R007", 1, "D001", "D009", "must_differ", True),
    rel("supplier", "R204", 1, "D006", "D050", "same_entity", True),
]

# ---------------------------------------------------------------------------
# 上游变更（装载器日志）：删除并重新插入 D008
# ---------------------------------------------------------------------------

D008_ROW = {
    "device_id": "D008",
    "vendor": "Beta",
    "model": "M210",
    "firmware": "1.2.0",
    "temperature": 38.6,
    "cycle_count": 233,
}

UPSTREAM_DEL_D008 = [
    {"event": "BEGIN", "txn_id": "T20"},
    {"event": "DELETE", "txn_id": "T20", "seq": 1, "device_id": "D008"},
    {"event": "COMMIT", "txn_id": "T20", "seq": 2, "commit_lsn": 2400},
]

UPSTREAM_INS_D008 = [
    {"event": "BEGIN", "txn_id": "T21"},
    {"event": "INSERT", "txn_id": "T21", "seq": 1, "row": D008_ROW},
    {"event": "COMMIT", "txn_id": "T21", "seq": 2, "commit_lsn": 2500},
]

BATCH_FILES = {
    "b1_init.jsonl": B1_INIT,
    "b2_chain_conflict.jsonl": B2_CHAIN_CONFLICT,
    "b3_chain_ok.jsonl": B3_CHAIN_OK,
    "b4_split_merge.jsonl": B4_SPLIT_MERGE,
    "b5_tamper.jsonl": B5_TAMPER,
    "b5r_replay_and_new.jsonl": B5R_REPLAY_AND_NEW,
    "b6_two_sources.jsonl": B6_TWO_SOURCES,
    "b7_revoke_one.jsonl": B7_REVOKE_ONE,
    "b8_crash.jsonl": B8_CRASH,
}

SCENARIO = {
    "devices_after_loader_sample": [
        "D001", "D002", "D003", "D004", "D006", "D007", "D008",
        "D009", "D010", "D011", "D012", "D013", "D050",
    ],
    "expected_versions": 8,
    "expected_batches": 7,
    "expected_final_entities": {
        "E-D001": ["D001", "D002", "D006", "D007", "D013", "D050"],
        "E-D003": ["D003", "D004", "D008", "D009"],
        "E-D010": ["D010", "D011", "D012"],
    },
    "steps": [
        {"op": "refresh", "expect_version": 1},
        {"op": "ingest", "file": "b1_init.jsonl", "expect_exit": 0, "expect_version": 2},
        {"op": "ingest", "file": "b2_chain_conflict.jsonl", "expect_exit": 5,
         "note": "链式跨端冲突，整批拒绝"},
        {"op": "ingest", "file": "b3_chain_ok.jsonl", "expect_exit": 0, "expect_version": 3},
        {"op": "ingest", "file": "b4_split_merge.jsonl", "expect_exit": 0, "expect_version": 4,
         "note": "同批撤销桥边+新增连接 => 拆分与合并，只产生一个新版本"},
        {"op": "ingest", "file": "b5_tamper.jsonl", "expect_exit": 3,
         "note": "已被取代的旧修订内容被改写"},
        {"op": "ingest", "file": "b5r_replay_and_new.jsonl", "expect_exit": 0,
         "expect_version": None, "note": "旧修订一致重传 + 新约束；分组不变不出版本"},
        {"op": "ingest", "file": "b6_two_sources.jsonl", "expect_exit": 0, "expect_version": 5},
        {"op": "ingest", "file": "b7_revoke_one.jsonl", "expect_exit": 0,
         "expect_version": None, "note": "撤销一条冗余依据，分组不变"},
        {"op": "consume", "file": "upstream_del_d008.jsonl"},
        {"op": "refresh", "expect_version": 6, "note": "D008 退出，关系暂扣"},
        {"op": "consume", "file": "upstream_ins_d008.jsonl"},
        {"op": "refresh", "expect_version": 7, "note": "D008 重现，重新参与"},
        {"op": "ingest", "file": "b8_crash.jsonl", "fault": "after_ledger_before_decision",
         "expect_exit": 42, "note": "台账已写、决策未落定时崩溃"},
        {"op": "ingest", "file": "b8_crash.jsonl", "expect_exit": 0, "expect_version": 8,
         "note": "重投同批，无半批状态"},
    ],
}


def generate(out_dir: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    if os.listdir(out_dir):
        raise SystemExit(f"输出目录非空，拒绝覆盖: {out_dir}")
    for fname, rows in BATCH_FILES.items():
        write_jsonl(os.path.join(out_dir, fname), rows)
    write_jsonl(os.path.join(out_dir, "upstream_del_d008.jsonl"), UPSTREAM_DEL_D008)
    write_jsonl(os.path.join(out_dir, "upstream_ins_d008.jsonl"), UPSTREAM_INS_D008)
    with open(os.path.join(out_dir, "scenario.json"), "w", encoding="utf-8") as fh:
        json.dump(SCENARIO, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    return SCENARIO


def main() -> int:
    ap = argparse.ArgumentParser(description="生成实体归并层确定性样例")
    ap.add_argument("out_dir", help="输出目录（必须为空或不存在）")
    args = ap.parse_args()
    scenario = generate(args.out_dir)
    print(
        f"已生成实体归并样例到 {args.out_dir}：{len(BATCH_FILES)} 个批次文件，"
        f"期望 {scenario['expected_versions']} 个决策版本"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
