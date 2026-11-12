#!/usr/bin/env python3
"""读数事实仓端到端演示：覆盖题目要求的全部场景。

场景清单
========
  0. 装载器快照 -> 归并层 refresh/ingest：链归并（版本2）后又拆开（版本3）
  1. 口径(截止1, 版本2 合并) 与 (截止1, 版本3 拆分)：两种归属的完整
     明细与指标对比（合并把格子合在一起，拆分重新分配各条读数）
  2. 未知设备 DZ（b2）：待归属清单；版本3 仍不在成员中，清单保持
  3. 带时差的午夜读数（+08:00 -> UTC 前一天）与十进制 0.1+0.2=0.3
  4. 更正跨天/跨设备（r1 改到次日 DB）+ 撤回当前最大值（r2）：原格子
     计数与极值被修正；撤回项可查
  5. 同稿篡改拒收、迟来旧稿不盖新版、重传幂等
  6. 进程间同步：构建门点等待期间另一个进程收新批次，截止范围保持
  7. 故障注入：明细已写未发布时真实崩溃（42），重开旧日报可读、
     building 残档可安全重建
  8. 重复生成同一对口径复用结果
  9. 独立离线核算器：逐格/逐条下钻/逐条待归属全量对照
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from cdc.cli import main as loader_main  # noqa: E402
from entity.cli import main as entity_main  # noqa: E402
from reading.cli import main as reading_main  # noqa: E402
from reading.fault import (  # noqa: E402
    CRASH_EXIT_CODE,
    FAULT_AFTER_DETAIL_BEFORE_PUBLISH,
)
from examples.generate_reading_sample import generate as generate_sample  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def check(name: str, cond: bool, detail: str = "") -> bool:
    record(name, cond, detail)
    return cond


def _run(main_fn, argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main_fn(argv)
    return code, out.getvalue(), err.getvalue()


def run_loader(argv):
    code, out, _ = _run(loader_main, argv)
    return code, out


def run_entity(argv):
    code, out, err = _run(entity_main, argv)
    return code, json.loads(out) if out.strip() else {}, err


def run_reading(argv):
    code, out, err = _run(reading_main, argv)
    return code, (json.loads(out) if out.strip() else {}), err


def run_reading_subprocess(argv, gate_dir=None):
    env = dict(os.environ)
    proc = subprocess.Popen(
        [sys.executable, "-I", os.path.join(ROOT, "run_reading.py"), *argv],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    return proc


class Workspace:
    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="reading-demo-")
        self.sample = os.path.join(self.dir, "sample")
        generate_sample(self.sample)
        self.loader_db = os.path.join(self.dir, "loader.db")
        self.ledger_db = os.path.join(self.dir, "ledger.db")
        self.reading_db = os.path.join(self.dir, "reading.db")

    def loader(self, name):
        return os.path.join(self.sample, "loader", name)

    def entity_batch(self, name):
        return os.path.join(self.sample, "entity", name)

    def reading_batch(self, name):
        return os.path.join(self.sample, "reading", name)

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def cells_index(rep: dict) -> dict:
    return {(c["entity_id"], c["utc_date"], c["metric"]): c for c in rep["cells"]}


def main() -> int:
    ws = Workspace()
    LD = ["--reading-db", ws.reading_db]
    LE = ["--ledger-db", ws.ledger_db]
    try:
        # ---- 0. 上游快照 + 归并层：版本1 拆分 / 版本2 合并 / 版本3 拆开 ----
        code, _ = run_loader([
            "import-snapshot", "--db", ws.loader_db,
            "--manifest", ws.loader("manifest.json"),
            "--shards", ws.loader("shard_0.jsonl"),
        ])
        assert code == 0
        code, rep, _ = run_entity(["refresh", *LE, "--loader-db", ws.loader_db])
        check("0a. refresh 建立分组版本1（5 个单设备实体）",
              code == 0 and rep["version_created"] == 1 and rep["entity_count"] == 5,
              json.dumps(rep, ensure_ascii=False))
        code, rep, _ = run_entity(
            ["ingest", *LE, "--batch", ws.entity_batch("g1_merge.jsonl")])
        check("0b. 归并 DA-DB -> 版本2（4 实体）",
              code == 0 and rep["version_created"] == 2 and rep["entity_count"] == 4,
              json.dumps(rep, ensure_ascii=False))
        code, rep, _ = run_entity(
            ["ingest", *LE, "--batch", ws.entity_batch("g2_split.jsonl")])
        check("0c. 撤销关系拆开 -> 版本3（5 实体）",
              code == 0 and rep["version_created"] == 3 and rep["entity_count"] == 5,
              json.dumps(rep, ensure_ascii=False))

        # ---- 读数批次 1/2/3 进入事实仓 ----
        for name in ["b1_base.jsonl", "b2_unknown.jsonl", "b3_decimal.jsonl"]:
            code, rep, _ = run_reading(["ingest", *LD, "--batch", ws.reading_batch(name)])
            assert code == 0, rep
        code, st, _ = run_reading(["status", *LD])
        check("0d. 三个确认批次落事实仓（3 批次/8 稿件/8 观测）",
              code == 0 and st["last_batch_id"] == 3 and st["editions"] == 8
              and st["observations"] == 8, json.dumps(st, ensure_ascii=False))

        # ---- 1. 两种归属口径对比（截止批次1：仅 b1）----
        c_merge, m1, _ = run_reading(
            ["build", *LD, "--ledger-db", ws.ledger_db,
             "--cutoff-batch", "1", "--grouping-version", "2"])
        c_split, s1, _ = run_reading(
            ["build", *LD, "--ledger-db", ws.ledger_db,
             "--cutoff-batch", "1", "--grouping-version", "3"])
        check("1a. 生成合并口径(1,2)与拆分口径(1,3)",
              c_merge == 0 and m1["status"] == "published"
              and c_split == 0 and s1["status"] == "published",
              json.dumps(m1, ensure_ascii=False))

        _, cells_m, _ = run_reading(
            ["cells", *LD, "--cutoff-batch", "1", "--grouping-version", "2"])
        _, cells_s, _ = run_reading(
            ["cells", *LD, "--cutoff-batch", "1", "--grouping-version", "3"])
        im, is_ = cells_index(cells_m), cells_index(cells_s)
        merged_cell = im[("E-DA", "2026-11-11", "vibration_rms")]
        check("1b. 合并口径把 DA/DB 四条读数合到同一个 E-DA 格子（count=4 sum=17.0）",
              merged_cell["row_count"] == 4 and merged_cell["amount_sum"] == "17.0"
              and merged_cell["amount_min"] == "1.0" and merged_cell["amount_max"] == "9.0",
              json.dumps(merged_cell, ensure_ascii=False))
        da_cell = is_[("E-DA", "2026-11-11", "vibration_rms")]
        db_cell = is_[("E-DB", "2026-11-11", "vibration_rms")]
        check("1c. 拆分口径重新分配：E-DA 留 r1/r2/r3（sum15），E-DB 独立成格（r4）",
              da_cell["row_count"] == 3 and da_cell["amount_sum"] == "15.0"
              and db_cell["row_count"] == 1 and db_cell["amount_sum"] == "2.0",
              json.dumps({"da": da_cell, "db": db_cell}, ensure_ascii=False))

        # 下钻：合并格子应能看到全部读数身份与采用稿件、归属依据
        _, drill, _ = run_reading(
            ["drill", *LD, "--cutoff-batch", "1", "--grouping-version", "2",
             "--entity", "E-DA", "--date", "2026-11-11", "--metric", "vibration_rms"])
        identities = sorted((d["origin"], d["reading_key"]) for d in drill["details"])
        basis_ok = all(d["basis"]["outcome"] == "assigned"
                       and d["basis"]["grouping_version"] == 2 for d in drill["details"])
        check("1d. 格子下钻到读数身份/采用稿件/归属依据（r1,r2,r3,r4，依据版本2）",
              identities == [("plant-a", "r1"), ("plant-a", "r2"),
                             ("plant-a", "r3"), ("plant-b", "r4")]
              and basis_ok,
              json.dumps(drill["details"], ensure_ascii=False))

        # ---- 2. 未知设备 DZ：待归属清单（不静默丢弃、不套用今天成员）----
        run_reading(["build", *LD, "--ledger-db", ws.ledger_db,
                     "--cutoff-batch", "2", "--grouping-version", "2"])
        _, un2, _ = run_reading(
            ["unassigned", *LD, "--cutoff-batch", "2", "--grouping-version", "2"])
        dz = [i for i in un2["items"] if i["reading_key"] == "r6"]
        check("2a. 未知设备 DZ 进待归属清单（outcome=unassigned，原因可查）",
              len(dz) == 1 and dz[0]["outcome"] == "unassigned"
              and dz[0]["device"] == "DZ" and un2["unassigned_count"] == 1,
              json.dumps(dz, ensure_ascii=False))
        run_reading(["build", *LD, "--ledger-db", ws.ledger_db,
                     "--cutoff-batch", "2", "--grouping-version", "3"])
        _, un2v3, _ = run_reading(
            ["unassigned", *LD, "--cutoff-batch", "2", "--grouping-version", "3",
             "--outcome", "unassigned"])
        check("2b. 换到分组版本3，DZ 仍待归属（不套用其它版本成员）",
              len(un2v3["items"]) == 1 and un2v3["items"][0]["device"] == "DZ",
              json.dumps(un2v3, ensure_ascii=False))

        # ---- 3. 时区午夜读数 + 精确十进制 ----
        run_reading(["build", *LD, "--ledger-db", ws.ledger_db,
                     "--cutoff-batch", "3", "--grouping-version", "3"])
        _, cells_3, _ = run_reading(
            ["cells", *LD, "--cutoff-batch", "3", "--grouping-version", "3"])
        i3 = cells_index(cells_3)
        da_d1 = i3[("E-DA", "2026-11-11", "vibration_rms")]
        tz_in_d1 = ("E-DA", "2026-11-11", "vibration_rms") in i3
        tz_not_d2 = ("E-DA", "2026-11-12", "vibration_rms") not in i3
        check("3a. +08:00 午夜读数归一到 UTC 前一天 2026-11-11（不按本地日期）",
              tz_in_d1 and tz_not_d2 and da_d1["row_count"] == 5,
              json.dumps(da_d1, ensure_ascii=False))
        # r1=1.0 r2=9.0 r3=5.0 r_tz=0.1 r_dec=0.2 -> 精确和 15.3（二进制浮点会得 15.29999...）
        check("3b. 十进制精确求和 1.0+9.0+5.0+0.1+0.2 = 15.3，最小 0.1",
              da_d1["amount_sum"] == "15.3" and da_d1["amount_min"] == "0.1",
              f"sum={da_d1['amount_sum']} min={da_d1['amount_min']}")

        # ---- 4. 跨天/跨设备更正 + 撤回当前最大值 ----
        code, rep4, _ = run_reading(
            ["ingest", *LD, "--batch", ws.reading_batch("b4_correct.jsonl")])
        check("4a. 更正批次4 接收（r1 出第2版、r2 出撤回第2版）",
              code == 0 and rep4["batch_id"] == 4 and rep4["new_editions"] == 2,
              json.dumps(rep4, ensure_ascii=False))
        run_reading(["build", *LD, "--ledger-db", ws.ledger_db,
                     "--cutoff-batch", "4", "--grouping-version", "3"])
        _, cells_4, _ = run_reading(
            ["cells", *LD, "--cutoff-batch", "4", "--grouping-version", "3"])
        i4 = cells_index(cells_4)
        da_d1_v4 = i4[("E-DA", "2026-11-11", "vibration_rms")]
        db_d2_v4 = i4[("E-DB", "2026-11-12", "vibration_rms")]
        check("4b. r1 改到次日 DB：原 E-DA/D1 格子去掉 r1（剩 r3,r_tz,r_dec）",
              da_d1_v4["row_count"] == 3 and da_d1_v4["amount_sum"] == "5.3"
              and da_d1_v4["amount_min"] == "0.1",
              json.dumps(da_d1_v4, ensure_ascii=False))
        check("4c. r1 落到次日 E-DB 格子（1.0）",
              db_d2_v4["row_count"] == 1 and db_d2_v4["amount_sum"] == "1.0",
              json.dumps(db_d2_v4, ensure_ascii=False))
        _, un4, _ = run_reading(
            ["unassigned", *LD, "--cutoff-batch", "4", "--grouping-version", "3",
             "--outcome", "withdrawn"])
        r2w = [i for i in un4["items"] if i["reading_key"] == "r2"]
        check("4d. 撤回的当前最大值 r2 进撤回清单，原格子极值随之修正",
              len(r2w) == 1 and r2w[0]["outcome"] == "withdrawn"
              and da_d1_v4["amount_max"] == "5.0",
              json.dumps({"withdrawn": r2w, "max": da_d1_v4["amount_max"]},
                         ensure_ascii=False))
        # 审计：一条观测的全部稿件有据可查
        _, audit, _ = run_reading(
            ["audit", *LD, "--origin", "plant-a", "--reading-key", "r1"])
        check("4e. 观测 r1 的稿件历史可审计（edition 1->2，设备/日期改变）",
              [e["edition"] for e in audit["editions"]] == [1, 2]
              and audit["editions"][0]["device"] == "DA"
              and audit["editions"][1]["device"] == "DB",
              json.dumps(audit, ensure_ascii=False))

        # 截止批次4之后才送达的 b5：先落事实仓（批次5），但 cutoff=4 的
        # 门控统计绝不能把它混入（在第6节验证）。
        code, rep5, _ = run_reading(
            ["ingest", *LD, "--batch", ws.reading_batch("b5_late.jsonl")])
        assert code == 0 and rep5["batch_id"] == 5, rep5

        # ---- 5. 协议红线：同稿篡改拒收、迟来旧稿不盖新版、重传幂等 ----
        tamper = os.path.join(ws.dir, "tamper.jsonl")
        with open(tamper, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "origin": "plant-a", "reading_key": "r1", "edition": 1,
                "device": "DA", "occurred_at": f"2026-11-11T08:00:00+00:00",
                "metric": "vibration_rms", "amount": "1.5",  # 改了金额
                "withdrawn": False}) + "\n")
        code, _, err = run_reading(["ingest", *LD, "--batch", tamper])
        code2, st_after, _ = run_reading(["status", *LD])
        check("5a. 同稿件换内容明确拒收（退出3），事实仓批次不前进",
              code == 3 and st_after["last_batch_id"] == 5
              and "EditionConflict" in err, err.strip())
        # 迟来的旧稿件（edition 1 重传，内容一致）幂等，且不盖过 edition 2
        code, rep_replay, _ = run_reading(
            ["ingest", *LD, "--batch", ws.reading_batch("b1_base.jsonl")])
        check("5b. 旧稿一致重传幂等（new_editions=0），选版仍是最新 edition",
              code == 0 and rep_replay["new_editions"] == 0
              and rep_replay["replayed_editions"] == 5,
              json.dumps(rep_replay, ensure_ascii=False))
        _, drill_r1, _ = run_reading(
            ["drill", *LD, "--cutoff-batch", "4", "--grouping-version", "3",
             "--entity", "E-DB", "--date", "2026-11-12", "--metric", "vibration_rms"])
        check("5c. 迟来旧稿没有盖过新版（r1 仍采用 edition 2）",
              len(drill_r1["details"]) == 1
              and drill_r1["details"][0]["chosen_edition"] == 2,
              json.dumps(drill_r1["details"], ensure_ascii=False))

        # ---- 6. 进程间同步：构建期间收新批次，截止范围保持一致 ----
        # b5（批次5）已在第4节后落仓；门控构建口径仍截止=4。
        gate_dir = os.path.join(ws.dir, "gate")
        os.makedirs(gate_dir, exist_ok=True)
        proc = run_reading_subprocess([
            "build", *LD, "--ledger-db", ws.ledger_db,
            "--cutoff-batch", "4", "--grouping-version", "2",
            "--gate-dir", gate_dir,
        ])
        waiting = os.path.join(gate_dir, "waiting")
        deadline = time.monotonic() + 20
        while not os.path.exists(waiting) and time.monotonic() < deadline:
            time.sleep(0.05)
        check("6a. 构建进程到达同步门并等待（waiting 文件出现）",
              os.path.exists(waiting))
        # 门点等待期间再投递一份新稿件（批次7），模拟资料送达与统计生成交错。
        interleave = os.path.join(ws.dir, "interleave.jsonl")
        with open(interleave, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "origin": "plant-c", "reading_key": "r_il", "edition": 1,
                "device": "DC", "occurred_at": "2026-11-12T10:00:00+00:00",
                "metric": "vibration_rms", "amount": "6.6",
                "withdrawn": False}) + "\n")
        code, rep_il, _ = run_reading(["ingest", *LD, "--batch", interleave])
        check("6b. 构建期间另一进程成功确认新批次（批次7）",
              code == 0 and rep_il["batch_id"] == 7, json.dumps(rep_il, ensure_ascii=False))
        with open(os.path.join(gate_dir, "release"), "w", encoding="utf-8") as fh:
            fh.write("go\n")
        stdout, stderr = proc.communicate(timeout=30)
        gate_build = json.loads(stdout)
        check("6c. 门控构建完成发布",
              proc.returncode == 0 and gate_build["status"] == "published",
              stderr.strip())
        _, cells_gate, _ = run_reading(
            ["cells", *LD, "--cutoff-batch", "4", "--grouping-version", "2"])
        keys_gate = {(c["entity_id"], c["utc_date"]) for c in cells_gate["cells"]}
        _, audit_il, _ = run_reading(
            ["audit", *LD, "--origin", "plant-c", "--reading-key", "r_il"])
        check("6d. 截止=4 的统计不混入批次5/7 才到的内容（r_late/r_il 均不在格子）",
              gate_build["included_count"] == 6
              and all("r_late" not in json.dumps(c) for c in cells_gate["cells"])
              and audit_il["editions"][0]["ingest_batch_id"] == 7,
              json.dumps({"keys": sorted(keys_gate)}, ensure_ascii=False))
        # 对照：截止=7 的新口径应纳入交错内容
        run_reading(["build", *LD, "--ledger-db", ws.ledger_db,
                     "--cutoff-batch", "7", "--grouping-version", "3"])
        _, un6, _ = run_reading(
            ["unassigned", *LD, "--cutoff-batch", "7", "--grouping-version", "3",
             "--outcome", "unassigned"])
        check("6e. 新口径截止=7 纳入交错读数（待归属只剩 DZ，r_il 已归 E-DC）",
              [i["reading_key"] for i in un6["items"]] == ["r6"],
              json.dumps(un6["items"], ensure_ascii=False))

        # ---- 7. 故障注入：明细已写未发布时崩溃，重开旧日报可读、可重建 ----
        crash_gate = None
        proc = run_reading_subprocess([
            "build", *LD, "--ledger-db", ws.ledger_db,
            "--cutoff-batch", "3", "--grouping-version", "2",
            "--fault", FAULT_AFTER_DETAIL_BEFORE_PUBLISH,
        ])
        _, stderr = proc.communicate(timeout=30)
        check("7a. 明细已写未发布阶段真实崩溃（退出42）",
              proc.returncode == CRASH_EXIT_CODE, stderr.strip())
        _, runs, _ = run_reading(["runs", *LD])
        building = [r for r in runs["runs"] if r["status"] == "building"]
        check("7b. 重开后崩溃口径残留 building，而已发布日报不受影响",
              any(r["cutoff_batch_id"] == 3 and r["grouping_version"] == 2
                  for r in building),
              json.dumps(building, ensure_ascii=False))
        # 在线查询仍能读到上一份完整日报（例如口径 3,3 已发布）
        code, old_cells, _ = run_reading(
            ["cells", *LD, "--cutoff-batch", "3", "--grouping-version", "3"])
        check("7c. 崩溃后上一份完整日报仍可读（口径3,3 正常返回格子）",
              code == 0 and len(old_cells["cells"]) > 0,
              json.dumps(old_cells["cells"], ensure_ascii=False))
        # 崩溃口径在重建前不可读
        code, _, err = run_reading(
            ["cells", *LD, "--cutoff-batch", "3", "--grouping-version", "2"])
        check("7d. 崩溃的 building 口径不对外可见（明确报错而非半空结果）",
              code == 7, err.strip())
        code, rebuild, _ = run_reading(
            ["build", *LD, "--ledger-db", ws.ledger_db,
             "--cutoff-batch", "3", "--grouping-version", "2"])
        check("7e. 同一对口径安全重建成功（published）",
              code == 0 and rebuild["status"] == "published",
              json.dumps(rebuild, ensure_ascii=False))

        # ---- 8. 重复生成同一对口径复用结果 ----
        code, reuse, _ = run_reading(
            ["build", *LD, "--ledger-db", ws.ledger_db,
             "--cutoff-batch", "3", "--grouping-version", "2"])
        check("8. 重复生成同一对固定口径复用结果（status=reused）",
              code == 0 and reuse["status"] == "reused",
              json.dumps(reuse, ensure_ascii=False))

        # ---- 9. 独立离线核算器：全量对照（格子/下钻/待归属，不只总行数）----
        code, verify_out, _ = _run(reading_main, [
            "verify", *LD, "--ledger-db", ws.ledger_db])
        verify = json.loads(verify_out)
        check("9. 独立离线核算器对照全部已发布 run（ok，含 building 标记）",
              code == 0 and verify["ok"]
              and verify["published_runs_checked"] == 9
              and verify["building_run_ids"] == [],
              json.dumps({"checked": verify["published_runs_checked"],
                          "problems": verify["problems"]}, ensure_ascii=False))

    finally:
        ws.cleanup()

    failed = [r for r in results if not r[1]]
    print(f"\n读数事实仓演示：{len(results) - len(failed)}/{len(results)} 项通过")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
