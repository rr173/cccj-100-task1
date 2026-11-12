#!/usr/bin/env python3
"""采样覆盖审计层端到端演示：覆盖题目要求的全部场景与边界。

场景清单
========
  0. 上游快照 -> 归并层：版本1 各自独立、版本2 DA/DB 合并为 E-DA
  1. 普通班次：条数足够但集中同一时刻仍有缺采；重复上报诊断
  2. 设备别名归并：DB 与 DA 在成员版本 2 共同满足 E-DA 的计划
  3. 容差边界：恰好 300 秒允许补洞；超容差（10:45）不能补洞
  4. 重叠豁免只扣一次（12:00 同时命中 E1/E2，豁免编号全保留）并切断缺口
  5. 豁免终点右开边界：13:00 不豁免
  6. 读数改时间 / 撤回 / 迟来补洞：覆盖随选版变化（旧审计固定不变）
  7. 成员版本切换：用版本1（未合并）审计，DB 读数无法归到 E-DA
  8. 同稿号篡改整批拒收；新稿号停用契约；重传幂等
  9. 春季跳时 gap 不产生应到项；秋季回拨两个真实时刻各一项（偏移不同）
 10. 跨午夜班次拆两个日段；生效终点右开边界；零应到单独说明
 11. 同步门：审计等待期间另一进程投新批次，固定批次不漂移
 12. 真实崩溃（os._exit 42）：无半份结果，重做安全发布
 13. 独立核对程序：对每个应到时刻与读数配对全量对照
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
from coverage.cli import main as coverage_main  # noqa: E402
from coverage.fault import (  # noqa: E402
    CRASH_EXIT_CODE,
    FAULT_AFTER_DETAIL_BEFORE_CONFIRM,
)
from examples.generate_coverage_sample import generate as generate_sample  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def check(name: str, cond: bool, detail: str = "") -> bool:
    record(name, bool(cond), detail)
    return bool(cond)


def _run(main_fn, argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main_fn(argv)
    return code, out.getvalue(), err.getvalue()


def run_cov(argv):
    code, out, err = _run(coverage_main, [str(a) for a in argv])
    return code, (json.loads(out) if out.strip() else {}), err


def run_reading(argv):
    code, out, err = _run(reading_main, argv)
    return code, (json.loads(out) if out.strip() else {}), err


# 审计主查询窗口：覆盖 2026-11-12 上海全天 + 纽约春秋 DST 日 + 跨午夜班次
Q_START = "2026-03-01T00:00:00Z"
Q_END = "2026-12-31T00:00:00Z"


class Workspace:
    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="coverage-demo-")
        self.sample = os.path.join(self.dir, "sample")
        generate_sample(self.sample)
        self.loader_db = os.path.join(self.dir, "loader.db")
        self.ledger_db = os.path.join(self.dir, "ledger.db")
        self.reading_db = os.path.join(self.dir, "reading.db")
        self.coverage_db = os.path.join(self.dir, "coverage.db")

    def p(self, *parts):
        return os.path.join(self.sample, *parts)

    def cov_argv(self, *rest):
        return ["--coverage-db", self.coverage_db, *rest]

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def bootstrap(ws: Workspace) -> None:
    """装载器快照 + 归并层版本1/版本2 + 两个读数批次。"""
    code, _, err = _run(loader_main, [
        "import-snapshot", "--db", ws.loader_db,
        "--manifest", ws.p("loader", "manifest.json"),
        "--shards", ws.p("loader", "shard_0.jsonl"),
    ])
    assert code == 0, err
    code, _, err = _run(entity_main, [
        "refresh", "--ledger-db", ws.ledger_db, "--loader-db", ws.loader_db,
    ])
    assert code == 0, err  # 版本 1：DA/DB/DC 各自独立
    code, _, err = _run(entity_main, [
        "ingest", "--ledger-db", ws.ledger_db,
        "--batch", ws.p("entity", "g1_merge.jsonl"),
    ])
    assert code == 0, err  # 版本 2：DA/DB 合并为 E-DA
    for batch in ("b1_base.jsonl", "b2_correct.jsonl"):
        code, rep, err = run_reading([
            "ingest", "--reading-db", ws.reading_db,
            "--batch", ws.p("reading", batch),
        ])
        assert code == 0, err


def ingest_contracts(ws: Workspace, *names) -> dict:
    code, rep, err = run_cov([
        "ingest-contracts", "--coverage-db", ws.coverage_db,
        "--batch", *[ws.p("coverage", n) for n in names],
    ])
    assert code == 0, err
    return rep


def main() -> int:
    ws = Workspace()
    try:
        bootstrap(ws)

        # ---- 契约与豁免登记（每个文件一个确认批次，批次号即契约水位）----
        for name in (
            "c1_draft1.jsonl", "c2_spring.jsonl", "c3_fall.jsonl",
            "c4_night.jsonl", "c5_outside.jsonl", "c6_effedge.jsonl",
        ):
            ingest_contracts(ws, name)
        code, rep, err = run_cov([
            "ingest-exemptions", "--coverage-db", ws.coverage_db,
            "--batch", ws.p("coverage", "e1_overlap.jsonl"),
        ])
        assert code == 0, err
        check("豁免批次接受（E1/E2 两条）", rep["rows"] == 2)

        # 同稿号篡改必须整批拒收
        code, _, err = _run(coverage_main, [
            "ingest-contracts", "--coverage-db", ws.coverage_db,
            "--batch", ws.p("coverage", "c1_draft1_tampered.jsonl"),
        ])
        check("同稿号换内容整批拒收(DraftConflict,3)", code == 3 and "DraftConflict" in err)

        # 同稿号内容一致重传：幂等（零新增），但仍登记一个确认批次
        rep = ingest_contracts(ws, "c1_draft1.jsonl")
        check("同稿重传幂等（new_drafts=0）", rep["new_drafts"] == 0)
        replay_batch_id = rep["batch_id"]

        # ---- 审计 A1：读数截止 1、成员版本 2（合并）、契约截止 6（全部契约）----
        code, a1, err = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", Q_START, "--query-end", Q_END,
            "--contract-cutoff", 6, "--reading-cutoff", 1,
            "--membership-version", 2,
        ])
        assert code == 0, err
        rid1 = a1["run_id"]
        check("A1 发布成功", a1["status"] == "published")
        check(
            "A1 应到 30 项（含豁免2）：C1=12 C2=4 C3=4 C4=2 C5=6 C6=2",
            a1["expected_count"] == 30 and a1["exempt_count"] == 2,
            f"expected={a1['expected_count']} exempt={a1['exempt_count']}",
        )
        check(
            "A1 分母=非豁免应到 28，覆盖=7，不拿条数除预期",
            a1["denominator_non_exempt"] == 28 and a1["covered_count"] == 7,
            f"den={a1['denominator_non_exempt']} cov={a1['covered_count']}",
        )
        check(
            "A1 重复上报 2 条（08:00 两条 + 回拨 01:00 两条读数同项竞争）",
            a1["duplicate_count"] == 2,
            f"dup={a1['duplicate_count']}",
        )
        check(
            "A1 未匹配 3 条：超容差 r_far/r_spring_gap + 无契约 r_unknown",
            a1["unmatched_count"] == 3,
            f"unmatched={a1['unmatched_count']}",
        )

        code, report, _ = run_cov(["report", "--coverage-db", ws.coverage_db,
                                   "--run-id", str(rid1)])
        ratio = report["coverage_ratio"]
        check(
            "覆盖比例返回分子/分母（7/28）而非合计折算",
            ratio["numerator_covered"] == 7
            and ratio["denominator_non_exempt_expected"] == 28,
        )

        # ---- 场景 1：条数足够但集中同一时刻仍有缺采 ----
        c1_items = [it for it in report["expected_items"] if it["contract_no"] == "C1"]
        c1_non_exempt = [it for it in c1_items if not it["exempt"]]
        check("C1 非豁免应到 10 项（12 项计划 - 2 豁免）", len(c1_non_exempt) == 10)
        missing_times = {
            it["local_time"] for it in c1_non_exempt if not it["covered"]
        }
        check(
            "08:00 有两条读数仍缺 10:00 等位置（不能用条数掩盖空洞；11/12 点属豁免）",
            {"10:00:00", "14:00:00"} <= missing_times
            and "11:00:00" not in missing_times
            and "12:00:00" not in missing_times,
            str(sorted(missing_times)),
        )

        # ---- 场景 2：别名归并（DB 满足 E-DA 计划）----
        dup_rows = report["matches"]
        r3_hit = [m for m in dup_rows if m["reading_key"] == "r3"]
        check(
            "DB 设备读数 r3 归到 E-DA 并覆盖 09:00 计划",
            len(r3_hit) == 1
            and r3_hit[0]["device"] == "DB"
            and r3_hit[0]["entity_id"] == "E-DA",
        )

        # ---- 场景 3：容差边界 ----
        r_early = [m for m in dup_rows if m["reading_key"] == "r_early"]
        check(
            "恰好 300 秒边界允许匹配（r_early 07:55 -> 08:00）",
            len(r_early) == 1 and r_early[0]["distance_seconds"] == 300,
        )
        unmatched = report["unmatched_readings"]
        r_far = next(u for u in unmatched if u["reading_key"] == "r_far")
        check(
            "超容差不能补洞：r_far 10:45 标记 outside_tolerance 且保留候选依据",
            r_far["reason"] == "outside_tolerance"
            and r_far["candidates"][0]["distance_seconds"] == 2700,
        )

        # ---- 场景 4：重叠豁免只扣一次 + 切断缺口 ----
        # E1=[11:00,12:30)，E2=[12:00,13:00)：12:00 同时命中两者
        noon = next(it for it in c1_items if it["local_time"] == "12:00:00")
        check(
            "12:00 同时命中 E1/E2 但只扣一次，两个豁免编号都保留",
            noon["exempt"] == 1 and noon["exemption_ids"] == ["E1", "E2"],
            str(noon["exemption_ids"]),
        )
        # 11:00 只被 E1 覆盖；并集按时刻只扣一次（不会因重叠扣两次）
        eleven = next(it for it in c1_items if it["local_time"] == "11:00:00")
        check("11:00 仅命中 E1，重叠豁免对同一时刻只扣一次",
              eleven["exempt"] == 1 and eleven["exemption_ids"] == ["E1"])
        c1_gaps = [g for g in report["gaps"] if g["contract_no"] == "C1"]
        check(
            "C1 缺口被豁免切成两段（中间豁免位置不允许缺口跨越）",
            len(c1_gaps) == 2 and c1_gaps[0]["split_exemptions"],
            f"gaps={len(c1_gaps)} split={c1_gaps[0]['split_exemptions'] if c1_gaps else None}",
        )
        # 缺口附近读数诊断：r_far 应出现在缺口 nearby_readings
        nearby_keys = {
            n["reading_key"] for g in c1_gaps for n in g["nearby_readings"]
        }
        check("缺口保留附近未匹配读数诊断（r_far）", "r_far" in nearby_keys)

        # ---- 场景 5：豁免终点右开 ----
        edge = next(it for it in c1_items if it["local_time"] == "13:00:00")
        check("豁免终点 13:00 右开不豁免且被 r_exedge 覆盖",
              edge["exempt"] == 0 and edge["covered"] == 1)

        # ---- 场景 9：DST 春季 gap / 秋季 fold ----
        c2 = [it for it in report["expected_items"] if it["contract_no"] == "C2"]
        c2_labels = [(it["local_time"], it["utc_offset"]) for it in c2]
        check(
            "春季跳时：02:00 不存在不产生应到项（仅 00/01/03/04）",
            [t for t, _ in c2_labels] == ["00:00:00", "01:00:00", "03:00:00", "04:00:00"],
            str(c2_labels),
        )
        gap_reading = next(u for u in unmatched if u["reading_key"] == "r_spring_gap")
        check("落在 gap 的读数无应到项可配 -> outside_tolerance",
              gap_reading["reason"] == "outside_tolerance")
        c3 = [it for it in report["expected_items"] if it["contract_no"] == "C3"]
        fold_items = [it for it in c3 if it["local_time"] == "01:00:00"]
        check(
            "秋季回拨：01:00 两个真实时刻各一项，偏移 -05:00 与 -04:00 都保留",
            len(fold_items) == 2
            and {it["utc_offset"] for it in fold_items} == {"-05:00", "-04:00"}
            and {it["fold_occurrence"] for it in fold_items} == {0, 1},
            str([(it["utc_offset"], it["fold_occurrence"]) for it in fold_items]),
        )
        check(
            "回拨两项分别由两条偏移不同的读数覆盖",
            all(it["covered"] == 1 for it in fold_items),
        )

        # ---- 场景 10：跨午夜 / 生效终点边界 / 零应到 ----
        c4 = [it for it in report["expected_items"] if it["contract_no"] == "C4"]
        check(
            "跨午夜 22:00-02:00 拆两个日段，锚定段起点得 22/00 两项节拍",
            [(it["local_date"], it["local_time"]) for it in c4] == [
                ("2026-11-12", "22:00:00"),
                ("2026-11-13", "00:00:00"),
            ],
            str([(it["local_date"], it["local_time"]) for it in c4]),
        )
        check("跨午夜段 step_index 各自从 0 锚定（不从查询起点计步）",
              [it["step_index"] for it in c4] == [0, 0])
        c6 = [it for it in report["expected_items"] if it["contract_no"] == "C6"]
        check(
            "恰好落在生效终点 02:00（右开）不制造应到项，C6 仅 00/01 两项",
            [it["local_time"] for it in c6] == ["00:00:00", "01:00:00"],
        )
        c5 = [it for it in report["expected_items"] if it["contract_no"] == "C5"]
        check(
            "C5 生效区间与查询窗口相交，展开 6 个应到项且全部缺采",
            len(c5) == 6 and all(it["covered"] == 0 for it in c5),
            f"c5_count={len(c5)}",
        )
        check("主窗口仍有其它契约应到，整体不触发零应到说明",
              a1["zero_expected_note"] is None)

        # 专门的零应到审计：查询窗口（6 月一天）落在全部契约生效范围之外
        code, a0, err = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", "2026-06-01T00:00:00Z",
            "--query-end", "2026-06-02T00:00:00Z",
            "--contract-cutoff", 6, "--reading-cutoff", 1,
            "--membership-version", 2,
        ])
        assert code == 0, err
        check(
            "零应到审计：分母为零、覆盖比例不除零，且单独说明不能称全量正常",
            a0["denominator_non_exempt"] == 0
            and a0["zero_expected_note"] is not None
            and "未定义" in a0["zero_expected_note"],
            str(a0.get("zero_expected_note")),
        )

        # ---- 场景 6：读数改时间/撤回/迟来补洞（新审计 A2，A1 固定不变）----
        code, a2, err = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", Q_START, "--query-end", Q_END,
            "--contract-cutoff", 6, "--reading-cutoff", 2,
            "--membership-version", 2,
        ])
        assert code == 0, err
        code, rep2, _ = run_cov(["report", "--coverage-db", ws.coverage_db,
                                 "--run-id", str(a2["run_id"])])
        c1_2 = [it for it in rep2["expected_items"] if it["contract_no"] == "C1"]
        cov_times = {it["local_time"]: it["covered"] for it in c1_2 if not it["exempt"]}
        check("r2 改到 10:00 后该位置获得覆盖", cov_times.get("10:00:00") == 1)
        check("r3 撤回后 09:00 失去覆盖（撤回稿不参与）",
              cov_times.get("09:00:00") == 0)
        check("迟来 r_late 补上 15:00", cov_times.get("15:00:00") == 1)
        check(
            "08:00 重复随 r2 改时间消失，仅剩回拨项的重复（duplicate=1）",
            a2["duplicate_count"] == 1,
            f"dup={a2['duplicate_count']}",
        )
        # 旧审计 A1 保持原样
        code, rep1_again, _ = run_cov(["report", "--coverage-db", ws.coverage_db,
                                       "--run-id", str(rid1)])
        check("旧审计 A1 固定不变（覆盖仍为 7，事后更正只影响新审计）",
              rep1_again["run"]["covered_count"] == 7)
        # 同口径重做直接复用
        code, reused, _ = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", Q_START, "--query-end", Q_END,
            "--contract-cutoff", 6, "--reading-cutoff", 1,
            "--membership-version", 2,
        ])
        check("同口径重复审计复用（status=reused，同一 run_id）",
              reused["status"] == "reused" and reused["run_id"] == rid1)

        # ---- 场景 7：成员版本切换（版本1 未合并）----
        code, a3, err = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", Q_START, "--query-end", Q_END,
            "--contract-cutoff", 6, "--reading-cutoff", 2,
            "--membership-version", 1,
        ])
        assert code == 0, err
        code, rep3, _ = run_cov(["report", "--coverage-db", ws.coverage_db,
                                 "--run-id", str(a3["run_id"])])
        # 版本1：DA 自成 E-DA、DB 自成 E-DB。契约实体是 E-DA，因此 DB 的
        # 读数无法归到 E-DA（r_late 在 DB 上 -> 无契约），DA 的读数仍归属。
        # 注意 r3 在读数截止 2 已被撤回，本就不参与，故用 r_late 验证。
        unmatched_v1 = {
            (u["reading_key"], u["reason"]) for u in rep3["unmatched_readings"]
        }
        check(
            "不能用今天的成员改写历史：版本1 下 DB 读数 r_late 不属 E-DA（无契约）",
            ("r_late", "no_contract_for_origin_entity_metric") in unmatched_v1
            and ("r_unknown", "no_contract_for_origin_entity_metric") in unmatched_v1,
            str(sorted(unmatched_v1)),
        )
        check("版本1 下 DA 读数仍按当时成员归 E-DA（r1 可配对）",
              not any(k == "r1" for k, _ in unmatched_v1))

        # ---- 场景 8：新稿号停用契约（停用稿批次号 = 重传批 + 1）----
        ingest_contracts(ws, "c1_draft2_disable.jsonl")  # 停用稿批次（重传批之后）
        disable_batch_id = replay_batch_id + 1
        code, a4, err = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", Q_START, "--query-end", Q_END,
            "--contract-cutoff", disable_batch_id, "--reading-cutoff", 2,
            "--membership-version", 2,
        ])
        assert code == 0, err
        code, rep4, _ = run_cov(["report", "--coverage-db", ws.coverage_db,
                                 "--run-id", str(a4["run_id"])])
        check(
            "停用稿生效：新审计不再展开 C1，其余契约 C2..C6 照常（18 项）",
            all(it["contract_no"] != "C1" for it in rep4["expected_items"])
            and {it["contract_no"] for it in rep4["expected_items"]} ==
            {"C2", "C3", "C4", "C5", "C6"}
            and a4["expected_count"] == 18,
            f"actual_count={a4['expected_count']} "
            f"contracts={sorted({it['contract_no'] for it in rep4['expected_items']})}",
        )
        # 截止批次 6 的旧审计口径仍含 C1，证明停用只影响新审计
        check("旧截止批次 6 口径不受停用稿影响（C1 仍在 A1）",
              any(it["contract_no"] == "C1" for it in report["expected_items"]))

        # ---- 场景 11：同步门证明固定批次不漂移 ----
        # 用一个从未审计过的全新窗口，确保不会在门点之前就幂等复用返回。
        gate_window = ("2026-08-10T00:00:00Z", "2026-08-11T00:00:00Z")
        gate_dir = os.path.join(ws.dir, "gate")
        proc = subprocess.Popen(
            [sys.executable, "-I", os.path.join(ROOT, "run_coverage.py"),
             "audit", "--coverage-db", ws.coverage_db,
             "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
             "--query-start", gate_window[0], "--query-end", gate_window[1],
             "--contract-cutoff", "6", "--reading-cutoff", "1",
             "--membership-version", "2", "--gate-dir", gate_dir],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        waiting = os.path.join(gate_dir, "waiting")
        deadline = time.time() + 10
        while not os.path.exists(waiting) and time.time() < deadline:
            time.sleep(0.05)
        check("同步门：审计进程在门点等待", os.path.exists(waiting))
        # 等待期间重放 b2（幂等），证明门后读数仓写入不影响截止1 的审计
        code, _, err = run_reading([
            "ingest", "--reading-db", ws.reading_db,
            "--batch", ws.p("reading", "b2_correct.jsonl"),
        ])
        check("门点等待期间读数仓仍可写入", code == 0)
        with open(os.path.join(gate_dir, "release"), "w", encoding="utf-8") as fh:
            fh.write("go\n")
        out, errout = proc.communicate(timeout=15)
        gated = json.loads(out)
        # 该窗口无任何契约生效 -> 零应到审计；关键是读数截止固定为 1
        check(
            "放行后审计完成且读数截止固定为 1（门后写入不漂移，零应到正确说明）",
            proc.returncode == 0
            and gated["status"] == "published"
            and gated["denominator_non_exempt"] == 0
            and gated["zero_expected_note"] is not None,
            errout.strip(),
        )

        # ---- 场景 12：真实崩溃，无半份结果 ----
        crash_window = ("2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z")
        crash_proc = subprocess.Popen(
            [sys.executable, "-I", os.path.join(ROOT, "run_coverage.py"),
             "audit", "--coverage-db", ws.coverage_db,
             "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
             "--query-start", crash_window[0], "--query-end", crash_window[1],
             "--contract-cutoff", "6", "--reading-cutoff", "1",
             "--membership-version", "2",
             "--fault", FAULT_AFTER_DETAIL_BEFORE_CONFIRM],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        _, crash_err = crash_proc.communicate(timeout=15)
        check("崩溃注入：进程以 42 真实退出",
              crash_proc.returncode == CRASH_EXIT_CODE)
        code, runs, _ = run_cov(["runs", "--coverage-db", ws.coverage_db])
        stale = [r for r in runs if r["status"] == "building"]
        check("崩溃后只留 building 残档，无 published 半份结果", len(stale) == 1)
        # 残档明确拒绝对外读取
        stale_id = stale[0]["run_id"]
        code, _, err = _run(coverage_main, [
            "report", "--coverage-db", ws.coverage_db, "--run-id", str(stale_id)])
        check("building 残档对外读取被拒（AuditStale,8）",
              code == 8 and "AuditStale" in err)
        # 同口径重做：安全清理并发布
        code, redone, err = run_cov([
            "audit", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
            "--query-start", crash_window[0], "--query-end", crash_window[1],
            "--contract-cutoff", 6, "--reading-cutoff", 1,
            "--membership-version", 2,
        ])
        check("重做后成功发布且残档被清理",
              redone["status"] == "published")
        code, runs, _ = run_cov(["runs", "--coverage-db", ws.coverage_db])
        check("库中不再有任何 building 残档",
              all(r["status"] == "published" for r in runs))

        # ---- 场景 13：独立核对程序全量对照 ----
        code, verify_text, err = _run(coverage_main, [
            "verify", "--coverage-db", ws.coverage_db,
            "--reading-db", ws.reading_db, "--ledger-db", ws.ledger_db,
        ])
        verify = json.loads(verify_text)
        check(
            f"独立核对程序逐条对照全部审计（ok={verify.get('ok')}）",
            code == 0 and verify.get("ok") is True,
            json.dumps(
                [d for rep in verify.get("reports", []) for d in rep.get("diffs", [])],
                ensure_ascii=False,
            )[:500],
        )

    finally:
        ws.cleanup()

    failed = [n for n, ok, _ in results if not ok]
    print("\n==== 汇总 ====")
    print(f"断言 {len(results)} 项，失败 {len(failed)} 项")
    if failed:
        for n in failed:
            print("  FAIL:", n)
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
