#!/usr/bin/env python3
"""实体归并层端到端演示：覆盖题目要求的全部场景。

场景清单：
  1. refresh 初始化在场设备（版本 1）
  2. 基础归并批次（版本 2）
  3. 两条独立连接构成链后触发跨端冲突 -> 整批拒绝且报告关系路径
  4. 修正重投（版本 3）
  5. 同批撤销桥边 + 新增连接 -> 拆分与合并，只产生一个新版本（版本 4）
  6. 已被新修订取代的旧修订被篡改 -> 拒绝，整批回滚
  7. 旧修订一致重传 + 新约束 -> 接受但分组不变、不出版本
  8. 两个来源对同一对端点提供依据（版本 5）；撤销其一，另一条仍有效
  9. 上游删除设备 -> refresh 后退出活动成员、关系暂扣（版本 6）
 10. 上游重新插入 -> refresh 后重新参与约束判定（版本 7）
 11. 故障注入：台账已写、决策未落定时真实崩溃（退出码 42），
     重开后验证无半批状态，重投同批成功（版本 8）
 12. 历史分组 / 相邻版本变动 / 按设备解释
 13. 全部批次重投幂等（不增版本、不增修订）
 14. 独立全量重算器逐版本对照完整分组与冲突结论

崩溃通过子进程 + os._exit(42) 真实模拟，再以新进程重开同一台账继续。
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from cdc.cli import main as loader_main  # noqa: E402
from entity.cli import main as entity_main  # noqa: E402
from entity.fault import (  # noqa: E402
    CRASH_EXIT_CODE,
    FAULT_AFTER_LEDGER_BEFORE_DECISION,
)
from examples.generate_entity_sample import generate as generate_entity  # noqa: E402
from examples.generate_sample import generate as generate_loader  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def check(name: str, cond: bool, detail: str = "") -> bool:
    record(name, cond, detail)
    return cond


def run_loader(argv: list[str]) -> tuple[int, str]:
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = loader_main(argv)
    return code, buf.getvalue()


def run_entity(argv: list[str]) -> tuple[int, str, str]:
    import contextlib
    import io

    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = entity_main(argv)
    return code, out.getvalue(), err.getvalue()


def run_entity_json(argv: list[str]) -> tuple[int, dict, str]:
    code, out, err = run_entity(argv)
    return code, json.loads(out), err


def run_entity_subprocess(argv: list[str]) -> tuple[int, str, str]:
    """独立子进程跑实体 CLI（真实崩溃演示；隔离模式 -I）。"""
    proc = subprocess.run(
        [sys.executable, "-I", os.path.join(ROOT, "run_entity.py"), *argv],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


class Workspace:
    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="entity-demo-")
        self.loader_sample = os.path.join(self.dir, "loader_sample")
        self.entity_sample = os.path.join(self.dir, "entity_sample")
        generate_loader(self.loader_sample)
        self.scenario = generate_entity(self.entity_sample)
        self.loader_db = os.path.join(self.dir, "loader.db")
        self.ledger_db = os.path.join(self.dir, "ledger.db")

    def sample(self, name: str) -> str:
        return os.path.join(self.loader_sample, name)

    def batch(self, name: str) -> str:
        return os.path.join(self.entity_sample, name)

    def cleanup(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


def main() -> int:
    ws = Workspace()
    try:
        # ---- 装载器：导入快照并追日志，得到最终 device_master ----
        code, _ = run_loader([
            "import-snapshot", "--db", ws.loader_db,
            "--manifest", ws.sample("manifest.json"),
            "--shards", ws.sample("shard_0.jsonl"), ws.sample("shard_1.jsonl"),
            ws.sample("shard_2.jsonl"),
        ])
        assert code == 0
        code, _ = run_loader([
            "consume", "--db", ws.loader_db,
            "--logs", ws.sample("log_1.jsonl"), ws.sample("log_2.jsonl"),
        ])
        assert code == 0

        L = ["--ledger-db", ws.ledger_db]

        # ---- 1. refresh：初始化在场设备（版本 1，全部单设备实体）----
        code, rep, _ = run_entity_json(["refresh", *L, "--loader-db", ws.loader_db])
        check("1. refresh 初始化（版本1/13设备/13单设备实体）",
              code == 0 and rep["version_created"] == 1
              and rep["device_count"] == 13 and rep["entity_count"] == 13,
              json.dumps(rep, ensure_ascii=False))

        # ---- 2. 基础归并批次 ----
        code, rep, _ = run_entity_json(["ingest", *L, "--batch", ws.batch("b1_init.jsonl")])
        check("2. b1 基础归并（版本2/9实体）",
              code == 0 and rep["version_created"] == 2 and rep["entity_count"] == 9
              and rep["new_revisions"] == 4, json.dumps(rep, ensure_ascii=False))

        # ---- 3. 链式跨端冲突：整批拒绝 + 报告具体关系路径 ----
        code, rep, err = run_entity_json(
            ["ingest", *L, "--batch", ws.batch("b2_chain_conflict.jsonl")])
        path_ok = False
        if code == 5 and rep.get("conflicts"):
            conflict = rep["conflicts"][0]
            steps = conflict["path"]
            path_ok = (
                conflict["constraint"]["endpoints"] == ["D010", "D012"]
                and [(s["from"], s["to"], s["source"], s["relation_id"]) for s in steps]
                == [("D010", "D011", "procurement", "R002"),
                    ("D011", "D012", "maintenance", "R103")]
            )
        code2, st, _ = run_entity_json(["status", *L])
        check("3. b2 链式冲突拒绝（退出5/约束与路径D010→D011→D012）",
              code == 5 and path_ok, json.dumps(rep, ensure_ascii=False))
        check("3b. 冲突批不留痕迹（版本2/批次1/修订4）",
              st["current_version"] == 2 and st["batches"] == 1
              and st["revisions"] == 4, json.dumps(st, ensure_ascii=False))

        # ---- 4. 修正重投（b3）----
        code, rep, _ = run_entity_json(["ingest", *L, "--batch", ws.batch("b3_chain_ok.jsonl")])
        check("4. b3 修正重投（版本3/5实体）",
              code == 0 and rep["version_created"] == 3 and rep["entity_count"] == 5,
              json.dumps(rep, ensure_ascii=False))

        # ---- 5. 同批撤销桥边 + 新增连接：拆分与合并，一个新版本 ----
        code, rep, _ = run_entity_json(
            ["ingest", *L, "--batch", ws.batch("b4_split_merge.jsonl")])
        ok5 = code == 0 and rep["version_created"] == 4
        code, ch, _ = run_entity_json(["changes", *L, "--version", "4"])
        split_ok = any(
            s["entity"] == "E-D001" and s["into_entities"] == ["E-D001", "E-D003"]
            for s in ch["splits"]
        )
        merge_ok = any(
            m["entity"] == "E-D003" and m["from_entities"] == ["E-D001", "E-D004"]
            for m in ch["merges"]
        )
        check("5. b4 同批拆分+合并（仅版本4）",
              ok5 and split_ok and merge_ok,
              json.dumps(ch, ensure_ascii=False))

        # ---- 6. 篡改已被取代的旧修订 -> 拒绝且整批回滚 ----
        code, out, err = run_entity(
            ["ingest", *L, "--batch", ws.batch("b5_tamper.jsonl")])
        code2, st, _ = run_entity_json(["status", *L])
        check("6. b5 篡改旧修订拒绝（退出3/指认R004）",
              code == 3 and "R004" in err, err.strip())
        check("6b. 篡改批不同事的新行也不入账（修订数不变/版本4）",
              st["revisions"] == 11 and st["current_version"] == 4,
              json.dumps(st, ensure_ascii=False))

        # ---- 7. 旧修订一致重传 + 新约束：接受但不出版本 ----
        code, rep, _ = run_entity_json(
            ["ingest", *L, "--batch", ws.batch("b5r_replay_and_new.jsonl")])
        check("7. b5r 重传+新约束（接受/重传1新增1/不出版本）",
              code == 0 and rep["replayed_revisions"] == 1
              and rep["new_revisions"] == 1 and rep["version_created"] is None
              and rep["current_version"] == 4, json.dumps(rep, ensure_ascii=False))

        # ---- 8. 两个来源同一对端点；撤销其一另一条仍有效 ----
        code, rep, _ = run_entity_json(
            ["ingest", *L, "--batch", ws.batch("b6_two_sources.jsonl")])
        code, ex, _ = run_entity_json(["explain", *L, "--device", "D050"])
        basis = {(e["source"], e["relation_id"]) for e in ex["basis_edges"]}
        ok8 = (rep["version_created"] == 5 and ex["members"] == ["D013", "D050"]
               and basis == {("procurement", "R006"), ("supplier", "R203")})
        code, rep2, _ = run_entity_json(
            ["ingest", *L, "--batch", ws.batch("b7_revoke_one.jsonl")])
        code, ex2, _ = run_entity_json(["explain", *L, "--device", "D050"])
        basis2 = {(e["source"], e["relation_id"]) for e in ex2["basis_edges"]}
        check("8. 双依据与撤销其一（版本5/撤销后不出版本/成员不变/仅剩R203）",
              ok8 and rep2["version_created"] is None
              and rep2["current_version"] == 5
              and ex2["members"] == ["D013", "D050"]
              and basis2 == {("supplier", "R203")},
              json.dumps(ex2, ensure_ascii=False))

        # ---- 9. 上游删除 D008 -> refresh 后退出成员、关系暂扣 ----
        code, _ = run_loader([
            "consume", "--db", ws.loader_db,
            "--logs", ws.batch("upstream_del_d008.jsonl")])
        assert code == 0
        code, rep, _ = run_entity_json(["refresh", *L, "--loader-db", ws.loader_db])
        code, ch, _ = run_entity_json(["changes", *L, "--version", "6"])
        code, st, _ = run_entity_json(["status", *L])
        parked = {(p["relation_id"]) for p in st["parked_relations"]}
        split6 = any(
            s["entity"] == "E-D003" and s["into_entities"] == ["E-D003", "E-D004"]
            and s["lost_devices"] == ["D008"] for s in ch["splits"]
        )
        check("9. 上游删除D008（版本6/拆分/暂扣R102,R105）",
              rep["version_created"] == 6 and split6
              and ch["devices_disappeared"] == ["D008"]
              and parked == {"R102", "R105"},
              json.dumps(ch, ensure_ascii=False))

        # ---- 10. 上游重现 D008 -> 重新参与约束判定 ----
        code, _ = run_loader([
            "consume", "--db", ws.loader_db,
            "--logs", ws.batch("upstream_ins_d008.jsonl")])
        assert code == 0
        code, rep, _ = run_entity_json(["refresh", *L, "--loader-db", ws.loader_db])
        code, ch, _ = run_entity_json(["changes", *L, "--version", "7"])
        merge7 = any(
            m["entity"] == "E-D003" and m["from_entities"] == ["E-D003", "E-D004"]
            and m["new_devices"] == ["D008"] for m in ch["merges"]
        )
        code, st, _ = run_entity_json(["status", *L])
        check("10. D008重现（版本7/合并回来/无暂扣）",
              rep["version_created"] == 7 and merge7
              and ch["devices_appeared"] == ["D008"]
              and st["parked_relations"] == [],
              json.dumps(ch, ensure_ascii=False))

        # ---- 11. 崩溃注入：台账已写、决策未落定 -> 退出42；重开无半批 ----
        rc, _, serr = run_entity_subprocess([
            "ingest", *L, "--batch", ws.batch("b8_crash.jsonl"),
            "--fault", FAULT_AFTER_LEDGER_BEFORE_DECISION,
        ])
        code, st, _ = run_entity_json(["status", *L])
        clean = (st["current_version"] == 7 and st["last_batch_id"] == 6
                 and st["revisions"] == 15 and st["batches"] == 6)
        check("11. 崩溃于决策落定前（退出42/版本7/批次6/修订15，无半批状态）",
              rc == CRASH_EXIT_CODE and clean,
              f"rc={rc} status={json.dumps(st, ensure_ascii=False)} stderr={serr.strip()}")
        code, rep, _ = run_entity_json(
            ["ingest", *L, "--batch", ws.batch("b8_crash.jsonl")])
        check("11b. 重投同批成功（批次7/版本8）",
              code == 0 and rep["batch_id"] == 7 and rep["version_created"] == 8,
              json.dumps(rep, ensure_ascii=False))

        # ---- 12. 历史分组 / 相邻变动 / 按设备解释 ----
        code, hi, _ = run_entity_json(["history", *L])
        code, sv3, _ = run_entity_json(["show-version", *L, "--version", "3"])
        v3 = {e["entity_id"]: e["members"] for e in sv3["entities"]}
        code, ex_old, _ = run_entity_json(
            ["explain", *L, "--device", "D008", "--version", "6"])
        check("12. 历史查询（8版本/v3五实体/v6时D008不在场）",
              len(hi["versions"]) == 8
              and v3.get("E-D001") == ["D001", "D002", "D003", "D006", "D007", "D008"]
              and ex_old["present"] is False,
              json.dumps(hi, ensure_ascii=False))

        # ---- 13. 全部批次重投幂等 ----
        before = run_entity_json(["status", *L])[1]
        for name in ["b1_init.jsonl", "b3_chain_ok.jsonl", "b4_split_merge.jsonl",
                     "b5r_replay_and_new.jsonl", "b6_two_sources.jsonl",
                     "b7_revoke_one.jsonl", "b8_crash.jsonl"]:
            code, rep, _ = run_entity_json(["ingest", *L, "--batch", ws.batch(name)])
            assert code == 0 and rep["new_revisions"] == 0, (name, rep)
            assert rep["version_created"] is None, (name, rep)
        after = run_entity_json(["status", *L])[1]
        check("13. 重投全部批次幂等（版本8/修订17不变）",
              after["current_version"] == 8 and after["revisions"] == 17
              and before["revisions"] == after["revisions"],
              json.dumps(after, ensure_ascii=False))
        code, out, err = run_entity(["ingest", *L, "--batch", ws.batch("b5_tamper.jsonl")])
        check("13b. 篡改批重投仍然拒绝（退出3）", code == 3, err.strip())

        # ---- 14. 独立全量重算器 ----
        code, ver, _ = run_entity_json(["verify", *L])
        check("14. 独立重算器逐版本对照（8版本全ok/头部一致）",
              code == 0 and ver["ok"] and ver["versions_checked"] == 8
              and ver["head_check_ok"], json.dumps(ver, ensure_ascii=False))

        # ---- 与场景文件的期望终态对照 ----
        code, sv8, _ = run_entity_json(["show-version", *L, "--version", "8"])
        final = {e["entity_id"]: e["members"] for e in sv8["entities"]}
        check("15. 终态分组与场景期望一致",
              final == ws.scenario["expected_final_entities"],
              json.dumps(final, ensure_ascii=False))

    finally:
        ws.cleanup()

    failed = [r for r in results if not r[1]]
    print(f"\n实体归并层演示：{len(results) - len(failed)}/{len(results)} 项通过")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
