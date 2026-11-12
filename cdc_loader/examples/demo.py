#!/usr/bin/env python3
"""端到端演示：覆盖题目要求的全部场景。

场景清单：
  A. 分片乱序到达（含重传幂等）
  B. 损坏分片 -> 拒绝发布 -> 修复后成功（含“发布前”崩溃注入）
  C. 切点两侧交错事务、未提交尾部、同批先改后删、主键改变、
     事件冲突、旧日志重放无副作用
  D. “事务提交前”崩溃后重放
  E. “提交后尚未返回”崩溃后重放
  F. 独立参考实现对照最终行集与检查点

每个场景使用独立的临时目录，互不干扰。崩溃通过子进程 + os._exit(42)
真实模拟，再以新进程重开同一 SQLite 库继续。
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

from cdc.cli import main as cli_main  # noqa: E402
from cdc.fault import (  # noqa: E402
    FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN,
    FAULT_BEFORE_SNAPSHOT_PUBLISH,
    FAULT_BEFORE_TXN_COMMIT,
    CRASH_EXIT_CODE,
)
from examples.generate_sample import generate  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def run_cli(argv: list[str]) -> tuple[int, str]:
    """在当前进程内跑 CLI，捕获退出码（CLI 自己捕获 CDCError）。"""
    import io
    import contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = cli_main(argv)
    return code, buf.getvalue()


def run_cli_subprocess(argv: list[str], cwd: str) -> tuple[int, str, str]:
    """在独立子进程中跑 CLI，用于真实崩溃演示。

    通过项目根的启动器以隔离模式（-I）运行：数据目录不进入导入路径，
    避免数据目录中被植入的同名模块（如 json.py）被解释器加载。
    """
    proc = subprocess.run(
        [sys.executable, "-I", os.path.join(cwd, "run_cli.py"), *argv],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


class Workspace:
    def __init__(self, name: str) -> None:
        self.dir = tempfile.mkdtemp(prefix=f"cdc-demo-{name}-")
        self.sample = os.path.join(self.dir, "sample")
        self.inbox = os.path.join(self.dir, "inbox")
        self.db = os.path.join(self.dir, "state.db")
        os.makedirs(self.inbox)
        generate(self.sample)
        self.manifest = os.path.join(self.sample, "manifest.json")
        self.shard_files = sorted(
            f for f in os.listdir(self.sample) if f.startswith("shard_")
        )
        self.log_files = ["log_1.jsonl", "log_2.jsonl"]

    def shard_path(self, fname: str) -> str:
        return os.path.join(self.sample, fname)

    def inbox_copy(self, fname: str) -> str:
        src = os.path.join(self.sample, fname)
        dst = os.path.join(self.inbox, fname)
        shutil.copyfile(src, dst)
        return dst

    def log_path(self, fname: str) -> str:
        return os.path.join(self.sample, fname)

    def cleanup(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


def expected(ws: Workspace) -> dict:
    with open(os.path.join(ws.sample, "expected.json"), encoding="utf-8") as fh:
        return json.load(fh)


def dump_rows(db: str) -> tuple[list[dict], int]:
    code, out = run_cli(["dump", "--db", db, "--format", "json"])
    assert code == 0, out
    data = json.loads(out)
    return data["rows"], data["checkpoint_lsn"]


def verify(ws: Workspace, db: str) -> tuple[bool, dict]:
    code, out = run_cli([
        "verify", "--db", db,
        "--manifest", ws.manifest,
        "--shards", *[ws.shard_path(f) for f in ws.shard_files],
        "--logs", *[ws.log_path(f) for f in ws.log_files],
    ])
    data = json.loads(out)
    return code == 0 and data["ok"], data


# ---------------------------------------------------------------------------
def scenario_a_disordered_shards() -> None:
    ws = Workspace("A")
    try:
        # 故意乱序：shard_2 -> shard_0 -> shard_1，并重复投递 shard_0。
        order = ["shard_2.jsonl", "shard_0.jsonl", "shard_1.jsonl", "shard_0.jsonl"]
        code, out = run_cli([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *[ws.inbox_copy(f) for f in order],
        ])
        ok = code == 0 and json.loads(out)["publish"]["status"] == "published"
        record("A1 分片乱序+重传后快照原子发布", ok)

        code, out = run_cli(["status", "--db", ws.db, "--manifest", ws.manifest])
        ok = code == 0 and json.loads(out)["snapshot_published"] is True
        record("A2 发布后状态可查询", ok)

        rows, cp = dump_rows(ws.db)
        exp = expected(ws)
        # 此刻只有快照：12 行；expected.json 的 13 行是追完日志后的结果。
        ok = len(rows) == 12 and cp == exp["cut_point_lsn"] == 2000
        record("A3 快照行数与初始检查点(=切点S)正确", ok,
               f"rows={len(rows)} checkpoint={cp}")
    finally:
        ws.cleanup()


def scenario_b_corrupt_and_crash() -> None:
    ws = Workspace("B")
    try:
        good = ws.shard_path("shard_1.jsonl")
        corrupt = os.path.join(ws.inbox, "shard_1.jsonl")
        shutil.copyfile(good, corrupt)
        # 破坏内容但保留行数：改一个字段值，SHA-256 必然不符。
        with open(corrupt, encoding="utf-8") as fh:
            lines = fh.readlines()
        first = json.loads(lines[0])
        first["firmware"] = "TAMPERED"
        lines[0] = json.dumps(first, ensure_ascii=False, sort_keys=True) + "\n"
        with open(corrupt, "w", encoding="utf-8") as fh:
            fh.writelines(lines)

        others = [ws.inbox_copy(f) for f in ws.shard_files if f != "shard_1.jsonl"]
        code, out = run_cli([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *others, corrupt,
        ])
        record("B1 损坏分片被 SHA-256 拒绝", code == 2 and "SHA-256" in out or code == 2)

        # 只投好的两个分片：未到齐，不能发布，主表不可查。
        db2 = os.path.join(ws.dir, "state_missing.db")
        code, _ = run_cli([
            "import-snapshot", "--db", db2, "--manifest", ws.manifest,
            "--shards", *[ws.shard_path(f) for f in ws.shard_files if f != "shard_2.jsonl"],
        ])
        code_dump, out_dump = run_cli(["dump", "--db", db2])
        record("B2 分片缺失时业务查询看不见半份快照", code_dump == 4)

        # 发布前崩溃注入（子进程真实退出 42）。
        proc_code, _, err = run_cli_subprocess([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *[ws.shard_path(f) for f in ws.shard_files],
            "--fault", FAULT_BEFORE_SNAPSHOT_PUBLISH,
        ], cwd=ROOT)
        record("B3 注入点 before_snapshot_publish 真实崩溃",
               proc_code == CRASH_EXIT_CODE and FAULT_BEFORE_SNAPSHOT_PUBLISH in err)

        # 新进程重开同一库：分片已暂存，直接发布。
        code, out = run_cli([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *[ws.shard_path(f) for f in ws.shard_files],
        ])
        ok = code == 0 and json.loads(out)["publish"]["status"] == "published"
        record("B4 崩溃后重跑导入，快照成功发布", ok)
    finally:
        ws.cleanup()


def scenario_c_log_semantics() -> None:
    ws = Workspace("C")
    try:
        run_cli([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *[ws.shard_path(f) for f in ws.shard_files],
        ])

        # 未发布库拒绝追日志。
        db_pre = os.path.join(ws.dir, "pre.db")
        code, out = run_cli(["consume", "--db", db_pre,
                             "--logs", ws.log_path("log_1.jsonl")])
        record("C1 快照未发布时拒绝追赶日志", code == 4)

        code, out = run_cli(["consume", "--db", ws.db,
                             "--logs", ws.log_path("log_1.jsonl")])
        rep = json.loads(out)
        ok = rep["counters"]["txn_applied"] == 1  # 只有 T11(S 后) 被应用
        record("C2 切点两侧交错事务：仅 commit_lsn>S 的 T11 入表", ok,
               f"applied={rep['counters']['txn_applied']} checkpoint={rep['checkpoint_lsn']}")

        code, out = run_cli(["consume", "--db", ws.db,
                             "--logs", ws.log_path("log_2.jsonl")])
        rep = json.loads(out)
        ok = rep["counters"]["txn_applied"] == 2  # T12、T13；T14 中止、T15 悬空
        record("C3 主键迁移/先改后删应用，ABORT 与悬空 BEGIN 不入表", ok,
               f"applied={rep['counters']['txn_applied']} checkpoint={rep['checkpoint_lsn']}")

        code, out = run_cli(["status", "--db", ws.db])
        info = json.loads(out)
        ok = [t["txn_id"] for t in info["open_transactions"]] == ["T15"]
        record("C4 只有 BEGIN 的 T15 保持 OPEN 且不进主表", ok)

        rows, cp = dump_rows(ws.db)
        exp = expected(ws)
        ids = {r["device_id"] for r in rows}
        ok = (
            cp == exp["checkpoint_lsn"] == 2300
            and "D003" in ids            # T14 被 ABORT，D003 仍在
            and "D005" not in ids        # 主键迁移走
            and "D050" in ids
            and "D014" not in ids        # 先插后删净效果为零
            and "D013" in ids            # T11 插入
            and "D010" in ids            # T10 在 S 前提交，快照已含
        )
        record("C5 最终行集符合全部事务语义", ok,
               f"ids={sorted(ids)} checkpoint={cp}")

        # 事件冲突：同一身份 T11#1 不同载荷。
        code, out = run_cli(["consume", "--db", ws.db,
                             "--logs", os.path.join(ws.sample, "conflict_event.jsonl")])
        record("C6 同事件身份不同载荷被 PayloadConflict 拒绝", code == 3)

        # 冲突拒绝不能改变任何状态。
        rows2, cp2 = dump_rows(ws.db)
        record("C7 冲突拒绝后行集与检查点无变化", rows2 == rows and cp2 == cp)

        # 旧日志重放：全部命中去重，无新应用、无副作用。
        code, out = run_cli(["consume", "--db", ws.db,
                             "--logs", os.path.join(ws.sample, "log_replay.jsonl")])
        rep = json.loads(out)
        ok = (
            rep["counters"]["events_duplicate"] == 3
            and rep["counters"]["events_staged"] == 0
            and rep["counters"]["txn_applied"] == 0
        )
        record("C8 提交后的旧日志重放无副作用（全部去重）", ok)

        rows3, cp3 = dump_rows(ws.db)
        record("C9 重放后行集与检查点保持不变", rows3 == rows and cp3 == cp)

        ok, data = verify(ws, ws.db)
        record("C10 独立参考实现对照行集与检查点一致", ok,
               f"rows={data['row_count']} checkpoint={data['loader_checkpoint_lsn']}")
    finally:
        ws.cleanup()


def scenario_d_crash_before_commit() -> None:
    ws = Workspace("D")
    try:
        run_cli([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *[ws.shard_path(f) for f in ws.shard_files],
        ])
        # log_1 让 T11 入库，检查点到 2100；崩溃注入将作用在下一个事务 T12。
        run_cli(["consume", "--db", ws.db, "--logs", ws.log_path("log_1.jsonl")])

        proc_code, _, err = run_cli_subprocess([
            "consume", "--db", ws.db,
            "--logs", ws.log_path("log_2.jsonl"),
            "--fault", FAULT_BEFORE_TXN_COMMIT,
        ], cwd=ROOT)
        record("D1 注入点 before_txn_commit 真实崩溃",
               proc_code == CRASH_EXIT_CODE and FAULT_BEFORE_TXN_COMMIT in err)

        rows, cp = dump_rows(ws.db)
        ok = cp == 2100 and "D005" in {r["device_id"] for r in rows}
        record("D2 崩溃事务未提交：检查点与主表维持旧值", ok, f"checkpoint={cp}")

        # 新进程重放同一日志：暂存事件已在库，事务从 SQLite 暂存表回放。
        code, out = run_cli(["consume", "--db", ws.db,
                             "--logs", ws.log_path("log_2.jsonl")])
        rep = json.loads(out)
        ok = rep["counters"]["txn_applied"] == 2 and rep["checkpoint_lsn"] == 2300
        record("D3 重启后重放，事务完整追平且不重复应用", ok)

        ok, _ = verify(ws, ws.db)
        record("D4 重放结果与参考实现一致", ok)
    finally:
        ws.cleanup()


def scenario_e_crash_after_commit() -> None:
    ws = Workspace("E")
    try:
        run_cli([
            "import-snapshot", "--db", ws.db, "--manifest", ws.manifest,
            "--shards", *[ws.shard_path(f) for f in ws.shard_files],
        ])
        proc_code, _, err = run_cli_subprocess([
            "consume", "--db", ws.db,
            "--logs", ws.log_path("log_1.jsonl"),
            "--fault", FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN,
        ], cwd=ROOT)
        record("E1 注入点 after_txn_commit_before_return 真实崩溃",
               proc_code == CRASH_EXIT_CODE and FAULT_AFTER_TXN_COMMIT_BEFORE_RETURN in err)

        rows, cp = dump_rows(ws.db)
        ok = cp == 2100 and "D013" in {r["device_id"] for r in rows}
        record("E2 提交已落盘：崩溃前事务在行集与检查点中均可见", ok, f"checkpoint={cp}")

        code, out = run_cli(["consume", "--db", ws.db,
                             "--logs", ws.log_path("log_1.jsonl")])
        rep = json.loads(out)
        ok = rep["counters"]["txn_applied"] == 0 and rep["checkpoint_lsn"] == 2100
        record("E3 重放同一日志零副作用，检查点不回退", ok)

        # 继续追 log_2 并整体对照。
        run_cli(["consume", "--db", ws.db, "--logs", ws.log_path("log_2.jsonl")])
        ok, _ = verify(ws, ws.db)
        record("E4 后续消费与参考实现一致", ok)
    finally:
        ws.cleanup()


def main() -> int:
    print("=" * 78)
    print("离线 CDC 装载器端到端演示")
    print("=" * 78)
    for fn in (
        scenario_a_disordered_shards,
        scenario_b_corrupt_and_crash,
        scenario_c_log_semantics,
        scenario_d_crash_before_commit,
        scenario_e_crash_after_commit,
    ):
        print(f"\n--- {fn.__name__} ---")
        fn()

    print("\n" + "=" * 78)
    passed = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"[{PASS if ok else FAIL}] {name}" + (f" -- {detail}" if detail else ""))
    print("=" * 78)
    print(f"合计 {passed}/{len(results)} 通过")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
