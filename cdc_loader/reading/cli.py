"""读数事实仓与按日指标投影的命令行入口。

子命令
======

``ingest``        接收一次读数投递（确认批次，原子落事实仓）
``status``        事实仓状态（批次、稿件、观测数、统计 run 总览）
``build``         按固定口径 (截止批次, 分组版本) 生成并发布统计
``runs``          列出全部统计口径（含 building 残档）
``cells``         按口径查按日指标格子（可按实体/日期/指标过滤）
``drill``         从一个格子下钻到读数身份、采用稿件与归属依据
``unassigned``    待归属/撤回清单（未计入格子的观测及原因）
``audit``         一条观测的全部稿件事实（选版与篡改审计）
``verify``        独立离线核算器：全量对照格子/下钻/待归属

建议以 ``python3 -I run_reading.py ...`` 隔离模式运行。
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import ingest, query, store
from .build import SyncGate, build_run
from .errors import ReadingError
from .fault import ALL_FAULTS, FaultInjector
from .reconcile import run_verify


def _print(obj: Any) -> None:
    sys.stdout.write(
        json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )


def cmd_ingest(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        _print(ingest.ingest_batch(conn, args.batch))
        return 0
    finally:
        conn.close()


def cmd_status(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        info = {
            "last_batch_id": ingest.max_batch_id(conn),
            "batches": conn.execute(
                "SELECT COUNT(*) AS c FROM ingest_batches"
            ).fetchone()["c"],
            "editions": conn.execute(
                "SELECT COUNT(*) AS c FROM reading_editions"
            ).fetchone()["c"],
            "observations": conn.execute(
                "SELECT COUNT(*) AS c FROM "
                "(SELECT DISTINCT origin, reading_key FROM reading_editions)"
            ).fetchone()["c"],
            "stats_runs": conn.execute(
                "SELECT COUNT(*) AS c FROM stats_runs"
            ).fetchone()["c"],
            "published_runs": conn.execute(
                "SELECT COUNT(*) AS c FROM stats_runs WHERE status='published'"
            ).fetchone()["c"],
            "current_caliber": query.current_caliber(conn),
        }
        _print(info)
        return 0
    finally:
        conn.close()


def cmd_build(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        gate = SyncGate(args.gate_dir, args.gate_timeout) if args.gate_dir else None
        fault = FaultInjector(args.fault) if args.fault else None
        report = build_run(
            conn,
            args.ledger_db,
            args.cutoff_batch,
            args.grouping_version,
            gate=gate,
            fault=fault,
        )
        _print(report)
        return 0
    finally:
        conn.close()


def cmd_runs(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        _print(query.list_runs(conn))
        return 0
    finally:
        conn.close()


def _caliber_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--cutoff-batch", type=int, required=True, help="截止批次号")
    p.add_argument("--grouping-version", type=int, required=True, help="归并层分组版本号")


def cmd_cells(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        _print(
            query.query_cells(
                conn,
                args.cutoff_batch,
                args.grouping_version,
                entity_id=args.entity,
                utc_date=args.date,
                metric=args.metric,
            )
        )
        return 0
    finally:
        conn.close()


def cmd_drill(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        _print(
            query.drill_cell(
                conn,
                args.cutoff_batch,
                args.grouping_version,
                args.entity,
                args.date,
                args.metric,
            )
        )
        return 0
    finally:
        conn.close()


def cmd_unassigned(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        _print(
            query.list_unassigned(
                conn, args.cutoff_batch, args.grouping_version, outcome=args.outcome
            )
        )
        return 0
    finally:
        conn.close()


def cmd_audit(args: argparse.Namespace) -> int:
    conn = store.connect(args.reading_db)
    try:
        _print(query.fact_audit(conn, args.origin, args.reading_key))
        return 0
    finally:
        conn.close()


def cmd_verify(args: argparse.Namespace) -> int:
    report = run_verify(args.reading_db, args.ledger_db)
    _print(report)
    return 0 if report["ok"] else 6


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reading",
        description="振动读数事实仓 + 按日指标投影（口径冻结/精确十进制/原子发布）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="接收一次读数投递（确认批次）")
    p_ing.add_argument("--reading-db", required=True)
    p_ing.add_argument("--batch", nargs="+", required=True, help="读数 JSONL（可多个文件）")
    p_ing.set_defaults(func=cmd_ingest)

    p_st = sub.add_parser("status", help="事实仓状态总览")
    p_st.add_argument("--reading-db", required=True)
    p_st.set_defaults(func=cmd_status)

    p_b = sub.add_parser("build", help="按固定口径生成并发布统计")
    p_b.add_argument("--reading-db", required=True)
    p_b.add_argument("--ledger-db", required=True, help="归并层台账库（只读复用成员）")
    _caliber_args(p_b)
    p_b.add_argument("--gate-dir", help="进程间同步门目录（测试用；生产不传）")
    p_b.add_argument("--gate-timeout", type=float, default=60.0)
    p_b.add_argument("--fault", choices=sorted(ALL_FAULTS))
    p_b.set_defaults(func=cmd_build)

    p_runs = sub.add_parser("runs", help="列出全部统计口径登记")
    p_runs.add_argument("--reading-db", required=True)
    p_runs.set_defaults(func=cmd_runs)

    p_cells = sub.add_parser("cells", help="按口径查按日指标格子")
    p_cells.add_argument("--reading-db", required=True)
    _caliber_args(p_cells)
    p_cells.add_argument("--entity")
    p_cells.add_argument("--date")
    p_cells.add_argument("--metric")
    p_cells.set_defaults(func=cmd_cells)

    p_drill = sub.add_parser("drill", help="从格子下钻到读数身份/采用稿件/归属依据")
    p_drill.add_argument("--reading-db", required=True)
    _caliber_args(p_drill)
    p_drill.add_argument("--entity", required=True)
    p_drill.add_argument("--date", required=True)
    p_drill.add_argument("--metric", required=True)
    p_drill.set_defaults(func=cmd_drill)

    p_un = sub.add_parser("unassigned", help="待归属/撤回清单")
    p_un.add_argument("--reading-db", required=True)
    _caliber_args(p_un)
    p_un.add_argument("--outcome", choices=["unassigned", "withdrawn"])
    p_un.set_defaults(func=cmd_unassigned)

    p_au = sub.add_parser("audit", help="一条观测的全部稿件事实")
    p_au.add_argument("--reading-db", required=True)
    p_au.add_argument("--origin", required=True)
    p_au.add_argument("--reading-key", required=True)
    p_au.set_defaults(func=cmd_audit)

    p_ve = sub.add_parser("verify", help="独立离线核算器全量对照")
    p_ve.add_argument("--reading-db", required=True)
    p_ve.add_argument("--ledger-db", required=True)
    p_ve.set_defaults(func=cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ReadingError as exc:
        print(f"错误[{type(exc).__name__}]: {exc}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
