"""采样覆盖审计层命令行入口（只做数据质量计算，不含可视化界面）。

子命令
======

``ingest-contracts``   接收契约批次（不可变稿号，同稿篡改整批拒收）
``ingest-exemptions``  接收停机豁免批次（区间允许相交/包含，按并集扣除）
``status``             契约/豁免批次与审计总览
``audit``              固定四坐标执行一次覆盖审计并原子发布
``runs``               列出全部审计（含 building 残档）
``report``             取一份已发布审计的完整报告（分子/分母 + 五类明细）
``missing``            缺采应到项清单
``duplicates``         重复上报诊断
``unmatched``          未匹配读数及原因
``explain``            单个应到项溯源（日程/豁免/命中读数）
``verify``             独立核对程序全量对照（不调用正式展开/配对函数）
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import ingest, query, store
from .audit import SyncGate, require_published, run_audit
from .errors import CoverageError
from .fault import ALL_FAULTS, FaultInjector
from .verifier import verify_all


def _print(obj: Any) -> None:
    sys.stdout.write(
        json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )


def cmd_ingest_contracts(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(ingest.ingest_contract_batch(conn, args.batch))
        return 0
    finally:
        conn.close()


def cmd_ingest_exemptions(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(ingest.ingest_exemption_batch(conn, args.batch))
        return 0
    finally:
        conn.close()


def cmd_status(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        info = {
            "last_contract_batch_id": store.get_meta(conn, "last_contract_batch_id", 0),
            "last_exemption_batch_id": store.get_meta(conn, "last_exemption_batch_id", 0),
            "contract_drafts": conn.execute(
                "SELECT COUNT(*) AS c FROM contract_drafts"
            ).fetchone()["c"],
            "exemptions": conn.execute(
                "SELECT COUNT(*) AS c FROM exemptions"
            ).fetchone()["c"],
            "audit_runs": conn.execute(
                "SELECT COUNT(*) AS c FROM audit_runs"
            ).fetchone()["c"],
            "published_audits": conn.execute(
                "SELECT COUNT(*) AS c FROM audit_runs WHERE status='published'"
            ).fetchone()["c"],
            "stale_audits": conn.execute(
                "SELECT COUNT(*) AS c FROM audit_runs WHERE status!='published'"
            ).fetchone()["c"],
        }
        _print(info)
        return 0
    finally:
        conn.close()


def cmd_audit(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        gate = SyncGate(args.gate_dir, args.gate_timeout) if args.gate_dir else None
        fault = FaultInjector(args.fault) if args.fault else None
        report = run_audit(
            conn,
            args.reading_db,
            args.ledger_db,
            args.query_start,
            args.query_end,
            args.contract_cutoff,
            args.reading_cutoff,
            args.membership_version,
            exemption_cutoff_batch=args.exemption_cutoff,
            gate=gate,
            fault=fault,
        )
        _print(report)
        return 0
    finally:
        conn.close()


def cmd_runs(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(query.list_audits(conn, include_stale=True))
        return 0
    finally:
        conn.close()


def cmd_report(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(query.get_report(conn, args.run_id))
        return 0
    finally:
        conn.close()


def cmd_missing(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(query.list_missing(conn, args.run_id))
        return 0
    finally:
        conn.close()


def cmd_duplicates(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(query.list_duplicates(conn, args.run_id))
        return 0
    finally:
        conn.close()


def cmd_unmatched(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        require_published(conn, args.run_id)
        rows = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM audit_unmatched WHERE run_id=? ORDER BY reading_key",
                (args.run_id,),
            )
        ]
        for r in rows:
            r["candidates"] = json.loads(r["candidates"])
        _print(rows)
        return 0
    finally:
        conn.close()


def cmd_explain(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        _print(query.explain_item(conn, args.run_id, args.item_id))
        return 0
    finally:
        conn.close()


def cmd_verify(args: argparse.Namespace) -> int:
    conn = store.connect(args.coverage_db)
    try:
        report = verify_all(conn, args.reading_db, args.ledger_db)
        _print(report)
        return 0 if report["ok"] else 6
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="coverage",
        description="采样覆盖审计（契约日程展开/容差配对/缺采缺口/原子发布）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ic = sub.add_parser("ingest-contracts", help="接收契约批次")
    p_ic.add_argument("--coverage-db", required=True)
    p_ic.add_argument("--batch", nargs="+", required=True)
    p_ic.set_defaults(func=cmd_ingest_contracts)

    p_ie = sub.add_parser("ingest-exemptions", help="接收停机豁免批次")
    p_ie.add_argument("--coverage-db", required=True)
    p_ie.add_argument("--batch", nargs="+", required=True)
    p_ie.set_defaults(func=cmd_ingest_exemptions)

    p_st = sub.add_parser("status", help="审计层状态总览")
    p_st.add_argument("--coverage-db", required=True)
    p_st.set_defaults(func=cmd_status)

    p_au = sub.add_parser("audit", help="固定口径执行覆盖审计")
    p_au.add_argument("--coverage-db", required=True)
    p_au.add_argument("--reading-db", required=True, help="读数事实仓（只读）")
    p_au.add_argument("--ledger-db", required=True, help="归并层台账（只读）")
    p_au.add_argument("--query-start", required=True, help="查询起点（含，带时区）")
    p_au.add_argument("--query-end", required=True, help="查询终点（不含，带时区）")
    p_au.add_argument("--contract-cutoff", type=int, required=True)
    p_au.add_argument("--reading-cutoff", type=int, required=True)
    p_au.add_argument("--membership-version", type=int, required=True)
    p_au.add_argument("--exemption-cutoff", type=int, help="默认取当前最大豁免批次")
    p_au.add_argument("--gate-dir")
    p_au.add_argument("--gate-timeout", type=float, default=60.0)
    p_au.add_argument("--fault", choices=sorted(ALL_FAULTS))
    p_au.set_defaults(func=cmd_audit)

    p_runs = sub.add_parser("runs", help="列出全部审计（含残档）")
    p_runs.add_argument("--coverage-db", required=True)
    p_runs.set_defaults(func=cmd_runs)

    p_rep = sub.add_parser("report", help="完整审计报告")
    p_rep.add_argument("--coverage-db", required=True)
    p_rep.add_argument("--run-id", type=int, required=True)
    p_rep.set_defaults(func=cmd_report)

    p_mi = sub.add_parser("missing", help="缺采应到项清单")
    p_mi.add_argument("--coverage-db", required=True)
    p_mi.add_argument("--run-id", type=int, required=True)
    p_mi.set_defaults(func=cmd_missing)

    p_du = sub.add_parser("duplicates", help="重复上报诊断")
    p_du.add_argument("--coverage-db", required=True)
    p_du.add_argument("--run-id", type=int, required=True)
    p_du.set_defaults(func=cmd_duplicates)

    p_un = sub.add_parser("unmatched", help="未匹配读数及原因")
    p_un.add_argument("--coverage-db", required=True)
    p_un.add_argument("--run-id", type=int, required=True)
    p_un.set_defaults(func=cmd_unmatched)

    p_ex = sub.add_parser("explain", help="单个应到项溯源")
    p_ex.add_argument("--coverage-db", required=True)
    p_ex.add_argument("--run-id", type=int, required=True)
    p_ex.add_argument("--item-id", type=int, required=True)
    p_ex.set_defaults(func=cmd_explain)

    p_ve = sub.add_parser("verify", help="独立核对程序全量对照")
    p_ve.add_argument("--coverage-db", required=True)
    p_ve.add_argument("--reading-db", required=True)
    p_ve.add_argument("--ledger-db", required=True)
    p_ve.set_defaults(func=cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except CoverageError as exc:
        print(f"错误[{type(exc).__name__}]: {exc}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
