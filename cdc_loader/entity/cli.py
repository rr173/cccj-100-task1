"""实体归并层命令行入口。

子命令
======

``ingest``        摄入一次投递（一个不可分割的决策批次）
``refresh``       从装载器 device_master 同步在场设备并重算决策
``status``        台账状态（版本、批次、在场设备、暂扣关系）
``explain``       从某设备解释当前（或指定）版本的成员及依据
``history``       决策版本列表
``show-version``  查看某版本的完整分组
``changes``       相邻版本变动（合并/拆分/出现/消失/成员增减）
``verify``        独立全量重算器逐版本对照完整分组与冲突结论

建议以 ``python3 -I run_entity.py ...`` 隔离模式运行。
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import ledger, store
from .engine import compute_decision
from .errors import ConstraintConflict, EntityError
from .fault import ALL_FAULTS, FaultInjector
from .recompute import run_verify


def _print(obj: Any) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def cmd_ingest(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        fault = FaultInjector(args.fault) if args.fault else None
        report = ledger.ingest_batch(conn, args.batch, fault)
        _print(report)
        return 0
    finally:
        conn.close()


def cmd_refresh(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        report = ledger.refresh_presence(conn, args.loader_db)
        _print(report)
        return 0
    finally:
        conn.close()


def cmd_status(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        current = ledger.current_version(conn)
        info: dict[str, Any] = {
            "presence_initialized": bool(
                store.get_meta(conn, "presence_initialized", False)
            ),
            "last_batch_id": int(store.get_meta(conn, "last_batch_id", 0)),
            "current_version": current,
            "batches": conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"],
            "revisions": conn.execute(
                "SELECT COUNT(*) AS c FROM revisions"
            ).fetchone()["c"],
            "relations": conn.execute(
                "SELECT COUNT(DISTINCT source || char(0) || relation_id) AS c "
                "FROM revisions"
            ).fetchone()["c"],
        }
        if info["presence_initialized"]:
            devices = ledger.presence_devices(conn)
            decision = compute_decision(devices, ledger.load_active_relations(conn))
            info["device_count"] = len(devices)
            info["entity_count"] = len(decision["entities"])
            info["active_same_entity"] = len(decision["edges"])
            info["active_must_differ"] = len(decision["constraints"])
            info["parked_relations"] = [ledger._rel_desc(r) for r in decision["parked"]]
        _print(info)
        return 0
    finally:
        conn.close()


def cmd_explain(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        _print(ledger.explain_device(conn, args.device, args.version))
        return 0
    finally:
        conn.close()


def cmd_history(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        _print({"versions": ledger.list_versions(conn)})
        return 0
    finally:
        conn.close()


def cmd_show_version(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        vrow = ledger.get_version_row(conn, args.version)
        entities = ledger.version_entities(conn, args.version)
        _print(
            {
                "version": args.version,
                "cause": vrow["cause"],
                "batch_id": vrow["batch_id"],
                "ledger_seq": vrow["ledger_seq"],
                "grouping_hash": vrow["grouping_hash"],
                "entities": [
                    {"entity_id": eid, "members": members}
                    for eid, members in sorted(entities.items())
                ],
            }
        )
        return 0
    finally:
        conn.close()


def cmd_changes(args: argparse.Namespace) -> int:
    conn = store.connect(args.ledger_db)
    try:
        _print(ledger.changes_into(conn, args.version))
        return 0
    finally:
        conn.close()


def cmd_verify(args: argparse.Namespace) -> int:
    report = run_verify(args.ledger_db)
    _print(report)
    return 0 if report["ok"] else 6


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="entity",
        description="设备实体归并层（对照资料 -> SQLite 关系台账 -> 决策版本）",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest", help="摄入一次投递（原子决策批次）")
    p_ing.add_argument("--ledger-db", required=True, help="实体台账 SQLite 路径")
    p_ing.add_argument("--batch", nargs="+", required=True, help="对照资料 JSONL（可多个文件）")
    p_ing.add_argument("--fault", choices=sorted(ALL_FAULTS))
    p_ing.set_defaults(func=cmd_ingest)

    p_ref = sub.add_parser("refresh", help="从 device_master 同步在场设备并重算")
    p_ref.add_argument("--ledger-db", required=True)
    p_ref.add_argument("--loader-db", required=True, help="装载器状态库路径（只读）")
    p_ref.set_defaults(func=cmd_refresh)

    p_st = sub.add_parser("status", help="台账状态总览")
    p_st.add_argument("--ledger-db", required=True)
    p_st.set_defaults(func=cmd_status)

    p_ex = sub.add_parser("explain", help="从某设备解释实体成员及依据")
    p_ex.add_argument("--ledger-db", required=True)
    p_ex.add_argument("--device", required=True)
    p_ex.add_argument("--version", type=int, help="缺省为当前版本")
    p_ex.set_defaults(func=cmd_explain)

    p_hi = sub.add_parser("history", help="决策版本列表")
    p_hi.add_argument("--ledger-db", required=True)
    p_hi.set_defaults(func=cmd_history)

    p_sv = sub.add_parser("show-version", help="查看某版本的完整分组")
    p_sv.add_argument("--ledger-db", required=True)
    p_sv.add_argument("--version", type=int, required=True)
    p_sv.set_defaults(func=cmd_show_version)

    p_ch = sub.add_parser("changes", help="相邻版本变动（version-1 -> version）")
    p_ch.add_argument("--ledger-db", required=True)
    p_ch.add_argument("--version", type=int, required=True)
    p_ch.set_defaults(func=cmd_changes)

    p_ve = sub.add_parser("verify", help="独立全量重算器逐版本对照")
    p_ve.add_argument("--ledger-db", required=True)
    p_ve.set_defaults(func=cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ConstraintConflict as exc:
        # 冲突报告必须可机读：完整 JSON（含冲突约束与关系路径）输出到 stdout。
        if exc.report:
            _print(exc.report)
        print(f"错误[{type(exc).__name__}]: {exc}", file=sys.stderr)
        return exc.exit_code
    except EntityError as exc:
        print(f"错误[{type(exc).__name__}]: {exc}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
