"""实际运行入口（命令行）。

子命令
======

``import-snapshot``  暂存分片并在完整时发布快照
``consume``          继续消费日志文件（可多次调用，支持断点续传）
``dump``             读取当前主表（JSON / JSONL / 表格文本）与检查点
``status``           查看发布状态、分片到达情况、未闭合事务
``verify``           用独立参考实现对照最终行集与检查点

所有命令都只依赖标准库，建议用 ``python -I -m cdc.cli ...`` 运行，
避免当前目录下被植入的同名模块影响导入。
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import log_ingest, snapshot, store
from .errors import CDCError
from .fault import ALL_FAULTS, FaultInjector
from .reference import run_reference
from .store import connect


def _print(obj: Any) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def cmd_import(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        if snapshot.is_published(conn) and not args.force:
            _print(
                {
                    "status": "already_published",
                    "cut_point_lsn": store.get_meta(conn, "snapshot_meta")["cut_point_lsn"],
                }
            )
            return 0
        fault = FaultInjector(args.fault) if args.fault else None
        report = snapshot.import_snapshot(conn, args.manifest, args.shards, fault)
        _print(report)
        return 0
    finally:
        conn.close()


def cmd_consume(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        fault = FaultInjector(args.fault) if args.fault else None
        report = log_ingest.ingest_logs(conn, args.logs, fault)
        _print(report)
        return 0
    finally:
        conn.close()


def _load_master(conn) -> list[dict]:
    return [
        json.loads(r["payload"])
        for r in conn.execute(
            "SELECT payload FROM device_master ORDER BY device_id ASC"
        ).fetchall()
    ]


def cmd_dump(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        if not snapshot.is_published(conn):
            print("错误：快照尚未发布，主表不可查询", file=sys.stderr)
            return 4
        rows = _load_master(conn)
        checkpoint = store.get_meta(conn, "checkpoint_lsn")
        if args.format == "json":
            _print({"checkpoint_lsn": checkpoint, "count": len(rows), "rows": rows})
        elif args.format == "jsonl":
            for row in rows:
                sys.stdout.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            sys.stdout.write(json.dumps({"_checkpoint_lsn": checkpoint}, sort_keys=True) + "\n")
        else:
            fields = store.get_meta(conn, "snapshot_meta")["fields"]
            header = [f["name"] for f in fields]
            print("\t".join(header + ["# checkpoint_lsn=" + str(checkpoint)]))
            for row in rows:
                print("\t".join("" if row.get(h) is None else str(row[h]) for h in header))
            print(f"# {len(rows)} rows")
        return 0
    finally:
        conn.close()


def cmd_status(args: argparse.Namespace) -> int:
    conn = connect(args.db)
    try:
        published = snapshot.is_published(conn)
        info: dict[str, Any] = {"snapshot_published": published}
        if published:
            manifest_meta = store.get_meta(conn, "snapshot_meta")
            info["cut_point_lsn"] = manifest_meta["cut_point_lsn"]
            info["checkpoint_lsn"] = store.get_meta(conn, "checkpoint_lsn")
            info["master_rows"] = conn.execute(
                "SELECT COUNT(*) AS c FROM device_master"
            ).fetchone()["c"]
            info["open_transactions"] = log_ingest.open_transactions(conn)
            info["staged_events"] = conn.execute(
                "SELECT COUNT(*) AS c FROM log_events"
            ).fetchone()["c"]
            info["staged_txns"] = conn.execute(
                "SELECT COUNT(*) AS c FROM log_txn"
            ).fetchone()["c"]
        else:
            try:
                manifest = snapshot.load_manifest(args.manifest)
                info["shards"] = snapshot.shard_status(conn, manifest)
            except CDCError as exc:
                info["manifest_error"] = str(exc)
        _print(info)
        return 0
    finally:
        conn.close()


def cmd_verify(args: argparse.Namespace) -> int:
    ref = run_reference(args.manifest, args.shards, args.logs)
    conn = connect(args.db)
    try:
        if not snapshot.is_published(conn):
            print("错误：装载器快照未发布，无法对照", file=sys.stderr)
            return 4
        actual_rows = {r["device_id"]: r for r in _load_master(conn)}
        ref_rows = ref["rows"]
        actual_cp = store.get_meta(conn, "checkpoint_lsn")

        problems: list[str] = []
        if set(actual_rows) != set(ref_rows):
            only_db = sorted(set(actual_rows) - set(ref_rows))
            only_ref = sorted(set(ref_rows) - set(actual_rows))
            problems.append(f"主键集合不一致: 仅库中有={only_db} 仅参考中有={only_ref}")
        for pk in sorted(set(actual_rows) & set(ref_rows)):
            if actual_rows[pk] != ref_rows[pk]:
                problems.append(
                    f"行内容不一致 device={pk}: 库={json.dumps(actual_rows[pk], sort_keys=True)} "
                    f"参考={json.dumps(ref_rows[pk], sort_keys=True)}"
                )
        if actual_cp != ref["checkpoint_lsn"]:
            problems.append(f"检查点不一致: 库={actual_cp} 参考={ref['checkpoint_lsn']}")

        result = {
            "ok": not problems,
            "problems": problems,
            "loader_checkpoint_lsn": actual_cp,
            "reference_checkpoint_lsn": ref["checkpoint_lsn"],
            "row_count": len(actual_rows),
            "reference_row_count": len(ref_rows),
            "reference_applied_txns": ref["applied_txn_ids"],
            "reference_open_txns": ref["open_txn_ids"],
            "reference_aborted_txns": ref["aborted_txn_ids"],
        }
        _print(result)
        return 0 if not problems else 6
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc", description="离线 CDC 装载器（快照 + 变更日志 -> SQLite 设备主数据）"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_imp = sub.add_parser("import-snapshot", help="暂存分片并发布完整快照")
    p_imp.add_argument("--db", required=True, help="SQLite 状态库路径")
    p_imp.add_argument("--manifest", required=True, help="快照清单 JSON 路径")
    p_imp.add_argument("--shards", nargs="+", required=True, help="分片 JSONL 路径（可乱序）")
    p_imp.add_argument("--fault", choices=sorted(ALL_FAULTS))
    p_imp.add_argument("--force", action="store_true", help="已发布时仍重新暂存重传分片")
    p_imp.set_defaults(func=cmd_import)

    p_con = sub.add_parser("consume", help="继续消费变更日志（断点续传、幂等）")
    p_con.add_argument("--db", required=True)
    p_con.add_argument("--logs", nargs="+", required=True, help="JSONL 日志路径（可多次传入）")
    p_con.add_argument("--fault", choices=sorted(ALL_FAULTS))
    p_con.set_defaults(func=cmd_consume)

    p_dump = sub.add_parser("dump", help="读取当前设备主表与检查点")
    p_dump.add_argument("--db", required=True)
    p_dump.add_argument("--format", choices=["json", "jsonl", "table"], default="json")
    p_dump.set_defaults(func=cmd_dump)

    p_st = sub.add_parser("status", help="查看装载状态")
    p_st.add_argument("--db", required=True)
    p_st.add_argument("--manifest", help="未发布时用于对照分片到达情况")
    p_st.set_defaults(func=cmd_status)

    p_ver = sub.add_parser("verify", help="用独立参考实现对照行集与检查点")
    p_ver.add_argument("--db", required=True)
    p_ver.add_argument("--manifest", required=True)
    p_ver.add_argument("--shards", nargs="+", required=True)
    p_ver.add_argument("--logs", nargs="+", required=True)
    p_ver.set_defaults(func=cmd_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except CDCError as exc:
        print(f"错误[{type(exc).__name__}]: {exc}", file=sys.stderr)
        return exc.exit_code


if __name__ == "__main__":
    sys.exit(main())
