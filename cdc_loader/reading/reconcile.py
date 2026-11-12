"""独立离线核算器：只依赖标准库，**不 import 读数仓的任何其它模块**。

按本模块公开的规则独立重算，对读数仓中**每一个已发布 run**：

  1. 用口径的 cutoff_batch_id 重新扫描 reading_editions，在截止范围内
     为每条观测选最大 edition（独立实现选版，不读 stats_observations）；
  2. 只读打开归并层台账，取 grouping_version 的成员快照做设备归属，
     设备不在该版成员 -> 独立判定为待归属；
  3. 用 Decimal 独立聚合 (实体, UTC 日期, 指标) 的条数/精确和/极值；
  4. 与 stats_cells 逐格比对、与 stats_observations 逐条下钻明细比对、
     与 stats_attribution 的待归属/撤回记录逐条比对；
  5. 复核计数口径（observation/included/withdrawn/unassigned/cell）。

比对覆盖**所有格子、全部下钻明细与每一条待归属记录**，不是只核对
总行数。任何不一致逐条列出并以退出码 6 结束。building 残档不参与
对照（它们本就不对在线查询可见），但会在报告中单独列出。
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal


def _canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _load_grouping(ledger_conn: sqlite3.Connection, version: int) -> dict:
    entity_of: dict[str, str] = {}
    for r in ledger_conn.execute(
        "SELECT entity_id, device_id FROM version_entities WHERE version=?",
        (version,),
    ):
        entity_of[r["device_id"]] = r["entity_id"]
    return entity_of


def _independent_latest(conn: sqlite3.Connection, cutoff: int) -> dict:
    """独立选版：截止批次内每条观测取最大 edition。"""
    latest: dict[tuple, dict] = {}
    rows = conn.execute(
        "SELECT origin, reading_key, edition, device, occurred_at, "
        "occurred_date, metric, amount, withdrawn, ingest_batch_id "
        "FROM reading_editions WHERE ingest_batch_id <= ? "
        "ORDER BY origin, reading_key, edition",
        (cutoff,),
    ).fetchall()
    for r in rows:
        key = (r["origin"], r["reading_key"])
        prev = latest.get(key)
        if prev is None or r["edition"] > prev["edition"]:
            latest[key] = dict(r)
    return latest


def _recompute_run(
    conn: sqlite3.Connection, ledger_conn: sqlite3.Connection, run: sqlite3.Row
) -> dict:
    cutoff = int(run["cutoff_batch_id"])
    gv = int(run["grouping_version"])
    entity_of = _load_grouping(ledger_conn, gv)
    latest = _independent_latest(conn, cutoff)

    cells: dict[tuple, dict] = {}
    expect_observations: dict[tuple, dict] = {}
    expect_unassigned: list[dict] = []
    withdrawn = 0
    unassigned = 0

    for (origin, reading_key), r in sorted(latest.items()):
        is_withdrawn = bool(r["withdrawn"])
        expect_observations[(origin, reading_key)] = {
            "chosen_edition": r["edition"],
            "device": r["device"],
            "occurred_at": r["occurred_at"],
            "occurred_date": r["occurred_date"],
            "metric": r["metric"],
            "amount": r["amount"],
            "withdrawn": 1 if is_withdrawn else 0,
            "ingest_batch_id": r["ingest_batch_id"],
        }
        if is_withdrawn:
            withdrawn += 1
            expect_unassigned.append(
                {
                    "origin": origin,
                    "reading_key": reading_key,
                    "outcome": "withdrawn",
                    "device": r["device"],
                }
            )
            continue
        entity_id = entity_of.get(r["device"])
        if entity_id is None:
            unassigned += 1
            expect_unassigned.append(
                {
                    "origin": origin,
                    "reading_key": reading_key,
                    "outcome": "unassigned",
                    "device": r["device"],
                }
            )
            continue
        key = (entity_id, r["occurred_date"], r["metric"])
        amount = Decimal(r["amount"])
        st = cells.get(key)
        if st is None:
            cells[key] = {
                "row_count": 1,
                "amount_sum": amount,
                "amount_min": amount,
                "amount_max": amount,
            }
        else:
            st["row_count"] += 1
            st["amount_sum"] += amount
            st["amount_min"] = min(st["amount_min"], amount)
            st["amount_max"] = max(st["amount_max"], amount)

    return {
        "cells": cells,
        "observations": expect_observations,
        "unassigned": expect_unassigned,
        "counts": {
            "observation_count": len(latest),
            "included_count": sum(s["row_count"] for s in cells.values()),
            "withdrawn_count": withdrawn,
            "unassigned_count": unassigned,
            "cell_count": len(cells),
        },
    }


def _check_cells(
    conn: sqlite3.Connection, run_id: int, expect: dict, problems: list
) -> None:
    stored_rows = conn.execute(
        "SELECT entity_id, utc_date, metric, row_count, amount_sum, "
        "amount_min, amount_max FROM stats_cells WHERE run_id=? "
        "ORDER BY entity_id, utc_date, metric",
        (run_id,),
    ).fetchall()
    stored = {
        (r["entity_id"], r["utc_date"], r["metric"]): dict(r) for r in stored_rows
    }
    if set(stored) != set(expect["cells"]):
        problems.append(
            f"run {run_id} 格子集合不一致: 仅库中={sorted(set(stored) - set(expect['cells']))} "
            f"仅重算={sorted(set(expect['cells']) - set(stored))}"
        )
    for key, st in expect["cells"].items():
        got = stored.get(key)
        if got is None:
            continue
        want = {
            "row_count": st["row_count"],
            "amount_sum": str(st["amount_sum"]),
            "amount_min": str(st["amount_min"]),
            "amount_max": str(st["amount_max"]),
        }
        got_cmp = {
            "row_count": got["row_count"],
            "amount_sum": got["amount_sum"],
            "amount_min": got["amount_min"],
            "amount_max": got["amount_max"],
        }
        if want != got_cmp:
            problems.append(f"run {run_id} 格子 {key} 聚合不一致: 库={got_cmp} 重算={want}")


def _check_observations(
    conn: sqlite3.Connection, run_id: int, expect: dict, problems: list
) -> None:
    stored_rows = conn.execute(
        "SELECT origin, reading_key, chosen_edition, device, occurred_at, "
        "occurred_date, metric, amount, withdrawn, ingest_batch_id "
        "FROM stats_observations WHERE run_id=?",
        (run_id,),
    ).fetchall()
    stored = {(r["origin"], r["reading_key"]): dict(r) for r in stored_rows}
    if set(stored) != set(expect["observations"]):
        problems.append(
            f"run {run_id} 下钻观测集合不一致: 条数 库={len(stored)} "
            f"重算={len(expect['observations'])}"
        )
    for key, want in expect["observations"].items():
        got = stored.get(key)
        if got is None:
            continue
        for field, value in want.items():
            if got[field] != value:
                problems.append(
                    f"run {run_id} 观测 {key} 字段 {field} 不一致: "
                    f"库={got[field]} 重算={value}"
                )


def _check_attribution(
    conn: sqlite3.Connection, run_id: int, expect: dict, problems: list
) -> None:
    rows = conn.execute(
        "SELECT origin, reading_key, device, assigned, entity_id, basis "
        "FROM stats_attribution WHERE run_id=?",
        (run_id,),
    ).fetchall()
    stored_unassigned = []
    for r in rows:
        basis = json.loads(r["basis"])
        if not r["assigned"]:
            stored_unassigned.append(
                {
                    "origin": r["origin"],
                    "reading_key": r["reading_key"],
                    "outcome": basis.get("outcome"),
                    "device": r["device"],
                }
            )
        elif not r["entity_id"] or basis.get("outcome") != "assigned":
            problems.append(
                f"run {run_id} 归属记录 {(r['origin'], r['reading_key'])} "
                "标记 assigned 但依据不完整"
            )
    want_un = sorted(
        (d["origin"], d["reading_key"], d["outcome"], d["device"])
        for d in expect["unassigned"]
    )
    got_un = sorted(
        (d["origin"], d["reading_key"], d["outcome"], d["device"])
        for d in stored_unassigned
    )
    if want_un != got_un:
        problems.append(
            f"run {run_id} 待归属/撤回记录不一致: 库={got_un} 重算={want_un}"
        )


def run_verify(reading_db_path: str, ledger_db_path: str) -> dict:
    """全量离线核算并对照，返回结构化报告（ok=True 表示完全一致）。"""
    conn = sqlite3.connect(reading_db_path)
    conn.row_factory = sqlite3.Row
    ledger_conn = sqlite3.connect(f"file:{ledger_db_path}?mode=ro", uri=True)
    ledger_conn.row_factory = sqlite3.Row
    try:
        problems: list[str] = []
        run_reports: list[dict] = []
        runs = conn.execute("SELECT * FROM stats_runs ORDER BY run_id").fetchall()
        building = [int(r["run_id"]) for r in runs if r["status"] != "published"]

        for run in runs:
            if run["status"] != "published":
                continue
            rid = int(run["run_id"])
            r_problems: list[str] = []
            expect = _recompute_run(conn, ledger_conn, run)

            for field, value in expect["counts"].items():
                if int(run[field]) != value:
                    r_problems.append(
                        f"计数 {field} 不一致: 库={run[field]} 重算={value}"
                    )
            _check_cells(conn, rid, expect, r_problems)
            _check_observations(conn, rid, expect, r_problems)
            _check_attribution(conn, rid, expect, r_problems)

            problems.extend(r_problems)
            run_reports.append(
                {
                    "run_id": rid,
                    "cutoff_batch_id": int(run["cutoff_batch_id"]),
                    "grouping_version": int(run["grouping_version"]),
                    "ok": not r_problems,
                    "counts": expect["counts"],
                    "problems": r_problems,
                }
            )

        return {
            "ok": not problems,
            "published_runs_checked": len(run_reports),
            "building_run_ids": building,
            "runs": run_reports,
            "problems": problems,
        }
    finally:
        ledger_conn.close()
        conn.close()
