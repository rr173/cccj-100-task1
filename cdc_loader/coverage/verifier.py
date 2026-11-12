"""独立核对程序（刻意不 import scheduler / matcher / gaps / audit）。

本模块只依赖标准库与本包的协议/存储/时间基础工具（``protocol``、
``store``、``timeutil``、``snapshot`` 的只读取数），**独立重写**日程
展开与读数配对：

* 日程：逐日逐段、以段起点为锚朴素枚举，逐个真实时刻做生效/查询/豁免
  判定，不调用 :func:`scheduler.expand_all`；
* 配对：对每个 (读数, 应到项) 配对做完整 O(n*m) 比较，独立复算容差、
  最近选择、同距取早、重复命中，不调用 :func:`matcher.match_all`；
* 缺口：按契约时间轴朴素扫描缺采/豁免/覆盖序列独立分段。

随后与已发布审计的每张明细表逐条比对，任何不一致列出差异并以退出码 6
结束。building 残档不参与对照，单独列出。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, time, timedelta, timezone

from . import snapshot, timeutil
from .timeutil import (
    format_offset,
    get_zone,
    in_range,
    parse_hhmm,
    split_segments,
)


def _utc(raw: str) -> datetime:
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# 独立日程展开（朴素实现）
# ---------------------------------------------------------------------------


def _indep_expand(contracts, query_start, query_end, exemptions_by_contract):
    items = []
    for c in sorted(contracts, key=lambda x: x["contract_no"]):
        if not c["active"]:
            continue
        zone = get_zone(c["timezone"])
        eff_lo, eff_hi = _utc(c["effective_start_utc"]), _utc(c["effective_end_utc"])
        win_lo, win_hi = max(eff_lo, query_start), min(eff_hi, query_end)
        if not win_lo < win_hi:
            continue
        exemptions = exemptions_by_contract.get(c["contract_no"], [])
        ex_intervals = [
            (_utc(e["start_utc"]), _utc(e["end_utc"]), e["exemption_id"])
            for e in exemptions
        ]
        day = win_lo.astimezone(zone).date()
        last_day = win_hi.astimezone(zone).date()
        while day <= last_day:
            for seg in c["segments"]:
                st, et = parse_hhmm(seg["start"], "s"), parse_hhmm(seg["end"], "e")
                for local_seg in split_segments(day, st, et):
                    if local_seg.day.isoweekday() not in set(c["weekdays"]):
                        continue
                    naive_start = datetime.combine(
                        local_seg.day, local_seg.start_time
                    )
                    if local_seg.end_time == time(23, 59, 59, 999999):
                        naive_end = datetime.combine(local_seg.day, time()) + timedelta(days=1)
                    else:
                        naive_end = datetime.combine(local_seg.day, local_seg.end_time)
                    step_index = 0
                    cur = naive_start
                    while cur < naive_end:
                        instants = timeutil.real_instants(zone, cur)
                        for instant in instants:
                            u = instant.astimezone(timezone.utc)
                            if not (
                                in_range(u, eff_lo, eff_hi)
                                and in_range(u, query_start, query_end)
                            ):
                                continue
                            hit = sorted(
                                {
                                    eid
                                    for lo, hi, eid in ex_intervals
                                    if in_range(u, lo, hi)
                                }
                            )
                            local_dt = u.astimezone(zone)
                            kind = timeutil.classify_wall_time(zone, cur)
                            fold = int(local_dt.fold) if kind == "fold" else 0
                            items.append(
                                {
                                    "contract_no": c["contract_no"],
                                    "draft_no": c["draft_no"],
                                    "origin": c["origin"],
                                    "entity_id": c["entity_id"],
                                    "metric": c["metric"],
                                    "scheduled_utc": u.isoformat(),
                                    "tz_name": c["timezone"],
                                    "local_time": local_dt.time().isoformat(),
                                    "local_date": local_dt.date().isoformat(),
                                    "utc_offset": format_offset(local_dt),
                                    "fold_occurrence": fold,
                                    "segment_date": local_seg.day.isoformat(),
                                    "segment_start": local_seg.start_time.isoformat(),
                                    # 步序以日段起点为锚，按“真实时刻位置”
                                    # 计数：fold 的两个真实时刻各占一步，
                                    # gap 不产生真实时刻故不占步
                                    "step_index": step_index,
                                    "exempt": bool(hit),
                                    "exemption_ids": hit,
                                }
                            )
                            step_index += 1
                        cur += timedelta(seconds=c["step_seconds"])
            day += timedelta(days=1)
    items.sort(
        key=lambda x: (
            x["contract_no"], _utc(x["scheduled_utc"]),
            x["fold_occurrence"], x["segment_start"], x["step_index"],
        )
    )
    for i, it in enumerate(items, start=1):
        it["item_id"] = i
    return items


# ---------------------------------------------------------------------------
# 独立配对（全量 O(n*m) 比较）
# ---------------------------------------------------------------------------


def _indep_match(readings, items, contracts, entity_of):
    tol = {c["contract_no"]: c["tolerance_seconds"] for c in contracts}
    non_exempt = [it for it in items if not it["exempt"]]
    matched_decisions = {}
    unmatched = []
    for r in sorted(readings, key=lambda x: (x["origin"], x["reading_key"])):
        entity = entity_of.get(r["device"])
        if entity is None:
            unmatched.append((r, "device_not_in_membership_version", None, []))
            continue
        keys = {
            (it["origin"], it["entity_id"], it["metric"]) for it in items
        }
        if (r["origin"], entity, r["metric"]) not in keys:
            unmatched.append(
                (r, "no_contract_for_origin_entity_metric", None, [])
            )
            continue
        # 完整比较：枚举所有同键应到项，逐个算距离
        scored = []
        for it in non_exempt:
            if (it["origin"], it["entity_id"], it["metric"]) != (
                r["origin"], entity, r["metric"]
            ):
                continue
            dist = int(
                abs(
                    (
                        _utc(r["occurred_at"]) - _utc(it["scheduled_utc"])
                    ).total_seconds()
                )
            )
            scored.append((dist, it))
        if not scored:
            unmatched.append((r, "all_scheduled_positions_exempt", None, []))
            continue
        scored.sort(key=lambda p: (p[0], _utc(p[1]["scheduled_utc"]), p[1]["item_id"]))
        best_dist, best = scored[0]
        basis = [
            {
                "item_id": it["item_id"],
                "contract_no": it["contract_no"],
                "scheduled_utc": it["scheduled_utc"],
                "distance_seconds": d,
                "tolerance_seconds": tol[it["contract_no"]],
            }
            for d, it in scored[:5]
        ]
        if best_dist > tol[best["contract_no"]]:
            unmatched.append((r, "outside_tolerance", best_dist, basis))
            continue
        matched_decisions[(r["origin"], r["reading_key"])] = (
            best["item_id"], best_dist, basis
        )

    # 覆盖与重复
    hit_count: dict[int, int] = {}
    match_rows = []
    for r in sorted(readings, key=lambda x: (x["origin"], x["reading_key"])):
        key = (r["origin"], r["reading_key"])
        if key not in matched_decisions:
            continue
        item_id, dist, _ = matched_decisions[key]
        prior = hit_count.get(item_id, 0)
        hit_count[item_id] = prior + 1
        match_rows.append(
            {
                "item_id": item_id,
                "origin": r["origin"],
                "reading_key": r["reading_key"],
                "chosen_edition": r["edition"],
                "device": r["device"],
                "entity_id": entity_of[r["device"]],
                "occurred_at_utc": r["occurred_at"],
                "distance_seconds": dist,
                "duplicate_hit": 1 if prior > 0 else 0,
                "ingest_batch_id": r["ingest_batch_id"],
            }
        )
    covered_ids = set(hit_count)
    missing = [
        it for it in non_exempt if it["item_id"] not in covered_ids
    ]
    return match_rows, unmatched, missing, covered_ids


def _indep_gaps(items, missing, unmatched):
    missing_ids = {it["item_id"] for it in missing}
    gaps = []
    seq = 0
    for contract_no in sorted({it["contract_no"] for it in items}):
        chain = sorted(
            [it for it in items if it["contract_no"] == contract_no],
            key=lambda x: (_utc(x["scheduled_utc"]), x["fold_occurrence"], x["item_id"]),
        )
        run = []

        def flush(run_items):
            nonlocal seq
            if not run_items:
                return
            seq += 1
            gaps.append(
                {
                    "gap_id": seq,
                    "contract_no": contract_no,
                    "missing_item_ids": [it["item_id"] for it in run_items],
                }
            )

        for it in chain:
            if it["item_id"] in missing_ids:
                run.append(it)
            else:
                flush(run)
                run = []
        flush(run)
    return gaps


# ---------------------------------------------------------------------------
# 对照
# ---------------------------------------------------------------------------


def _diff(name: str, expected, actual, diffs: list[str]) -> None:
    if expected != actual:
        diffs.append(f"{name} 不一致:\n  独立核算={json.dumps(expected, ensure_ascii=False, sort_keys=True)}\n  库中明细={json.dumps(actual, ensure_ascii=False, sort_keys=True)}")


def verify_audit(
    conn: sqlite3.Connection,
    reading_db_path: str,
    ledger_db_path: str,
    run_id: int,
) -> dict:
    run = conn.execute(
        "SELECT * FROM audit_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if run is None:
        return {"ok": False, "run_id": run_id, "diffs": [f"审计 {run_id} 不存在"]}
    if run["status"] != "published":
        return {
            "ok": False,
            "run_id": run_id,
            "diffs": [f"审计状态 {run['status']}，非 published，不参与核对"],
        }

    query_start, query_end = _utc(run["query_start_utc"]), _utc(run["query_end_utc"])
    contracts = snapshot.load_current_contracts(
        conn, run["contract_cutoff_batch"]
    )
    active = [c for c in contracts if c["active"]]
    exemptions = snapshot.load_exemptions(
        conn, [c["contract_no"] for c in active],
        run["exemption_cutoff_batch"],
    )
    ex_by = {}
    for e in exemptions:
        ex_by.setdefault(e["contract_no"], []).append(e)
    readings = snapshot.load_readings(
        reading_db_path, run["reading_cutoff_batch"]
    )
    membership = snapshot.load_membership(
        ledger_db_path, run["membership_version"]
    )
    entity_of = membership["entity_of"]

    exp_items = _indep_expand(active, query_start, query_end, ex_by)
    match_rows, unmatched, missing, covered_ids = _indep_match(
        readings, exp_items, active, entity_of
    )
    exp_gaps = _indep_gaps(exp_items, missing, unmatched)

    diffs: list[str] = []

    # 应到项逐条
    db_items = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_expected WHERE run_id=? ORDER BY item_id", (run_id,)
        )
    ]
    if len(db_items) != len(exp_items):
        diffs.append(f"应到项数量不一致: 独立={len(exp_items)} 库中={len(db_items)}")
    for db_it, exp in zip(db_items, exp_items):
        for field in (
            "item_id", "contract_no", "draft_no", "origin", "entity_id", "metric",
            "scheduled_utc", "tz_name", "local_time", "local_date", "utc_offset",
            "fold_occurrence", "segment_date", "segment_start", "step_index",
        ):
            _diff(f"item {exp['item_id']}.{field}", exp[field], db_it[field], diffs)
        _diff(
            f"item {exp['item_id']}.exempt",
            1 if exp["exempt"] else 0, db_it["exempt"], diffs,
        )
        _diff(
            f"item {exp['item_id']}.exemption_ids",
            exp["exemption_ids"], json.loads(db_it["exemption_ids"]), diffs,
        )
        _diff(
            f"item {exp['item_id']}.covered",
            1 if exp["item_id"] in covered_ids else 0, db_it["covered"], diffs,
        )

    # 配对逐条
    db_matches = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_matches WHERE run_id=? "
            "ORDER BY item_id, reading_key", (run_id,)
        )
    ]
    exp_match_keys = sorted(
        (m["item_id"], m["origin"], m["reading_key"]) for m in match_rows
    )
    db_match_keys = sorted(
        (r["item_id"], r["origin"], r["reading_key"]) for r in db_matches
    )
    _diff("配对集合", exp_match_keys, db_match_keys, diffs)
    db_match_map = {
        (r["item_id"], r["origin"], r["reading_key"]): r for r in db_matches
    }
    for m in match_rows:
        dbm = db_match_map.get((m["item_id"], m["origin"], m["reading_key"]))
        if dbm is None:
            continue
        for field in (
            "chosen_edition", "device", "entity_id", "occurred_at_utc",
            "distance_seconds", "duplicate_hit", "ingest_batch_id",
        ):
            _diff(
                f"match {m['reading_key']}->{m['item_id']}.{field}",
                m[field], dbm[field], diffs,
            )

    # 未匹配逐条
    db_unmatched = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_unmatched WHERE run_id=? ORDER BY reading_key",
            (run_id,),
        )
    ]
    exp_unmatched = sorted(
        (r["origin"], r["reading_key"], reason)
        for r, reason, _d, _b in unmatched
    )
    db_unmatched_keys = sorted(
        (r["origin"], r["reading_key"], r["reason"]) for r in db_unmatched
    )
    _diff("未匹配集合(含原因)", exp_unmatched, db_unmatched_keys, diffs)

    # 缺口
    db_gaps = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_gaps WHERE run_id=? ORDER BY gap_id", (run_id,)
        )
    ]
    if len(db_gaps) != len(exp_gaps):
        diffs.append(f"缺口段数量不一致: 独立={len(exp_gaps)} 库中={len(db_gaps)}")
    for db_g, exp_g in zip(db_gaps, exp_gaps):
        _diff(
            f"gap {exp_g['gap_id']}.missing_item_ids",
            exp_g["missing_item_ids"],
            json.loads(db_g["missing_item_ids"]),
            diffs,
        )

    # 汇总计数
    non_exempt_n = sum(1 for it in exp_items if not it["exempt"])
    covered_n = len(covered_ids)
    exempt_n = sum(1 for it in exp_items if it["exempt"])
    dup_n = sum(max(0, c - 1) for c in
                {i: sum(1 for m in match_rows if m["item_id"] == i)
                 for i in covered_ids}.values())
    for name, exp_v, db_v in (
        ("expected_count", len(exp_items), run["expected_count"]),
        ("covered_count", covered_n, run["covered_count"]),
        ("missing_count", len(missing), run["missing_count"]),
        ("exempt_count", exempt_n, run["exempt_count"]),
        ("unmatched_count", len(unmatched), run["unmatched_count"]),
        ("duplicate_count", dup_n, run["duplicate_count"]),
        ("denominator", non_exempt_n,
         run["expected_count"] - run["exempt_count"]),
    ):
        _diff(name, exp_v, db_v, diffs)

    return {
        "ok": not diffs,
        "run_id": run_id,
        "checked_expected": len(exp_items),
        "checked_matches": len(match_rows),
        "checked_unmatched": len(unmatched),
        "checked_gaps": len(exp_gaps),
        "diffs": diffs,
    }


def verify_all(conn, reading_db_path: str, ledger_db_path: str) -> dict:
    runs = conn.execute("SELECT run_id, status FROM audit_runs ORDER BY run_id").fetchall()
    reports = []
    stale = [r["run_id"] for r in runs if r["status"] != "published"]
    for r in runs:
        if r["status"] != "published":
            continue
        reports.append(
            verify_audit(conn, reading_db_path, ledger_db_path, r["run_id"])
        )
    ok = all(rep["ok"] for rep in reports)
    return {
        "ok": ok,
        "verified_runs": [rep["run_id"] for rep in reports],
        "stale_run_ids": stale,
        "reports": reports,
    }
