"""覆盖审计的只读查询：口径列表、汇总、应到/覆盖/缺采/重复/未匹配明细。"""

from __future__ import annotations

import json

from .audit import require_published


def list_audits(conn, include_stale: bool = False) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM audit_runs ORDER BY run_id"
    ).fetchall()
    out = []
    for r in rows:
        if not include_stale and r["status"] != "published":
            continue
        out.append(_run_summary(r))
    return out


def _run_summary(r) -> dict:
    return {
        "run_id": r["run_id"],
        "status": r["status"],
        "query_window_utc": [r["query_start_utc"], r["query_end_utc"]],
        "contract_cutoff_batch": r["contract_cutoff_batch"],
        "exemption_cutoff_batch": r["exemption_cutoff_batch"],
        "reading_cutoff_batch": r["reading_cutoff_batch"],
        "membership_version": r["membership_version"],
        "expected_count": r["expected_count"],
        "covered_count": r["covered_count"],
        "missing_count": r["missing_count"],
        "exempt_count": r["exempt_count"],
        "duplicate_count": r["duplicate_count"],
        "unmatched_count": r["unmatched_count"],
        "zero_expected_note": r["zero_expected_note"],
        "input_hash": r["input_hash"],
        "created_at": r["created_at"],
        "published_at": r["published_at"],
    }


def get_report(conn, run_id: int) -> dict:
    """完整审计报告：覆盖比例分子/分母 + 五类明细 + 缺口段。"""
    run = require_published(conn, run_id)
    expected = [
        dict(r) for r in conn.execute(
            "SELECT * FROM audit_expected WHERE run_id=? ORDER BY item_id", (run_id,)
        )
    ]
    for r in expected:
        r["exemption_ids"] = json.loads(r["exemption_ids"])
    matches = [
        dict(r) for r in conn.execute(
            "SELECT * FROM audit_matches WHERE run_id=? "
            "ORDER BY item_id, distance_seconds, reading_key", (run_id,)
        )
    ]
    unmatched = [
        dict(r) for r in conn.execute(
            "SELECT * FROM audit_unmatched WHERE run_id=? "
            "ORDER BY origin, reading_key", (run_id,)
        )
    ]
    for r in unmatched:
        r["candidates"] = json.loads(r["candidates"])
    gaps = [
        dict(r) for r in conn.execute(
            "SELECT * FROM audit_gaps WHERE run_id=? ORDER BY gap_id", (run_id,)
        )
    ]
    for r in gaps:
        r["missing_item_ids"] = json.loads(r["missing_item_ids"])
        r["split_exemptions"] = json.loads(r["split_exemptions"])
        r["nearby_readings"] = json.loads(r["nearby_readings"])

    denominator = run["expected_count"] - run["exempt_count"]
    ratio = None
    if denominator > 0:
        ratio = run["covered_count"] / denominator
    return {
        "run": _run_summary(run),
        "coverage_ratio": {
            "numerator_covered": run["covered_count"],
            "denominator_non_exempt_expected": denominator,
            "ratio": ratio,
            "note": run["zero_expected_note"],
        },
        "expected_items": expected,
        "matches": matches,
        "unmatched_readings": unmatched,
        "gaps": gaps,
    }


def list_missing(conn, run_id: int) -> list[dict]:
    require_published(conn, run_id)
    return [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_expected WHERE run_id=? AND exempt=0 AND covered=0 "
            "ORDER BY item_id",
            (run_id,
        )
    )]


def list_duplicates(conn, run_id: int) -> list[dict]:
    require_published(conn, run_id)
    return [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_matches WHERE run_id=? AND duplicate_hit=1 "
            "ORDER BY item_id, reading_key",
            (run_id,),
        )
    ]


def explain_item(conn, run_id: int, item_id: int) -> dict:
    """追查单个应到项：日程来源、豁免、命中读数与附近未匹配读数。"""
    require_published(conn, run_id)
    item = conn.execute(
        "SELECT * FROM audit_expected WHERE run_id=? AND item_id=?",
        (run_id, item_id),
    ).fetchone()
    if item is None:
        raise CaliberNotFound(f"审计 {run_id} 无应到项 {item_id}")
    item = dict(item)
    item["exemption_ids"] = json.loads(item["exemption_ids"])
    hits = [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM audit_matches WHERE run_id=? AND item_id=? "
            "ORDER BY distance_seconds, reading_key",
            (run_id, item_id),
        )
    ]
    return {"item": item, "matching_readings": hits}
