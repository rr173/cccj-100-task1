"""统计查询：口径解析、按格子投影、下钻明细、待归属清单。

所有查询默认只看 ``published`` 的 run——构建中途（building）对在线
查询不可见，因此构建较慢时分析人员仍能读到上一份完整日报，绝不会
先清空在线结果。
"""

from __future__ import annotations

import json

from .errors import CaliberNotFound


def _row(conn, sql: str, params: tuple):
    return conn.execute(sql, params).fetchone()


def resolve_published_run(
    conn, cutoff_batch_id: int, grouping_version: int
) -> dict:
    """按口径坐标找已发布 run；building 或不存在都明确报错。"""
    row = _row(
        conn,
        "SELECT * FROM stats_runs WHERE cutoff_batch_id=? AND grouping_version=?",
        (cutoff_batch_id, grouping_version),
    )
    if row is None:
        raise CaliberNotFound(
            f"口径 (cutoff_batch={cutoff_batch_id}, "
            f"grouping_version={grouping_version}) 尚未生成"
        )
    if row["status"] != "published":
        raise CaliberNotFound(
            f"口径 (cutoff_batch={cutoff_batch_id}, "
            f"grouping_version={grouping_version}) 正在构建中（status=building），"
            "在线结果仍为上一份已发布日报"
        )
    return dict(row)


def list_runs(conn) -> dict:
    """列出全部口径登记（含 building 残档），供运维复查。"""
    rows = conn.execute(
        "SELECT run_id, cutoff_batch_id, grouping_version, status, created_at, "
        "published_at, observation_count, included_count, withdrawn_count, "
        "unassigned_count, cell_count, input_hash FROM stats_runs "
        "ORDER BY run_id"
    ).fetchall()
    return {"runs": [dict(r) for r in rows]}


def current_caliber(conn) -> dict | None:
    """最新一条已发布口径（在线日报当前指向）。"""
    row = conn.execute(
        "SELECT run_id, cutoff_batch_id, grouping_version, published_at, "
        "cell_count, included_count, unassigned_count FROM stats_runs "
        "WHERE status='published' ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row is not None else None


def query_cells(
    conn,
    cutoff_batch_id: int,
    grouping_version: int,
    entity_id: str | None = None,
    utc_date: str | None = None,
    metric: str | None = None,
) -> dict:
    """按口径取按日指标格子，可按实体/日期/指标过滤。"""
    run = resolve_published_run(conn, cutoff_batch_id, grouping_version)
    sql = (
        "SELECT entity_id, utc_date, metric, row_count, amount_sum, "
        "amount_min, amount_max FROM stats_cells WHERE run_id=?"
    )
    params: list = [run["run_id"]]
    if entity_id is not None:
        sql += " AND entity_id=?"
        params.append(entity_id)
    if utc_date is not None:
        sql += " AND utc_date=?"
        params.append(utc_date)
    if metric is not None:
        sql += " AND metric=?"
        params.append(metric)
    sql += " ORDER BY entity_id, utc_date, metric"
    cells = [dict(r) for r in conn.execute(sql, params)]
    return {
        "caliber": {
            "run_id": run["run_id"],
            "cutoff_batch_id": cutoff_batch_id,
            "grouping_version": grouping_version,
        },
        "cells": cells,
    }


def drill_cell(
    conn,
    cutoff_batch_id: int,
    grouping_version: int,
    entity_id: str,
    utc_date: str,
    metric: str,
) -> dict:
    """从一个统计格子下钻到读数身份、采用稿件与归属依据。

    联合 stats_observations（采用稿件）与 stats_attribution（归属依据），
    只返回归属到该实体、该 UTC 日期、该指标的有效观测。
    """
    run = resolve_published_run(conn, cutoff_batch_id, grouping_version)
    rid = run["run_id"]
    # 先确认格子存在（不存在则给出明确报错而非空清单）。
    cell = _row(
        conn,
        "SELECT * FROM stats_cells WHERE run_id=? AND entity_id=? AND utc_date=? "
        "AND metric=?",
        (rid, entity_id, utc_date, metric),
    )
    if cell is None:
        raise CaliberNotFound(
            f"格子不存在: entity={entity_id} date={utc_date} metric={metric}"
        )
    rows = conn.execute(
        """
        SELECT o.origin, o.reading_key, o.chosen_edition, o.device,
               o.occurred_at, o.occurred_date, o.metric, o.amount,
               o.ingest_batch_id, a.assigned, a.entity_id, a.basis
        FROM stats_observations o
        JOIN stats_attribution a
          ON a.run_id = o.run_id AND a.origin = o.origin
         AND a.reading_key = o.reading_key
        WHERE o.run_id=? AND a.assigned=1 AND a.entity_id=?
          AND o.occurred_date=? AND o.metric=? AND o.withdrawn=0
        ORDER BY o.origin, o.reading_key
        """,
        (rid, entity_id, utc_date, metric),
    ).fetchall()
    details = []
    for r in rows:
        d = dict(r)
        d["basis"] = json.loads(d["basis"])
        details.append(d)
    return {
        "caliber": {
            "run_id": rid,
            "cutoff_batch_id": cutoff_batch_id,
            "grouping_version": grouping_version,
        },
        "cell": {
            "entity_id": entity_id,
            "utc_date": utc_date,
            "metric": metric,
            "row_count": cell["row_count"],
            "amount_sum": cell["amount_sum"],
            "amount_min": cell["amount_min"],
            "amount_max": cell["amount_max"],
        },
        "details": details,
    }


def list_unassigned(
    conn,
    cutoff_batch_id: int,
    grouping_version: int,
    outcome: str | None = None,
) -> dict:
    """待归属/撤回清单：没有进入格子的观测及其原因，绝不静默丢弃。

    ``outcome`` 可选 ``unassigned``（设备不在该版成员）或 ``withdrawn``
    （最新稿撤回）；缺省返回全部未计入格子的观测。
    """
    run = resolve_published_run(conn, cutoff_batch_id, grouping_version)
    rid = run["run_id"]
    sql = (
        """
        SELECT o.origin, o.reading_key, o.chosen_edition, o.device,
               o.occurred_at, o.occurred_date, o.metric, o.amount,
               o.withdrawn, o.ingest_batch_id, a.basis
        FROM stats_observations o
        JOIN stats_attribution a
          ON a.run_id = o.run_id AND a.origin = o.origin
         AND a.reading_key = o.reading_key
        WHERE o.run_id=? AND a.assigned=0
        ORDER BY o.origin, o.reading_key
        """
    )
    rows = conn.execute(sql, (rid,)).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        basis = json.loads(d.pop("basis"))
        d["outcome"] = basis["outcome"]
        d["basis"] = basis
        if outcome is not None and d["outcome"] != outcome:
            continue
        items.append(d)
    return {
        "caliber": {
            "run_id": rid,
            "cutoff_batch_id": cutoff_batch_id,
            "grouping_version": grouping_version,
        },
        "unassigned_count": run["unassigned_count"],
        "withdrawn_count": run["withdrawn_count"],
        "items": items,
    }


def fact_audit(conn, origin: str, reading_key: str) -> dict:
    """审计一条观测的全部稿件事实（各稿件有据可查、选版过程可复核）。"""
    rows = conn.execute(
        "SELECT edition, device, occurred_at, occurred_date, metric, amount, "
        "withdrawn, ingest_batch_id, ingest_seq, content_hash "
        "FROM reading_editions WHERE origin=? AND reading_key=? "
        "ORDER BY edition",
        (origin, reading_key),
    ).fetchall()
    if not rows:
        raise CaliberNotFound(f"观测不存在: origin={origin} reading_key={reading_key}")
    return {
        "origin": origin,
        "reading_key": reading_key,
        "editions": [dict(r) for r in rows],
    }
