"""读数事实仓与按日指标投影层单元测试。

覆盖：读数行协议（字段/十进制/带时区时间）、同稿篡改拒收与重传幂等、
截止批次选版（迟来旧稿不盖新版）、撤回排除、按分组版本成员归属与
待归属、Decimal 精确聚合与时区归日、口径冻结不混入新批次、building
崩溃后安全重建、重复口径复用、独立离线核算器全量对照。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from reading import ingest, protocol, query, store  # noqa: E402
from reading.build import build_run  # noqa: E402
from reading.errors import (  # noqa: E402
    CaliberNotFound,
    EditionConflict,
    ReadingProtocolError,
)
from reading.reconcile import run_verify  # noqa: E402


def rd(origin, key, edition, device, occurred_at, amount,
       metric="vibration_rms", withdrawn=False):
    return {
        "origin": origin,
        "reading_key": key,
        "edition": edition,
        "device": device,
        "occurred_at": occurred_at,
        "metric": metric,
        "amount": amount,
        "withdrawn": withdrawn,
    }


def write_jsonl(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def make_ledger(path: str) -> None:
    """手工构造最小归并层台账：版本1 单设备，版本2 DA-DB 合并，版本3 拆开。"""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE decision_versions (
            version INTEGER PRIMARY KEY, cause TEXT, batch_id INTEGER,
            ledger_seq INTEGER, entity_count INTEGER, device_count INTEGER,
            grouping_hash TEXT, presence_hash TEXT);
        CREATE TABLE version_entities (
            version INTEGER, entity_id TEXT, device_id TEXT,
            PRIMARY KEY (version, entity_id, device_id)) WITHOUT ROWID;
        CREATE TABLE version_devices (
            version INTEGER, device_id TEXT,
            PRIMARY KEY (version, device_id)) WITHOUT ROWID;
        """
    )
    devices = ["DA", "DB", "DC"]
    versions = {
        1: {"DA": "E-DA", "DB": "E-DB", "DC": "E-DC"},
        2: {"DA": "E-DA", "DB": "E-DA", "DC": "E-DC"},
        3: {"DA": "E-DA", "DB": "E-DB", "DC": "E-DC"},
    }
    for v, mapping in versions.items():
        conn.execute(
            "INSERT INTO decision_versions VALUES(?,?,?,?,?,?,?,?)",
            (v, "test", v, v, len(set(mapping.values())), len(devices),
             f"hash{v}", f"pres{v}"),
        )
        for d in devices:
            conn.execute("INSERT INTO version_devices VALUES(?,?)", (v, d))
        for d, e in mapping.items():
            conn.execute("INSERT INTO version_entities VALUES(?,?,?)", (v, e, d))
    conn.commit()
    conn.close()


class ProtocolTests(unittest.TestCase):
    def test_field_set_must_match_exactly(self):
        row = rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")
        with self.assertRaises(ReadingProtocolError):
            protocol.validate_reading_row({k: v for k, v in row.items() if k != "amount"})
        bad = dict(row)
        bad["extra"] = 1
        with self.assertRaises(ReadingProtocolError):
            protocol.validate_reading_row(bad)

    def test_edition_must_be_positive_int_not_bool(self):
        for bad_edition in (0, -1, True, "1", 1.0):
            row = rd("o", "k", bad_edition, "DA",
                     "2026-11-11T08:00:00+00:00", "1.0")
            with self.assertRaises(ReadingProtocolError):
                protocol.validate_reading_row(row)

    def test_amount_must_be_finite_decimal_text(self):
        ok = protocol.parse_amount("0.1", "t")
        self.assertEqual(ok, "0.1")
        for bad in ("NaN", "Infinity", "-Infinity", "abc", "", 1.0, True):
            with self.assertRaises(ReadingProtocolError):
                protocol.parse_amount(bad, "t")

    def test_occurred_at_requires_timezone_and_normalizes_utc(self):
        dt, iso = protocol.parse_occurred_at("2026-11-12T00:30:00+08:00", "t")
        self.assertEqual(protocol.utc_date(dt), "2026-11-11")
        self.assertTrue(iso.endswith("+00:00"))
        for bad in ("2026-11-12T00:30:00", "not-a-time"):
            with self.assertRaises(ReadingProtocolError):
                protocol.parse_occurred_at(bad, "t")

    def test_withdrawn_must_be_bool(self):
        row = rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")
        row["withdrawn"] = 1
        with self.assertRaises(ReadingProtocolError):
            protocol.validate_reading_row(row)


class FactAndBuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="reading-test-")
        self.reading_db = os.path.join(self.tmp, "reading.db")
        self.ledger_db = os.path.join(self.tmp, "ledger.db")
        make_ledger(self.ledger_db)
        self.conn = store.connect(self.reading_db)

    def tearDown(self):
        self.conn.close()

    def _ingest(self, rows):
        path = os.path.join(self.tmp, f"b_{len(os.listdir(self.tmp))}.jsonl")
        write_jsonl(path, rows)
        return ingest.ingest_batch(self.conn, [path])

    def test_replay_idempotent_and_tamper_rejected(self):
        self._ingest([rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")])
        rep = self._ingest([rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")])
        self.assertEqual(rep["new_editions"], 0)
        with self.assertRaises(EditionConflict):
            self._ingest([rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.5")])
        # 拒收不推进批次
        self.assertEqual(ingest.max_batch_id(self.conn), 2)

    def test_late_old_edition_does_not_override_newer(self):
        self._ingest([rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")])
        self._ingest([rd("o", "k", 2, "DB", "2026-11-12T08:00:00+00:00", "2.0")])
        self._ingest([rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")])
        rep = build_run(self.conn, self.ledger_db, 3, 2)
        self.assertEqual(rep["status"], "published")
        drill = query.drill_cell(
            self.conn, 3, 2, "E-DA", "2026-11-12", "vibration_rms")
        self.assertEqual(drill["details"][0]["chosen_edition"], 2)
        self.assertEqual(drill["details"][0]["device"], "DB")

    def test_withdrawn_excluded_but_queryable(self):
        self._ingest([
            rd("o", "k1", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0"),
            rd("o", "k2", 1, "DA", "2026-11-11T09:00:00+00:00", "9.0"),
        ])
        self._ingest([rd("o", "k2", 2, "DA", "2026-11-11T09:00:00+00:00",
                         "9.0", withdrawn=True)])
        rep = build_run(self.conn, self.ledger_db, 2, 1)
        self.assertEqual(rep["withdrawn_count"], 1)
        cells = query.query_cells(self.conn, 2, 1)["cells"]
        self.assertEqual(len(cells), 1)
        self.assertEqual(cells[0]["row_count"], 1)
        un = query.list_unassigned(self.conn, 2, 1, outcome="withdrawn")
        self.assertEqual([i["reading_key"] for i in un["items"]], ["k2"])

    def test_unassigned_unknown_device_not_dropped(self):
        self._ingest([rd("o", "k", 1, "DZ", "2026-11-11T08:00:00+00:00", "1.0")])
        rep = build_run(self.conn, self.ledger_db, 1, 2)
        self.assertEqual(rep["unassigned_count"], 1)
        self.assertEqual(rep["cell_count"], 0)
        un = query.list_unassigned(self.conn, 1, 2)
        self.assertEqual(un["items"][0]["outcome"], "unassigned")

    def test_decimal_precision_and_timezone_date(self):
        self._ingest([
            rd("o", "a", 1, "DA", "2026-11-11T10:00:00+00:00", "0.1"),
            rd("o", "b", 1, "DA", "2026-11-11T11:00:00+00:00", "0.2"),
            rd("o", "c", 1, "DA", "2026-11-12T00:30:00+08:00", "0.3"),
        ])
        build_run(self.conn, self.ledger_db, 1, 1)
        cells = {c["utc_date"]: c for c in
                 query.query_cells(self.conn, 1, 1)["cells"]}
        self.assertNotIn("2026-11-12", cells)  # +08:00 午夜归到 UTC 前一天
        d1 = cells["2026-11-11"]
        self.assertEqual(d1["amount_sum"], "0.6")  # 精确十进制，非 0.6000000000000001
        self.assertEqual(d1["row_count"], 3)
        self.assertEqual(d1["amount_min"], "0.1")
        self.assertEqual(d1["amount_max"], "0.3")

    def test_caliber_freeze_ignores_later_batches(self):
        self._ingest([rd("o", "early", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")])
        build_run(self.conn, self.ledger_db, 1, 1)
        self._ingest([rd("o", "late", 1, "DA", "2026-11-11T08:00:00+00:00", "2.0")])
        cells = query.query_cells(self.conn, 1, 1)["cells"]
        self.assertEqual(len(cells), 1)  # 截止=1，不混入批次2
        with self.assertRaises(CaliberNotFound):
            build_run(self.conn, self.ledger_db, 9, 1)  # 截止批次不存在

    def test_reuse_same_caliber_and_rebuild_after_crash(self):
        self._ingest([rd("o", "k", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0")])
        first = build_run(self.conn, self.ledger_db, 1, 1)
        again = build_run(self.conn, self.ledger_db, 1, 1)
        self.assertEqual(first["run_id"], again["run_id"])
        self.assertEqual(again["status"], "reused")

        # 模拟崩溃：为已存在口径手工留下一个 building 残档（明细未发布）。
        cur = self.conn.execute(
            "INSERT INTO stats_runs(cutoff_batch_id, grouping_version, status, "
            "created_at) VALUES(1, 2, 'building', 'x')")
        crash_run = cur.lastrowid
        self.conn.commit()
        with self.assertRaises(CaliberNotFound):
            query.query_cells(self.conn, 1, 2)  # building 不可见
        rebuilt = build_run(self.conn, self.ledger_db, 1, 2)
        self.assertEqual(rebuilt["status"], "published")
        # 残档被清理并复用同一 run 槽位
        self.assertEqual(rebuilt["run_id"], crash_run)

    def test_grouping_version_changes_attribution(self):
        self._ingest([
            rd("a", "x", 1, "DA", "2026-11-11T08:00:00+00:00", "1.0"),
            rd("b", "y", 1, "DB", "2026-11-11T08:00:00+00:00", "2.0"),
        ])
        build_run(self.conn, self.ledger_db, 1, 2)  # 合并版本
        build_run(self.conn, self.ledger_db, 1, 3)  # 拆开版本
        merged = query.query_cells(self.conn, 1, 2)["cells"]
        split = query.query_cells(self.conn, 1, 3)["cells"]
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["entity_id"], "E-DA")
        self.assertEqual(len(split), 2)

    def test_independent_reconciler_matches_everything(self):
        self._ingest([
            rd("a", "x", 1, "DA", "2026-11-11T08:00:00+00:00", "0.1"),
            rd("a", "y", 1, "DZ", "2026-11-11T08:00:00+00:00", "5.0"),
        ])
        self._ingest([rd("a", "x", 2, "DA", "2026-11-12T08:00:00+00:00", "0.2")])
        build_run(self.conn, self.ledger_db, 2, 2)
        report = run_verify(self.reading_db, self.ledger_db)
        self.assertTrue(report["ok"], report["problems"])
        self.assertEqual(report["published_runs_checked"], 1)


if __name__ == "__main__":
    unittest.main()
