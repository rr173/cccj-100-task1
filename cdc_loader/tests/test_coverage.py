"""采样覆盖审计层单元测试。

覆盖：契约/豁免协议校验与不可变稿号、DST 跳时/回拨语义、跨午夜拆分、
步长锚定、生效/查询左闭右开交集、豁免并集只扣一次、容差配对（最近/
同距取早/边界/超容差）、别名归并、重复上报、撤回不参与、缺口被豁免
切断、零应到不除零、building 残档拒读与重做、独立核对程序全量对照。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from coverage import ingest, protocol, scheduler, store, timeutil  # noqa: E402
from coverage.audit import run_audit  # noqa: E402
from coverage.errors import (  # noqa: E402
    AuditStale,
    CaliberNotFound,
    CoverageProtocolError,
    DraftConflict,
)
from coverage.fault import (  # noqa: E402
    FAULT_AFTER_DETAIL_BEFORE_CONFIRM,
    FaultInjector,
)
from coverage.matcher import Reading, match_all, parse_reading_rows  # noqa: E402
from coverage.verifier import verify_audit  # noqa: E402


def contract_row(**over):
    base = {
        "contract_no": "C1",
        "draft_no": 1,
        "active": True,
        "origin": "plant-a",
        "entity_id": "E-DA",
        "metric": "vibration_rms",
        "effective_start": "2026-11-11T16:00:00Z",
        "effective_end": "2026-11-12T16:00:00Z",
        "timezone": "Asia/Shanghai",
        "weekdays": [1, 2, 3, 4, 5, 6, 7],
        "segments": [{"start": "08:00", "end": "20:00"}],
        "step_seconds": 3600,
        "tolerance_seconds": 300,
    }
    base.update(over)
    return base


def exemption_row(ex_id="X1", **over):
    base = {
        "exemption_id": ex_id,
        "contract_no": "C1",
        "start": "2026-11-12T03:00:00Z",
        "end": "2026-11-12T04:00:00Z",
        "reason": "检修",
    }
    base.update(over)
    return base


class ProtocolTests(unittest.TestCase):
    def test_field_set_must_match_exactly(self):
        row = contract_row()
        del row["metric"]
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(row)
        row2 = contract_row(unknown_field=1)
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(row2)

    def test_naive_time_rejected_and_unknown_tz_rejected(self):
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(
                contract_row(effective_start="2026-11-12T00:00:00")
            )
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(contract_row(timezone="Mars/Olympus"))

    def test_weekdays_and_segments_validation(self):
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(contract_row(weekdays=[0, 8]))
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(
                contract_row(segments=[{"start": "10:00", "end": "10:00"}])
            )
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_contract_row(contract_row(step_seconds=True))

    def test_exemption_requires_half_open_window(self):
        with self.assertRaises(CoverageProtocolError):
            protocol.validate_exemption_row(
                exemption_row(start="2026-11-12T04:00:00Z",
                              end="2026-11-12T04:00:00Z")
            )


class TimeSemanticsTests(unittest.TestCase):
    def test_spring_forward_gap_produces_no_instant(self):
        ny = timeutil.get_zone("America/New_York")
        naive = datetime(2026, 3, 8, 2, 0)
        self.assertEqual(timeutil.classify_wall_time(ny, naive), "gap")
        self.assertEqual(timeutil.real_instants(ny, naive), [])

    def test_fall_back_fold_produces_two_instants_with_offsets(self):
        ny = timeutil.get_zone("America/New_York")
        naive = datetime(2026, 11, 1, 1, 0)
        self.assertEqual(timeutil.classify_wall_time(ny, naive), "fold")
        instants = timeutil.real_instants(ny, naive)
        self.assertEqual(len(instants), 2)
        offsets = {timeutil.format_offset(i) for i in instants}
        self.assertEqual(offsets, {"-04:00", "-05:00"})
        # 真实时刻先后有序
        self.assertTrue(
            instants[0].astimezone(timezone.utc) < instants[1].astimezone(timezone.utc)
        )

    def test_cross_midnight_splits_into_two_segments(self):
        segs = timeutil.split_segments(date(2026, 11, 12), time(22, 0), time(2, 0))
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0].day, date(2026, 11, 12))
        self.assertEqual(segs[1].day, date(2026, 11, 13))

    def test_step_anchored_at_segment_start_not_query(self):
        ny = timeutil.get_zone("UTC")
        seg = timeutil.LocalSegment(date(2026, 11, 12), time(0, 0), time(3, 0))
        instants = timeutil.enumerate_segment_instants(ny, seg, 3600)
        self.assertEqual(
            [i.replace(tzinfo=None).hour for i in instants], [0, 1, 2]
        )

    def test_half_open_membership_and_union_merge(self):
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertTrue(timeutil.in_range(t0, t0, t0.replace(hour=1)))
        self.assertFalse(timeutil.in_range(t0.replace(hour=1), t0, t0.replace(hour=1)))
        merged = timeutil.merge_intervals([
            (t0, t0.replace(hour=3)),
            (t0.replace(hour=2), t0.replace(hour=5)),
        ])
        self.assertEqual(merged, [(t0, t0.replace(hour=5))])


def _write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")


class ImmutabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "cov.db")
        self.conn = store.connect(self.db)
        self.f1 = os.path.join(self.tmp, "c1.jsonl")
        _write_jsonl(self.f1, [contract_row()])

    def tearDown(self):
        self.conn.close()

    def _ingest(self, path):
        return ingest.ingest_contract_batch(self.conn, [path])

    def test_draft_conflict_rejects_whole_batch_and_keeps_state(self):
        self._ingest(self.f1)
        bad = os.path.join(self.tmp, "bad.jsonl")
        _write_jsonl(bad, [contract_row(step_seconds=1800)])
        with self.assertRaises(DraftConflict):
            self._ingest(bad)
        # 批次水位没有前进
        self.assertEqual(store.get_meta(self.conn, "last_contract_batch_id"), 1)

    def test_replay_same_draft_is_idempotent(self):
        self._ingest(self.f1)
        rep = self._ingest(self.f1)
        self.assertEqual(rep["new_drafts"], 0)
        self.assertEqual(rep["replayed_drafts"], 1)

    def test_new_draft_can_disable_contract(self):
        self._ingest(self.f1)
        f2 = os.path.join(self.tmp, "c2.jsonl")
        _write_jsonl(f2, [contract_row(draft_no=2, active=False)])
        rep = self._ingest(f2)
        self.assertEqual(rep["new_drafts"], 1)
        rows = self.conn.execute(
            "SELECT active FROM contract_drafts WHERE contract_no='C1' "
            "ORDER BY draft_no"
        ).fetchall()
        self.assertEqual([r["active"] for r in rows], [1, 0])


class MatcherTests(unittest.TestCase):
    def _item(self, item_id, hour, contract="C1", exempt=False):
        return scheduler.ExpectedItem(
            contract_no=contract, draft_no=1, origin="plant-a", entity_id="E-DA",
            metric="vibration_rms",
            scheduled_utc=datetime(2026, 11, 12, hour, tzinfo=timezone.utc),
            tz_name="UTC", local_date="2026-11-12",
            local_time=f"{hour:02d}:00:00", utc_offset="+00:00",
            fold_occurrence=0, segment_date="2026-11-12",
            segment_start="00:00:00", step_index=hour, exempt=exempt,
            exemption_ids=[], item_id=item_id,
        )

    def _reading(self, key, hour, device="DA", edition=1, batch=1):
        return Reading(
            origin="plant-a", reading_key=key, chosen_edition=edition,
            device=device,
            occurred_at_utc=datetime(2026, 11, 12, hour, tzinfo=timezone.utc),
            metric="vibration_rms", ingest_batch_id=batch,
            entity_id="E-DA",
        )

    def test_nearest_wins_tie_picks_earlier_item(self):
        items = [self._item(1, 10), self._item(2, 11)]
        contracts = [{"contract_no": "C1", "tolerance_seconds": 1800}]
        # 10:30 距 10:00 与 11:00 都是 1800 秒 -> 同距取更早（10:00 项）
        r = self._reading("r", 10)
        r.occurred_at_utc = datetime(2026, 11, 12, 10, 30, tzinfo=timezone.utc)
        decisions = match_all([r], items, contracts)
        self.assertEqual(decisions[0].item.item_id, 1)
        self.assertEqual(decisions[0].distance_seconds, 1800)

    def test_nearest_picks_closer_item_when_not_tied(self):
        items = [self._item(1, 10), self._item(2, 11)]
        contracts = [{"contract_no": "C1", "tolerance_seconds": 1800}]
        r = self._reading("r", 10)
        r.occurred_at_utc = datetime(2026, 11, 12, 10, 20, tzinfo=timezone.utc)
        decisions = match_all([r], items, contracts)
        self.assertEqual(decisions[0].item.item_id, 1)
        self.assertEqual(decisions[0].distance_seconds, 1200)

    def test_outside_tolerance_cannot_fill_hole(self):
        items = [self._item(1, 10), self._item(2, 11)]
        contracts = [{"contract_no": "C1", "tolerance_seconds": 300}]
        r = self._reading("r", 10)
        r.occurred_at_utc = datetime(2026, 11, 12, 10, 6, tzinfo=timezone.utc)
        decisions = match_all([r], items, contracts)
        self.assertIsNone(decisions[0].item)
        self.assertEqual(decisions[0].reason, "outside_tolerance")

    def test_alias_devices_share_entity_plan(self):
        items = [self._item(1, 10)]
        contracts = [{"contract_no": "C1", "tolerance_seconds": 300}]
        r_db = self._reading("r", 10, device="DB")  # 别名设备
        decisions = match_all([r_db], items, contracts)
        self.assertEqual(decisions[0].item.item_id, 1)

    def test_duplicate_hits_count_once_but_keep_diagnosis(self):
        from coverage.matcher import apply_coverage
        items = [self._item(1, 10)]
        contracts = [{"contract_no": "C1", "tolerance_seconds": 300}]
        decisions = match_all(
            [self._reading("r1", 10), self._reading("r2", 10)], items, contracts
        )
        summary = apply_coverage(items, decisions)
        self.assertEqual(summary["covered_count"], 1)
        self.assertEqual(summary["duplicate_count"], 1)
        self.assertEqual(len(summary["match_rows"]), 2)

    def test_exempt_item_not_coverable(self):
        items = [self._item(1, 10, exempt=True)]
        contracts = [{"contract_no": "C1", "tolerance_seconds": 300}]
        decisions = match_all([self._reading("r", 10)], items, contracts)
        self.assertEqual(decisions[0].reason, "all_scheduled_positions_exempt")


class SchedulerTests(unittest.TestCase):
    def test_exemption_union_subtracts_once_and_keeps_ids(self):
        c = protocol.validate_contract_row(contract_row())
        qs = datetime(2026, 11, 11, 16, tzinfo=timezone.utc)
        qe = datetime(2026, 11, 12, 16, tzinfo=timezone.utc)
        exs = [
            protocol.validate_exemption_row(exemption_row(
                "E1", start="2026-11-12T03:00:00Z", end="2026-11-12T04:30:00Z")),
            protocol.validate_exemption_row(exemption_row(
                "E2", start="2026-11-12T04:00:00Z", end="2026-11-12T05:00:00Z")),
        ]
        items = scheduler.expand_contract(c, qs, qe, exs)
        noon = next(i for i in items if i.local_time == "12:00:00")  # UTC 04:00
        self.assertTrue(noon.exempt)
        self.assertEqual(noon.exemption_ids, ["E1", "E2"])
        # 豁免项与非豁免项都在明细中，分母只数非豁免
        self.assertEqual(sum(1 for i in items if i.exempt), 2)

    def test_effective_end_half_open_excludes_boundary_instant(self):
        c = protocol.validate_contract_row(contract_row(
            effective_start="2026-11-12T00:00:00Z",
            effective_end="2026-11-12T02:00:00Z",
            timezone="UTC",
            segments=[{"start": "00:00", "end": "03:00"}],
        ))
        items = scheduler.expand_contract(
            c, datetime(2026, 11, 12, tzinfo=timezone.utc),
            datetime(2026, 11, 13, tzinfo=timezone.utc), []
        )
        self.assertEqual([i.local_time for i in items], ["00:00:00", "01:00:00"])


class AuditIntegrationTests(unittest.TestCase):
    """用内存/文件库跑最小端到端，验证原子发布与残档语义。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.cov_db = os.path.join(self.tmp, "cov.db")
        self.reading_db = os.path.join(self.tmp, "reading.db")
        self.ledger_db = os.path.join(self.tmp, "ledger.db")
        self._build_upstream()

    def _build_upstream(self):
        # 归并层台账（手工建最小版本快照）：版本1 DA->E-DA
        lc = sqlite3.connect(self.ledger_db)
        lc.executescript(
            """
            CREATE TABLE decision_versions(version INTEGER PRIMARY KEY);
            CREATE TABLE version_devices(version INTEGER, device_id TEXT);
            CREATE TABLE version_entities(version INTEGER, entity_id TEXT,
                                          device_id TEXT);
            """
        )
        lc.execute("INSERT INTO decision_versions(version) VALUES(1)")
        lc.execute("INSERT INTO version_devices(version, device_id) VALUES(1,'DA')")
        lc.execute(
            "INSERT INTO version_entities(version, entity_id, device_id) "
            "VALUES(1,'E-DA','DA')"
        )
        lc.commit(); lc.close()
        # 读数仓（手工建最小表 + 一条 10:00 读数）
        rc = sqlite3.connect(self.reading_db)
        rc.executescript(
            """
            CREATE TABLE ingest_batches(batch_id INTEGER PRIMARY KEY);
            CREATE TABLE reading_editions(
                origin TEXT, reading_key TEXT, edition INTEGER, device TEXT,
                occurred_at TEXT, occurred_date TEXT, metric TEXT, amount TEXT,
                withdrawn INTEGER, ingest_batch_id INTEGER, ingest_seq INTEGER,
                content_hash TEXT);
            """
        )
        rc.execute("INSERT INTO ingest_batches(batch_id) VALUES(1)")
        rc.execute(
            "INSERT INTO reading_editions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            ("plant-a", "r1", 1, "DA", "2026-11-12T02:00:00+00:00",
             "2026-11-12", "vibration_rms", "1.0", 0, 1, 0, "h"),
        )
        rc.commit(); rc.close()

    def _seed_contract(self):
        conn = store.connect(self.cov_db)
        try:
            path = os.path.join(self.tmp, "c.jsonl")
            _write_jsonl(path, [contract_row(
                timezone="UTC",
                effective_start="2026-11-12T00:00:00Z",
                effective_end="2026-11-13T00:00:00Z",
                segments=[{"start": "00:00", "end": "04:00"}],
            )])
            ingest.ingest_contract_batch(conn, [path])
            # 豁免批次（覆盖 01:00）
            ep = os.path.join(self.tmp, "e.jsonl")
            _write_jsonl(ep, [exemption_row(
                start="2026-11-12T01:00:00Z", end="2026-11-12T02:00:00Z")])
            ingest.ingest_exemption_batch(conn, [ep])
        finally:
            conn.close()

    def test_full_run_counts_and_zero_denominator_note(self):
        self._seed_contract()
        conn = store.connect(self.cov_db)
        try:
            rep = run_audit(
                conn, self.reading_db, self.ledger_db,
                "2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z",
                contract_cutoff_batch=1, reading_cutoff_batch=1,
                membership_version=1,
            )
            # 计划 00/01/02/03 四项，01 被豁免，10:00 读数（UTC02:00）覆盖 02
            self.assertEqual(rep["expected_count"], 4)
            self.assertEqual(rep["exempt_count"], 1)
            self.assertEqual(rep["denominator_non_exempt"], 3)
            self.assertEqual(rep["covered_count"], 1)
            self.assertIsNone(rep["zero_expected_note"])

            # 零应到窗口
            zero = run_audit(
                conn, self.reading_db, self.ledger_db,
                "2030-01-01T00:00:00Z", "2030-01-02T00:00:00Z",
                contract_cutoff_batch=1, reading_cutoff_batch=1,
                membership_version=1,
            )
            self.assertEqual(zero["denominator_non_exempt"], 0)
            self.assertIn("未定义", zero["zero_expected_note"])
        finally:
            conn.close()

    def test_crash_leaves_only_building_stale_and_rerun_publishes(self):
        # 故障注入是 os._exit(42)，会终止整个解释器，因此放到子进程中验证
        import subprocess
        self._seed_contract()
        helper = os.path.join(self.tmp, "_crash_helper.py")
        with open(helper, "w", encoding="utf-8") as fh:
            fh.write(
                "import sys\n"
                f"sys.path.insert(0, {ROOT!r})\n"
                "from coverage import store\n"
                "from coverage.audit import run_audit\n"
                "from coverage.fault import FaultInjector, "
                "FAULT_AFTER_DETAIL_BEFORE_CONFIRM\n"
                f"conn = store.connect({self.cov_db!r})\n"
                "run_audit(conn, %r, %r, '2026-11-12T00:00:00Z', "
                "'2026-11-12T12:00:00Z', contract_cutoff_batch=1, "
                "reading_cutoff_batch=1, membership_version=1, "
                "fault=FaultInjector(FAULT_AFTER_DETAIL_BEFORE_CONFIRM))\n"
                % (self.reading_db, self.ledger_db)
            )
        proc = subprocess.run(
            [sys.executable, "-I", helper], capture_output=True, text=True
        )
        self.assertEqual(proc.returncode, 42)

        conn = store.connect(self.cov_db)
        try:
            stale = conn.execute(
                "SELECT run_id FROM audit_runs WHERE status='building'"
            ).fetchall()
            self.assertEqual(len(stale), 1)
            with self.assertRaises(AuditStale):
                from coverage.audit import require_published
                require_published(conn, stale[0]["run_id"])
            # 同口径重做安全发布
            rep = run_audit(
                conn, self.reading_db, self.ledger_db,
                "2026-11-12T00:00:00Z", "2026-11-12T12:00:00Z",
                contract_cutoff_batch=1, reading_cutoff_batch=1,
                membership_version=1,
            )
            self.assertEqual(rep["status"], "published")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) c FROM audit_runs WHERE status!='published'"
                ).fetchone()["c"], 0,
            )
        finally:
            conn.close()

    def test_audit_without_any_exemption_batch_is_valid_zero_watermark(self):
        # 只登记契约、不登记任何豁免批次：豁免水位 0 是合法口径
        conn = store.connect(self.cov_db)
        try:
            path = os.path.join(self.tmp, "c_noex.jsonl")
            _write_jsonl(path, [contract_row(
                timezone="UTC",
                effective_start="2026-11-12T00:00:00Z",
                effective_end="2026-11-13T00:00:00Z",
                segments=[{"start": "00:00", "end": "02:00"}],
            )])
            ingest.ingest_contract_batch(conn, [path])
            conn.close()
            conn = store.connect(self.cov_db)
            rep = run_audit(
                conn, self.reading_db, self.ledger_db,
                "2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z",
                contract_cutoff_batch=1, reading_cutoff_batch=1,
                membership_version=1,
            )
            self.assertEqual(rep["exempt_count"], 0)
            self.assertEqual(rep["status"], "published")
        finally:
            conn.close()

    def test_independent_verifier_matches_published_detail(self):
        self._seed_contract()
        conn = store.connect(self.cov_db)
        try:
            rep = run_audit(
                conn, self.reading_db, self.ledger_db,
                "2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z",
                contract_cutoff_batch=1, reading_cutoff_batch=1,
                membership_version=1,
            )
            result = verify_audit(
                conn, self.reading_db, self.ledger_db, rep["run_id"]
            )
            self.assertTrue(result["ok"], result["diffs"])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
