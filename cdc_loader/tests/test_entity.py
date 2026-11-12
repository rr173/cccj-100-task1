"""实体归并层单元测试。

覆盖：对照资料协议、修订链规则、旧修订篡改检测、链式冲突与确定性
路径、批次原子性、同批拆分+合并、版本幂等、实体标识稳定、refresh
暂扣/恢复、双来源冗余依据、端点/类别变更、崩溃恢复、独立重算器。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from entity import ledger, protocol, store  # noqa: E402
from entity.errors import (  # noqa: E402
    ConstraintConflict,
    RelationProtocolError,
    RevisionConflict,
    UpstreamNotReady,
    VersionNotFound,
)
from entity.fault import (  # noqa: E402
    CRASH_EXIT_CODE,
    FAULT_AFTER_LEDGER_BEFORE_DECISION,
    FaultInjector,
)
from entity.recompute import run_verify  # noqa: E402


def rel(source, relation_id, revision, a, b, category="same_entity", effective=True):
    return {
        "source": source,
        "relation_id": relation_id,
        "revision": revision,
        "endpoint_a": a,
        "endpoint_b": b,
        "category": category,
        "effective": effective,
    }


def make_loader_db(path: str, devices: list[str]) -> None:
    """手工构造一个最小的装载器状态库（meta + device_master）。"""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE device_master (device_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO meta VALUES('snapshot_published', 'true')"
    )
    conn.executemany(
        "INSERT INTO device_master VALUES(?, ?)",
        [(d, json.dumps({"device_id": d})) for d in devices],
    )
    conn.commit()
    conn.close()


def set_loader_devices(path: str, devices: list[str]) -> None:
    conn = sqlite3.connect(path)
    conn.execute("DELETE FROM device_master")
    conn.executemany(
        "INSERT INTO device_master VALUES(?, ?)",
        [(d, json.dumps({"device_id": d})) for d in devices],
    )
    conn.commit()
    conn.close()


class EntityCase(unittest.TestCase):
    DEVICES = ["A", "B", "C", "D", "E", "F"]

    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="entity-test-")
        self.loader_db = os.path.join(self.dir, "loader.db")
        self.ledger_db = os.path.join(self.dir, "ledger.db")
        make_loader_db(self.loader_db, self.DEVICES)
        self.conn = store.connect(self.ledger_db)
        ledger.refresh_presence(self.conn, self.loader_db)

    def tearDown(self) -> None:
        self.conn.close()
        import shutil

        shutil.rmtree(self.dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def write_batch(self, name: str, rows: list[dict]) -> str:
        path = os.path.join(self.dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        return path

    def ingest(self, rows: list[dict], name: str = "batch.jsonl",
               fault: FaultInjector | None = None) -> dict:
        return ledger.ingest_batch(self.conn, [self.write_batch(name, rows)], fault)

    def entities(self, version: int | None = None) -> dict[str, list[str]]:
        if version is None:
            version = ledger.current_version(self.conn)
        return ledger.version_entities(self.conn, version)

    def revision_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) AS c FROM revisions").fetchone()["c"]


class TestProtocol(EntityCase):
    def test_valid_row(self):
        row = protocol.validate_relation_row(rel("s", "R1", 1, "A", "B"))
        self.assertEqual(row["category"], "same_entity")
        self.assertTrue(row["effective"])

    def test_missing_and_extra_field(self):
        bad = rel("s", "R1", 1, "A", "B")
        del bad["source"]
        with self.assertRaises(RelationProtocolError):
            protocol.validate_relation_row(bad)
        bad2 = rel("s", "R1", 1, "A", "B")
        bad2["comment"] = "x"
        with self.assertRaises(RelationProtocolError):
            protocol.validate_relation_row(bad2)

    def test_bad_revision(self):
        for rev in (0, -1, True, "1", 1.5):
            with self.assertRaises(RelationProtocolError, msg=f"revision={rev!r}"):
                protocol.validate_relation_row(rel("s", "R1", rev, "A", "B"))

    def test_same_endpoints_rejected(self):
        with self.assertRaises(RelationProtocolError):
            protocol.validate_relation_row(rel("s", "R1", 1, "A", "A"))

    def test_bad_category_and_effective(self):
        with self.assertRaises(RelationProtocolError):
            protocol.validate_relation_row(rel("s", "R1", 1, "A", "B", category="same"))
        with self.assertRaises(RelationProtocolError):
            protocol.validate_relation_row(rel("s", "R1", 1, "A", "B", effective=1))


class TestIngestPrerequisites(EntityCase):
    def test_ingest_requires_refresh(self):
        conn = store.connect(os.path.join(self.dir, "fresh.db"))
        try:
            with self.assertRaises(UpstreamNotReady):
                ledger.ingest_batch(conn, [self.write_batch("b.jsonl", [rel("s", "R1", 1, "A", "B")])])
        finally:
            conn.close()

    def test_refresh_requires_published_snapshot(self):
        conn = store.connect(os.path.join(self.dir, "fresh2.db"))
        try:
            with self.assertRaises(UpstreamNotReady):
                ledger.refresh_presence(conn, os.path.join(self.dir, "nonexistent.db"))
        finally:
            conn.close()


class TestRevisionRules(EntityCase):
    def test_revision_chain_must_be_continuous(self):
        with self.assertRaises(RelationProtocolError):  # 首修订必须是 1
            self.ingest([rel("s", "R1", 2, "A", "B")], "gap1.jsonl")
        self.ingest([rel("s", "R1", 1, "A", "B")], "ok.jsonl")
        with self.assertRaises(RelationProtocolError):  # 跳号 1 -> 3
            self.ingest([rel("s", "R1", 3, "A", "C")], "gap2.jsonl")
        # 同批连续两段修订合法
        rep = self.ingest(
            [rel("s", "R1", 2, "A", "C"), rel("s", "R1", 3, "A", "D")], "chain.jsonl"
        )
        self.assertEqual(rep["new_revisions"], 2)

    def test_replay_identical_is_noop(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "a.jsonl")
        before = self.revision_count()
        rep = self.ingest([rel("s", "R1", 1, "A", "B")], "b.jsonl")
        self.assertEqual(rep["replayed_revisions"], 1)
        self.assertEqual(rep["new_revisions"], 0)
        self.assertEqual(self.revision_count(), before)

    def test_tampered_old_revision_rejected_even_when_superseded(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "a.jsonl")
        self.ingest([rel("s", "R1", 2, "A", "C")], "b.jsonl")  # R1 已到修订 2
        # 改写已被取代的修订 1：每个内容字段逐一尝试
        for mutated in (
            rel("s", "R1", 1, "A", "D"),
            rel("s", "R1", 1, "D", "B"),
            rel("s", "R1", 1, "A", "B", category="must_differ"),
            rel("s", "R1", 1, "A", "B", effective=False),
        ):
            with self.assertRaises(RevisionConflict, msg=str(mutated)):
                self.ingest([mutated], "tamper.jsonl")
        # 内容一致的重传仍然接受
        rep = self.ingest([rel("s", "R1", 1, "A", "B")], "replay.jsonl")
        self.assertEqual(rep["replayed_revisions"], 1)

    def test_conflicting_duplicates_within_batch(self):
        with self.assertRaises(RevisionConflict):
            self.ingest(
                [rel("s", "R1", 1, "A", "B"), rel("s", "R1", 1, "A", "C")],
                "dup.jsonl",
            )


class TestConflicts(EntityCase):
    def test_chain_conflict_reports_path_and_keeps_state(self):
        self.ingest([rel("s1", "R1", 1, "A", "B")], "a.jsonl")
        before_version = ledger.current_version(self.conn)
        before_revisions = self.revision_count()
        with self.assertRaises(ConstraintConflict) as ctx:
            self.ingest(
                [
                    rel("s2", "R2", 1, "B", "C"),
                    rel("s3", "R3", 1, "A", "C", category="must_differ"),
                ],
                "conflict.jsonl",
            )
        report = ctx.exception.report
        self.assertEqual(report["status"], "rejected")
        conflict = report["conflicts"][0]
        self.assertEqual(conflict["constraint"]["endpoints"], ["A", "C"])
        # 确定性路径：A -> B -> C，标明每步的关系身份
        self.assertEqual(
            [(s["from"], s["to"], s["source"], s["relation_id"]) for s in conflict["path"]],
            [("A", "B", "s1", "R1"), ("B", "C", "s2", "R2")],
        )
        # 整批回滚：版本与修订台账不变
        self.assertEqual(ledger.current_version(self.conn), before_version)
        self.assertEqual(self.revision_count(), before_revisions)

    def test_direct_pair_conflict(self):
        with self.assertRaises(ConstraintConflict):
            self.ingest(
                [
                    rel("s", "R1", 1, "A", "B"),
                    rel("s", "R2", 1, "A", "B", category="must_differ"),
                ],
                "direct.jsonl",
            )

    def test_batch_atomic_when_mixing_valid_and_conflicting(self):
        with self.assertRaises(ConstraintConflict):
            self.ingest(
                [
                    rel("s", "R1", 1, "A", "B"),  # 合法新关系
                    rel("s", "R2", 1, "C", "D"),
                    rel("s", "R3", 1, "D", "C", category="must_differ"),
                ],
                "mixed.jsonl",
            )
        # 合法行也不入账
        self.assertEqual(self.revision_count(), 0)

    def test_path_selection_is_deterministic(self):
        # 两条平行路径 A-B-C 与 A-D-C：必须稳定选字典序路径
        rows = [
            rel("s", "R1", 1, "A", "D"),
            rel("s", "R2", 1, "D", "C"),
            rel("s", "R3", 1, "A", "B"),
            rel("s", "R4", 1, "B", "C"),
            rel("s", "R5", 1, "A", "C", category="must_differ"),
        ]
        paths = []
        for _ in range(3):
            fresh = store.connect(os.path.join(self.dir, "det.db"))
            ledger.refresh_presence(fresh, self.loader_db)
            try:
                ledger.ingest_batch(fresh, [self.write_batch("det.jsonl", rows)])
            except ConstraintConflict as exc:
                paths.append(
                    [(s["from"], s["to"]) for s in exc.report["conflicts"][0]["path"]]
                )
            fresh.close()
            os.remove(os.path.join(self.dir, "det.db"))
        self.assertEqual(paths, [[("A", "B"), ("B", "C")]] * 3)


class TestDecisions(EntityCase):
    def test_split_and_merge_in_one_batch_one_version(self):
        self.ingest(
            [rel("s", "R1", 1, "A", "B"), rel("s", "R2", 1, "C", "D"),
             rel("s", "R5", 1, "E", "F")],
            "1.jsonl",
        )
        self.ingest([rel("s", "R3", 1, "B", "C")], "2.jsonl")  # 桥：ABCD 一体
        v_before = ledger.current_version(self.conn)
        self.assertEqual(self.entities(v_before)["E-A"], ["A", "B", "C", "D"])
        # 同批：撤销桥 R3 + 新增 D-E => {A,B} 拆出，{C,D} 与 {E,F} 合并
        rep = self.ingest(
            [rel("s", "R3", 2, "B", "C", effective=False), rel("s", "R6", 1, "D", "E")],
            "3.jsonl",
        )
        v_after = rep["version_created"]
        self.assertEqual(v_after, v_before + 1)  # 只产生一个新版本
        ents = self.entities(v_after)
        self.assertEqual(ents["E-A"], ["A", "B"])
        self.assertEqual(ents["E-C"], ["C", "D", "E", "F"])
        changes = ledger.changes_into(self.conn, v_after)
        self.assertEqual(changes["splits"][0]["entity"], "E-A")
        self.assertEqual(changes["splits"][0]["into_entities"], ["E-A", "E-C"])
        self.assertEqual(changes["merges"][0]["entity"], "E-C")
        self.assertEqual(changes["merges"][0]["from_entities"], ["E-A", "E-E"])

    def test_no_new_version_when_grouping_unchanged(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        v = ledger.current_version(self.conn)
        # 纯约束批次：分组不变 -> 不出版本
        rep = self.ingest([rel("s", "R2", 1, "A", "C", category="must_differ")], "2.jsonl")
        self.assertIsNone(rep["version_created"])
        self.assertEqual(ledger.current_version(self.conn), v)
        # 重投同一批次：也不出版本
        rep = self.ingest([rel("s", "R2", 1, "A", "C", category="must_differ")], "3.jsonl")
        self.assertIsNone(rep["version_created"])
        self.assertEqual(ledger.current_version(self.conn), v)

    def test_entity_id_stable_and_public(self):
        self.ingest([rel("s", "R1", 1, "B", "A")], "1.jsonl")
        self.assertIn("E-A", self.entities())  # E- + 最小成员编号
        self.ingest([rel("s", "R2", 1, "C", "D")], "2.jsonl")  # 无关变化
        self.assertIn("E-A", self.entities())  # 成员未变，标识不变

    def test_endpoint_change_moves_edge(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        self.assertEqual(self.entities()["E-A"], ["A", "B"])
        # 更高修订号更换端点：A-B -> A-C
        self.ingest([rel("s", "R1", 2, "A", "C")], "2.jsonl")
        ents = self.entities()
        self.assertEqual(ents["E-A"], ["A", "C"])
        self.assertEqual(ents["E-B"], ["B"])

    def test_category_change_via_higher_revision(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        self.assertEqual(self.entities()["E-A"], ["A", "B"])
        # 更高修订号改变类别：same_entity -> must_differ（撤销归并）
        self.ingest([rel("s", "R1", 2, "A", "B", category="must_differ")], "2.jsonl")
        ents = self.entities()
        self.assertEqual(ents["E-A"], ["A"])
        self.assertEqual(ents["E-B"], ["B"])

    def test_two_sources_revoke_one_keeps_entity(self):
        self.ingest(
            [
                rel("p", "R1", 1, "A", "B"),
                rel("q", "R2", 1, "A", "B"),
            ],
            "1.jsonl",
        )
        v = ledger.current_version(self.conn)
        rep = self.ingest([rel("p", "R1", 2, "A", "B", effective=False)], "2.jsonl")
        self.assertIsNone(rep["version_created"])  # 另一条依据仍有效
        self.assertEqual(ledger.current_version(self.conn), v)
        self.assertEqual(self.entities()["E-A"], ["A", "B"])
        ex = ledger.explain_device(self.conn, "A", None)
        self.assertEqual(
            [(e["source"], e["relation_id"]) for e in ex["basis_edges"]],
            [("q", "R2")],
        )


class TestRefresh(EntityCase):
    def test_delete_and_reappear(self):
        self.ingest([rel("s", "R1", 1, "A", "B"), rel("s", "R2", 1, "B", "C")], "1.jsonl")
        self.assertEqual(self.entities()["E-A"], ["A", "B", "C"])
        # 上游删除 B：A、C 断开，关系暂扣
        set_loader_devices(self.loader_db, ["A", "C", "D", "E", "F"])
        rep = ledger.refresh_presence(self.conn, self.loader_db)
        self.assertIsNotNone(rep["version_created"])
        ents = self.entities()
        self.assertEqual(ents["E-A"], ["A"])
        self.assertEqual(ents["E-C"], ["C"])
        status_decision = ledger.engine.compute_decision(  # type: ignore[attr-defined]
            ledger.presence_devices(self.conn), ledger.load_active_relations(self.conn)
        )
        self.assertEqual(len(status_decision["parked"]), 2)
        # 设备重现：重新参与，实体恢复
        set_loader_devices(self.loader_db, self.DEVICES)
        rep = ledger.refresh_presence(self.conn, self.loader_db)
        self.assertIsNotNone(rep["version_created"])
        self.assertEqual(self.entities()["E-A"], ["A", "B", "C"])

    def test_refresh_idempotent(self):
        ledger.refresh_presence(self.conn, self.loader_db)
        v = ledger.current_version(self.conn)
        rep = ledger.refresh_presence(self.conn, self.loader_db)
        self.assertIsNone(rep["version_created"])
        self.assertEqual(ledger.current_version(self.conn), v)


class TestHistoryAndExplain(EntityCase):
    def test_history_show_changes_explain(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        self.ingest([rel("s", "R2", 1, "B", "C")], "2.jsonl")
        versions = ledger.list_versions(self.conn)
        self.assertEqual(len(versions), 3)  # refresh + 2 批
        v3 = ledger.current_version(self.conn)
        ents = self.entities(v3)
        self.assertEqual(ents["E-A"], ["A", "B", "C"])

        changes = ledger.changes_into(self.conn, v3)
        self.assertEqual(changes["merges"][0]["from_entities"], ["E-A", "E-C"])

        ex = ledger.explain_device(self.conn, "C", None)
        self.assertEqual(ex["entity_id"], "E-A")
        self.assertEqual(ex["members"], ["A", "B", "C"])
        # 从 C 到 A 的路径：C -> B -> A
        self.assertEqual(
            [(s["from"], s["to"]) for s in ex["paths_from_device"]["A"]],
            [("C", "B"), ("B", "A")],
        )
        # 历史版本中的解释
        ex_v2 = ledger.explain_device(self.conn, "C", 2)
        self.assertEqual(ex_v2["entity_id"], "E-C")  # 版本 2 时 C 还是单设备实体
        with self.assertRaises(VersionNotFound):
            ledger.changes_into(self.conn, 1)
        with self.assertRaises(VersionNotFound):
            ledger.explain_device(self.conn, "A", 99)


class TestCrashRecovery(EntityCase):
    def test_crash_between_ledger_and_decision(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        before_version = ledger.current_version(self.conn)
        before_revisions = self.revision_count()
        rows = [rel("s", "R2", 1, "C", "D")]
        batch = self.write_batch("crash.jsonl", rows)
        # 子进程真实崩溃（os._exit(42)）
        proc = subprocess.run(
            [
                sys.executable, "-I", os.path.join(ROOT, "run_entity.py"),
                "ingest", "--ledger-db", self.ledger_db,
                "--batch", batch,
                "--fault", FAULT_AFTER_LEDGER_BEFORE_DECISION,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, CRASH_EXIT_CODE)
        # 重开台账：无半批状态
        conn2 = store.connect(self.ledger_db)
        try:
            self.assertEqual(ledger.current_version(conn2), before_version)
            self.assertEqual(
                conn2.execute("SELECT COUNT(*) AS c FROM revisions").fetchone()["c"],
                before_revisions,
            )
            self.assertEqual(int(store.get_meta(conn2, "last_batch_id")), 1)
            # 重投同批成功
            rep = ledger.ingest_batch(conn2, [batch])
            self.assertEqual(rep["status"], "accepted")
            self.assertEqual(rep["version_created"], before_version + 1)
        finally:
            conn2.close()


class TestRecompute(EntityCase):
    def test_verify_ok(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        self.ingest([rel("s", "R2", 1, "B", "C")], "2.jsonl")
        set_loader_devices(self.loader_db, ["A", "B", "C", "D"])
        ledger.refresh_presence(self.conn, self.loader_db)
        self.conn.close()
        report = run_verify(self.ledger_db)
        self.assertTrue(report["ok"], json.dumps(report, ensure_ascii=False))
        self.assertEqual(report["versions_checked"], 4)
        self.assertTrue(report["head_check_ok"])
        self.conn = store.connect(self.ledger_db)

    def test_verify_detects_tampered_history(self):
        self.ingest([rel("s", "R1", 1, "A", "B")], "1.jsonl")
        # 直接篡改历史版本的成员记录
        self.conn.execute(
            "UPDATE version_entities SET device_id='Z' "
            "WHERE version=2 AND entity_id='E-A' AND device_id='A'"
        )
        self.conn.commit()
        self.conn.close()
        report = run_verify(self.ledger_db)
        self.assertFalse(report["ok"])
        self.assertTrue(any("分组不一致" in p for p in report["problems"]))
        self.conn = store.connect(self.ledger_db)


if __name__ == "__main__":
    unittest.main()
