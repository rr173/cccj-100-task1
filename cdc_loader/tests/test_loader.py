"""装载器单元测试（标准库 unittest，无第三方依赖）。

在项目根执行：
    python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc import applier, log_ingest, protocol, snapshot, store  # noqa: E402
from cdc.errors import PayloadConflict, ProtocolError, SnapshotIncomplete  # noqa: E402
from cdc.fault import FaultInjector  # noqa: E402

FIELD_MAP = {
    "device_id": "TEXT",
    "vendor": "TEXT",
    "model": "TEXT",
    "firmware": "TEXT",
    "temperature": "REAL",
    "cycle_count": "INTEGER",
}


def r(device_id: str, vendor="Acme", model="X", firmware="1.0",
      temperature=1.0, cycle_count=1) -> dict:
    return {
        "device_id": device_id,
        "vendor": vendor,
        "model": model,
        "firmware": firmware,
        "temperature": temperature,
        "cycle_count": cycle_count,
    }


class ProtocolTests(unittest.TestCase):
    def test_canonical_json_is_order_independent(self) -> None:
        self.assertEqual(
            protocol.canonical_json({"a": 1, "b": 2}),
            protocol.canonical_json({"b": 2, "a": 1}),
        )

    def test_integer_rejects_bool(self) -> None:
        with self.assertRaises(ProtocolError):
            protocol.validate_row(
                {"device_id": "D1", "vendor": "v", "model": "m",
                 "temperature": 1.0, "cycle_count": True},
                FIELD_MAP,
            )

    def test_real_accepts_int_rejects_bool(self) -> None:
        row = protocol.validate_row(
            {"device_id": "D1", "vendor": "v", "model": "m", "firmware": "f",
             "temperature": 3, "cycle_count": 2},
            FIELD_MAP,
        )
        self.assertEqual(row["temperature"], 3.0)
        with self.assertRaises(ProtocolError):
            protocol.validate_row(
                {"device_id": "D1", "vendor": "v", "model": "m", "firmware": "f",
                 "temperature": True, "cycle_count": 2},
                FIELD_MAP,
            )

    def test_missing_and_extra_fields_rejected(self) -> None:
        with self.assertRaises(ProtocolError):
            protocol.validate_row({"device_id": "D1"}, FIELD_MAP)
        with self.assertRaises(ProtocolError):
            protocol.validate_row({**r("D1"), "ghost": 1}, FIELD_MAP)

    def test_manifest_requires_device_id_first_text_field(self) -> None:
        bad = {
            "cut_point_lsn": 10,
            "fields": [{"name": "vendor", "type": "TEXT"},
                       {"name": "device_id", "type": "TEXT"}],
            "shards": [{"file": "s.jsonl", "rows": 0, "sha256": "a" * 64}],
        }
        with self.assertRaises(ProtocolError):
            protocol.validate_manifest(bad)

    def test_validate_update_allows_pk_migration(self) -> None:
        ev = protocol.validate_event(
            {"event": "UPDATE", "txn_id": "T", "seq": 1,
             "before": r("D1"), "after": r("D2")},
            FIELD_MAP,
        )
        self.assertEqual((ev["old_pk"], ev["new_pk"]), ("D1", "D2"))

    def test_commit_requires_lsn(self) -> None:
        with self.assertRaises(ProtocolError):
            protocol.validate_event(
                {"event": "COMMIT", "txn_id": "T", "seq": 9}, FIELD_MAP
            )


class LoaderHarness:
    """构造临时快照库并提供便捷写入/读取。"""

    def __init__(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="cdc-test-")
        self.db_path = os.path.join(self.dir, "state.db")
        self.manifest_path = os.path.join(self.dir, "manifest.json")
        self.rows = [r("D001"), r("D002"), r("D003")]
        self.cut = 100
        self._write_shards()

    def _write_shards(self) -> None:
        files = []
        per = 2
        idx = 0
        while idx < len(self.rows):
            part = self.rows[idx:idx + per]
            fname = f"shard_{idx}.jsonl"
            path = os.path.join(self.dir, fname)
            with open(path, "w", encoding="utf-8") as fh:
                for row in part:
                    fh.write(json.dumps(row, sort_keys=True) + "\n")
            files.append({"file": fname, "rows": len(part),
                          "sha256": protocol.sha256_file(path)})
            idx += per
        manifest = {
            "cut_point_lsn": self.cut,
            "fields": [{"name": k, "type": v} for k, v in FIELD_MAP.items()],
            "shards": files,
        }
        with open(self.manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh)
        self.shard_paths = [os.path.join(self.dir, f["file"]) for f in files]

    def connect(self):
        return store.connect(self.db_path)

    def publish(self, conn=None) -> "store.sqlite3.Connection":
        own = conn is None
        conn = conn or self.connect()
        snapshot.import_snapshot(conn, self.manifest_path, self.shard_paths)
        return conn

    def write_log(self, name: str, events: list[dict]) -> str:
        path = os.path.join(self.dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev, sort_keys=True) + "\n")
        return path

    def master_ids(self, conn) -> set[str]:
        return {
            json.loads(row["payload"])["device_id"]
            for row in conn.execute("SELECT payload FROM device_master")
        }


class SnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h = LoaderHarness()

    def test_missing_shard_blocks_publish_and_query(self) -> None:
        conn = self.h.connect()
        with self.assertRaises(SnapshotIncomplete):
            snapshot.import_snapshot(
                conn, self.h.manifest_path, self.h.shard_paths[:-1]
            )
        self.assertFalse(snapshot.is_published(conn))
        self.assertEqual(
            conn.execute("SELECT COUNT(*) AS c FROM device_master").fetchone()["c"], 0
        )

    def test_corrupt_shard_rejected(self) -> None:
        path = self.h.shard_paths[0]
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
        bad = json.loads(lines[0])
        bad["vendor"] = "TAMPERED"
        lines[0] = json.dumps(bad, sort_keys=True) + "\n"
        with open(path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        conn = self.h.connect()
        with self.assertRaises(ProtocolError):
            snapshot.import_snapshot(conn, self.h.manifest_path, self.h.shard_paths)
        self.assertFalse(snapshot.is_published(conn))

    def test_publish_is_idempotent_and_resumes_after_crash(self) -> None:
        import cdc.fault as fault_mod

        conn = self.h.connect()
        inj = FaultInjector("before_snapshot_publish")
        # 把 os._exit 替换成抛 SystemExit 的替身，在单进程内验证崩溃点，
        # 真实的 os._exit(42) 行为由 examples/demo.py 的子进程场景覆盖。
        orig_exit = fault_mod.os._exit
        fault_mod.os._exit = lambda code: (_ for _ in ()).throw(SystemExit(code))
        try:
            with self.assertRaises(SystemExit):
                snapshot.import_snapshot(
                    conn, self.h.manifest_path, self.h.shard_paths, inj
                )
        finally:
            fault_mod.os._exit = orig_exit
        conn.close()

        # 注入点只触发一次且进程已“崩溃”：重开同一库，分片已暂存，应直接发布。
        conn2 = self.h.connect()
        report = snapshot.import_snapshot(
            conn2, self.h.manifest_path, self.h.shard_paths
        )
        self.assertEqual(report["publish"]["status"], "published")
        self.assertEqual(self.h.master_ids(conn2), {"D001", "D002", "D003"})


class LogSemanticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h = LoaderHarness()

    def test_commit_at_cut_point_is_not_applied(self) -> None:
        conn = self.h.publish()
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "TE"},
            {"event": "INSERT", "txn_id": "TE", "seq": 1, "row": r("D999")},
            {"event": "COMMIT", "txn_id": "TE", "seq": 2, "commit_lsn": self.h.cut},
        ])
        rep = log_ingest.ingest_logs(conn, [log])
        self.assertEqual(rep["counters"]["txn_applied"], 0)
        self.assertEqual(store.get_meta(conn, "checkpoint_lsn"), self.h.cut)
        self.assertNotIn("D999", self.h.master_ids(conn))

    def test_aborted_and_open_txns_never_reach_master(self) -> None:
        conn = self.h.publish()
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "TA"},
            {"event": "DELETE", "txn_id": "TA", "seq": 1, "device_id": "D001"},
            {"event": "ABORT", "txn_id": "TA", "seq": 2},
            {"event": "BEGIN", "txn_id": "TO"},
            {"event": "DELETE", "txn_id": "TO", "seq": 1, "device_id": "D002"},
        ])
        log_ingest.ingest_logs(conn, [log])
        self.assertEqual(self.h.master_ids(conn), {"D001", "D002", "D003"})
        self.assertEqual(
            [t["txn_id"] for t in log_ingest.open_transactions(conn)], ["TO"]
        )

    def test_pk_migration_and_multi_change_in_one_txn(self) -> None:
        conn = self.h.publish()
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "TM"},
            {"event": "UPDATE", "txn_id": "TM", "seq": 1,
             "before": r("D001"), "after": r("D001", vendor="Z")},
            {"event": "UPDATE", "txn_id": "TM", "seq": 2,
             "before": r("D001", vendor="Z"), "after": r("D100", vendor="Z")},
            {"event": "COMMIT", "txn_id": "TM", "seq": 3, "commit_lsn": 101},
        ])
        log_ingest.ingest_logs(conn, [log])
        ids = self.h.master_ids(conn)
        self.assertNotIn("D001", ids)
        self.assertIn("D100", ids)
        self.assertEqual(store.get_meta(conn, "checkpoint_lsn"), 101)

    def test_update_before_image_mismatch_rejected(self) -> None:
        conn = self.h.publish()
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "TX"},
            {"event": "UPDATE", "txn_id": "TX", "seq": 1,
             "before": r("D001", vendor="WRONG"), "after": r("D001")},
            {"event": "COMMIT", "txn_id": "TX", "seq": 2, "commit_lsn": 101},
        ])
        with self.assertRaises(ProtocolError):
            log_ingest.ingest_logs(conn, [log])
        # 应用事务回滚，主表与检查点不变。
        self.assertEqual(store.get_meta(conn, "checkpoint_lsn"), self.h.cut)
        self.assertEqual(self.h.master_ids(conn), {"D001", "D002", "D003"})

    def test_insert_existing_and_delete_missing_rejected(self) -> None:
        conn = self.h.publish()
        bad_insert = self.h.write_log("i.jsonl", [
            {"event": "BEGIN", "txn_id": "TI"},
            {"event": "INSERT", "txn_id": "TI", "seq": 1, "row": r("D001")},
            {"event": "COMMIT", "txn_id": "TI", "seq": 2, "commit_lsn": 101},
        ])
        with self.assertRaises(ProtocolError):
            log_ingest.ingest_logs(conn, [bad_insert])
        bad_delete = self.h.write_log("d.jsonl", [
            {"event": "BEGIN", "txn_id": "TD"},
            {"event": "DELETE", "txn_id": "TD", "seq": 1, "device_id": "D404"},
            {"event": "COMMIT", "txn_id": "TD", "seq": 2, "commit_lsn": 102},
        ])
        with self.assertRaises(ProtocolError):
            log_ingest.ingest_logs(conn, [bad_delete])

    def test_seq_must_increase(self) -> None:
        conn = self.h.publish()
        # seq 先 2 后 1（且事件身份不同），应触发“序号未递增”而非载荷冲突。
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "TS"},
            {"event": "DELETE", "txn_id": "TS", "seq": 2, "device_id": "D001"},
            {"event": "DELETE", "txn_id": "TS", "seq": 1, "device_id": "D002"},
        ])
        with self.assertRaises(ProtocolError):
            log_ingest.ingest_logs(conn, [log])

    def test_commit_positions_must_increase_on_first_seen(self) -> None:
        conn = self.h.publish()
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "T1"},
            {"event": "COMMIT", "txn_id": "T1", "seq": 1, "commit_lsn": 120},
            {"event": "BEGIN", "txn_id": "T2"},
            {"event": "COMMIT", "txn_id": "T2", "seq": 1, "commit_lsn": 110},
        ])
        with self.assertRaises(ProtocolError):
            log_ingest.ingest_logs(conn, [log])

    def test_duplicate_payload_idempotent_conflicting_payload_rejected(self) -> None:
        conn = self.h.publish()
        events = [
            {"event": "BEGIN", "txn_id": "TR"},
            {"event": "INSERT", "txn_id": "TR", "seq": 1, "row": r("D777")},
            {"event": "COMMIT", "txn_id": "TR", "seq": 2, "commit_lsn": 101},
        ]
        first = self.h.write_log("first.jsonl", events)
        rep1 = log_ingest.ingest_logs(conn, [first])
        self.assertEqual(rep1["counters"]["txn_applied"], 1)

        # 完全相同的重传：全部去重。
        replay = self.h.write_log("replay.jsonl", events)
        rep2 = log_ingest.ingest_logs(conn, [replay])
        self.assertEqual(rep2["counters"]["events_duplicate"], 3)
        self.assertEqual(rep2["counters"]["txn_applied"], 0)

        # 同一身份不同载荷：冲突。
        conflict = self.h.write_log("conflict.jsonl", [
            {"event": "INSERT", "txn_id": "TR", "seq": 1,
             "row": r("D777", vendor="CHANGED")},
        ])
        with self.assertRaises(PayloadConflict):
            log_ingest.ingest_logs(conn, [conflict])

    def test_resume_without_in_memory_state(self) -> None:
        """关闭连接模拟进程退出，重开库后暂存事件仍可继续应用。"""
        conn = self.h.publish()
        # 只暂存一个尚未提交的事务。
        partial = self.h.write_log("partial.jsonl", [
            {"event": "BEGIN", "txn_id": "TP"},
            {"event": "INSERT", "txn_id": "TP", "seq": 1, "row": r("D555")},
        ])
        log_ingest.ingest_logs(conn, [partial])
        conn.close()

        conn2 = self.h.connect()
        commit = self.h.write_log("commit.jsonl", [
            {"event": "COMMIT", "txn_id": "TP", "seq": 2, "commit_lsn": 130},
        ])
        rep = log_ingest.ingest_logs(conn2, [commit])
        self.assertEqual(rep["counters"]["txn_applied"], 1)
        self.assertIn("D555", self.h.master_ids(conn2))
        self.assertEqual(store.get_meta(conn2, "checkpoint_lsn"), 130)

    def test_apply_ready_orders_by_commit_lsn(self) -> None:
        conn = self.h.publish()
        # 文件内先出现 T-high 的 BEGIN，提交顺序由 commit_lsn 决定。
        log = self.h.write_log("l.jsonl", [
            {"event": "BEGIN", "txn_id": "TH"},
            {"event": "BEGIN", "txn_id": "TL"},
            {"event": "INSERT", "txn_id": "TH", "seq": 1, "row": r("D901")},
            {"event": "INSERT", "txn_id": "TL", "seq": 1, "row": r("D900")},
            {"event": "COMMIT", "txn_id": "TH", "seq": 2, "commit_lsn": 202},
            {"event": "COMMIT", "txn_id": "TL", "seq": 2, "commit_lsn": 201},
        ])
        with self.assertRaises(ProtocolError):
            # 首次提交位置必须递增：202 之后不允许再首次出现 201。
            log_ingest.ingest_logs(conn, [log])


class ApplierUnitTests(unittest.TestCase):
    def test_apply_txn_skips_terminal_events(self) -> None:
        # store.connect 已按标准 SCHEMA 建好 device_master / log_events。
        conn = store.connect(":memory:")
        conn.execute(
            "INSERT INTO device_master VALUES(?, ?)",
            ("D1", protocol.canonical_json(r("D1"))),
        )
        events = [
            (1, "UPDATE", {"event": "UPDATE", "txn_id": "T", "seq": 1,
                           "before": r("D1"), "after": r("D1", vendor="Q"),
                           "old_pk": "D1", "new_pk": "D1"}),
            (2, "COMMIT", {"event": "COMMIT", "txn_id": "T", "seq": 2,
                           "commit_lsn": 5}),
        ]
        for seq, etype, ev in events:
            conn.execute(
                "INSERT INTO log_events(txn_id, seq, event_type, payload, "
                "source_file, line_no) VALUES(?, ?, ?, ?, ?, ?)",
                ("T", seq, etype, protocol.canonical_json(ev), "test", seq),
            )
        count = applier.apply_txn(conn, "T")
        self.assertEqual(count, 1)
        row = json.loads(
            conn.execute("SELECT payload FROM device_master").fetchone()["payload"]
        )
        self.assertEqual(row["vendor"], "Q")


if __name__ == "__main__":
    unittest.main(verbosity=2)
