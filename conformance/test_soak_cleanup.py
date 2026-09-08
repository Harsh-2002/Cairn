#!/usr/bin/env python3
"""Deterministic regressions for the soak observer; no server, sockets, or load campaign."""
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from soak_cleanup import CleanupProbe, PROBE_PAGE, TerminalCleanup


class QueueTests(unittest.TestCase):
    def test_overflow_fails_without_evicting_or_growing_history(self):
        tracker = TerminalCleanup(lambda ids: dict.fromkeys(ids, "debt"), limit=2)
        for uid in ("first", "second", "overflow"):
            tracker.register(uid)
        self.assertEqual(list(tracker.pending), ["first", "second"])
        self.assertEqual((tracker.registered, tracker.failed, tracker.peak), (3, 1, 2))

    def test_retirement_forgets_ids_and_allows_bounded_reuse(self):
        tracker = TerminalCleanup(lambda ids: dict.fromkeys(ids, ""), limit=2)
        for n in range(100):
            tracker.register(str(n))
            tracker.poll()
        self.assertEqual((tracker.verified, tracker.peak, len(tracker.pending)), (100, 1, 0))

    def test_pages_are_bounded_and_pending_peers_do_not_starve(self):
        pages = []
        def probe(ids):
            pages.append(ids)
            return dict.fromkeys(ids, "debt")
        tracker = TerminalCleanup(probe)
        for n in range(PROBE_PAGE + 1):
            tracker.register(str(n))
        tracker.poll()
        tracker.poll()
        self.assertEqual([len(page) for page in pages], [PROBE_PAGE, PROBE_PAGE])
        self.assertEqual(pages[1][0], str(PROBE_PAGE))

    def test_retry_and_final_drain_preserve_original_deadline(self):
        now = [0]
        tracker = TerminalCleanup(lambda ids: dict.fromkeys(ids, "debt"), clock=lambda: now[0])
        tracker.register("old")
        now[0] = 29
        tracker.poll()
        self.assertEqual(tracker.pending["old"], 30)
        now[0] = 31
        tracker.start()
        result = tracker.finish()
        self.assertEqual((result["verified"], result["pending"], result["failed"]), (0, 1, 1))

    def test_absence_first_observed_after_deadline_still_fails(self):
        now = [0]
        tracker = TerminalCleanup(lambda ids: dict.fromkeys(ids, ""), clock=lambda: now[0])
        tracker.register("late")
        now[0] = 30
        tracker.poll()
        self.assertEqual((tracker.verified, tracker.failed, len(tracker.pending)), (0, 1, 1))

    def test_register_does_not_wait_for_probe_io_and_final_drain_checks_every_id(self):
        entered, release = threading.Event(), threading.Event()
        def probe(ids):
            entered.set()
            if not release.wait(2):
                raise AssertionError("test did not release probe")
            return dict.fromkeys(ids, "")
        tracker = TerminalCleanup(probe)
        tracker.register("first")
        tracker.start()
        try:
            self.assertTrue(entered.wait(2))
            tracker.register("during-io")
            self.assertEqual(tracker.registered, 2)
        finally:
            release.set()
        result = tracker.finish()
        self.assertEqual((result["verified"], result["pending"], result["failed"]), (2, 0, 0))

    def test_probe_errors_fail_without_discarding_unverified_ids(self):
        def probe(_):
            raise sqlite3.OperationalError("unsupported schema")
        tracker = TerminalCleanup(probe)
        tracker.register("unverified")
        tracker.poll()
        self.assertEqual(list(tracker.pending), ["unverified"])
        self.assertIn("unsupported schema", tracker.first_failure)


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "metadata.db"
        self.conn = sqlite3.connect(self.db)
        self.addCleanup(self.conn.close)
        self.conn.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id TEXT);
            CREATE TABLE multipart_uploads
                (id TEXT PRIMARY KEY, bucket_name TEXT, owner_id TEXT, initiated_by TEXT);
            CREATE TABLE multipart_parts (upload_id TEXT, size INTEGER);
            CREATE TABLE multipart_part_reservations (upload_id TEXT, reserved_bytes INTEGER);
            CREATE TABLE multipart_staging_cleanups
                (upload_id TEXT, bucket_name TEXT, principal_id TEXT, bytes INTEGER);
            CREATE TABLE multipart_bucket_stats
                (bucket_name TEXT PRIMARY KEY, active_uploads INTEGER, staged_bytes INTEGER);
            CREATE TABLE multipart_principal_stats
                (principal_id TEXT PRIMARY KEY, active_uploads INTEGER, staged_bytes INTEGER);
            CREATE TABLE storage_write_intents (upload_id TEXT);
            CREATE TABLE storage_cleanups (storage_path TEXT, quota_owner_path TEXT);
            INSERT INTO buckets VALUES ('soak-mp', 'root');
            INSERT INTO multipart_bucket_stats VALUES ('soak-mp', 0, 0);
            INSERT INTO multipart_principal_stats VALUES ('root', 0, 0);
        """)
        self.probe = CleanupProbe(self.db, self.root, "soak-mp")
        self.directory = self.root / ".staging" / "multipart" / "terminal"

    def debt(self):
        self.conn.executescript("""
            INSERT INTO multipart_staging_cleanups VALUES ('terminal', 'soak-mp', 'root', 7);
            UPDATE multipart_bucket_stats SET staged_bytes=7;
            UPDATE multipart_principal_stats SET staged_bytes=7;
        """)

    def test_physical_absence_alone_cannot_retire_quota_debt(self):
        self.debt()
        self.assertIn("multipart_staging_cleanups", self.probe(["terminal"])["terminal"])
        self.conn.executescript("""
            DELETE FROM multipart_staging_cleanups;
            UPDATE multipart_bucket_stats SET staged_bytes=0;
            UPDATE multipart_principal_stats SET staged_bytes=0;
        """)
        self.assertEqual(self.probe(["terminal"]), {"terminal": ""})

    def test_directory_or_symlink_surviving_quota_retirement_fails(self):
        self.directory.mkdir(parents=True)
        with self.assertRaisesRegex(AssertionError, "outlived its quota debt"):
            self.probe(["terminal"])
        self.directory.rmdir()
        self.directory.symlink_to(self.root / "missing")
        with self.assertRaisesRegex(AssertionError, "outlived its quota debt"):
            self.probe(["terminal"])

    def test_live_files_with_charged_debt_remain_pending(self):
        self.debt()
        self.directory.mkdir(parents=True)
        (self.directory / "part").write_bytes(b"charged")
        self.assertIn("staging directory", self.probe(["terminal"])["terminal"])

    def test_exact_path_and_spool_owner_queries_do_not_match_neighbor_sessions(self):
        for path, owner in ((".staging/multipart/terminal/part", None),
                            (".staging/spool.index.tmp", ".staging/multipart/terminal/part")):
            with self.subTest(path=path):
                self.conn.execute("INSERT INTO storage_cleanups VALUES (?, ?)", (path, owner))
                self.conn.commit()
                state = self.probe(["terminal", "termina", "terminal-extra"])
                self.assertIn("storage_cleanups.", state["terminal"])
                self.assertEqual((state["termina"], state["terminal-extra"]), ("", ""))
                self.conn.execute("DELETE FROM storage_cleanups")
                self.conn.commit()

    def test_unfinished_intent_prevents_retirement_even_without_a_file(self):
        self.conn.execute("INSERT INTO storage_write_intents VALUES ('terminal')")
        self.conn.commit()
        self.assertIn("storage_write_intents", self.probe(["terminal"])["terminal"])

    def test_live_parts_reservations_and_debt_all_contribute_to_both_rollups(self):
        self.debt()
        self.conn.executescript("""
            INSERT INTO multipart_uploads VALUES ('active', 'soak-mp', 'root', NULL);
            INSERT INTO multipart_parts VALUES ('active', 10);
            INSERT INTO multipart_part_reservations VALUES ('active', 3);
            UPDATE multipart_bucket_stats SET active_uploads=1, staged_bytes=20;
            UPDATE multipart_principal_stats SET active_uploads=1, staged_bytes=20;
        """)
        self.assertEqual(self.probe(["other"]), {"other": ""})
        for table in ("multipart_bucket_stats", "multipart_principal_stats"):
            with self.subTest(table=table):
                self.conn.execute(f"UPDATE {table} SET staged_bytes=13")
                self.conn.commit()
                with self.assertRaisesRegex(AssertionError, table):
                    self.probe(["other"])
                self.conn.execute(f"UPDATE {table} SET staged_bytes=20")
                self.conn.commit()

    def test_concurrent_retirement_cannot_create_a_mixed_time_accounting_view(self):
        self.debt()
        accounting = self.probe.accounting
        def retire_after_snapshot(conn, bucket, principal):
            self.conn.executescript("""
                DELETE FROM multipart_staging_cleanups;
                UPDATE multipart_bucket_stats SET staged_bytes=0;
                UPDATE multipart_principal_stats SET staged_bytes=0;
            """)
            accounting(conn, bucket, principal)
        self.probe.accounting = retire_after_snapshot
        self.assertIn("multipart_staging_cleanups", self.probe(["terminal"])["terminal"])
        self.assertEqual(self.probe(["terminal"]), {"terminal": ""})

    def test_missing_schema_is_a_failure(self):
        self.conn.execute("DROP TABLE storage_write_intents")
        self.conn.commit()
        with self.assertRaises(sqlite3.OperationalError):
            self.probe(["terminal"])


if __name__ == "__main__":
    unittest.main()
