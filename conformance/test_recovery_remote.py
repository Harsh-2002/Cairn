#!/usr/bin/env python3
"""Focused wire regressions for the disposable recovery fault proxy (stdlib only)."""
import contextlib
import copy
import http.client
import http.server
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from recovery_remote import FaultProxy
from recovery_state import (database_rows, durable_tables, rows,
                            recovered_artifacts_absent, recovered_database_rows,
                            same_database_rows, same_live_files)


@contextlib.contextmanager
def proxy_response(body=b"", headers=(), length=None):
    class Peer(http.server.BaseHTTPRequestHandler):
        def respond(self):
            self.send_response(200)
            self.send_header("content-length", str(len(body) if length is None else length))
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        do_POST = do_PUT = do_HEAD = respond

        def log_message(self, *_):
            pass

    peer = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Peer)
    peer.daemon_threads = True
    threading.Thread(target=peer.serve_forever, daemon=True).start()
    proxy = FaultProxy(peer.server_port, None)
    connection = http.client.HTTPConnection("127.0.0.1", proxy.server_port, timeout=5)
    try:
        yield connection, proxy
    finally:
        connection.close()
        proxy.release.set()
        proxy.shutdown()
        proxy.server_close()
        peer.shutdown()
        peer.server_close()


class FaultProxyTests(unittest.TestCase):
    def test_xml_control_body_is_opaque_even_with_recursive_entities(self):
        body = (b'<!DOCTYPE InitiateMultipartUploadResult [<!ENTITY recursive "&recursive;">]>'
                b'<InitiateMultipartUploadResult><UploadId>&recursive;</UploadId></InitiateMultipartUploadResult>')
        with proxy_response(body) as (connection, proxy):
            connection.request("POST", "/journal/object?uploads", body=b"")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), body)
            self.assertEqual(proxy.errors, [])

    def test_only_required_fixed_response_headers_are_forwarded(self):
        with proxy_response(headers=(("ETag", '"part-identity"'), ("X-Untrusted-Input", "extra"))) as (connection, _):
            connection.request("PUT", "/journal/object?partNumber=1&uploadId=fixture", body=b"")
            response = connection.getresponse()
            self.assertEqual(response.getheader("etag"), '"part-identity"')
            self.assertIsNone(response.getheader("x-untrusted-input"))
            self.assertEqual(response.read(), b"")

    def test_folded_crlf_response_value_fails_before_headers_are_written(self):
        with proxy_response(headers=(("ETag", '"part"\r\n X-Injected: extra'),)) as (connection, proxy):
            connection.request("PUT", "/journal/object?partNumber=1&uploadId=fixture", body=b"")
            with self.assertRaises(http.client.RemoteDisconnected):
                connection.getresponse()
            self.assertEqual(proxy.errors, ["ValueError"])

    def test_head_retains_the_peer_object_length_without_a_body(self):
        with proxy_response(length=99) as (connection, _):
            connection.request("HEAD", "/journal/object")
            response = connection.getresponse()
            self.assertEqual(response.getheader("content-length"), "99")
            self.assertEqual(response.read(), b"")


class RestoreStateTests(unittest.TestCase):
    @staticmethod
    def fixture():
        before = {
            "storage_recovery_state": [{"singleton": 1, "generation": "1" * 32,
                                        "coverage_state": "incomplete", "coverage_identity": None,
                                        "baseline_completed_at": None}],
            "storage_write_intents": [{"attempt_id": "owned"}],
            "storage_intent_paths": [{"attempt_id": "owned", "storage_path": "scratch"}],
            "storage_cleanups": [{"id": "cleanup", "claim_token": "old-token", "storage_path": "old-blob"}],
            "multipart_uploads": [{"id": "upload", "bucket_name": "bucket", "owner_id": "owner",
                                   "initiated_by": "writer", "status": "completing",
                                   "completion_claim_token": "old-token", "additive_field": "preserve"}],
            "multipart_parts": [{"upload_id": "upload", "size": 7, "storage_path": "part",
                                 "etag": "part-etag"}],
            "multipart_part_reservations": [{"upload_id": "upload", "reserved_bytes": 11}],
            "multipart_staging_cleanups": [{"bucket_name": "bucket", "principal_id": "writer", "bytes": 13}],
            "multipart_bucket_stats": [{"bucket_name": "bucket", "active_uploads": 1,
                                        "staged_bytes": 31, "additive_field": "preserve"}],
            "multipart_principal_stats": [{"principal_id": "writer", "active_uploads": 1,
                                           "staged_bytes": 31}],
            "object_versions": [{"storage_path": "object", "metadata": "preserve"}],
            "future_empty_table": [],
        }
        after = copy.deepcopy(before)
        after["storage_recovery_state"][0]["generation"] = "2" * 32
        for table in ("storage_write_intents", "storage_intent_paths", "storage_cleanups",
                      "multipart_part_reservations", "multipart_staging_cleanups"):
            after[table] = []
        after["multipart_uploads"][0].update(status="active", completion_claim_token=None)
        for table in ("multipart_bucket_stats", "multipart_principal_stats"):
            after[table][0]["staged_bytes"] = 7
        return before, after

    def test_exact_recovery_changes_pass_with_charged_reservation_and_debt(self):
        recovered_database_rows(*self.fixture())

    def test_missing_fresh_generation_or_changed_coverage_fails(self):
        for change in ({"generation": "1" * 32}, {"generation": "not-a-token"},
                       {"coverage_state": "complete"}, {"baseline_completed_at": 123}):
            with self.subTest(change=change):
                before, after = self.fixture()
                after["storage_recovery_state"][0].update(change)
                with self.assertRaises(AssertionError):
                    recovered_database_rows(before, after)

    def test_quota_undercharge_and_overcharge_fail(self):
        for amount in (0, 6, 8, 31):
            with self.subTest(amount=amount):
                before, after = self.fixture()
                for table in ("multipart_bucket_stats", "multipart_principal_stats"):
                    after[table][0]["staged_bytes"] = amount
                with self.assertRaises(AssertionError):
                    recovered_database_rows(before, after)

    def test_dropping_a_live_part_cannot_be_hidden_by_adjusting_quota(self):
        before, after = self.fixture()
        after["multipart_parts"] = []
        for table in ("multipart_bucket_stats", "multipart_principal_stats"):
            after[table][0]["staged_bytes"] = 0
        with self.assertRaises(AssertionError):
            recovered_database_rows(before, after)

    def test_additive_fields_and_remaining_claims_are_not_ignored(self):
        mutations = [
            lambda state: state["multipart_uploads"][0].update(additive_field="lost"),
            lambda state: state["multipart_bucket_stats"][0].update(additive_field="lost"),
            lambda state: state["object_versions"][0].update(metadata="lost"),
            lambda state: state["multipart_uploads"][0].update(completion_claim_token="old-token"),
            lambda state: state["storage_cleanups"].append({"id": "unresolved"}),
            lambda state: state.pop("future_empty_table"),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                before, after = self.fixture()
                mutate(after)
                with self.assertRaises(AssertionError):
                    recovered_database_rows(before, after)

    def test_observation_connections_close_before_offline_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "source.db"
            with contextlib.closing(sqlite3.connect(database)) as connection:
                connection.executescript("CREATE TABLE metadata (id); INSERT INTO metadata VALUES (1);")
            connect, opened = sqlite3.connect, []

            def observe(*args, **kwargs):
                connection = connect(*args, **kwargs)
                opened.append(connection)  # Retain references so garbage collection cannot close them.
                return connection

            with patch("recovery_state.sqlite3.connect", side_effect=observe):
                self.assertEqual(durable_tables(database), ["metadata"])
                self.assertEqual(rows(database, "metadata"), [{"id": 1}])
                self.assertEqual(database_rows(database), {"metadata": [{"id": 1}]})
                with self.assertRaises(sqlite3.OperationalError):
                    rows(database, "missing")
            self.assertEqual(len(opened), 4)
            for connection in opened:
                with self.assertRaises(sqlite3.ProgrammingError):
                    connection.execute("SELECT 1")

    def test_snapshot_comparison_includes_empty_tables_and_all_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            left, right = (Path(directory) / name for name in ("source.db", "snapshot.db"))
            for database in (left, right):
                with sqlite3.connect(database) as connection:
                    connection.executescript("CREATE TABLE future_empty (id); "
                                             "CREATE TABLE metadata (id, additive_field); "
                                             "INSERT INTO metadata VALUES (1, 'preserve');")
            same_database_rows(left, right)
            with sqlite3.connect(right) as connection:
                connection.execute("UPDATE metadata SET additive_field='lost'")
            with self.assertRaises(AssertionError):
                same_database_rows(left, right)
            with sqlite3.connect(right) as connection:
                connection.execute("UPDATE metadata SET additive_field='preserve'")
                connection.execute("DROP TABLE future_empty")
            with self.assertRaises(AssertionError):
                same_database_rows(left, right)

    def test_retired_artifacts_must_be_physically_absent_but_live_aliases_survive(self):
        before, _ = self.fixture()
        before["storage_intent_paths"].append({"storage_path": "part"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part").write_bytes(b"live")
            recovered_artifacts_absent(root, before)
            (root / "scratch").write_bytes(b"not reclaimed")
            with self.assertRaises(AssertionError):
                recovered_artifacts_absent(root, before)
            (root / "scratch").unlink()
            (root / "old-blob").symlink_to("missing-target")
            with self.assertRaises(AssertionError):
                recovered_artifacts_absent(root, before)

    def test_live_file_verification_detects_same_size_corruption_and_absence(self):
        before, _ = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            snapshot, restored = (Path(directory) / name for name in ("snapshot", "restored"))
            for root in (snapshot, restored):
                root.mkdir()
                (root / "object").write_bytes(b"object")
                (root / "part").write_bytes(b"part")
            same_live_files(snapshot, restored, before)
            (restored / "part").write_bytes(b"rot!")
            with self.assertRaises(AssertionError):
                same_live_files(snapshot, restored, before)
            (restored / "part").unlink()
            with self.assertRaises(AssertionError):
                same_live_files(snapshot, restored, before)


if __name__ == "__main__":
    unittest.main()
