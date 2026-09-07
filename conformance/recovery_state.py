#!/usr/bin/env python3
"""Offline single-SQLite recovery fidelity, including real interrupted replication claims.

Stdlib-only; writes use public HTTP APIs, metadata inspection is read-only. --crash-multipart
requires a failpoints binary and additionally kills a Complete paused after durable assembly.
Every populated durable table is compared by complete rows, preserving additive schema fields.
"""
import argparse
import base64
import concurrent.futures
import datetime
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET


NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
def durable_tables(database):
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as conn:
        return [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]


def rows(database, table):
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(f'SELECT * FROM "{table}"')]


def same_rows(left, right, table):
    assert sorted(map(repr, rows(left, table))) == sorted(map(repr, rows(right, table))), table


def wait_for(probe, label, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out: {label}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-multipart", action="store_true")
    args = parser.parse_args()
    binary = str(Path(os.environ.get("BIN", "target/debug/cairn")).resolve())
    release_sink = threading.Event()

    class Stall(http.server.BaseHTTPRequestHandler):
        def do_PUT(self):
            # Wait until the source is killed, leaving a real durable claimed outbox row.
            release_sink.wait(120)
        def log_message(self, *_):
            pass

    sink = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Stall)
    sink.daemon_threads = True
    threading.Thread(target=sink.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory(prefix="cairn-recovery-state-") as directory:
        work = Path(directory)
        primary, restored, snapshot = (work / name for name in ("primary", "restored", "snapshot"))
        base_env = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_") and k != "FAILPOINTS"}
        base_env.update(CAIRN_MASTER_KEY=secrets.token_hex(32), CAIRN_WEB_ADDR="off",
                        CAIRN_META_BACKEND="sqlite", CAIRN_META_SHARDS="1",
                        CAIRN_ENCRYPT_AT_REST="true", CAIRN_LOG_LEVEL="error",
                        CAIRN_REPLICATION_ENDPOINT=f"http://127.0.0.1:{sink.server_port}",
                        CAIRN_REPLICATION_ACCESS_KEY="recovery-target", CAIRN_REPLICATION_SECRET=secrets.token_hex(32),
                        CAIRN_ALLOW_INTERNAL_ENDPOINTS="true", CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP="true",
                        CAIRN_REPLICATION_INTERVAL_SECS="1", CAIRN_REPLICATION_DELIVERY_TIMEOUT_SECS="120")
        # The OS chooses an unused port, avoiding fixed-port collisions with parallel harnesses.
        import socket
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            port = reserve.getsockname()[1]
        base_env["CAIRN_LISTEN_ADDR"] = f"127.0.0.1:{port}"

        def env(data, **extra):
            return dict(base_env, CAIRN_DATA_DIR=str(data), CAIRN_DB_PATH=str(data / "cairn.db"), **extra)

        def cli(data, *command, succeeds=True, **extra):
            result = subprocess.run([binary, *command], env=env(data, **extra), capture_output=True, text=True, timeout=60)
            assert (result.returncode == 0) == succeeds, (command, result.stdout, result.stderr)
            return result

        token = next(line.split()[-1] for line in cli(primary, "bootstrap").stdout.splitlines() if "Authorization: Bearer" in line)
        proc = None
        log = None

        def request(method, path, body=b"", headers=None, expected=200):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=25)
            try:
                connection.request(method, path, body=body, headers=dict({"Authorization": f"Bearer {token}"}, **(headers or {})))
                response = connection.getresponse()
                data = response.read()
                assert response.status == expected, (method, path, response.status, data)
                return data, dict((key.lower(), value) for key, value in response.getheaders())
            finally:
                connection.close()

        def start(data, failpoints=False):
            nonlocal proc, log
            log = open(work / "server.log", "a")
            settings = env(data)
            if failpoints:
                settings["FAILPOINTS"] = "blob_after_assemble=pause"
            # Recovery verification does not deliver outbound writes before claims are inspected.
            if data == restored:
                settings["CAIRN_REPLICATION_INTERVAL_SECS"] = "3600"
                settings.pop("CAIRN_REPLICATION_ENDPOINT")
                settings.pop("CAIRN_REPLICATION_ACCESS_KEY")
                settings.pop("CAIRN_REPLICATION_SECRET")
            proc = subprocess.Popen([binary, "serve"], env=settings, stdout=log, stderr=subprocess.STDOUT)
            def healthy():
                assert proc.poll() is None, (work / "server.log").read_text()
                try:
                    request("GET", "/healthz")
                    return True
                except OSError:
                    return False
            wait_for(healthy, "server readiness")

        def stop(crash=False):
            nonlocal proc
            if proc is not None:
                proc.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
                proc.wait(timeout=45)
                proc = None
                log.close()

        try:
            start(primary, args.crash_multipart)
            request("PUT", "/recovery", headers={"x-amz-bucket-object-lock-enabled": "true"})
            request("PUT", "/recovery?ownershipControls", b"<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>")
            request("PUT", "/recovery?replication", b'<ReplicationConfiguration><Role>unused</Role><Rule><ID>recover</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>arn:aws:s3:::recovery</Bucket></Destination></Rule></ReplicationConfiguration>', expected=204)
            future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            versions = []
            for value in (b"historical encrypted body", b"current encrypted body"):
                _, headers = request("PUT", "/recovery/history", value, {
                    "x-amz-tagging": "purpose=recovery&generation=preserved", "x-amz-acl": "public-read",
                    "x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": future,
                    "x-amz-object-lock-legal-hold": "ON"})
                versions.append((headers["x-amz-version-id"], value))
            _, marker = request("DELETE", "/recovery/history", expected=204)
            # Keep active multipart state with independent encrypted part keys and durable intent.
            upload, _ = request("POST", "/recovery/incomplete?uploads", headers={
                "x-amz-server-side-encryption": "AES256", "x-amz-tagging": "purpose=multipart-recovery",
                "x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": future,
                "x-amz-object-lock-legal-hold": "ON"})
            upload_id = ET.fromstring(upload).findtext(f"{NS}UploadId")
            assert upload_id
            query = urllib.parse.urlencode({"uploadId": upload_id})
            part_body = b"encrypted multipart recovery body"
            _, part_headers = request("PUT", f"/recovery/incomplete?{query}&partNumber=1", part_body)
            completion = ('<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>' +
                          part_headers["etag"] + '</ETag></Part></CompleteMultipartUpload>').encode()
            database = primary / "cairn.db"
            replica = None
            # Receiver-capable binaries must exercise non-NULL persisted replica intent; older
            # binaries retain baseline coverage without pretending to test the newer field.
            if "replica_intent" in rows(database, "multipart_uploads")[0]:
                replica_version = secrets.token_hex(16)
                replica_headers = {"x-amz-meta-cairn-replica": "true",
                                   "x-amz-meta-cairn-replica-version-id": replica_version,
                                   "cache-control": "max-age=123", "x-amz-tagging": "purpose=replica-recovery",
                                   "x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": future,
                                   "x-amz-object-lock-legal-hold": "ON",
                                   "x-amz-checksum-sha256": base64.b64encode(hashlib.sha256(part_body).digest()).decode()}
                response, _ = request("POST", "/recovery/replica?uploads", headers=replica_headers)
                replica_upload = ET.fromstring(response).findtext(f"{NS}UploadId")
                replica_query = urllib.parse.urlencode({"uploadId": replica_upload})
                _, replica_part = request("PUT", f"/recovery/replica?{replica_query}&partNumber=1", part_body)
                replica_completion = ('<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>' +
                                      replica_part["etag"] + '</ETag></Part></CompleteMultipartUpload>').encode()
                replica = replica_query, replica_completion, replica_version
                replica_session = next(row for row in rows(database, "multipart_uploads") if row["id"] == replica_upload)
                assert replica_session["replica_intent"] is not None
            wait_for(lambda: any(row["status"] == "claimed" for row in rows(database, "replication_outbox")), "replication claim")
            # Disconnect an admitted part body at process death, preserving non-empty staging
            # reservations/accounting that intentionally may name no authoritative part file.
            interrupted_part = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            interrupted_part.putrequest("PUT", f"/recovery/incomplete?{query}&partNumber=2")
            interrupted_part.putheader("Authorization", f"Bearer {token}")
            interrupted_part.putheader("Content-Length", "1024")
            interrupted_part.endheaders()
            interrupted_part.send(b"partial")
            wait_for(lambda: bool(rows(database, "multipart_part_reservations")), "staging reservation")
            # Offline operations must refuse both the live owner and unsupported topology.
            cli(primary, "backup", str(work / "live-refused"), succeeds=False)
            executor = None
            pending = None
            if args.crash_multipart:
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                pending = executor.submit(request, "POST", f"/recovery/incomplete?{query}", completion)
                wait_for(lambda: next(row for row in rows(database, "multipart_uploads") if row["id"] == upload_id)["status"] == "completing", "multipart claim")
                wait_for(lambda: len(list((primary / "recovery").iterdir())) == 3, "durable assembled orphan")
            stop(crash=True)
            interrupted_part.close()
            if executor:
                try:
                    pending.result(timeout=30)
                    raise AssertionError("paused Complete unexpectedly returned")
                except (OSError, http.client.HTTPException):
                    pass
                executor.shutdown()
            cli(primary, "backup", str(work / "shards-refused"), succeeds=False, CAIRN_META_SHARDS="2")
            cli(primary, "backup", str(snapshot))
            manifest = json.loads((snapshot / "manifest.json").read_text())
            assert manifest["complete"] is True
            source_db = snapshot / "metadata.sqlite3"
            for table in durable_tables(source_db):
                same_rows(database, source_db, table)
            for table in ("object_versions", "object_tags", "object_locks", "multipart_uploads", "multipart_parts", "multipart_part_reservations", "replication_outbox"):
                assert rows(source_db, table), f"vacuous coverage: {table}"
            for row in rows(source_db, "object_versions"):
                if "internal_sha256" in row and not row["is_delete_marker"]:
                    expected_body = dict(versions)[row["version_id"]]
                    assert row["internal_sha256"] == hashlib.sha256(expected_body).hexdigest()
            assert all(row["sse_descriptor"] for row in rows(source_db, "object_versions") if not row["is_delete_marker"])
            assert all(row["part_dek"] for row in rows(source_db, "multipart_parts"))
            for table in ("object_versions", "multipart_parts"):
                for row in rows(source_db, table):
                    if row.get("is_delete_marker"):
                        continue
                    blob = (snapshot / "blobs" / row["storage_path"]).read_bytes()
                    assert blob[-34:-29] == b"CRNB\x03", "snapshot must retain authenticated encrypted containers"
                    assert part_body not in blob and all(body not in blob for _, body in versions)
            claimed = [row for row in rows(source_db, "replication_outbox") if row["status"] == "claimed"]
            assert claimed
            if "claim_token" in claimed[0]:
                assert all(row["claim_token"] for row in claimed)
            if args.crash_multipart:
                session = next(row for row in rows(source_db, "multipart_uploads") if row["id"] == upload_id)
                assert session["status"] == "completing" and session["completion_claim_token"]
            # An absent committed part must reject the snapshot before publishing target metadata.
            broken_snapshot = work / "missing-part-snapshot"
            shutil.copytree(snapshot, broken_snapshot)
            part_path = rows(source_db, "multipart_parts")[0]["storage_path"]
            (broken_snapshot / "blobs" / part_path).unlink()
            refused_target = work / "refused-target"
            cli(refused_target, "restore", str(broken_snapshot), succeeds=False)
            assert not (refused_target / "cairn.db").exists()
            cli(restored, "restore", str(snapshot))
            for table in durable_tables(source_db):
                same_rows(source_db, restored / "cairn.db", table)
            # An assembled file without committed metadata is not authoritative snapshot data.
            assert len(list((restored / "recovery").iterdir())) == 2
            start(restored)
            request("GET", "/recovery/history", expected=404)
            for version, body in versions:
                suffix = urllib.parse.urlencode({"versionId": version})
                actual, headers = request("GET", f"/recovery/history?{suffix}")
                assert actual == body and headers["x-amz-version-id"] == version
                tags, _ = request("GET", f"/recovery/history?tagging&{suffix}")
                assert b"generation" in tags and b"preserved" in tags
                acl, _ = request("GET", f"/recovery/history?acl&{suffix}")
                assert b"AllUsers" in acl and b"READ" in acl, acl
                request("DELETE", f"/recovery/history?{suffix}", expected=403)
            versions_xml, _ = request("GET", "/recovery?versions")
            assert marker["x-amz-version-id"].encode() in versions_xml
            restored_db = restored / "cairn.db"
            session = next(row for row in rows(restored_db, "multipart_uploads") if row["id"] == upload_id)
            assert session["status"] == "active" and session["completion_claim_token"] is None
            recovered_outbox = rows(restored_db, "replication_outbox")
            assert all(row["status"] != "claimed" and row.get("claim_token") is None for row in recovered_outbox)
            assert {row["id"] for row in recovered_outbox} == {row["id"] for row in rows(source_db, "replication_outbox")}

            request("POST", f"/recovery/incomplete?{query}", completion)
            body, _ = request("GET", "/recovery/incomplete")
            assert body == part_body
            completed_row = next(row for row in rows(restored_db, "object_versions") if row["key"] == "incomplete")
            if "internal_sha256" in completed_row:
                assert completed_row["internal_sha256"] == hashlib.sha256(part_body).hexdigest()
            request("DELETE", "/recovery/incomplete?" + urllib.parse.urlencode({"versionId": completed_row["version_id"]}), expected=403)
            tags, _ = request("GET", "/recovery/incomplete?tagging")
            assert b"multipart-recovery" in tags
            if replica:
                replica_query, replica_completion, replica_version = replica
                _, headers = request("POST", f"/recovery/replica?{replica_query}", replica_completion)
                assert headers["x-amz-version-id"] == replica_version
                replica_body, headers = request("GET", "/recovery/replica")
                assert replica_body == part_body and headers["cache-control"] == "max-age=123"
                assert not any(row["key"] == "replica" for row in rows(restored_db, "replication_outbox"))
                print("PASS: persisted replica multipart identity and loop prevention survive restore", flush=True)
            stop()
            assert not rows(restored_db, "multipart_parts")
            assert not rows(restored_db, "multipart_part_reservations")
            assert not list((restored / ".staging" / "multipart" / upload_id).glob("*"))
            # Key material is deliberately external to the snapshot; a wrong ring must fail closed.
            wrong_key = cli(restored, "serve", succeeds=False, CAIRN_MASTER_KEY=secrets.token_hex(32))
            assert "key" in (wrong_key.stdout + wrong_key.stderr).lower(), "startup failed for an unrelated reason"
            print("PASS: offline snapshot preserves complete durable rows, encrypted history/parts, locks, ACL, tags and interrupted claims", flush=True)
        finally:
            stop(crash=True)
            release_sink.set()
            sink.shutdown()
            sink.server_close()


if __name__ == "__main__":
    main()
