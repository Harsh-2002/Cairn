#!/usr/bin/env python3
"""Real-HTTP remote multipart journal recovery; requires schema v33 and streaming sender.

A forwarding proxy holds a successful destination response until the source is killed. No
production failpoints or SQL mutations are used. Snapshot rows are compared exactly; restore must
preserve live rows and validate fresh storage ownership.
Run BIN=/path/to/cairn python3 conformance/recovery_remote.py. --cleanup-lease additionally
crashes an owned abort and proves startup releases its abandoned cleanup claim.
"""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import tempfile
import threading
import urllib.parse
import xml.etree.ElementTree as ET

from recovery_state import (database_rows, durable_tables, fresh_storage_generation,
                            recovered_artifacts_absent, recovered_database_rows, rows,
                            same_database_rows, same_live_files)
from replication_large import Client, MIB, NS, chunks, unused_port, wait_for


class FaultProxy(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, peer_port, stage):
        super().__init__(("127.0.0.1", 0), Forward)
        self.peer_port, self.stage = peer_port, stage
        self.held, self.release = threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.errors = []
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def arm_abort(self):
        self.held.clear()
        self.release.clear()
        self.stage = "abort"


class Forward(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def forward(self):
        peer = http.client.HTTPConnection("127.0.0.1", self.server.peer_port, timeout=400)
        try:
            # Preserve the signed Host header while changing only the TCP destination.
            assert "transfer-encoding" not in self.headers, "fixture expects length-delimited sender requests"
            peer.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for key, value in self.headers.items():
                peer.putheader(key, value)
            peer.endheaders()
            remaining = int(self.headers.get("content-length", "0"))
            while remaining:
                body = self.rfile.read(min(remaining, MIB))
                assert body, "truncated request body"
                peer.send(body)
                remaining -= len(body)
            response = peer.getresponse()
            body = response.read(MIB + 1)
            assert len(body) <= MIB, "unexpectedly large control response"
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)
            initiation = self.command == "POST" and "uploads" in query
            with self.server.lock:
                hold = ((self.server.stage == "initiation" and initiation)
                        or (self.server.stage == "part" and self.command == "PUT" and query.get("partNumber") == ["1"])
                        or (self.server.stage == "abort" and self.command == "DELETE" and "uploadId" in query))
                if hold:
                    assert 200 <= response.status < 300, "fault must follow a successful peer mutation"
                    self.server.stage = None
                    self.server.held.set()
            if hold:
                self.server.release.wait(400)
            # This fixture needs only protocol result headers, never arbitrary peer header names.
            # Validate before writing the status line; the explicit normalization also keeps the
            # bytes sent to http.server free of line breaks without silently accepting a mutation.
            headers = []
            for name in ("etag", "content-type", "x-amz-version-id"):
                value = response.getheader(name)
                if value is not None:
                    clean = value.replace("\r", "").replace("\n", "")
                    if clean != value:
                        raise ValueError("peer response header contains a line break")
                    headers.append((name, clean))
            length = int(response.getheader("content-length", "0")) if self.command == "HEAD" else len(body)
            self.send_response(response.status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("content-length", str(length))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # The deliberately killed source cannot receive the held response.
        except Exception as error:
            self.server.errors.append(type(error).__name__)
        finally:
            peer.close()

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = forward


def scenario(binary, work, stage, cleanup_lease):
    source, restored, peer_data = (work / name for name in ("source", "restored", "peer"))
    source_port, peer_port, ui_port = unused_port(), unused_port(), unused_port()
    proxy = FaultProxy(peer_port, stage)
    base = {key: value for key, value in os.environ.items() if not key.startswith("CAIRN_") and key != "FAILPOINTS"}
    base.update(CAIRN_MASTER_KEY=secrets.token_hex(32), CAIRN_META_SHARDS="1", CAIRN_META_BACKEND="sqlite",
                CAIRN_ENCRYPT_AT_REST="false", CAIRN_LOG_LEVEL="error", CAIRN_ALLOW_INTERNAL_ENDPOINTS="true",
                CAIRN_REPLICATION_INTERVAL_SECS="1", CAIRN_REPLICATION_BATCH_SIZE="1",
                CAIRN_REPLICATION_WORKER_CONCURRENCY="1", CAIRN_REPLICATION_DELIVERY_TIMEOUT_SECS="600",
                CAIRN_REPLICATION_BUFFER_BUDGET_BYTES=str(128 * MIB), CAIRN_REQUEST_TIMEOUT_SECS="600")
    processes, logs = {}, []

    def environment(data, peer=False):
        settings = dict(base, CAIRN_DATA_DIR=str(data), CAIRN_DB_PATH=str(data / "cairn.db"),
                        CAIRN_LISTEN_ADDR=f"127.0.0.1:{peer_port if peer else source_port}",
                        CAIRN_WEB_ADDR="off" if peer else f"127.0.0.1:{ui_port}")
        return settings

    def cli(data, *args, peer=False):
        result = subprocess.run([binary, *args], env=environment(data, peer), capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, (args, "CLI failed; credential output suppressed")
        return result.stdout

    def bootstrap(data, peer=False):
        output = cli(data, "bootstrap", peer=peer)
        def field(label):
            return next(line.split()[-1] for line in output.splitlines() if label in line)
        return Client(peer_port if peer else source_port, field("Authorization: Bearer"), field("Access Key Id"), field("Secret Access Key"))

    def stop(name, crash=False):
        process = processes.pop(name, None)
        if process and process.poll() is None:
            process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            process.wait(timeout=35)

    def start(name, data, client, peer=False):
        log = open(work / f"{name}.log", "a")
        logs.append(log)
        processes[name] = subprocess.Popen([binary, "serve"], env=environment(data, peer), stdout=log, stderr=subprocess.STDOUT)
        def ready():
            assert processes[name].poll() is None, f"{name} exited during startup"
            try:
                client.small("GET", "/healthz")
                return True
            except OSError:
                return False
        wait_for(ready, f"{name} readiness", 60)

    def wait_held(label):
        def held():
            assert not proxy.errors, proxy.errors
            return proxy.held.is_set()
        wait_for(held, label, 180)

    try:
        client, destination = bootstrap(source), bootstrap(peer_data, peer=True)
        ui = Client(ui_port, client.bearer)
        start("peer", peer_data, destination, peer=True)
        start("source", source, client)
        database = source / "cairn.db"
        assert "replication_uploads" in durable_tables(database), "schema v33 streaming binary required"
        for api in (client, destination):
            api.small("PUT", "/journal")
            api.small("PUT", "/journal", {"versioning": ""},
                      b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>", expected=(200, 204))
        ui.small("PUT", "/api/v1/buckets/journal/compression", body=b'{"algorithm":"zstd","block_size":262144}',
                 headers={"content-type": "application/json"}, expected=(200, 204))
        payload = {"endpoint": f"http://127.0.0.1:{proxy.server_port}", "region": "us-east-1", "dest_bucket": "journal",
                   "access_key": destination.access, "secret": destination.secret}
        _, created, _ = ui.small("POST", "/api/v1/buckets/journal/replication/targets", body=json.dumps(payload).encode(),
                               headers={"content-type": "application/json"}, expected=(201,))
        arn = json.loads(created)["arn"]
        client.small("PUT", "/journal", {"replication": ""},
                     f"<ReplicationConfiguration><Role>fixture</Role><Rule><ID>journal</ID><Status>Enabled</Status><Prefix></Prefix><Destination><Bucket>{arn}</Bucket></Destination></Rule></ReplicationConfiguration>".encode(), expected=(204,))
        size, digest = 65 * MIB + 17, hashlib.sha256()
        connection = client.begin("PUT", "/journal/object", {}, size, "UNSIGNED-PAYLOAD", {})
        try:
            for body in chunks(size, b"remote-journal"):
                digest.update(body)
                connection.send(body)
            response = connection.getresponse()
            assert response.status == 200, "source write failed"
            response.read(MIB)
            version = response.getheader("x-amz-version-id")
        finally:
            connection.close()
        wait_held(f"successful peer {stage} response held")
        assert not proxy.errors, proxy.errors
        journal = rows(database, "replication_uploads")
        assert len(journal) == 1 and journal[0]["origin_token"]
        owner = next(row for row in rows(database, "replication_outbox") if row["id"] == journal[0]["outbox_id"])
        assert owner["status"] == "claimed" and owner["claim_token"] == journal[0]["origin_token"], "journal must bind the exact live delivery attempt"
        # The response stays opaque in the proxy. Read the peer's authoritative row instead of
        # adding an XML parser solely to learn the upload ID for this fixture's assertions.
        peer_uploads = rows(peer_data / "cairn.db", "multipart_uploads")
        assert len(peer_uploads) == 1, "peer must durably own exactly one upload"
        attempt, remote_id = journal[0]["id"], peer_uploads[0]["id"]
        assert remote_id, "peer must actually create an upload"
        assert (journal[0]["upload_id"] is None) == (stage == "initiation")
        if stage == "part":
            assert journal[0]["upload_id"] == remote_id
        stop("source", crash=True)
        proxy.release.set()
        if cleanup_lease:
            # The first recovery claims DELETE. Its successful response is then lost at a second
            # crash, preserving an owned cleanup lease even though the peer upload is already gone.
            proxy.arm_abort()
            start("source", source, client)
            wait_held("successful abort response held")
            claimed = next(row for row in rows(database, "replication_uploads") if row["id"] == attempt)
            assert claimed["cleanup_token"] and claimed["lease_until"]
            stop("source", crash=True)
            proxy.release.set()
        snapshot = work / "snapshot"
        cli(source, "backup", str(snapshot))
        snapshot_db = snapshot / "metadata.sqlite3"
        same_database_rows(database, snapshot_db)
        cli(restored, "restore", str(snapshot))
        snapshot_state = database_rows(snapshot_db)
        restored_db = restored / "cairn.db"
        restored_state = database_rows(restored_db)
        recovered_database_rows(snapshot_state, restored_state)
        same_live_files(snapshot / "blobs", restored, snapshot_state)
        recovered_artifacts_absent(restored, snapshot_state)
        start("source", restored, client)
        fresh_storage_generation(restored_state["storage_recovery_state"],
                                 rows(restored_db, "storage_recovery_state"))
        if cleanup_lease:
            row = next((row for row in rows(restored_db, "replication_uploads") if row["id"] == attempt), None)
            assert row is None or row["cleanup_token"] != claimed["cleanup_token"], "startup must release the abandoned cleanup claim"
        if stage == "initiation":
            wait_for(lambda: next(row for row in rows(restored_db, "replication_uploads") if row["id"] == attempt)["orphan_reported"] == 1,
                     "durable unknown-ID orphan incident", 90)
        else:
            wait_for(lambda: all(row["id"] != attempt for row in rows(restored_db, "replication_uploads")),
                     "known upload cleanup after restore", 180)
        assert rows(restored_db, "replication_outbox"), "restored outbox must not be empty"
        wait_for(lambda: all(row["status"] == "completed" for row in rows(restored_db, "replication_outbox")),
                 "restored delivery completes", 180)
        assert destination.download_hash("/journal/object", size) == digest.hexdigest()
        _, listing, _ = destination.small("GET", "/journal", {"versions": ""})
        assert [element.findtext(f"{NS}VersionId") for element in ET.fromstring(listing).findall(f"{NS}Version")] == [version]
        _, uploads, _ = destination.small("GET", "/journal", {"uploads": ""})
        ids = [element.findtext(f"{NS}UploadId") for element in ET.fromstring(uploads).findall(f"{NS}Upload")]
        if stage == "initiation":
            assert ids == [remote_id], "unknown receipt must remain visible for destination lifecycle cleanup"
            retained = next(row for row in rows(restored_db, "replication_uploads") if row["id"] == attempt)
            assert retained["upload_id"] is None and retained["orphan_reported"] == 1 and retained["last_error"]
            destination.small("DELETE", "/journal/object", {"uploadId": remote_id}, expected=(204,))
        else:
            assert not ids, "known remote upload must not leak"
        assert not proxy.errors, proxy.errors
        print(f"PASS: {stage} response loss; exact snapshot and validated restore transitions; " +
              ("abandoned cleanup claim released safely" if cleanup_lease else "unknown ID retained for lifecycle" if stage == "initiation" else "known upload reclaimed"), flush=True)
    finally:
        proxy.release.set()
        stop("source", crash=True)
        stop("peer", crash=True)
        proxy.shutdown()
        proxy.server_close()
        for log in logs:
            log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleanup-lease", action="store_true")
    args = parser.parse_args()
    binary = str(Path(os.environ.get("BIN", "target/debug/cairn")).resolve())
    with tempfile.TemporaryDirectory(prefix="cairn-remote-recovery-", dir=os.environ.get("LARGE_WORK_ROOT")) as directory:
        for stage, owned in [("initiation", False), ("part", False)] + ([("part", True)] if args.cleanup_lease else []):
            work = Path(directory) / f"{stage}-{owned}"
            work.mkdir()
            scenario(binary, work, stage, owned)


if __name__ == "__main__":
    main()
