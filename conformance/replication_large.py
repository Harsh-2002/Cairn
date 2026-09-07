#!/usr/bin/env python3
"""Stream actual multi-GiB objects through replication to Cairn and generic MinIO.

One deterministic MiB buffer generates multipart uploads and streaming download hashes. The
compressible fixture saves disk space, but every logical byte crosses real HTTP connections.
No assertion equates MinIO's generated version IDs or ambiguous retries with Cairn identity.

Default: 2 GiB + 17 bytes and 5 GiB + 17 bytes, compressed source, two real destinations.
Use --smoke for a 65 MiB wiring check; --skip-replication tests only fixture generation.
LARGE_ENCRYPT=true also exercises encrypted source and receiver staging (slower in debug).
LARGE_MAX_SOURCE_RSS_MIB defaults to 512; LARGE_TIMEOUT_SECS to 1800 per transfer.
LARGE_REPORT optionally saves the JSON byte/hash/version/elapsed-time/peak-RSS evidence.
This healthy-transfer harness does not claim ambiguous-success/crash cleanup coverage.
Linux /proc and enough local disk for MinIO's two complete objects are required.
"""
import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET

MIB = 1024 * 1024
GIB = 1024 * MIB
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def chunks(size, seed, offset=0):
    # Every MiB differs so an incorrect reopened range/part offset cannot pass the final hash.
    while size:
        block, within = divmod(offset, MIB)
        pattern = hashlib.sha256(seed + str(block).encode()).digest() * (MIB // 32)
        count = min(size, MIB - within)
        yield memoryview(pattern)[within:within + count]
        size -= count
        offset += count


def generated_hash(size, seed, offset=0):
    digest = hashlib.sha256()
    for chunk in chunks(size, seed, offset):
        digest.update(chunk)
    return digest.hexdigest()


class Client:
    def __init__(self, port, bearer=None, access=None, secret=None):
        self.port, self.bearer, self.access, self.secret = port, bearer, access, secret

    def begin(self, method, path, query, size, digest, headers):
        encoded = "&".join(f"{urllib.parse.quote(str(key), safe='')}={urllib.parse.quote(str(value), safe='')}"
                           for key, value in sorted(query.items()))
        headers = {key.lower(): str(value) for key, value in headers.items()}
        headers.update(host=f"127.0.0.1:{self.port}", **{"content-length": str(size)})
        if self.bearer:
            headers["authorization"] = f"Bearer {self.bearer}"
        else:
            now = datetime.datetime.now(datetime.timezone.utc)
            day, stamp = now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%SZ")
            headers.update({"x-amz-date": stamp, "x-amz-content-sha256": digest})
            signed = ";".join(sorted(headers))
            canonical = "\n".join((method, path, encoded,
                "".join(f"{key}:{headers[key]}\n" for key in sorted(headers)), signed, digest))
            scope = f"{day}/us-east-1/s3/aws4_request"
            message = f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
            signing = ("AWS4" + self.secret).encode()
            for value in (day, "us-east-1", "s3", "aws4_request"):
                signing = hmac.new(signing, value.encode(), hashlib.sha256).digest()
            signature = hmac.new(signing, message.encode(), hashlib.sha256).hexdigest()
            headers["authorization"] = f"AWS4-HMAC-SHA256 Credential={self.access}/{scope}, SignedHeaders={signed}, Signature={signature}"
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=900)
        connection.putrequest(method, path + ("?" + encoded if query else ""), skip_host=True, skip_accept_encoding=True)
        for key, value in headers.items():
            connection.putheader(key, value)
        connection.endheaders()
        return connection

    def small(self, method, path, query=None, body=b"", headers=None, expected=(200,)):
        connection = self.begin(method, path, query or {}, len(body), hashlib.sha256(body).hexdigest(), headers or {})
        try:
            if body:
                connection.send(body)
            response = connection.getresponse()
            data = response.read(1024 * 1024 + 1)
            assert len(data) <= MIB, "unexpectedly large control response"
            assert response.status in expected, (method, path, response.status, data[:1024])
            result_headers = {key.lower(): value for key, value in response.getheaders()}
            if data and response.status == 200 and method == "POST":
                assert ET.fromstring(data).tag.rsplit("}", 1)[-1] != "Error", "embedded HTTP 200 error"
            return response.status, data, result_headers
        finally:
            connection.close()

    def upload_part(self, path, query, size, seed, offset, full_hash):
        digest = generated_hash(size, seed, offset)
        connection = self.begin("PUT", path, query, size, digest, {})
        try:
            for chunk in chunks(size, seed, offset):
                full_hash.update(chunk)
                connection.send(chunk)
            response = connection.getresponse()
            data = response.read(MIB + 1)
            assert response.status == 200, ("UploadPart", response.status, data[:1024])
            return response.getheader("etag")
        finally:
            connection.close()

    def download_hash(self, path, expected_size):
        connection = self.begin("GET", path, {}, 0, hashlib.sha256(b"").hexdigest(), {})
        try:
            response = connection.getresponse()
            assert response.status == 200, ("GetObject", response.status)
            assert int(response.getheader("content-length")) == expected_size
            digest, count = hashlib.sha256(), 0
            while chunk := response.read(MIB):
                digest.update(chunk)
                count += len(chunk)
            assert count == expected_size
            return digest.hexdigest()
        finally:
            connection.close()


def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_for(probe, label, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if probe():
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out: {label}")


class Resources:
    def __init__(self, processes, source_limit):
        self.processes = processes
        self.source_limit = source_limit
        self.peak = dict.fromkeys(processes, 0)
        self.samples = dict.fromkeys(processes, 0)
        self.failures = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop.is_set():
            for name, process in self.processes.items():
                try:
                    lines = Path(f"/proc/{process.pid}/status").read_text().splitlines()
                    rss = int(next(line.split()[1] for line in lines if line.startswith("VmRSS:"))) * 1024
                    self.samples[name] += 1
                    self.peak[name] = max(self.peak[name], rss)
                    if name == "source" and rss > self.source_limit:
                        self.failures.append("source RSS exceeded bounded-memory ceiling")
                        process.kill()  # Prevent the old whole-object buffering path from exhausting the host.
                        return
                except (OSError, StopIteration):
                    self.failures.append(f"could not sample {name}")
                    return
            self.stop.wait(0.1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="65 MiB wiring test; never claims large-object acceptance")
    parser.add_argument("--skip-replication", action="store_true", help="fixture self-test only; not an acceptance run")
    args = parser.parse_args()
    sizes = [65 * MIB + 17] if args.smoke else [2 * GIB + 17, 5 * GIB + 17]
    binary = str(Path(os.environ.get("BIN", "target/debug/cairn")).resolve())
    minio = str(Path(os.environ["MINIO"]).resolve())
    deadline = int(os.environ.get("LARGE_TIMEOUT_SECS", "1800"))
    source_limit = int(os.environ.get("LARGE_MAX_SOURCE_RSS_MIB", "512")) * MIB
    assert deadline > 0 and source_limit > 0
    encrypt = os.environ.get("LARGE_ENCRYPT", "false")
    assert encrypt in ("true", "false"), "LARGE_ENCRYPT must be true or false"
    base_env = {key: value for key, value in os.environ.items() if not key.startswith("CAIRN_") and key != "FAILPOINTS"}
    report = {"mode": "fixture-only" if args.skip_replication else "smoke" if args.smoke else "large", "encrypted": encrypt == "true", "objects": []}
    processes, logs = {}, []
    resources = None
    with tempfile.TemporaryDirectory(prefix="cairn-large-", dir=os.environ.get("LARGE_WORK_ROOT")) as directory:
        work = Path(directory)
        ports = {name: unused_port() for name in ("source", "cairn", "minio", "console", "source-ui", "cairn-ui")}
        try:
            clients, settings = {}, {}
            for name in ("source", "cairn"):
                data = work / name
                setting = dict(base_env, CAIRN_DATA_DIR=str(data), CAIRN_DB_PATH=str(data / "cairn.db"),
                    CAIRN_MASTER_KEY=secrets.token_hex(32), CAIRN_LISTEN_ADDR=f"127.0.0.1:{ports[name]}",
                    CAIRN_WEB_ADDR=f"127.0.0.1:{ports[name + '-ui']}", CAIRN_LOG_LEVEL="error", CAIRN_ENCRYPT_AT_REST=encrypt,
                    CAIRN_REQUEST_TIMEOUT_SECS=str(deadline), CAIRN_ALLOW_INTERNAL_ENDPOINTS="true",
                    CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP="true",
                    CAIRN_REPLICATION_DELIVERY_TIMEOUT_SECS=str(deadline), CAIRN_REPLICATION_BUFFER_BUDGET_BYTES=str(128 * MIB),
                    CAIRN_REPLICATION_INTERVAL_SECS="1", CAIRN_REPLICATION_WORKER_CONCURRENCY="2",
                    CAIRN_REPLICATION_BATCH_SIZE="1")
                result = subprocess.run([binary, "bootstrap"], env=setting, capture_output=True, text=True, timeout=60)
                assert result.returncode == 0, "bootstrap failed (credential output suppressed)"
                bearer = next(line.split()[-1] for line in result.stdout.splitlines() if "Authorization: Bearer" in line)
                access = next(line.split()[-1] for line in result.stdout.splitlines() if "Access Key Id" in line)
                secret = next(line.split()[-1] for line in result.stdout.splitlines() if "Secret Access Key" in line)
                clients[name] = Client(ports[name], bearer, access, secret)
                clients[name + "-ui"] = Client(ports[name + "-ui"], bearer)
                settings[name] = setting
            minio_access, minio_secret = "large-fixture", secrets.token_hex(32)
            clients["minio"] = Client(ports["minio"], access=minio_access, secret=minio_secret)
            targets = [{"name": "cairn-copy", "endpoint": f"http://127.0.0.1:{ports['cairn']}", "region": "us-east-1",
                        "dest_bucket": "cairn-copy", "access_key": clients["cairn"].access, "secret": clients["cairn"].secret},
                       {"name": "minio-copy", "endpoint": f"http://127.0.0.1:{ports['minio']}", "region": "us-east-1",
                        "dest_bucket": "minio-copy", "access_key": minio_access, "secret": minio_secret}]
            for name in ("source", "cairn", "minio"):
                log = open(work / f"{name}.log", "w")
                logs.append(log)
                command = [binary, "serve"]
                setting = settings.get(name)
                if name == "minio":
                    command = [minio, "server", str(work / "minio"), "--address", f"127.0.0.1:{ports[name]}",
                               "--console-address", f"127.0.0.1:{ports['console']}"]
                    setting = dict(base_env, MINIO_ROOT_USER=minio_access, MINIO_ROOT_PASSWORD=minio_secret, MINIO_BROWSER="off")
                processes[name] = subprocess.Popen(command, env=setting, stdout=log, stderr=subprocess.STDOUT)
                def healthy(name=name):
                    assert processes[name].poll() is None, f"{name} exited before readiness"
                    try:
                        clients[name].small("GET", "/minio/health/live" if name == "minio" else "/healthz")
                        return True
                    except OSError:
                        return False
                wait_for(healthy, f"{name} readiness", 60)
            resources = Resources(processes, source_limit)
            resources.thread.start()
            for name, bucket in (("source", "large-source"), ("cairn", "cairn-copy"), ("minio", "minio-copy")):
                clients[name].small("PUT", f"/{bucket}")
                clients[name].small("PUT", f"/{bucket}", {"versioning": ""},
                    b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>", expected=(200, 204))
            for name, bucket in (("source", "large-source"), ("cairn", "cairn-copy")):
                clients[name + "-ui"].small("PUT", f"/api/v1/buckets/{bucket}/compression",
                    body=b'{"algorithm":"zstd","block_size":262144}', headers={"content-type": "application/json"}, expected=(200, 204))
            if not args.skip_replication:
                rules = []
                for target in targets:
                    payload = {key: value for key, value in target.items() if key != "name"}
                    _, created, _ = clients["source-ui"].small("POST", "/api/v1/buckets/large-source/replication/targets",
                        body=json.dumps(payload).encode(), headers={"content-type": "application/json"}, expected=(201,))
                    arn = json.loads(created)["arn"]
                    rules.append(f'<Rule><ID>{target["name"]}</ID><Status>Enabled</Status><Prefix></Prefix>'
                                 f'<Destination><Bucket>{arn}</Bucket></Destination></Rule>')
                clients["source"].small("PUT", "/large-source", {"replication": ""},
                    f"<ReplicationConfiguration><Role>fixture</Role>{''.join(rules)}</ReplicationConfiguration>".encode(), expected=(204,))
            for size in sizes:
                key, start = f"bytes-{size}", time.monotonic()
                seed = key.encode()
                source, path = clients["source"], f"/large-source/{key}"
                _, data, _ = source.small("POST", path, {"uploads": ""}, headers={"content-type": "application/octet-stream",
                                                                                       "x-amz-tagging": "purpose=large-replication"})
                upload = ET.fromstring(data).findtext(f"{NS}UploadId")
                assert upload
                full_hash, parts, remaining = hashlib.sha256(), [], size
                while remaining:
                    count = min(64 * MIB, remaining)
                    part = len(parts) + 1
                    etag = source.upload_part(path, {"uploadId": upload, "partNumber": part}, count, seed, size - remaining, full_hash)
                    assert etag
                    parts.append(f"<Part><PartNumber>{part}</PartNumber><ETag>{etag}</ETag></Part>")
                    remaining -= count
                _, _, headers = source.small("POST", path, {"uploadId": upload},
                    ("<CompleteMultipartUpload>" + ''.join(parts) + "</CompleteMultipartUpload>").encode())
                source_version = headers["x-amz-version-id"]
                upload_elapsed = time.monotonic() - start
                with sqlite3.connect(f"file:{work / 'source' / 'cairn.db'}?mode=ro", uri=True) as conn:
                    physical = conn.execute("SELECT size_physical FROM object_versions WHERE key=?", (key,)).fetchone()[0]
                assert physical < size // 10, "compressible fixture must actually use the compressed logical-range path"
                expected_hash = full_hash.hexdigest()
                assert source.download_hash(path, size) == expected_hash
                result = {"size_bytes": size, "source_version": source_version, "sha256": expected_hash,
                          "upload_seconds": upload_elapsed, "source_physical_bytes": physical, "destinations": {}}
                if not args.skip_replication:
                    def replicated():
                        assert not resources.failures, resources.failures
                        with sqlite3.connect(f"file:{work / 'source' / 'cairn.db'}?mode=ro", uri=True) as conn:
                            states = [row[0] for row in conn.execute("SELECT status FROM replication_outbox WHERE key=?", (key,))]
                        assert len(states) == 2, f"both targets must have durable outbox work: {states}"
                        assert "failed" not in states, f"terminal replication failure: {states}"
                        return states == ["completed", "completed"]
                    wait_for(replicated, f"both replicas of {size} bytes", deadline)
                    with sqlite3.connect(f"file:{work / 'source' / 'cairn.db'}?mode=ro", uri=True) as conn:
                        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='replication_uploads'").fetchone():
                            assert conn.execute("SELECT count(*) FROM replication_uploads").fetchone()[0] == 0, "healthy transfer left journal debt"
                    for name, bucket in (("cairn", "cairn-copy"), ("minio", "minio-copy")):
                        client, verify_start = clients[name], time.monotonic()
                        assert client.download_hash(f"/{bucket}/{key}", size) == expected_hash, name
                        _, listing, _ = client.small("GET", f"/{bucket}", {"versions": "", "prefix": key})
                        versions = [element.findtext(f"{NS}VersionId") for element in ET.fromstring(listing).findall(f"{NS}Version")
                                    if element.findtext(f"{NS}Key") == key]
                        assert versions, name
                        if name == "cairn":
                            assert versions == [source_version], "Cairn must preserve exact source identity"
                        # Generic S3 is at-least-once: another version after ambiguous success is valid.
                        result["destinations"][name] = {"version_count": len(versions), "verify_seconds": time.monotonic() - verify_start}
                        _, uploads, _ = client.small("GET", f"/{bucket}", {"uploads": ""})
                        assert not ET.fromstring(uploads).findall(f"{NS}Upload"), f"{name} leaked a remote multipart upload"
                result["total_seconds"] = time.monotonic() - start
                report["objects"].append(result)
                print(f"PASS: {size} actual bytes; sha256={expected_hash}; mode={report['mode']}", flush=True)
            assert not resources.failures, resources.failures
            assert all(resources.samples.values()), "resource sampling must not pass vacuously"
            report["peak_rss_bytes"] = resources.peak
            report["rss_samples"] = resources.samples
            print(json.dumps(report, sort_keys=True), flush=True)
            if output := os.environ.get("LARGE_REPORT"):
                Path(output).write_text(json.dumps(report, indent=2) + "\n")
        finally:
            if resources:
                resources.stop.set()
                resources.thread.join(timeout=5)
            for process in processes.values():
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            for process in processes.values():
                try:
                    process.wait(timeout=35)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            for log in logs:
                log.close()


if __name__ == "__main__":
    main()
