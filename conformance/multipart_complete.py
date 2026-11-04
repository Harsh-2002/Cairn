#!/usr/bin/env python3
"""Completion-inclusive multipart upload comparison for Cairn and RustFS on one host.

The Warp scoreboard's `multipart_put_5m` case reports `PUTPART` throughput: its
number counts 5 MiB part operations, so the completed-object rate is one tenth of
it, and the summary has no line for the per-upload completion. This driver measures
the client-visible completed-object rate instead: for each object it calls
`CreateMultipartUpload`, uploads exactly ten 5 MiB parts with bounded concurrency,
calls `CompleteMultipartUpload`, and only then starts the next object on that
worker. Nothing is pipelined across objects beyond the fixed object-worker count,
so `objects / wall` is a real completed-object rate.

Every arm starts a fresh strict single-node store through the same helpers as
`rustfs_compare.py`. The scored window is separate from a single-object warm-up
and from the read-back verification: each completed object is fetched in full and
one rotating `?partNumber` is fetched, and both responses must hash to the exact
expected payload. The composite ETag must equal the locally derived
MD5-of-part-MD5s value, and a bounded listing must show exactly the completed key
set.

This is a diagnostic macro benchmark, not a durability-equivalence proof; the
strict settings are those `rustfs_compare.py` already pins. It never replaces or
pools with the Warp `PUTPART` row.
"""

import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import xml.etree.ElementTree as ElementTree
from pathlib import Path

import delete_single_key as driver
import rustfs_compare as common


BUCKET = common.BUCKET
OBJECT_PARTS = 10
PART_BYTES = 5 * 1024 * 1024
OBJECT_BYTES = OBJECT_PARTS * PART_BYTES
OBJECT_WORKERS = 4
PART_CONCURRENCY = 4
ERROR_DETAIL_LIMIT = 20
PREFIX = "complete-"
WARMUP_KEY = "warmup-object"
READ_CHUNK = 256 * 1024
MIB = 1024 * 1024
MULTIPART_STAGE_PREFIX = "cairn_blob_multipart_stage_seconds"
MULTIPART_STAGES = ("permit_wait", "assembly", "durability")


class MultipartError(RuntimeError):
    """An S3 response or payload broke the multipart contract under test."""


def part_payload(index):
    """One deterministic 5 MiB buffer per part position, built once per arm.

    Positions differ so a server that reorders or duplicates parts fails the
    byte checks; the same ten buffers are reused for every object.
    """
    header = f"cairn-complete-part-{index:02d}-".encode()
    return (header * (PART_BYTES // len(header) + 1))[:PART_BYTES]


def object_key(index):
    return f"{PREFIX}{index:06d}"


def composite_etag(part_md5_hexes):
    """S3 multipart ETag: MD5 of the concatenated per-part MD5 digests, then `-N`."""
    joined = bytes.fromhex("".join(part_md5_hexes))
    return f"{hashlib.md5(joined).hexdigest()}-{len(part_md5_hexes)}"


def complete_body(part_etags):
    """Build the exact `CompleteMultipartUpload` XML for ordered part ETags."""
    parts = "".join(
        "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>".format(
            number,
            str(etag).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
        for number, etag in enumerate(part_etags, 1))
    return ("<CompleteMultipartUpload>" + parts + "</CompleteMultipartUpload>").encode()


def response_text(payload, tag):
    """Read one direct child of an S3 XML response; fail closed on malformed XML."""
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as error:
        raise MultipartError(f"response is not XML: {error}") from error
    for child in root:
        # S3 responses are unnamespaced in practice; tolerate a default namespace.
        if child.tag.rsplit("}", 1)[-1] == tag and child.text is not None:
            return child.text
    raise MultipartError(f"response lacks <{tag}>")


def parse_upload_id(payload):
    upload_id = response_text(payload, "UploadId")
    if not upload_id:
        raise MultipartError("empty <UploadId>")
    return upload_id


def parse_complete_etag(payload):
    """Unquote the complete result ETag; `&quot;` is decoded by the XML parser."""
    etag = response_text(payload, "ETag").strip()
    if len(etag) < 2 or not etag.startswith('"') or not etag.endswith('"'):
        raise MultipartError(f"completion ETag is not quoted: {etag!r}")
    return etag[1:-1]


def count_listed_keys(payload):
    """Return (key count, truncated) from one ListObjectsV2 page."""
    return len(re.findall(rb"<Key>", payload)), b"<IsTruncated>true</IsTruncated>" in payload


def completion_share(cycles, completions):
    """Fraction of summed object time spent inside `CompleteMultipartUpload`."""
    if (not cycles or len(cycles) != len(completions)
            or any(value < 0 for value in cycles + completions)):
        return None
    total = sum(cycles)
    return sum(completions) / total if total > 0 else None


def load_average():
    return [float(value) for value in Path("/proc/loadavg").read_text().split()[:3]]


class Client:
    """Minimal SigV4 S3 client; one reused connection per calling thread."""

    def __init__(self, port, secret, timeout=60):
        self.port = port
        self.secret = secret
        self.timeout = timeout
        self._local = threading.local()

    def _connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
            self._local.connection = connection
        return connection

    def _drop(self):
        connection = getattr(self._local, "connection", None)
        self._local.connection = None
        if connection is not None:
            connection.close()

    def _target(self, connection, method, path, query, body, digest):
        headers, canonical_query = driver.signed_headers(
            connection, self.secret, method, path, query, body, digest)
        return headers, path + ("?" + canonical_query if query else "")

    def request(self, method, path, query=(), body=b"", digest=None, extra_headers=None):
        connection = self._connection()
        try:
            headers, target = self._target(connection, method, path, query, body, digest)
            if extra_headers:
                headers.update(extra_headers)
            connection.request(method, target, body=body, headers=headers)
            response = connection.getresponse()
            payload = response.read()
            return (response.status,
                    {name.lower(): value for name, value in response.getheaders()},
                    payload)
        except (OSError, http.client.HTTPException):
            self._drop()
            raise

    def digest_get(self, path, query=()):
        """GET and hash a body without buffering it: (status, sha256 hex, bytes)."""
        connection = self._connection()
        try:
            headers, target = self._target(connection, "GET", path, query, b"", None)
            connection.request("GET", target, body=b"", headers=headers)
            response = connection.getresponse()
            hasher = hashlib.sha256()
            total = 0
            while True:
                chunk = response.read(READ_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
                total += len(chunk)
            return response.status, hasher.hexdigest(), total
        except (OSError, http.client.HTTPException):
            self._drop()
            raise


def initiate(client, key):
    status, _, payload = client.request(
        "POST", f"/{BUCKET}/{key}", query=(("uploads", ""),))
    if status != 200:
        raise MultipartError(
            f"initiate {key} returned HTTP {status}: {payload[:200].decode(errors='replace')}")
    return parse_upload_id(payload)


def upload_object(part_pool, client, key, index, payloads, digests):
    """Run one complete object cycle and return its timing record."""
    cycle_start = time.monotonic()
    upload_id = initiate(client, key)
    etags = [None] * len(payloads)

    def upload_part(position):
        started = time.monotonic()
        status, headers, payload = client.request(
            "PUT", f"/{BUCKET}/{key}",
            query=(("partNumber", str(position + 1)), ("uploadId", upload_id)),
            body=payloads[position], digest=digests[position])
        elapsed = time.monotonic() - started
        if status != 200:
            raise MultipartError(
                f"part {position + 1} of {key} returned HTTP {status}: "
                f"{payload[:200].decode(errors='replace')}")
        etag = headers.get("etag")
        if not etag:
            raise MultipartError(f"part {position + 1} of {key} returned no ETag")
        return position, etag, elapsed

    futures = [part_pool.submit(upload_part, position)
               for position in range(len(payloads))]
    part_latencies = []
    for position, etag, elapsed in (future.result() for future in futures):
        etags[position] = etag
        part_latencies.append(elapsed)

    completion_start = time.monotonic()
    status, _, payload = client.request(
        "POST", f"/{BUCKET}/{key}", query=(("uploadId", upload_id),),
        body=complete_body(etags), extra_headers={"content-type": "application/xml"})
    completion_seconds = time.monotonic() - completion_start
    if status != 200:
        raise MultipartError(
            f"complete {key} returned HTTP {status}: {payload[:200].decode(errors='replace')}")
    return {
        "index": index,
        "key": key,
        "etag": parse_complete_etag(payload),
        "cycle_seconds": time.monotonic() - cycle_start,
        "completion_seconds": completion_seconds,
        "part_latencies": part_latencies,
    }


def warm_up(port, secret, payloads, digests, deadline):
    """One unscored object warms code paths and connections on both engines."""
    client = Client(port, secret)
    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=PART_CONCURRENCY) as part_pool:
            upload_object(part_pool, client, WARMUP_KEY, 0, payloads, digests)
        if time.monotonic() >= deadline:
            raise MultipartError("warm-up exceeded the arm deadline")
    finally:
        client._drop()
    return time.monotonic() - started


def scored_objects(port, secret, objects, payloads, digests, deadline):
    """Upload `objects` complete objects with a fixed worker/part-concurrency shape."""
    errors = []
    aborted = threading.Event()
    lock = threading.Lock()

    def worker(worker_index):
        records = []
        client = Client(port, secret)
        try:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=PART_CONCURRENCY) as part_pool:
                for index in range(worker_index, objects, OBJECT_WORKERS):
                    if aborted.is_set():
                        break
                    if time.monotonic() >= deadline:
                        with lock:
                            errors.append({"key": object_key(index),
                                           "error": "arm deadline exceeded"})
                        aborted.set()
                        break
                    try:
                        records.append(upload_object(
                            part_pool, client, object_key(index), index, payloads, digests))
                    except (MultipartError, OSError, http.client.HTTPException) as error:
                        with lock:
                            if len(errors) < ERROR_DETAIL_LIMIT:
                                errors.append({"key": object_key(index),
                                               "error": str(error)})
                            else:
                                errors.append({"key": object_key(index),
                                               "error": type(error).__name__})
                        aborted.set()
                        break
        finally:
            client._drop()
        return records

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=OBJECT_WORKERS) as pool:
        groups = list(pool.map(worker, range(OBJECT_WORKERS)))
    wall_seconds = time.monotonic() - started
    records = [record for group in groups for record in group]
    return records, errors, wall_seconds


def verify_objects(client, records, part_digests, expected_digest, expected_etag,
                   part_count):
    """Fetch every completed object in full and one rotating part; assert bytes."""
    checks = []
    failures = []
    part_coverage = {number: 0 for number in range(1, part_count + 1)}
    for record in records:
        part_number = record["index"] % part_count + 1
        part_coverage[part_number] += 1
        full_status, full_digest, full_bytes = client.digest_get(
            f"/{BUCKET}/{record['key']}")
        part_status, part_digest, part_bytes = client.digest_get(
            f"/{BUCKET}/{record['key']}", query=(("partNumber", str(part_number)),))
        check = {
            "key": record["key"],
            "full_status": full_status,
            "full_bytes": full_bytes,
            "full_sha256_ok": full_status == 200 and full_digest == expected_digest,
            "part_number": part_number,
            "part_status": part_status,
            "part_bytes": part_bytes,
            "part_sha256_ok": (
                part_status in (200, 206)
                and part_digest == part_digests[part_number - 1]),
            "etag_ok": record["etag"] == expected_etag,
        }
        check["ok"] = (check["full_sha256_ok"] and check["part_sha256_ok"]
                       and check["etag_ok"] and full_bytes == OBJECT_BYTES
                       and part_bytes == PART_BYTES)
        checks.append(check)
        if not check["ok"]:
            failures.append(check)
    return checks, failures, part_coverage


def verify_listing(client, expected_keys):
    """One bounded listing must show exactly the completed key set."""
    status, _, payload = client.request(
        "GET", f"/{BUCKET}",
        query=(("list-type", "2"), ("prefix", PREFIX), ("max-keys", "1000")))
    count, truncated = count_listed_keys(payload)
    return {"status": status, "keys": count, "truncated": truncated,
            "exact": status == 200 and count == expected_keys and not truncated}


def scrape_multipart_stages(port):
    """Bounded read of Cairn's fixed multipart stage histograms (no labels beyond stage)."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        text = response.read().decode(errors="replace")
        status = response.status
    finally:
        connection.close()
    series = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name, _, value = line.rpartition(" ")
        if not name.startswith(MULTIPART_STAGE_PREFIX):
            continue
        try:
            series[name] = float(value)
        except ValueError:
            continue
    return status, series


def stage_means(before, after):
    """Mean stage seconds inside the window, or None when the samples are incomplete."""
    report = {}
    for stage in MULTIPART_STAGES:
        labels = f'{{stage="{stage}"}}'
        total_name = MULTIPART_STAGE_PREFIX + "_sum" + labels
        count_name = MULTIPART_STAGE_PREFIX + "_count" + labels
        if total_name not in after or count_name not in after:
            continue
        total = after[total_name] - (before or {}).get(total_name, 0.0)
        count = after[count_name] - (before or {}).get(count_name, 0.0)
        if count <= 0 or total < 0:
            continue
        report[stage] = {"samples": count, "mean_ms": total / count * 1000}
    return report


def port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def run_arm(args, root, engine, pair, ordinal, deadline):
    capacity_before = common.host_capacity()
    load_before = load_average()
    if time.monotonic() + args.arm_timeout + 15 >= deadline:
        raise RuntimeError("remaining time cannot admit another arm")
    arm = Path(tempfile.mkdtemp(prefix=f"{ordinal:02d}-{engine}-multipart-complete-", dir=root))
    volume = arm / "volume"
    volume.mkdir()
    port = 19700 + ordinal
    if not port_available(port):
        shutil.rmtree(arm)
        raise RuntimeError(f"port {port} is already in use")
    endpoint = f"127.0.0.1:{port}"
    secret = secrets.token_hex(24)
    env = common.server_environment(engine, volume, endpoint, secret,
                                    "a" + secrets.token_hex(32)[1:])
    if engine == "cairn":
        try:
            bootstrap = subprocess.run((args.cairn, "bootstrap"), env=env, capture_output=True,
                                       timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            shutil.rmtree(arm)
            raise
        if bootstrap.returncode:
            shutil.rmtree(arm)
            raise RuntimeError(f"Cairn bootstrap failed: {bootstrap.stderr[-300:]!r}")
        command = (args.cairn, "serve")
        health = f"http://{endpoint}/healthz"
    else:
        command = (args.rustfs, "server", "--address", endpoint, str(volume))
        health = f"http://{endpoint}/minio/health/live"
    log = open(arm / "server.log", "wb")
    try:
        proc = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
    except OSError:
        log.close()
        shutil.rmtree(arm)
        raise
    base_bytes, base_inodes = common.tree_usage(root)
    peak = {"server_peak_rss": 0, "peak_bytes": 0, "peak_inodes": 0}
    stop = threading.Event()

    def observe():
        while not stop.wait(2):
            point = common.cpu_rss(proc.pid)
            if point:
                peak["server_peak_rss"] = max(peak["server_peak_rss"], point[2])
            used, inodes = common.arm_usage(base_bytes, base_inodes, arm)
            peak["peak_bytes"] = max(peak["peak_bytes"], used)
            peak["peak_inodes"] = max(peak["peak_inodes"], inodes)
            if used > args.max_bytes:
                stop.set()

    monitor = threading.Thread(target=observe, daemon=True)
    before = common.cpu_rss(proc.pid)
    payloads = [part_payload(index) for index in range(OBJECT_PARTS)]
    digests = [hashlib.sha256(payload).hexdigest() for payload in payloads]
    expected_digest = hashlib.sha256(b"".join(payloads)).hexdigest()
    expected_etag = composite_etag([hashlib.md5(payload).hexdigest() for payload in payloads])
    try:
        common.wait_health(health, proc)
        monitor.start()
        durability = common.prepare_verified_bucket(port, secret, engine)
        arm_deadline = min(deadline, time.monotonic() + args.arm_timeout)
        warmup_seconds = warm_up(port, secret, payloads, digests, arm_deadline)

        # Symmetric settle window: Cairn's multipart stage histograms are drained by a 15 s
        # background tick, and waiting only on Cairn's arm would give it a quieter start.
        if args.metrics_drain_seconds:
            time.sleep(args.metrics_drain_seconds)
        metrics_before = scrape_multipart_stages(port) if engine == "cairn" else None
        client_cpu_start = time.process_time()
        records, errors, wall_seconds = scored_objects(
            port, secret, args.objects, payloads, digests, arm_deadline)
        client_cpu_seconds = time.process_time() - client_cpu_start
        if args.metrics_drain_seconds:
            time.sleep(args.metrics_drain_seconds)
        metrics_after = scrape_multipart_stages(port) if engine == "cairn" else None

        client = Client(port, secret)
        try:
            checks, failures, part_coverage = verify_objects(
                client, records, digests, expected_digest, expected_etag,
                OBJECT_PARTS)
            listing = verify_listing(client, len(records))
        finally:
            client._drop()

        after = common.cpu_rss(proc.pid)
        capacity_after = common.host_capacity()
        load_after = load_average()
        used, inodes = common.arm_usage(base_bytes, base_inodes, arm)
        peak["peak_bytes"] = max(peak["peak_bytes"], used)
        peak["peak_inodes"] = max(peak["peak_inodes"], inodes)
        cycles = [record["cycle_seconds"] for record in records]
        completions = [record["completion_seconds"] for record in records]
        part_latencies = [value for record in records
                          for value in record["part_latencies"]]
        result = {
            "pair": pair,
            "ordinal": ordinal,
            "engine": engine,
            "objects": args.objects,
            "parts_per_object": OBJECT_PARTS,
            "part_bytes": PART_BYTES,
            "object_bytes": OBJECT_BYTES,
            "object_workers": OBJECT_WORKERS,
            "part_concurrency": PART_CONCURRENCY,
            "successful_completions": len(records),
            "errors": len(errors),
            "error_details": errors,
            "objects_per_second": len(records) / wall_seconds if wall_seconds else None,
            "parts_per_second": (len(records) * OBJECT_PARTS / wall_seconds
                                 if wall_seconds else None),
            "mib_per_second": (len(records) * OBJECT_BYTES / MIB / wall_seconds
                               if wall_seconds else None),
            "wall_seconds": wall_seconds,
            "warmup_seconds": warmup_seconds,
            "object_cycle": driver.latency_summary(cycles),
            "completion": driver.latency_summary(completions),
            "part": driver.latency_summary(part_latencies),
            "completion_share_of_object_time": completion_share(cycles, completions),
            "worker_utilization": (sum(cycles) / (wall_seconds * OBJECT_WORKERS)
                                   if wall_seconds else None),
            "verification": {
                "objects_checked": len(checks),
                "full_object_checks": sum(1 for check in checks if check["full_sha256_ok"]),
                "part_checks": sum(1 for check in checks if check["part_sha256_ok"]),
                "etag_checks": sum(1 for check in checks if check["etag_ok"]),
                "part_number_coverage": {str(number): count
                                         for number, count in part_coverage.items()},
                "failures": failures[:ERROR_DETAIL_LIMIT],
                "failure_count": len(failures),
                "checks": checks,
            },
            "listing": listing,
            "durability": durability,
            "client_cpu_seconds": client_cpu_seconds,
            "client_cpu_per_object": (client_cpu_seconds / len(records)
                                      if records else None),
            "server_cpu_seconds": (after[0] - before[0] if before and after else None),
            "host_capacity_before": capacity_before,
            "host_capacity_after": capacity_after,
            "load_average_before": load_before,
            "load_average_after": load_after,
            **peak,
        }
        if engine == "cairn":
            result["multipart_stage_means"] = (
                stage_means(metrics_before[1], metrics_after[1])
                if metrics_before and metrics_after else None)
            result["multipart_metrics_status"] = (
                [metrics_before[0], metrics_after[0]]
                if metrics_before and metrics_after else None)
        if engine == "rustfs":
            result["durability_after"] = common.verify_rustfs_durability(port, secret)
        result["status"] = "PASS" if (
            not stop.is_set()
            and not errors
            and len(records) == args.objects
            and not failures
            and listing["exact"]
            and result["completion"]["count"] == args.objects
            and common.capacity_compatible(capacity_before, capacity_after)
            and (engine != "rustfs" or result["durability_after"].get("mode") == "strict")
        ) else "INCONCLUSIVE"
        return result
    finally:
        stop.set()
        if monitor.is_alive():
            monitor.join(timeout=5)
        common.stop_group(proc)
        log.close()
        # Only the exact task-created arm is removed, after process-group quiescence.
        shutil.rmtree(arm)


def pair_summary(arms):
    """Join PASS arms by declared pair; a missing or failed arm is never scored."""
    grouped = {}
    for arm in arms:
        if arm["status"] != "PASS":
            continue
        grouped.setdefault(arm["pair"], {})[arm["engine"]] = arm
    pairs = []
    for pair in sorted(grouped):
        engines = grouped[pair]
        if set(engines) != {"cairn", "rustfs"}:
            continue
        cairn = engines["cairn"]["objects_per_second"]
        rustfs = engines["rustfs"]["objects_per_second"]
        if cairn is None or rustfs is None or rustfs <= 0:
            continue
        pairs.append({
            "pair": pair,
            "cairn_objects_per_second": cairn,
            "rustfs_objects_per_second": rustfs,
            "cairn_over_rustfs": cairn / rustfs,
            "cairn_completion_p50_ms": engines["cairn"]["completion"]["p50_ms"],
            "rustfs_completion_p50_ms": engines["rustfs"]["completion"]["p50_ms"],
            "cairn_completion_share": engines["cairn"]["completion_share_of_object_time"],
            "rustfs_completion_share": engines["rustfs"]["completion_share_of_object_time"],
        })
    return pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cairn", required=True)
    parser.add_argument("--rustfs", required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--objects", type=int, default=40,
                        help="exact scored object count; 40 x 50 MiB targets ~10 s per arm")
    parser.add_argument("--arm-timeout", type=int, default=300,
                        help="bound on warm-up plus the scored window, in seconds")
    parser.add_argument("--metrics-drain-seconds", type=int, default=16,
                        help="wait for Cairn's 15 s metrics tick before each scrape; both "
                             "engines wait the same settle window")
    parser.add_argument("--max-seconds", type=int, default=1800)
    parser.add_argument("--max-bytes", type=int, default=10_000_000_000)
    args = parser.parse_args()
    if not (1 <= args.pairs <= 5 and 10 <= args.objects <= 200
            and 60 <= args.arm_timeout <= 900
            and 0 <= args.metrics_drain_seconds <= 30
            and 60 <= args.max_seconds <= 7200):
        parser.error("one or more limits are outside the bounded range")
    root = args.root.resolve(strict=True)
    if not root.is_dir() or root.is_symlink() or not str(root).startswith(
            "/var/tmp/cairn-performance-"):
        parser.error("root must be a private /var/tmp/cairn-performance-* directory")
    for name, binary in (("cairn", args.cairn), ("rustfs", args.rustfs)):
        if not Path(binary).is_file() or not os.access(binary, os.X_OK):
            parser.error(f"not executable: {name}={binary}")
    data = {
        "schema": 1,
        "case": "multipart_complete",
        "method": ("fresh strict single-node store per arm; one unscored warm-up object; "
                   "exact scored object count; initiate -> 10 x 5 MiB parts at 4-way "
                   "concurrency -> complete per object; 4 object workers with no further "
                   "pipelining; then full-object plus rotating ?partNumber GET verification"),
        "pairs": args.pairs,
        "objects": args.objects,
        "parts_per_object": OBJECT_PARTS,
        "part_bytes": PART_BYTES,
        "object_workers": OBJECT_WORKERS,
        "part_concurrency": PART_CONCURRENCY,
        "warp_putpart_comparable": False,
        "arm_timeout_seconds": args.arm_timeout,
        "metrics_drain_seconds": args.metrics_drain_seconds,
        "strict_settings": common.STRICT_SETTINGS,
        "binary_sha256": {"cairn": common.sha256(args.cairn),
                          "rustfs": common.sha256(args.rustfs)},
        "host_capacity": common.host_capacity(),
        "load_average_start": load_average(),
        "arms": [],
    }
    path = root / "multipart-complete-results.json"
    began = time.monotonic()
    deadline = began + args.max_seconds - 30
    ordinal = 0
    try:
        for pair in range(1, args.pairs + 1):
            order = (("cairn", "rustfs") if pair % 2 == 1 else ("rustfs", "cairn"))
            for engine in order:
                if common.tree_usage(root)[0] > args.max_bytes - 2_000_000_000:
                    raise RuntimeError("task-owned disk headroom exhausted")
                ordinal += 1
                total = 2 * args.pairs
                print(f"[{ordinal}/{total}] multipart complete pair {pair} {engine}", flush=True)
                arm = run_arm(args, root, engine, pair, ordinal, deadline)
                data["arms"].append(arm)
                data["pair_ratios"] = pair_summary(data["arms"])
                data["elapsed_seconds"] = time.monotonic() - began
                data["load_average_end"] = load_average()
                path.write_text(json.dumps(data, indent=2) + "\n")
                print(f"  {arm['status']} {arm['objects_per_second']:.2f} obj/s "
                      f"({arm['parts_per_second']:.1f} parts/s); "
                      f"{arm['successful_completions']}/{args.objects} completed; "
                      f"complete p50 {arm['completion']['p50_ms']:.1f} ms; "
                      f"share {100 * (arm['completion_share_of_object_time'] or 0):.1f}%; "
                      f"errors={arm['errors']}", flush=True)
                if arm["status"] != "PASS":
                    raise RuntimeError("inconclusive arm; stopped without retry")
        ratios = data.get("pair_ratios", [])
        if ratios:
            data["mean_cairn_over_rustfs"] = sum(
                item["cairn_over_rustfs"] for item in ratios) / len(ratios)
            path.write_text(json.dumps(data, indent=2) + "\n")
            print(f"mean completed-object Cairn/RustFS ratio: "
                  f"{data['mean_cairn_over_rustfs']:.3f} over {len(ratios)} pair(s)",
                  flush=True)
    except (KeyboardInterrupt, RuntimeError, OSError) as error:
        data["stopped_reason"] = str(error)
        data["elapsed_seconds"] = time.monotonic() - began
        data["load_average_end"] = load_average()
        path.write_text(json.dumps(data, indent=2) + "\n")
        raise SystemExit(str(error))
    print(f"Completed {ordinal} arms in {time.monotonic() - began:.1f}s; {path}")


if __name__ == "__main__":
    main()
