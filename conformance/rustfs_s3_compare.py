#!/usr/bin/env python3
"""Matched stdlib SigV4 DELETE and completed-multipart comparison.

Complements rustfs_compare.py where Warp 1.8 cannot give a valid result. Every
arm uses a fresh single-node store, exact response checks, and private data.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import hmac
import http.client
import json
import os
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

import rustfs_compare as common


BUCKET = "cairn-benchmark"
ACCESS = "benchaccess"


def signed_request(connection, secret, method, path, query=(), body=b"", digest=None):
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    body_hash = digest or hashlib.sha256(body).hexdigest()
    headers = {
        "host": f"127.0.0.1:{connection.port}",
        "x-amz-content-sha256": body_hash,
        "x-amz-date": stamp,
    }
    names = ";".join(sorted(headers))
    canonical_query = urllib.parse.urlencode(sorted(query), quote_via=urllib.parse.quote)
    canonical = "\n".join((method, path, canonical_query,
                            "".join(f"{key}:{headers[key]}\n" for key in sorted(headers)),
                            names, body_hash))
    scope = f"{day}/us-east-1/s3/aws4_request"
    signing = ("AWS4" + secret).encode()
    for part in (day, "us-east-1", "s3", "aws4_request"):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    to_sign = "\n".join(("AWS4-HMAC-SHA256", stamp, scope,
                         hashlib.sha256(canonical.encode()).hexdigest()))
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (f"AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, "
                                f"SignedHeaders={names}, Signature={signature}")
    target = path + ("?" + canonical_query if query else "")
    connection.request(method, target, body=body, headers=headers)
    response = connection.getresponse()
    data = response.read()
    status = response.status
    result_headers = dict(response.getheaders())
    if status not in (200, 204):
        raise RuntimeError(f"{method} {path} returned HTTP {status}: {data[:200]!r}")
    return data, result_headers


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction)))]


def create_bucket(port, secret):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            signed_request(connection, secret, "PUT", f"/{BUCKET}")
            return
        except RuntimeError as error:
            if "HTTP 503" not in str(error):
                raise
        finally:
            connection.close()
        time.sleep(0.25)
    raise RuntimeError("S3 bucket creation stayed unready for 30 seconds")


def measure_delete(port, secret, count, concurrency, duration, engine=None):
    payload = hashlib.sha256(b"delete-fixture").digest() * 128
    digest = hashlib.sha256(payload).hexdigest()
    setup_start = time.monotonic()
    create_bucket(port, secret)
    if engine == "rustfs":
        common.verify_rustfs_durability(port, secret)

    def prepare(worker):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        try:
            for index in range(worker, count, concurrency):
                signed_request(client, secret, "PUT", f"/{BUCKET}/delete-{index:06d}",
                               body=payload, digest=digest)
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(prepare, range(concurrency)))
    preparation_seconds = time.monotonic() - setup_start
    start_cpu = time.process_time()
    begin = time.monotonic()
    deadline = begin + duration

    def worker(index):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        latencies = []
        try:
            for item in range(index, count, concurrency):
                if time.monotonic() >= deadline:
                    break
                before = time.monotonic()
                signed_request(client, secret, "DELETE", f"/{BUCKET}/delete-{item:06d}")
                latencies.append(time.monotonic() - before)
            return latencies
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        per_worker = list(pool.map(worker, range(concurrency)))
    elapsed = time.monotonic() - begin
    values = [value for group in per_worker for value in group]
    return {
        "successful_operations": len(values),
        "objects_per_second": len(values) / elapsed,
        "elapsed_seconds": elapsed,
        "preparation_seconds": preparation_seconds,
        "client_cpu_seconds": time.process_time() - start_cpu,
        "p50_ms": percentile(values, 0.50) * 1000 if values else None,
        "p95_ms": percentile(values, 0.95) * 1000 if values else None,
        "p99_ms": percentile(values, 0.99) * 1000 if len(values) >= 10000 else None,
        "complete_interval": elapsed >= duration * 0.95,
    }


def measure_multipart(port, secret, concurrency, duration, engine=None):
    part = hashlib.shake_256(b"multipart-fixture").digest(5 * 1024 * 1024)
    digest = hashlib.sha256(part).hexdigest()
    expected = hashlib.sha256(part + part).hexdigest()
    create_bucket(port, secret)
    if engine == "rustfs":
        common.verify_rustfs_durability(port, secret)
    begin = time.monotonic()
    deadline = begin + duration
    start_cpu = time.process_time()

    def worker(index):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
        values = []
        sequence = 0
        try:
            while time.monotonic() < deadline:
                path = f"/{BUCKET}/multipart-{index:02d}-{sequence:06d}"
                sequence += 1
                before = time.monotonic()
                initiated, _ = signed_request(connection, secret, "POST", path, (("uploads", ""),))
                upload = ET.fromstring(initiated).findtext("{*}UploadId")
                if not upload:
                    raise RuntimeError("multipart initiation omitted UploadId")
                etags = []
                for number in (1, 2):
                    _, headers = signed_request(connection, secret, "PUT", path,
                                                (("partNumber", str(number)), ("uploadId", upload)),
                                                part, digest)
                    etag = headers.get("etag")
                    if not etag:
                        raise RuntimeError("UploadPart omitted ETag")
                    etags.append(etag)
                complete = ("<CompleteMultipartUpload>" + "".join(
                    f"<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
                    for number, etag in enumerate(etags, 1)) + "</CompleteMultipartUpload>").encode()
                completed, _ = signed_request(connection, secret, "POST", path,
                                              (("uploadId", upload),), complete)
                if ET.fromstring(completed).tag.rsplit("}", 1)[-1] == "Error":
                    raise RuntimeError(f"CompleteMultipartUpload returned error: {completed[:200]!r}")
                body, _ = signed_request(connection, secret, "GET", path)
                if len(body) != 10 * 1024 * 1024 or hashlib.sha256(body).hexdigest() != expected:
                    raise RuntimeError("completed multipart bytes failed SHA-256 verification")
                signed_request(connection, secret, "DELETE", path)
                values.append(time.monotonic() - before)
            return values
        finally:
            connection.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        groups = list(pool.map(worker, range(concurrency)))
    elapsed = time.monotonic() - begin
    values = [value for group in groups for value in group]
    return {
        "successful_operations": len(values),
        "objects_per_second": len(values) / elapsed,
        "mib_per_second": 10 * len(values) / elapsed,
        "elapsed_seconds": elapsed,
        "client_cpu_seconds": time.process_time() - start_cpu,
        "p50_ms": percentile(values, 0.50) * 1000 if values else None,
        "p95_ms": percentile(values, 0.95) * 1000 if values else None,
        "p99_ms": percentile(values, 0.99) * 1000 if len(values) >= 10000 else None,
        "complete_interval": elapsed >= duration * 0.95,
        "verified_bytes": 10 * 1024 * 1024 * len(values),
    }


def run_arm(args, root, engine, ordinal):
    arm = Path(tempfile.mkdtemp(prefix=f"s3-{ordinal:02d}-{engine}-{args.case}-", dir=root))
    volume = arm / "volume"
    volume.mkdir()
    port = 19000 + ordinal
    secret = secrets.token_hex(24)
    env = common.server_environment(engine, volume, f"127.0.0.1:{port}", secret,
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
        health = f"http://127.0.0.1:{port}/healthz"
    else:
        command = (args.rustfs, "server", "--address", f"127.0.0.1:{port}", str(volume))
        health = f"http://127.0.0.1:{port}/minio/health/live"
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
    try:
        common.wait_health(health, proc)
        monitor.start()
        if args.case == "delete":
            result = measure_delete(port, secret, args.objects, args.concurrency, args.duration, engine)
        else:
            result = measure_multipart(port, secret, args.concurrency, args.duration, engine)
        after = common.cpu_rss(proc.pid)
        final_bytes, final_inodes = common.arm_usage(base_bytes, base_inodes, arm)
        peak["peak_bytes"] = max(peak["peak_bytes"], final_bytes)
        peak["peak_inodes"] = max(peak["peak_inodes"], final_inodes)
        if final_bytes > args.max_bytes:
            stop.set()
        result.update(engine=engine, ordinal=ordinal, case=args.case,
                      server_cpu_seconds=after[0] - before[0] if before and after else None,
                      strict_settings=common.STRICT_SETTINGS[engine], **peak)
        if engine == "rustfs":
            result["durability_after"] = common.verify_rustfs_durability(port, secret)
        result["status"] = "PASS" if (
            not stop.is_set() and result["successful_operations"] > 0
            and (args.case != "delete" or result["complete_interval"])
        ) else "INCONCLUSIVE"
        return result
    finally:
        stop.set()
        if monitor.is_alive():
            monitor.join(timeout=5)
        common.stop_group(proc)
        log.close()
        # Exact task-created arm, removed only after process quiescence.
        shutil.rmtree(arm)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cairn", required=True)
    parser.add_argument("--rustfs", required=True)
    parser.add_argument("--case", choices=("delete", "multipart"), required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--duration", type=int, default=12)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--objects", type=int, default=12000)
    parser.add_argument("--max-seconds", type=int, default=1200)
    parser.add_argument("--max-bytes", type=int, default=10_000_000_000)
    args = parser.parse_args()
    if not (1 <= args.pairs <= 5 and 1 <= args.duration <= 60 and 1 <= args.concurrency <= 32
            and 100 <= args.objects <= 20000 and 60 <= args.max_seconds <= 3600):
        parser.error("one or more limits are outside the bounded range")
    root = args.root.resolve(strict=True)
    if not root.is_dir() or root.is_symlink() or not str(root).startswith("/var/tmp/cairn-performance-"):
        parser.error("root must be a private /var/tmp/cairn-performance-* directory")
    data = {"schema": 2, "case": args.case, "pairs": args.pairs,
            "strict_settings": common.STRICT_SETTINGS,
            "duration": args.duration, "concurrency": args.concurrency,
            "objects": args.objects if args.case == "delete" else None,
            "binary_sha256": {"cairn": common.sha256(args.cairn),
                              "rustfs": common.sha256(args.rustfs)}, "arms": []}
    path = root / f"s3-{args.case}-results.json"
    began = time.monotonic()
    ordinal = 0
    try:
        for pair in range(args.pairs):
            for engine in (("cairn", "rustfs") if pair % 2 == 0 else ("rustfs", "cairn")):
                if time.monotonic() - began > args.max_seconds - 60:
                    raise RuntimeError("time budget exhausted before another arm")
                if common.tree_usage(root)[0] > args.max_bytes - 1_000_000_000:
                    raise RuntimeError("task-owned disk headroom exhausted")
                ordinal += 1
                print(f"[{ordinal}/{2 * args.pairs}] {args.case} pair {pair + 1} {engine}", flush=True)
                arm = run_arm(args, root, engine, ordinal)
                data["arms"].append(arm)
                data["elapsed_seconds"] = time.monotonic() - began
                path.write_text(json.dumps(data, indent=2) + "\n")
                print(f"  {arm['status']} {arm['objects_per_second']:.2f} obj/s; "
                      f"{arm['successful_operations']} verified", flush=True)
                if arm["status"] != "PASS":
                    raise RuntimeError("inconclusive arm; stopped without retry")
    except (KeyboardInterrupt, RuntimeError, OSError) as error:
        data["stopped_reason"] = str(error)
        data["elapsed_seconds"] = time.monotonic() - began
        path.write_text(json.dumps(data, indent=2) + "\n")
        raise SystemExit(str(error))


if __name__ == "__main__":
    main()
