#!/usr/bin/env python3
"""Exact single-key DELETE comparison for Cairn and RustFS on one host.

The bulk `delete_4k` case in `rustfs_compare.py` uses Warp with `--batch 100`,
so it is a multi-object-delete measurement. This script is deliberately a
different shape: it creates an exact pool of distinct 4 KiB keys and issues one
signed S3 DELETE per key, with bounded client concurrency. The pool is fully
depleted in every scored arm, so the reported rate is `pool / wall time`, not a
sampled interval.

The DELETE arm is separate evidence and is never pooled with the bulk
`delete_4k` ratio. Both engines use the same strict settings as
`rustfs_compare.py`; every arm starts a fresh single-node store and its private
arm directory is removed after the server process group is reaped.
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
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path

import rustfs_compare as common


BUCKET = common.BUCKET
ACCESS = common.ACCESS
KEY_BYTES = 4 * 1024
PREFILL_CONCURRENCY = 32
ERROR_DETAIL_LIMIT = 20
LATENCY_LIMIT = 200_000


def signed_request(connection, secret, method, path, query=(), body=b"", digest=None):
    """Reuse one HTTP connection per worker; sign every request independently."""
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
    payload = response.read()
    return response.status, payload


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int((len(ordered) - 1) * fraction)))]


def latency_summary(values):
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "mean_ms": sum(values) / len(values) * 1000,
        "p50_ms": percentile(values, 0.50) * 1000,
        "p90_ms": percentile(values, 0.90) * 1000,
        "p99_ms": percentile(values, 0.99) * 1000,
        "max_ms": max(values) * 1000,
    }


def key_name(index):
    return f"single-{index:09d}"


def port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def prefill_pool(port, secret, count, deadline):
    """Create exactly `count` distinct keys; the prefill is outside the delete window."""
    body = b"x" * KEY_BYTES
    digest = hashlib.sha256(body).hexdigest()
    errors = []
    lock = threading.Lock()

    def prepare(worker):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            for index in range(worker, count, PREFILL_CONCURRENCY):
                if time.monotonic() >= deadline:
                    with lock:
                        errors.append({"key": key_name(index), "error": "prefill deadline exceeded"})
                    return
                status, payload = signed_request(
                    client, secret, "PUT", f"/{BUCKET}/{key_name(index)}",
                    body=body, digest=digest)
                if status not in (200, 204):
                    with lock:
                        errors.append({"key": key_name(index), "status": status,
                                       "body": payload[:200].decode(errors="replace")})
                    return
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=PREFILL_CONCURRENCY) as pool:
        list(pool.map(prepare, range(PREFILL_CONCURRENCY)))
    return errors


def delete_pool(port, secret, count, concurrency, deadline):
    """Issue exactly one DELETE per key and fail closed on any non-2xx response."""
    latencies = []
    errors = []
    lock = threading.Lock()
    aborted = threading.Event()

    def worker(index):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        local = []
        try:
            for item in range(index, count, concurrency):
                if aborted.is_set():
                    return local
                if time.monotonic() >= deadline:
                    with lock:
                        errors.append({"key": key_name(item), "error": "delete deadline exceeded"})
                    aborted.set()
                    return local
                before = time.monotonic()
                try:
                    status, payload = signed_request(
                        client, secret, "DELETE", f"/{BUCKET}/{key_name(item)}")
                except (OSError, http.client.HTTPException) as error:
                    with lock:
                        errors.append({"key": key_name(item), "error": type(error).__name__})
                    aborted.set()
                    return local
                local.append(time.monotonic() - before)
                if status not in (200, 204):
                    with lock:
                        if len(errors) < ERROR_DETAIL_LIMIT:
                            errors.append({"key": key_name(item), "status": status,
                                           "body": payload[:200].decode(errors="replace")})
                        else:
                            errors.append({"key": key_name(item), "status": status})
                    aborted.set()
                    return local
            return local
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        groups = list(pool.map(worker, range(concurrency)))
    for group in groups:
        latencies.extend(group[:LATENCY_LIMIT])
    return latencies, errors


def verify_absent(port, secret, count):
    """Prove the endpoints removed the sampled keys, not just answered 204."""
    samples = sorted({0, count // 2, count - 1})
    checks = []
    for index in samples:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            status, _ = signed_request(
                connection, secret, "HEAD", f"/{BUCKET}/{key_name(index)}")
        finally:
            connection.close()
        checks.append({"key": key_name(index), "status": status})
    return checks


def verify_empty(port, secret):
    """A sampled HEAD cannot prove a whole pool; one bounded LIST can see any survivor."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        status, payload = signed_request(
            connection, secret, "GET", f"/{BUCKET}",
            query=(("list-type", "2"), ("max-keys", "1")))
    finally:
        connection.close()
    empty = status == 200 and b"<Contents>" not in payload and b"<Key>" not in payload
    return {"status": status, "empty": empty, "body_prefix": payload[:200].decode(errors="replace")}


def run_arm(args, root, engine, pair, ordinal, deadline):
    capacity_before = common.host_capacity()
    if time.monotonic() + args.arm_timeout + 10 >= deadline:
        raise RuntimeError("remaining time cannot admit another arm")
    arm = Path(tempfile.mkdtemp(prefix=f"{ordinal:02d}-{engine}-delete-single-key-", dir=root))
    volume = arm / "volume"
    volume.mkdir()
    port = 19500 + ordinal
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
    try:
        common.wait_health(health, proc)
        monitor.start()
        durability = common.prepare_verified_bucket(port, secret, engine)
        prefill_start = time.monotonic()
        prefill_errors = prefill_pool(port, secret, args.objects,
                                      min(deadline, prefill_start + args.arm_timeout))
        if prefill_errors:
            raise RuntimeError(f"prefill failed: {prefill_errors[:3]!r}")
        prefill_seconds = time.monotonic() - prefill_start

        used, inodes = common.arm_usage(base_bytes, base_inodes, arm)
        peak["peak_bytes"] = max(peak["peak_bytes"], used)
        peak["peak_inodes"] = max(peak["peak_inodes"], inodes)

        client_cpu_start = time.process_time()
        delete_start = time.monotonic()
        latencies, errors = delete_pool(port, secret, args.objects, args.concurrency,
                                        delete_start + args.arm_timeout)
        wall_seconds = time.monotonic() - delete_start
        client_cpu_seconds = time.process_time() - client_cpu_start
        checks = verify_absent(port, secret, args.objects)
        empty = verify_empty(port, secret)
        absent = all(check["status"] == 404 for check in checks) and empty["empty"]

        after = common.cpu_rss(proc.pid)
        final_bytes, final_inodes = common.arm_usage(base_bytes, base_inodes, arm)
        peak["peak_bytes"] = max(peak["peak_bytes"], final_bytes)
        peak["peak_inodes"] = max(peak["peak_inodes"], final_inodes)
        capacity_after = common.host_capacity()
        result = {
            "pair": pair,
            "ordinal": ordinal,
            "engine": engine,
            "pool_objects": args.objects,
            "key_bytes": KEY_BYTES,
            "concurrency": args.concurrency,
            "successful_operations": len(latencies),
            "errors": len(errors),
            "error_details": errors,
            "objects_per_second": len(latencies) / wall_seconds if wall_seconds else None,
            "wall_seconds": wall_seconds,
            "prefill_seconds": prefill_seconds,
            "latency": latency_summary(latencies),
            "durability": durability,
            "deletion_spot_check": checks,
            "bucket_empty": empty,
            "client_cpu_seconds": client_cpu_seconds,
            "client_cpu_per_object": (client_cpu_seconds / len(latencies)
                                      if latencies else None),
            "server_cpu_seconds": (after[0] - before[0] if before and after else None),
            "host_capacity_before": capacity_before,
            "host_capacity_after": capacity_after,
            **peak,
        }
        if engine == "rustfs":
            result["durability_after"] = common.verify_rustfs_durability(port, secret)
        result["status"] = "PASS" if (
            not stop.is_set() and absent and not errors
            and len(latencies) == args.objects
            and empty["status"] == 200
            and common.capacity_compatible(capacity_before, capacity_after)
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
    """Join PASS arms by declared pair; a missing or failed arm is not scored."""
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
        pairs.append({"pair": pair, "cairn_objects_per_second": cairn,
                      "rustfs_objects_per_second": rustfs,
                      "cairn_over_rustfs": cairn / rustfs})
    return pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cairn", required=True)
    parser.add_argument("--rustfs", required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--objects", type=int, default=12_000,
                        help="exact pool size; 12,000 x 4 KiB targets a ~15 s Cairn arm")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--arm-timeout", type=int, default=300,
                        help="bound on prefill and on the scored delete, in seconds")
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--max-bytes", type=int, default=10_000_000_000)
    args = parser.parse_args()
    if not (1 <= args.pairs <= 5 and 1_000 <= args.objects <= 100_000
            and 1 <= args.concurrency <= 32 and 30 <= args.arm_timeout <= 900
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
        "case": "delete_single_key",
        "method": ("exact pool of distinct 4 KiB keys; one signed S3 DELETE per key with "
                   "bounded concurrency; prefill and scored delete are separate intervals"),
        "pairs": args.pairs,
        "pool_objects": args.objects,
        "key_bytes": KEY_BYTES,
        "concurrency": args.concurrency,
        "arm_timeout_seconds": args.arm_timeout,
        "strict_settings": common.STRICT_SETTINGS,
        "binary_sha256": {"cairn": common.sha256(args.cairn),
                          "rustfs": common.sha256(args.rustfs)},
        "arms": [],
    }
    path = root / "delete-single-key-results.json"
    began = time.monotonic()
    deadline = began + args.max_seconds - 30
    ordinal = 0
    try:
        for pair in range(1, args.pairs + 1):
            order = (("cairn", "rustfs") if pair % 2 == 1 else ("rustfs", "cairn"))
            for engine in order:
                if common.tree_usage(root)[0] > args.max_bytes - 1_000_000_000:
                    raise RuntimeError("task-owned disk headroom exhausted")
                ordinal += 1
                total = 2 * args.pairs
                print(f"[{ordinal}/{total}] single-key delete pair {pair} {engine}", flush=True)
                arm = run_arm(args, root, engine, pair, ordinal, deadline)
                data["arms"].append(arm)
                data["pair_ratios"] = pair_summary(data["arms"])
                data["elapsed_seconds"] = time.monotonic() - began
                path.write_text(json.dumps(data, indent=2) + "\n")
                print(f"  {arm['status']} {arm['objects_per_second']:.2f} obj/s; "
                      f"{arm['successful_operations']}/{args.objects} deleted; "
                      f"errors={arm['errors']}", flush=True)
                if arm["status"] != "PASS":
                    raise RuntimeError("inconclusive arm; stopped without retry")
        ratios = data.get("pair_ratios", [])
        if ratios:
            data["mean_cairn_over_rustfs"] = sum(item["cairn_over_rustfs"]
                                                 for item in ratios) / len(ratios)
            path.write_text(json.dumps(data, indent=2) + "\n")
            print(f"mean Cairn/RustFS ratio: {data['mean_cairn_over_rustfs']:.3f} "
                  f"over {len(ratios)} pair(s)", flush=True)
    except (KeyboardInterrupt, RuntimeError, OSError) as error:
        data["stopped_reason"] = str(error)
        data["elapsed_seconds"] = time.monotonic() - began
        path.write_text(json.dumps(data, indent=2) + "\n")
        raise SystemExit(str(error))
    print(f"Completed {ordinal} arms in {time.monotonic() - began:.1f}s; {path}")


if __name__ == "__main__":
    main()
