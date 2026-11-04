#!/usr/bin/env python3
"""Durability-equivalent versioned single-key DELETE comparison for Cairn and RustFS.

`delete_single_key.py` measures one DELETE per key on an unversioned bucket. For RustFS
1.0.0 that arm is **as-configured**, not equal-durability: its `strict` policy is not
consulted by the unversioned delete path, which unlinks without syncing. This script
enables S3 versioning before the prefill, so every DELETE creates a delete marker and
RustFS `strict` must commit the marker (`xl.meta`) durably. It is the
durability-equivalent companion arm and is never pooled with either unversioned ratio.

Each arm starts a fresh single-node store and removes its private arm directory after
the server process group is reaped. The pool is exactly 12,000 distinct 4 KiB keys and
the scored interval is the full pool depletion; the prefill is timed but excluded.
"""

import argparse
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import os
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import xml.etree.ElementTree as ElementTree
from pathlib import Path

import delete_single_key as single
import rustfs_compare as common


BUCKET = common.BUCKET
ACCESS = common.ACCESS
KEY_BYTES = single.KEY_BYTES
PREFILL_CONCURRENCY = single.PREFILL_CONCURRENCY
ERROR_DETAIL_LIMIT = single.ERROR_DETAIL_LIMIT
LATENCY_LIMIT = single.LATENCY_LIMIT
VERSIONING_BODY = (
    b'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    b"<Status>Enabled</Status></VersioningConfiguration>"
)
VERSION_PAGE_LIMIT = 1000
VERSION_AUDIT_SECONDS = 180


def load_average():
    try:
        return [float(value) for value in
                Path("/proc/loadavg").read_text().split()[:3]]
    except (OSError, ValueError):
        return None


def enable_versioning(port, secret):
    """Enable versioning, then prove the engine read it back as Enabled."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        status, payload = single.signed_request(
            connection, secret, "PUT", f"/{BUCKET}",
            query=(("versioning", ""),), body=VERSIONING_BODY)
        if status not in (200, 204):
            raise RuntimeError(f"enable versioning returned HTTP {status}: {payload[:200]!r}")
        status, payload = single.signed_request(
            connection, secret, "GET", f"/{BUCKET}", query=(("versioning", ""),))
    finally:
        connection.close()
    if status != 200:
        raise RuntimeError(f"read versioning returned HTTP {status}: {payload[:200]!r}")
    if b"<Status>Enabled</Status>" not in payload:
        raise RuntimeError(f"versioning readback is not Enabled: {payload[:200]!r}")
    return {"status": "Enabled",
            "readback_prefix": payload[:200].decode(errors="replace")}


def tag_name(element):
    return element.tag.rsplit("}", 1)[-1]


def parse_version_page(payload):
    """Parse one ListObjectVersions page into entries, truncation and next markers."""
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as error:
        raise ValueError("version listing page is not XML") from error
    versions = []
    markers = []
    truncated = False
    next_key = ""
    next_version = ""
    for child in root:
        name = tag_name(child)
        if name in ("Version", "DeleteMarker"):
            fields = {tag_name(field): (field.text or "") for field in child}
            entry = {"key": fields.get("Key"), "version_id": fields.get("VersionId")}
            (versions if name == "Version" else markers).append(entry)
        elif name == "IsTruncated":
            truncated = (child.text or "").strip().lower() == "true"
        elif name == "NextKeyMarker":
            next_key = child.text or ""
        elif name == "NextVersionIdMarker":
            next_version = child.text or ""
    return versions, markers, truncated, next_key, next_version


def version_history_audit(port, secret, count):
    """Page the whole `?versions` listing and count versions and delete markers.

    The pooled count is the whole-pool proof: every DELETE must have produced exactly
    one marker while every prefilled key keeps its version. A sampled HEAD cannot show
    that, and `list-type=2` hides both by construction.
    """
    expected = {single.key_name(index) for index in range(count)}
    versions = []
    markers = []
    pages = 0
    key_marker = ""
    version_marker = ""
    deadline = time.monotonic() + VERSION_AUDIT_SECONDS
    while True:
        query = [("versions", ""), ("max-keys", str(VERSION_PAGE_LIMIT))]
        if key_marker:
            query.append(("key-marker", key_marker))
        if version_marker:
            query.append(("version-id-marker", version_marker))
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            status, payload = single.signed_request(
                connection, secret, "GET", f"/{BUCKET}", query=tuple(query))
        finally:
            connection.close()
        if status != 200:
            raise RuntimeError(f"version listing page {pages + 1} returned HTTP {status}: "
                               f"{payload[:200]!r}")
        try:
            page_versions, page_markers, truncated, next_key, next_version = (
                parse_version_page(payload))
        except ValueError as error:
            raise RuntimeError(f"version listing page {pages + 1}: {error}") from error
        versions.extend(page_versions)
        markers.extend(page_markers)
        entries = len(page_versions) + len(page_markers)
        pages += 1
        if not truncated:
            break
        if entries == 0 or (next_key, next_version) == (key_marker, version_marker):
            raise RuntimeError("version listing pagination did not advance")
        if time.monotonic() >= deadline:
            raise RuntimeError("version listing audit deadline exceeded")
        key_marker, version_marker = next_key, next_version
        if pages > (count * 2) // VERSION_PAGE_LIMIT + 16:
            raise RuntimeError("version listing audit exceeded its page bound")
    version_keys = {entry["key"] for entry in versions}
    marker_keys = {entry["key"] for entry in markers}
    return {
        "pages": pages,
        "version_entries": len(versions),
        "delete_marker_entries": len(markers),
        "version_keys_exact": version_keys == expected and len(versions) == count,
        "delete_marker_keys_exact": marker_keys == expected and len(markers) == count,
        "duplicates": (len(versions) - len(version_keys)) + (len(markers) - len(marker_keys)),
    }


def marker_spot_checks(port, secret, count):
    """A marker must answer HEAD with 404 plus `x-amz-delete-marker: true`."""
    checks = []
    for index in sorted({0, count // 2, count - 1}):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        try:
            status, headers, _ = single.signed_request_with_headers(
                connection, secret, "HEAD", f"/{BUCKET}/{single.key_name(index)}")
        finally:
            connection.close()
        checks.append({"key": single.key_name(index), "status": status,
                       "delete_marker": headers.get("x-amz-delete-marker")})
    return checks


def delete_pool(port, secret, count, concurrency, deadline):
    """Issue exactly one DELETE per key, requiring a delete-marker acknowledgement."""
    latencies = []
    errors = []
    markers = 0
    lock = threading.Lock()
    aborted = threading.Event()

    def worker(index):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        local = []
        local_markers = 0
        try:
            for item in range(index, count, concurrency):
                if aborted.is_set():
                    return local, local_markers
                if time.monotonic() >= deadline:
                    with lock:
                        errors.append({"key": single.key_name(item),
                                       "error": "delete deadline exceeded"})
                    aborted.set()
                    return local, local_markers
                before = time.monotonic()
                try:
                    status, headers, payload = single.signed_request_with_headers(
                        client, secret, "DELETE", f"/{BUCKET}/{single.key_name(item)}")
                except (OSError, http.client.HTTPException) as error:
                    with lock:
                        errors.append({"key": single.key_name(item),
                                       "error": type(error).__name__})
                    aborted.set()
                    return local, local_markers
                local.append(time.monotonic() - before)
                if status not in (200, 204):
                    with lock:
                        if len(errors) < ERROR_DETAIL_LIMIT:
                            errors.append({"key": single.key_name(item), "status": status,
                                           "body": payload[:200].decode(errors="replace")})
                        else:
                            errors.append({"key": single.key_name(item), "status": status})
                    aborted.set()
                    return local, local_markers
                if headers.get("x-amz-delete-marker") != "true":
                    with lock:
                        errors.append({"key": single.key_name(item),
                                       "error": "DELETE did not acknowledge a delete marker",
                                       "status": status})
                    aborted.set()
                    return local, local_markers
                local_markers += 1
            return local, local_markers
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        groups = list(pool.map(worker, range(concurrency)))
    for group, group_markers in groups:
        latencies.extend(group[:LATENCY_LIMIT])
        markers += group_markers
    return latencies, errors, markers


def run_arm(args, root, engine, pair, ordinal, deadline):
    capacity_before = common.host_capacity()
    load_before = load_average()
    if time.monotonic() + args.arm_timeout + 10 >= deadline:
        raise RuntimeError("remaining time cannot admit another arm")
    arm = Path(tempfile.mkdtemp(prefix=f"{ordinal:02d}-{engine}-delete-versioned-", dir=root))
    volume = arm / "volume"
    volume.mkdir()
    port = 19600 + ordinal
    if not single.port_available(port):
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
        durability = {**durability, "versioning": enable_versioning(port, secret)}
        prefill_start = time.monotonic()
        prefill_errors = single.prefill_pool(
            port, secret, args.objects, min(deadline, prefill_start + args.arm_timeout))
        if prefill_errors:
            raise RuntimeError(f"prefill failed: {prefill_errors[:3]!r}")
        prefill_seconds = time.monotonic() - prefill_start

        used, inodes = common.arm_usage(base_bytes, base_inodes, arm)
        peak["peak_bytes"] = max(peak["peak_bytes"], used)
        peak["peak_inodes"] = max(peak["peak_inodes"], inodes)

        client_cpu_start = time.process_time()
        delete_start = time.monotonic()
        latencies, errors, markers = delete_pool(
            port, secret, args.objects, args.concurrency, delete_start + args.arm_timeout)
        wall_seconds = time.monotonic() - delete_start
        client_cpu_seconds = time.process_time() - client_cpu_start

        marker_checks = marker_spot_checks(port, secret, args.objects)
        current_listing = single.verify_empty(port, secret)
        history = version_history_audit(port, secret, args.objects)
        markers_present = all(check["status"] == 404
                              and check["delete_marker"] == "true"
                              for check in marker_checks)
        history_exact = (history["version_entries"] == args.objects
                         and history["delete_marker_entries"] == args.objects
                         and history["version_keys_exact"]
                         and history["delete_marker_keys_exact"])

        after = common.cpu_rss(proc.pid)
        final_bytes, final_inodes = common.arm_usage(base_bytes, base_inodes, arm)
        peak["peak_bytes"] = max(peak["peak_bytes"], final_bytes)
        peak["peak_inodes"] = max(peak["peak_inodes"], final_inodes)
        capacity_after = common.host_capacity()
        result = {
            "pair": pair,
            "ordinal": ordinal,
            "engine": engine,
            "versioned": True,
            "pool_objects": args.objects,
            "key_bytes": KEY_BYTES,
            "concurrency": args.concurrency,
            "successful_operations": len(latencies),
            "delete_markers_acknowledged": markers,
            "errors": len(errors),
            "error_details": errors,
            "objects_per_second": len(latencies) / wall_seconds if wall_seconds else None,
            "wall_seconds": wall_seconds,
            "prefill_seconds": prefill_seconds,
            "latency": single.latency_summary(latencies),
            "durability": durability,
            "marker_spot_checks": marker_checks,
            "current_listing": current_listing,
            "version_history": history,
            "client_cpu_seconds": client_cpu_seconds,
            "client_cpu_per_object": (client_cpu_seconds / len(latencies)
                                      if latencies else None),
            "server_cpu_seconds": (after[0] - before[0] if before and after else None),
            "host_load_before": load_before,
            "host_load_after": load_average(),
            "host_capacity_before": capacity_before,
            "host_capacity_after": capacity_after,
            **peak,
        }
        if engine == "rustfs":
            result["durability_after"] = common.verify_rustfs_durability(port, secret)
        result["status"] = "PASS" if (
            not stop.is_set() and markers_present and history_exact
            and current_listing["empty"] and not errors
            and len(latencies) == args.objects
            and markers == args.objects
            and current_listing["status"] == 200
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cairn", required=True)
    parser.add_argument("--rustfs", required=True)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--objects", type=int, default=12_000,
                        help="exact pool size; 12,000 x 4 KiB matches the unversioned arm")
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
        "case": "delete_versioned_single_key",
        "method": ("versioning Enabled; exact pool of distinct 4 KiB keys; one signed S3 "
                   "DELETE per key with bounded concurrency, each acknowledged with a delete "
                   "marker; prefill and scored delete are separate intervals"),
        "durability_equivalent": True,
        "pairs": args.pairs,
        "pool_objects": args.objects,
        "key_bytes": KEY_BYTES,
        "concurrency": args.concurrency,
        "arm_timeout_seconds": args.arm_timeout,
        "strict_settings": common.STRICT_SETTINGS,
        "binary_sha256": {"cairn": common.sha256(args.cairn),
                          "rustfs": common.sha256(args.rustfs)},
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "host_load_at_start": load_average(),
        "arms": [],
    }
    path = root / "delete-versioned-single-key-results.json"
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
                print(f"[{ordinal}/{total}] versioned delete pair {pair} {engine}", flush=True)
                arm = run_arm(args, root, engine, pair, ordinal, deadline)
                data["arms"].append(arm)
                data["pair_ratios"] = single.pair_summary(data["arms"])
                data["elapsed_seconds"] = time.monotonic() - began
                data["host_load_at_end"] = load_average()
                path.write_text(json.dumps(data, indent=2) + "\n")
                print(f"  {arm['status']} {arm['objects_per_second']:.2f} obj/s; "
                      f"{arm['successful_operations']}/{args.objects} deleted with "
                      f"{arm['delete_markers_acknowledged']} markers; "
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
