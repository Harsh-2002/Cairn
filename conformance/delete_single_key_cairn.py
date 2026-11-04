#!/usr/bin/env python3
"""Bounded Cairn-only single-key DELETE diagnostic with writer stage attribution.

This is the diagnostic companion to `delete_single_key.py`. It runs one Cairn arm
(fresh store, exact pool, one signed DELETE per key) and additionally scrapes
`/metrics` immediately before and after the scored delete window, so the arm can
attribute the measured rate to the writer stages the harness normally hides:
admission, queue, begin, apply, commit, and the checkpoint loop.

The client is imported verbatim from `delete_single_key.py` so a diagnostic rate
is directly comparable to that harness. This script only ever measures Cairn; it
is not an engine ratio and must not be reported as one.
"""

import argparse
import http.client
import json
import os
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import delete_single_key as driver
import rustfs_compare as common

PROCESS_IO_FIELDS = ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes")
STAGE_NAMES = ("admission", "queue", "begin", "apply", "commit", "checkpoint")
INTERESTING = (
    "cairn_writer_stage_seconds",
    "cairn_writer_commit_seconds",
    "cairn_writer_batch_size",
    "cairn_request_duration_seconds",
    "cairn_requests_total",
    "cairn_wal_bytes",
    "cairn_writer_queue_depth",
    "cairn_wal_checkpoints_total",
    "cairn_wal_checkpoints_busy_total",
    "cairn_wal_checkpointed_frames_total",
    "cairn_overlay_pending",
)


def load_average():
    return [float(value) for value in Path("/proc/loadavg").read_text().split()[:3]]


def process_io(pid):
    try:
        values = {}
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            key, separator, value = line.partition(":")
            if separator and key in PROCESS_IO_FIELDS:
                values[key] = int(value.strip())
        return values if set(values) == set(PROCESS_IO_FIELDS) else None
    except (OSError, ValueError):
        return None


def io_delta(before, after):
    if before is None or after is None:
        return None
    delta = {key: after[key] - before[key] for key in PROCESS_IO_FIELDS}
    return delta if all(value >= 0 for value in delta.values()) else None


def scrape(port):
    """Parse the exposition text into name+labels -> value.

    Histograms render as summaries, so `_sum`/`_count` are the cumulative pair and the
    quantile series describe the exporter's rolling window.
    """
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
        try:
            number = float(value)
        except ValueError:
            continue
        if not name.startswith(INTERESTING):
            continue
        series[name] = number
    return status, series


def counter_delta(before, after, name):
    """Delta of a monotonic counter series, or None when the window did not observe it.

    A series can first appear inside the window — the `DELETE`-labelled request histogram or a
    WAL checkpoint counter that never fired during the prefill — and its absence before the window
    is a zero start, not missing evidence. An absent after-value is always missing evidence.
    """
    if after is None or name not in after:
        return None
    return after[name] - (before or {}).get(name, 0.0)


def stage_report(before, after):
    report = {}
    for stage in STAGE_NAMES:
        labels = f'{{stage="{stage}",result="ok"}}'
        total = counter_delta(before, after, "cairn_writer_stage_seconds_sum" + labels)
        count = counter_delta(before, after, "cairn_writer_stage_seconds_count" + labels)
        if total is None or count is None:
            continue
        report[stage] = {
            "samples": count,
            "mean_ms": total / count * 1000 if count else None,
            "total_seconds": total,
        }
    commit_sum = counter_delta(before, after, "cairn_writer_commit_seconds_sum")
    commit_count = counter_delta(before, after, "cairn_writer_commit_seconds_count")
    batch_sum = counter_delta(before, after, "cairn_writer_batch_size_sum")
    batch_count = counter_delta(before, after, "cairn_writer_batch_size_count")
    if commit_sum is not None and commit_count:
        report["commit"] = {
            "commits": commit_count,
            "mean_ms": commit_sum / commit_count * 1000,
            "total_seconds": commit_sum,
        }
    if batch_sum is not None and batch_count:
        report["batch"] = {
            "batches": batch_count,
            "mean_size": batch_sum / batch_count,
        }
    delete_sum = counter_delta(before, after,
                       'cairn_request_duration_seconds_sum{method="DELETE",route="s3"}')
    delete_count = counter_delta(before, after,
                         'cairn_request_duration_seconds_count{method="DELETE",route="s3"}')
    if delete_sum is not None and delete_count:
        report["request"] = {
            "count": delete_count,
            "mean_ms": delete_sum / delete_count * 1000,
            "total_seconds": delete_sum,
        }
    return report


def run_arm(args):
    capacity_before = common.host_capacity()
    arm = Path(tempfile.mkdtemp(prefix="cairn-delete-diag-", dir=args.root))
    volume = arm / "volume"
    volume.mkdir()
    port = args.port
    endpoint = f"127.0.0.1:{port}"
    secret = secrets.token_hex(24)
    env = common.server_environment("cairn", volume, endpoint, secret,
                                    "a" + secrets.token_hex(32)[1:])
    if args.linger_us:
        # Diagnostic only: the official comparison pins the linger at 0. Raising it here measures
        # how much of the single-key cost a larger group-commit batch could amortize.
        env["CAIRN_META_GROUP_COMMIT_LINGER_MICROS"] = str(args.linger_us)
    bootstrap = subprocess.run((args.cairn, "bootstrap"), env=env, capture_output=True,
                               timeout=30)
    if bootstrap.returncode:
        shutil.rmtree(arm)
        raise RuntimeError(f"Cairn bootstrap failed: {bootstrap.stderr[-300:]!r}")
    log = open(arm / "server.log", "wb")
    proc = subprocess.Popen((args.cairn, "serve"), env=env, stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)
    samples = []
    sampling = threading.Event()
    stop = threading.Event()

    def observe():
        # Only the scored delete window: prefill and the metrics settle are different workloads.
        while not stop.wait(0.1):
            if not sampling.is_set():
                continue
            try:
                _, values = scrape(port)
            except OSError:
                continue
            samples.append({
                "at": time.monotonic(),
                "queue_depth": values.get("cairn_writer_queue_depth"),
                "wal_bytes": values.get("cairn_wal_bytes"),
                "overlay_pending": values.get("cairn_overlay_pending"),
            })

    monitor = threading.Thread(target=observe, daemon=True)
    peak = {"peak_rss": 0}
    try:
        common.wait_health(f"http://{endpoint}/healthz", proc)
        monitor.start()
        common.prepare_verified_bucket(port, secret, "cairn")
        prefill_start = time.monotonic()
        prefill_errors = driver.prefill_pool(port, secret, args.objects,
                                            prefill_start + args.arm_timeout)
        if prefill_errors:
            raise RuntimeError(f"prefill failed: {prefill_errors[:3]!r}")
        prefill_seconds = time.monotonic() - prefill_start

        # Let the 15 s store-metrics tick drain prefill writer samples so the before/after
        # delta describes only the scored delete window.
        time.sleep(args.drain_settle)
        metrics_status_before, before = scrape(port)
        cpu_before = common.cpu_rss(proc.pid)
        io_before = process_io(proc.pid)
        load_before = load_average()
        delete_start = time.monotonic()
        sampling.set()
        latencies, errors = driver.delete_pool(port, secret, args.objects, args.concurrency,
                                              delete_start + args.arm_timeout)
        wall_seconds = time.monotonic() - delete_start
        sampling.clear()
        load_after = load_average()
        io_after = process_io(proc.pid)
        cpu_after = common.cpu_rss(proc.pid)
        # Wait one full metrics tick so every writer sample from the arm is drained.
        time.sleep(args.metrics_drain_seconds)
        metrics_status_after, after = scrape(port)
        checks = driver.verify_absent(port, secret, args.objects)
        empty = driver.verify_empty(port, secret)
        point = common.cpu_rss(proc.pid)
        if point:
            peak["peak_rss"] = point[2]
        deletions = len(latencies)
        result = {
            "variant": args.variant,
            "engine": "cairn",
            "binary": str(args.cairn),
            "binary_sha256": common.sha256(args.cairn),
            "pool_objects": args.objects,
            "concurrency": args.concurrency,
            "linger_us": args.linger_us,
            "successful_operations": deletions,
            "errors": len(errors),
            "error_details": errors,
            "objects_per_second": deletions / wall_seconds if wall_seconds else None,
            "wall_seconds": wall_seconds,
            "prefill_seconds": prefill_seconds,
            "latency": driver.latency_summary(latencies),
            "deletion_spot_check": checks,
            "bucket_empty": empty,
            "load_average": {"before": load_before, "after": load_after},
            "server_cpu_seconds": (cpu_after[0] - cpu_before[0]
                                   if cpu_before and cpu_after else None),
            "server_cpu_ms_per_object": ((cpu_after[0] - cpu_before[0]) / deletions * 1000
                                         if cpu_before and cpu_after and deletions else None),
            "server_io": io_delta(io_before, io_after),
            "peak_rss": peak["peak_rss"],
            "metrics_status": [metrics_status_before, metrics_status_after],
            "writer": stage_report(before, after),
            "wal_bytes_after": after.get("cairn_wal_bytes"),
            "wal_checkpoints": counter_delta(before, after, "cairn_wal_checkpoints_total"),
            "wal_checkpoints_busy": counter_delta(
                before, after, "cairn_wal_checkpoints_busy_total"),
            "wal_checkpointed_frames": counter_delta(
                before, after, "cairn_wal_checkpointed_frames_total"),
            "queue_depth_samples": len(samples),
            "queue_depth_max": max((s["queue_depth"] for s in samples
                                    if s["queue_depth"] is not None), default=None),
            "wal_bytes_window": {
                "min": min((s["wal_bytes"] for s in samples
                            if s["wal_bytes"] is not None), default=None),
                "max": max((s["wal_bytes"] for s in samples
                            if s["wal_bytes"] is not None), default=None),
                "last": next((s["wal_bytes"] for s in reversed(samples)
                              if s["wal_bytes"] is not None), None),
            },
            "host_capacity": capacity_before,
        }
        result["status"] = "PASS" if (
            deletions == args.objects and not errors
            and empty["status"] == 200 and empty["empty"]
            and all(check["status"] == 404 for check in checks)
        ) else "INCONCLUSIVE"
        return result
    finally:
        stop.set()
        if monitor.is_alive():
            monitor.join(timeout=5)
        common.stop_group(proc)
        log.close()
        shutil.rmtree(arm)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cairn", type=Path, required=True)
    parser.add_argument("--variant", default="candidate")
    parser.add_argument("--objects", type=int, default=6_000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--port", type=int, default=19800)
    parser.add_argument("--arm-timeout", type=int, default=300)
    parser.add_argument("--linger-us", type=int, default=0,
                        help="diagnostic group-commit linger; the official comparison pins 0")
    parser.add_argument("--drain-settle", type=float, default=17.0)
    parser.add_argument("--metrics-drain-seconds", type=float, default=17.0)
    parser.add_argument("--max-seconds", type=int, default=900)
    args = parser.parse_args()
    if not (1_000 <= args.objects <= 100_000 and 1 <= args.concurrency <= 32
            and 0 <= args.linger_us <= 1000 and 30 <= args.arm_timeout <= 900
            and 0 <= args.drain_settle <= 20 and 0 <= args.metrics_drain_seconds <= 20
            and 60 <= args.max_seconds <= 1800):
        parser.error("one or more limits are outside the bounded range")
    args.root = args.root.resolve(strict=True)
    if not args.root.is_dir() or args.root.is_symlink() or not str(args.root).startswith(
            "/var/tmp/cairn-performance-"):
        parser.error("root must be a private /var/tmp/cairn-performance-* directory")
    if not args.cairn.is_file() or not os.access(args.cairn, os.X_OK):
        parser.error(f"not executable: {args.cairn}")
    began = time.monotonic()
    result = run_arm(args)
    result["elapsed_seconds"] = time.monotonic() - began
    output = args.root / f"cairn-delete-diag-{args.variant}.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in (
        "variant", "status", "objects_per_second", "latency", "writer",
        "server_cpu_ms_per_object", "wal_bytes_after", "wal_checkpoints",
        "wal_checkpoints_busy", "queue_depth_max", "load_average")}, indent=2))
    print(f"{output}")


if __name__ == "__main__":
    main()
