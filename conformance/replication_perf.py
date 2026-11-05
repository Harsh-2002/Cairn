#!/usr/bin/env python3
"""Controlled two-node replication throughput benchmark (ARCH 20, 30).

Stands up a TARGET Cairn and a SOURCE Cairn on 127.0.0.1, creates two versioned
buckets on the source -- `replbench` with an enabled replication rule and
`ctrlbench` with none -- and drives Warp `put` against each in interleaved pairs.
The control arm measures the same source write path minus the durable outbox
enqueue; the replicated arm additionally measures the worker (shipped bytes and
objects, lag, queue depth, source CPU per shipped MiB).

Two object shapes are kept strictly separate and never pooled:

  * 1 MiB at concurrency 16;
  * 64 MiB at concurrency 4.

Both data directories live on /var/tmp (XFS on this host). The driver refuses to
weaken durability: both nodes run with CAIRN_META_SYNCHRONOUS=full, one shard and
zero group-commit linger. Run it through conformance/replication_perf.sh, which
takes the host-wide benchmark lock (/tmp/cairn-deep-perf.lock); never run it
alongside another storage benchmark.

Measurement definitions (all reported per arm):

  source PUT obj/s, MiB/s   exact source version/logical-byte deltas over the
                            raw load window (Warp v1.8 `Ran` + 4 s; bucket
                            preparation and report printing wrote nothing),
                            cross-checked against Warp's scored report; never
                            merged across shapes.
  shipped obj/s, MiB/s      cairn_replication_completed_total and
                            cairn_replication_bytes_total deltas over the same
                            window (work delivered while load was offered).
  lag max / p95             live GET /api/v1/replication/summary samples every
                            0.5 s. The /metrics lag gauge only refreshes on the
                            15 s store-metrics tick, so it is recorded separately
                            and never used for the percentile.
  unreplicated max          live pending + claimed + failed samples.
  source CPU s/MiB          /proc/<pid>/stat utime+stime delta divided by the
                            shipped MiB in the same window.
  drain seconds             post-load time until the outbox and the asynchronous
                            index overlay are both idle, including a 2 s quiet
                            settle; the drain-active rate subtracts that settle
                            and the drain-inclusive sustained rate is also
                            reported, separately labelled.

Control arms must show zero replication activity; replicated arms must end fully
drained with zero failed entries, matching source writes/target versions, and a
sampled byte-for-byte + version-id verification.

Usage:
  conformance/replication_perf.sh                     # locked, 3 interleaved pairs
  conformance/replication_perf.sh --pairs 2 --duration 25
  conformance/replication_perf.sh --workers 16        # follow-up concurrency sweep
  conformance/replication_perf.sh --keep-data         # retain logs + stores for forensics

Exit status: 0 only when every arm is complete and every invariant holds;
non-zero on an infrastructure failure, an incomplete drain, a verification
mismatch, missing credentials, or replication errors.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import secrets
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config as BotoConfig
from botocore.credentials import Credentials

REGION = "us-east-1"
REPL_BUCKET = "replbench"
CTRL_BUCKET = "ctrlbench"
TARGET_PORT = 9101
SOURCE_PORT = 9102
SAMPLE_INTERVAL = 0.5
METRICS_TICK_SECONDS = 15.0
REPLICATION_INTERVAL_SECS = 1

# The two scored shapes. The order is fixed and each shape is scored on its own.
SHAPES = (
    ("1m", 1024 * 1024, 16),
    ("64m", 64 * 1024 * 1024, 4),
)

METRIC_KEYS = (
    "cairn_replication_bytes_total",
    "cairn_replication_completed_total",
    "cairn_replication_failed_total",
    "cairn_replication_dek_resolve_failed_total",
    "cairn_replication_lag_seconds",
    "cairn_replication_queue_depth",
    "cairn_replication_claimed",
    "cairn_replication_failed",
    "cairn_replication_unreplicated",
    "cairn_overlay_pending",
    "cairn_overlay_fold_failures_total",
)

STRICT_ENV = {
    "CAIRN_META_SYNCHRONOUS": "full",
    "CAIRN_META_SHARDS": "1",
    "CAIRN_META_GROUP_COMMIT_LINGER_MICROS": "0",
}


# ---------------------------------------------------------------------------------------
# Pure helpers (covered by conformance/test_replication_perf.py)
# ---------------------------------------------------------------------------------------


def parse_prometheus(text, names):
    """Return {series: value} for the wanted metric families.

    A series is the bare family name for an unlabeled metric and `name{labels}`
    otherwise. Values that are not finite numbers are ignored.
    """
    wanted = set(names)
    series = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        head, _, raw = line.rpartition(" ")
        if not head or not raw:
            continue
        family = head.split("{", 1)[0]
        if family not in wanted:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if math.isfinite(value):
            series[head] = value
    return series


def plain_metrics(series):
    """Collapse a parsed series map to its unlabeled family values."""
    plain = {}
    for key, value in series.items():
        plain[key.split("{", 1)[0]] = value
    return plain


def percentile(values, fraction):
    """Nearest-rank percentile over a non-empty sample; None when empty."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def parse_warp(output):
    """Extract the scored PUT block from Warp's final report."""
    text = re.sub(r"\x1b\[[0-9;]*m", "", output)
    header = re.search(r"^\s*Report: PUT\..*$", text, re.MULTILINE)
    block = text[header.start():] if header else text
    average = re.search(r"^\s*\* Average:\s*([0-9.]+)\s*MiB/s,\s*([0-9.]+)\s*obj/s",
                        block, re.MULTILINE)
    if average is not None:
        mib = float(average.group(1))
        objects = float(average.group(2))
    else:
        objects_only = re.search(r"^\s*\* Average:\s*([0-9.]+)\s*obj/s", block,
                                 re.MULTILINE)
        mib = None
        objects = float(objects_only.group(1)) if objects_only else None
    ran = re.search(r"Ran:\s*([0-9.]+)s", header.group(0) if header else "")
    reqs = re.search(r"^\s*\* Reqs:[^\n]*", block, re.MULTILINE)
    latency = {}
    if reqs is not None:
        for label, value, unit in re.findall(
                r"\b(Avg|50%|90%|99%):\s*([0-9.]+)(µs|us|ms|s)\b", reqs.group(0)):
            factor = {"µs": 0.001, "us": 0.001, "ms": 1.0, "s": 1000.0}[unit]
            latency[{"Avg": "avg", "50%": "p50", "90%": "p90",
                     "99%": "p99"}[label]] = float(value) * factor
    error_lines = [int(x) for x in re.findall(r"^\s*\*?\s*Errors:\s+(\d+)", text,
                                              re.MULTILINE)]
    inline = [int(x) for x in re.findall(r"\b(\d+) errors\b", text)]
    return {
        "objects_per_second": objects,
        "mib_per_second": mib,
        "ran_seconds": float(ran.group(1)) if ran else None,
        "request_latency_ms": latency,
        "reported_errors": max(sum(error_lines), sum(inline), text.count("<ERROR>")),
    }


def parse_bootstrap(output):
    """Parse the one-time root credentials printed by `cairn bootstrap`."""
    akid = re.search(r"Access Key Id:\s*(\S+)", output)
    secret = re.search(r"Secret Access Key:\s*(\S+)", output)
    if not akid or not secret:
        raise ValueError("bootstrap output did not contain both credentials")
    return akid.group(1), secret.group(1)


def summarize_samples(samples, phase="load"):
    """Live lag/queue statistics for one arm phase.

    `samples` are the 0.5 s records written by Sampler. Lag and unreplicated
    come from the live replication summary because the /metrics gauges only move
    on the 15 s store-metrics tick; that gauge-derived maximum is kept as a
    separate, clearly labelled field.
    """
    live = [s for s in samples if s.get("phase") == phase and "lag_s" in s]
    lags = [float(s["lag_s"]) for s in live]
    unreplicated = [int(s["pending"]) + int(s["claimed"]) + int(s["failed"])
                    for s in live]
    pending = [int(s["pending"]) for s in live]
    claimed = [int(s["claimed"]) for s in live]
    failed = [int(s["failed"]) for s in live]
    metric_lags = [float(s["metrics"]["cairn_replication_lag_seconds"])
                   for s in samples
                   if s.get("phase") == phase
                   and (s.get("metrics") or {}).get("cairn_replication_lag_seconds")
                   is not None]
    metric_queue = [float(s["metrics"]["cairn_replication_queue_depth"])
                    for s in samples
                    if s.get("phase") == phase
                    and (s.get("metrics") or {}).get("cairn_replication_queue_depth")
                    is not None]
    metric_unreplicated = [float(s["metrics"]["cairn_replication_unreplicated"])
                           for s in samples
                           if s.get("phase") == phase
                           and (s.get("metrics") or {}).get("cairn_replication_unreplicated")
                           is not None]
    return {
        "samples": len(live),
        "lag_max_s": max(lags) if lags else None,
        "lag_p95_s": percentile(lags, 0.95),
        "lag_last_s": lags[-1] if lags else None,
        "unreplicated_max": max(unreplicated) if unreplicated else None,
        "pending_max": max(pending) if pending else None,
        "claimed_max": max(claimed) if claimed else None,
        "failed_max": max(failed) if failed else None,
        "lag_metric_max_s": max(metric_lags) if metric_lags else None,
        "queue_depth_metric_max": max(metric_queue) if metric_queue else None,
        "unreplicated_metric_max": (max(metric_unreplicated)
                                    if metric_unreplicated else None),
        "metric_note": (f"/metrics gauges refresh on a {METRICS_TICK_SECONDS:.0f} s tick; "
                        "the live API lag/unreplicated fields are authoritative"),
    }


def rate(count, seconds):
    return count / seconds if seconds and seconds > 0 else None


def redact_command(command):
    """Redact credential flag values before a command is written to evidence."""
    redacted = []
    hide_next = False
    for token in command:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        redacted.append(token)
        if token in ("--access-key", "--secret-key"):
            hide_next = True
    return redacted


def load_window(wall_seconds, warp_ran_seconds):
    """The raw load window the metric deltas cover.

    Warp v1.8's report trims two 1 s segments at each boundary of the displayed
    score, so `Ran + 4 s` is the raw load window. Never exceed the measured wall.
    """
    if warp_ran_seconds is None:
        return wall_seconds
    return min(wall_seconds, warp_ran_seconds + 4.0)


def _mean(values):
    present = [v for v in values if v is not None]
    return statistics.mean(present) if present else None


def summarize_arms(arms):
    """Per-shape means plus same-pair replicated/control source-PUT ratios.

    Shapes are never pooled: the 1 MiB and 64 MiB rows have different framing,
    durability tails and replication paths. Pairs alternate arm order, so the
    ratio per pair and the mean of those ratios are the controlled signal.
    """
    summary = {}
    for shape in sorted({arm["shape"] for arm in arms}):
        rows = [arm for arm in arms if arm["shape"] == shape and "source_put" in arm]
        pair_rows = []
        for pair in sorted({row["pair"] for row in rows}):
            control = next((r for r in rows if r["pair"] == pair and r["arm"] == "control"),
                           None)
            replicated = next((r for r in rows if r["pair"] == pair
                               and r["arm"] == "replicated"), None)
            if control is None or replicated is None:
                continue
            control_rate = control["source_put"]["obj_s"]
            replicated_rate = replicated["source_put"]["obj_s"]
            control_cpu = control["cpu"].get("source_total_cpu_s_per_put")
            replicated_cpu = replicated["cpu"].get("source_total_cpu_s_per_put")
            pair_rows.append({
                "pair": pair,
                "control_first": control["index"] < replicated["index"],
                "control_put_obj_s": control_rate,
                "replicated_put_obj_s": replicated_rate,
                "replicated_over_control_put_ratio": (replicated_rate / control_rate)
                if control_rate else None,
                "control_total_cpu_s_per_put": control_cpu,
                "replicated_total_cpu_s_per_put": replicated_cpu,
                "replicated_minus_control_total_cpu_s_per_put": (
                    replicated_cpu - control_cpu)
                if control_cpu is not None and replicated_cpu is not None else None,
                "replicated_shipped_obj_s": replicated["ship"]["shipped_obj_s"],
                "replicated_shipped_mib_s": replicated["ship"]["shipped_mib_s"],
            })
        ratios = [row["replicated_over_control_put_ratio"] for row in pair_rows
                  if row["replicated_over_control_put_ratio"] is not None]
        shape_summary = {
            "arms": len(rows),
            "pairs": pair_rows,
            "replicated_over_control_put_ratio_mean": _mean(ratios),
            "source_put_obj_s_ratio_pct_mean": (100.0 * (_mean(ratios) - 1.0))
            if ratios else None,
            "replicated_minus_control_total_cpu_s_per_put_mean": _mean(
                [row["replicated_minus_control_total_cpu_s_per_put"]
                 for row in pair_rows]),
        }
        for arm_type in ("control", "replicated"):
            subset = [row for row in rows if row["arm"] == arm_type]
            shape_summary[arm_type] = {
                "source_put_obj_s_mean": _mean([r["source_put"]["obj_s"] for r in subset]),
                "source_put_mib_s_mean": _mean([r["source_put"]["mib_s"] for r in subset]),
                "source_put_p50_ms_mean": _mean(
                    [r["warp"]["request_latency_ms"].get("p50") for r in subset]),
                "source_put_p99_ms_mean": _mean(
                    [r["warp"]["request_latency_ms"].get("p99") for r in subset]),
                "shipped_obj_s_mean": _mean([r["ship"]["shipped_obj_s"] for r in subset]),
                "shipped_mib_s_mean": _mean([r["ship"]["shipped_mib_s"] for r in subset]),
                "keep_up_fraction_mean": _mean(
                    [r["ship"].get("keep_up_fraction") for r in subset]),
                "lag_max_s_max": max((r["ship"]["live"]["lag_max_s"] for r in subset
                                      if r["ship"]["live"]["lag_max_s"] is not None),
                                     default=None),
                "lag_p95_s_mean": _mean([r["ship"]["live"]["lag_p95_s"] for r in subset]),
                "unreplicated_max": max((r["ship"]["live"]["unreplicated_max"]
                                         for r in subset
                                         if r["ship"]["live"]["unreplicated_max"]
                                         is not None), default=None),
                "drain_seconds_mean": _mean([r["ship"]["drain_seconds"] for r in subset]),
                "drain_active_seconds_mean": _mean(
                    [r["ship"].get("drain_active_seconds") for r in subset]),
                "drain_shipped_obj_s_mean": _mean(
                    [r["ship"].get("drain_shipped_obj_s") for r in subset]),
                "drain_shipped_mib_s_mean": _mean(
                    [r["ship"].get("drain_shipped_mib_s") for r in subset]),
                "source_cpu_s_per_shipped_mib_mean": _mean(
                    [r["cpu"]["source_total_cpu_s_per_shipped_mib"] for r in subset]),
                "source_wall_cpu_s_per_shipped_mib_mean": _mean(
                    [r["cpu"]["source_cpu_s_per_shipped_mib"] for r in subset]),
                "target_cpu_s_per_shipped_mib_mean": _mean(
                    [r["cpu"].get("target_total_cpu_s_per_shipped_mib") for r in subset]),
                "source_cpu_s_per_put_mean": _mean(
                    [r["cpu"]["source_cpu_s_per_put"] for r in subset]),
            }
        summary[shape] = shape_summary
    return summary


# ---------------------------------------------------------------------------------------
# Process / host probes
# ---------------------------------------------------------------------------------------


def fetch_bytes(url, timeout=10.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def s3_metrics(port):
    return parse_prometheus(fetch_bytes(f"http://127.0.0.1:{port}/metrics").decode(
        "utf-8", "replace"), METRIC_KEYS)


def signed_api(port, akid, secret, path):
    url = f"http://127.0.0.1:{port}{path}"
    # S3-style SigV4 signs the payload hash from x-amz-content-sha256; without the
    # header the server's canonical request differs and every GET fails 403.
    request = AWSRequest(method="GET", url=url,
                         headers={"x-amz-content-sha256":
                                  hashlib.sha256(b"").hexdigest()})
    SigV4Auth(Credentials(akid, secret), "s3", REGION).add_auth(request)
    http_request = urllib.request.Request(url, headers=dict(request.headers), method="GET")
    with urllib.request.urlopen(http_request, timeout=15) as response:
        return json.loads(response.read())


def process_cpu_seconds(pid):
    """utime+stime from /proc/<pid>/stat, excluding reaped children."""
    data = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    fields = data[data.rfind(")") + 2:].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def process_rss_kib(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        return None
    return None


def load_average():
    with open("/proc/loadavg", "r", encoding="ascii") as handle:
        values = handle.read().split()
    return [float(v) for v in values[:3]]


def free_bytes(path):
    stats = os.statvfs(path)
    return stats.f_bavail * stats.f_frsize


def port_available(port):
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def filesystem_type(path):
    completed = subprocess.run(["stat", "-f", "-c", "%T", str(path)],
                               capture_output=True, text=True, check=False)
    return completed.stdout.strip() or "unknown"


def host_capacity():
    meminfo = Path("/proc/meminfo").read_text(encoding="ascii")
    memtotal = re.search(r"^MemTotal:\s+(\d+) kB$", meminfo, re.MULTILINE)
    swaptotal = re.search(r"^SwapTotal:\s+(\d+) kB$", meminfo, re.MULTILINE)
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", meminfo, re.MULTILINE)
    return {
        "logical_cpus": os.cpu_count(),
        "memtotal_kib": int(memtotal.group(1)) if memtotal else None,
        "swaptotal_kib": int(swaptotal.group(1)) if swaptotal else None,
        "memavailable_kib": int(available.group(1)) if available else None,
    }


def sha256_file(path):
    completed = subprocess.run(["sha256sum", str(path)], capture_output=True,
                               text=True, check=True)
    return completed.stdout.split()[0]


# ---------------------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------------------


def node_env(port, data_dir, master_key, akid, secret, extra=None):
    env = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_")}
    env.update({
        "CAIRN_DATA_DIR": str(data_dir / "data"),
        "CAIRN_DB_PATH": str(data_dir / "data" / "cairn.db"),
        "CAIRN_API_ADDR": f"127.0.0.1:{port}",
        "CAIRN_CONSOLE_ADDR": "off",
        "CAIRN_MASTER_KEY": master_key,
        "CAIRN_ROOT_ACCESS_KEY": akid,
        "CAIRN_ROOT_SECRET_KEY": secret,
        "CAIRN_LOG_LEVEL": "error",
        **STRICT_ENV,
    })
    env.update(extra or {})
    return env


def bootstrap(binary, env):
    completed = subprocess.run([binary, "bootstrap"], env=env, capture_output=True,
                               text=True, timeout=180)
    if completed.returncode != 0:
        raise RuntimeError(f"bootstrap failed: {completed.stderr.strip()[-800:]}")
    return parse_bootstrap(completed.stdout)


def start_server(binary, env, log_path):
    log = open(log_path, "wb")
    process = subprocess.Popen([binary, "serve"], env=env, stdout=log, stderr=log,
                               start_new_session=True)
    return process, log


def wait_healthy(port, process, log_path, timeout=45.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = Path(log_path).read_text(encoding="utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"server on {port} exited during startup:\n{tail}")
        try:
            fetch_bytes(f"http://127.0.0.1:{port}/healthz", timeout=2.0)
            return
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.1)
    raise RuntimeError(f"server on {port} did not become healthy in {timeout}s")


def stop_server(process, log):
    if process is not None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
    if log is not None:
        log.close()


def make_client(port, akid, secret):
    return boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=akid,
        aws_secret_access_key=secret,
        region_name=REGION,
        config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 2},
                          max_pool_connections=64, connect_timeout=30, read_timeout=180),
    )


def setup_buckets(source, target):
    """Target first (the rule ships into it), then versioning + the rule on the source.

    Both source buckets are versioned so the only difference between the control
    and replicated arms is the enabled replication rule.
    """
    target.create_bucket(Bucket=REPL_BUCKET)
    target.put_bucket_versioning(Bucket=REPL_BUCKET,
                                 VersioningConfiguration={"Status": "Enabled"})
    source.create_bucket(Bucket=REPL_BUCKET)
    source.create_bucket(Bucket=CTRL_BUCKET)
    for bucket in (REPL_BUCKET, CTRL_BUCKET):
        source.put_bucket_versioning(Bucket=bucket,
                                     VersioningConfiguration={"Status": "Enabled"})
    source.put_bucket_replication(
        Bucket=REPL_BUCKET,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::cairn:role/replication-perf",
            "Rules": [{
                "ID": "replication-perf",
                "Status": "Enabled",
                "Prefix": "",
                "Destination": {"Bucket": f"arn:aws:s3:::{REPL_BUCKET}"},
            }],
        },
    )
    if source.get_bucket_versioning(Bucket=REPL_BUCKET).get("Status") != "Enabled":
        raise RuntimeError("replbench versioning did not report Enabled")
    rules = source.get_bucket_replication(Bucket=REPL_BUCKET)["ReplicationConfiguration"]
    if not rules["Rules"] or rules["Rules"][0]["Status"] != "Enabled":
        raise RuntimeError("replbench replication rule did not report Enabled")


# ---------------------------------------------------------------------------------------
# Sampling and arm state
# ---------------------------------------------------------------------------------------


class Sampler(threading.Thread):
    """0.5 s source-side samples: /metrics, live replication summary, process CPU."""

    def __init__(self, source, source_pid, target_pid, interval=SAMPLE_INTERVAL):
        super().__init__(daemon=True)
        self.source = source
        self.source_pid = source_pid
        self.target_pid = target_pid
        self.interval = interval
        self.stop_event = threading.Event()
        self.phase = "load"
        self.samples = []
        self.errors = 0
        self._start = time.monotonic()

    def run(self):
        while not self.stop_event.is_set():
            started = time.monotonic()
            sample = {
                "t": round(started - self._start, 3),
                "unix": time.time(),
                "phase": self.phase,
            }
            try:
                plain = plain_metrics(s3_metrics(self.source["port"]))
                sample["metrics"] = {name: plain.get(name, 0.0) for name in METRIC_KEYS}
            except Exception as exc:  # noqa: BLE001 - recorded, never abort the arm
                self.errors += 1
                sample["metrics_error"] = str(exc)[:120]
            try:
                summary = signed_api(self.source["port"], self.source["akid"],
                                     self.source["secret"], "/api/v1/replication/summary")
                sample["pending"] = summary.get("pending")
                sample["claimed"] = summary.get("claimed")
                sample["failed"] = summary.get("failed")
                sample["lag_s"] = summary.get("lag_seconds")
            except Exception as exc:  # noqa: BLE001
                self.errors += 1
                sample["summary_error"] = str(exc)[:120]
            try:
                sample["cpu_source_s"] = round(process_cpu_seconds(self.source_pid), 3)
                sample["cpu_target_s"] = round(process_cpu_seconds(self.target_pid), 3)
            except (OSError, IndexError):
                pass
            self.samples.append(sample)
            wait = self.interval - (time.monotonic() - started)
            if wait > 0:
                self.stop_event.wait(wait)


def source_snapshot(source):
    plain = plain_metrics(s3_metrics(source["port"]))
    overview = signed_api(source["port"], source["akid"], source["secret"],
                          "/api/v1/overview")
    return {
        "metrics": {name: plain.get(name, 0.0) for name in METRIC_KEYS},
        "versions": overview["versions"],
        "objects": overview["objects"],
        "logical_bytes": overview["logical_bytes"],
        "physical_bytes": overview["physical_bytes"],
    }


def target_snapshot(target):
    overview = signed_api(target["port"], target["akid"], target["secret"],
                          "/api/v1/overview")
    return {
        "versions": overview["versions"],
        "objects": overview["objects"],
        "logical_bytes": overview["logical_bytes"],
        "physical_bytes": overview["physical_bytes"],
    }


def wait_drained(source, settle_seconds=2.0, timeout=240.0):
    """Wait for the outbox and the asynchronous source index overlay to go idle.

    `pending == 0` alone is not enough: protocol-3 ordinary PUTs ACK before their
    index fold, so the last writes can surface as outbox rows after Warp exits.
    Require idle counters, a stable version count, an empty overlay, and a short
    settle window so a late fold cannot be mistaken for a drained arm.
    """
    started = time.monotonic()
    deadline = started + timeout
    stability_needed = max(1, int(round(settle_seconds / 0.5)))
    stable = 0
    previous_versions = None
    last = None
    while time.monotonic() < deadline:
        try:
            summary = signed_api(source["port"], source["akid"], source["secret"],
                                 "/api/v1/replication/summary")
            overview = signed_api(source["port"], source["akid"], source["secret"],
                                  "/api/v1/overview")
            plain = plain_metrics(s3_metrics(source["port"]))
            last = {
                "pending": summary["pending"],
                "claimed": summary["claimed"],
                "failed": summary["failed"],
                "lag_seconds": summary["lag_seconds"],
                "versions": overview["versions"],
                "logical_bytes": overview["logical_bytes"],
                "overlay_pending": plain.get("cairn_overlay_pending"),
            }
            if summary["failed"] > 0:
                # A terminal failure will never drain; do not burn the full timeout.
                return {"settled": False, "seconds": time.monotonic() - started,
                        "final": last, "failed": summary["failed"]}
            idle = (summary["pending"] == 0 and summary["claimed"] == 0
                    and summary["failed"] == 0
                    and not last["overlay_pending"])
            unchanged = previous_versions == overview["versions"]
            if idle and unchanged:
                stable += 1
                if stable >= stability_needed:
                    return {"settled": True, "seconds": time.monotonic() - started,
                            "final": last}
            else:
                stable = 0
            previous_versions = overview["versions"]
        except Exception:  # noqa: BLE001 - transient scrape; keep polling to the deadline
            stable = 0
        time.sleep(0.5)
    return {"settled": False, "seconds": time.monotonic() - started, "final": last}


def wait_target_settled(target, settle_seconds=2.0, timeout=60.0):
    """Wait for the target's asynchronous replica index fold to expose every write.

    The source drain proves the outbox is empty; the destination applies each
    replica through its own ordinary write path, whose index fold is also
    asynchronous. A byte check or version count taken before the target settles
    can therefore undercount even though every replica has arrived.
    """
    started = time.monotonic()
    deadline = started + timeout
    stability_needed = max(1, int(round(settle_seconds / 0.5)))
    stable = 0
    previous_versions = None
    last = None
    while time.monotonic() < deadline:
        try:
            overview = signed_api(target["port"], target["akid"], target["secret"],
                                  "/api/v1/overview")
            plain = plain_metrics(s3_metrics(target["port"]))
            last = {
                "versions": overview["versions"],
                "logical_bytes": overview["logical_bytes"],
                "overlay_pending": plain.get("cairn_overlay_pending"),
            }
            idle = not last["overlay_pending"]
            if idle and previous_versions == overview["versions"]:
                stable += 1
                if stable >= stability_needed:
                    return {"settled": True, "seconds": time.monotonic() - started,
                            "final": last}
            else:
                stable = 0
            previous_versions = overview["versions"]
        except Exception:  # noqa: BLE001 - transient scrape; keep polling
            stable = 0
        time.sleep(0.5)
    return {"settled": False, "seconds": time.monotonic() - started, "final": last}


def sample_keys(client, bucket, limit, rng, object_bytes, max_pages=64):
    """Sample drained versions of the arm's object size, paging through the bucket.

    The bucket accumulates both shapes across a run, and `list_object_versions`
    is key-ordered rather than time-ordered, so filter on the version's logical
    size instead of taking the first page (which can be all 1 MiB versions in a
    64 MiB arm).
    """
    candidates = []
    scanned = 0
    key_marker = version_marker = None
    for _ in range(max_pages):
        kwargs = {"Bucket": bucket, "MaxKeys": 1000}
        if key_marker is not None:
            kwargs["KeyMarker"] = key_marker
            kwargs["VersionIdMarker"] = version_marker
        page = client.list_object_versions(**kwargs)
        versions = page.get("Versions", [])
        scanned += len(versions)
        candidates.extend((v["Key"], v["VersionId"]) for v in versions
                          if v.get("VersionId") and v.get("Size") == object_bytes)
        if len(candidates) >= limit * 8 or not page.get("IsTruncated"):
            break
        key_marker = page.get("NextKeyMarker")
        version_marker = page.get("NextVersionIdMarker")
        if key_marker is None:
            break
    if not candidates:
        return [], scanned
    return rng.sample(candidates, min(limit, len(candidates))), scanned


def verify_sample(source, target, bucket, limit, rng, object_bytes):
    """Byte-for-byte + version-identity check of drained versions of this shape."""
    result = {"objects": 0, "bytes": 0, "mismatches": 0, "version_id_mismatches": 0,
              "object_bytes": object_bytes, "scanned_versions": 0, "errors": []}
    keys, scanned = sample_keys(source, bucket, limit, rng, object_bytes)
    result["scanned_versions"] = scanned
    for key, version in keys:
        try:
            ours = source.get_object(Bucket=bucket, Key=key, VersionId=version)
            theirs = target.get_object(Bucket=bucket, Key=key, VersionId=version)
        except Exception as exc:  # noqa: BLE001
            result["mismatches"] += 1
            result["errors"].append(f"{key}: {str(exc)[:120]}")
            continue
        if ours.get("VersionId") != theirs.get("VersionId"):
            result["version_id_mismatches"] += 1
        ours_body = ours["Body"].read()
        theirs_body = theirs["Body"].read()
        if ours_body != theirs_body:
            result["mismatches"] += 1
            result["errors"].append(
                f"{key}: source {len(ours_body)} bytes, target {len(theirs_body)} bytes")
            continue
        result["objects"] += 1
        result["bytes"] += len(ours_body)
    return result


def run_arm(args, run_dir, index, shape_name, object_bytes, concurrency, replicated,
            pair, rng, source, target, source_proc, target_proc):
    bucket = REPL_BUCKET if replicated else CTRL_BUCKET
    arm_name = f"{index:02d}-{shape_name}-{'repl' if replicated else 'ctrl'}-pair{pair}"
    arm_dir = run_dir / arm_name
    arm_dir.mkdir(parents=True, exist_ok=False)
    print(f"\n=== arm {index}: {shape_name} {object_bytes // 1048576} MiB "
          f"c={concurrency} {'replicated' if replicated else 'control'} (pair {pair}) ===",
          flush=True)

    if free_bytes(run_dir) < args.min_free_bytes:
        raise RuntimeError(f"refusing to arm: free space under {args.min_free_bytes} bytes")

    before = source_snapshot(source)
    before_cpu = process_cpu_seconds(source_proc.pid)
    target_before = target_snapshot(target)
    target_cpu_before = process_cpu_seconds(target_proc.pid)
    load_start = load_average()
    free_start = free_bytes(run_dir)

    sampler = Sampler(source, source_proc.pid, target_proc.pid)
    sampler.start()
    wall_start = time.monotonic()
    unix_start = time.time()
    command = [
        args.warp, "put",
        "--host", f"127.0.0.1:{SOURCE_PORT}",
        "--access-key", source["akid"],
        "--secret-key", source["secret"],
        "--region", REGION,
        "--lookup", "path",
        "--bucket", bucket,
        "--obj.size", f"{object_bytes // 1048576}MiB",
        "--concurrent", str(concurrency),
        "--duration", f"{args.duration}s",
        "--disable-multipart",
        "--noclear",
        "--no-color",
    ]
    completed = subprocess.run(command, capture_output=True, text=True,
                               timeout=args.duration + 600)
    wall_seconds = time.monotonic() - wall_start
    unix_end = time.time()
    sampler.phase = "drain"
    warp_result = parse_warp(completed.stdout + completed.stderr)
    # Warp v1.8 trims two 1 s segments at each boundary of the displayed score,
    # so its Report `Ran` is the trimmed interval and the raw load window is
    # Ran + 4 s. Use that window for the metric-delta rates: the wall also
    # contains bucket preparation and report printing, which wrote nothing.
    load_seconds = load_window(wall_seconds, warp_result["ran_seconds"])
    (arm_dir / "warp.txt").write_text(completed.stdout + completed.stderr, encoding="utf-8")

    wall_after = source_snapshot(source)
    wall_cpu = process_cpu_seconds(source_proc.pid)
    target_wall_cpu = process_cpu_seconds(target_proc.pid)

    drain = wait_drained(source, timeout=args.drain_timeout)
    # Source-side totals stop at the source drain; the target settle below is a
    # destination concern and must not inflate the source's CPU-per-MiB figure.
    final = source_snapshot(source)
    final_cpu = process_cpu_seconds(source_proc.pid)
    if replicated:
        target_settle = wait_target_settled(target)
    else:
        target_settle = {"settled": True, "seconds": 0.0, "final": None}
    sampler.stop_event.set()
    sampler.join(timeout=10)

    target_final = target_snapshot(target)
    target_final_cpu = process_cpu_seconds(target_proc.pid)

    put_objects = final["versions"] - before["versions"]
    put_bytes = final["logical_bytes"] - before["logical_bytes"]
    completed_wall = (wall_after["metrics"]["cairn_replication_completed_total"]
                      - before["metrics"]["cairn_replication_completed_total"])
    shipped_bytes_wall = (wall_after["metrics"]["cairn_replication_bytes_total"]
                          - before["metrics"]["cairn_replication_bytes_total"])
    completed_final = (final["metrics"]["cairn_replication_completed_total"]
                       - before["metrics"]["cairn_replication_completed_total"])
    shipped_bytes_final = (final["metrics"]["cairn_replication_bytes_total"]
                           - before["metrics"]["cairn_replication_bytes_total"])
    failed = (final["metrics"]["cairn_replication_failed_total"]
              - before["metrics"]["cairn_replication_failed_total"])
    dek_failed = (final["metrics"]["cairn_replication_dek_resolve_failed_total"]
                  - before["metrics"]["cairn_replication_dek_resolve_failed_total"])
    fold_failures = (final["metrics"]["cairn_overlay_fold_failures_total"]
                     - before["metrics"]["cairn_overlay_fold_failures_total"])
    source_cpu_wall = wall_cpu - before_cpu
    source_cpu_total = final_cpu - before_cpu
    target_cpu_wall = target_wall_cpu - target_cpu_before
    target_cpu_total = target_final_cpu - target_cpu_before

    live = summarize_samples(sampler.samples, "load")
    drain_live = summarize_samples(sampler.samples, "drain")

    verification = None
    if replicated:
        limit = 4 if object_bytes <= 1024 * 1024 else 2
        verification = verify_sample(source["client"], target["client"], REPL_BUCKET,
                                     limit, rng, object_bytes)
    target_versions = target_final["versions"] - target_before["versions"]
    target_bytes = target_final["logical_bytes"] - target_before["logical_bytes"]

    checks = {
        "warp_exit_zero": completed.returncode == 0,
        "warp_zero_errors": warp_result["reported_errors"] == 0,
        "warp_rate_parsed": (warp_result["objects_per_second"] is not None
                             and warp_result["mib_per_second"] is not None),
        "drain_settled": drain["settled"],
        "target_settled": target_settle["settled"],
        "live_samples_present": live["samples"] > 0,
        "failed_zero": failed == 0,
        "dek_failures_zero": dek_failed == 0,
        "index_fold_failures_zero": fold_failures == 0,
        "sample_errors_zero": sampler.errors == 0,
    }
    if replicated:
        checks.update({
            "completed_matches_writes": completed_final == put_objects,
            "shipped_bytes_match": shipped_bytes_final == put_bytes,
            "target_versions_match": target_versions == put_objects,
            "target_bytes_match": target_bytes == shipped_bytes_final,
            "verification_clean": (verification is not None
                                   and verification["objects"] > 0
                                   and verification["bytes"] == verification["objects"]
                                   * object_bytes
                                   and verification["mismatches"] == 0
                                   and verification["version_id_mismatches"] == 0),
        })
    else:
        checks.update({
            "control_no_outbox": completed_final == 0 and shipped_bytes_final == 0,
            "control_target_unchanged": target_versions == 0,
        })

    if not checks["warp_exit_zero"]:
        print(completed.stderr[-1200:] or completed.stdout[-1200:], file=sys.stderr)

    arm = {
        "index": index,
        "shape": shape_name,
        "object_bytes": object_bytes,
        "concurrency": concurrency,
        "arm": "replicated" if replicated else "control",
        "pair": pair,
        "bucket": bucket,
        "started_unix": unix_start,
        "ended_unix": unix_end,
        "wall_seconds": wall_seconds,
        "load_seconds": load_seconds,
        "warp": warp_result,
        "command": redact_command(command),
        "source_put": {
            "objects": put_objects,
            "logical_bytes": put_bytes,
            "obj_s": rate(put_objects, load_seconds),
            "mib_s": (put_bytes / 1048576 / load_seconds) if put_bytes else 0.0,
            "warp_obj_s": warp_result["objects_per_second"],
            "warp_mib_s": warp_result["mib_per_second"],
        },
        "ship": {
            "completed_during_wall": completed_wall,
            "bytes_during_wall": shipped_bytes_wall,
            "shipped_obj_s": rate(completed_wall, load_seconds),
            "shipped_mib_s": (shipped_bytes_wall / 1048576 / load_seconds)
            if shipped_bytes_wall else 0.0,
            "keep_up_fraction": ((shipped_bytes_wall / 1048576 / load_seconds)
                                 / (put_bytes / 1048576 / load_seconds))
            if replicated and shipped_bytes_wall and put_bytes else None,
            "completed_after_drain": completed_final,
            "bytes_after_drain": shipped_bytes_final,
            "drain_seconds": drain["seconds"],
            "drain_active_seconds": max(drain["seconds"] - 2.0, 0.5),
            "drain_completed": completed_final - completed_wall,
            "drain_bytes": shipped_bytes_final - shipped_bytes_wall,
            "drain_shipped_obj_s": rate(completed_final - completed_wall,
                                        max(drain["seconds"] - 2.0, 0.5)),
            "drain_shipped_mib_s": ((shipped_bytes_final - shipped_bytes_wall)
                                    / 1048576 / max(drain["seconds"] - 2.0, 0.5))
            if drain["seconds"] else None,
            "sustained_mib_s": (shipped_bytes_final / 1048576
                                / (load_seconds + drain["seconds"]))
            if shipped_bytes_final else 0.0,
            "failed_delta": failed,
            "dek_failed_delta": dek_failed,
            "index_fold_failures_delta": fold_failures,
            "live": live,
            "drain_live": drain_live,
            "drain_final": drain.get("final"),
            "target_settle_seconds": target_settle["seconds"],
            "target_settle_final": target_settle.get("final"),
        },
        "cpu": {
            "source_cpu_s": source_cpu_wall,
            "source_cpu_s_per_shipped_mib": (source_cpu_wall * 1048576
                                             / shipped_bytes_wall)
            if shipped_bytes_wall else None,
            "source_total_cpu_s": source_cpu_total,
            "source_total_cpu_s_per_shipped_mib": (source_cpu_total * 1048576
                                                   / shipped_bytes_final)
            if shipped_bytes_final else None,
            "source_cpu_s_per_put": rate(source_cpu_wall, put_objects),
            "source_total_cpu_s_per_put": rate(source_cpu_total, put_objects),
            "target_cpu_s": target_cpu_wall,
            "target_cpu_s_per_shipped_mib": (target_cpu_wall * 1048576
                                             / shipped_bytes_wall)
            if shipped_bytes_wall else None,
            "target_total_cpu_s": target_cpu_total,
            "target_total_cpu_s_per_shipped_mib": (target_cpu_total * 1048576
                                                   / shipped_bytes_final)
            if shipped_bytes_final else None,
            "source_rss_kib": process_rss_kib(source_proc.pid),
        },
        "target": {
            "versions_delta": target_versions,
            "logical_bytes_delta": target_bytes,
            "objects_delta": target_final["objects"] - target_before["objects"],
            "versions_before": target_before["versions"],
            "versions_after": target_final["versions"],
        },
        "verification": verification,
        "sample_series": [{
            "t": sample.get("t"),
            "phase": sample.get("phase"),
            "lag_s": sample.get("lag_s"),
            "pending": sample.get("pending"),
            "claimed": sample.get("claimed"),
            "failed": sample.get("failed"),
            "queue_depth": (sample.get("metrics") or {}).get(
                "cairn_replication_queue_depth"),
            "unreplicated": (sample.get("metrics") or {}).get(
                "cairn_replication_unreplicated"),
            "cpu_source_s": sample.get("cpu_source_s"),
        } for sample in sampler.samples if sample.get("t") is not None],
        "metrics_before": before["metrics"],
        "metrics_after_wall": wall_after["metrics"],
        "metrics_final": final["metrics"],
        "host": {
            "loadavg_start": load_start,
            "loadavg_end": load_average(),
            "free_bytes_start": free_start,
            "free_bytes_end": free_bytes(run_dir),
        },
        "checks": checks,
        "ok": all(checks.values()),
    }
    print(f"  put {arm['source_put']['objects']} obj, "
          f"{arm['source_put']['obj_s']:.1f} obj/s, "
          f"{arm['source_put']['mib_s']:.1f} MiB/s | "
          f"shipped {arm['ship']['shipped_obj_s'] or 0:.1f} obj/s, "
          f"{arm['ship']['shipped_mib_s']:.1f} MiB/s "
          f"(drain {arm['ship']['drain_shipped_obj_s'] or 0:.1f} obj/s) | "
          f"lag max {live['lag_max_s']} s p95 {live['lag_p95_s']} s | "
          f"drain {drain['seconds']:.1f} s | ok={arm['ok']}", flush=True)
    return arm


# ---------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", default="/home/dev/Cairn/target/release/cairn")
    parser.add_argument("--warp", default="/tmp/cairn-perf-tools/warp")
    parser.add_argument("--root", default="/var/tmp/cairn-replication-perf")
    parser.add_argument("--output", required=True,
                        help="JSON evidence path, e.g. "
                             "docs/performance-evidence/2026-11/replication-perf.json")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--workers", type=int, default=4,
                        help="replication worker loops; sets "
                             "CAIRN_REPLICATION_WORKER_CONCURRENCY (1-64, default 4)")
    parser.add_argument("--shape", action="append", choices=[s[0] for s in SHAPES],
                        help="restrict to one or more shapes (default: both)")
    parser.add_argument("--warmup", type=int, default=5,
                        help="unscored Warp seconds per bucket per shape before the pairs")
    parser.add_argument("--drain-timeout", type=int, default=600)
    parser.add_argument("--min-free-bytes", type=int, default=40 * 1024 ** 3)
    parser.add_argument("--keep-data", action="store_true",
                        help="keep the two stores and server logs for forensics")
    args = parser.parse_args()

    if not os.access(args.binary, os.X_OK):
        raise SystemExit(f"binary not executable: {args.binary}")
    if not os.access(args.warp, os.X_OK):
        raise SystemExit(f"warp not executable: {args.warp}")
    if args.duration < 5 or args.pairs < 1:
        raise SystemExit("--duration must be >= 5 and --pairs >= 1")
    if not 1 <= args.workers <= 64:
        raise SystemExit("--workers must be between 1 and 64")
    for port in (TARGET_PORT, SOURCE_PORT):
        if not port_available(port):
            raise SystemExit(f"port {port} is already in use; refusing to disturb it")

    output = Path(args.output).resolve()
    root = Path(args.root).resolve()
    run_id = time.strftime("%Y%m%d-%H%M%S")
    run_dir = root / run_id
    if root.exists():
        shutil.rmtree(root)
    run_dir.mkdir(parents=True)
    print(f"run dir: {run_dir} ({filesystem_type(run_dir)})", flush=True)

    capacity = host_capacity()
    load_start = load_average()
    free_start = free_bytes(run_dir)

    target_proc = source_proc = None
    target_log = source_log = None
    interrupted = False

    def terminate(*_):
        nonlocal interrupted
        interrupted = True
        raise SystemExit("interrupted")

    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGTERM, terminate)

    success = False
    try:
        target_akid = "perftarget" + secrets.token_hex(8)
        target_secret = secrets.token_hex(24)
        source_akid = "perfsource" + secrets.token_hex(8)
        source_secret = secrets.token_hex(24)

        target_env = node_env(TARGET_PORT, run_dir / "target", secrets.token_hex(32),
                              target_akid, target_secret)
        source_env = node_env(SOURCE_PORT, run_dir / "source", secrets.token_hex(32),
                              source_akid, source_secret)

        print("bootstrapping target and source", flush=True)
        if bootstrap(args.binary, target_env) != (target_akid, target_secret):
            raise RuntimeError("target bootstrap credentials did not match the pinned root")
        if bootstrap(args.binary, source_env) != (source_akid, source_secret):
            raise RuntimeError("source bootstrap credentials did not match the pinned root")

        source_env.update({
            "CAIRN_REPLICATION_ENDPOINT": f"http://127.0.0.1:{TARGET_PORT}",
            "CAIRN_REPLICATION_ACCESS_KEY": target_akid,
            "CAIRN_REPLICATION_SECRET": target_secret,
            "CAIRN_REPLICATION_REGION": REGION,
            "CAIRN_ALLOW_INTERNAL_ENDPOINTS": "true",
            "CAIRN_REPLICATION_INTERVAL_SECS": str(REPLICATION_INTERVAL_SECS),
            "CAIRN_REPLICATION_WORKER_CONCURRENCY": str(args.workers),
            "CAIRN_REPLICATION_BATCH_SIZE": "64",
            "CAIRN_REPLICATION_BUFFER_BUDGET_BYTES": str(2 * 1024 ** 3),
        })

        target_proc, target_log = start_server(args.binary, target_env,
                                               run_dir / "target.log")
        source_proc, source_log = start_server(args.binary, source_env,
                                               run_dir / "source.log")
        wait_healthy(TARGET_PORT, target_proc, run_dir / "target.log")
        wait_healthy(SOURCE_PORT, source_proc, run_dir / "source.log")
        print("both nodes healthy", flush=True)

        target = {"port": TARGET_PORT, "akid": target_akid, "secret": target_secret,
                  "client": make_client(TARGET_PORT, target_akid, target_secret)}
        source = {"port": SOURCE_PORT, "akid": source_akid, "secret": source_secret,
                  "client": make_client(SOURCE_PORT, source_akid, source_secret)}
        setup_buckets(source["client"], target["client"])
        print(f"buckets ready: {REPL_BUCKET} (replicated), {CTRL_BUCKET} (control)",
              flush=True)

        shapes = [s for s in SHAPES if not args.shape or s[0] in args.shape]
        rng = random.Random(20261105)
        arms = []
        index = 0
        fatal_error = None
        for shape_name, object_bytes, concurrency in shapes:
            if args.warmup:
                for bucket, replicated in ((CTRL_BUCKET, False), (REPL_BUCKET, True)):
                    print(f"warmup {shape_name} "
                          f"{'replicated' if replicated else 'control'} {args.warmup}s",
                          flush=True)
                    warm = subprocess.run([
                        args.warp, "put", "--host", f"127.0.0.1:{SOURCE_PORT}",
                        "--access-key", source_akid, "--secret-key", source_secret,
                        "--region", REGION, "--lookup", "path", "--bucket", bucket,
                        "--obj.size", f"{object_bytes // 1048576}MiB",
                        "--concurrent", str(concurrency), "--duration",
                        f"{args.warmup}s", "--disable-multipart", "--noclear",
                        "--no-color",
                    ], capture_output=True, text=True, timeout=args.warmup + 300)
                    if warm.returncode != 0:
                        raise RuntimeError(f"warmup failed: {warm.stderr[-800:]}")
                    drain = wait_drained(source, timeout=args.drain_timeout)
                    if not drain["settled"]:
                        raise RuntimeError("warmup did not drain; refusing to score")
                    if replicated:
                        settle = wait_target_settled(target)
                        if not settle["settled"]:
                            raise RuntimeError("warmup replicas did not settle on the target")

            for pair in range(1, args.pairs + 1):
                # Alternate which arm runs first so a monotonic host drift does not
                # favour one side across the whole shape.
                order = (False, True) if pair % 2 else (True, False)
                stop_shape = False
                for replicated in order:
                    index += 1
                    try:
                        arm = run_arm(args, run_dir, index, shape_name, object_bytes,
                                      concurrency, replicated, pair, rng, source,
                                      target, source_proc, target_proc)
                    except Exception as exc:  # noqa: BLE001 - retain partial evidence
                        fatal_error = f"{type(exc).__name__}: {exc}"
                        arms.append({
                            "index": index,
                            "shape": shape_name,
                            "object_bytes": object_bytes,
                            "concurrency": concurrency,
                            "arm": "replicated" if replicated else "control",
                            "pair": pair,
                            "error": fatal_error,
                            "traceback": traceback.format_exc()[-4000:],
                            "ok": False,
                        })
                        print(f"  ARM ERROR: {fatal_error}", file=sys.stderr)
                        stop_shape = True
                        break
                    arms.append(arm)
                    if not arm["ok"]:
                        failed_checks = [k for k, v in arm["checks"].items() if not v]
                        print(f"  ARM FAILED: {failed_checks}", flush=True)
                    if not arm["checks"]["warp_exit_zero"] or not arm["checks"][
                            "warp_rate_parsed"]:
                        # The workload itself did not run; later arms cannot be scored.
                        stop_shape = True
                        break
                if stop_shape:
                    break
            if fatal_error:
                break

        warp_version = subprocess.run([args.warp, "--version"], capture_output=True,
                                       text=True, check=False).stdout.strip()
        document = {
            "schema": 1,
            "benchmark": "replication-throughput",
            "date": time.strftime("%Y-%m-%d"),
            "run_id": run_id,
            "run_dir": str(run_dir),
            "binary": {"path": args.binary, "sha256": sha256_file(args.binary)},
            "warp": {
                "path": args.warp,
                "version": warp_version.splitlines()[0] if warp_version else None,
                "sha256": sha256_file(args.warp),
            },
            "host": {
                "capacity": capacity,
                "capacity_end": host_capacity(),
                "filesystem": filesystem_type(run_dir),
                "data_root": str(run_dir),
                "loadavg_start": load_start,
                "loadavg_end": load_average(),
                "free_bytes_start": free_start,
                "free_bytes_end": free_bytes(run_dir),
                "caveat": ("two nodes plus Warp and the sampler share this host; absolute "
                           "rates move with host load, the control/replicated pairing is "
                           "the controlled signal"),
            },
            "settings": {
                "duration_seconds": args.duration,
                "pairs": args.pairs,
                "warmup_seconds": args.warmup,
                "sample_interval_seconds": SAMPLE_INTERVAL,
                "replication_interval_secs": REPLICATION_INTERVAL_SECS,
                "replication_worker_concurrency": args.workers,
                "replication_workers_env": "CAIRN_REPLICATION_WORKER_CONCURRENCY",
                "replication_batch_size": 64,
                "replication_buffer_budget_bytes": 2 * 1024 ** 3,
                "warp_flags": {"disable_multipart": True,
                               "client_sha256_payload": "default (enabled)"},
                "replication_read_path": ("SHA-256 first pass, then the immutable source "
                                          "version range is reopened and streamed"),
                "strict": STRICT_ENV,
                "buckets": {"replicated": REPL_BUCKET, "control": CTRL_BUCKET},
                "shapes": [{"name": n, "object_bytes": b, "concurrency": c}
                           for n, b, c in shapes],
            },
            "arms": arms,
            "summary": summarize_arms(arms),
            "ok": all(arm["ok"] for arm in arms),
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        print(f"\nwrote {output}", flush=True)
        if not document["ok"]:
            print("benchmark FAILED one or more arm checks", file=sys.stderr)
            return 1
        success = True
        return 0
    finally:
        stop_server(target_proc, target_log)
        stop_server(source_proc, source_log)
        if not args.keep_data and success and not interrupted:
            shutil.rmtree(root, ignore_errors=True)
        elif root.exists():
            print(f"kept stores/logs at {run_dir}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
