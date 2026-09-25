#!/usr/bin/env python3
"""Bounded, sequential Cairn/RustFS Warp comparison on one host.

This is a diagnostic macro benchmark, not a durability-equivalence proof or a
production-format adoption gate. See docs/performance-execution-2026-09.md.
"""

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import math
import os
import re
import secrets
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from sync_probe import initialize as initialize_sync_probe
from sync_probe import delta as sync_probe_delta
from sync_probe import read as read_sync_probe


CASES = (
    ("put_4k", "put", "4KiB", 32, ()),
    ("put_1m", "put", "1MiB", 16, ()),
    ("get_4k", "get", "4KiB", 16, ("--objects", "1000")),
    ("get_1m", "get", "1MiB", 16, ("--objects", "64")),
    ("head_4k", "stat", "4KiB", 16, ("--objects", "1000")),
    ("list_4k", "list", "4KiB", 16, ("--objects", "1000")),
    ("delete_4k", "delete", "4KiB", 16, ("--objects", "12000")),
    ("mixed_1m", "mixed", "1MiB", 16, ("--objects", "100", "--get-distrib", "45",
                                           "--stat-distrib", "30", "--put-distrib", "15",
                                           "--delete-distrib", "10")),
    ("multipart_5m", "multipart", "5MiB", 16, ("--parts", "100", "--part.size", "5MiB")),
)
BUCKET = "cairn-benchmark"
ACCESS = "benchaccess"
STRICT_SETTINGS = {
    "cairn": {"CAIRN_META_SYNCHRONOUS": "full", "CAIRN_META_SHARDS": "1",
              "CAIRN_META_GROUP_COMMIT_LINGER_MICROS": "0"},
    "rustfs": {"RUSTFS_DURABILITY_MODE": "strict",
               "RUSTFS_NEW_BUCKET_DURABILITY_MODE": "strict",
               "RUSTFS_DRIVE_SYNC_ENABLE": "true"},
}
MAX_CENSUS_PROCESSES = 4096
PROCESS_IO_FIELDS = ("rchar", "wchar", "syscr", "syscw", "read_bytes",
                     "write_bytes")
MAX_WARP_BENCHDATA_BYTES = 32_000_000
MAX_WARP_AGGREGATE_BYTES = 1_000_000


def parse_host_capacity(meminfo, logical_cpus):
    """Record stable host capacity, not available memory or page-cache occupancy."""
    values = {}
    for name in ("MemTotal", "SwapTotal"):
        match = re.search(rf"^{name}:\s+(\d+) kB$", meminfo, re.MULTILINE)
        if match is None:
            raise ValueError(f"missing {name} in /proc/meminfo")
        values[f"{name.lower()}_kib"] = int(match.group(1))
    if values["memtotal_kib"] <= 0 or not isinstance(logical_cpus, int) or logical_cpus <= 0:
        raise ValueError("invalid host capacity")
    values["logical_cpus"] = logical_cpus
    return values


def host_capacity():
    return parse_host_capacity(Path("/proc/meminfo").read_text(), os.cpu_count())


def capacity_compatible(before, after, allow_ballooning=False):
    """Keep fixed CPU/swap checks; tolerate bounded VM RAM ballooning only on opt-in."""
    if not allow_ballooning:
        return before == after
    if not isinstance(before, dict) or not isinstance(after, dict):
        return False
    if (before.get("logical_cpus") != after.get("logical_cpus")
            or before.get("swaptotal_kib") != after.get("swaptotal_kib")
            or before.get("logical_cpus", 0) <= 0
            or before.get("swaptotal_kib", -1) < 0):
        return False
    first = before.get("memtotal_kib")
    second = after.get("memtotal_kib")
    return (type(first) is int and type(second) is int and first > 0
            and second > 0 and abs(second - first) <= first // 10)


def warp_benchdata_paths(arm):
    """Match the output suffix that Warp v1.8 appends to --benchdata."""
    base = arm / "warp"
    return base, arm / "warp.json.zst"


def put_timeline_valid(operation, timeline):
    """A scored PUT arm needs the exact analyzer interval and score-parity proof."""
    return operation != "put" or timeline["status"].startswith(
        "captured source-derived Warp scored interval with report-score parity")


def server_environment(engine, volume, endpoint, secret, master):
    """Pin relevant child configuration; never inherit another storage-mode override."""
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("CAIRN_", "RUSTFS_", "AWS_", "WARP_"))
           and key not in ("LD_PRELOAD", "SYNC_PROBE_FILE")}
    if engine == "cairn":
        env.update(CAIRN_DATA_DIR=str(volume), CAIRN_DB_PATH=str(volume / "cairn.db"),
                   CAIRN_MASTER_KEY=master, CAIRN_ROOT_ACCESS_KEY=ACCESS,
                   CAIRN_ROOT_SECRET_KEY=secret, CAIRN_API_ADDR=endpoint,
                   CAIRN_CONSOLE_ADDR="off", CAIRN_LOG_LEVEL="error",
                   CAIRN_UPDATE_CHECK_ENABLED="false")
    elif engine == "rustfs":
        env.update(RUSTFS_ACCESS_KEY=ACCESS, RUSTFS_SECRET_KEY=secret,
                   RUSTFS_CONSOLE_ENABLE="false", RUSTFS_REGION="us-east-1",
                   RUSTFS_ADDRESS=endpoint)
    else:
        raise ValueError(f"unsupported benchmark engine: {engine}")
    env.update(STRICT_SETTINGS[engine])
    return env


def signed_request(port, secret, method, path, body=b""):
    """Minimal SigV4 request for bucket preparation and RustFS mode verification."""
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    body_hash = hashlib.sha256(body).hexdigest()
    headers = {"host": f"127.0.0.1:{port}", "x-amz-content-sha256": body_hash,
               "x-amz-date": stamp}
    names = ";".join(sorted(headers))
    canonical = "\n".join((method, path, "",
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
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        data = response.read()
        if response.status not in (200, 204):
            raise RuntimeError(f"{method} {path} returned HTTP {response.status}: {data[:200]!r}")
        return data
    finally:
        connection.close()


def verify_rustfs_durability(port, secret):
    payload = signed_request(port, secret, "GET",
                             f"/rustfs/admin/v3/bucket-durability/{BUCKET}")
    try:
        state = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as error:
        raise RuntimeError("RustFS bucket durability readback is not JSON") from error
    if not isinstance(state, dict) or state.get("bucket") != BUCKET or state.get("mode") != "strict":
        raise RuntimeError(f"RustFS bucket durability is not explicit strict: {state!r}")
    return state


def prepare_verified_bucket(port, secret, engine):
    # RustFS can report health before S3 quorum/readiness is established.
    deadline = time.monotonic() + 30
    while True:
        try:
            signed_request(port, secret, "PUT", f"/{BUCKET}")
            break
        except RuntimeError as error:
            if "HTTP 503" not in str(error) or time.monotonic() >= deadline:
                raise
            time.sleep(0.25)
    if engine == "rustfs":
        return verify_rustfs_durability(port, secret)
    return {"bucket": BUCKET, "mode": "full-metadata"}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_usage(path):
    allocated = 0
    inodes = 0
    for base, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            item = Path(base, name)
            try:
                stat = item.lstat()
            except FileNotFoundError:
                # Warp may delete a prepared object while the footprint walk runs.
                continue
            allocated += stat.st_blocks * 512
            inodes += 1
    return allocated, inodes


def arm_usage(base_bytes, base_inodes, arm):
    """Bounded live-footprint estimate without rescanning toolchains every sample."""
    live_bytes, live_inodes = tree_usage(arm)
    return base_bytes + live_bytes, base_inodes + live_inodes


def cpu_rss(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
        status = Path(f"/proc/{pid}/status").read_text()
        rss = int(re.search(r"^VmRSS:\s+(\d+)", status, re.MULTILINE).group(1)) * 1024
        hwm = int(re.search(r"^VmHWM:\s+(\d+)", status, re.MULTILINE).group(1)) * 1024
        return cpu, rss, hwm
    except (FileNotFoundError, AttributeError, ValueError):
        return None


def process_io_snapshot(pid):
    """Read fixed Linux per-process I/O counters, never substituting zero for loss."""
    try:
        lines = Path(f"/proc/{pid}/io").read_text().splitlines()
        values = {key: int(value.strip()) for line in lines
                  for key, separator, value in (line.partition(":"),)
                  if separator and key in PROCESS_IO_FIELDS}
        if any(key not in values or values[key] < 0 for key in PROCESS_IO_FIELDS):
            return None
        return values
    except (OSError, ValueError):
        return None


def process_io_delta(before, after):
    if (before is None or after is None
            or any(key not in before or key not in after for key in PROCESS_IO_FIELDS)):
        return None
    delta = {key: after[key] - before[key] for key in PROCESS_IO_FIELDS}
    return delta if all(value >= 0 for value in delta.values()) else None


def parse_process_stat(raw):
    """Read identity and CPU ticks without mistaking a `)` in comm for the field boundary."""
    head, separator, fields = raw.rpartition(") ")
    if not separator:
        return None
    pid, separator, comm = head.partition(" (")
    columns = fields.split()
    if not separator or len(columns) < 20:
        return None
    try:
        return (int(pid), int(columns[19])), comm, int(columns[11]) + int(columns[12])
    except ValueError:
        return None


def process_cpu_snapshot(exclude=()):
    """Names-only capped census; no command lines, arguments, or environment data."""
    processes = {}
    try:
        with os.scandir("/proc") as entries:
            for entry in entries:
                if not entry.name.isdecimal() or int(entry.name) in exclude:
                    continue
                try:
                    parsed = parse_process_stat(Path(entry.path, "stat").read_text())
                except (OSError, UnicodeError):
                    continue  # An exited process is normal during a census.
                if parsed is not None:
                    identity, comm, ticks = parsed
                    processes[identity] = (comm, ticks)
                    if len(processes) > MAX_CENSUS_PROCESSES:
                        return None
    except OSError:
        return None
    return processes


def unrelated_cpu_delta(before, after):
    """Top unrelated CPU users in one interval; exited-between-scans work is unavailable."""
    if before is None or after is None:
        return None
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    observed = []
    for identity, (comm, ticks) in after.items():
        previous = before.get(identity)
        delta = ticks - previous[1] if previous is not None else ticks
        if delta > 0:
            observed.append({"pid": identity[0], "name": comm,
                             "cpu_seconds": delta / ticks_per_second})
    observed.sort(key=lambda item: item["cpu_seconds"], reverse=True)
    return {"sampled_cpu_seconds": sum(item["cpu_seconds"] for item in observed),
            "top": observed[:5], "processes_seen": len(after),
            "exited_since_previous": len(before.keys() - after.keys())}


def io_pressure_totals():
    """Cumulative host I/O stall microseconds; diagnostic, never server attribution."""
    try:
        lines = Path("/proc/pressure/io").read_text().splitlines()
        return {name: int(re.search(r"\btotal=(\d+)", line).group(1))
                for name, line in (entry.split(" ", 1) for entry in lines)}
    except (OSError, AttributeError, ValueError):
        return None


def parse_diskstats(text, device):
    """Host-wide counters for the volume's exact major/minor; never per-server I/O."""
    major, minor = os.major(device), os.minor(device)
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 14:
            continue
        try:
            if (int(fields[0]), int(fields[1])) != (major, minor):
                continue
            values = [int(fields[index]) for index in (3, 5, 6, 7, 9, 10, 12, 13)]
        except ValueError:
            continue
        return dict(zip(("reads", "read_sectors", "read_ms", "writes",
                         "write_sectors", "write_ms", "io_ms", "weighted_io_ms"),
                        values), name=fields[2])
    return None


def volume_diskstats(device):
    try:
        return parse_diskstats(Path("/proc/diskstats").read_text(), device)
    except OSError:
        return None


def diskstats_delta(before, after):
    """Reject missing, replaced, or reset host counters rather than report false progress."""
    if not before or not after or before["name"] != after["name"]:
        return None
    delta = {key: after[key] - before[key] for key in before if key != "name"}
    return delta if all(value >= 0 for value in delta.values()) else None


def stop_group(proc, grace_seconds=10):
    if proc.poll() is not None:
        return
    os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)


def wait_health(url, proc):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited before health check: {proc.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.2)
    raise RuntimeError("server readiness timed out")


def parse_warp(output, operation):
    text = re.sub(r"\x1b\[[0-9;]*m", "", output)
    headers = list(re.finditer(r"^(?:Operation|Report):\s+([A-Za-z]+)[^\n]*", text, re.MULTILINE))
    reports = {}
    for index, header in enumerate(headers):
        end = headers[index + 1].start() if index + 1 < len(headers) else len(text)
        block = text[header.start():end]
        average = re.search(r"^[ \t]*\* Average:[^\n]*", block, re.MULTILINE)
        requests = re.search(r"^[ \t]*\* Reqs:[^\n]*", block, re.MULTILINE)
        ran = re.search(r"Ran:\s*([0-9.]+)s", header.group(0))
        latency = {}
        if requests:
            for label, value, unit in re.findall(
                    r"\b(Avg|50%|90%|99%):\s*([0-9.]+)(µs|us|ms|s)\b", requests.group(0)):
                factor = {"µs": 0.001, "us": 0.001, "ms": 1, "s": 1000}[unit]
                latency[{"Avg": "avg", "50%": "p50", "90%": "p90", "99%": "p99"}[label]] = (
                    float(value) * factor)
        reports[header.group(1).upper()] = {
            "summary": average.group(0).strip() if average else None,
            "analyzed_seconds": float(ran.group(1)) if ran else None,
            "request_latency_ms": latency,
        }
    target = reports.get("TOTAL" if operation == "mixed" else operation.upper(), {})
    line = target.get("summary")
    if operation == "mixed" and line is None:
        cluster = re.search(r"^Cluster Total:.*$", text, re.MULTILINE)
        line = cluster.group(0) if cluster else None
    objects = re.search(r"([0-9.]+) obj/s", line or "")
    bandwidth = re.search(r"([0-9.]+) MiB/s", line or "")
    error_lines = [int(x) for x in re.findall(r"^[ \t]*\*?\s*Errors:\s+(\d+)",
                                             text, re.MULTILINE)]
    inline_errors = [int(x) for x in re.findall(r"\b(\d+) errors\b", text)]
    errors = max(sum(error_lines), sum(inline_errors), text.count("<ERROR>"))
    return {
        "summary": line,
        "analyzed_seconds": target.get("analyzed_seconds"),
        "objects_per_second": float(objects.group(1)) if objects else None,
        "mib_per_second": float(bandwidth.group(1)) if bandwidth else None,
        "reports": reports,
        "reported_errors": errors,
        "error_line_count": len(error_lines),
    }


def parse_warp_timestamp(value):
    """Accept RFC3339 or Warp's Go `time.Time.String()` segment timestamps."""
    try:
        timestamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        match = re.fullmatch(
            r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)(?:\.(\d{1,9}))? "
            r"([+-]\d{4})(?: [A-Za-z0-9_+/-]+)?", value)
        if match is None:
            raise ValueError("invalid Warp timeline timestamp") from None
        fraction = (match.group(2) or "")[:6].ljust(6, "0")
        offset = match.group(3)
        try:
            timestamp = datetime.datetime.fromisoformat(
                f"{match.group(1)}.{fraction}{offset[:3]}:{offset[3:]}")
        except ValueError as error:
            raise ValueError("invalid Warp timeline timestamp") from error
    if timestamp.tzinfo is None:
        raise ValueError("Warp timeline timestamp is missing a timezone")
    return timestamp.timestamp()


def parse_warp_aggregate(payload, operation="PUT"):
    """Read Warp v1.8's scored operation from its default realtime JSON aggregate."""
    if len(payload) > MAX_WARP_AGGREGATE_BYTES:
        raise ValueError("Warp aggregate exceeded bounded JSON size")
    try:
        result = json.loads(payload)
        if result["v"] != 2 or result["final"] is not True:
            raise ValueError("Warp aggregate is not a final v2 realtime report")
        if operation not in result["by_op_type"]:
            raise ValueError("Warp aggregate lacks one successful scored operation")
        scored = result["by_op_type"][operation]
        if scored["total_errors"] != 0 or scored["total_requests"] <= 0:
            raise ValueError("Warp aggregate lacks one successful scored operation")
        throughput = scored["throughput"]
        if throughput["errors"] != 0 or throughput["ops"] <= 0:
            raise ValueError("Warp aggregate lacks one successful scored operation")
        first = parse_warp_timestamp(throughput["start_time"])
        raw_last = parse_warp_timestamp(throughput["end_time"])
        duration_ms = throughput["measure_duration_millis"]
        byte_count = throughput["bytes"]
        segmented = throughput["segmented"]
        segments = segmented["segments"]
        segment_ms = segmented["segment_duration_millis"]
        if (type(duration_ms) is not int or duration_ms <= 0
                or type(byte_count) not in (int, float) or not math.isfinite(byte_count)
                or byte_count <= 0 or type(segment_ms) is not int or segment_ms <= 0
                or len(segments) < 2 or len(segments) * segment_ms != duration_ms):
            raise ValueError("invalid Warp aggregate interval or byte count")
        # Warp v1.8's liveThroughput.asThroughput() trims two 1-s segments at each
        # boundary, then sets EndTime using the *untrimmed* segment count. The
        # displayed score uses MeasureDurationMillis and Bytes. Recover its actual
        # end as StartTime + that duration; accept only the documented 4-s offset.
        last = first + duration_ms / 1000
        if abs(raw_last - last - 4) > 0.002:
            raise ValueError("invalid Warp aggregate interval or byte count")
    except (KeyError, TypeError, IndexError, json.JSONDecodeError) as error:
        raise ValueError("invalid Warp aggregate JSON") from error
    return {"start_unix_seconds": first, "end_unix_seconds": last,
            "raw_end_unix_seconds": raw_last,
            "interval_source": "Warp v1.8 throughput start plus measured duration; raw end is +4s",
            "analyzed_seconds_exact": last - first,
            "aggregate_mib_per_second": byte_count * 1000 / duration_ms / 1048576}


def scored_host_window(samples, load_start_unix_seconds, timeline):
    """Keep only complete host-census intervals inside recorded PUT operations.

    Partial boundary samples are excluded, not proportionally allocated. This is
    evidence coverage, not a noise correction or a request-attributed CPU figure.
    """
    previous_time = load_start_unix_seconds
    previous_pressure = None
    previous_server_io = None
    coverage = 0.0
    intervals = 0
    cpu = 0.0
    cpu_intervals = 0
    pressure = {"some": 0, "full": 0}
    pressure_intervals = 0
    server_io = {key: 0 for key in PROCESS_IO_FIELDS}
    server_io_intervals = 0
    for sample in samples:
        current_time = sample.get("unix_seconds")
        if current_time is None or current_time <= previous_time:
            raise ValueError("invalid host-sample timeline")
        current_pressure = sample.get("host_io_pressure_totals_us")
        current_server_io = sample.get("server_process_io")
        if (previous_time >= timeline["start_unix_seconds"]
                and current_time <= timeline["end_unix_seconds"]):
            intervals += 1
            coverage += current_time - previous_time
            unrelated = sample.get("unrelated_process_cpu")
            if unrelated is not None:
                cpu += unrelated["sampled_cpu_seconds"]
                cpu_intervals += 1
            if previous_pressure is not None and current_pressure is not None:
                delta = {key: current_pressure[key] - previous_pressure[key]
                         for key in pressure}
                if all(value >= 0 for value in delta.values()):
                    for key, value in delta.items():
                        pressure[key] += value
                    pressure_intervals += 1
            io_delta = process_io_delta(previous_server_io, current_server_io)
            if io_delta is not None:
                for key, value in io_delta.items():
                    server_io[key] += value
                server_io_intervals += 1
        previous_time = current_time
        previous_pressure = current_pressure
        previous_server_io = current_server_io
    return {"complete_intervals": intervals, "coverage_seconds": coverage,
            "unrelated_cpu_seconds": cpu if cpu_intervals else None,
            "unrelated_cpu_valid_intervals": cpu_intervals,
            "host_io_pressure_delta_us": pressure if pressure_intervals else None,
            "host_io_pressure_valid_intervals": pressure_intervals,
            "server_process_io_delta": server_io if server_io_intervals else None,
            "server_process_io_valid_intervals": server_io_intervals}


def analyze_warp_put_timeline(warp, arm, load_start_unix_seconds, load_end_unix_seconds,
                              samples, deadline, expected_mib_per_second):
    """Reduce Warp v1.8's aggregate artifact to its scored PUT interval after load."""
    # Warp appends .json.zst to --benchdata in its default aggregate mode.
    # Its v1.8 benchmark command does not emit full per-request .csv.zst data.
    _, benchdata = warp_benchdata_paths(arm)
    if not benchdata.is_file() or benchdata.stat().st_size > MAX_WARP_BENCHDATA_BYTES:
        return {"status": "unavailable: missing or oversized Warp benchdata"}
    if deadline - time.monotonic() < 10:
        return {"status": "unavailable: no analysis time before arm deadline"}
    command = [warp, "analyze", "--json", "--analyze.op=PUT",
               "--analyze.dur=1s", str(benchdata)]
    try:
        result = subprocess.run(command, capture_output=True, timeout=min(
            30, max(1, deadline - time.monotonic() - 5)))
        if result.returncode:
            return {"status": "unavailable: Warp analyzer failed"}
        timeline = parse_warp_aggregate(result.stdout)
        # Wall-clock jumps or a preparation interval must not masquerade as scored load.
        if (timeline["start_unix_seconds"] < load_start_unix_seconds - 1
                or timeline["end_unix_seconds"] > load_end_unix_seconds + 1):
            return {"status": "unavailable: scored span outside client-load interval"}
        if (expected_mib_per_second is None or not math.isfinite(expected_mib_per_second) or
                abs(timeline["aggregate_mib_per_second"] - expected_mib_per_second) > 0.02):
            return {"status": "unavailable: analyzer score differs from benchmark report",
                    **timeline}
        return {"status": "captured source-derived Warp scored interval with report-score parity",
                **timeline, "host_window": scored_host_window(
                    samples, load_start_unix_seconds, timeline)}
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return {"status": "unavailable: Warp timeline could not be reduced"}


def cairn_metrics(endpoint):
    """Keep only bounded, fixed-label stage and Writer evidence from the scored node."""
    with urllib.request.urlopen(f"http://{endpoint}/metrics", timeout=5) as response:
        body = response.read(2_000_001)
    if len(body) > 2_000_000:
        raise RuntimeError("Cairn metrics response exceeded diagnostic bound")
    prefixes = ("cairn_blob_object_write_stage_seconds",
                "cairn_blob_object_write_timing_dropped_total",
                "cairn_put_stage_seconds", "cairn_put_timing_dropped_total",
                "cairn_writer_stage_seconds", "cairn_writer_commit_seconds",
                "cairn_writer_batch_size", "cairn_writer_queue_depth", "cairn_wal_")
    return [line for line in body.decode().splitlines()
            if line.startswith(prefixes) and not line.startswith("#")]


def run_arm(args, root, case, engine, index, deadline):
    capacity_before = host_capacity()
    max_load_seconds = getattr(args, "max_load_seconds", None)
    if max_load_seconds is not None and not 1 <= max_load_seconds <= 60:
        raise ValueError("measured client-load cap outside diagnostic bounds")
    name, operation, size, concurrency, extras = case
    arm = Path(tempfile.mkdtemp(prefix=f"{index:02d}-{engine}-{name}-", dir=root))
    # Freeze the static build/tool footprint before this arm creates its volume or server data.
    # Sampling adds only this arm's live tree; taking the base after bootstrap double-counts it.
    base_bytes, base_inodes = tree_usage(root)
    volume = arm / "volume"
    volume.mkdir()
    volume_device = volume.stat().st_dev
    port = 18000 + index
    endpoint = f"127.0.0.1:{port}"
    secret = secrets.token_hex(24)
    master = "a" + secrets.token_hex(32)[1:]
    env = server_environment(engine, volume, endpoint, secret, master)
    probe_file = None
    if engine == "cairn":
        linger_us = getattr(args, "cairn_linger_us", 0)
        if not 0 <= linger_us <= 1000:
            shutil.rmtree(arm)
            raise ValueError("Cairn Writer linger outside benchmark screen bounds")
        env["CAIRN_META_GROUP_COMMIT_LINGER_MICROS"] = str(linger_us)
        try:
            bootstrap = subprocess.run([args.cairn, "bootstrap"], env=env, capture_output=True,
                                       timeout=30)
        except (OSError, subprocess.TimeoutExpired):
            shutil.rmtree(arm)
            raise
        if bootstrap.returncode:
            shutil.rmtree(arm)
            raise RuntimeError(f"Cairn bootstrap failed: {bootstrap.stderr.decode(errors='replace')[-500:]}")
        command = [args.cairn, "serve"]
        health = f"http://{endpoint}/healthz"
    else:
        command = [args.rustfs, "server", "--address", endpoint, str(volume)]
        health = f"http://{endpoint}/minio/health/live"

    if getattr(args, "sync_probe_lib", None):
        probe_file = arm / "sync-probe.bin"
        initialize_sync_probe(probe_file)
        env["SYNC_PROBE_FILE"] = str(probe_file)
        env["LD_PRELOAD"] = str(args.sync_probe_lib)

    server_log = open(arm / "server.log", "wb")
    try:
        proc = subprocess.Popen(command, env=env, stdout=server_log, stderr=subprocess.STDOUT,
                                start_new_session=True)
    except OSError:
        server_log.close()
        shutil.rmtree(arm)
        raise
    try:
        try:
            wait_health(health, proc)
        except RuntimeError as error:
            server_log.flush()
            log_tail = (arm / "server.log").read_bytes()[-1_000:].decode(errors="replace")
            raise RuntimeError(f"{error}; server log tail: {log_tail}") from error
        durability = prepare_verified_bucket(port, secret, engine)
        benchdata_base, _ = warp_benchdata_paths(arm)
        warp_cmd = [
            args.warp, operation, "--host", endpoint,
            "--region", "us-east-1", "--lookup", "path",
            "--bucket", BUCKET,
            "--concurrent", str(concurrency), "--duration", f"{args.duration}s",
            "--no-color", "--benchdata", str(benchdata_base),
            *extras,
        ]
        if operation != "multipart":
            warp_cmd.extend(("--obj.size", size))
        warp_env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("AWS_", "WARP_"))
                    and key not in ("LD_PRELOAD", "SYNC_PROBE_FILE")}
        warp_env.update(WARP_ACCESS_KEY=ACCESS, WARP_SECRET_KEY=secret)
        sample = {"server_peak_rss": 0, "client_peak_rss": 0, "peak_bytes": 0, "peak_inodes": 0,
                  "server_last_cpu": None, "client_last_cpu": None}
        done = threading.Event()
        limit_hit = threading.Event()
        pressure_before = io_pressure_totals()
        device_before = volume_diskstats(volume_device)
        server_io_before = process_io_snapshot(proc.pid)
        load_samples = []
        load_samples_dropped = 0
        unrelated_observed_cpu = 0.0
        unrelated_valid_intervals = 0
        probe_before_load = read_sync_probe(probe_file) if probe_file is not None else None
        load_start = time.monotonic()
        load_start_unix_seconds = time.time()
        load_deadline = load_start + max_load_seconds if max_load_seconds is not None else None
        client = subprocess.Popen(warp_cmd, env=warp_env, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, start_new_session=True)
        census_excluded_pids = (os.getpid(), proc.pid, client.pid)
        unrelated_before = process_cpu_snapshot(census_excluded_pids)

        def observe():
            nonlocal load_samples_dropped, unrelated_before
            nonlocal unrelated_observed_cpu, unrelated_valid_intervals
            while not done.wait(2):
                points = {}
                for label, pid in (("server", proc.pid), ("client", client.pid)):
                    point = cpu_rss(pid)
                    if point:
                        sample[f"{label}_peak_rss"] = max(sample[f"{label}_peak_rss"], point[2])
                        sample[f"{label}_last_cpu"] = point[0]
                        points[label] = {"cpu_seconds": point[0], "rss_bytes": point[1]}
                unrelated_after = process_cpu_snapshot(census_excluded_pids)
                unrelated = unrelated_cpu_delta(unrelated_before, unrelated_after)
                unrelated_before = unrelated_after
                if unrelated is not None:
                    unrelated_observed_cpu += unrelated["sampled_cpu_seconds"]
                    unrelated_valid_intervals += 1
                if len(load_samples) < 128:
                    load_samples.append({
                        "elapsed_seconds": time.monotonic() - load_start,
                        "unix_seconds": time.time(),
                        "processes": points,
                        "unrelated_process_cpu": unrelated,
                        "host_io_pressure_totals_us": io_pressure_totals(),
                        "host_volume_diskstats": volume_diskstats(volume_device),
                        "server_process_io": process_io_snapshot(proc.pid),
                    })
                else:
                    load_samples_dropped += 1
                used, inodes = arm_usage(base_bytes, base_inodes, arm)
                sample["peak_bytes"] = max(sample["peak_bytes"], used)
                sample["peak_inodes"] = max(sample["peak_inodes"], inodes)
                if (used > args.max_bytes or time.monotonic() > deadline
                        or (load_deadline is not None and time.monotonic() > load_deadline)):
                    limit_hit.set()
                    stop_group(client, grace_seconds=1 if load_deadline is not None else 10)

        before_server = cpu_rss(proc.pid)
        before_client = cpu_rss(client.pid)
        monitor = threading.Thread(target=observe, daemon=True)
        monitor.start()
        try:
            output, _ = client.communicate(timeout=min(
                args.duration + 180, max(1, deadline - time.monotonic()),
                max_load_seconds if max_load_seconds is not None else float("inf")))
        except subprocess.TimeoutExpired:
            stop_group(client, grace_seconds=1 if load_deadline is not None else 10)
            output, _ = client.communicate(timeout=2 if load_deadline is not None else 5)
            limit_hit.set()
        finally:
            done.set()
            monitor.join(timeout=5)
        load_elapsed = time.monotonic() - load_start
        capacity_after = host_capacity()
        load_end_unix_seconds = time.time()
        unrelated_after = process_cpu_snapshot(census_excluded_pids)
        unrelated_final = unrelated_cpu_delta(unrelated_before, unrelated_after)
        if unrelated_final is not None:
            unrelated_observed_cpu += unrelated_final["sampled_cpu_seconds"]
            unrelated_valid_intervals += 1
        pressure_after = io_pressure_totals()
        device_after = volume_diskstats(volume_device)
        server_io_after = process_io_snapshot(proc.pid)
        final_used, final_inodes = arm_usage(base_bytes, base_inodes, arm)
        sample["peak_bytes"] = max(sample["peak_bytes"], final_used)
        sample["peak_inodes"] = max(sample["peak_inodes"], final_inodes)
        if final_used > args.max_bytes:
            limit_hit.set()
        output = output.decode(errors="replace")
        (arm / "warp.txt").write_text(output)
        after_server = cpu_rss(proc.pid)
        parsed = parse_warp(output, operation)
        warp_timeline = (
            analyze_warp_put_timeline(args.warp, arm, load_start_unix_seconds,
                                      load_end_unix_seconds, load_samples, deadline,
                                      parsed["mib_per_second"])
            if operation == "put" else {"status": "unavailable: only standalone PUT is aligned"}
        )
        result = {
            "case": name, "engine": engine, "order": index,
            "duration_requested_seconds": args.duration, "concurrency": concurrency,
            "command": warp_cmd,
            "durability": durability,
            "strict_settings": {key: env[key] for key in STRICT_SETTINGS[engine]},
            "warp_exit": client.returncode, "limit_hit": limit_hit.is_set(),
            "load_elapsed_seconds": load_elapsed,
            "host_capacity_before": capacity_before,
            "host_capacity_after": capacity_after,
            "load_start_unix_seconds": load_start_unix_seconds,
            "load_end_unix_seconds": load_end_unix_seconds,
            "warp_operation_timeline": warp_timeline,
            "host_io_pressure_delta_us": ({name: pressure_after[name] - pressure_before[name]
                                           for name in ("some", "full")}
                                          if pressure_before and pressure_after else None),
            "host_volume_device": {"major": os.major(volume_device),
                                   "minor": os.minor(volume_device)},
            "host_volume_diskstats_before": device_before,
            "host_volume_diskstats_after": device_after,
            "host_volume_diskstats_load_delta": diskstats_delta(device_before, device_after),
            "server_process_io_load_delta": process_io_delta(server_io_before,
                                                              server_io_after),
            "load_samples": load_samples,
            "load_samples_dropped": load_samples_dropped,
            "unrelated_process_cpu_final_interval": unrelated_final,
            "unrelated_process_cpu_observed_seconds": (
                unrelated_observed_cpu if unrelated_valid_intervals else None),
            "unrelated_process_cpu_valid_intervals": unrelated_valid_intervals,
            "server_cpu_seconds": after_server[0] - before_server[0] if before_server and after_server else None,
            "client_cpu_seconds": (
                sample["client_last_cpu"] - before_client[0]
                if before_client and sample["client_last_cpu"] is not None else None
            ),
            **sample, **parsed,
            "warp_output": output[-20_000:],
            "warp_output_truncated": len(output) > 20_000,
        }
        if probe_file is not None:
            result["sync_probe"] = read_sync_probe(probe_file)
            result["sync_probe_load_delta"] = sync_probe_delta(
                probe_before_load, result["sync_probe"])
            result["sync_probe_status"] = (
                "captured" if sum(item["calls"] for item in
                                  result["sync_probe_load_delta"].values())
                else "unavailable: binary may be static or bypass libc sync symbols"
            )
        if engine == "cairn" and args.metrics_drain_seconds:
            time.sleep(args.metrics_drain_seconds)
            result["metrics_after_drain"] = cairn_metrics(endpoint)
        if engine == "rustfs":
            # Catch any client-side bucket recreation before promoting the arm to a score.
            result["durability_after"] = verify_rustfs_durability(port, secret)
        result["status"] = "PASS" if (
            client.returncode == 0 and not limit_hit.is_set()
            and capacity_compatible(capacity_before, capacity_after,
                                    getattr(args, "allow_ballooning", False))
            and parsed["summary"] and parsed["reported_errors"] == 0
            and put_timeline_valid(operation, warp_timeline)
            and (probe_file is None or result["sync_probe_status"] == "captured")
        ) else "INCONCLUSIVE"
        return result, output
    finally:
        stop_group(proc)
        server_log.close()
        # Only the exact tempfile.mkdtemp arm is removed, after both process groups are reaped.
        shutil.rmtree(arm)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--cairn", type=str)
    parser.add_argument("--rustfs", type=str)
    parser.add_argument("--warp", type=str, required=True)
    parser.add_argument("--duration", type=int, default=12)
    parser.add_argument("--metrics-drain-seconds", type=int, default=0)
    parser.add_argument("--allow-ballooning", action="store_true",
                        help="diagnostic only: permit up to 10%% MemTotal drift; retain samples")
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--max-bytes", type=int, default=10_000_000_000)
    parser.add_argument("--case", choices=[case[0] for case in CASES] + ["all"], default="all")
    parser.add_argument("--engine", choices=("both", "cairn", "rustfs"), default="both",
                        help="single-engine runs are diagnostic only and require --sync-probe-lib")
    parser.add_argument("--sync-probe-lib", type=Path,
                        help="diagnostic-only glibc preload for aggregate sync syscall times")
    args = parser.parse_args()
    diagnostic = args.engine != "both"
    minimum_seconds = 30 if diagnostic else 60
    if not (1 <= args.duration <= 60 and 1 <= args.pairs <= 5
            and minimum_seconds <= args.max_seconds <= 3600
            and 0 <= args.metrics_drain_seconds <= 20):
        parser.error("duration, pairs or max-seconds outside safe bounds")
    root = args.root.resolve(strict=True)
    if not root.is_dir() or root.is_symlink() or not str(root).startswith("/var/tmp/cairn-performance-"):
        parser.error("root must be an existing private /var/tmp/cairn-performance-* directory")
    selected = ("cairn", "rustfs") if args.engine == "both" else (args.engine,)
    binaries = {name: getattr(args, name) for name in selected}
    binaries["warp"] = args.warp
    for name, binary in binaries.items():
        if not binary or not Path(binary).is_file() or not os.access(binary, os.X_OK):
            parser.error(f"not executable: {name}={binary}")
    if args.sync_probe_lib is not None:
        args.sync_probe_lib = args.sync_probe_lib.resolve(strict=True)
        if not args.sync_probe_lib.is_file():
            parser.error("sync probe is not a regular file")
    if args.engine != "both" and args.sync_probe_lib is None:
        parser.error("single-engine runs require an explicit diagnostic sync probe")
    cases = CASES if args.case == "all" else [case for case in CASES if case[0] == args.case]
    declared = {
        "schema": 2, "cases": cases, "pairs": args.pairs, "duration": args.duration,
        "host_capacity": host_capacity(),
        "allow_ballooning": args.allow_ballooning,
        "metrics_drain_seconds": args.metrics_drain_seconds,
        "diagnostic_sync_probe": args.sync_probe_lib is not None,
        "engine_selection": args.engine,
        "strict_settings": STRICT_SETTINGS,
        "max_seconds": args.max_seconds, "max_bytes": args.max_bytes,
        "binary_sha256": {name: sha256(path) for name, path in binaries.items()},
        "arms": [],
    }
    result_path = root / "comparison-results.json"
    start = time.monotonic()
    teardown_reserve = 10 if diagnostic else 30
    deadline = start + args.max_seconds - teardown_reserve
    index = 0
    try:
        for case in cases:
            for pair in range(args.pairs):
                order = (("cairn", "rustfs") if pair % 2 == 0 else ("rustfs", "cairn"))
                if args.engine != "both":
                    order = (args.engine,)
                for engine in order:
                    # A single-engine diagnostic has no paired-arm setup and a shorter
                    # teardown reserve; the active arm still observes its deadline.
                    preparation_reserve = 5 if diagnostic else 10
                    if time.monotonic() + args.duration + preparation_reserve >= deadline:
                        raise RuntimeError("remaining time cannot admit another arm")
                    if tree_usage(root)[0] > args.max_bytes - 1_000_000_000:
                        raise RuntimeError("insufficient owned disk headroom")
                    index += 1
                    total = len(cases) * args.pairs * len(order)
                    print(f"[{index}/{total}] {case[0]} pair {pair + 1} {engine}", flush=True)
                    arm, _ = run_arm(args, root, case, engine, index, deadline)
                    declared["arms"].append(arm)
                    declared["elapsed_seconds"] = time.monotonic() - start
                    result_path.write_text(json.dumps(declared, indent=2) + "\n")
                    print(f"  {arm['status']} {arm['summary']} errors={arm['reported_errors']}", flush=True)
                    if arm["status"] != "PASS":
                        raise RuntimeError(f"arm {index} did not pass; stopped without retry")
    except (KeyboardInterrupt, OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        declared["stopped_reason"] = str(error)
        declared["elapsed_seconds"] = time.monotonic() - start
        result_path.write_text(json.dumps(declared, indent=2) + "\n")
        raise SystemExit(str(error))
    print(f"Completed {index} arms in {time.monotonic() - start:.1f}s; {result_path}")


if __name__ == "__main__":
    main()
