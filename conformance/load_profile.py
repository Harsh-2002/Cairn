#!/usr/bin/env python3
"""Macro load profiles for Cairn (ARCH 30.2, docs/GAPS.md Medium #11).

MinIO's `warp` is unavailable in this environment, so this is an equivalent concurrent harness
built directly on boto3 (real SigV4 signing, the same client path the conformance suite drives).
It runs two profiles against a running Cairn server and scrapes `/metrics` between phases to
characterize the **single-writer ceiling** the spec foregrounds:

  (a) LARGE-OBJECT BANDWIDTH — N concurrent workers each PUT then GET a large object (default
      32 MiB). Reports aggregate up/down throughput in MiB/s. This profile is bound by disk and
      network bandwidth plus the two durable-commit fsyncs (ARCH 30.1), not by the writer.

  (b) SMALL-OBJECT RATE — N concurrent workers PUT many 4 KiB objects. Reports ops/s and the
      p50/p99/p999 PUT latency. Per ARCH 30.1/30.2 the binding constraint here is the single
      group-committing metadata writer and the fsync rate; this is where the single-writer
      ceiling shows up. The profile is swept across concurrency 1, 4, 16 so the report shows how
      ops/s and tail latency move as concurrency rises — the operator-visible signature of the
      writer becoming the bottleneck (ARCH 30.2). Between sweeps it samples `/metrics` so the
      `cairn_*` gauges and the request-duration summary are recorded alongside the client-side
      numbers.

Usage:
    load_profile.py ACCESS_KEY SECRET_KEY ENDPOINT [--quick]

`--quick` shrinks sizes/counts for a smoke run. The defaults keep total runtime ~1-2 min.
"""

import argparse
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config


def make_client(akid, secret, endpoint):
    # A per-thread client: botocore clients are not guaranteed thread-safe to share, and we want
    # each worker to drive its own connection so concurrency is real, not serialized on a lock.
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=akid,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"total_max_attempts": 1, "mode": "standard"},
            max_pool_connections=64,
            connect_timeout=30,
            read_timeout=120,
        ),
    )


# --- a tiny incompressible-ish payload generator (avoids the compressor inflating throughput) ----
def make_payload(size):
    # A simple LCG fill: cheap, deterministic, and not trivially compressible, so the large-object
    # numbers reflect real bytes moved rather than the compressor's view of a run of zeros.
    buf = bytearray(size)
    x = 0x2545F4914F6CDD1D
    for i in range(0, size, 8):
        x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        chunk = x.to_bytes(8, "little")
        end = min(i + 8, size)
        buf[i:end] = chunk[: end - i]
    return bytes(buf)


def percentile(samples, pct):
    """Nearest-rank percentile over a list of floats (seconds)."""
    if not samples:
        return 0.0
    ordered = sorted(samples)
    k = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * len(ordered) + 0.5)) - 1))
    return ordered[k]


# ------------------------------------------------------------------------------------------------
# Metrics scraping
# ------------------------------------------------------------------------------------------------
def scrape_metrics(endpoint):
    """GET /metrics and return (gauges, duration_summary) where gauges is a dict of cairn_* single
    values and duration_summary maps method -> {quantile: seconds, count, sum}."""
    url = endpoint.rstrip("/") + "/metrics"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            text = resp.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError) as e:
        return {}, {}, f"scrape failed: {e}"

    gauges = {}
    duration = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        # name{labels} value   OR   name value
        try:
            metric, value = line.rsplit(" ", 1)
            value = float(value)
        except ValueError:
            continue
        if metric.startswith("cairn_request_duration_seconds"):
            base, _, label_blob = metric.partition("{")
            labels = _parse_labels(label_blob.rstrip("}"))
            method = labels.get("method", "?")
            d = duration.setdefault(method, {})
            if base.endswith("_count"):
                d["count"] = value
            elif base.endswith("_sum"):
                d["sum"] = value
            elif "quantile" in labels:
                d[f"q{labels['quantile']}"] = value
        elif metric.startswith("cairn_") and "{" not in metric:
            gauges[metric] = value
    return gauges, duration, None


def _parse_labels(blob):
    out = {}
    for pair in blob.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            out[k.strip()] = v.strip().strip('"')
    return out


# ------------------------------------------------------------------------------------------------
# Profile (a): large-object bandwidth
# ------------------------------------------------------------------------------------------------
def profile_large_objects(akid, secret, endpoint, bucket, workers, obj_size, count):
    payload = make_payload(obj_size)
    total_bytes = obj_size * count

    # Each worker owns a client; we hand out object indices round-robin.
    local = threading.local()

    def client():
        if not hasattr(local, "c"):
            local.c = make_client(akid, secret, endpoint)
        return local.c

    def put_one(i):
        key = f"large/obj-{i:05d}.bin"
        client().put_object(Bucket=bucket, Key=key, Body=payload)
        return key

    def get_one(key):
        body = client().get_object(Bucket=bucket, Key=key)["Body"]
        n = 0
        for chunk in iter(lambda: body.read(1 << 20), b""):
            n += len(chunk)
        return n

    # --- PUT phase (up) ---
    t0 = time.perf_counter()
    keys = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for fut in as_completed([pool.submit(put_one, i) for i in range(count)]):
            keys.append(fut.result())
    up_secs = time.perf_counter() - t0

    # --- GET phase (down) ---
    t0 = time.perf_counter()
    down_bytes = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for fut in as_completed([pool.submit(get_one, k) for k in keys]):
            down_bytes += fut.result()
    down_secs = time.perf_counter() - t0

    mib = 1024 * 1024
    return {
        "workers": workers,
        "obj_size_mib": obj_size / mib,
        "count": count,
        "total_mib": total_bytes / mib,
        "up_secs": up_secs,
        "down_secs": down_secs,
        "up_mibs": (total_bytes / mib) / up_secs if up_secs else 0.0,
        "down_mibs": (down_bytes / mib) / down_secs if down_secs else 0.0,
        "down_ok": down_bytes == total_bytes,
    }


# ------------------------------------------------------------------------------------------------
# Profile (b): small-object rate, swept across concurrency
# ------------------------------------------------------------------------------------------------
def profile_small_objects(akid, secret, endpoint, bucket, workers, count, obj_size=4096):
    payload = make_payload(obj_size)
    local = threading.local()

    def client():
        if not hasattr(local, "c"):
            local.c = make_client(akid, secret, endpoint)
        return local.c

    latencies = []
    lat_lock = threading.Lock()
    errors = [0]

    def put_one(i):
        key = f"small/c{workers}/obj-{i:06d}"
        t = time.perf_counter()
        try:
            client().put_object(Bucket=bucket, Key=key, Body=payload)
        except Exception:  # noqa: BLE001 - count and continue; a failed op is not a latency sample
            with lat_lock:
                errors[0] += 1
            return
        dt = time.perf_counter() - t
        with lat_lock:
            latencies.append(dt)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(as_completed([pool.submit(put_one, i) for i in range(count)]))
    wall = time.perf_counter() - t0

    ok = len(latencies)
    return {
        "workers": workers,
        "count": count,
        "obj_size": obj_size,
        "ok": ok,
        "errors": errors[0],
        "wall_secs": wall,
        "ops_per_sec": ok / wall if wall else 0.0,
        "p50_ms": percentile(latencies, 50) * 1000,
        "p99_ms": percentile(latencies, 99) * 1000,
        "p999_ms": percentile(latencies, 99.9) * 1000,
        "mean_ms": (statistics.fmean(latencies) * 1000) if latencies else 0.0,
    }


# ------------------------------------------------------------------------------------------------
# Reporting
# ------------------------------------------------------------------------------------------------
def fmt_gauges(gauges):
    keys = [
        "cairn_buckets",
        "cairn_objects",
        "cairn_versions",
        "cairn_logical_bytes",
        "cairn_physical_bytes",
        "cairn_compression_ratio",
        "cairn_wal_bytes",
        "cairn_wal_checkpoints_total",
        "cairn_wal_checkpointed_frames_total",
    ]
    rows = []
    for k in keys:
        if k in gauges:
            v = gauges[k]
            rows.append(f"      {k:38s} {v:,.0f}" if v == int(v) else f"      {k:38s} {v:,.3f}")
    return "\n".join(rows) if rows else "      (no cairn_* gauges published yet)"


def fmt_duration(duration):
    rows = []
    for method, d in sorted(duration.items()):
        cnt = d.get("count", 0)
        q50 = d.get("q0.5", 0) * 1000
        q99 = d.get("q0.99", 0) * 1000
        q999 = d.get("q0.999", 0) * 1000
        rows.append(
            f"      {method:6s} count={cnt:>8.0f}  "
            f"p50={q50:8.3f}ms  p99={q99:8.3f}ms  p999={q999:8.3f}ms"
        )
    return "\n".join(rows) if rows else "      (no request-duration summary published yet)"


def main():
    ap = argparse.ArgumentParser(description="Cairn macro load profiles (boto3, no warp).")
    ap.add_argument("access_key")
    ap.add_argument("secret_key")
    ap.add_argument("endpoint")
    ap.add_argument("--quick", action="store_true", help="smaller sizes/counts for a smoke run")
    args = ap.parse_args()

    if args.quick:
        large_workers, large_size, large_count = 4, 8 * 1024 * 1024, 8
        small_sweep = [(1, 200), (4, 400), (16, 800)]
    else:
        large_workers, large_size, large_count = 8, 32 * 1024 * 1024, 16
        small_sweep = [(1, 500), (4, 1500), (16, 3000)]

    admin = make_client(args.access_key, args.secret_key, args.endpoint)
    bucket = "loadtest"
    try:
        admin.create_bucket(Bucket=bucket)
    except Exception:  # noqa: BLE001 - already exists is fine
        pass

    print("=" * 92)
    print("CAIRN MACRO LOAD PROFILES (boto3 concurrent harness; warp-equivalent) — ARCH 30.2")
    print("=" * 92)

    g0, d0, err0 = scrape_metrics(args.endpoint)
    print("\n[metrics] before load:")
    if err0:
        print("      " + err0)
    print(fmt_gauges(g0))

    # ---- profile (a): large-object bandwidth ----
    print("\n" + "-" * 92)
    print("PROFILE (a) — LARGE-OBJECT BANDWIDTH")
    print("-" * 92)
    la = profile_large_objects(
        args.access_key, args.secret_key, args.endpoint, bucket, large_workers, large_size, large_count
    )
    print(
        f"  {la['count']} x {la['obj_size_mib']:.0f} MiB objects, "
        f"{la['workers']} concurrent workers  ({la['total_mib']:.0f} MiB total)"
    )
    print(f"  PUT (up)   : {la['up_mibs']:8.1f} MiB/s   ({la['up_secs']:.2f} s)")
    print(f"  GET (down) : {la['down_mibs']:8.1f} MiB/s   ({la['down_secs']:.2f} s)")
    print(f"  download integrity: {'OK' if la['down_ok'] else 'MISMATCH'}")

    g1, d1, _ = scrape_metrics(args.endpoint)
    print("\n[metrics] after large-object phase:")
    print(fmt_gauges(g1))

    # ---- profile (b): small-object rate, swept across concurrency ----
    print("\n" + "-" * 92)
    print("PROFILE (b) — SMALL-OBJECT RATE  (4 KiB PUTs; single-writer-ceiling sweep)")
    print("-" * 92)
    print(
        f"  {'conc':>4s}  {'objects':>8s}  {'ops/s':>9s}  "
        f"{'p50':>9s}  {'p99':>9s}  {'p999':>9s}  {'errors':>6s}"
    )
    small_results = []
    for workers, count in small_sweep:
        r = profile_small_objects(
            args.access_key, args.secret_key, args.endpoint, bucket, workers, count
        )
        small_results.append(r)
        print(
            f"  {r['workers']:>4d}  {r['ok']:>8d}  {r['ops_per_sec']:>9.1f}  "
            f"{r['p50_ms']:>8.3f}ms  {r['p99_ms']:>8.3f}ms  {r['p999_ms']:>8.3f}ms  {r['errors']:>6d}"
        )
        # sample the writer-side metrics right after each concurrency level
        gs, ds, _ = scrape_metrics(args.endpoint)
        print("        " + fmt_duration(ds).strip())

    # ---- single-writer-ceiling interpretation ----
    print("\n" + "-" * 92)
    print("SINGLE-WRITER CEILING (ARCH 8.3 / 30.2)")
    print("-" * 92)
    base = small_results[0]
    top = small_results[-1]
    ops_scale = (top["ops_per_sec"] / base["ops_per_sec"]) if base["ops_per_sec"] else 0.0
    conc_scale = top["workers"] / base["workers"]
    tail_growth = (top["p999_ms"] / base["p999_ms"]) if base["p999_ms"] else 0.0
    print(
        f"  concurrency {base['workers']}->{top['workers']} ({conc_scale:.0f}x): "
        f"ops/s {base['ops_per_sec']:.1f}->{top['ops_per_sec']:.1f} ({ops_scale:.2f}x), "
        f"p999 {base['p999_ms']:.2f}ms->{top['p999_ms']:.2f}ms ({tail_growth:.2f}x)"
    )
    print(
        "  Reading: a single group-committing writer owns the one write connection (ARCH 7.2).\n"
        "  As small-object concurrency rises, group commit coalesces more mutations per durability\n"
        "  barrier, so ops/s climbs sublinearly while per-op tail latency grows: arriving PUTs wait\n"
        "  behind the in-flight batch. ops/s scaling well below the concurrency multiple, together\n"
        "  with growing p999, is the operator-visible signature that the single writer + fsync rate\n"
        "  is the binding constraint (ARCH 30.1). The write-queue-depth gauge (ARCH 30.2) would\n"
        "  make this directly observable server-side; until it is wired, the request-duration\n"
        "  summary above and this client-side tail-vs-concurrency curve are the available window."
    )

    gN, dN, _ = scrape_metrics(args.endpoint)
    print("\n[metrics] after small-object sweep:")
    print(fmt_gauges(gN))
    print("\n  request-duration summary (server-side, cumulative):")
    print(fmt_duration(dN))

    print("\n" + "=" * 92)
    print("LOAD PROFILES COMPLETE")
    print("=" * 92)


if __name__ == "__main__":
    sys.exit(main())
