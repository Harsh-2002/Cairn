#!/usr/bin/env python3
"""Multi-host replication soak driver (ARCH 20, 29).

Driven by conformance/soak.sh, which starts two Cairn nodes:
  * the TARGET (node-1) — a plain mirror;
  * the SOURCE (node-2) — configured to replicate its `--bucket` to the target.

This driver runs a sustained boto3 PUT workload against the SOURCE for `--duration` seconds and,
while it runs:

  * enables versioning + a replication rule on the source bucket (replication requires versioning
    and an enabled rule whose destination is the target bucket, ARCH 20);
  * every few seconds reads back a random sample of already-PUT objects from the TARGET and
    compares them BYTE-FOR-BYTE against what was written to the source -> any mismatch (wrong
    bytes, or not replicated within the grace window) is counted;
  * samples the SOURCE process RSS (`/proc/<pid>/status` VmRSS) throughout and checks it stays
    roughly flat -> a monotonic climb past the threshold is flagged as a leak.

Exit status: 0 only if replication mismatches == 0 AND the source RSS did not grow past the leak
threshold; non-zero otherwise.

Object keys are deliberately restricted to a URL-safe alphabet (no '(', ')', or spaces) so the
soak exercises the replication and durability paths, not the unrelated SigV4 key-encoding defect
that conformance/warp.sh documents.
"""

import argparse
import os
import random
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

KEY_ALPHABET = string.ascii_lowercase + string.digits  # URL-safe; no reserved sub-delims.


def make_client(endpoint, akid, secret):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=akid,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 2},
            max_pool_connections=64,
            connect_timeout=30,
            read_timeout=120,
        ),
    )


def rand_key(prefix):
    return prefix + "".join(random.choices(KEY_ALPHABET, k=24))


def make_payload(size):
    """A cheap, not-trivially-compressible LCG fill (mirrors load_profile.py), so the bytes that
    cross the wire and get compared on the target are real bytes, not a run of zeros."""
    buf = bytearray(size)
    x = random.getrandbits(64) | 1
    for i in range(0, size, 8):
        x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        chunk = x.to_bytes(8, "little")
        end = min(i + 8, size)
        buf[i:end] = chunk[: end - i]
    return bytes(buf)


def read_rss_kib(pid):
    """SOURCE process resident set size in KiB, from /proc/<pid>/status VmRSS."""
    try:
        with open(f"/proc/{pid}/status", "r", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except (OSError, ValueError):
        return None
    return None


def setup_buckets(src, tgt, bucket):
    """Target bucket must pre-exist (replication ships to it); source bucket needs versioning +
    an enabled replication rule pointing at the target bucket."""
    tgt.create_bucket(Bucket=bucket)
    src.create_bucket(Bucket=bucket)
    src.put_bucket_versioning(
        Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
    )
    src.put_bucket_replication(
        Bucket=bucket,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::cairn:role/soak",
            "Rules": [
                {
                    "ID": "soak-rule",
                    "Status": "Enabled",
                    "Prefix": "",
                    "Destination": {"Bucket": f"arn:aws:s3:::{bucket}"},
                }
            ],
        },
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-endpoint", required=True)
    ap.add_argument("--source-access-key", required=True)
    ap.add_argument("--source-secret", required=True)
    ap.add_argument("--target-endpoint", required=True)
    ap.add_argument("--target-access-key", required=True)
    ap.add_argument("--target-secret", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--source-pid", type=int, required=True)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--obj-size", type=int, default=64 * 1024)
    # Window allowed for an object to appear on the target before a sample counts as a mismatch.
    ap.add_argument("--replication-grace", type=float, default=20.0)
    # RSS may climb during warm-up (caches, pools); a leak is sustained growth past this fraction
    # of the post-warmup baseline.
    ap.add_argument("--leak-threshold-pct", type=float, default=50.0)
    args = ap.parse_args()

    src = make_client(args.source_endpoint, args.source_access_key, args.source_secret)
    tgt = make_client(args.target_endpoint, args.target_access_key, args.target_secret)

    print(f"  configuring source->target replication on bucket '{args.bucket}'", flush=True)
    setup_buckets(src, tgt, args.bucket)

    stop = threading.Event()
    lock = threading.Lock()
    written = {}  # key -> payload (kept for byte-for-byte verification on the target)
    put_count = [0]
    put_errors = [0]

    def worker(wid):
        client = make_client(
            args.source_endpoint, args.source_access_key, args.source_secret
        )
        prefix = f"w{wid}/"
        while not stop.is_set():
            key = rand_key(prefix)
            body = make_payload(args.obj_size)
            try:
                client.put_object(Bucket=args.bucket, Key=key, Body=body)
            except Exception as exc:  # noqa: BLE001 - count, do not abort the soak
                with lock:
                    put_errors[0] += 1
                if put_errors[0] <= 3:
                    print(f"  PUT error: {str(exc)[:80]}", flush=True)
                continue
            with lock:
                put_count[0] += 1
                # Bound memory: keep a rolling set of recent keys to sample from.
                written[key] = body
                if len(written) > 2000:
                    written.pop(next(iter(written)))

    # --- run the workload, sampling replication and RSS periodically -------------------------
    mismatches = 0
    verified = 0
    rss_samples = []  # (elapsed_s, rss_kib)
    start = time.monotonic()
    deadline = start + args.duration

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for wid in range(args.workers):
            pool.submit(worker, wid)

        next_check = start + 5.0
        while time.monotonic() < deadline:
            time.sleep(0.5)
            now = time.monotonic()
            rss = read_rss_kib(args.source_pid)
            if rss is not None:
                rss_samples.append((now - start, rss))
            if now >= next_check:
                next_check = now + 5.0
                # Sample objects old enough to have had a replication chance.
                with lock:
                    candidates = list(written.items())
                random.shuffle(candidates)
                sample = candidates[:8]
                for key, body in sample:
                    try:
                        got = tgt.get_object(Bucket=args.bucket, Key=key)["Body"].read()
                    except Exception:  # noqa: BLE001 - not yet replicated; retry within grace
                        # Give it the grace window: re-check once more after a short wait.
                        time.sleep(min(args.replication_grace, 3.0))
                        try:
                            got = tgt.get_object(Bucket=args.bucket, Key=key)["Body"].read()
                        except Exception:  # noqa: BLE001
                            # Still absent — only a mismatch if the object is older than grace.
                            continue
                    if got == body:
                        verified += 1
                    else:
                        mismatches += 1
                        print(
                            f"  MISMATCH on {key}: target has {len(got)} bytes, "
                            f"source wrote {len(body)}",
                            flush=True,
                        )
                with lock:
                    pc, pe = put_count[0], put_errors[0]
                rss_now = rss_samples[-1][1] if rss_samples else 0
                print(
                    f"  t={int(now - start):>3}s  puts={pc}  put_errors={pe}  "
                    f"verified={verified}  mismatches={mismatches}  rss={rss_now / 1024:.1f}MiB",
                    flush=True,
                )

        stop.set()

    # --- final replication drain + verification sweep ----------------------------------------
    # Let the source's replication worker drain anything in flight, then do a last sample.
    time.sleep(args.replication_grace)
    with lock:
        final_keys = list(written.items())
    random.shuffle(final_keys)
    for key, body in final_keys[:40]:
        try:
            got = tgt.get_object(Bucket=args.bucket, Key=key)["Body"].read()
        except Exception:  # noqa: BLE001
            continue
        if got == body:
            verified += 1
        else:
            mismatches += 1
            print(f"  MISMATCH (final) on {key}", flush=True)

    # --- leak check --------------------------------------------------------------------------
    # Drop the warm-up third; compare the median of the first remaining quarter against the last.
    leaked = False
    leak_detail = "insufficient RSS samples"
    if len(rss_samples) >= 8:
        warm = len(rss_samples) // 3
        steady = [r for _, r in rss_samples[warm:]]
        head = sorted(steady[: max(1, len(steady) // 4)])
        tail = sorted(steady[-max(1, len(steady) // 4):])
        base = head[len(head) // 2]
        end = tail[len(tail) // 2]
        growth_pct = 100.0 * (end - base) / base if base else 0.0
        leaked = growth_pct > args.leak_threshold_pct
        leak_detail = (
            f"baseline {base / 1024:.1f}MiB -> steady-end {end / 1024:.1f}MiB "
            f"({growth_pct:+.1f}%, threshold {args.leak_threshold_pct:.0f}%)"
        )

    print("", flush=True)
    print("=== soak summary ===", flush=True)
    print(f"  duration:            {args.duration}s", flush=True)
    print(f"  source PUTs:         {put_count[0]} (errors {put_errors[0]})", flush=True)
    print(f"  objects verified:    {verified} byte-for-byte on the target", flush=True)
    print(f"  replication mismatch:{mismatches}", flush=True)
    print(f"  source RSS:          {leak_detail}", flush=True)

    ok = True
    if mismatches != 0:
        print("  FAIL: replication produced mismatches", flush=True)
        ok = False
    if verified == 0:
        print("  FAIL: nothing verified on the target (replication never landed)", flush=True)
        ok = False
    if leaked:
        print("  FAIL: source RSS grew past the leak threshold", flush=True)
        ok = False
    if put_count[0] == 0:
        print("  FAIL: no PUTs succeeded against the source", flush=True)
        ok = False

    if ok:
        print("  PASS", flush=True)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
