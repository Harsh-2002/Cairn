#!/usr/bin/env python3
"""Correctness-under-concurrency driver for conformance/stress_encrypted.sh (the encrypted-path
stress harness). It runs against the ENCRYPTED leg (CAIRN_ENCRYPT_AT_REST=true) while that server is
still hot from the warp phases, and it is the harness's most valuable GATE: throughput numbers are
advisory, but "AES-GCM still returns exactly the bytes you wrote, under concurrency" is not.

Three arms, all with a thread pool hammering the same node:

  1. transparent at-rest round-trip — concurrent PUTs of marker-rich, high-entropy bodies spanning
     BOTH sides of the 256 KiB small-object read threshold, then concurrent GETs asserting
     BYTE-EXACT equality (SHA-256 over the returned body vs the body written).
  2. on-disk proof — the committed blobs for those objects are VERSION_ENCRYPTED CRNB containers
     (magic `CRNB`, version byte 2, 34-byte trailer — crates/cairn-blob/src/compress.rs) and the
     known plaintext marker is ABSENT from the stored bytes. Same logic as conformance/encryption.py;
     here it runs after concurrent load rather than after a single quiet PUT.
  3. explicit-SSE arm — warp cannot send SSE headers, so this drives concurrent PUT/GET with
     `x-amz-server-side-encryption: AES256` and with `aws:kms` (+ the CAIRN_KMS_KEY_IDS-allow-listed
     key id), asserting the wire echo and byte-exact round-trips under concurrency. Deliberately
     small: it is a correctness arm, not a throughput arm.

Exit status 0 only if every assertion held.

Usage: stress_encrypted.py <ak> <sk> <s3-endpoint> <data-dir> <key-id> [objects] [workers]
"""
import concurrent.futures
import hashlib
import os
import sys

import boto3
from botocore.config import Config

AK, SK, EP, DATA_DIR = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
KEY_ID = sys.argv[5] if len(sys.argv) > 5 else "alias/cairn-stress"
N_OBJECTS = int(sys.argv[6]) if len(sys.argv) > 6 else 48
WORKERS = int(sys.argv[7]) if len(sys.argv) > 7 else 8

s3 = boto3.client(
    "s3", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}, retries={"total_max_attempts": 1, "mode": "standard"},
                  max_pool_connections=max(WORKERS * 2, 16)),
)

# Encrypted CRNB container trailer: 34 bytes, magic `CRNB` then the version byte;
# VERSION_ENCRYPTED == 2 (crates/cairn-blob/src/compress.rs).
VERSION_ENCRYPTED = 2
MARKER = b"PLAINTEXT-MARKER-DO-NOT-FIND-ON-DISK-"

fails = []


def check(label, cond):
    print(("    ok: " if cond else "    FAIL: ") + label)
    if not cond:
        fails.append(label)
    return cond


def body_of(size):
    """Marker-rich but high-entropy: repeated known plaintext interleaved with random bytes, so the
    block compressor cannot make the marker vanish on its own — if the marker is missing from the
    stored bytes it is because they were ENCRYPTED, which is exactly what we are proving."""
    out = bytearray()
    while len(out) < size:
        out += MARKER + os.urandom(64)
    return bytes(out[:size])


def committed_blobs(bucket):
    """Opaque-id blob files committed under $DATA/<bucket>/ (never named by key)."""
    d = os.path.join(DATA_DIR, bucket)
    return [os.path.join(d, f) for f in sorted(os.listdir(d))] if os.path.isdir(d) else []


def trailer_encrypted(blob):
    return len(blob) >= 34 and blob[-34:-30] == b"CRNB" and blob[-30] == VERSION_ENCRYPTED


def assert_disk_encrypted(bucket, label, sample=8):
    blobs = committed_blobs(bucket)
    check(f"{label}: committed blobs exist on disk ({len(blobs)})", len(blobs) > 0)
    bad_trailer, bad_marker = [], []
    for path in blobs[:sample]:
        with open(path, "rb") as fh:
            blob = fh.read()
        if not trailer_encrypted(blob):
            bad_trailer.append(os.path.basename(path))
        if MARKER in blob:
            bad_marker.append(os.path.basename(path))
    check(f"{label}: every sampled blob is a VERSION_ENCRYPTED CRNB container "
          f"(bad: {bad_trailer or 'none'})", not bad_trailer)
    check(f"{label}: the known plaintext marker is ABSENT from every sampled blob "
          f"(leaked: {bad_marker or 'none'})", not bad_marker)


# Sizes straddle the 256 KiB small-object inline-read threshold: an encrypted object is disqualified
# from BOTH the sendfile zero-copy path and the inline read, so both sides fall back to the streamed
# read. Correctness must hold on both.
SIZES = [64 * 1024, 200 * 1024, 300 * 1024, 1024 * 1024]


def concurrent_roundtrip(bucket, label, sse=None, key_id=None):
    """PUT N objects concurrently, GET them all back concurrently, assert byte-exact."""
    s3.create_bucket(Bucket=bucket)
    plan = {f"obj-{i:04d}": body_of(SIZES[i % len(SIZES)]) for i in range(N_OBJECTS)}
    digests = {k: hashlib.sha256(v).hexdigest() for k, v in plan.items()}
    put_errs, echo_errs = [], []

    def put_one(item):
        key, body = item
        extra = {}
        if sse == "AES256":
            extra["ServerSideEncryption"] = "AES256"
        elif sse == "aws:kms":
            extra["ServerSideEncryption"] = "aws:kms"
            extra["SSEKMSKeyId"] = key_id
        try:
            r = s3.put_object(Bucket=bucket, Key=key, Body=body, **extra)
        except Exception as exc:  # noqa: BLE001 - any failure is a harness failure
            put_errs.append(f"{key}: {exc}")
            return
        if sse and r.get("ServerSideEncryption") != sse:
            echo_errs.append(f"{key}: echoed {r.get('ServerSideEncryption')!r}, want {sse!r}")
        if sse == "aws:kms" and r.get("SSEKMSKeyId") != key_id:
            echo_errs.append(f"{key}: echoed key id {r.get('SSEKMSKeyId')!r}, want {key_id!r}")

    get_errs, mismatch = [], []

    def get_one(key):
        try:
            got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as exc:  # noqa: BLE001
            get_errs.append(f"{key}: {exc}")
            return
        if hashlib.sha256(got).hexdigest() != digests[key] or len(got) != len(plan[key]):
            mismatch.append(f"{key}: got {len(got)}B digest mismatch")

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        list(pool.map(put_one, plan.items()))
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        list(pool.map(get_one, list(plan)))

    check(f"{label}: all {N_OBJECTS} concurrent PUTs succeeded (errors: {put_errs[:3] or 'none'})",
          not put_errs)
    check(f"{label}: all {N_OBJECTS} concurrent GETs succeeded (errors: {get_errs[:3] or 'none'})",
          not get_errs)
    check(f"{label}: every object read back BYTE-EXACT under concurrency "
          f"(mismatches: {mismatch[:3] or 'none'})", not mismatch)
    if sse:
        check(f"{label}: every PUT echoed the SSE headers (bad: {echo_errs[:3] or 'none'})",
              not echo_errs)


def main():
    print("  [1/3] transparent at-rest: concurrent round-trip across the small-object threshold")
    concurrent_roundtrip("enc-verify", "at-rest")
    print("  [2/3] on-disk proof after concurrent load")
    assert_disk_encrypted("enc-verify", "at-rest")

    print("  [3/3] explicit-SSE arm (warp cannot send SSE headers)")
    concurrent_roundtrip("enc-sse-s3", "SSE-S3", sse="AES256")
    assert_disk_encrypted("enc-sse-s3", "SSE-S3")
    concurrent_roundtrip("enc-sse-kms", "SSE-KMS", sse="aws:kms", key_id=KEY_ID)
    assert_disk_encrypted("enc-sse-kms", "SSE-KMS")

    if fails:
        print(f"  correctness arm FAILED ({len(fails)}): " + "; ".join(fails[:5]))
        return 1
    print("  correctness arm PASSED (byte-exact under concurrency; ciphertext on disk)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
