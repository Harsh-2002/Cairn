#!/usr/bin/env python3
"""Two-node REPLICATION-under-sustained-load driver for conformance/stress_replication.sh (ARCH 20,
27, 29-30).

THE GAP THIS FILLS. `soak.sh` is the only other multi-node soak: it ships PLAINTEXT objects at one
fixed 64 KiB size between two nodes and samples nothing but the SOURCE's RSS. Nothing anywhere
crosses a MASTER-KEY BOUNDARY under sustained load, and nothing watches the replication OUTBOX for
backlog growth while writes continue. `mesh.py` wires five nodes with distinct keys but drives a
handful of objects per scenario, not a load.

THE TOPOLOGY (the launcher builds it; see its header for the wiring choice):
  * SOURCE and TARGET have DIFFERENT master keys. That is the whole point: the source's per-version
    DEK is sealed under the SOURCE ring and cannot be shipped verbatim, so a replica that reads back
    byte-exact on the target proves the source DECRYPTED to logical bytes and the target RE-SEALED
    under its OWN ring.
  * The TARGET runs `CAIRN_ENCRYPT_AT_REST=true`, so every replica it accepts is committed as a
    VERSION_ENCRYPTED CRNB container under the TARGET's key. The source stores its `repl-plain`
    blobs as plain files. Both halves are asserted ON DISK, which is what makes "the target stores
    its own sse_descriptor" a fact rather than a claim.

THE LOAD IS CONSTANT. A FIXED-SIZE worker pool loops without sleeping until the deadline, so offered
concurrency is identical from the first second to the last. Every worker owns a private key
namespace (`w<N>/...`) in every bucket, so a self-inflicted 404 can never fire the zero-errors gate.

THE MIX (all continuous, all against the source):
  * single-part PUTs into a versioned bucket;
  * VERSION CHURN — a small owned key set overwritten repeatedly, so a key carries many versions;
  * DELETE MARKERS on churn keys, so the marker-replication path runs all run;
  * MULTIPART-COMPLETED uploads (a real >= 5 MiB non-final part), so an assembled blob replicates;
  * an SSE arm (`AES256` + `aws:kms`, single-part AND multipart) into a second versioned bucket
    — GATED exactly like the plaintext leg; see below.

=== WHY THE SSE ARM IS GATED (this harness IS the regression guard) ================================
This driver was written while `ReplicationEngine::put_object` opened the source blob with
`BlobStore::open` — with NO DEK — so a server-side-encrypted source version shipped its raw
CIPHERTEXT, truncated to `size_logical`, as if it were the logical body. Single-part versions were
refused `400 BadDigest` by the destination's MD5 check and therefore NEVER REPLICATED AT ALL;
multipart-completed versions carry a composite `<md5>-<n>` ETag the destination cannot verify, so
the ciphertext was ACCEPTED and the replica existed, had exactly the right size, answered `200`, and
was GARBAGE. The SSE arm was reported and not gated because the product was broken.

The engine now resolves `row.sse_descriptor` through `cairn_types::sse::open_dek` and reads via
`open_with_dek`, so it ships PLAINTEXT. The arm is therefore GATED: every SSE-encrypted source
version must be present on the target and byte-exact, exactly like a plaintext one. Nothing else in
CI would catch a regression of that fix — this is the guard. Do not soften these gates back into
"reported": an un-gated SSE arm is what let the bug live.

NOTE FOR THE TWO-NODE RIG: because replication now ships DECRYPTED bodies, a client-encrypted
(SSE-S3 / SSE-KMS) object is REFUSED by the sink when the destination endpoint is plaintext
`http://`, unless `CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP=true`. The launcher must set that
knob (or use an https endpoint) or every object on this arm will sit rescheduled and the gates below
will fail with "missing on the target" — which is the guard working, not a flake.
====================================================================================================

GATES — every one valid REGARDLESS OF OFFERED LOAD (the lesson stress.sh / stress_multipart.py
encode), so they hold on a contended 2-core runner:
  * BYTE-EXACT ACROSS THE KEY BOUNDARY (the headline) — every version on BOTH legs, plaintext and
    SSE-encrypted, read from the TARGET by the SOURCE's version id, is byte-identical to what was
    written. A count, not a rate.
  * FAIL-CLOSED on both legs — no version is ever present on the target with WRONG bytes. For the
    SSE leg this is the specific shape of the old silent corruption: right size, `200`, garbage.
  * VERSION-ID IDENTITY — the target resolves the source's version id (that is how the byte-exact
    read is issued), and a sampled churn key's whole version-id SET is identical on both nodes.
  * DELETE MARKERS — every replicated marker exists on the target with the SAME version id and the
    key answers EXACTLY `NoSuchKey` / 404 there (an exact code, never "some 4xx").
  * ON-DISK RE-ENCRYPTION — target blobs are VERSION_ENCRYPTED CRNB with the plaintext marker
    absent; the corresponding SOURCE blobs are NOT containers and DO contain the marker. The target
    re-encrypted under its own key; it did not store what the source sent.
  * OUTBOX DRAINS — after writes stop, pending+claimed reaches 0 within a bounded wait, with a
    no-progress STALL detector. A monotonically growing backlog is the failure this harness exists
    to catch; the in-run backlog SHAPE is reported but not gated (see the launcher header).
  * zero driver operation errors; `/healthz` on BOTH nodes never STOPS answering (a wedge timeout,
    not a latency budget).
  * NON-VACUITY — every assertion loop has a non-zero-count companion, and every env knob that could
    empty a loop is validated below.

ADVISORY (printed + written to the JSON, never gating): convergence-latency percentiles, replication
throughput, the outbox-depth series, and the lag metric. All of
those are load- and build-profile-bound (CI drives the DEBUG artifact, whose AES-GCM is unoptimized
software crypto).

Usage: stress_replication.py <src-ak> <src-sk> <src-ep> <src-ui-ep> <tgt-ak> <tgt-sk> <tgt-ep>
                             <src-data-dir> <tgt-data-dir> <kms-key-id> <out-json>
"""
import collections
import concurrent.futures
import hashlib
import http.client
import json
import os
import statistics
import sys
import threading
import time
import urllib.parse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

(SRC_AK, SRC_SK, SRC_EP, SRC_UI, TGT_AK, TGT_SK, TGT_EP,
 SRC_DATA, TGT_DATA) = sys.argv[1:10]
KEY_ID = sys.argv[10] if len(sys.argv) > 10 else "alias/cairn-replstress"
OUT_JSON = sys.argv[11] if len(sys.argv) > 11 else ""


def env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


MIN_PART = 5 * 1024 * 1024          # S3's hard minimum for a NON-FINAL multipart part

RUN_SECS = env_int("REPL_SECS", 75)
WORKERS = env_int("REPL_WORKERS", 4)
MP_WORKERS = env_int("REPL_MP_WORKERS", 1)
OBJ_SIZE = env_int("REPL_OBJ_SIZE", 32 * 1024)
MP_PART = env_int("REPL_MP_PART", MIN_PART)
MP_TAIL = env_int("REPL_MP_TAIL", 32 * 1024)
VER_KEYS = env_int("REPL_VER_KEYS", 3)        # churn keys per worker (accumulate versions)
DELETE_EVERY = env_int("REPL_DELETE_EVERY", 10)   # 1-in-N churn iterations issues a delete marker
SSE_EVERY = env_int("REPL_SSE_EVERY", 3)      # 1-in-N worker iterations goes to the SSE (gap) leg
MP_SSE_EVERY = env_int("REPL_MP_SSE_EVERY", 4)   # 1-in-N multipart uploads is an aws:kms upload
CONV_SAMPLE_EVERY = env_int("REPL_CONV_SAMPLE_EVERY", 6)  # 1-in-N versions timed for convergence
DRAIN_TIMEOUT = env_int("REPL_DRAIN_TIMEOUT", 300)
DRAIN_STALL_SECS = env_int("REPL_DRAIN_STALL_SECS", 90)
HEALTHZ_TIMEOUT = env_int("REPL_HEALTHZ_TIMEOUT", 60)
VERIFY_THREADS = env_int("REPL_VERIFY_THREADS", 8)
DISK_SAMPLE = env_int("REPL_DISK_SAMPLE", 12)
LATEST_SAMPLE = env_int("REPL_LATEST_SAMPLE", 12)

# --- non-vacuity guards on the knobs -------------------------------------------------------------
# Every one of these would let an assertion loop iterate zero times and "pass". A harness that can be
# silently neutered by an env var is worse than no harness.
if RUN_SECS < 30:
    sys.exit(f"REPL_SECS must be >= 30 so every arm (churn, markers, multipart, SSE) actually runs "
             f"(got {RUN_SECS})")
if WORKERS < 2:
    sys.exit(f"REPL_WORKERS must be >= 2 for concurrent source writes (got {WORKERS})")
if MP_WORKERS < 1:
    sys.exit(f"REPL_MP_WORKERS must be >= 1 or no assembled blob ever replicates (got {MP_WORKERS})")
if VER_KEYS < 2:
    sys.exit(f"REPL_VER_KEYS must be >= 2 for versions to accumulate per key (got {VER_KEYS})")
if DELETE_EVERY < 2:
    sys.exit(f"REPL_DELETE_EVERY must be >= 2 so churn keys keep BOTH versions and markers "
             f"(got {DELETE_EVERY})")
if SSE_EVERY < 2:
    sys.exit(f"REPL_SSE_EVERY must be >= 2 so BOTH the plaintext leg and the SSE arm run "
             f"(got {SSE_EVERY})")
if MP_SSE_EVERY < 2:
    sys.exit(f"REPL_MP_SSE_EVERY must be >= 2 so plaintext multipart still runs (got {MP_SSE_EVERY})")
if MP_PART < MIN_PART:
    sys.exit(f"REPL_MP_PART must be >= {MIN_PART} (S3's non-final part minimum) or Complete rejects "
             f"the upload and the multipart arm asserts nothing (got {MP_PART})")
if OBJ_SIZE < 1024:
    sys.exit(f"REPL_OBJ_SIZE must be >= 1024 (got {OBJ_SIZE})")
if CONV_SAMPLE_EVERY < 1 or VERIFY_THREADS < 1 or DISK_SAMPLE < 1 or LATEST_SAMPLE < 1:
    sys.exit("REPL_CONV_SAMPLE_EVERY / REPL_VERIFY_THREADS / REPL_DISK_SAMPLE / REPL_LATEST_SAMPLE "
             "must each be >= 1")
if DRAIN_TIMEOUT < 30 or DRAIN_STALL_SECS < 10:
    sys.exit("REPL_DRAIN_TIMEOUT must be >= 30 and REPL_DRAIN_STALL_SECS >= 10")

POOL = WORKERS + MP_WORKERS + 4

# `total_max_attempts` (NOT `max_attempts`) is what disables retries. botocore reads `max_attempts`
# as the number of RETRIES and derives total = max_attempts + 1, so `retries={"max_attempts": 1}`
# still allows ONE retry — which would silently rewrite a deliberate rejection (the target's
# `NoSuchKey` behind a delete marker) into whatever a second attempt returned. One attempt, no retry.
CFG = Config(s3={"addressing_style": "path"},
             retries={"total_max_attempts": 1, "mode": "standard"},
             connect_timeout=20, read_timeout=300, max_pool_connections=POOL * 2)


def s3_client(ep, akid, secret):
    return boto3.client("s3", endpoint_url=ep, aws_access_key_id=akid,
                        aws_secret_access_key=secret, region_name="us-east-1", config=CFG)


src = s3_client(SRC_EP, SRC_AK, SRC_SK)
tgt = s3_client(TGT_EP, TGT_AK, TGT_SK)

B_PLAIN = "repl-plain"      # the GATED leg: plaintext at the source, re-encrypted at the target
B_SSE = "repl-sse"          # the PINNED-GAP arm: SSE-S3 / aws:kms at the source

# Encrypted CRNB container trailer: 34 bytes, magic `CRNB` then the version byte; VERSION_ENCRYPTED
# == 2 (crates/cairn-blob/src/compress.rs). Same shape encryption.py / stress_encrypted.py assert.
VERSION_ENCRYPTED = 2
MARKER = b"PLAINTEXT-MARKER-DO-NOT-FIND-ON-DISK-"

LOCK = threading.Lock()
COUNTS = collections.Counter()
ERRORS = []          # capped sample; COUNTS["errors"] is the true total
RECORDS = []         # every version written: verified against the TARGET after the drain
MARKERS = []         # every delete marker written
CONV_Q = collections.deque()   # sampled (record, t_put) awaiting a convergence-latency measurement
CONV_MS = []         # advisory: measured convergence latencies, milliseconds
STATE = {"stop": False}


def bump(name, n=1):
    with LOCK:
        COUNTS[name] += n


def err_of(exc):
    if isinstance(exc, ClientError):
        return (exc.response["ResponseMetadata"]["HTTPStatusCode"],
                exc.response["Error"].get("Code"))
    return (None, type(exc).__name__)


def oops(where, exc):
    with LOCK:
        COUNTS["errors"] += 1
        if len(ERRORS) < 40:
            ERRORS.append(f"{where}: {type(exc).__name__} {err_of(exc)}")


def running(deadline):
    return not STATE["stop"] and time.monotonic() < deadline


# --- bodies ---------------------------------------------------------------------------------------
# The pool is generated ONCE up front: the workers must spend their time in the server, not in
# `os.urandom`, or the offered load would be bounded by the driver's own CPU and would drift as the
# box warms up. Each body is then made UNIQUE per write by overwriting a 40-byte header, so a
# byte-exact comparison really pins THIS version's bytes and not merely "some body from the pool".
# Marker-rich but high-entropy (the encryption.py shape) so the block compressor cannot make the
# plaintext marker vanish on its own — an absent marker on disk means ENCRYPTED, which is the proof.
def body_of(size, salt):
    out = bytearray()
    while len(out) < size:
        out += MARKER + salt + os.urandom(48)
    return bytes(out[:size])


BODIES = [body_of(OBJ_SIZE, f"-{i:02d}-".encode()) for i in range(8)]
MP_HEAD = body_of(MP_PART, b"-mp-")
MP_TAIL_B = body_of(MP_TAIL, b"-tl-")
_SERIAL = [0]


HEADER = 40


def unique_body(base):
    """`base` with a unique 40-byte header stamped over its front — a memcpy, not a re-randomise.

    The header must be EXACTLY `HEADER` bytes: a short one would SHORTEN the body, which silently
    pushed every non-final multipart part below S3's 5 MiB floor and turned the whole multipart arm
    into `EntityTooSmall` (caught live). The `ljust` + slice makes the length unconditional.
    """
    with LOCK:
        _SERIAL[0] += 1
        n = _SERIAL[0]
    head = (f"CAIRN-REPL-{n:012d}-".encode() + os.urandom(HEADER)).ljust(HEADER, b"=")[:HEADER]
    assert len(head) == HEADER
    return head + base[HEADER:]


def record(bucket, key, vid, body, leg, kind):
    rec = {"bucket": bucket, "key": key, "vid": vid,
           "sha": hashlib.sha256(body).hexdigest(), "size": len(body),
           "leg": leg, "kind": kind}
    with LOCK:
        RECORDS.append(rec)
        if len(RECORDS) % CONV_SAMPLE_EVERY == 0 and leg == "plain":
            CONV_Q.append((rec, time.monotonic()))
    return rec


# --- setup ----------------------------------------------------------------------------------------
def setup():
    """Both buckets must exist and be VERSIONED on BOTH nodes: replication requires versioning on the
    source (ARCH 20), and the target must be versioned for the preserved source version id to remain
    addressable after the next overwrite."""
    for cli in (tgt, src):
        for b in (B_PLAIN, B_SSE):
            try:
                cli.create_bucket(Bucket=b)
            except ClientError as exc:
                # Tolerated ONLY for the idempotent re-create; anything else is a real failure.
                if err_of(exc)[1] != "BucketAlreadyOwnedByYou":
                    raise
            cli.put_bucket_versioning(Bucket=b, VersioningConfiguration={"Status": "Enabled"})
    for b in (B_PLAIN, B_SSE):
        src.put_bucket_replication(
            Bucket=b,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::cairn:role/stress-replication",
                "Rules": [{
                    "ID": f"{b}-rule",
                    "Status": "Enabled",
                    "Priority": 1,
                    "Filter": {"Prefix": ""},
                    # Markers only propagate when the rule says so; the marker gate depends on it.
                    "DeleteMarkerReplication": {"Status": "Enabled"},
                    "Destination": {"Bucket": f"arn:aws:s3:::{b}"},
                }],
            })


# --- workers --------------------------------------------------------------------------------------
def put_worker(wid, deadline):
    """Constant single-part + version-churn load. Never sleeps; the deadline is the only exit."""
    cli = s3_client(SRC_EP, SRC_AK, SRC_SK)
    churn = [f"w{wid}/churn-{i}" for i in range(VER_KEYS)]
    i = 0
    churn_n = 0
    while running(deadline):
        i += 1
        body = unique_body(BODIES[i % len(BODIES)])
        try:
            if i % SSE_EVERY == 0:
                # --- the PINNED-GAP arm: server-side-encrypted source versions -------------------
                sse = "AES256" if (i // SSE_EVERY) % 2 == 0 else "aws:kms"
                kw = {"ServerSideEncryption": sse}
                if sse == "aws:kms":
                    kw["SSEKMSKeyId"] = KEY_ID
                key = f"w{wid}/sse-{i:06d}"
                resp = cli.put_object(Bucket=B_SSE, Key=key, Body=body, **kw)
                if resp.get("ServerSideEncryption") != sse:
                    with LOCK:
                        ERRORS.append(f"source did not echo {sse} on {key}")
                        COUNTS["errors"] += 1
                record(B_SSE, key, resp["VersionId"], body, "sse", sse)
                bump("sse_puts")
                bump("sse_aes256" if sse == "AES256" else "sse_kms")
            elif i % DELETE_EVERY == 0:
                # --- TOMB: written once, deleted once, NEVER touched again ------------------------
                # A churn key's marker can be superseded by the next churn PUT, so "the target hides
                # the key" is not deterministic there. A tomb key's marker is definitively the newest
                # thing on that key for the rest of the run, which is what makes the exact
                # `404 NoSuchKey` assertion on the target sound instead of racy.
                key = f"w{wid}/tomb-{i:06d}"
                resp = cli.put_object(Bucket=B_PLAIN, Key=key, Body=body)
                record(B_PLAIN, key, resp["VersionId"], body, "plain", "tomb")
                bump("tomb_puts")
                d = cli.delete_object(Bucket=B_PLAIN, Key=key)
                with LOCK:
                    MARKERS.append({"bucket": B_PLAIN, "key": key,
                                    "vid": d.get("VersionId"), "kind": "tomb"})
                bump("delete_markers")
                bump("tomb_markers")
            elif i % 2 == 0:
                # --- version churn: the same key overwritten, so versions accumulate -------------
                key = churn[i % VER_KEYS]
                resp = cli.put_object(Bucket=B_PLAIN, Key=key, Body=body)
                record(B_PLAIN, key, resp["VersionId"], body, "plain", "churn")
                bump("churn_puts")
                churn_n += 1
                if churn_n % DELETE_EVERY == 0:
                    # A marker on a key that ALREADY carries many versions — the interesting
                    # marker-replication shape. Its version id is gated; whether it still hides the
                    # key at the end is not, because a later churn PUT legitimately supersedes it.
                    d = cli.delete_object(Bucket=B_PLAIN, Key=key)
                    with LOCK:
                        MARKERS.append({"bucket": B_PLAIN, "key": key,
                                        "vid": d.get("VersionId"), "kind": "churn"})
                    bump("delete_markers")
                    bump("churn_markers")
            else:
                key = f"w{wid}/obj-{i:06d}"
                resp = cli.put_object(Bucket=B_PLAIN, Key=key, Body=body)
                record(B_PLAIN, key, resp["VersionId"], body, "plain", "single")
                bump("single_puts")
        except Exception as exc:  # noqa: BLE001 — count, never abort the run
            oops(f"put-{wid}", exc)


def mp_worker(wid, deadline):
    """Continuous multipart-COMPLETED uploads: a real >= 5 MiB non-final part plus a small tail, so
    an ASSEMBLED blob (not just a single-part stage) crosses the replication boundary."""
    cli = s3_client(SRC_EP, SRC_AK, SRC_SK)
    i = 0
    while running(deadline):
        i += 1
        head = unique_body(MP_HEAD)
        body = head + MP_TAIL_B
        kms = (i % MP_SSE_EVERY == 0)
        bucket = B_SSE if kms else B_PLAIN
        key = f"m{wid}/mp-{i:05d}"
        upload = None
        try:
            kw = ({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KEY_ID} if kms else {})
            upload = cli.create_multipart_upload(Bucket=bucket, Key=key, **kw)["UploadId"]
            parts = []
            for n, chunk in enumerate((head, MP_TAIL_B), start=1):
                e = cli.upload_part(Bucket=bucket, Key=key, UploadId=upload,
                                    PartNumber=n, Body=chunk)
                parts.append({"ETag": e["ETag"], "PartNumber": n})
            resp = cli.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={"Parts": parts})
            record(bucket, key, resp["VersionId"], body,
                   "sse" if kms else "plain", "multipart")
            bump("mp_kms_completes" if kms else "mp_completes")
        except Exception as exc:  # noqa: BLE001
            oops(f"mp-{wid}", exc)
            if upload:
                try:
                    cli.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
                except Exception:  # noqa: BLE001
                    pass


def conv_prober(deadline):
    """ADVISORY convergence latency: for sampled plaintext-leg versions, the wall time from the
    source PUT returning to the target first answering a version-scoped HEAD. Never a gate — it is
    pure offered-load-and-hardware, and the drain gate is what actually protects the pipeline."""
    cli = s3_client(TGT_EP, TGT_AK, TGT_SK)
    while running(deadline):
        with LOCK:
            item = CONV_Q.popleft() if CONV_Q else None
        if item is None:
            time.sleep(0.05)
            continue
        rec, t0 = item
        while running(deadline):
            try:
                cli.head_object(Bucket=rec["bucket"], Key=rec["key"], VersionId=rec["vid"])
                with LOCK:
                    CONV_MS.append((time.monotonic() - t0) * 1000.0)
                break
            except ClientError:
                time.sleep(0.1)
            except Exception as exc:  # noqa: BLE001
                oops("conv-probe", exc)
                break


def healthz_prober(label, ep, deadline):
    """WEDGE detector, per node. The gate is that `/healthz` never STOPS answering, with a
    deliberately generous per-probe timeout: probe latency under a saturating workload on a debug
    build is offered load, not signal, and gating on it would be gating on runner contention."""
    netloc = urllib.parse.urlparse(ep).netloc
    host, port = netloc.split(":")[0], int(netloc.split(":")[1])
    while running(deadline):
        t0 = time.monotonic()
        try:
            conn = http.client.HTTPConnection(host, port, timeout=HEALTHZ_TIMEOUT)
            conn.request("GET", "/healthz")
            resp = conn.getresponse()
            resp.read()
            conn.close()
            bump(f"healthz_{label}_probes")
            if resp.status != 200:
                bump(f"healthz_{label}_failures")
        except Exception as exc:  # noqa: BLE001
            bump(f"healthz_{label}_failures")
            oops(f"healthz-{label}", exc)
        with LOCK:
            COUNTS[f"healthz_{label}_worst_ms"] = max(
                COUNTS[f"healthz_{label}_worst_ms"], int((time.monotonic() - t0) * 1000))
        time.sleep(1.0)


# --- the source's own outbox view -----------------------------------------------------------------
def summary():
    """`GET /api/v1/replication/summary` on the SOURCE's control listener: exact pending / claimed /
    failed / completed counts and the true lag, straight from the outbox (the `/metrics` gauges are
    the same numbers republished on a background cadence, so they are sampled as advisory instead)."""
    u = urllib.parse.urlparse(SRC_UI)
    try:
        conn = http.client.HTTPConnection(u.hostname, u.port, timeout=20)
        conn.request("GET", "/api/v1/replication/summary",
                     headers={"authorization": f"Bearer {SRC_AK}.{SRC_SK}"})
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        if resp.status != 200:
            return None
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def drain(adv):
    """Wait for the outbox to DRAIN after the writes stop. Returns (ok, detail).

    Load-independent by construction: the gate is not "how fast" but "does it reach zero at all",
    with a NO-PROGRESS stall detector. A backlog that keeps growing (or wedges) never sets a new
    minimum and fails within `DRAIN_STALL_SECS`; a merely slow-but-progressing drain keeps setting
    new minima and is allowed the whole `DRAIN_TIMEOUT`.
    """
    t0 = time.monotonic()
    best = None
    best_at = t0
    series = []
    zero_streak = 0
    while True:
        s = summary()
        if s is None:
            if time.monotonic() - t0 > 30:
                return False, "the source control API never answered /replication/summary"
            time.sleep(0.5)
            continue
        depth = int(s.get("pending", 0)) + int(s.get("claimed", 0))
        series.append(depth)
        adv["repl_failed_at_drain"] = int(s.get("failed", 0))
        adv["repl_completed_at_drain"] = int(s.get("completed", 0))
        adv["repl_lag_at_drain"] = int(s.get("lag_seconds", 0))
        if best is None or depth < best:
            best, best_at = depth, time.monotonic()
        if depth == 0:
            zero_streak += 1
            if zero_streak >= 2:      # two consecutive zeroes: nothing in flight, nothing due
                adv["drain_series"] = series[:400]
                adv["drain_secs"] = round(time.monotonic() - t0, 1)
                return True, f"pending+claimed reached 0 in {adv['drain_secs']}s"
        else:
            zero_streak = 0
        now = time.monotonic()
        if now - best_at > DRAIN_STALL_SECS:
            adv["drain_series"] = series[:400]
            adv["drain_secs"] = round(now - t0, 1)
            return False, (f"outbox STALLED: pending+claimed stuck at >= {best} for "
                           f"{DRAIN_STALL_SECS}s (now {depth}) — a backlog that never shrinks")
        if now - t0 > DRAIN_TIMEOUT:
            adv["drain_series"] = series[:400]
            adv["drain_secs"] = round(now - t0, 1)
            return False, (f"outbox did not drain within {DRAIN_TIMEOUT}s "
                           f"(pending+claimed still {depth}, best seen {best})")
        time.sleep(0.5)


# --- verification ---------------------------------------------------------------------------------
def verify_records():
    """Read EVERY recorded version from the TARGET by the SOURCE's version id and compare bytes.

    Three outcomes per record: `match` (present, byte-exact), `missing` (absent), `wrong` (present
    with different bytes). Both are gated on BOTH legs. `wrong` on the SSE leg is the exact shape of
    the silent corruption this harness now guards against: right size, `200`, garbage bytes.
    """
    out = {"match": collections.Counter(), "missing": collections.Counter(),
           "wrong": collections.Counter(), "wrong_sample": []}
    cli_local = threading.local()

    def one(rec):
        cli = getattr(cli_local, "c", None)
        if cli is None:
            cli = s3_client(TGT_EP, TGT_AK, TGT_SK)
            cli_local.c = cli
        try:
            got = cli.get_object(Bucket=rec["bucket"], Key=rec["key"],
                                 VersionId=rec["vid"])["Body"].read()
        except ClientError as exc:
            status, code = err_of(exc)
            if status == 404:
                return ("missing", rec, code)
            return ("missing", rec, f"{status}/{code}")
        except Exception as exc:  # noqa: BLE001
            return ("missing", rec, type(exc).__name__)
        if hashlib.sha256(got).hexdigest() == rec["sha"]:
            return ("match", rec, None)
        return ("wrong", rec, f"{len(got)} bytes vs {rec['size']}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=VERIFY_THREADS) as pool:
        for verdict, rec, detail in pool.map(one, RECORDS):
            out[verdict][rec["leg"]] += 1
            out[verdict][f"{rec['leg']}-{rec['kind']}"] += 1
            if verdict == "wrong":
                out.setdefault(f"wrong_{rec['leg']}", [])
                if len(out[f"wrong_{rec['leg']}"]) < 10:
                    out[f"wrong_{rec['leg']}"].append(
                        f"{rec['leg']}/{rec['kind']}:{rec['bucket']}/{rec['key']}@{rec['vid']} "
                        f"{detail}")
                if len(out["wrong_sample"]) < 10:
                    out["wrong_sample"].append(
                        f"{rec['leg']}/{rec['kind']}:{rec['bucket']}/{rec['key']}@{rec['vid']} "
                        f"{detail}")
    return out


def verify_markers():
    """Every replicated delete marker must exist on the TARGET with the SAME version id, and the key
    must answer EXACTLY `NoSuchKey` / 404 there — an exact code, never "some 4xx"."""
    bad_id, bad_code = [], []
    checked = 0
    listings = {}
    for m in MARKERS:
        if not m.get("vid"):
            bad_id.append(f"{m['key']}: source returned no marker version id")
            continue
        key = m["key"]
        if key not in listings:
            ids = set()
            token = None
            while True:
                kw = {"Bucket": m["bucket"], "Prefix": key}
                if token:
                    kw["KeyMarker"] = token
                page = tgt.list_object_versions(**kw)
                ids |= {d["VersionId"] for d in page.get("DeleteMarkers", [])
                        if d["Key"] == key}
                if not page.get("IsTruncated"):
                    break
                token = page.get("NextKeyMarker")
                if not token:
                    break
            listings[key] = ids
        checked += 1
        if m["vid"] not in listings[key]:
            bad_id.append(f"{key}@{m['vid']} not in the target's delete-marker set")
    # Only TOMB keys are probed for "the target hides it": a tomb is written once, deleted once and
    # never touched again, so its marker is definitively the newest thing on the key. A churn key's
    # marker is legitimately superseded by the next churn PUT, so asserting 404 there would be racy.
    hidden = 0
    tombs = [m["key"] for m in MARKERS if m.get("kind") == "tomb"]
    for key in tombs:
        try:
            tgt.get_object(Bucket=B_PLAIN, Key=key)
            bad_code.append(f"{key}: target still serves the object behind a delete marker")
        except ClientError as exc:
            status, code = err_of(exc)
            if status == 404 and code == "NoSuchKey":
                hidden += 1
            else:
                bad_code.append(f"{key}: expected 404 NoSuchKey, got {status} {code}")
        except Exception as exc:  # noqa: BLE001
            bad_code.append(f"{key}: {type(exc).__name__}")
    return {"checked": checked, "hidden": hidden, "tombs": len(tombs),
            "bad_id": bad_id, "bad_code": bad_code}


def verify_version_sets():
    """A sampled churn key's full version-id SET must be identical on both nodes — the strongest
    statement of version-id identity, and it also catches duplicate re-delivery on the target."""
    keys = sorted({r["key"] for r in RECORDS
                   if r["bucket"] == B_PLAIN and r["kind"] == "churn"})[:6]
    divergent = []
    for key in keys:
        def ids(cli):
            got = set()
            page = cli.list_object_versions(Bucket=B_PLAIN, Prefix=key)
            for v in page.get("Versions", []):
                if v["Key"] == key:
                    got.add(v["VersionId"])
            for d in page.get("DeleteMarkers", []):
                if d["Key"] == key:
                    got.add(d["VersionId"])
            return got
        a, b = ids(src), ids(tgt)
        if a != b:
            divergent.append(f"{key}: source-only {sorted(a - b)[:3]}, target-only {sorted(b - a)[:3]}")
    return {"keys": len(keys), "divergent": divergent}


def verify_latest_pointer():
    """A plain (non-version-scoped) GET on the target must serve the SOURCE's newest bytes for the
    key — i.e. replication moved the latest pointer, not just the version rows."""
    # Only `single` keys: they are unique per write and never deleted, so their newest version is
    # unambiguously the latest. A churn key can end the run behind a marker and a tomb key always
    # does, which would make a plain GET legitimately 404 — a race, not a signal.
    newest = {r["key"]: r for r in RECORDS
              if r["bucket"] == B_PLAIN and r["kind"] == "single"}
    sample = list(newest.values())[:LATEST_SAMPLE]
    bad = []
    for rec in sample:
        try:
            got = tgt.get_object(Bucket=B_PLAIN, Key=rec["key"])["Body"].read()
            if hashlib.sha256(got).hexdigest() != rec["sha"]:
                bad.append(f"{rec['key']}: latest body differs from the source's newest version")
        except Exception as exc:  # noqa: BLE001
            bad.append(f"{rec['key']}: {err_of(exc)}")
    return {"sampled": len(sample), "bad": bad}


def committed_blobs(data_dir, bucket):
    """Opaque-id blob files committed under <data-dir>/<bucket>/ (never named by key)."""
    d = os.path.join(data_dir, bucket)
    return [os.path.join(d, f) for f in sorted(os.listdir(d))] if os.path.isdir(d) else []


def trailer_encrypted(blob):
    return len(blob) >= 34 and blob[-34:-30] == b"CRNB" and blob[-30] == VERSION_ENCRYPTED


def scan_blobs(data_dir, bucket, sample):
    enc = plain_marker = total = 0
    names_not_enc, names_with_marker = [], []
    paths = committed_blobs(data_dir, bucket)
    for path in paths[:sample]:
        try:
            with open(path, "rb") as fh:
                blob = fh.read()
        except OSError:
            continue
        total += 1
        if trailer_encrypted(blob):
            enc += 1
        else:
            names_not_enc.append(os.path.basename(path))
        if MARKER in blob:
            plain_marker += 1
            names_with_marker.append(os.path.basename(path))
    return {"files": len(paths), "sampled": total, "encrypted": enc,
            "with_marker": plain_marker, "not_encrypted": names_not_enc[:4],
            "marker_leaked": names_with_marker[:4]}


# --- main -----------------------------------------------------------------------------------------
def main():
    print(f"  replication stress: {RUN_SECS}s constant load — {WORKERS} PUT + {MP_WORKERS} multipart "
          f"workers, source->target across DIFFERENT master keys", flush=True)
    setup()

    t0 = time.monotonic()
    deadline = t0 + RUN_SECS
    jobs = ([("put", i) for i in range(WORKERS)]
            + [("mp", i) for i in range(MP_WORKERS)]
            + [("conv", 0), ("hz-src", 0), ("hz-tgt", 0)])

    def run(job):
        kind, i = job
        try:
            if kind == "put":
                put_worker(i, deadline)
            elif kind == "mp":
                mp_worker(i, deadline)
            elif kind == "conv":
                conv_prober(deadline)
            elif kind == "hz-src":
                healthz_prober("src", SRC_EP, deadline)
            elif kind == "hz-tgt":
                healthz_prober("tgt", TGT_EP, deadline)
        except Exception as exc:  # noqa: BLE001 — a worker must never take the run down silently
            oops(f"{kind}-{i} worker", exc)

    adv = {}
    # In-run outbox depth, sampled from the source's own outbox while the load is CONSTANT. Reported,
    # NOT gated: source and target share one box, so a persistently non-zero backlog here can simply
    # mean the shipping side is CPU-starved on a debug build. The load-independent statement is the
    # DRAIN below — a backlog that never comes back down is what actually fails.
    depth_series = []

    def depth_sampler():
        while running(deadline):
            s = summary()
            if s:
                depth_series.append(int(s.get("pending", 0)) + int(s.get("claimed", 0)))
                with LOCK:
                    COUNTS["lag_peak"] = max(COUNTS["lag_peak"], int(s.get("lag_seconds", 0)))
            time.sleep(1.0)

    sampler = threading.Thread(target=depth_sampler, daemon=True)
    sampler.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        list(pool.map(run, jobs))
    STATE["stop"] = True
    load_secs = time.monotonic() - t0
    sampler.join(timeout=5)

    print(f"  load phase done in {load_secs:.1f}s — {len(RECORDS)} versions written, "
          f"{len(MARKERS)} delete markers; waiting for the outbox to drain", flush=True)
    drained, drain_detail = drain(adv)

    print("  verifying every recorded version against the TARGET", flush=True)
    ver = verify_records()
    mk = verify_markers()
    vsets = verify_version_sets()
    latest = verify_latest_pointer()
    tgt_plain_disk = scan_blobs(TGT_DATA, B_PLAIN, DISK_SAMPLE)
    src_plain_disk = scan_blobs(SRC_DATA, B_PLAIN, DISK_SAMPLE)
    src_sse_disk = scan_blobs(SRC_DATA, B_SSE, DISK_SAMPLE)

    plain_total = sum(1 for r in RECORDS if r["leg"] == "plain")
    sse_total = sum(1 for r in RECORDS if r["leg"] == "sse")
    plain_match = ver["match"]["plain"]
    sse_match = ver["match"]["sse"]

    # --- verdict ---------------------------------------------------------------------------------
    fails = []
    c = COUNTS

    def gate(label, cond):
        print(("    ok: " if cond else "    FAIL: ") + label, flush=True)
        if not cond:
            fails.append(label)

    print("\n  === correctness gates (load-independent) ===", flush=True)
    gate(f"zero driver operation errors ({c['errors']}; sample {ERRORS[:3] or 'none'})",
         c["errors"] == 0)
    # THE HEADLINE. Read from the TARGET, by the SOURCE's version id, byte-compared. The two nodes
    # hold DIFFERENT master keys, so a byte-exact read here can only happen if the source decrypted
    # to logical bytes and the target re-sealed under its own ring.
    gate(f"every plaintext-leg version is BYTE-EXACT on the target across the master-key boundary "
         f"({plain_match}/{plain_total}; missing {ver['missing']['plain']})",
         plain_total > 0 and plain_match == plain_total)
    # THE REGRESSION GUARD. Identical to the plaintext gate, on the leg that used to be reported and
    # not gated. A SSE-encrypted source version must be DECRYPTED by the engine, shipped as logical
    # bytes, and re-sealed under the TARGET's own (different) master key — so this single count
    # covers both halves of the old bug: single-part versions that never arrived at all, and
    # multipart versions that arrived as ciphertext.
    gate(f"every SSE-leg version is BYTE-EXACT on the target across the master-key boundary "
         f"({sse_match}/{sse_total}; missing {ver['missing']['sse']})",
         sse_total > 0 and sse_match == sse_total)
    gate(f"the SSE multipart arm replicated byte-exact — the SILENT-CORRUPTION shape (composite "
         f"ETag, unverifiable at the destination) "
         f"({ver['match']['sse-multipart']}/{c['mp_kms_completes']} kms completes)",
         c["mp_kms_completes"] > 0
         and ver["match"]["sse-multipart"] == c["mp_kms_completes"])
    # FAIL-CLOSED, now on BOTH legs: a replica must never exist on the target with the wrong bytes.
    gate(f"no plaintext-leg version has wrong bytes on the target "
         f"({ver['wrong']['plain']}; {ver.get('wrong_plain', [])[:2] or 'none'})",
         ver["wrong"]["plain"] == 0)
    gate(f"no SSE-leg version has wrong bytes on the target "
         f"({ver['wrong']['sse']}; {ver.get('wrong_sse', [])[:2] or 'none'})",
         ver["wrong"]["sse"] == 0)
    gate(f"the multipart arm actually ran and every assembled blob replicated byte-exact "
         f"({ver['match']['plain-multipart']}/{c['mp_completes']} completes)",
         c["mp_completes"] > 0 and ver["match"]["plain-multipart"] == c["mp_completes"])
    gate(f"the version-churn arm actually ran and replicated byte-exact "
         f"({ver['match']['plain-churn']}/{c['churn_puts']} churn PUTs)",
         c["churn_puts"] > 0 and ver["match"]["plain-churn"] == c["churn_puts"])
    gate(f"every delete marker replicated with the SAME version id ({mk['checked']} markers: "
         f"{c['tomb_markers']} tomb + {c['churn_markers']} on already-versioned keys; "
         f"{mk['bad_id'][:2] or 'none'})",
         mk["checked"] > 0 and c["tomb_markers"] > 0 and c["churn_markers"] > 0
         and not mk["bad_id"])
    gate(f"every tombstoned key answers EXACTLY 404 NoSuchKey on the target "
         f"({mk['hidden']}/{mk['tombs']} hidden; {mk['bad_code'][:2] or 'none'})",
         mk["tombs"] > 0 and mk["hidden"] == mk["tombs"] and not mk["bad_code"])
    gate(f"a churn key's whole version-id SET is identical on both nodes "
         f"({vsets['keys']} keys; {vsets['divergent'][:1] or 'none'} divergent)",
         vsets["keys"] > 0 and not vsets["divergent"])
    gate(f"the target's LATEST pointer serves the source's newest bytes "
         f"({latest['sampled']} keys; {latest['bad'][:1] or 'none'})",
         latest["sampled"] > 0 and not latest["bad"])
    # ON-DISK: the target really RE-ENCRYPTED under its own key rather than storing what it was sent.
    gate(f"every sampled TARGET blob is a VERSION_ENCRYPTED CRNB container with the plaintext marker "
         f"ABSENT ({tgt_plain_disk['encrypted']}/{tgt_plain_disk['sampled']} of "
         f"{tgt_plain_disk['files']} files; not-encrypted {tgt_plain_disk['not_encrypted'] or 'none'}, "
         f"leaked {tgt_plain_disk['marker_leaked'] or 'none'})",
         tgt_plain_disk["sampled"] > 0
         and tgt_plain_disk["encrypted"] == tgt_plain_disk["sampled"]
         and tgt_plain_disk["with_marker"] == 0)
    gate(f"the matching SOURCE blobs are NOT containers and DO carry the plaintext marker — so the "
         f"ciphertext on the target is the TARGET's own doing "
         f"({src_plain_disk['sampled'] - src_plain_disk['encrypted']}/{src_plain_disk['sampled']} "
         f"plain, {src_plain_disk['with_marker']} carry the marker)",
         src_plain_disk["sampled"] > 0 and src_plain_disk["encrypted"] == 0
         and src_plain_disk["with_marker"] == src_plain_disk["sampled"])
    gate(f"the SSE arm really encrypted at the source ({src_sse_disk['encrypted']}/"
         f"{src_sse_disk['sampled']} blobs VERSION_ENCRYPTED, {src_sse_disk['with_marker']} leaked)",
         src_sse_disk["sampled"] > 0
         and src_sse_disk["encrypted"] == src_sse_disk["sampled"]
         and src_sse_disk["with_marker"] == 0)
    gate(f"both SSE modes ran on the gated arm (AES256 {c['sse_aes256']}, aws:kms {c['sse_kms']}, "
         f"kms multipart {c['mp_kms_completes']})",
         c["sse_aes256"] > 0 and c["sse_kms"] > 0 and c["mp_kms_completes"] > 0)
    gate(f"the replication OUTBOX drained after writes stopped ({drain_detail})", drained)
    for label in ("src", "tgt"):
        gate(f"/healthz on the {label} node never stopped answering "
             f"({c[f'healthz_{label}_probes']} probes, {c[f'healthz_{label}_failures']} failures, "
             f"worst {c[f'healthz_{label}_worst_ms']} ms, wedge timeout {HEALTHZ_TIMEOUT}s)",
             c[f"healthz_{label}_probes"] > 0 and c[f"healthz_{label}_failures"] == 0)

    # The SSE leg is gated above; these counts stay in the output as diagnostics so a failure
    # report says *which* shape broke (never arrived vs. arrived corrupt) without a re-run.
    print(f"\n  SSE leg: {sse_total} encrypted source versions "
          f"(AES256 {c['sse_aes256']}, aws:kms {c['sse_kms']}, aws:kms multipart "
          f"{c['mp_kms_completes']}) — byte-exact {sse_match}, missing {ver['missing']['sse']}, "
          f"wrong bytes {ver['wrong']['sse']} (multipart {ver['wrong']['sse-multipart']})",
          flush=True)
    if ver["wrong"]["sse"]:
        print(f"    samples: {ver['wrong_sample'][:3]}", flush=True)

    # --- advisory ---------------------------------------------------------------------------------
    adv.update({k: v for k, v in c.items()})
    adv["load_secs"] = round(load_secs, 1)
    adv["versions_written"] = len(RECORDS)
    adv["markers_written"] = len(MARKERS)
    adv["plain_total"] = plain_total
    adv["plain_match"] = plain_match
    adv["sse_total"] = sse_total
    adv["sse_match"] = sse_match
    adv["sse_missing"] = ver["missing"]["sse"]
    adv["sse_corrupt_on_target"] = ver["wrong"]["sse"]
    adv["sse_corrupt_multipart"] = ver["wrong"]["sse-multipart"]
    adv["plain_wrong_bytes"] = ver["wrong"]["plain"]
    adv["wrong_bytes_sample"] = ver["wrong_sample"][:10]
    adv["write_ops_per_sec"] = round(len(RECORDS) / load_secs, 1) if load_secs > 0 else 0
    replicated_bytes = sum(r["size"] for r in RECORDS if r["leg"] == "plain")
    adv["replicated_mib"] = round(replicated_bytes / 1048576.0, 1)
    total_secs = load_secs + adv.get("drain_secs", 0)
    adv["replication_mib_per_sec"] = (round(replicated_bytes / 1048576.0 / total_secs, 2)
                                      if total_secs > 0 else 0)
    adv["replication_obj_per_sec"] = (round(plain_total / total_secs, 1) if total_secs > 0 else 0)
    if CONV_MS:
        s = sorted(CONV_MS)
        adv["convergence_samples"] = len(s)
        adv["convergence_p50_ms"] = round(statistics.median(s))
        adv["convergence_p90_ms"] = round(s[min(len(s) - 1, int(0.90 * len(s)))])
        adv["convergence_p99_ms"] = round(s[min(len(s) - 1, int(0.99 * len(s)))])
        adv["convergence_max_ms"] = round(s[-1])
    if depth_series:
        adv["outbox_depth_min"] = min(depth_series)
        adv["outbox_depth_med"] = round(statistics.median(depth_series))
        adv["outbox_depth_max"] = max(depth_series)
        adv["outbox_depth_series"] = depth_series[:400]
    adv["lag_peak_secs"] = c["lag_peak"]
    adv["target_disk"] = tgt_plain_disk
    adv["source_disk"] = src_plain_disk
    adv["source_sse_disk"] = src_sse_disk
    adv["errors_sample"] = ERRORS[:10]
    adv["failures"] = fails
    # Nothing in this mix may legitimately answer 5xx on EITHER node: the one deliberate rejection
    # here — the destination refusing an encrypted source version — is a 400 BadDigest. The budget is
    # therefore exactly zero on both nodes, and the launcher gates each node's counter against it.
    adv["declared_5xx"] = 0
    if OUT_JSON:
        with open(OUT_JSON, "w", encoding="utf-8") as fh:
            json.dump(adv, fh)

    print("\n  === ADVISORY (never gating) ===", flush=True)
    print(f"    write throughput: {adv['write_ops_per_sec']} versions/s over {adv['load_secs']}s; "
          f"{adv['replicated_mib']} MiB replicated", flush=True)
    print(f"    replication throughput incl. drain: {adv['replication_obj_per_sec']} obj/s, "
          f"{adv['replication_mib_per_sec']} MiB/s; drain took {adv.get('drain_secs', '?')}s",
          flush=True)
    if "convergence_p50_ms" in adv:
        print(f"    convergence latency ({adv['convergence_samples']} samples): "
              f"p50 {adv['convergence_p50_ms']}ms  p90 {adv['convergence_p90_ms']}ms  "
              f"p99 {adv['convergence_p99_ms']}ms  max {adv['convergence_max_ms']}ms", flush=True)
    if "outbox_depth_max" in adv:
        print(f"    outbox depth during the constant load: min {adv['outbox_depth_min']}  "
              f"median {adv['outbox_depth_med']}  max {adv['outbox_depth_max']}; "
              f"peak lag {adv['lag_peak_secs']}s; terminal failures {adv.get('repl_failed_at_drain', 0)}",
              flush=True)

    if fails:
        print(f"\n  replication stress FAILED ({len(fails)}): " + "; ".join(fails[:5]), flush=True)
        return 1
    print(f"\n  replication stress PASSED — {plain_match} plaintext + {sse_match} SSE-encrypted "
          f"versions byte-exact on the target across a master-key boundary, outbox drained, "
          f"0 errors", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
