#!/usr/bin/env python3
"""Mixed-FEATURE soak driver for conformance/soak_features.sh (ARCH 29-30).

THE GAP THIS FILLS. Every feature has a functional harness and the stress harnesses push one path
hard, but nothing runs the FEATURES TOGETHER under a sustained, CONSTANT load for long enough to
expose a slow leak. `soak.sh` is the only long-running harness and it is two-node replication only:
plaintext, one bucket, a fixed 64 KiB body, and it samples nothing but the source's RSS. So a leak
that only shows up when SSE + versioning + composite-checksum multipart + object-lock + STS sessions
+ lifecycle churn are all live at once has had nowhere to show up.

CONSTANT LOAD IS THE POINT. This driver holds a FIXED-SIZE worker pool busy for the whole run: every
worker loops without sleeping until the deadline, so the offered concurrency is exactly the pool size
from the first second to the last and never drifts. That is what makes the launcher's steady-state
SHAPE gates legitimate here and deliberately not in `stress.sh`: `stress.sh` RAMPS concurrency, so a
climb in RSS/fds/threads/WAL there just means more load was offered, whereas under constant load a
monotonic climb IS a leak signal.

THE MIX (all running continuously and concurrently against ONE node):
  * SSE — single-part PUT/GET/DELETE alternating `AES256` and `aws:kms`, wire echo asserted, a
    sampled fraction read back byte-exact.
  * VERSIONING — repeated PUTs of a small owned key set creating versions (each PUT sealed under its
    own per-version DEK), version-scoped GETs byte-exact, plus periodic delete-markers and
    version-scoped deletes so the reclaim path runs continuously.
  * MULTIPART + COMPOSITE CHECKSUMS — small 2-part uploads created and completed continuously with a
    flexible checksum requested (`CRC32`/`COMPOSITE`), so the composite path runs; a fixed fraction is
    deliberately ABORTED to exercise the immediate staging-reclaim path, and a few sessions are
    ABANDONED at the start so the background multipart SWEEPER has real work to reclaim.
  * OBJECT LOCK — a lock-enabled bucket where versions get GOVERNANCE retention; a checker
    continuously attempts to delete an already-locked version and asserts WORM holds all run.
  * STS — session credentials minted periodically off the S3-port AWS-STS surface, with a fraction of
    the SSE traffic driven by the newest session credential, plus a fail-closed check on every mint.
  * LIFECYCLE — a bucket with an immediately-due expiration rule under one prefix and a control
    prefix the rule must never touch, so the scanner runs every second for the whole soak.

KEY PARTITIONING. Every worker owns a private key namespace (`w<N>/...`) in every bucket, so no two
workers can race on the same key. That is deliberate: a self-inflicted 404 would fire the
zero-operation-errors gate for a reason that has nothing to do with the server.

GATES (all valid regardless of offered load) vs ADVISORY: see the launcher's header. Everything this
driver gates is correctness or a count — byte-exactness, WORM holding, exact rejection of a bad
session token, "the scanner deleted something and never touched the control prefix", zero unexpected
errors, `/healthz` never STOPPING (a wedge timeout, not a latency budget). Every rate, wall time and
throughput number it reports is ADVISORY: CI drives a DEBUG artifact whose AES-GCM is unoptimized.

Usage: soak_features.py <ak> <sk> <s3-endpoint> <data-dir> <key-id> <out-json>
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

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AK, SK, EP, DATA_DIR = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
KEY_ID = sys.argv[5] if len(sys.argv) > 5 else "alias/cairn-soak"
OUT_JSON = sys.argv[6] if len(sys.argv) > 6 else ""


def env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


MIN_PART = 5 * 1024 * 1024        # S3's hard minimum for a NON-FINAL part

SOAK_SECS = env_int("SOAK_SECS", 180)
SSE_WORKERS = env_int("SOAK_SSE_WORKERS", 4)
VER_WORKERS = env_int("SOAK_VER_WORKERS", 4)
MP_WORKERS = env_int("SOAK_MP_WORKERS", 2)
OBJ_SIZE = env_int("SOAK_OBJ_SIZE", 64 * 1024)
VER_KEYS = env_int("SOAK_VER_KEYS", 4)        # keys per worker that accumulate versions
VER_KEEP = env_int("SOAK_VER_KEEP", 6)        # versions tracked per key before the oldest is deleted
MP_PART_SIZE = env_int("SOAK_MP_PART_SIZE", MIN_PART)
MP_TAIL_SIZE = env_int("SOAK_MP_TAIL_SIZE", 32 * 1024)
MP_ABORT_EVERY = env_int("SOAK_MP_ABORT_EVERY", 4)   # 1-in-N multipart sessions is aborted, not completed
MP_ABANDON = env_int("SOAK_MP_ABANDON", 3)    # sessions left for the background sweeper
MP_LIFETIME = env_int("SOAK_MP_LIFETIME", 90)          # mirrors CAIRN_MULTIPART_UPLOAD_LIFETIME_SECS
MP_SWEEP = env_int("SOAK_MP_SWEEP", 5)                 # mirrors CAIRN_MULTIPART_SWEEP_INTERVAL_SECS
VERIFY_EVERY = env_int("SOAK_VERIFY_EVERY", 4)  # 1-in-N ops is read back and compared byte-exact
STS_EVERY_SECS = env_int("SOAK_STS_EVERY_SECS", 20)
STS_DURATION = env_int("SOAK_STS_DURATION", 900)  # 900 s is the server's floor (ARCH 14)
STS_PCT = env_int("SOAK_STS_PCT", 25)         # % of SSE traffic driven by session credentials
LOCK_CHECK_EVERY = env_int("SOAK_LOCK_CHECK_EVERY", 1)  # WORM probes per lock-worker iteration
HEALTHZ_TIMEOUT = env_int("SOAK_HEALTHZ_TIMEOUT", 60)   # a WEDGE detector, NOT a latency budget

# Guards against tuning the harness into vacuous truth. Every one of these would let an assertion
# loop iterate zero times and "pass" — a harness that can be silently neutered by an env var is worse
# than no harness (the lesson stress_multipart.py encodes).
if SOAK_SECS < 60:
    sys.exit(f"SOAK_SECS must be >= 60 for the sampler's third-vs-third windows to mean anything "
             f"(got {SOAK_SECS})")
if SSE_WORKERS < 1 or VER_WORKERS < 1 or MP_WORKERS < 1:
    sys.exit("SOAK_SSE_WORKERS / SOAK_VER_WORKERS / SOAK_MP_WORKERS must each be >= 1 so every "
             f"feature is actually exercised (got {SSE_WORKERS}/{VER_WORKERS}/{MP_WORKERS})")
if VER_KEYS < 2 or VER_KEEP < 2:
    sys.exit(f"SOAK_VER_KEYS and SOAK_VER_KEEP must be >= 2 for versions to accumulate "
             f"(got {VER_KEYS}/{VER_KEEP})")
if MP_ABORT_EVERY < 2:
    sys.exit(f"SOAK_MP_ABORT_EVERY must be >= 2 so BOTH complete and abort run (got {MP_ABORT_EVERY})")
if VERIFY_EVERY < 1:
    sys.exit(f"SOAK_VERIFY_EVERY must be >= 1 (got {VERIFY_EVERY})")
if MP_ABANDON < 1:
    sys.exit("SOAK_MP_ABANDON must be >= 1 or the sweeper gate asserts nothing at all "
             f"(got {MP_ABANDON})")
if MP_LIFETIME < 60:
    sys.exit("SOAK_MP_LIFETIME must be >= 60 s: a live multipart session on a contended debug runner "
             f"must never be mistaken for an abandoned one (got {MP_LIFETIME})")

POOL = SSE_WORKERS + VER_WORKERS + MP_WORKERS + 8
# `total_max_attempts` (NOT `max_attempts`) is what disables retries. botocore reads `max_attempts`
# as the number of RETRIES and derives total = max_attempts + 1, so `retries={"max_attempts": 1}`
# still allows ONE retry — which would silently rewrite a deliberate rejection (an expired session
# token, a WORM-blocked delete) into whatever the second attempt returned. One attempt, no retries.
CFG = Config(s3={"addressing_style": "path"},
             retries={"total_max_attempts": 1, "mode": "standard"},
             connect_timeout=20, read_timeout=300, max_pool_connections=POOL)


def s3_client(akid, secret, token=None):
    return boto3.client("s3", endpoint_url=EP, aws_access_key_id=akid,
                        aws_secret_access_key=secret, aws_session_token=token,
                        region_name="us-east-1", config=CFG)


s3 = s3_client(AK, SK)
sts = boto3.client("sts", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK,
                   region_name="us-east-1",
                   config=Config(retries={"total_max_attempts": 1, "mode": "standard"}))

B_SSE, B_VER, B_MP, B_LOCK, B_LC = "soak-sse", "soak-ver", "soak-mp", "soak-lock", "soak-lc"
LC_EXPIRE_PREFIX = "exp/"       # the lifecycle rule's filter — these objects are meant to vanish
LC_KEEP_PREFIX = "keep/"        # the control prefix the rule must NEVER touch

STATE = {"stop": False}
LOCK = threading.Lock()
COUNTS = collections.Counter()
ERRORS = []          # capped sample of the operation errors; COUNTS["errors"] is the true total
MISMATCH = []        # byte-exactness violations — the harness's most valuable gate
WORM_BREACH = []     # a locked version that DELETED — the single worst outcome here
LC_LOST_KEEP = []    # a control-prefix object the lifecycle scanner destroyed
COMPLETE_WALL = []   # advisory: multipart Complete wall times
SESSION = {"client": None, "creds": None, "first": None}


def bump(name, n=1):
    with LOCK:
        COUNTS[name] += n


def oops(where, exc):
    with LOCK:
        COUNTS["errors"] += 1
        if len(ERRORS) < 40:
            ERRORS.append(f"{where}: {type(exc).__name__} {err_of(exc)}")


def err_of(exc):
    """(http status, S3 error code) of a ClientError; (None, type name) for anything else."""
    if isinstance(exc, ClientError):
        return (exc.response["ResponseMetadata"]["HTTPStatusCode"],
                exc.response["Error"].get("Code"))
    return (None, type(exc).__name__)


def is_404(exc):
    return (isinstance(exc, ClientError)
            and exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404)


# --- bodies ---------------------------------------------------------------------------------------
# Generated ONCE up front, not per operation: the workers must spend their time in the server, not in
# `os.urandom`, or the offered load would be bounded by the driver's own CPU and would drift as the
# box warms up. Marker-rich but high-entropy, the encryption.py shape, so a body is still a hostile
# input for the block compressor.
MARKER = b"PLAINTEXT-MARKER-DO-NOT-FIND-ON-DISK-"


def body_of(size, salt):
    out = bytearray()
    while len(out) < size:
        out += MARKER + salt + os.urandom(48)
    return bytes(out[:size])


BODIES = [body_of(OBJ_SIZE, f"-{i:02d}-".encode()) for i in range(8)]
DIGESTS = [hashlib.sha256(b).hexdigest() for b in BODIES]
# The object-lock and lifecycle arms write objects that are never deleted by the driver (a locked
# version CANNOT be deleted, by definition), so they use a deliberately small body: at soak length
# their footprint is rate x duration, and a 64 KiB body would put gigabytes of undeletable data in
# the temp dir on a fast box without making the invariant one bit stronger.
SMALL_SIZE = env_int("SOAK_SMALL_SIZE", 4096)
SMALL = [body_of(SMALL_SIZE, f"-s{i:02d}-".encode()) for i in range(8)]
SMALL_DIGESTS = [hashlib.sha256(b).hexdigest() for b in SMALL]
MP_PART = body_of(MP_PART_SIZE, b"-mp-")
MP_TAIL = body_of(MP_TAIL_SIZE, b"-tail-")
MP_SHA = hashlib.sha256(MP_PART + MP_TAIL).hexdigest()
MP_LEN = len(MP_PART) + len(MP_TAIL)


def staging_dir(upload_id):
    return os.path.join(DATA_DIR, ".staging", "multipart", upload_id)


def sha_of_get(client, bucket, key, **kw):
    """(sha256 hex, length) of a GET body, streamed so a multipart object is not buffered whole."""
    h = hashlib.sha256()
    n = 0
    body = client.get_object(Bucket=bucket, Key=key, **kw)["Body"]
    while True:
        chunk = body.read(1 << 20)
        if not chunk:
            break
        h.update(chunk)
        n += len(chunk)
    return h.hexdigest(), n


def running(deadline):
    return not STATE["stop"] and time.monotonic() < deadline


# --- workers ---------------------------------------------------------------------------------------
def sse_worker(idx, deadline):
    """SSE-S3 and SSE-KMS single-part churn, a slice of it driven by STS session credentials."""
    n = 0
    while running(deadline):
        n += 1
        key = f"w{idx}/sse-{n % 16:02d}"
        bi = n % len(BODIES)
        kms = (n % 2 == 0)
        extra = ({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": KEY_ID} if kms
                 else {"ServerSideEncryption": "AES256"})
        # A deterministic slice of the traffic rides the newest session credential, so the
        # session-credential auth path is under continuous load rather than probed once.
        client = s3
        if STS_PCT and (n % 100) < STS_PCT and SESSION["client"] is not None:
            client = SESSION["client"]
            bump("sts_driven_ops")
        try:
            r = client.put_object(Bucket=B_SSE, Key=key, Body=BODIES[bi], **extra)
            bump("sse_puts")
            want = "aws:kms" if kms else "AES256"
            if r.get("ServerSideEncryption") != want:
                with LOCK:
                    MISMATCH.append(f"{key}: SSE echo {r.get('ServerSideEncryption')!r} != {want!r}")
        except Exception as exc:  # noqa: BLE001 — any failure is a harness failure
            oops(f"sse put {key}", exc)
            continue
        if n % VERIFY_EVERY == 0:
            try:
                digest, size = sha_of_get(client, B_SSE, key)
                bump("sse_verified")
                if digest != DIGESTS[bi] or size != OBJ_SIZE:
                    with LOCK:
                        MISMATCH.append(f"{key}: sse GET not byte-exact ({size}B)")
            except Exception as exc:  # noqa: BLE001
                oops(f"sse get {key}", exc)
        if n % 3 == 0:
            try:
                client.delete_object(Bucket=B_SSE, Key=key)
                bump("sse_deletes")
            except Exception as exc:  # noqa: BLE001
                oops(f"sse delete {key}", exc)


def ver_worker(idx, deadline):
    """Version churn on owned keys: new versions, version-scoped GETs, delete markers, version
    deletes. Each PUT is SSE-S3 so every version carries its OWN DEK and every version delete runs
    the per-version reclaim path."""
    tracked = {k: collections.deque() for k in range(VER_KEYS)}
    markers = {k: None for k in range(VER_KEYS)}   # last delete-marker version id per slot
    n = 0
    while running(deadline):
        n += 1
        slot = n % VER_KEYS
        key = f"w{idx}/ver-{slot}"
        bi = n % len(BODIES)
        try:
            r = s3.put_object(Bucket=B_VER, Key=key, Body=BODIES[bi],
                              ServerSideEncryption="AES256")
            bump("ver_puts")
            vid = r.get("VersionId")
            if not vid:
                with LOCK:
                    MISMATCH.append(f"{key}: versioned PUT returned no VersionId")
                continue
            tracked[slot].append((vid, bi))
        except Exception as exc:  # noqa: BLE001
            oops(f"ver put {key}", exc)
            continue

        # Version-scoped GET of an older tracked version: proves each version still opens under its
        # own DEK while newer versions and delete markers pile up on the same key.
        if n % VERIFY_EVERY == 0 and len(tracked[slot]) >= 2:
            vid, want = tracked[slot][0]
            try:
                digest, size = sha_of_get(s3, B_VER, key, VersionId=vid)
                bump("ver_verified")
                if digest != DIGESTS[want] or size != OBJ_SIZE:
                    with LOCK:
                        MISMATCH.append(f"{key}@{vid}: version GET not byte-exact ({size}B)")
            except Exception as exc:  # noqa: BLE001
                oops(f"ver get {key}@{vid}", exc)

        # A delete marker hides the key; the next PUT un-hides it. Tracked versions stay readable.
        if n % 5 == 0:
            try:
                d = s3.delete_object(Bucket=B_VER, Key=key)
                bump("ver_delete_markers")
                if d.get("DeleteMarker") is not True:
                    with LOCK:
                        MISMATCH.append(f"{key}: delete on a versioned bucket made no delete marker")
                # Reap the PREVIOUS marker for this slot. Delete markers are versions too: leaving
                # them is monotonic accumulation for the whole run, i.e. workload growth the leak
                # gates would eventually read as a server leak. Deleting the marker version also
                # exercises the un-hide path.
                mv = d.get("VersionId")
                old = markers[slot]
                markers[slot] = mv
                if old:
                    try:
                        s3.delete_object(Bucket=B_VER, Key=key, VersionId=old)
                        bump("ver_marker_reaped")
                    except Exception as exc:  # noqa: BLE001
                        oops(f"ver marker reap {key}@{old}", exc)
            except Exception as exc:  # noqa: BLE001
                oops(f"ver delete-marker {key}", exc)

        # Version-scoped delete of the OLDEST tracked version: the blob-reclaim path, continuously.
        while len(tracked[slot]) > VER_KEEP:
            vid, _ = tracked[slot].popleft()
            try:
                s3.delete_object(Bucket=B_VER, Key=key, VersionId=vid)
                bump("ver_version_deletes")
            except Exception as exc:  # noqa: BLE001
                oops(f"ver version-delete {key}@{vid}", exc)


def mp_worker(idx, deadline):
    """Composite-checksum multipart churn: 2-part uploads with a flexible checksum requested, a fixed
    1-in-N fraction ABORTED so the immediate staging-reclaim path runs continuously."""
    n = 0
    while running(deadline):
        n += 1
        key = f"w{idx}/mp-{n:05d}"
        abort = (n % MP_ABORT_EVERY == 0)
        uid = None
        try:
            c = s3.create_multipart_upload(Bucket=B_MP, Key=key,
                                           ChecksumAlgorithm="CRC32", ChecksumType="COMPOSITE")
            uid = c["UploadId"]
            p1 = s3.upload_part(Bucket=B_MP, Key=key, UploadId=uid, PartNumber=1,
                                Body=MP_PART, ChecksumAlgorithm="CRC32")
            p2 = s3.upload_part(Bucket=B_MP, Key=key, UploadId=uid, PartNumber=2,
                                Body=MP_TAIL, ChecksumAlgorithm="CRC32")
        except Exception as exc:  # noqa: BLE001
            oops(f"mp stage {key}", exc)
            continue

        if abort:
            try:
                s3.abort_multipart_upload(Bucket=B_MP, Key=key, UploadId=uid)
                bump("mp_aborts")
            except Exception as exc:  # noqa: BLE001
                oops(f"mp abort {key}", exc)
                continue
            # The staging directory for an aborted upload must be gone immediately — this is the
            # reclaim path that, if it regressed, would show up as the staging-bytes leak gate.
            if os.path.isdir(staging_dir(uid)):
                with LOCK:
                    MISMATCH.append(f"{key}: staging dir survived abort ({uid})")
            continue

        parts = [{"PartNumber": 1, "ETag": p1["ETag"], "ChecksumCRC32": p1["ChecksumCRC32"]},
                 {"PartNumber": 2, "ETag": p2["ETag"], "ChecksumCRC32": p2["ChecksumCRC32"]}]
        t0 = time.monotonic()
        try:
            done = s3.complete_multipart_upload(Bucket=B_MP, Key=key, UploadId=uid,
                                                MultipartUpload={"Parts": parts},
                                                ChecksumType="COMPOSITE")
            bump("mp_completes")
        except Exception as exc:  # noqa: BLE001
            oops(f"mp complete {key}", exc)
            continue
        with LOCK:
            COMPLETE_WALL.append(time.monotonic() - t0)
        # The COMPOSITE path is what makes this multipart churn interesting rather than a big PUT:
        # a composite CRC32 is the checksum-of-checksums, rendered with the `-<nparts>` suffix.
        if not done.get("ChecksumCRC32", "").endswith("-2") or done.get("ChecksumType") != "COMPOSITE":
            with LOCK:
                MISMATCH.append(f"{key}: composite checksum {done.get('ChecksumCRC32')!r} "
                                f"type {done.get('ChecksumType')!r}")
        else:
            bump("mp_composite_ok")
        if os.path.isdir(staging_dir(uid)):
            with LOCK:
                MISMATCH.append(f"{key}: staging dir survived complete ({uid})")
        # Verify on a DIFFERENT phase from the abort selector. With the defaults both are 1-in-4, so
        # `n % VERIFY_EVERY == 0` would only ever land on iterations that had already aborted and
        # `continue`d — the byte-exactness loop would iterate zero times and the gate would be
        # vacuous. `1 % VERIFY_EVERY` keeps this correct even when VERIFY_EVERY is 1.
        if n % VERIFY_EVERY == 1 % VERIFY_EVERY:
            try:
                digest, size = sha_of_get(s3, B_MP, key)
                bump("mp_verified")
                if digest != MP_SHA or size != MP_LEN:
                    with LOCK:
                        MISMATCH.append(f"{key}: assembled object not byte-exact ({size}B)")
            except Exception as exc:  # noqa: BLE001
                oops(f"mp get {key}", exc)
        try:
            s3.delete_object(Bucket=B_MP, Key=key)   # keep the bucket bounded over a long run
        except Exception as exc:  # noqa: BLE001
            oops(f"mp delete {key}", exc)


def lock_worker(deadline):
    """WORM under sustained load: keep locking new versions, and keep trying to delete already-locked
    ones. Not one probe at the start — the invariant has to hold for the WHOLE soak."""
    far = "2099-01-01T00:00:00Z"
    locked = collections.deque(maxlen=64)
    n = 0
    while running(deadline):
        n += 1
        key = f"lock-{n % 32:02d}"
        # A locked version can never be deleted, so newly locked versions are pure accumulation:
        # lock a new one only every 4th iteration and spend the rest of the loop probing WORM, which
        # is the invariant under test. Keeps the run's disk footprint bounded without weakening it.
        if n % 4 == 1:
            try:
                r = s3.put_object(Bucket=B_LOCK, Key=key, Body=SMALL[n % len(SMALL)])
                vid = r["VersionId"]
                s3.put_object_retention(Bucket=B_LOCK, Key=key, VersionId=vid,
                                        Retention={"Mode": "GOVERNANCE", "RetainUntilDate": far})
                # `locked` is a bounded deque: capture whatever it is about to EVICT and reap that
                # version with an explicit governance bypass. Without this, locked versions are pure
                # monotonic accumulation for the whole run — workload growth that the RSS/staging
                # leak gates would eventually read as a server leak on a deep run. Reaping also
                # gates the bypass path itself, which nothing else covered.
                evicted = (locked[0] if len(locked) == locked.maxlen else None)
                locked.append((key, vid))
                bump("lock_versions_locked")
                if evicted is not None:
                    ek, ev = evicted
                    try:
                        s3.delete_object(Bucket=B_LOCK, Key=ek, VersionId=ev,
                                         BypassGovernanceRetention=True)
                        bump("worm_bypass_deletes")
                    except Exception as exc:  # noqa: BLE001
                        with LOCK:
                            MISMATCH.append(
                                f"governance BYPASS delete of {ek}@{ev} failed: {exc}")
            except Exception as exc:  # noqa: BLE001
                oops(f"lock put/retain {key}", exc)
                continue
        for _ in range(LOCK_CHECK_EVERY):
            if not locked:
                break
            lk, lv = locked[n % len(locked)]
            bump("worm_delete_attempts")
            try:
                # No BypassGovernanceRetention header: this MUST be refused, for the entire run.
                s3.delete_object(Bucket=B_LOCK, Key=lk, VersionId=lv)
                with LOCK:
                    WORM_BREACH.append(f"{lk}@{lv} DELETED while under GOVERNANCE retention")
            except ClientError as exc:
                status = exc.response["ResponseMetadata"]["HTTPStatusCode"]
                code = exc.response.get("Error", {}).get("Code", "")
                # Require the EXACT refusal. Accepting "any 4xx" would keep this gate green through a
                # regression that made locked versions merely unaddressable (404 NoSuchVersion) or
                # denied for an unrelated authz reason — neither of which proves WORM held.
                if status == 403 and code == "AccessDenied":
                    bump("worm_delete_refused")
                else:
                    with LOCK:
                        MISMATCH.append(
                            f"WORM delete of {lk}@{lv} refused with {status} {code}, "
                            f"expected 403 AccessDenied")
            except Exception as exc:  # noqa: BLE001
                oops(f"worm delete {lk}", exc)


def lifecycle_worker(deadline):
    """Keep feeding the scanner: objects under the expiring prefix (which it must remove) and under a
    control prefix (which it must never touch). The bucket is UNVERSIONED, so an expiration really
    destroys data and 'it vanished' is unambiguous."""
    n = 0
    pending = collections.deque(maxlen=64)     # exp/ keys awaiting the scanner
    keep = collections.deque(maxlen=32)        # keep/ keys that must survive the whole run
    while running(deadline):
        n += 1
        ek = f"{LC_EXPIRE_PREFIX}o-{n:06d}"
        # 32 control keys and 8 bodies: 32 % 8 == 0, so a given control key is ALWAYS rewritten with
        # the same body. That is what makes the tracked (key, body-index) pair stay truthful even
        # though the key is overwritten many times during the run.
        kk = f"{LC_KEEP_PREFIX}o-{n % 32:03d}"
        bi = n % len(SMALL)
        try:
            s3.put_object(Bucket=B_LC, Key=ek, Body=SMALL[bi])
            bump("lc_expiring_puts")
            pending.append(ek)
        except Exception as exc:  # noqa: BLE001
            oops(f"lc put {ek}", exc)
        try:
            s3.put_object(Bucket=B_LC, Key=kk, Body=SMALL[bi])
            bump("lc_keep_puts")
            keep.append((kk, bi))
        except Exception as exc:  # noqa: BLE001
            oops(f"lc put {kk}", exc)

        # Did the scanner take one? (HEAD 404 on an unversioned bucket == really deleted.)
        if pending:
            probe = pending[0]
            try:
                s3.head_object(Bucket=B_LC, Key=probe)
            except ClientError as exc:
                if is_404(exc):
                    pending.popleft()
                    bump("lc_expirations_observed")
                else:
                    oops(f"lc head {probe}", exc)
            except Exception as exc:  # noqa: BLE001
                oops(f"lc head {probe}", exc)

        # The control prefix must be untouched — an over-broad scanner is a data-loss bug, and it is
        # exactly the kind that only shows up once the scanner has run hundreds of times.
        if keep and n % VERIFY_EVERY == 0:
            kk2, bi2 = keep[n % len(keep)]
            try:
                digest, size = sha_of_get(s3, B_LC, kk2)
                bump("lc_keep_verified")
                if digest != SMALL_DIGESTS[bi2] or size != SMALL_SIZE:
                    with LOCK:
                        MISMATCH.append(f"{kk2}: control-prefix object not byte-exact ({size}B)")
            except ClientError as exc:
                if is_404(exc):
                    with LOCK:
                        LC_LOST_KEEP.append(kk2)
                else:
                    oops(f"lc keep get {kk2}", exc)
            except Exception as exc:  # noqa: BLE001
                oops(f"lc keep get {kk2}", exc)


def sts_minter(deadline):
    """Mint session credentials on a fixed cadence for the whole run, publish the newest one for the
    SSE workers to drive traffic with, and fail-closed-check every mint."""
    while running(deadline):
        try:
            creds = sts.get_session_token(DurationSeconds=STS_DURATION)["Credentials"]
            bump("sts_mints")
            if not creds["AccessKeyId"].startswith("CAIRNTMP"):
                with LOCK:
                    MISMATCH.append(f"STS minted a non-session key {creds['AccessKeyId']!r}")
            client = s3_client(creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"])
            client.head_bucket(Bucket=B_SSE)          # the session must actually work
            bump("sts_sessions_usable")
            # Fail-closed: the same key + secret with a TAMPERED token must be refused. Checked on
            # every mint, so a regression that starts ignoring the token cannot hide in one probe.
            bad = s3_client(creds["AccessKeyId"], creds["SecretAccessKey"], "not-the-real-token")
            try:
                bad.head_bucket(Bucket=B_SSE)
                with LOCK:
                    MISMATCH.append("a TAMPERED session token was accepted")
            except ClientError as exc:
                st = exc.response["ResponseMetadata"]["HTTPStatusCode"]
                # Only an AUTH refusal proves the token was rejected; a 404 NoSuchBucket would
                # otherwise count as "refused" and hide a broken session-token check.
                if st in (401, 403):
                    bump("sts_tampered_refused")
                else:
                    with LOCK:
                        MISMATCH.append(f"tampered session token refused with {st}, expected 401/403")
            finally:
                # Close both the probe client and the session client we are replacing: a leaked
                # botocore client per mint is a DRIVER-caused monotonic fd climb that the fd leak
                # gate would misread as a server leak on a deep run.
                try:
                    bad.close()
                except Exception:  # noqa: BLE001
                    pass
            with LOCK:
                prev = SESSION.get("client")
                SESSION["client"] = client
                SESSION["creds"] = creds
                if SESSION["first"] is None:
                    SESSION["first"] = (creds, time.monotonic())
            if prev is not None and prev is not client:
                try:
                    prev.close()
                except Exception:  # noqa: BLE001
                    pass
        except Exception as exc:  # noqa: BLE001
            oops("sts mint", exc)
        # The one deliberate sleep in the harness: minting is a cadence, not a load generator, and a
        # tight loop here would add thousands of session rows and change what the row-count sample
        # means. Broken into short naps so the deadline is honoured promptly.
        for _ in range(STS_EVERY_SECS * 4):
            if not running(deadline):
                return
            time.sleep(0.25)


def healthz_prober(deadline, host, port):
    """WEDGE detector. The gate is that /healthz never STOPS answering, with a deliberately generous
    per-probe timeout: probe latency under a saturating mixed workload on a debug build is offered
    load, not a signal, and gating on it would be gating on the runner's contention."""
    worst = 0.0
    while running(deadline):
        t0 = time.monotonic()
        try:
            conn = http.client.HTTPConnection(host, port, timeout=HEALTHZ_TIMEOUT)
            conn.request("GET", "/healthz")
            resp = conn.getresponse()
            resp.read()
            conn.close()
            bump("healthz_probes")
            if resp.status != 200:
                bump("healthz_failures")
                with LOCK:
                    MISMATCH.append(f"/healthz answered {resp.status}")
        except Exception as exc:  # noqa: BLE001
            bump("healthz_failures")
            oops("healthz", exc)
        worst = max(worst, time.monotonic() - t0)
        with LOCK:
            COUNTS["healthz_worst_ms"] = max(COUNTS["healthz_worst_ms"], int(worst * 1000))
        time.sleep(1.0)


# --- setup / teardown checks -----------------------------------------------------------------------
def setup():
    s3.create_bucket(Bucket=B_SSE)
    s3.create_bucket(Bucket=B_VER)
    s3.put_bucket_versioning(Bucket=B_VER, VersioningConfiguration={"Status": "Enabled"})
    s3.create_bucket(Bucket=B_MP)
    s3.create_bucket(Bucket=B_LOCK, ObjectLockEnabledForBucket=True)
    s3.create_bucket(Bucket=B_LC)
    # An expiration `Date` in the past is due immediately and is evaluated independently of object
    # age (`matching_expiration_rule`, cairn-lifecycle/scanner.rs) — the only way to get a sub-day
    # rule, and the same trick lifecycle.py uses. Midnight-aligned, as real S3 requires.
    s3.put_bucket_lifecycle_configuration(
        Bucket=B_LC,
        LifecycleConfiguration={"Rules": [{
            "ID": "soak-expire-now",
            "Status": "Enabled",
            "Filter": {"Prefix": LC_EXPIRE_PREFIX},
            "Expiration": {"Date": "2020-01-01T00:00:00Z"},
        }]})


def abandon_sessions():
    """Leave MP_ABANDON multipart sessions staged and untouched so the background SWEEPER has real
    work: it aborts sessions idle beyond CAIRN_MULTIPART_UPLOAD_LIFETIME_SECS and reclaims their
    staged parts. Returns [(key, upload_id)]."""
    out = []
    for i in range(MP_ABANDON):
        key = f"abandoned/{i:02d}"
        c = s3.create_multipart_upload(Bucket=B_MP, Key=key)
        uid = c["UploadId"]
        s3.upload_part(Bucket=B_MP, Key=key, UploadId=uid, PartNumber=1, Body=MP_TAIL)
        out.append((key, uid))
    return out


def check_sweeper(abandoned, elapsed, adv):
    """The sweeper gate, but ONLY when the run was long enough for it to be a real assertion —
    otherwise it is reported as explicitly SKIPPED rather than silently passing."""
    need = MP_LIFETIME + 3 * MP_SWEEP + 10
    if elapsed < need:
        adv["sweeper"] = (f"SKIPPED: run was {elapsed:.0f}s, needs >{need}s "
                          f"(lifetime {MP_LIFETIME}s + sweeps)")
        return None
    left = []
    if not abandoned:
        # Nothing was abandoned => there is nothing for the sweeper to reclaim, so a green here
        # would assert precisely nothing. Fail loudly instead of passing vacuously.
        left.append("no sessions were abandoned — the sweeper gate would assert nothing")
    for key, uid in abandoned:
        try:
            s3.list_parts(Bucket=B_MP, Key=key, UploadId=uid)
            left.append(f"{key}: session still live")
        except ClientError as exc:
            if not is_404(exc):
                left.append(f"{key}: {err_of(exc)}")
        if os.path.isdir(staging_dir(uid)):
            left.append(f"{key}: staging dir survived the sweeper")
    adv["sweeper"] = "ok" if not left else f"FAILED: {left[:3]}"
    return left


def check_sts_expiry(elapsed, adv):
    """A genuinely EXPIRED session credential must be refused. The server's floor for a session
    lifetime is 900 s (ARCH 14, MIN_DURATION_SECS), so this can only be asserted on a run longer than
    that — a CI-length soak reports it as explicitly SKIPPED rather than pretending to cover it."""
    first = SESSION["first"]
    if first is None:
        adv["sts_expiry"] = "SKIPPED: no session credential was ever minted"
        return None
    creds, minted_at = first
    age = time.monotonic() - minted_at
    if age < STS_DURATION + 5:
        adv["sts_expiry"] = (f"SKIPPED: oldest session is {age:.0f}s old, expires at "
                             f"{STS_DURATION}s (raise SOAK_SECS past {STS_DURATION + 30} to gate it)")
        return None
    expired = s3_client(creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"])
    try:
        expired.head_bucket(Bucket=B_SSE)
        adv["sts_expiry"] = "FAILED: an EXPIRED session credential was accepted"
        return ["expired session credential accepted"]
    except ClientError as exc:
        status = exc.response["ResponseMetadata"]["HTTPStatusCode"]
        adv["sts_expiry"] = f"ok (refused {status})"
        return [] if status < 500 else ["expired session credential answered 5xx"]


def main():
    host_port = EP.split("//", 1)[1]
    host, port = host_port.split(":")[0], int(host_port.split(":")[1])

    print(f"  soak: {SOAK_SECS}s constant load — "
          f"{SSE_WORKERS} SSE + {VER_WORKERS} versioning + {MP_WORKERS} multipart workers, "
          f"plus object-lock / lifecycle / STS drivers", flush=True)
    setup()
    abandoned = abandon_sessions()

    t0 = time.monotonic()
    deadline = t0 + SOAK_SECS
    jobs = ([("sse", i) for i in range(SSE_WORKERS)]
            + [("ver", i) for i in range(VER_WORKERS)]
            + [("mp", i) for i in range(MP_WORKERS)]
            + [("lock", 0), ("lc", 0), ("sts", 0), ("healthz", 0)])

    def run(job):
        kind, i = job
        try:
            if kind == "sse":
                sse_worker(i, deadline)
            elif kind == "ver":
                ver_worker(i, deadline)
            elif kind == "mp":
                mp_worker(i, deadline)
            elif kind == "lock":
                lock_worker(deadline)
            elif kind == "lc":
                lifecycle_worker(deadline)
            elif kind == "sts":
                sts_minter(deadline)
            elif kind == "healthz":
                healthz_prober(deadline, host, port)
        except Exception as exc:  # noqa: BLE001 — a worker must never take the run down silently
            oops(f"{kind}-{i} worker", exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        list(pool.map(run, jobs))
    elapsed = time.monotonic() - t0

    adv = {}
    sweeper_fail = check_sweeper(abandoned, elapsed, adv)
    sts_fail = check_sts_expiry(elapsed, adv)

    # --- verdict ---------------------------------------------------------------------------------
    fails = []
    c = COUNTS

    def gate(label, cond):
        print(("    ok: " if cond else "    FAIL: ") + label, flush=True)
        if not cond:
            fails.append(label)

    print("\n  === correctness gates (load-independent) ===", flush=True)
    gate(f"zero operation errors ({c['errors']}; sample {ERRORS[:3] or 'none'})", c["errors"] == 0)
    gate(f"every sampled read was BYTE-EXACT ({len(MISMATCH)} violations; {MISMATCH[:3] or 'none'})",
         not MISMATCH)
    # Non-vacuity: each of these loops must have actually run, or "no mismatches" means nothing.
    gate(f"SSE round-trips verified ({c['sse_verified']}) and both SSE modes ran ({c['sse_puts']} PUTs)",
         c["sse_verified"] > 0 and c["sse_puts"] > 1)
    gate(f"version-scoped reads verified ({c['ver_verified']}) with delete markers "
         f"({c['ver_delete_markers']}) and version deletes ({c['ver_version_deletes']})",
         c["ver_verified"] > 0 and c["ver_delete_markers"] > 0 and c["ver_version_deletes"] > 0)
    gate(f"multipart churn completed ({c['mp_completes']}) AND aborted ({c['mp_aborts']}) sessions, "
         f"all composite-checksummed ({c['mp_composite_ok']}), assemblies verified ({c['mp_verified']})",
         c["mp_completes"] > 0 and c["mp_aborts"] > 0
         and c["mp_composite_ok"] == c["mp_completes"] and c["mp_verified"] > 0)
    gate(f"WORM held for the whole soak: {c['worm_delete_attempts']} delete attempts on locked "
         f"versions, {c['worm_delete_refused']} refused, {len(WORM_BREACH)} breached "
         f"({WORM_BREACH[:2] or 'none'})",
         c["worm_delete_attempts"] > 0 and not WORM_BREACH
         and c["worm_delete_refused"] == c["worm_delete_attempts"])
    gate(f"lifecycle scanner ran ({c['lc_expirations_observed']} expirations observed) and never "
         f"touched the control prefix ({len(LC_LOST_KEEP)} lost; {LC_LOST_KEEP[:2] or 'none'})",
         c["lc_expirations_observed"] > 0 and not LC_LOST_KEEP and c["lc_keep_verified"] > 0)
    gate(f"STS sessions minted ({c['sts_mints']}), usable ({c['sts_sessions_usable']}), drove traffic "
         f"({c['sts_driven_ops']} ops), tampered tokens refused ({c['sts_tampered_refused']})",
         c["sts_mints"] > 0 and c["sts_sessions_usable"] == c["sts_mints"]
         and c["sts_tampered_refused"] == c["sts_mints"] and c["sts_driven_ops"] > 0)
    gate(f"/healthz never stopped answering ({c['healthz_probes']} probes, "
         f"{c['healthz_failures']} failures, worst {c['healthz_worst_ms']} ms, "
         f"wedge timeout {HEALTHZ_TIMEOUT}s)",
         c["healthz_probes"] > 0 and c["healthz_failures"] == 0)
    if sweeper_fail is not None:
        gate(f"multipart sweeper reclaimed the {MP_ABANDON} abandoned sessions "
             f"({sweeper_fail[:2] or 'none left'})", not sweeper_fail)
    else:
        print(f"    skipped: sweeper — {adv['sweeper']}", flush=True)
    if sts_fail is not None:
        gate(f"an EXPIRED session credential is refused ({adv['sts_expiry']})", not sts_fail)
    else:
        print(f"    skipped: STS expiry — {adv['sts_expiry']}", flush=True)

    adv.update({k: v for k, v in c.items()})
    adv["elapsed_secs"] = round(elapsed, 1)
    adv["total_ops"] = sum(v for k, v in c.items()
                           if k.endswith(("_puts", "_deletes", "_completes", "_aborts",
                                          "_verified", "_delete_markers", "_version_deletes")))
    adv["ops_per_sec"] = round(adv["total_ops"] / elapsed, 1) if elapsed > 0 else 0
    if COMPLETE_WALL:
        adv["complete_wall_min"] = round(min(COMPLETE_WALL), 3)
        adv["complete_wall_med"] = round(statistics.median(COMPLETE_WALL), 3)
        adv["complete_wall_max"] = round(max(COMPLETE_WALL), 3)
    adv["workers"] = SSE_WORKERS + VER_WORKERS + MP_WORKERS + 4
    adv["mismatches"] = MISMATCH[:10]
    adv["errors_sample"] = ERRORS[:10]
    adv["failures"] = fails
    # Nothing in this mix may legitimately answer 5xx: every deliberate rejection here (WORM,
    # tampered token, expired session) is a 4xx. The budget is therefore exactly zero, and the
    # launcher gates the server's own 5xx counter against it.
    adv["declared_5xx"] = 0
    if OUT_JSON:
        with open(OUT_JSON, "w", encoding="utf-8") as fh:
            json.dump(adv, fh)

    if fails:
        print(f"\n  mixed-feature soak FAILED ({len(fails)}): " + "; ".join(fails[:5]))
        return 1
    print(f"\n  mixed-feature soak PASSED — {adv['total_ops']} ops over {adv['elapsed_secs']}s "
          f"({adv['ops_per_sec']} ops/s advisory), 0 errors")
    return 0


if __name__ == "__main__":
    sys.exit(main())
