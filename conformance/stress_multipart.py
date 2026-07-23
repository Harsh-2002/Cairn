#!/usr/bin/env python3
"""Concurrent-multipart stress driver for conformance/stress_multipart.sh (ARCH 27 + ARCH 30).

THE GAP THIS FILLS. Every existing harness drives multipart SERIALLY: `multipart.py` walks the
session-state contract one call at a time, `encryption.py` proves a single SSE multipart is
ciphertext on disk, and the two stress harnesses (`stress.sh`, `stress_encrypted.sh`) drive warp,
which never issues a multipart Complete at all. So the CONCURRENT write path -- the one place where
`LocalBlobStore::assemble` holds ONE of the 64 blob-write permits across an entire SERIAL
`assemble_into` loop that, with part-level encryption, decrypts and re-encrypts every part -- has
never been exercised. Single-part PUTs (`stage`) draw from that SAME 64-permit pool, so N concurrent
assembles are exactly the shape that could starve ordinary writes. On top of that sit three
untested races: the `ClaimMultipart` complete/abort race, the `RecordPart` supersede race
(`INSERT OR REPLACE`), and `UploadPartCopy` under load.

Four scenarios, all against one hot node:

  1. BARRIER-FIRED CONCURRENT COMPLETE vs a steady single-part PUT stream. N SSE-KMS sessions are
     pre-staged, then every CompleteMultipartUpload is released simultaneously from a
     `threading.Barrier` while a separate thread pool keeps PUTting ordinary single-part objects.
  2. FAIL-CLOSED UNDER CONCURRENCY. One session in that same barrier has a staged part corrupted on
     disk (one ciphertext byte flipped, the `encryption.py` technique) before the barrier fires.
     SCOPE NOTE (uncovered invariant, stated so the boundary is visible rather than implied): a
     flipped ciphertext BODY byte fails GCM auth inside `assemble`, i.e. AFTER `ClaimMultipart`, so
     it deliberately bricks that session in `completing`. The complementary invariant -- that a bad
     *part DEK* is opened BEFORE the claim so the upload stays RETRYABLE (audit #14,
     service.rs open_part_dek pre-claim) -- is NOT exercised here; reaching it would require
     corrupting the sealed DEK envelope in the metadata DB underneath a running server. That
     invariant IS covered in-process by `wrong_master_key_fails_complete` in
     crates/cairn-protocol/tests/protocol_core.rs (complete under a different ring fails pre-claim,
     commits nothing, then succeeds under the correct ring). What is missing is only its behaviour
     under CONCURRENCY.
  3. SESSION-STATE RACES. Complete and Abort fired simultaneously at the same upload id; and a part
     number re-uploaded (RecordPart supersede) while another part uploads concurrently.
  4. UPLOADPARTCOPY UNDER CONCURRENCY. Sessions building parts by server-side copy from an SSE
     source, interleaved with sessions uploading part bodies, all staged concurrently.

GATES vs ADVISORY. Every gate here is valid REGARDLESS OF OFFERED LOAD, which is the lesson the
other stress harnesses encode: correctness (byte-exact), zero operation errors, exact expected error
CODES, liveness (`/healthz` never gaps), and "ordinary writes made progress at all" (a > 0 count,
never a rate). Timings -- Complete wall times, background PUT throughput with vs without the barrier
-- are ADVISORY and reported, never gating: they move with the runner's contention and with the
build profile (a debug binary's AES-GCM is unoptimized software crypto, and assembly is a
decrypt-then-re-encrypt pass).

PART-SIZE TRADEOFF. S3 requires every non-final part to be >= 5 MiB, so a genuinely multi-part
Complete cannot be cheap. This harness pays that 5 MiB exactly where a multi-part assemble is the
thing under test (scenarios 1/2/4 and the supersede session) and uses a small final tail part
everywhere else; the complete/abort race sessions are SINGLE-part (the final part has no minimum),
because what is under test there is session state, not assembly. All sizes and counts are
env-tunable so a manual run can go much bigger than the CI default.

Usage: stress_multipart.py <ak> <sk> <s3-endpoint> <data-dir> <key-id> <out-json>
Env knobs: MP_SESSIONS MP_PART_SIZE MP_TAIL_SIZE MP_BG_WORKERS MP_BG_BASE_SECS MP_STAGE_WORKERS
           MP_RACE_ROUNDS MP_COPY_SESSIONS MP_BODY_SESSIONS MP_BG_VERIFY
"""
import concurrent.futures
import hashlib
import http.client
import json
import os
import sys
import threading
import time
import urllib.parse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AK, SK, EP, DATA_DIR = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
KEY_ID = sys.argv[5] if len(sys.argv) > 5 else "alias/cairn-stress"
OUT_JSON = sys.argv[6] if len(sys.argv) > 6 else ""


def env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


MIN_PART = 5 * 1024 * 1024          # S3's hard minimum for a NON-FINAL part
SESSIONS = env_int("MP_SESSIONS", 12)         # concurrent sessions in the scenario-1/2 barrier
PART_SIZE = env_int("MP_PART_SIZE", MIN_PART)  # size of the non-final part
TAIL_SIZE = env_int("MP_TAIL_SIZE", 64 * 1024)  # final part: no minimum, so keep it cheap
BG_WORKERS = env_int("MP_BG_WORKERS", 6)      # steady single-part PUT stream
BG_BASE_SECS = env_int("MP_BG_BASE_SECS", 5)  # baseline window before the barrier fires
BG_SIZE = env_int("MP_BG_SIZE", 64 * 1024)
BG_VERIFY = env_int("MP_BG_VERIFY", 24)       # how many background objects to GET back byte-exact
STAGE_WORKERS = env_int("MP_STAGE_WORKERS", 6)
RACE_ROUNDS = env_int("MP_RACE_ROUNDS", 6)    # complete-vs-abort rounds
COPY_SESSIONS = env_int("MP_COPY_SESSIONS", 3)
BODY_SESSIONS = env_int("MP_BODY_SESSIONS", 3)

HEALTHZ_TIMEOUT = env_int("MP_HEALTHZ_TIMEOUT", 60)  # a WEDGE detector, not a latency budget

# Guard against tuning the harness into vacuous truth: with too few sessions the barrier is not a
# barrier and the byte-exactness loops would iterate zero times and "pass". A harness that can be
# silently neutered by an env var is worse than no harness.
if SESSIONS < 3:
    sys.exit(f"MP_SESSIONS must be >= 3 to make the barrier meaningful (got {SESSIONS})")
if RACE_ROUNDS < 1:
    sys.exit(f"MP_RACE_ROUNDS must be >= 1 (got {RACE_ROUNDS})")
if COPY_SESSIONS + BODY_SESSIONS < 2:
    sys.exit("MP_COPY_SESSIONS + MP_BODY_SESSIONS must be >= 2 for scenario 4 to mean anything "
             f"(got {COPY_SESSIONS} + {BODY_SESSIONS})")

POOL = max(SESSIONS + BG_WORKERS + 8, 32)
# `total_max_attempts` (NOT `max_attempts`) is what disables retries, and the difference is
# load-bearing for this harness: botocore reads `max_attempts` as the number of RETRIES and derives
# total = max_attempts + 1, so the `retries={"max_attempts": 1}` used by the other conformance
# drivers still allows ONE retry. That silently rewrote both of this harness's 5xx outcomes -- the
# deliberate fail-closed 500 of scenario 2 came back to the caller as the second attempt's 404
# NoSuchUpload (the first attempt had already moved the session to `completing`), and so did the
# complete/abort race -- so the harness would assert against a status the server never returned to
# the request it actually made, and the 5xx-budget gate could never balance. One attempt, no retries.
s3 = boto3.client(
    "s3", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"},
                  retries={"total_max_attempts": 1, "mode": "standard"},
                  connect_timeout=20, read_timeout=300, max_pool_connections=POOL),
)

# Encrypted CRNB block-container trailer: 34 bytes, magic `CRNB` then the version byte;
# VERSION_ENCRYPTED == 2 (crates/cairn-blob/src/compress.rs).
VERSION_ENCRYPTED = 2
MARKER = b"PLAINTEXT-MARKER-DO-NOT-FIND-ON-DISK-"

fails = []
ADVISORY = {}
# The 5xx BUDGET. Exactly ONE operation in this harness legitimately answers 5xx, declared here so
# the launcher can gate the server's own 5xx counter against EXACTLY this number: any 5xx from any
# other request still fails the run.
#   * scenario 2's poisoned Complete -- a tampered stored part is an integrity failure, so
#     BlobError::Corruption -> Error::Internal -> 500 is the CORRECT fail-closed answer.
# (Scenario 3a's abort-race Complete used to be a second, pinned-not-gated deviation returning 500;
# it is now FIXED to NoSuchUpload and hard-gated, so it no longer contributes to this budget.)
DECLARED_5XX = [0]


def declare_5xx(status):
    if status is not None and 500 <= status < 600:
        DECLARED_5XX[0] += 1
        return True
    return False


def check(label, cond):
    print(("    ok: " if cond else "    FAIL: ") + label, flush=True)
    if not cond:
        fails.append(label)
    return bool(cond)




def body_of(size):
    """Marker-rich but high-entropy: repeated known plaintext interleaved with random bytes, so the
    block compressor cannot make the marker vanish on its own. If the marker is absent from the
    stored bytes it is because they were ENCRYPTED -- which is what the on-disk checks prove."""
    out = bytearray()
    chunk = 64 * 1024
    while len(out) < size:
        piece = bytearray()
        while len(piece) < chunk:
            piece += MARKER + os.urandom(64)
        out += piece
    return bytes(out[:size])


def staging_dir(upload_id):
    return os.path.join(DATA_DIR, ".staging", "multipart", upload_id)


def staged_parts(upload_id):
    d = staging_dir(upload_id)
    return [os.path.join(d, f) for f in sorted(os.listdir(d))] if os.path.isdir(d) else []


def trailer_encrypted(blob):
    return len(blob) >= 34 and blob[-34:-30] == b"CRNB" and blob[-30] == VERSION_ENCRYPTED


def err_of(exc):
    """(http status, S3 error code) of a ClientError; (None, type name) for anything else."""
    if isinstance(exc, ClientError):
        return (exc.response["ResponseMetadata"]["HTTPStatusCode"],
                exc.response["Error"].get("Code"))
    return (None, type(exc).__name__)


def digest_of_get(bucket, key):
    """(sha256 hex, length, sighted-marker) of a GET body, streamed so a big object is not buffered."""
    h = hashlib.sha256()
    n = 0
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    while True:
        chunk = body.read(1 << 20)
        if not chunk:
            break
        h.update(chunk)
        n += len(chunk)
    return h.hexdigest(), n


def absent(bucket, key):
    """True when the key is genuinely not committed (404 NoSuchKey on HEAD)."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return False
    except ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# --------------------------------------------------------------------------- /healthz watchdog
class Healthz(threading.Thread):
    """Probes /healthz every 100 ms for as long as it runs, and gates on "did it EVER fail to
    answer", never on how fast it answered. The distinction is the whole gate-philosophy point: on a
    contended 2-core runner, 12 concurrent debug-build assembles (a decrypt-then-re-encrypt pass
    each) genuinely starve the box, and a probe measured at 5 s there is contention, not a wedge --
    gating on probe LATENCY would be gating on offered load. So the timeout is deliberately huge
    (MP_HEALTHZ_TIMEOUT, default 60 s): a server that cannot answer /healthz inside half a minute has
    stopped serving, which is load-independent. Worst observed latency is reported as advisory."""

    def __init__(self, endpoint):
        super().__init__(daemon=True)
        u = urllib.parse.urlparse(endpoint)
        self.host, self.port = u.hostname, u.port
        self.stop = threading.Event()
        self.probes = 0
        self.failures = 0
        self.worst_ms = 0.0

    def run(self):
        while not self.stop.is_set():
            t0 = time.time()
            try:
                c = http.client.HTTPConnection(self.host, self.port, timeout=HEALTHZ_TIMEOUT)
                c.request("GET", "/healthz")
                r = c.getresponse()
                r.read()
                c.close()
                ok = r.status == 200
            except Exception:  # noqa: BLE001 - any transport failure is a liveness gap
                ok = False
            dt = (time.time() - t0) * 1000.0
            self.probes += 1
            self.worst_ms = max(self.worst_ms, dt)
            if not ok:
                self.failures += 1
            self.stop.wait(0.1)


# --------------------------------------------------------------------------- session staging
def create_session(bucket, key, sse="aws:kms"):
    kw = {"ServerSideEncryption": sse}
    if sse == "aws:kms":
        kw["SSEKMSKeyId"] = KEY_ID
    return s3.create_multipart_upload(Bucket=bucket, Key=key, **kw)["UploadId"]


def stage_session(bucket, key, nparts=2, big=None, tail=None, sse="aws:kms"):
    """Create a session and upload `nparts` parts: non-final parts at `big` (>= 5 MiB), final at
    `tail`. Bodies are hashed and dropped as they go, so peak memory is one part, not one object."""
    big = PART_SIZE if big is None else big
    tail = TAIL_SIZE if tail is None else tail
    uid = create_session(bucket, key, sse)
    h = hashlib.sha256()
    total = 0
    parts = []
    for pn in range(1, nparts + 1):
        body = body_of(tail if pn == nparts else big)
        r = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=pn, Body=body)
        parts.append({"PartNumber": pn, "ETag": r["ETag"]})
        h.update(body)
        total += len(body)
        del body
    return {"bucket": bucket, "key": key, "upload_id": uid, "parts": parts,
            "sha": h.hexdigest(), "size": total, "nparts": nparts}


def complete(sess, parts=None):
    return s3.complete_multipart_upload(
        Bucket=sess["bucket"], Key=sess["key"], UploadId=sess["upload_id"],
        MultipartUpload={"Parts": parts if parts is not None else sess["parts"]})


def verify_exact(sess, label):
    try:
        digest, size = digest_of_get(sess["bucket"], sess["key"])
    except Exception as exc:  # noqa: BLE001
        return check(f"{label}: GET of the assembled object succeeded ({exc})", False)
    return check(f"{label}: assembled object is BYTE-EXACT ({size} B)",
                 digest == sess["sha"] and size == sess["size"])


# ============================================================ scenarios 1 + 2 (one barrier)
def scenario_1_and_2():
    print(f"\n[1+2] barrier-fired concurrent SSE Complete ({SESSIONS} sessions, "
          f"{PART_SIZE}+{TAIL_SIZE} B parts) vs a steady single-part PUT stream", flush=True)
    bucket = "mpstress"
    s3.create_bucket(Bucket=bucket)

    # --- pre-stage every session (concurrently: staging is itself a write-permit consumer) -------
    t_stage = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=STAGE_WORKERS) as pool:
        sessions = list(pool.map(
            lambda i: stage_session(bucket, f"mp-{i:03d}", nparts=2),
            range(SESSIONS)))
    ADVISORY["stage_secs"] = round(time.time() - t_stage, 2)
    ADVISORY["staged_bytes"] = sum(s["size"] for s in sessions)

    # --- scenario 2: poison ONE session's staged part before the barrier fires -------------------
    poisoned = sessions[SESSIONS // 2]
    parts = staged_parts(poisoned["upload_id"])
    if check(f"[2] located the staged part files of the poison session ({len(parts)})", len(parts) >= 1):
        # The biggest staged file is the non-final 5 MiB part; flip one byte mid-ciphertext, well
        # clear of the 34-byte CRNB trailer, so AES-GCM authentication fails at assembly time.
        path = max(parts, key=os.path.getsize)
        with open(path, "r+b") as fh:
            raw = bytearray(fh.read())
            off = max(0, (len(raw) - 34) // 2)
            raw[off] ^= 0xFF
            fh.seek(0)
            fh.write(raw)
        print(f"    flipped one ciphertext byte at offset {off} of "
              f"{os.path.basename(path)} (upload {poisoned['upload_id'][:12]}…)", flush=True)
        check("[2] the poisoned staged part was a VERSION_ENCRYPTED CRNB container",
              trailer_encrypted(bytes(raw)))

    # --- background single-part PUT stream --------------------------------------------------------
    bg_body = body_of(BG_SIZE)
    bg_sha = hashlib.sha256(bg_body).hexdigest()
    bg_stop = threading.Event()
    bg_stamps = []          # list.append is atomic under the GIL
    bg_errors = []
    bg_keys = []

    def bg_worker(idx):
        n = 0
        while not bg_stop.is_set():
            key = f"bg-{idx}-{n:06d}"
            try:
                s3.put_object(Bucket=bucket, Key=key, Body=bg_body,
                              ServerSideEncryption="AES256")
                bg_stamps.append(time.time())
                if len(bg_keys) < BG_VERIFY:
                    bg_keys.append(key)
            except Exception as exc:  # noqa: BLE001 - ANY background failure is a gate failure
                bg_errors.append(f"{key}: {err_of(exc)} {exc}")
            n += 1

    health = Healthz(EP)
    health.start()
    bg_pool = concurrent.futures.ThreadPoolExecutor(max_workers=BG_WORKERS)
    bg_futures = [bg_pool.submit(bg_worker, i) for i in range(BG_WORKERS)]
    t_base0 = time.time()
    time.sleep(BG_BASE_SECS)
    t_base1 = time.time()

    # --- BARRIER: every Complete released at the same instant --------------------------------------
    barrier = threading.Barrier(SESSIONS)
    results = {}

    # Stamp the instant each thread is RELEASED by the barrier. t_bar0 must be the release, not the
    # executor-spawn time: threads are spawned and block on wait(), so spawn-time would widen the
    # window to include pre-barrier seconds and the "background PUTs progressed during the barrier"
    # gate could be satisfied by a PUT that landed before any assemble had even started.
    release_stamps = []

    def complete_one(sess):
        barrier.wait()
        t0 = time.time()
        release_stamps.append(t0)
        try:
            r = complete(sess)
            results[sess["key"]] = ("ok", time.time() - t0, r)
        except Exception as exc:  # noqa: BLE001
            results[sess["key"]] = ("err", time.time() - t0, exc)

    t_spawn = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=SESSIONS) as pool:
        list(pool.map(complete_one, sessions))
    t_bar1 = time.time()
    # All threads clear the barrier together; take the last release as the true window start.
    t_bar0 = max(release_stamps) if release_stamps else t_spawn

    bg_stop.set()
    for f in bg_futures:
        f.result()
    bg_pool.shutdown()
    health.stop.set()
    health.join(timeout=5)

    # --- gates: the healthy sessions ---------------------------------------------------------------
    healthy = [s for s in sessions if s is not poisoned]
    bad = []
    for sess in healthy:
        kind, _, payload = results[sess["key"]]
        if kind != "ok":
            bad.append(f"{sess['key']}: {err_of(payload)}")
    check(f"[1] every one of the {len(healthy)} concurrent Completes returned 200 "
          f"(failures: {bad[:3] or 'none'})", not bad)
    mismatch = []
    for sess in healthy:
        if results[sess["key"]][0] != "ok":
            continue
        try:
            digest, size = digest_of_get(sess["bucket"], sess["key"])
        except Exception as exc:  # noqa: BLE001
            mismatch.append(f"{sess['key']}: GET raised {err_of(exc)}")
            continue
        if digest != sess["sha"] or size != sess["size"]:
            mismatch.append(f"{sess['key']}: {size}B/{digest[:12]} != {sess['size']}B/{sess['sha'][:12]}")
    check(f"[1] every concurrently-assembled object GETs back BYTE-EXACT "
          f"(mismatches: {mismatch[:3] or 'none'})", not mismatch)
    etag_bad = [s["key"] for s in healthy
                if results[s["key"]][0] == "ok"
                and not str(results[s["key"]][2].get("ETag", "")).rstrip('"').endswith("-2")]
    check(f"[1] every completion returned a multipart-shaped ETag (-2) "
          f"(bad: {etag_bad[:3] or 'none'})", not etag_bad)

    # --- gates: scenario 2, fail-closed and ISOLATED ------------------------------------------------
    pkind, _, ppayload = results[poisoned["key"]]
    check("[2] the poisoned session's Complete ERRORS (never 200)", pkind == "err")
    if pkind == "err":
        status, code = err_of(ppayload)
        print(f"    (poisoned Complete rejected with HTTP {status} {code})", flush=True)
        ADVISORY["poison_status"] = status
        ADVISORY["poison_code"] = code
        # A corrupted stored part is an integrity failure, not a client error: BlobError::Corruption
        # maps to Error::Internal -> HTTP 500 InternalError. That 500 is the CORRECT fail-closed
        # answer, so it is expected here and declared to the launcher, which gates the server's 5xx
        # counter against EXACTLY this count -- any other 5xx anywhere still fails the run.
        declare_5xx(status)
        check("[2] the poisoned Complete was REJECTED with a real HTTP error status "
              f"(got {status})", status is not None and status >= 400)
    check("[2] NO object was committed for the poisoned key (HEAD 404)",
          absent(bucket, poisoned["key"]))
    ADVISORY["poison_isolated_ok"] = len([s for s in healthy if results[s["key"]][0] == "ok"])

    # --- gates: the background single-part stream ----------------------------------------------------
    check(f"[1] every background single-part PUT succeeded (errors: {bg_errors[:3] or 'none'})",
          not bg_errors)
    during = [t for t in bg_stamps if t_bar0 <= t <= t_bar1]
    check(f"[1] ordinary single-part PUTs made progress DURING the concurrent-assemble barrier "
          f"({len(during)} completed) — concurrent assembles did not starve them", len(during) > 0)
    bg_mismatch = []
    for key in bg_keys[:BG_VERIFY]:
        try:
            digest, size = digest_of_get(bucket, key)
        except Exception as exc:  # noqa: BLE001
            bg_mismatch.append(f"{key}: {err_of(exc)}")
            continue
        if digest != bg_sha or size != len(bg_body):
            bg_mismatch.append(f"{key}: digest mismatch")
    check(f"[1] background objects written under the barrier read back BYTE-EXACT "
          f"(bad: {bg_mismatch[:3] or 'none'})", not bg_mismatch)
    check(f"[1] /healthz never stopped answering during the whole phase "
          f"({health.probes} probes, {health.failures} unanswered within {HEALTHZ_TIMEOUT}s, "
          f"worst {health.worst_ms:.0f} ms)", health.failures == 0)

    # --- advisory ------------------------------------------------------------------------------------
    base_span = max(t_base1 - t_base0 - 0.5, 0.001)
    base_n = len([t for t in bg_stamps if t_base0 + 0.5 <= t <= t_base1])
    bar_span = max(t_bar1 - t_bar0, 0.001)
    walls = sorted(r[1] for r in results.values())
    ADVISORY.update({
        "sessions": SESSIONS,
        "bg_put_baseline_ops": base_n,
        "bg_put_baseline_ops_s": round(base_n / base_span, 2),
        "bg_put_during_barrier_ops": len(during),
        "bg_put_during_barrier_ops_s": round(len(during) / bar_span, 2),
        "bg_put_total": len(bg_stamps),
        "barrier_wall_secs": round(bar_span, 2),
        "complete_wall_min": round(walls[0], 3),
        "complete_wall_med": round(walls[len(walls) // 2], 3),
        "complete_wall_max": round(walls[-1], 3),
        "healthz_probes": health.probes,
        "healthz_worst_ms": round(health.worst_ms, 1),
    })
    ratio = (ADVISORY["bg_put_during_barrier_ops_s"] /
             ADVISORY["bg_put_baseline_ops_s"] * 100.0) if base_n else 0.0
    ADVISORY["bg_put_barrier_ratio_pct"] = round(ratio, 1)
    print(f"    advisory: background PUT {ADVISORY['bg_put_baseline_ops_s']} ops/s alone -> "
          f"{ADVISORY['bg_put_during_barrier_ops_s']} ops/s during the barrier ({ratio:.0f}%); "
          f"Complete wall min/med/max {ADVISORY['complete_wall_min']}/"
          f"{ADVISORY['complete_wall_med']}/{ADVISORY['complete_wall_max']} s", flush=True)


# ============================================================ scenario 3a: complete vs abort
def scenario_3_race():
    print(f"\n[3a] complete-vs-abort on the SAME upload id, {RACE_ROUNDS} rounds", flush=True)
    bucket = "mprace"
    s3.create_bucket(Bucket=bucket)
    # Session state, not assembly, is what is under test -- so these sessions are SINGLE-part (the
    # final part has no 5 MiB minimum) and stay cheap enough to run many rounds.
    ALLOWED = {"NoSuchUpload", "InvalidRequest", "InvalidPart", "NoSuchKey"}
    tally = {"complete": 0, "abort": 0, "both": 0, "neither": 0}
    bad_codes, torn, orphans, unresolved = [], [], [], []

    for r in range(RACE_ROUNDS):
        sess = stage_session(bucket, f"race-{r:02d}", nparts=1, tail=TAIL_SIZE)
        gate = threading.Barrier(2)
        out = {}

        def do_complete():
            gate.wait()
            try:
                complete(sess)
                out["complete"] = ("ok", None)
            except Exception as exc:  # noqa: BLE001
                out["complete"] = ("err", err_of(exc))

        def do_abort():
            gate.wait()
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=sess["key"],
                                          UploadId=sess["upload_id"])
                out["abort"] = ("ok", None)
            except Exception as exc:  # noqa: BLE001
                out["abort"] = ("err", err_of(exc))

        ta = threading.Thread(target=do_complete)
        tb = threading.Thread(target=do_abort)
        ta.start(); tb.start(); ta.join(); tb.join()

        c_ok = out["complete"][0] == "ok"
        a_ok = out["abort"][0] == "ok"
        tally["both" if (c_ok and a_ok) else
              "complete" if c_ok else "abort" if a_ok else "neither"] += 1

        # Every non-winner must fail with a WELL-FORMED S3 error code: losing a documented race is a
        # client-visible condition, not a server fault. ONE interleaving is a known exception (see
        # below), so it is recorded and declared rather than gated.
        for side in ("complete", "abort"):
            kind, info = out[side]
            if kind != "err":
                continue
            status, code = info
            if code in ALLOWED and status is not None and status < 500:
                continue
            # FIXED and now GATED: when Abort won the race *after* Complete had claimed the session
            # and entered `assemble`, the loser used to fail BlobError::Io -> Error::Internal -> 500
            # instead of the AWS-shaped NoSuchUpload. complete_multipart now re-checks the session on
            # an assemble failure (abort commits its row delete BEFORE pulling the part bytes, so
            # bytes-gone implies row-gone) and returns NoSuchUpload — which is in ALLOWED, so it is
            # caught by the `code in ALLOWED and status < 500` branch above. Any complete-side 5xx
            # here is now a REGRESSION and falls through to `bad_codes`, failing the run.
            bad_codes.append(f"round {r} {side}: HTTP {status} {code}")

        # The object is either fully there (byte-exact) or not there at all -- never torn.
        if c_ok:
            try:
                digest, size = digest_of_get(bucket, sess["key"])
                if digest != sess["sha"] or size != sess["size"]:
                    torn.append(f"round {r}: completed object is not byte-exact")
            except Exception as exc:  # noqa: BLE001
                torn.append(f"round {r}: completed object GET raised {err_of(exc)}")
        elif not absent(bucket, sess["key"]):
            torn.append(f"round {r}: complete FAILED yet an object exists for the key")

        # Whoever won, the session must be RESOLVED (no zombie upload id) and must leave no staged
        # bytes behind. Both are absolute post-conditions, independent of who won the race.
        try:
            s3.list_parts(Bucket=bucket, Key=sess["key"], UploadId=sess["upload_id"])
            unresolved.append(f"round {r}: upload id still listable after the race")
        except ClientError as e:
            if e.response["Error"].get("Code") != "NoSuchUpload":
                unresolved.append(f"round {r}: ListParts -> {err_of(e)}")
        left = staged_parts(sess["upload_id"])
        if left:
            orphans.append(f"round {r}: {len(left)} staged file(s) left in "
                           f"{staging_dir(sess['upload_id'])}")

    print(f"    outcomes: complete won {tally['complete']}, abort won {tally['abort']}, "
          f"both succeeded {tally['both']}, neither {tally['neither']}", flush=True)
    check(f"[3a] every loser returned a well-formed S3 error code from "
          f"{sorted(ALLOWED)} — including the abort-race loser, now NoSuchUpload not 500 "
          f"(bad: {bad_codes[:3] or 'none'})",
          not bad_codes)
    check(f"[3a] the object is always all-or-nothing, never torn (bad: {torn[:3] or 'none'})", not torn)
    check(f"[3a] the upload id is RESOLVED after every race (bad: {unresolved[:3] or 'none'})",
          not unresolved)
    check(f"[3a] no staging orphans left behind by any race round "
          f"(bad: {orphans[:3] or 'none'})", not orphans)
    check("[3a] at least one side won every round (the race never deadlocks both)",
          tally["neither"] == 0)
    ADVISORY["race_outcomes"] = tally


# ============================================================ scenario 3b: RecordPart supersede
def scenario_3_supersede():
    print("\n[3b] RecordPart supersede: re-upload part 1 while part 2 uploads concurrently",
          flush=True)
    bucket = "mprace"
    key = "supersede"
    uid = create_session(bucket, key)
    v1, v2 = body_of(PART_SIZE), body_of(PART_SIZE)
    tail = body_of(TAIL_SIZE)
    etags = {}
    errs = []

    def upload_part1():
        # SEQUENTIAL by construction: v2's UploadPart starts only after v1's returned, so "the last
        # uploaded bytes for part 1" is deterministic even though part 2 races alongside.
        try:
            etags["v1"] = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid,
                                         PartNumber=1, Body=v1)["ETag"]
            etags["v2"] = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid,
                                         PartNumber=1, Body=v2)["ETag"]
        except Exception as exc:  # noqa: BLE001
            errs.append(f"part1: {err_of(exc)}")

    def upload_part2():
        try:
            etags["tail"] = s3.upload_part(Bucket=bucket, Key=key, UploadId=uid,
                                           PartNumber=2, Body=tail)["ETag"]
        except Exception as exc:  # noqa: BLE001
            errs.append(f"part2: {err_of(exc)}")

    ta = threading.Thread(target=upload_part1)
    tb = threading.Thread(target=upload_part2)
    ta.start(); tb.start(); ta.join(); tb.join()
    if not check(f"[3b] all concurrent part uploads succeeded (errors: {errs or 'none'})", not errs):
        return
    check("[3b] the two uploads of part 1 produced DIFFERENT ETags (distinct bodies)",
          etags["v1"] != etags["v2"])

    # The superseded (stale) ETag must no longer be accepted -- proof that INSERT OR REPLACE really
    # replaced the row rather than leaving two candidate rows for part 1.
    stale_code = None
    try:
        s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=uid,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etags["v1"]},
                                       {"PartNumber": 2, "ETag": etags["tail"]}]})
    except ClientError as e:
        stale_code = err_of(e)
    check(f"[3b] completing with the SUPERSEDED part ETag is rejected InvalidPart 400 "
          f"(got {stale_code})", stale_code == (400, "InvalidPart"))

    ok = True
    try:
        s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=uid,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etags["v2"]},
                                       {"PartNumber": 2, "ETag": etags["tail"]}]})
    except Exception as exc:  # noqa: BLE001
        ok = check(f"[3b] completing with the LAST part ETag succeeds ({err_of(exc)})", False)
    if ok:
        want = hashlib.sha256(v2 + tail).hexdigest()
        digest, size = digest_of_get(bucket, key)
        check("[3b] the completed object is byte-exact and holds the LAST bytes uploaded for part 1",
              digest == want and size == len(v2) + len(tail))
        check("[3b] the completed object does NOT hold the superseded part-1 bytes",
              digest != hashlib.sha256(v1 + tail).hexdigest())


# ============================================================ scenario 4: UploadPartCopy
def scenario_4_copy():
    print(f"\n[4] UploadPartCopy under concurrency ({COPY_SESSIONS} copy sessions + "
          f"{BODY_SESSIONS} body sessions, staged in one pool)", flush=True)
    bucket = "mpcopy"
    s3.create_bucket(Bucket=bucket)
    src_big, src_tail = body_of(PART_SIZE), body_of(TAIL_SIZE)
    s3.put_object(Bucket=bucket, Key="src-big", Body=src_big, ServerSideEncryption="AES256")
    s3.put_object(Bucket=bucket, Key="src-tail", Body=src_tail, ServerSideEncryption="AES256")
    copy_sha = hashlib.sha256(src_big + src_tail).hexdigest()
    copy_size = len(src_big) + len(src_tail)
    errs = []

    def stage_copy(i):
        key = f"copy-{i:02d}"
        try:
            uid = create_session(bucket, key)
            p1 = s3.upload_part_copy(Bucket=bucket, Key=key, UploadId=uid, PartNumber=1,
                                     CopySource={"Bucket": bucket, "Key": "src-big"})
            p2 = s3.upload_part_copy(Bucket=bucket, Key=key, UploadId=uid, PartNumber=2,
                                     CopySource={"Bucket": bucket, "Key": "src-tail"})
            return {"bucket": bucket, "key": key, "upload_id": uid, "nparts": 2,
                    "sha": copy_sha, "size": copy_size,
                    "parts": [{"PartNumber": 1, "ETag": p1["CopyPartResult"]["ETag"]},
                              {"PartNumber": 2, "ETag": p2["CopyPartResult"]["ETag"]}]}
        except Exception as exc:  # noqa: BLE001
            errs.append(f"{key}: {err_of(exc)} {exc}")
            return None

    def stage_body(i):
        try:
            return stage_session(bucket, f"body-{i:02d}", nparts=2)
        except Exception as exc:  # noqa: BLE001
            errs.append(f"body-{i:02d}: {err_of(exc)} {exc}")
            return None

    jobs = ([("copy", i) for i in range(COPY_SESSIONS)] +
            [("body", i) for i in range(BODY_SESSIONS)])
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        staged = list(pool.map(lambda j: stage_copy(j[1]) if j[0] == "copy" else stage_body(j[1]),
                               jobs))
    sessions = [s for s in staged if s]
    check(f"[4] every session staged its parts under concurrency (errors: {errs[:3] or 'none'})",
          not errs and len(sessions) == len(jobs))

    # On-disk proof BEFORE completing: a copied part is staged through the same seal path as an
    # uploaded one, so it too must be ciphertext on disk.
    bad_trailer, bad_marker, missing = [], [], []
    for sess in sessions:
        files = staged_parts(sess["upload_id"])
        if len(files) != sess["nparts"]:
            missing.append(f"{sess['key']}: {len(files)} staged files, want {sess['nparts']}")
        for path in files:
            with open(path, "rb") as fh:
                blob = fh.read()
            if not trailer_encrypted(blob):
                bad_trailer.append(f"{sess['key']}/{os.path.basename(path)}")
            if MARKER in blob:
                bad_marker.append(f"{sess['key']}/{os.path.basename(path)}")
    check(f"[4] every session staged exactly its parts on disk (bad: {missing[:3] or 'none'})",
          not missing)
    check(f"[4] every staged part (copied AND uploaded) is a VERSION_ENCRYPTED CRNB container "
          f"(bad: {bad_trailer[:3] or 'none'})", not bad_trailer)
    check(f"[4] the known plaintext marker is ABSENT from every staged part "
          f"(leaked: {bad_marker[:3] or 'none'})", not bad_marker)

    barrier = threading.Barrier(len(sessions))
    results = {}

    def complete_one(sess):
        barrier.wait()
        try:
            complete(sess)
            results[sess["key"]] = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            results[sess["key"]] = ("err", err_of(exc))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sessions)) as pool:
        list(pool.map(complete_one, sessions))
    bad = [f"{k}: {v[1]}" for k, v in results.items() if v[0] != "ok"]
    check(f"[4] every copy/body session completed under the barrier (failures: {bad[:3] or 'none'})",
          not bad)
    mismatch = []
    for sess in sessions:
        if results[sess["key"]][0] != "ok":
            continue
        # Guarded like the scenario-1 verify loop: an unguarded GET failure here would abort main()
        # before the ADVISORY JSON is written, so the launcher would read declared_5xx=0 and stack a
        # spurious `unexpected_http_5xx` gate failure on top of the real one.
        try:
            digest, size = digest_of_get(sess["bucket"], sess["key"])
        except Exception as e:  # noqa: BLE001 - any GET failure is a mismatch for this gate
            mismatch.append(f"{sess['key']}({type(e).__name__})")
            continue
        if digest != sess["sha"] or size != sess["size"]:
            mismatch.append(sess["key"])
    check(f"[4] every UploadPartCopy-built object is BYTE-EXACT (bad: {mismatch[:3] or 'none'})",
          not mismatch)


def main():
    t0 = time.time()
    scenario_1_and_2()
    scenario_3_race()
    scenario_3_supersede()
    scenario_4_copy()
    ADVISORY["driver_secs"] = round(time.time() - t0, 1)
    ADVISORY["declared_5xx"] = DECLARED_5XX[0]
    ADVISORY["failures"] = fails
    if OUT_JSON:
        with open(OUT_JSON, "w", encoding="utf-8") as fh:
            json.dump(ADVISORY, fh)
    if fails:
        print(f"\n  multipart stress FAILED ({len(fails)}): " + "; ".join(fails[:6]))
        return 1
    print(f"\n  multipart stress PASSED (all scenarios) in {ADVISORY['driver_secs']}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
