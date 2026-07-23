#!/usr/bin/env python3
"""Adversarial / limit stress driver for conformance/stress_adversarial.sh (ARCH 21 + ARCH 30).

THE GAP THIS FILLS. Every limit and abuse path in this codebase is probed SERIALLY, one request at
a time: `blob_limits.py` fills a tmpfs with one PUT, `objects.py` sends one bad `Content-MD5`,
`sts.py` tampers with one token, `listing.py` walks one continuation loop on a quiet server. Nothing
drives a REJECTION path under CONCURRENCY -- which is precisely where a limit check can be raced
past, a rejected request can corrupt a concurrent healthy one, or a bounded loop can stop being
bounded. The five scenarios here are all "the abusive request and the honest request are in flight
at the same instant":

  1. THE aws-chunked STREAMING DECODER (crates/cairn-protocol/src/chunked.rs, the F-5 component:
     get it wrong and objects corrupt SILENTLY). Many concurrent SigV4 streaming uploads released
     from one barrier, MIXED valid (signed + unsigned framing) and deliberately MALFORMED (bad
     chunk signature, truncated stream, wrong declared chunk length in both directions, oversized
     chunk header, non-hex chunk size, missing per-chunk signature). Driven over RAW SOCKETS with
     hand-rolled SigV4 -- no SDK will emit a malformed chunk stream.
  2. QUOTA and the OBJECT-SIZE CEILING under concurrent writes: a small per-bucket quota attacked by
     concurrent PUTs, and `CAIRN_MAX_OBJECT_SIZE` attacked by concurrent oversized PUTs.
  3. DEEP PAGINATION under concurrent mutation: a continuation-token loop over a large keyspace
     while other workers PUT and DELETE inside the very prefix being listed.
  4. STS session-credential CHURN: sessions minted, used, revoked and tampered with concurrently.
  5. LARGE-FANOUT DeleteObjects: concurrent ~400-key bulk deletes mixing present, absent, invalid
     and duplicated keys, with a control namespace that must survive untouched.

GATE PHILOSOPHY (identical to stress.sh / stress_multipart.sh / soak_features.sh): every gate holds
REGARDLESS OF OFFERED LOAD. Nothing here gates a rate, a latency or a %-throughput. What is gated is
correctness (byte-exact round-trips), EXACT (status, S3 code) pairs -- never "any 4xx" -- absolute
invariants (stored bytes <= quota; the listing loop terminates inside a keyspace-derived bound; no
duplicate key in one pass; no unrelated key destroyed), fail-closed-ness (a rejected request commits
nothing), and liveness. Every assertion loop has a non-zero-count companion gate and every env knob
that could empty a loop is validated up front, so the harness cannot be tuned into a vacuous pass.

FINDINGS, NOT A HIDDEN BUDGET. A malformed request should never be a 5xx. The four aws-chunked
framing malformations that once answered 500 (under-declared length, over-long header, non-hex size,
missing per-chunk signature) are now FIXED and GATED at exactly (400, InvalidArgument) -- the decoder
classifies each as BodyError::Malformed. The mechanism remains for any FUTURE deviation: a case can
pin a `gap` (status, code) that is printed loudly, written to the JSON, and declared into the
launcher's 5xx budget so the budget stays an EQUALITY (any *other* 5xx still fails the run) -- never
silently accepted. The one legitimately-declared 5xx is `507 InsufficientStorage`, the S3-correct
answer to a quota rejection, which simply happens to live in the 5xx range.

Usage: stress_adversarial.py <ak> <sk> <bearer> <s3-endpoint> <mgmt-endpoint> <out-json>
Env knobs (all validated): ADV_CHUNK_ROUNDS ADV_CHUNK_VALID ADV_CHUNK_BODY ADV_CHUNK_PIECE
  ADV_QUOTA_BYTES ADV_QUOTA_OBJ ADV_QUOTA_ATTEMPTS ADV_MAX_OBJECT_SIZE ADV_OVERSIZE_ATTEMPTS
  ADV_PAGE_KEYS ADV_PAGE_SIZE ADV_PAGE_LISTERS ADV_PAGE_CHURN_WORKERS ADV_PAGE_CHURN_KEYS
  ADV_STS_SESSIONS ADV_DEL_WORKERS ADV_DEL_PRESENT ADV_DEL_ABSENT ADV_DEL_CONTROL
  ADV_HEALTHZ_TIMEOUT
"""
import concurrent.futures
import datetime
import hashlib
import hmac
import http.client
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AK, SK, BEARER, EP, MGMT = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
OUT_JSON = sys.argv[6] if len(sys.argv) > 6 else ""

_u = urllib.parse.urlparse(EP)
HOST, PORT = _u.hostname, _u.port
HOSTHDR = f"{HOST}:{PORT}"
REGION = "us-east-1"


def env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


# --- profile ------------------------------------------------------------------------------------
CHUNK_ROUNDS = env_int("ADV_CHUNK_ROUNDS", 6)        # malformed uploads PER CASE
CHUNK_VALID = env_int("ADV_CHUNK_VALID", 24)         # valid streaming uploads in the same barrier
CHUNK_BODY = env_int("ADV_CHUNK_BODY", 192 * 1024)   # valid streaming payload size
CHUNK_PIECE = env_int("ADV_CHUNK_PIECE", 64 * 1024)  # chunk size within the framing (>= 2 chunks)

QUOTA_BYTES = env_int("ADV_QUOTA_BYTES", 512 * 1024)
QUOTA_OBJ = env_int("ADV_QUOTA_OBJ", 64 * 1024)
QUOTA_ATTEMPTS = env_int("ADV_QUOTA_ATTEMPTS", 48)   # >> QUOTA_BYTES/QUOTA_OBJ, so some MUST fail

# Must match the launcher's CAIRN_MAX_OBJECT_SIZE; the launcher exports it, and a mismatch is
# caught by the validation below rather than producing a silently-passing scenario.
MAX_OBJECT_SIZE = env_int("ADV_MAX_OBJECT_SIZE", 1024 * 1024)
OVERSIZE_ATTEMPTS = env_int("ADV_OVERSIZE_ATTEMPTS", 12)

PAGE_KEYS = env_int("ADV_PAGE_KEYS", 2000)           # stable keys, never mutated during the pass
PAGE_SIZE = env_int("ADV_PAGE_SIZE", 100)            # MaxKeys per page
PAGE_LISTERS = env_int("ADV_PAGE_LISTERS", 3)        # concurrent full passes
PAGE_CHURN_WORKERS = env_int("ADV_PAGE_CHURN_WORKERS", 4)
PAGE_CHURN_KEYS = env_int("ADV_PAGE_CHURN_KEYS", 50)  # FIXED per-worker key ring (see the bound)

STS_SESSIONS = env_int("ADV_STS_SESSIONS", 10)

DEL_WORKERS = env_int("ADV_DEL_WORKERS", 6)
DEL_PRESENT = env_int("ADV_DEL_PRESENT", 200)
DEL_ABSENT = env_int("ADV_DEL_ABSENT", 200)
DEL_CONTROL = env_int("ADV_DEL_CONTROL", 80)

HEALTHZ_TIMEOUT = env_int("ADV_HEALTHZ_TIMEOUT", 60)  # a WEDGE detector, not a latency budget

# --- anti-vacuity validation --------------------------------------------------------------------
# Rule: every env knob that could empty an assertion loop is rejected here. A harness that can be
# silently neutered into "0 iterations, 0 failures, PASS" is worse than no harness at all.
if CHUNK_ROUNDS < 1:
    sys.exit(f"ADV_CHUNK_ROUNDS must be >= 1 (got {CHUNK_ROUNDS})")
if CHUNK_VALID < 2:
    sys.exit(f"ADV_CHUNK_VALID must be >= 2 (one signed + one unsigned) (got {CHUNK_VALID})")
if CHUNK_PIECE < 1024 or CHUNK_BODY < 2 * CHUNK_PIECE:
    sys.exit("ADV_CHUNK_BODY must be >= 2*ADV_CHUNK_PIECE and ADV_CHUNK_PIECE >= 1024 so every "
             f"valid stream is genuinely MULTI-chunk (got {CHUNK_BODY}/{CHUNK_PIECE})")
if CHUNK_BODY > MAX_OBJECT_SIZE:
    sys.exit(f"ADV_CHUNK_BODY ({CHUNK_BODY}) must be <= ADV_MAX_OBJECT_SIZE ({MAX_OBJECT_SIZE}) or "
             "every valid streaming upload would be rejected by the size ceiling instead")
if QUOTA_OBJ < 1 or QUOTA_BYTES < 2 * QUOTA_OBJ:
    sys.exit("ADV_QUOTA_BYTES must fit at least 2 objects (got "
             f"{QUOTA_BYTES}/{QUOTA_OBJ})")
if QUOTA_ATTEMPTS <= QUOTA_BYTES // QUOTA_OBJ:
    sys.exit(f"ADV_QUOTA_ATTEMPTS ({QUOTA_ATTEMPTS}) must EXCEED the number of objects the quota "
             f"admits ({QUOTA_BYTES // QUOTA_OBJ}) or no PUT would ever be rejected")
if QUOTA_OBJ > MAX_OBJECT_SIZE:
    sys.exit("ADV_QUOTA_OBJ must be <= ADV_MAX_OBJECT_SIZE (the size ceiling would reject first, "
             "so the quota would never be the thing under test)")
if OVERSIZE_ATTEMPTS < 2:
    sys.exit(f"ADV_OVERSIZE_ATTEMPTS must be >= 2 (got {OVERSIZE_ATTEMPTS})")
if PAGE_SIZE < 1 or PAGE_KEYS <= PAGE_SIZE:
    sys.exit(f"ADV_PAGE_KEYS ({PAGE_KEYS}) must EXCEED ADV_PAGE_SIZE ({PAGE_SIZE}) or the listing "
             "would never paginate and the continuation-token loop would be vacuous")
if PAGE_LISTERS < 1 or PAGE_CHURN_WORKERS < 1 or PAGE_CHURN_KEYS < 1:
    sys.exit("ADV_PAGE_LISTERS / ADV_PAGE_CHURN_WORKERS / ADV_PAGE_CHURN_KEYS must all be >= 1 "
             "(a pass with no concurrent mutation is not the thing under test)")
if STS_SESSIONS < 3:
    sys.exit(f"ADV_STS_SESSIONS must be >= 3 (used / revoked / tampered) (got {STS_SESSIONS})")
if DEL_WORKERS < 2:
    sys.exit(f"ADV_DEL_WORKERS must be >= 2 for the fan-out to be concurrent (got {DEL_WORKERS})")
if DEL_PRESENT < 1 or DEL_ABSENT < 1 or DEL_CONTROL < 1:
    sys.exit("ADV_DEL_PRESENT / ADV_DEL_ABSENT / ADV_DEL_CONTROL must all be >= 1")
if DEL_PRESENT + DEL_ABSENT + 4 > 1000:
    sys.exit("ADV_DEL_PRESENT + ADV_DEL_ABSENT must leave room under S3's 1000-key DeleteObjects "
             "cap (this harness adds 1 invalid + 3 duplicate entries)")

POOL = max(CHUNK_VALID + CHUNK_ROUNDS * 8, DEL_WORKERS, PAGE_LISTERS + PAGE_CHURN_WORKERS, 32)
# `total_max_attempts` (NOT `max_attempts`, which botocore reads as the number of RETRIES, i.e.
# total = N+1) is what genuinely disables retries. It is load-bearing everywhere a deliberate error
# is asserted: a silent retry can rewrite the very status this harness gates on -- e.g. a retried
# quota rejection or a retried DeleteObjects would land on a different server state and answer a
# different, adjacent-but-wrong code.
BOTO_CFG = Config(s3={"addressing_style": "path"},
                  retries={"total_max_attempts": 1, "mode": "standard"},
                  connect_timeout=20, read_timeout=300, max_pool_connections=POOL)
s3 = boto3.client("s3", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK,
                  region_name=REGION, config=BOTO_CFG)

fails = []
findings = []
ADVISORY = {}
DECLARED_5XX = [0]


def declare_5xx(status):
    """Count a 5xx this harness DELIBERATELY provoked into the launcher's budget, which is gated as
    an EQUALITY against the server's own counter -- so any 5xx from any other request still fails."""
    if status is not None and 500 <= status < 600:
        DECLARED_5XX[0] += 1
        return True
    return False


def check(label, cond):
    print(("    ok: " if cond else "    FAIL: ") + label, flush=True)
    if not cond:
        fails.append(label)
    return bool(cond)


def finding(label):
    """A pinned, loudly-reported product observation that this HARNESS-ONLY change records rather
    than turning into a red gate. Everything around it is still gated (fail-closed, nothing
    committed, bounded count), so a regression in the behaviour is still caught."""
    print("    FINDING: " + label, flush=True)
    findings.append(label)


def err_of(exc):
    if isinstance(exc, ClientError):
        return (exc.response["ResponseMetadata"]["HTTPStatusCode"],
                exc.response["Error"].get("Code"))
    return (None, type(exc).__name__)


def body_of(size, seed=b""):
    h = hashlib.sha256(b"adv" + seed).digest()
    out = bytearray()
    while len(out) < size:
        h = hashlib.sha256(h).digest()
        out += h
    return bytes(out[:size])


def absent(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return False
    except ClientError as e:
        return e.response["ResponseMetadata"]["HTTPStatusCode"] == 404


# =============================================================== SigV4 + aws-chunked (raw sockets)
def _sha_hex(b):
    return hashlib.sha256(b).hexdigest()


def _hmac(key, msg):
    return hmac.new(key, msg.encode() if isinstance(msg, str) else msg, hashlib.sha256).digest()


def signing_key(secret, day):
    k = _hmac(("AWS4" + secret).encode(), day)
    k = _hmac(k, REGION)
    k = _hmac(k, "s3")
    return _hmac(k, "aws4_request")


def sign_request(method, path, headers, payload_hash):
    """Sign a request SigV4 (header auth). Returns (headers, seed_signature, amzdate, scope, key)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    h = {k.lower(): v for k, v in headers.items()}
    h["host"] = HOSTHDR
    h["x-amz-date"] = amzdate
    h["x-amz-content-sha256"] = payload_hash
    signed = sorted(h)
    canon_headers = "".join(f"{k}:{str(h[k]).strip()}\n" for k in signed)
    signed_hdrs = ";".join(signed)
    canon = "\n".join([method, urllib.parse.quote(path, safe="/"), "",
                       canon_headers, signed_hdrs, payload_hash])
    scope = f"{day}/{REGION}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, _sha_hex(canon.encode())])
    key = signing_key(SK, day)
    sig = hmac.new(key, sts.encode(), hashlib.sha256).hexdigest()
    h["authorization"] = (f"AWS4-HMAC-SHA256 Credential={AK}/{scope}, "
                          f"SignedHeaders={signed_hdrs}, Signature={sig}")
    return h, sig, amzdate, scope, key


def chunk_sig(key, amzdate, scope, prev, payload):
    """The rolling per-chunk signature (cairn-auth/src/chunked.rs chunk_string_to_sign)."""
    sts = "\n".join(["AWS4-HMAC-SHA256-PAYLOAD", amzdate, scope, prev,
                     _sha_hex(b""), _sha_hex(payload)])
    return hmac.new(key, sts.encode(), hashlib.sha256).hexdigest()


def raw_put(path, headers, body, timeout=120):
    """Send a fully-formed HTTP/1.1 PUT over a raw socket and return (status, s3-code).

    Raw sockets, not http.client: this harness deliberately sends bodies the server will reject
    MID-STREAM, after which hyper answers and may close the connection while the client is still
    writing. `http.client` turns that into a send-side exception and loses the response the server
    actually produced -- exactly the status this harness exists to assert. Here a broken pipe on the
    write is swallowed and the response is still read.
    """
    head = f"PUT {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
    sock = socket.create_connection((HOST, PORT), timeout=timeout)
    try:
        sock.settimeout(timeout)
        try:
            sock.sendall(head.encode() + body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass  # the server rejected mid-body; the response is still on the socket
        buf = b""
        while b"\r\n\r\n" not in buf:
            piece = sock.recv(65536)
            if not piece:
                break
            buf += piece
        if not buf:
            return (0, "NO_RESPONSE")
        head_bytes, _, rest = buf.partition(b"\r\n\r\n")
        lines = head_bytes.decode("latin-1").split("\r\n")
        status = int(lines[0].split()[1])
        clen = 0
        for line in lines[1:]:
            if line.lower().startswith("content-length:"):
                clen = int(line.split(":", 1)[1].strip())
        while len(rest) < clen:
            piece = sock.recv(65536)
            if not piece:
                break
            rest += piece
        code = ""
        text = rest.decode("utf-8", "replace")
        if "<Code>" in text:
            code = text.split("<Code>", 1)[1].split("</Code>", 1)[0]
        return (status, code)
    finally:
        sock.close()


def frame_signed(pieces, amzdate, scope, key, seed, corrupt=None):
    """Build a signed aws-chunked body. `corrupt` mutates the framing for the malformed cases."""
    out = bytearray()
    prev = seed
    for i, piece in enumerate(pieces):
        sig = chunk_sig(key, amzdate, scope, prev, piece)
        prev = sig
        if corrupt == "bad_signature" and i == 1:
            sig = "f" * 64
        if corrupt == "no_signature" and i == 1:
            out += f"{len(piece):x}\r\n".encode() + piece + b"\r\n"
            continue
        if corrupt == "bad_hex" and i == 1:
            out += f"zzzz;chunk-signature={sig}\r\n".encode() + piece + b"\r\n"
            continue
        if corrupt == "oversized_header" and i == 1:
            # A header line that never terminates, past MAX_HEADER_LINE (16 KiB, chunked.rs).
            out += b"a" * 20000
            return bytes(out)
        if corrupt == "declared_short" and i == 1:
            # The header UNDER-declares: the decoder takes 16 bytes as payload and then finds a
            # payload byte where the chunk's CRLF must be.
            out += f"10;chunk-signature={sig}\r\n".encode() + piece + b"\r\n"
            continue
        if corrupt == "declared_long" and i == len(pieces) - 1:
            # The header OVER-declares and the stream then ends: the decoder is mid-chunk at EOF.
            out += f"{len(piece) + 4096:x};chunk-signature={sig}\r\n".encode() + piece
            return bytes(out)
        out += f"{len(piece):x};chunk-signature={sig}\r\n".encode() + piece + b"\r\n"
    if corrupt == "truncated":
        return bytes(out)  # no terminating zero-size chunk at all
    zero = chunk_sig(key, amzdate, scope, prev, b"")
    out += f"0;chunk-signature={zero}\r\n\r\n".encode()
    return bytes(out)


def frame_unsigned(pieces):
    out = bytearray()
    for piece in pieces:
        out += f"{len(piece):x}\r\n".encode() + piece + b"\r\n"
    out += b"0\r\n\r\n"
    return bytes(out)


def streaming_put(bucket, key, payload, mode="signed", corrupt=None):
    """One SigV4 streaming (aws-chunked) PUT over a raw socket. Returns (status, s3-code)."""
    pieces = [payload[i:i + CHUNK_PIECE] for i in range(0, len(payload), CHUNK_PIECE)] or [b""]
    sentinel = ("STREAMING-AWS4-HMAC-SHA256-PAYLOAD" if mode == "signed"
                else "STREAMING-UNSIGNED-PAYLOAD")
    path = f"/{bucket}/{key}"
    hdrs = {"content-encoding": "aws-chunked",
            "x-amz-decoded-content-length": str(len(payload))}
    signed_hdrs, seed, amzdate, scope, key_bytes = sign_request("PUT", path, hdrs, sentinel)
    body = (frame_signed(pieces, amzdate, scope, key_bytes, seed, corrupt) if mode == "signed"
            else frame_unsigned(pieces))
    signed_hdrs["content-length"] = str(len(body))
    return raw_put(path, signed_hdrs, body)


# =============================================================================== /healthz watchdog
class Healthz(threading.Thread):
    """Gates on "did /healthz EVER fail to answer", never on how fast it answered: probe latency is
    offered load on a contended runner, a stopped answer is a wedge. Worst latency is advisory."""

    def __init__(self):
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.probes = 0
        self.failures = 0
        self.worst_ms = 0.0

    def run(self):
        while not self.stop.is_set():
            t0 = time.time()
            try:
                c = http.client.HTTPConnection(HOST, PORT, timeout=HEALTHZ_TIMEOUT)
                c.request("GET", "/healthz")
                r = c.getresponse()
                r.read()
                c.close()
                ok = r.status == 200
            except Exception:  # noqa: BLE001 - any transport failure is a liveness gap
                ok = False
            self.probes += 1
            self.worst_ms = max(self.worst_ms, (time.time() - t0) * 1000.0)
            if not ok:
                self.failures += 1
            self.stop.wait(0.1)


HEALTH = Healthz()


# ================================================================== 1. the aws-chunked decoder
# Each malformed case declares the EXACT (status, code) pair that is CORRECT for it. `gap` is the
# pair this server actually returns today where that differs -- pinned so the deviation is REPORTED
# (and its 5xx declared into the equality budget), never silently accepted. Anything outside
# {expect, gap} fails the run, so a wrong-but-adjacent code cannot pass.
MALFORMED_CASES = [
    # A tampered chunk signature is an authentication failure, and the decoder tags it so the
    # ingest path surfaces it as one (CHUNK_SIGNATURE_FAILURE_MARKER -> SignatureDoesNotMatch).
    {"name": "bad_chunk_signature", "corrupt": "bad_signature",
     "expect": (403, "SignatureDoesNotMatch"), "gap": None},
    # The body ends before the terminating zero-size chunk: DecodeError::Incomplete at finish()
    # -> BodyError::Truncated -> InvalidArgument ("client body ended prematurely").
    {"name": "truncated_stream", "corrupt": "truncated",
     "expect": (400, "InvalidArgument"), "gap": None},
    # The final chunk header OVER-declares its size and the stream ends mid-chunk: same
    # Incomplete -> Truncated path, reached from the Data state instead of the Header state.
    {"name": "declared_length_too_long", "corrupt": "declared_long",
     "expect": (400, "InvalidArgument"), "gap": None},
    # The rest UNDER-declare / mangle the framing. All are pure client-side malformation, so a 4xx
    # is the correct answer, and now GATED at exactly that: the decoder classifies each framing
    # DecodeError as BodyError::Malformed -> InvalidArgument (400) instead of the old blanket
    # Body(Transport(..)) -> `other => Error::Internal` (500). `gap: None` -> no 5xx is declared and
    # any answer other than (400, InvalidArgument) now FAILS the run (previously a pinned FINDING).
    {"name": "declared_length_too_short", "corrupt": "declared_short",
     "expect": (400, "InvalidArgument"), "gap": None},
    {"name": "oversized_chunk_header", "corrupt": "oversized_header",
     "expect": (400, "InvalidArgument"), "gap": None},
    {"name": "non_hex_chunk_size", "corrupt": "bad_hex",
     "expect": (400, "InvalidArgument"), "gap": None},
    {"name": "missing_chunk_signature", "corrupt": "no_signature",
     "expect": (400, "InvalidArgument"), "gap": None},
]


def scenario_1_chunked():
    print(f"\n[1] aws-chunked STREAMING DECODER under concurrency: {CHUNK_VALID} valid + "
          f"{len(MALFORMED_CASES) * CHUNK_ROUNDS} malformed uploads, one barrier", flush=True)
    bucket = "advchunk"
    s3.create_bucket(Bucket=bucket)

    jobs = []
    for i in range(CHUNK_VALID):
        mode = "signed" if i % 2 == 0 else "unsigned"
        jobs.append({"kind": "valid", "mode": mode, "key": f"valid-{mode}-{i:03d}",
                     "payload": body_of(CHUNK_BODY, str(i).encode())})
    for case in MALFORMED_CASES:
        for r in range(CHUNK_ROUNDS):
            jobs.append({"kind": "bad", "case": case, "key": f"bad-{case['name']}-{r:02d}",
                         "payload": body_of(CHUNK_BODY, b"bad")})

    barrier = threading.Barrier(len(jobs))
    results = {}

    def run_one(job):
        barrier.wait()
        try:
            if job["kind"] == "valid":
                results[job["key"]] = streaming_put(bucket, job["key"], job["payload"], job["mode"])
            else:
                results[job["key"]] = streaming_put(bucket, job["key"], job["payload"],
                                                    "signed", job["case"]["corrupt"])
        except Exception as exc:  # noqa: BLE001
            results[job["key"]] = (0, type(exc).__name__)

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        list(pool.map(run_one, jobs))
    ADVISORY["chunk_barrier_secs"] = round(time.time() - t0, 2)

    # --- every VALID stream round-trips byte-exact, alongside the malformed ones -----------------
    bad_status, mismatch = [], []
    for job in jobs:
        if job["kind"] != "valid":
            continue
        status, code = results[job["key"]]
        if status != 200:
            bad_status.append(f"{job['key']}: HTTP {status} {code}")
            continue
        try:
            got = s3.get_object(Bucket=bucket, Key=job["key"])["Body"].read()
        except Exception as exc:  # noqa: BLE001
            mismatch.append(f"{job['key']}: GET raised {err_of(exc)}")
            continue
        if got != job["payload"]:
            mismatch.append(f"{job['key']}: {len(got)} B != {len(job['payload'])} B")
    valid_n = sum(1 for j in jobs if j["kind"] == "valid")
    check(f"[1] all {valid_n} VALID streaming uploads returned 200 while malformed streams raced "
          f"alongside (bad: {bad_status[:3] or 'none'})", not bad_status)
    check(f"[1] every valid streaming object is BYTE-EXACT — a malformed concurrent stream never "
          f"corrupted one (bad: {mismatch[:3] or 'none'})", not mismatch)
    check(f"[1] the valid-upload loop was NOT vacuous ({valid_n} uploads, both signed and unsigned "
          "framing)", valid_n >= 2)

    # --- every MALFORMED stream is rejected with its exact code, and commits NOTHING ---------------
    wrong_code, committed = [], []
    gap_hits = {}
    for job in jobs:
        if job["kind"] != "bad":
            continue
        case = job["case"]
        got = results[job["key"]]
        if got == case["expect"]:
            pass
        elif case["gap"] is not None and got == case["gap"]:
            gap_hits[case["name"]] = gap_hits.get(case["name"], 0) + 1
            declare_5xx(got[0])
        else:
            wrong_code.append(f"{case['name']}: got HTTP {got[0]} {got[1]}, want {case['expect']}")
        if not absent(bucket, job["key"]):
            committed.append(job["key"])
    bad_n = sum(1 for j in jobs if j["kind"] == "bad")
    check(f"[1] every one of the {bad_n} MALFORMED streams was rejected with its EXACT expected "
          f"(status, code) — or its PINNED deviation (wrong: {wrong_code[:3] or 'none'})",
          not wrong_code)
    check("[1] no malformed stream ever returned 2xx (fail-closed)",
          all(not (200 <= results[j["key"]][0] < 300) for j in jobs if j["kind"] == "bad"))
    check(f"[1] NO object was committed for any malformed stream (leaked: {committed[:3] or 'none'})",
          not committed)
    check(f"[1] the malformed-upload loop was NOT vacuous ({bad_n} uploads across "
          f"{len(MALFORMED_CASES)} distinct malformation classes)",
          bad_n == len(MALFORMED_CASES) * CHUNK_ROUNDS and bad_n > 0)
    # Bounded: the pinned deviation may not fire more often than the malformed uploads that can
    # legitimately reach it, otherwise something ELSE is emitting InternalError.
    gap_total = sum(gap_hits.values())
    gap_capacity = CHUNK_ROUNDS * sum(1 for c in MALFORMED_CASES if c["gap"])
    check(f"[1] pinned 5xx deviations bounded by their case count ({gap_total} <= {gap_capacity})",
          gap_total <= gap_capacity)
    for name, n in sorted(gap_hits.items()):
        finding(f"malformed aws-chunked framing '{name}' answers 500 InternalError "
                f"({n}/{CHUNK_ROUNDS} rounds) instead of a 4xx — fail-closed (nothing committed) "
                "but a client-caused error surfacing as a server fault "
                "(BlobError::Body(Transport(..)) falls into the blanket `other => Error::Internal` "
                "arm in cairn-types/src/error.rs)")
    ADVISORY["chunk_gap_hits"] = gap_hits
    ADVISORY["chunk_valid"] = valid_n
    ADVISORY["chunk_malformed"] = bad_n
    check(f"[1] /healthz never stopped answering during the decoder barrier "
          f"({HEALTH.probes} probes, {HEALTH.failures} unanswered within {HEALTHZ_TIMEOUT}s)",
          HEALTH.failures == 0)


# ================================================================ 2. quota + object-size ceiling
def mgmt(method, path, payload=None):
    req = urllib.request.Request(
        f"{MGMT}/api/v1{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {BEARER}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None


def scenario_2_quota():
    admits = QUOTA_BYTES // QUOTA_OBJ
    print(f"\n[2] QUOTA under concurrent writes: quota {QUOTA_BYTES} B admits exactly {admits} × "
          f"{QUOTA_OBJ} B objects, {QUOTA_ATTEMPTS} concurrent PUTs attack it", flush=True)
    bucket = "advquota"
    s3.create_bucket(Bucket=bucket)
    status, _ = mgmt("PUT", f"/buckets/{bucket}/quota", {"quota_bytes": QUOTA_BYTES})
    if not check(f"[2] the per-bucket quota was set via the management API (HTTP {status})",
                 status in (200, 204)):
        return
    status, cfg = mgmt("GET", f"/buckets/{bucket}/config")
    check(f"[2] the store reports the configured quota back ({cfg.get('quota_bytes') if cfg else None})",
          bool(cfg) and cfg.get("quota_bytes") == QUOTA_BYTES)

    payload = body_of(QUOTA_OBJ, b"q")
    barrier = threading.Barrier(QUOTA_ATTEMPTS)
    outcomes = {}

    def put_one(i):
        barrier.wait()
        try:
            s3.put_object(Bucket=bucket, Key=f"q-{i:04d}", Body=payload)
            outcomes[i] = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            outcomes[i] = ("err", err_of(exc))

    with concurrent.futures.ThreadPoolExecutor(max_workers=QUOTA_ATTEMPTS) as pool:
        list(pool.map(put_one, range(QUOTA_ATTEMPTS)))

    accepted = [i for i, v in outcomes.items() if v[0] == "ok"]
    rejected = [v[1] for v in outcomes.values() if v[0] == "err"]
    stored = 0
    token, pages = None, 0
    while True:
        kw = {"Bucket": bucket, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        stored += sum(o["Size"] for o in page.get("Contents", []))
        pages += 1
        token = page.get("NextContinuationToken")
        if not page.get("IsTruncated") or not token or pages > 50:
            break

    # THE gate: no interleaving may push the bucket past its quota. Absolute, load-independent.
    check(f"[2] stored bytes NEVER exceed the quota under concurrent writes "
          f"({stored} <= {QUOTA_BYTES})", stored <= QUOTA_BYTES)
    # The quota is enforced inside the writer's commit transaction against a maintained counter, so
    # with equal-sized objects the admitted count is EXACT, not approximate -- a race that let one
    # extra write through would show up here even if it later fit under the byte gate.
    check(f"[2] exactly {admits} PUTs were admitted, no more and no fewer "
          f"(got {len(accepted)}) — no bypass under the race", len(accepted) == admits)
    check(f"[2] the rejection loop was NOT vacuous ({len(rejected)} PUTs rejected)",
          len(rejected) > 0)
    wrong = [r for r in rejected if r != (507, "InsufficientStorage")]
    check(f"[2] every quota rejection is EXACTLY 507 InsufficientStorage "
          f"(wrong: {wrong[:3] or 'none'})", not wrong)
    # 507 lives in the 5xx range, so each one must be declared or the launcher's equality budget
    # would fail on the harness's OWN deliberate rejections.
    for r in rejected:
        declare_5xx(r[0])

    # Raising the quota must let writes through again: a quota rejection is not a wedge.
    status, _ = mgmt("PUT", f"/buckets/{bucket}/quota", {"quota_bytes": None})
    after = None
    try:
        s3.put_object(Bucket=bucket, Key="q-after-raise", Body=payload)
        after = "ok"
    except Exception as exc:  # noqa: BLE001
        after = err_of(exc)
    check(f"[2] clearing the quota immediately re-admits writes (got {after}) — the rejection "
          "path left no permanent block", after == "ok")
    ADVISORY.update({"quota_bytes": QUOTA_BYTES, "quota_admitted": len(accepted),
                     "quota_rejected": len(rejected), "quota_stored_bytes": stored})

    # --- the object-size ceiling, also raced -----------------------------------------------------
    print(f"\n[2b] CAIRN_MAX_OBJECT_SIZE ({MAX_OBJECT_SIZE} B) under {OVERSIZE_ATTEMPTS} concurrent "
          "oversized PUTs", flush=True)
    big = body_of(MAX_OBJECT_SIZE + 64 * 1024, b"big")
    ob = "advsize"
    s3.create_bucket(Bucket=ob)
    barrier2 = threading.Barrier(OVERSIZE_ATTEMPTS)
    over = {}

    def put_big(i):
        barrier2.wait()
        try:
            s3.put_object(Bucket=ob, Key=f"big-{i:03d}", Body=big)
            over[i] = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            over[i] = ("err", err_of(exc))

    with concurrent.futures.ThreadPoolExecutor(max_workers=OVERSIZE_ATTEMPTS) as pool:
        list(pool.map(put_big, range(OVERSIZE_ATTEMPTS)))
    accepted_big = [i for i, v in over.items() if v[0] == "ok"]
    codes = [v[1] for v in over.values() if v[0] == "err"]
    check(f"[2b] EVERY oversized PUT was rejected ({len(accepted_big)} accepted, want 0)",
          not accepted_big)
    wrong = [c for c in codes if c != (400, "EntityTooLarge")]
    check(f"[2b] every oversized rejection is EXACTLY 400 EntityTooLarge "
          f"(wrong: {wrong[:3] or 'none'})", not wrong)
    check(f"[2b] the oversized loop was NOT vacuous ({len(codes)} rejections)",
          len(codes) == OVERSIZE_ATTEMPTS and len(codes) > 0)
    leaked = [f"big-{i:03d}" for i in range(OVERSIZE_ATTEMPTS)
              if not absent(ob, f"big-{i:03d}")]
    check(f"[2b] no oversized object was committed (leaked: {leaked[:3] or 'none'})", not leaked)
    # A write that FITS still works right after the ceiling was hammered concurrently.
    fit = None
    try:
        s3.put_object(Bucket=ob, Key="fits", Body=body_of(4096, b"f"))
        fit = s3.get_object(Bucket=ob, Key="fits")["Body"].read() == body_of(4096, b"f")
    except Exception as exc:  # noqa: BLE001
        fit = f"raised {err_of(exc)}"
    check(f"[2b] a fitting write still round-trips byte-exact afterwards (got {fit})", fit is True)


# ============================================================ 3. deep pagination under mutation
def scenario_3_pagination():
    # The page bound is derived from the KEYSPACE, not from how much load the runner offered: the
    # churn workers cycle a FIXED ring of keys, so the number of keys that can ever exist under the
    # prefix is (stable + workers*ring) no matter how fast the box is. A loop that needs more pages
    # than that is a token cycle, which is the bug this gate exists to catch.
    ceiling = PAGE_KEYS + PAGE_CHURN_WORKERS * PAGE_CHURN_KEYS
    max_pages = (ceiling // PAGE_SIZE) * 2 + 20
    print(f"\n[3] DEEP PAGINATION under concurrent mutation: {PAGE_KEYS} stable keys, MaxKeys "
          f"{PAGE_SIZE}, {PAGE_LISTERS} concurrent passes, {PAGE_CHURN_WORKERS} churn workers "
          f"(page bound {max_pages})", flush=True)
    bucket = "advpage"
    s3.create_bucket(Bucket=bucket)
    tiny = b"p"
    stable = [f"page/s-{i:05d}" for i in range(PAGE_KEYS)]

    def put_key(k):
        s3.put_object(Bucket=bucket, Key=k, Body=tiny)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(put_key, stable))
    check(f"[3] pre-staged the stable keyspace ({len(stable)} keys)", len(stable) == PAGE_KEYS)

    stop = threading.Event()
    churn_ops = []
    churn_errs = []

    def churn(w):
        n = 0
        while not stop.is_set():
            k = f"page/c-{w:02d}-{n % PAGE_CHURN_KEYS:04d}"
            try:
                if n % 2 == 0:
                    s3.put_object(Bucket=bucket, Key=k, Body=tiny)
                else:
                    s3.delete_object(Bucket=bucket, Key=k)
                churn_ops.append(1)
            except Exception as exc:  # noqa: BLE001
                churn_errs.append(f"{k}: {err_of(exc)}")
            n += 1

    passes = {}

    def one_pass(idx):
        token, pages, seen, order_bad, tok_err = None, 0, [], [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": "page/", "MaxKeys": PAGE_SIZE}
            if token:
                kw["ContinuationToken"] = token
            try:
                page = s3.list_objects_v2(**kw)
            except Exception as exc:  # noqa: BLE001
                tok_err = f"page {pages}: {err_of(exc)}"
                break
            pages += 1
            keys = [o["Key"] for o in page.get("Contents", [])]
            if seen and keys and keys[0] <= seen[-1]:
                order_bad.append(f"page {pages}: {keys[0]!r} <= previous {seen[-1]!r}")
            for a, b in zip(keys, keys[1:]):
                if b <= a:
                    order_bad.append(f"page {pages}: {b!r} <= {a!r} within the page")
            seen.extend(keys)
            truncated = bool(page.get("IsTruncated"))
            token = page.get("NextContinuationToken")
            if truncated and not token:
                tok_err = f"page {pages}: IsTruncated with no NextContinuationToken"
                break
            if not truncated:
                if token:
                    tok_err = f"page {pages}: NextContinuationToken on a non-truncated page"
                break
            if pages > max_pages:
                tok_err = f"exceeded the page bound {max_pages} — continuation token cycle"
                break
        passes[idx] = {"pages": pages, "seen": seen, "order_bad": order_bad, "tok_err": tok_err}

    health_before = HEALTH.failures
    churn_pool = concurrent.futures.ThreadPoolExecutor(max_workers=PAGE_CHURN_WORKERS)
    churn_futs = [churn_pool.submit(churn, w) for w in range(PAGE_CHURN_WORKERS)]
    t0 = time.time()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=PAGE_LISTERS) as pool:
            list(pool.map(one_pass, range(PAGE_LISTERS)))
    finally:
        # The churn threads spin until told to stop; a raised lister must not leave them running.
        stop.set()
        for f in churn_futs:
            f.result()
        churn_pool.shutdown()
    span = time.time() - t0

    tok_errs = [p["tok_err"] for p in passes.values() if p["tok_err"]]
    order_bad = [e for p in passes.values() for e in p["order_bad"]]
    dupes = []
    missing = []
    for idx, p in passes.items():
        if len(set(p["seen"])) != len(p["seen"]):
            counts = {}
            for k in p["seen"]:
                counts[k] = counts.get(k, 0) + 1
            dupes.append(f"pass {idx}: {[k for k, c in counts.items() if c > 1][:3]}")
        missed = set(stable) - set(p["seen"])
        if missed:
            missing.append(f"pass {idx}: {len(missed)} stable keys missing, e.g. {sorted(missed)[:2]}")

    check(f"[3] every continuation-token pass TERMINATED inside the keyspace-derived bound of "
          f"{max_pages} pages (errors: {tok_errs[:2] or 'none'})", not tok_errs)
    check(f"[3] no pass returned a DUPLICATE key ({dupes[:2] or 'none'})", not dupes)
    check(f"[3] keys are strictly increasing within and across pages "
          f"({order_bad[:2] or 'none'})", not order_bad)
    check(f"[3] every key that existed for the WHOLE pass was returned by it "
          f"({missing[:2] or 'none'})", not missing)
    check(f"[3] the churn really ran concurrently with the passes — non-vacuity "
          f"({len(churn_ops)} mutations, {len(churn_errs)} errors)",
          len(churn_ops) > 0 and not churn_errs)
    pages_total = sum(p["pages"] for p in passes.values())
    check(f"[3] the passes really PAGINATED — non-vacuity ({pages_total} pages over "
          f"{PAGE_LISTERS} passes)", pages_total >= PAGE_LISTERS * 2)
    check("[3] /healthz never stopped answering during the pagination phase",
          HEALTH.failures == health_before)
    ADVISORY.update({"page_pages_total": pages_total, "page_churn_ops": len(churn_ops),
                     "page_pass_secs": round(span, 2)})


# ==================================================================== 4. STS session-credential churn
def mint_session(policy, duration=900):
    status, body = mgmt("POST", "/credentials/temporary",
                        {"duration_secs": duration, "policy": policy})
    return status, body


def sess_client(cred, token=None):
    return boto3.client("s3", endpoint_url=EP,
                        aws_access_key_id=cred["access_key_id"],
                        aws_secret_access_key=cred["secret_access_key"],
                        aws_session_token=token if token is not None else cred["session_token"],
                        region_name=REGION, config=BOTO_CFG)


def scenario_4_sts():
    print(f"\n[4] STS session CHURN: {STS_SESSIONS} sessions minted, used, tampered with and "
          "revoked concurrently", flush=True)
    ok_bucket, other = "advsts", "advstsother"
    s3.create_bucket(Bucket=ok_bucket)
    s3.create_bucket(Bucket=other)
    payload = body_of(4096, b"sts")
    s3.put_object(Bucket=ok_bucket, Key="readme", Body=payload)
    s3.put_object(Bucket=other, Key="secret", Body=payload)
    policy = {"Version": "2012-10-17",
              "Statement": [{"Effect": "Allow", "Action": "s3:GetObject",
                             "Resource": f"arn:aws:s3:::{ok_bucket}/*"}]}

    mint_bad = []

    def mint(i):
        status, body = mint_session(policy)
        if status not in (200, 201) or not body:
            mint_bad.append(f"session {i}: HTTP {status}")
            return None
        return body

    with concurrent.futures.ThreadPoolExecutor(max_workers=STS_SESSIONS) as pool:
        minted = [m for m in pool.map(mint, range(STS_SESSIONS)) if m]
    if not check(f"[4] all {STS_SESSIONS} sessions minted concurrently "
                 f"(failures: {mint_bad[:3] or 'none'})",
                 not mint_bad and len(minted) == STS_SESSIONS):
        return
    check("[4] every minted credential is a session key with a token",
          all(m["access_key_id"].startswith("CAIRNTMP") and m["session_token"] for m in minted))

    # Every session is exercised CONCURRENTLY against the whole scope boundary at once.
    tally = {"in_scope": [], "ungranted_put": [], "cross_bucket": [], "tampered": [],
             "no_token": [], "revoked": []}
    lock = threading.Lock()
    barrier = threading.Barrier(len(minted))

    def exercise(i, cred):
        c = sess_client(cred)
        barrier.wait()
        # (a) in scope: must succeed byte-exact.
        try:
            got = c.get_object(Bucket=ok_bucket, Key="readme")["Body"].read()
            r_a = ("ok", got == payload)
        except Exception as exc:  # noqa: BLE001
            r_a = ("err", err_of(exc))
        # (b) an action the policy does not grant, in the SAME bucket.
        try:
            c.put_object(Bucket=ok_bucket, Key=f"nope-{i}", Body=b"x")
            r_b = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            r_b = ("err", err_of(exc))
        # (c) a bucket the policy never mentions.
        try:
            c.get_object(Bucket=other, Key="secret")
            r_c = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            r_c = ("err", err_of(exc))
        # (d) right key + secret, TAMPERED token.
        try:
            sess_client(cred, "not-the-real-token").get_object(Bucket=ok_bucket, Key="readme")
            r_d = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            r_d = ("err", err_of(exc))
        # (e) right key + secret, NO token at all.
        try:
            sess_client(cred, "").get_object(Bucket=ok_bucket, Key="readme")
            r_e = ("ok", None)
        except Exception as exc:  # noqa: BLE001
            r_e = ("err", err_of(exc))
        # (f) revoke it, then use it again. The launcher pins CAIRN_AUTH_CACHE_TTL_SECS=0 so this
        # is deterministic rather than a race with a cached credential.
        st, _ = mgmt("DELETE", f"/credentials/temporary/{cred['access_key_id']}")
        try:
            c.get_object(Bucket=ok_bucket, Key="readme")
            r_f = ("ok", st)
        except Exception as exc:  # noqa: BLE001
            r_f = ("err", err_of(exc))
        with lock:
            tally["in_scope"].append(r_a)
            tally["ungranted_put"].append(r_b)
            tally["cross_bucket"].append(r_c)
            tally["tampered"].append(r_d)
            tally["no_token"].append(r_e)
            tally["revoked"].append(r_f)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(minted)) as pool:
        list(pool.map(lambda p: exercise(*p), list(enumerate(minted))))

    def all_exact(name, want):
        wrong = [v for v in tally[name] if v != ("err", want)]
        return check(f"[4] {name}: every one of the {len(tally[name])} attempts refused with "
                     f"EXACTLY {want} (wrong: {wrong[:3] or 'none'})",
                     not wrong and len(tally[name]) == len(minted))

    bad_scope = [v for v in tally["in_scope"] if v != ("ok", True)]
    check(f"[4] in-scope GET succeeded byte-exact on every session "
          f"(bad: {bad_scope[:3] or 'none'})", not bad_scope and len(tally["in_scope"]) == len(minted))
    # A session never grants MORE than its scope: an ungranted action and a bucket outside the
    # policy are both AccessDenied (403), not a widened grant from the parent admin identity.
    all_exact("ungranted_put", (403, "AccessDenied"))
    all_exact("cross_bucket", (403, "AccessDenied"))
    # A tampered token fails the constant-time hash compare -> SignatureDoesNotMatch; an ABSENT
    # token is malformed credentials -> InvalidArgument. Both are exact, and deliberately distinct.
    all_exact("tampered", (403, "SignatureDoesNotMatch"))
    all_exact("no_token", (400, "InvalidArgument"))
    # A revoked session key is no longer a session key at all -> InvalidAccessKeyId.
    all_exact("revoked", (403, "InvalidAccessKeyId"))
    check(f"[4] no ungranted call ever SUCCEEDED (non-vacuity: {len(minted)} sessions × 5 refusal "
          "classes)", all(v[0] == "err" for name in
                          ("ungranted_put", "cross_bucket", "tampered", "no_token", "revoked")
                          for v in tally[name]))
    leaked = [f"nope-{i}" for i in range(len(minted)) if not absent(ok_bucket, f"nope-{i}")]
    check(f"[4] no ungranted PUT left an object behind (leaked: {leaked[:3] or 'none'})", not leaked)
    ADVISORY["sts_sessions"] = len(minted)


# ================================================================== 5. large-fanout DeleteObjects
OVERLONG = "z" * 1025  # one byte past MAX_KEY_LEN


def scenario_5_delete():
    per_batch = DEL_PRESENT + DEL_ABSENT + 4
    print(f"\n[5] LARGE-FANOUT DeleteObjects: {DEL_WORKERS} concurrent batches of {per_batch} keys "
          f"({DEL_PRESENT} present + {DEL_ABSENT} absent + 1 invalid + 3 duplicates)", flush=True)
    bucket = "advdel"
    s3.create_bucket(Bucket=bucket)
    tiny = b"d"

    # Disjoint per-worker namespaces: a worker can only ever delete its OWN keys, so the per-key
    # result split is deterministic under concurrency (and cross-worker destruction is visible).
    present = {w: [f"bulk/w{w:02d}/p-{i:05d}" for i in range(DEL_PRESENT)]
               for w in range(DEL_WORKERS)}
    dupe = {w: f"bulk/w{w:02d}/dup" for w in range(DEL_WORKERS)}
    control = [f"keep/k-{i:05d}" for i in range(DEL_CONTROL)]

    def put_key(k):
        s3.put_object(Bucket=bucket, Key=k, Body=tiny)

    to_stage = [k for ks in present.values() for k in ks] + list(dupe.values()) + control
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(put_key, to_stage))
    check(f"[5] pre-staged {len(to_stage)} keys ({DEL_WORKERS} disjoint worker namespaces + "
          f"{DEL_CONTROL} control keys)", len(to_stage) > 0)

    barrier = threading.Barrier(DEL_WORKERS)
    out = {}

    def bulk(w):
        entries = ([{"Key": k} for k in present[w]] +
                   [{"Key": f"bulk/w{w:02d}/a-{i:05d}"} for i in range(DEL_ABSENT)] +
                   [{"Key": OVERLONG}] +
                   [{"Key": dupe[w]}] * 3)
        barrier.wait()
        try:
            r = s3.delete_objects(Bucket=bucket, Delete={"Objects": entries, "Quiet": False})
            out[w] = ("ok", r, len(entries))
        except Exception as exc:  # noqa: BLE001
            out[w] = ("err", err_of(exc), len(entries))

    with concurrent.futures.ThreadPoolExecutor(max_workers=DEL_WORKERS) as pool:
        list(pool.map(bulk, range(DEL_WORKERS)))

    failed = [f"w{w}: {v[1]}" for w, v in out.items() if v[0] != "ok"]
    if not check(f"[5] every concurrent bulk delete returned 200 (failures: {failed[:3] or 'none'})",
                 not failed):
        return

    split_bad, err_bad, dup_bad = [], [], []
    for w, (_, r, n) in out.items():
        deleted = r.get("Deleted", [])
        errors = r.get("Errors", [])
        # Exactly one outcome per request entry, and the split is exact: everything except the one
        # structurally invalid key succeeds (S3's delete is idempotent, so an ABSENT key is a
        # SUCCESS, not an error), and a duplicated key is reported once per entry.
        if len(deleted) + len(errors) != n:
            split_bad.append(f"w{w}: {len(deleted)}+{len(errors)} != {n}")
        if len(errors) != 1 or errors[0].get("Key") != OVERLONG or \
                errors[0].get("Code") != "InvalidArgument":
            err_bad.append(f"w{w}: {[(e.get('Code'), e.get('Key', '')[:8]) for e in errors]}")
        if len(deleted) != n - 1:
            split_bad.append(f"w{w}: {len(deleted)} deleted, want {n - 1}")
        if sum(1 for d in deleted if d.get("Key") == dupe[w]) != 3:
            dup_bad.append(f"w{w}: duplicate key reported "
                           f"{sum(1 for d in deleted if d.get('Key') == dupe[w])}×, want 3")
    check(f"[5] every request entry produced EXACTLY one outcome and the present/absent split is "
          f"exact (bad: {split_bad[:3] or 'none'})", not split_bad)
    check(f"[5] the one structurally invalid key is the ONLY error, coded EXACTLY InvalidArgument "
          f"(bad: {err_bad[:3] or 'none'})", not err_bad)
    check(f"[5] a key duplicated 3× is reported 3× and never errors "
          f"(bad: {dup_bad[:3] or 'none'})", not dup_bad)

    survivors = []
    token, pages = None, 0
    while True:
        kw = {"Bucket": bucket, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        survivors.extend(o["Key"] for o in page.get("Contents", []))
        pages += 1
        token = page.get("NextContinuationToken")
        if not page.get("IsTruncated") or not token or pages > 50:
            break
    check(f"[5] every targeted key is gone ({len([k for k in survivors if k.startswith('bulk/')])} "
          "bulk keys survived, want 0)",
          not [k for k in survivors if k.startswith("bulk/")])
    # The whole point of the control namespace: a large concurrent fan-out must not touch a key it
    # was never asked about.
    check(f"[5] the control namespace is intact — no unrelated key destroyed "
          f"({len([k for k in survivors if k.startswith('keep/')])}/{DEL_CONTROL})",
          sorted(k for k in survivors if k.startswith("keep/")) == sorted(control))
    sample_bad = []
    for k in control[:8]:
        try:
            if s3.get_object(Bucket=bucket, Key=k)["Body"].read() != tiny:
                sample_bad.append(k)
        except Exception as exc:  # noqa: BLE001 - any read failure is a control-key failure
            sample_bad.append(f"{k}: {err_of(exc)}")
    check(f"[5] sampled control objects still read back byte-exact "
          f"(bad: {sample_bad[:3] or 'none'})", not sample_bad)
    ADVISORY.update({"del_workers": DEL_WORKERS, "del_keys_per_batch": per_batch,
                     "del_keys_total": DEL_WORKERS * per_batch})


def main():
    t0 = time.time()
    HEALTH.start()
    # Each scenario is isolated: an UNEXPECTED exception (a setup call raising, a GET the harness
    # did not guard) is recorded as a failure and the run continues to the JSON write. Letting one
    # escape would abort before ADVISORY is written, so the launcher would read declared_5xx=0 and
    # stack a spurious `unexpected_http_5xx` gate failure on top of the real one — burying the
    # actual cause under a second, misleading verdict (the stress_multipart.py lesson).
    try:
        for name, fn in (("1 chunked-decoder", scenario_1_chunked),
                         ("2 quota + size ceiling", scenario_2_quota),
                         ("3 deep pagination", scenario_3_pagination),
                         ("4 STS churn", scenario_4_sts),
                         ("5 DeleteObjects fan-out", scenario_5_delete)):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                check(f"[!] scenario {name} raised {type(exc).__name__}: {exc}", False)
    finally:
        HEALTH.stop.set()
        HEALTH.join(timeout=5)
    check(f"[*] /healthz answered every one of its {HEALTH.probes} probes for the whole run "
          f"({HEALTH.failures} unanswered within {HEALTHZ_TIMEOUT}s, worst {HEALTH.worst_ms:.0f} ms)",
          HEALTH.failures == 0 and HEALTH.probes > 0)
    ADVISORY.update({
        "driver_secs": round(time.time() - t0, 1),
        "declared_5xx": DECLARED_5XX[0],
        "findings": findings,
        "failures": fails,
        "healthz_probes": HEALTH.probes,
        "healthz_worst_ms": round(HEALTH.worst_ms, 1),
    })
    if findings:
        print(f"\n  FINDINGS pinned (reported, not gated, {len(findings)}):")
        for f in findings:
            print("    * " + f)
    if OUT_JSON:
        with open(OUT_JSON, "w", encoding="utf-8") as fh:
            json.dump(ADVISORY, fh)
    if fails:
        print(f"\n  adversarial stress FAILED ({len(fails)}): " + "; ".join(fails[:6]))
        return 1
    print(f"\n  adversarial stress PASSED (all 5 scenarios) in {ADVISORY['driver_secs']}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
