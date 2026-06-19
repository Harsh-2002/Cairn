#!/usr/bin/env python3
"""Multi-point durability crash regression (ARCH 8, F-4). crash_consistency.sh proves the property
at ONE fault seam (a plain PUT). This drives it at every blob-commit seam the build exposes, so the
"crash in the durability window leaves a reclaimable orphan, never a half-committed object" contract
is verified for each write PATH:

  blob_after_durable   -> a plain PutObject, crashed after the blob is durable, before metadata commit
  blob_after_assemble  -> a multipart CompleteMultipartUpload, crashed after the assembled blob is
                          durable, before metadata commit

For each: arm the seam, run the op (the in-flight task panics; tokio isolates it so the process
survives), stop, run `cairn integrity`, and assert it reclaimed >= 1 orphan and the object is absent.

Requires a binary built with --features failpoints. Config via env: BIN, DATA, PORT.
"""
import datetime, hashlib, hmac, http.client, os, signal, subprocess, sys, time, urllib.parse

BIN = os.environ.get("BIN", "target/debug/cairn")
ROOT = os.environ["DATA"]
PORT = int(os.environ.get("PORT", "9089"))
REGION = "us-east-1"
HOST = ("127.0.0.1", PORT)
BUCKET = "crashbkt"

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)
def note(m): print(f"  {m}", flush=True)

# ---------- SigV4 ----------
def _sha(b): return hashlib.sha256(b).hexdigest()
def _hmac(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
def sigv4(method, path, query, headers, body, ak, sk):
    host = f"{HOST[0]}:{HOST[1]}"
    now = datetime.datetime.now(datetime.timezone.utc)
    amz = now.strftime("%Y%m%dT%H%M%SZ"); day = now.strftime("%Y%m%d"); ph = _sha(body)
    h = {k.lower(): v for k, v in headers.items()}
    h["host"] = host; h["x-amz-date"] = amz; h["x-amz-content-sha256"] = ph
    cq = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(query.items()))
    cu = urllib.parse.quote(path, safe="/"); signed = sorted(h.keys())
    ch = "".join(f"{k}:{h[k].strip()}\n" for k in signed); sh = ";".join(signed)
    cr = "\n".join([method, cu, cq, ch, sh, ph]); scope = f"{day}/{REGION}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, _sha(cr.encode())])
    kd = _hmac(("AWS4" + sk).encode(), day); kr = hmac.new(kd, REGION.encode(), hashlib.sha256).digest()
    ks = hmac.new(kr, b"s3", hashlib.sha256).digest(); ksig = hmac.new(ks, b"aws4_request", hashlib.sha256).digest()
    sig = hmac.new(ksig, sts.encode(), hashlib.sha256).hexdigest()
    h["authorization"] = f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={sh}, Signature={sig}"
    return h
def s3(method, path, query=None, headers=None, body=b"", ak=None, sk=None):
    query = query or {}; headers = headers or {}
    if isinstance(body, str): body = body.encode()
    h = sigv4(method, path, query, headers, body, ak, sk)
    qs = "&".join(f"{urllib.parse.quote(k,safe='')}={urllib.parse.quote(v,safe='')}" for k, v in query.items())
    try:
        c = http.client.HTTPConnection(*HOST, timeout=15)
        c.request(method, path + ("?" + qs if qs else ""), body=body, headers=h)
        r = c.getresponse(); data = r.read(); et = r.getheader("etag"); c.close()
        return r.status, data, et
    except Exception:
        return 0, b"", None  # the seam panicked the task -> connection reset (this is expected)

# ---------- node lifecycle ----------
def env(failpoints=None):
    e = dict(os.environ)
    for k in list(e):
        if k.startswith("CAIRN_") or k == "FAILPOINTS": del e[k]
    e.update({
        "CAIRN_DATA_DIR": os.path.join(ROOT, "data"), "CAIRN_DB_PATH": os.path.join(ROOT, "data/cairn.db"),
        "CAIRN_LISTEN_ADDR": f"127.0.0.1:{PORT}", "CAIRN_UI_ADDR": "off",
        "CAIRN_MASTER_KEY": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
        "CAIRN_LOG_LEVEL": os.environ.get("CAIRN_LOG_LEVEL", "error"),
    })
    if failpoints: e["FAILPOINTS"] = failpoints
    return e
def bootstrap():
    out = subprocess.run([BIN, "bootstrap"], env=env(), capture_output=True, text=True)
    ak = sk = None
    for line in out.stdout.splitlines():
        if "Access Key Id" in line: ak = line.split()[-1]
        if "Secret Access Key" in line: sk = line.split()[-1]
    if not ak or not sk: raise RuntimeError(f"bootstrap parse failed: {out.stdout}\n{out.stderr}")
    return ak, sk
PROC = None
def serve(failpoints=None, tag="srv"):
    global PROC
    log = open(os.path.join(ROOT, f"{tag}.log"), "w")
    PROC = subprocess.Popen([BIN, "serve"], env=env(failpoints), stdout=log, stderr=subprocess.STDOUT)
    for _ in range(150):
        if PROC.poll() is not None: return False
        try:
            c = http.client.HTTPConnection(*HOST, timeout=2); c.request("GET", "/healthz")
            r = c.getresponse(); r.read(); c.close()
            if r.status == 200: time.sleep(0.3); return True
        except Exception: time.sleep(0.2)
    return False
def stop():
    global PROC
    if PROC and PROC.poll() is None:
        PROC.send_signal(signal.SIGINT)
        try: PROC.wait(timeout=10)
        except subprocess.TimeoutExpired: PROC.kill(); PROC.wait()
    PROC = None
    time.sleep(0.4)
def integrity():
    out = subprocess.run([BIN, "integrity"], env=env(), capture_output=True, text=True)
    txt = out.stdout + out.stderr
    reclaimed = None
    for tok in txt.split():
        if tok.startswith("orphans_reclaimed="):
            reclaimed = int(tok.split("=")[1])
    return reclaimed, txt.strip()

# ---------- the operations that crash mid-commit ----------
def op_put(ak, sk, bucket, key):
    return s3("PUT", f"/{bucket}/{key}", headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"},
              body=b"crash-me" * 64, ak=ak, sk=sk)[0]
def op_multipart(ak, sk, bucket, key):
    st, body, _ = s3("POST", f"/{bucket}/{key}", query={"uploads": ""}, ak=ak, sk=sk)
    if b"<UploadId>" not in body: return -1  # initiate failed
    uid = body.split(b"<UploadId>")[1].split(b"</UploadId>")[0].decode()
    st, _, etag = s3("PUT", f"/{bucket}/{key}", query={"partNumber": "1", "uploadId": uid},
                     headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, body=b"part-data" * 1024, ak=ak, sk=sk)
    xml = (f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part>"
           f"</CompleteMultipartUpload>")
    # The Complete assembles the parts into the final blob; the seam panics after it is durable.
    return s3("POST", f"/{bucket}/{key}", query={"uploadId": uid},
              headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}, body=xml, ak=ak, sk=sk)[0]

SEAMS = [
    ("blob_after_durable", "PutObject", op_put, "doomed-put"),
    ("blob_after_assemble", "CompleteMultipartUpload", op_multipart, "doomed-mpu"),
]

print(f"=== multi-point durability crash regression (BIN={BIN}) ===", flush=True)
for seam, opname, op, key in SEAMS:
    print(f"\n== seam {seam} via {opname} ==", flush=True)
    os.system(f"rm -rf {ROOT}/data && mkdir -p {ROOT}/data")
    ak, sk = bootstrap()
    if not serve(failpoints=f"{seam}=panic", tag=f"{seam}_armed"):
        check(f"[{seam}] server boots with the seam armed", False, "did not start"); continue
    s3("PUT", f"/{BUCKET}", headers={"x-amz-content-sha256": _sha(b"")}, ak=ak, sk=sk)
    status = op(ak, sk, BUCKET, key)
    note(f"{opname} returned status {status} (0 = connection reset by the panicking task)")
    check(f"[{seam}] the in-flight op did NOT cleanly commit (the seam fired)", status != 200, f"status={status}")
    check(f"[{seam}] the server process survived the task panic", PROC and PROC.poll() is None)
    stop()
    reclaimed, report = integrity()
    note(f"integrity: {report}")
    check(f"[{seam}] reconcile reclaimed >= 1 orphan blob", reclaimed is not None and reclaimed >= 1, f"orphans_reclaimed={reclaimed}")
    if serve(tag=f"{seam}_check"):
        st, _, _ = s3("GET", f"/{BUCKET}/{key}", headers={"x-amz-content-sha256": _sha(b"")}, ak=ak, sk=sk)
        check(f"[{seam}] the half-committed object is ABSENT after reconcile (no torn object)", st != 200, f"GET status={st}")
    stop()

print(f"\n== RESULT: {len(PASS)} passed, {len(FAIL)} failed ==", flush=True)
if FAIL:
    print("FAILED:", ", ".join(FAIL)); sys.exit(1)
print("ALL MULTI-POINT CRASH CHECKS PASSED")
