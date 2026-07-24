#!/usr/bin/env python3
"""Blob-store limit regression (ARCH 9). Pushes the storage layer to its edges and asserts it
degrades safely rather than corrupting or wedging:

  A  out of space  : a size-constrained filesystem fills mid-PUT -> 507 InsufficientStorage, the
                     server stays healthy, the failed object is absent (no orphan), and a write that
                     fits still succeeds (the OOM path cleaned up its staging artifact)
  B  huge object   : a large object round-trips byte-identical (multi-block write/read path)
  C  many objects  : thousands of tiny objects PUT concurrently, then listed across pages -> the
                     full set is returned (pagination correctness at volume)

Part A mounts a small tmpfs (needs sudo). Config via env: BIN, DATA, PORT, TMPFS_MB, HUGE_MB, MANY.
"""
import concurrent.futures, datetime, hashlib, hmac, http.client, os, signal, subprocess, sys, time, urllib.parse

BIN = os.environ.get("BIN", "target/debug/cairn")
ROOT = os.environ["DATA"]
PORT = int(os.environ.get("PORT", "9090"))
REGION = "us-east-1"
HOST = ("127.0.0.1", PORT)
TMPFS_MB = int(os.environ.get("TMPFS_MB", "48"))
HUGE_MB = int(os.environ.get("HUGE_MB", "128"))
MANY = int(os.environ.get("MANY", "1100"))   # > 1000 so listing pages

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)
def note(m): print(f"  {m}", flush=True)

# ---------- SigV4 ----------
def _sha(b): return hashlib.sha256(b).hexdigest()
def _hmac(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
def sigv4(method, path, query, headers, payload_hash, ak, sk):
    host = f"{HOST[0]}:{HOST[1]}"
    now = datetime.datetime.now(datetime.timezone.utc)
    amz = now.strftime("%Y%m%dT%H%M%SZ"); day = now.strftime("%Y%m%d")
    h = {k.lower(): v for k, v in headers.items()}
    h["host"] = host; h["x-amz-date"] = amz; h["x-amz-content-sha256"] = payload_hash
    cq = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(query.items()))
    cu = urllib.parse.quote(path, safe="/"); signed = sorted(h.keys())
    ch = "".join(f"{k}:{h[k].strip()}\n" for k in signed); sh = ";".join(signed)
    cr = "\n".join([method, cu, cq, ch, sh, payload_hash]); scope = f"{day}/{REGION}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, _sha(cr.encode())])
    kd = _hmac(("AWS4" + sk).encode(), day); kr = hmac.new(kd, REGION.encode(), hashlib.sha256).digest()
    ks = hmac.new(kr, b"s3", hashlib.sha256).digest(); ksig = hmac.new(ks, b"aws4_request", hashlib.sha256).digest()
    sig = hmac.new(ksig, sts.encode(), hashlib.sha256).hexdigest()
    h["authorization"] = f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={sh}, Signature={sig}"
    return h
AK = SK = None
def s3(method, path, query=None, headers=None, body=b"", unsigned=False):
    query = query or {}; headers = dict(headers or {})
    ph = "UNSIGNED-PAYLOAD" if unsigned else _sha(body)
    h = sigv4(method, path, query, headers, ph, AK, SK)
    qs = "&".join(f"{urllib.parse.quote(k,safe='')}={urllib.parse.quote(v,safe='')}" for k, v in query.items())
    try:
        c = http.client.HTTPConnection(*HOST, timeout=120)
        c.request(method, path + ("?" + qs if qs else ""), body=body, headers=h)
        r = c.getresponse(); data = r.read(); et = r.getheader("etag"); c.close()
        return r.status, data, et
    except Exception as e:
        return 0, str(e).encode(), None

# ---------- node lifecycle ----------
def env(data):
    e = dict(os.environ)
    for k in list(e):
        if k.startswith("CAIRN_"): del e[k]
    e.update({
        "CAIRN_DATA_DIR": data, "CAIRN_DB_PATH": os.path.join(data, "cairn.db"),
        "CAIRN_LISTEN_ADDR": f"127.0.0.1:{PORT}", "CAIRN_WEB_ADDR": "off",
        "CAIRN_MASTER_KEY": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
        "CAIRN_LOG_LEVEL": os.environ.get("CAIRN_LOG_LEVEL", "error"),
    })
    return e
def bootstrap(data):
    global AK, SK
    out = subprocess.run([BIN, "bootstrap"], env=env(data), capture_output=True, text=True)
    for line in out.stdout.splitlines():
        if "Access Key Id" in line: AK = line.split()[-1]
        if "Secret Access Key" in line: SK = line.split()[-1]
    if not AK or not SK: raise RuntimeError(f"bootstrap parse failed: {out.stdout}\n{out.stderr}")
PROC = None
def serve(data, tag):
    global PROC
    log = open(os.path.join(ROOT, f"{tag}.log"), "w")
    PROC = subprocess.Popen([BIN, "serve"], env=env(data), stdout=log, stderr=subprocess.STDOUT)
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
        try: PROC.wait(timeout=15)
        except subprocess.TimeoutExpired: PROC.kill(); PROC.wait()
    PROC = None; time.sleep(0.4)
def healthy():
    try:
        c = http.client.HTTPConnection(*HOST, timeout=2); c.request("GET", "/healthz")
        r = c.getresponse(); r.read(); c.close(); return r.status == 200
    except Exception: return False

print(f"=== blob-store limit regression (BIN={BIN}) ===", flush=True)
TMPFS = os.path.join(ROOT, "tmpfs")
NORMAL = os.path.join(ROOT, "normal")
mounted = False
try:
    # ---------- A: out of space ----------
    print(f"\n== A: out of space — {TMPFS_MB} MiB filesystem fills mid-PUT ==", flush=True)
    os.makedirs(TMPFS, exist_ok=True)
    rc = subprocess.run(["sudo", "mount", "-t", "tmpfs", "-o", f"size={TMPFS_MB}m", "tmpfs", TMPFS]).returncode
    check("[A] mounted a size-constrained tmpfs", rc == 0)
    if rc == 0:
        mounted = True
        subprocess.run(["sudo", "chmod", "777", TMPFS])
        bootstrap(TMPFS)
        check("[A] server boots on the constrained filesystem", serve(TMPFS, "a"))
        s3("PUT", "/blobs")
        # Fill the filesystem to ~zero free with a stray file, so even a TINY write cannot be staged.
        # A small body is fully sent before the server fails, yielding a clean 507; a body larger than
        # the disk would instead reset the connection mid-upload (the server responds + closes early).
        subprocess.run(f"dd if=/dev/zero of={TMPFS}/.filler bs=1M 2>/dev/null || true", shell=True)
        oversize = os.urandom(256 * 1024)  # 256 KiB
        st, _, _ = s3("PUT", "/blobs/oversize.bin", body=oversize, unsigned=True)
        check("[A] an over-capacity write fails 507 InsufficientStorage", st == 507, f"status={st}")
        if os.path.exists(f"{TMPFS}/.filler"): os.remove(f"{TMPFS}/.filler")  # free the space back up
        check("[A] server stays healthy after the out-of-space error", healthy())
        st, _, _ = s3("GET", "/blobs/oversize.bin")
        check("[A] the failed object is absent (no torn/partial object)", st == 404, f"GET status={st}")
        small = b"fits-fine" * 1024
        st, _, _ = s3("PUT", "/blobs/small.bin", body=small)
        check("[A] a write that fits still succeeds (OOM staging was cleaned up)", st in (200, 204), f"status={st}")
        st, body, _ = s3("GET", "/blobs/small.bin")
        check("[A] that object reads back byte-identical", st == 200 and body == small, f"status={st}")
        stop()
        subprocess.run(["sudo", "umount", TMPFS]); mounted = False

    # ---------- B + C: huge object + many objects ----------
    os.makedirs(NORMAL, exist_ok=True)
    bootstrap(NORMAL)
    if not serve(NORMAL, "bc"):
        check("[B/C] server boots", False); raise SystemExit
    s3("PUT", "/blobs")

    print(f"\n== B: huge object — {HUGE_MB} MiB round-trip byte-identical ==", flush=True)
    chunk = os.urandom(1 << 20)
    huge = chunk * HUGE_MB
    md5 = hashlib.md5(huge).hexdigest()
    st, _, etag = s3("PUT", "/blobs/huge.bin", body=huge, unsigned=True)
    check(f"[B] PUT of a {HUGE_MB} MiB object succeeds", st in (200, 204), f"status={st}")
    st, body, _ = s3("GET", "/blobs/huge.bin")
    check("[B] GET returns the exact byte length", st == 200 and len(body) == len(huge), f"status={st} len={len(body)}")
    check("[B] the huge object round-trips byte-identical (md5)", hashlib.md5(body).hexdigest() == md5, "md5 mismatch")

    print(f"\n== C: many objects — {MANY} tiny objects, listed across pages ==", flush=True)
    def put_one(i):
        return s3("PUT", f"/blobs/many/k{i:05d}", body=f"v{i}".encode())[0]
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as ex:
        codes = list(ex.map(put_one, range(MANY)))
    ok = sum(1 for c in codes if c in (200, 204))
    check(f"[C] all {MANY} tiny objects written", ok == MANY, f"{ok}/{MANY} in {time.time()-t0:.1f}s")
    # paginate ListObjectsV2 over the prefix and count keys
    seen = 0; token = None
    for _ in range(20):
        q = {"list-type": "2", "prefix": "many/", "max-keys": "1000"}
        if token: q["continuation-token"] = token
        st, body, _ = s3("GET", "/blobs", query=q)
        if st != 200: break
        seen += body.count(b"<Key>")
        if b"<IsTruncated>true</IsTruncated>" in body and b"<NextContinuationToken>" in body:
            token = body.split(b"<NextContinuationToken>")[1].split(b"</NextContinuationToken>")[0].decode()
        else:
            break
    check(f"[C] listing returns the full set across pages ({MANY})", seen == MANY, f"listed={seen}")
    stop()
finally:
    if PROC and PROC.poll() is None: stop()
    if mounted: subprocess.run(["sudo", "umount", TMPFS], stderr=subprocess.DEVNULL)

print(f"\n== RESULT: {len(PASS)} passed, {len(FAIL)} failed ==", flush=True)
if FAIL:
    print("FAILED:", ", ".join(FAIL)); sys.exit(1)
print("ALL BLOB LIMIT CHECKS PASSED")
