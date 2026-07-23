#!/usr/bin/env python3
"""End-to-end gate for master-key rotation (audit #29), driven against a real cairn binary.

Walks the full key lifecycle under sharding (CAIRN_META_SHARDS), restarting the server with
different CAIRN_MASTER_KEY_RING configs and inspecting envelope key-id bytes directly in every
shard DB:

  P1 single key K1            -> SSE DEKs + user secrets sealed under id=1
  P2a ring [1,2] active=2     -> with re-wrap OFF, the old key must NOT be retire-eligible
  P2b re-wrap ON              -> every stream re-wraps to id=2; old key becomes retire-eligible
  P3 retire id=1 (ring [2])   -> all data still opens (safe retire after re-wrap)
  P4 retire id=1 before rewrap -> startup retire-gate REFUSES to boot (fail-closed, no leak)
  P5 seal counter             -> survives a restart (primed from durable state)

All THREE encrypted modes ride the whole lifecycle (the server boots every phase with
CAIRN_ENCRYPT_AT_REST + CAIRN_KMS_KEY_IDS): explicit SSE-S3 (AES256), transparent at-rest
(mode "at-rest"), and SSE-KMS (aws:kms + key id). Since all three seal their DEK under the same
master ring, re-wrap and the retire-gate must treat them identically — and the additive descriptor
labels (mode, kms_key_id) MUST survive a re-wrap on disk (a dropped `mode` would silently downgrade
an at-rest object to advertising AES256). P2b/P3 assert the on-disk descriptor is resealed to the
new key with those labels intact and that GET/HEAD still advertise the right per-mode headers; P4
proves the retire-gate protects the at-rest and KMS objects, not only SSE-S3.

Config via env: BIN (cairn binary), DATA (temp dir), PORT (S3 port; UI = PORT+1), SHARDS.
"""
import base64, datetime, hashlib, hmac, http.client, json, os, signal, sqlite3, subprocess, sys, time, urllib.parse

BIN = os.environ.get("BIN", "target/debug/cairn")
ROOT = os.environ["DATA"]
PORT = int(os.environ.get("PORT", "9079"))
UIPORT = PORT + 1
SHARDS = int(os.environ.get("SHARDS", "4"))
REGION = "us-east-1"
AK, SK = "cairn", "cairnadmin"
S3 = ("127.0.0.1", PORT)
MGMT = ("127.0.0.1", UIPORT)
RDATA = os.path.join(ROOT, "rdata")
K1 = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
K2 = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
RING_12 = json.dumps([{"id": 1, "key": K1}, {"id": 2, "key": K2}])
RING_2 = json.dumps([{"id": 2, "key": K2}])
KMSID = "tenant-rot"  # the SSE-KMS key id (label-only; sealed under the master ring like any DEK)

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)

# ---------- SigV4 ----------
def _sha(b): return hashlib.sha256(b).hexdigest()
def _hmac(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
def sigv4(method, path, query, headers, body, ak, sk, service="s3"):
    host = f"{S3[0]}:{S3[1]}"
    now = datetime.datetime.now(datetime.timezone.utc)
    amz = now.strftime("%Y%m%dT%H%M%SZ"); day = now.strftime("%Y%m%d")
    ph = _sha(body)
    h = {k.lower(): v for k, v in headers.items()}
    h["host"] = host; h["x-amz-date"] = amz; h["x-amz-content-sha256"] = ph
    cq = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(query.items()))
    cu = urllib.parse.quote(path, safe="/")
    signed = sorted(h.keys())
    ch = "".join(f"{k}:{h[k].strip()}\n" for k in signed)
    sh = ";".join(signed)
    cr = "\n".join([method, cu, cq, ch, sh, ph])
    scope = f"{day}/{REGION}/{service}/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, _sha(cr.encode())])
    kd = _hmac(("AWS4" + sk).encode(), day)
    kr = hmac.new(kd, REGION.encode(), hashlib.sha256).digest()
    ks = hmac.new(kr, service.encode(), hashlib.sha256).digest()
    ksig = hmac.new(ks, b"aws4_request", hashlib.sha256).digest()
    sig = hmac.new(ksig, sts.encode(), hashlib.sha256).hexdigest()
    h["authorization"] = f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={sh}, Signature={sig}"
    return h
def s3(method, path, query=None, headers=None, body=b""):
    query = query or {}; headers = headers or {}
    if isinstance(body, str): body = body.encode()
    h = sigv4(method, path, query, headers, body, AK, SK)
    qs = "&".join(f"{urllib.parse.quote(k,safe='')}={urllib.parse.quote(v,safe='')}" for k, v in query.items())
    c = http.client.HTTPConnection(*S3, timeout=30)
    c.request(method, path + ("?" + qs if qs else ""), body=body, headers=h)
    r = c.getresponse(); d = r.read(); c.close()
    return r.status, d
def put_sse(bucket, key, body):
    return s3("PUT", f"/{bucket}/{key}", headers={"x-amz-server-side-encryption": "AES256"}, body=body)
def put_kms(bucket, key, body, key_id=KMSID):
    return s3("PUT", f"/{bucket}/{key}",
              headers={"x-amz-server-side-encryption": "aws:kms",
                       "x-amz-server-side-encryption-aws-kms-key-id": key_id}, body=body)
def put_plain(bucket, key, body):
    # No SSE header: under CAIRN_ENCRYPT_AT_REST=true this is stored as transparent at-rest (mode
    # "at-rest", advertises nothing on read).
    return s3("PUT", f"/{bucket}/{key}", body=body)
def head(bucket, key):
    h = sigv4("HEAD", f"/{bucket}/{key}", {}, {}, b"", AK, SK)
    c = http.client.HTTPConnection(*S3, timeout=30)
    c.request("HEAD", f"/{bucket}/{key}", headers=h)
    r = c.getresponse(); r.read(); hs = {k.lower(): v for k, v in r.getheaders()}; c.close()
    return r.status, hs
def mkuser(name):
    c = http.client.HTTPConnection(*MGMT, timeout=30)
    c.request("POST", "/api/v1/users", body=json.dumps({"display_name": name, "role": "member"}).encode(),
              headers={"authorization": f"Bearer {AK}.{SK}", "content-type": "application/json"})
    r = c.getresponse(); ok = r.status in (200, 201); r.read(); c.close(); return ok
def crypto_status():
    c = http.client.HTTPConnection(*MGMT, timeout=30)
    c.request("GET", "/api/v1/system/crypto-status", headers={"authorization": f"Bearer {AK}.{SK}"})
    r = c.getresponse(); d = r.read(); c.close()
    return (r.status, json.loads(d) if d else {})

# ---------- server lifecycle ----------
def base_env(extra):
    e = dict(os.environ)
    for k in ("CAIRN_MASTER_KEY", "CAIRN_MASTER_KEY_RING", "CAIRN_MASTER_KEY_ACTIVE_ID"):
        e.pop(k, None)
    e.update({
        "CAIRN_LISTEN_ADDR": f"127.0.0.1:{PORT}", "CAIRN_UI_ADDR": f"127.0.0.1:{UIPORT}",
        "CAIRN_DATA_DIR": RDATA, "CAIRN_DB_PATH": os.path.join(RDATA, "cairn.db"),
        "CAIRN_REGION": REGION, "CAIRN_ROOT_ACCESS_KEY": AK, "CAIRN_ROOT_SECRET_KEY": SK,
        "CAIRN_META_SHARDS": str(SHARDS), "CAIRN_META_BACKEND": "sqlite",
        # Sharding provisions the per-connection page cache PER SHARD, so the total footprint is
        # (read_pool+1) x shards x cache_bytes_per_conn. With SHARDS=4 and the 64 MiB default per-conn
        # cache that exceeds the 2 GiB default budget on a many-core runner, so size the budget up for
        # the sharded test (this harness exercises key rotation, not cache sizing).
        "CAIRN_META_CACHE_TOTAL_BUDGET_BYTES": str(8 * 1024 ** 3),
        "CAIRN_LOG_LEVEL": os.environ.get("CAIRN_LOG_LEVEL", "warn"),
        "CAIRN_KEY_REWRAP_INTERVAL_SECS": "0", "CAIRN_KEY_COUNTER_SYNC_SECS": "0",
        # Enable the two encrypted modes the rotation gate must also cover, on EVERY phase's boot:
        # transparent at-rest (a plain PUT becomes a `mode:"at-rest"` object) and SSE-KMS (the
        # `aws:kms` allow-list). Both seal their DEK under the same master ring, so re-wrap and the
        # retire-gate treat them exactly like SSE-S3 — this harness proves that end to end.
        "CAIRN_ENCRYPT_AT_REST": "true", "CAIRN_KMS_KEY_IDS": KMSID,
    })
    e.update(extra); return e
def _port_free():
    try:
        c = http.client.HTTPConnection(*S3, timeout=1); c.request("GET", "/healthz"); c.getresponse().read(); c.close()
        return False
    except Exception:
        return True
PROC = None
def start(env, tag, expect_ok=True):
    global PROC
    for _ in range(40):
        if _port_free(): break
        time.sleep(0.3)
    log = open(os.path.join(ROOT, f"{tag}.log"), "w")
    PROC = subprocess.Popen([BIN, "serve"], env=env, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(150):
        if PROC.poll() is not None:
            return False
        try:
            c = http.client.HTTPConnection(*S3, timeout=2); c.request("GET", "/healthz"); r = c.getresponse(); r.read(); c.close()
            if r.status in (200, 204, 404, 403):
                time.sleep(0.4); return True
        except Exception:
            time.sleep(0.4)
    return False
def stop():
    global PROC
    if PROC and PROC.poll() is None:
        PROC.send_signal(signal.SIGINT)
        try: PROC.wait(timeout=10)
        except subprocess.TimeoutExpired: PROC.kill(); PROC.wait()
    PROC = None
    for _ in range(40):
        if _port_free(): break
        time.sleep(0.3)

# ---------- envelope inspection across shards ----------
def shards():
    out = [os.path.join(RDATA, "cairn.db")] + [os.path.join(RDATA, f"cairn.db.shard{i}") for i in range(1, SHARDS)]
    return [p for p in out if os.path.exists(p)]
def kid(blob):
    return None if (blob is None or len(blob) < 6 or blob[:4] != b"CRK1") else int.from_bytes(blob[4:6], "big")
def sse_ids():
    out = []
    for p in shards():
        con = sqlite3.connect(p)
        for (d,) in con.execute("SELECT sse_descriptor FROM object_versions WHERE sse_descriptor IS NOT NULL"):
            j = json.loads(d); out.append(kid(base64.b64decode(j["wrapped_dek_b64"])))
        con.close()
    return out
def user_ids():
    out = []
    for p in shards():
        con = sqlite3.connect(p)
        for (ct,) in con.execute("SELECT sigv4_secret_ciphertext FROM users WHERE sigv4_secret_ciphertext IS NOT NULL"):
            out.append(kid(ct))
        con.close()
    return out
def descriptor_for(objkey):
    """The on-disk sse_descriptor of the object with this key: {mode, kms_key_id, kid}. `mode` is
    None for SSE-S3 (it is the default and serializes away), "at-rest" / "kms" otherwise. `kid` is
    the master-key id the wrapped DEK is sealed under (what re-wrap advances)."""
    for p in shards():
        con = sqlite3.connect(p)
        rows = con.execute("SELECT sse_descriptor FROM object_versions WHERE key=? AND sse_descriptor IS NOT NULL",
                           (objkey,)).fetchall()
        con.close()
        if rows:
            j = json.loads(rows[0][0])
            return {"mode": j.get("mode"), "kms_key_id": j.get("kms_key_id"),
                    "kid": kid(base64.b64decode(j["wrapped_dek_b64"]))}
    return None

# =====================================================================
print(f"=== audit #29 rotation e2e (BIN={BIN} SHARDS={SHARDS} PORT={PORT}) ===", flush=True)
os.system(f"rm -rf {RDATA} && mkdir -p {RDATA}")

print("\n== P1: single key K1 (zero rotation config) ==", flush=True)
ok = start(base_env({"CAIRN_MASTER_KEY": K1}), "p1")
check("[P1] boots with a single CAIRN_MASTER_KEY (no ring)", ok)
if ok:
    st, _ = s3("PUT", "/abkt"); check("[P1] create bucket", st in (200, 204), str(st))
    st, _ = put_sse("abkt", "s1.txt", b"payload-one"); check("[P1] PUT SSE object", st in (200, 204), str(st))
    st, body = s3("GET", "/abkt/s1.txt"); check("[P1] GET SSE object decrypts", st == 200 and body == b"payload-one", f"{st} {body!r}")
    check("[P1] create user (seals a SigV4 secret)", mkuser("alice"))
    s3("PUT", "/zbkt"); put_sse("zbkt", "s2.txt", b"payload-two")
    # Two more encrypted modes whose descriptors carry ADDITIVE labels (mode, kms_key_id) that a
    # re-wrap must preserve. A dropped `mode` would make the at-rest object silently start
    # advertising AES256; a dropped kms_key_id would orphan the KMS label.
    st, _ = put_plain("abkt", "atr.txt", b"at-rest-payload")
    check("[P1] PUT transparent at-rest object", st in (200, 204), str(st))
    st, hh = head("abkt", "atr.txt")
    check("[P1] at-rest object advertises NO SSE header (transparent)",
          st == 200 and "x-amz-server-side-encryption" not in hh, str(hh))
    st, _ = put_kms("zbkt", "kms.txt", b"kms-payload")
    check("[P1] PUT SSE-KMS object (aws:kms + key id)", st in (200, 204), str(st))
    st, hh = head("zbkt", "kms.txt")
    check("[P1] KMS object advertises aws:kms + the key id",
          st == 200 and hh.get("x-amz-server-side-encryption") == "aws:kms"
          and hh.get("x-amz-server-side-encryption-aws-kms-key-id") == KMSID, str(hh))
    da, dk = descriptor_for("atr.txt"), descriptor_for("kms.txt")
    check("[P1] at-rest descriptor: mode=at-rest, sealed id=1", da and da["mode"] == "at-rest" and da["kid"] == 1, str(da))
    check("[P1] KMS descriptor: mode=kms, kms_key_id preserved, sealed id=1",
          dk and dk["mode"] == "kms" and dk["kms_key_id"] == KMSID and dk["kid"] == 1, str(dk))
    sse = sse_ids(); usr = user_ids()
    check("[P1] every SSE DEK sealed under id=1", len(sse) >= 2 and all(i == 1 for i in sse), str(sse))
    check("[P1] every user SigV4 secret sealed under id=1", usr and all(i == 1 for i in usr), str(usr))
stop()

print("\n== P2a: ring [1,2] active=2, re-wrap OFF — old key NOT retire-eligible ==", flush=True)
ok = start(base_env({"CAIRN_MASTER_KEY_RING": RING_12, "CAIRN_MASTER_KEY_ACTIVE_ID": "2",
                     "CAIRN_KEY_REWRAP_INTERVAL_SECS": "0"}), "p2a")
check("[P2a] boots with a 2-key ring", ok)
if ok:
    st, body = s3("GET", "/abkt/s1.txt"); check("[P2a] old id=1 object still opens under the ring", st == 200 and body == b"payload-one", str(st))
    put_sse("abkt", "s3.txt", b"payload-three")
    st, cs = crypto_status()
    check("[P2a] crypto-status active_key_id=2", st == 200 and cs.get("active_key_id") == 2, str(cs))
    complete = [r["complete"] for r in cs.get("rewrap", [])]
    check("[P2a] re-wrap streams report NOT complete before any pass", complete and not any(complete), str(cs.get("rewrap")))
    k1 = next((k for k in cs.get("keys", []) if k["id"] == 1), None)
    check("[P2a] id=1 NOT retire_eligible before re-wrap (BUG-1 fix)", k1 and k1.get("retire_eligible") is False, str(k1))
    check("[P2a] worker off -> old SSE DEKs still id=1", any(i == 1 for i in sse_ids()), str(sse_ids()))
stop()

print("\n== P2b: re-wrap ON — drive every stream to real completion ==", flush=True)
ok = start(base_env({"CAIRN_MASTER_KEY_RING": RING_12, "CAIRN_MASTER_KEY_ACTIVE_ID": "2",
                     "CAIRN_KEY_REWRAP_INTERVAL_SECS": "2", "CAIRN_KEY_COUNTER_SYNC_SECS": "2"}), "p2b")
check("[P2b] boots (re-wrap enabled)", ok)
if ok:
    drained = False; cs = {}
    for _ in range(40):
        time.sleep(1)
        st, cs = crypto_status()
        if st == 200 and cs.get("rewrap") and all(r["complete"] for r in cs["rewrap"]):
            drained = True; break
    check("[P2b] re-wrap reports every stream genuinely complete", drained, str(cs))
    sse = sse_ids(); usr = user_ids()
    check("[P2b] ALL SSE DEKs re-wrapped to id=2", sse and all(i == 2 for i in sse), str(sse))
    check("[P2b] ALL user secrets re-wrapped to id=2", usr and all(i == 2 for i in usr), str(usr))
    k1 = next((k for k in cs.get("keys", []) if k["id"] == 1), None)
    check("[P2b] id=1 retire_eligible ONLY after real completion", k1 and k1.get("retire_eligible") is True, str(k1))
    check("[P2b] crypto-status leaks no key material", all(len(k.get("key_hash", "")) <= 16 for k in cs.get("keys", [])), str(cs.get("keys")))
    st, body = s3("GET", "/zbkt/s2.txt"); check("[P2b] re-wrapped cross-shard object still decrypts", st == 200 and body == b"payload-two", str(st))
    # The additive labels MUST survive re-wrap (the whole point of the in-place-mutate guard in
    # key_rewrap.rs): mode/kms_key_id unchanged, only the sealing key id advances 1 -> 2.
    da, dk = descriptor_for("atr.txt"), descriptor_for("kms.txt")
    check("[P2b] at-rest descriptor re-wrapped to id=2, mode label intact",
          da and da["kid"] == 2 and da["mode"] == "at-rest", str(da))
    check("[P2b] KMS descriptor re-wrapped to id=2, mode+kms_key_id labels intact",
          dk and dk["kid"] == 2 and dk["mode"] == "kms" and dk["kms_key_id"] == KMSID, str(dk))
    st, body = s3("GET", "/abkt/atr.txt")
    check("[P2b] at-rest object decrypts byte-exact after re-wrap", st == 200 and body == b"at-rest-payload", str(st))
    st, body = s3("GET", "/zbkt/kms.txt")
    check("[P2b] KMS object decrypts byte-exact after re-wrap", st == 200 and body == b"kms-payload", str(st))
    st, hh = head("zbkt", "kms.txt")
    check("[P2b] KMS object still advertises aws:kms + key id after re-wrap",
          st == 200 and hh.get("x-amz-server-side-encryption") == "aws:kms"
          and hh.get("x-amz-server-side-encryption-aws-kms-key-id") == KMSID, str(hh))
    st, hh = head("abkt", "atr.txt")
    check("[P2b] at-rest object still advertises nothing after re-wrap",
          st == 200 and "x-amz-server-side-encryption" not in hh, str(hh))
stop()

print("\n== P3: retire id=1 (ring [2]) — all data still opens ==", flush=True)
ok = start(base_env({"CAIRN_MASTER_KEY_RING": RING_2, "CAIRN_MASTER_KEY_ACTIVE_ID": "2"}), "p3")
check("[P3] boots with id=1 retired", ok)
if ok:
    for (b, k, want) in [("abkt", "s1.txt", b"payload-one"), ("zbkt", "s2.txt", b"payload-two"), ("abkt", "s3.txt", b"payload-three"),
                         ("abkt", "atr.txt", b"at-rest-payload"), ("zbkt", "kms.txt", b"kms-payload")]:
        st, body = s3("GET", f"/{b}/{k}"); check(f"[P3] GET {b}/{k} opens after retire", st == 200 and body == want, f"{st} {body!r}")
    st, hh = head("zbkt", "kms.txt")
    check("[P3] KMS object still advertises aws:kms + key id after retire",
          st == 200 and hh.get("x-amz-server-side-encryption") == "aws:kms"
          and hh.get("x-amz-server-side-encryption-aws-kms-key-id") == KMSID, str(hh))
    put_sse("abkt", "s4.txt", b"post-retire")
    st, body = s3("GET", "/abkt/s4.txt"); check("[P3] new SSE write/read after retire", st == 200 and body == b"post-retire", str(st))
    put_kms("zbkt", "kms2.txt", b"post-retire-kms")
    st, body = s3("GET", "/zbkt/kms2.txt"); check("[P3] new SSE-KMS write/read after retire", st == 200 and body == b"post-retire-kms", str(st))
stop()

print("\n== P4: retire id=1 BEFORE re-wrap — startup retire-gate must refuse ==", flush=True)
os.system(f"rm -rf {RDATA} && mkdir -p {RDATA}")
ok = start(base_env({"CAIRN_MASTER_KEY": K1}), "p4a")
check("[P4] baseline boots (single key id=1)", ok)
if ok:
    s3("PUT", "/failbkt"); put_sse("failbkt", "doomed.txt", b"PLAINTEXT-MUST-NOT-LEAK")
    st, body = s3("GET", "/failbkt/doomed.txt"); check("[P4] baseline object readable under id=1", st == 200 and body == b"PLAINTEXT-MUST-NOT-LEAK", str(st))
    # An at-rest and a KMS object sealed under id=1 too, so the retire-gate must refuse removing id=1
    # for ALL three modes (it scans object_versions.sse_descriptor regardless of mode), not just
    # SSE-S3. If the gate ever missed a mode, that mode would surface UnknownKeyId -> 5xx on read.
    put_plain("failbkt", "doomed-atr.txt", b"PLAINTEXT-AT-REST-MUST-NOT-LEAK")
    put_kms("failbkt", "doomed-kms.txt", b"PLAINTEXT-KMS-MUST-NOT-LEAK")
stop()
ok = start(base_env({"CAIRN_MASTER_KEY_RING": RING_2, "CAIRN_MASTER_KEY_ACTIVE_ID": "2"}), "p4b", expect_ok=False)
check("[P4] retire-gate REFUSES startup when id=1 removed before re-wrap", not ok, "server unexpectedly started")
log = ""
p = os.path.join(ROOT, "p4b.log")
if os.path.exists(p):
    with open(p, errors="replace") as f: log = f.read()
check("[P4] refusal diagnostic names the retire-gate", "retire-gate" in log, log[-300:])
check("[P4] refusal diagnostic names the removed key id 1", "[1]" in log, log[-300:])
check("[P4] no plaintext ever leaked (server never served a request)", "PLAINTEXT" not in log, "")
stop()

print("\n== P5: seal counter survives a restart ==", flush=True)
os.system(f"rm -rf {RDATA} && mkdir -p {RDATA}")
ok = start(base_env({"CAIRN_MASTER_KEY": K1, "CAIRN_KEY_COUNTER_SYNC_SECS": "1"}), "p5a")
before = 0
if ok:
    s3("PUT", "/cnt")
    for i in range(6): put_sse("cnt", f"o{i}.txt", b"x" * 32)
    mkuser("bob"); time.sleep(2.5)
    st, cs = crypto_status(); before = cs.get("seal_count", 0)
    check("[P5] seal_count > 0 before restart", st == 200 and before > 0, str(cs))
stop()
ok = start(base_env({"CAIRN_MASTER_KEY": K1, "CAIRN_KEY_COUNTER_SYNC_SECS": "1"}), "p5b")
if ok:
    st, cs = crypto_status(); after = cs.get("seal_count", 0)
    check("[P5] seal_count primed from durable state after restart", st == 200 and after >= before and after > 0, f"before={before} after={after}")
stop()

print(f"\n== RESULT: {len(PASS)} passed, {len(FAIL)} failed ==", flush=True)
if FAIL:
    print("FAILED:", ", ".join(FAIL)); sys.exit(1)
print("ALL ROTATION E2E CHECKS PASSED")
