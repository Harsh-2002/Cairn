#!/usr/bin/env python3
"""Concurrency / contention regression (ARCH 7, 11). Hammer a SINGLE key with many simultaneous
clients and assert the write path stays atomic and uncorrupted under contention — the limit
behaviour of the single group-committing writer + the conditional-write preconditions + the cache
TOCTOU guards.

  A  create race  : N clients PUT a fresh key with `If-None-Match: *`  -> EXACTLY one wins, rest 412
  B  CAS race     : N clients PUT with `If-Match: <same etag>`         -> EXACTLY one wins, rest 412
  C  last-writer  : N clients PUT one key, distinct bodies, no precond -> all 2xx; final body is
                    EXACTLY one of the writes (never torn/mixed) and its ETag is consistent

Config via env: BIN, DATA, PORT, N (clients).
"""
import http.client, os, signal, subprocess, sys, threading, time
from concurrent.futures import ThreadPoolExecutor

BIN = os.environ.get("BIN", "target/debug/cairn")
ROOT = os.environ["DATA"]
PORT = int(os.environ.get("PORT", "9087"))
N = int(os.environ.get("N", "32"))
BUCKET = "conc"

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)

def env():
    e = dict(os.environ)
    for k in list(e):
        if k.startswith("CAIRN_"): del e[k]
    e.update({
        "CAIRN_DATA_DIR": os.path.join(ROOT, "data"), "CAIRN_DB_PATH": os.path.join(ROOT, "data/cairn.db"),
        "CAIRN_LISTEN_ADDR": f"127.0.0.1:{PORT}", "CAIRN_UI_ADDR": "off",
        "CAIRN_MASTER_KEY": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
        "CAIRN_LOG_LEVEL": os.environ.get("CAIRN_LOG_LEVEL", "error"),
    })
    return e

def bootstrap(e):
    out = subprocess.run([BIN, "bootstrap"], env=e, capture_output=True, text=True)
    for line in out.stdout.splitlines():
        if "Authorization: Bearer" in line:
            return line.split()[-1]
    raise RuntimeError(f"no bearer in bootstrap: {out.stdout}\n{out.stderr}")

def req(method, path, body=b"", headers=None):
    h = {"authorization": f"Bearer {BEARER}"}
    if headers: h.update(headers)
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=30)
    c.request(method, path, body=body, headers=h)
    r = c.getresponse(); data = r.read(); etag = r.getheader("etag"); c.close()
    return r.status, data, etag

# ---------- server lifecycle ----------
os.system(f"rm -rf {ROOT}/data && mkdir -p {ROOT}/data")
BEARER = bootstrap(env())
log = open(os.path.join(ROOT, "server.log"), "w")
SRV = subprocess.Popen([BIN, "serve"], env=env(), stdout=log, stderr=subprocess.STDOUT)
def cleanup():
    if SRV.poll() is None:
        SRV.send_signal(signal.SIGINT)
        try: SRV.wait(timeout=10)
        except subprocess.TimeoutExpired: SRV.kill()
try:
    ready = False
    for _ in range(150):
        try:
            c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=2); c.request("GET", "/healthz")
            r = c.getresponse(); r.read(); c.close()
            if r.status == 200: ready = True; break
        except Exception: time.sleep(0.2)
    check("server is healthy", ready)
    req("PUT", f"/{BUCKET}")

    def race(make_request):
        """Fire N requests as simultaneously as possible (barrier-synced) and return the list of
        (status) results."""
        barrier = threading.Barrier(N)
        results = [None] * N
        def worker(i):
            barrier.wait()
            results[i] = make_request(i)
        with ThreadPoolExecutor(max_workers=N) as ex:
            list(ex.map(worker, range(N)))
        return results

    print(f"\n== A: {N}-way create race (If-None-Match: *) — exactly one winner ==", flush=True)
    res = race(lambda i: req("PUT", f"/{BUCKET}/create-race", body=f"body-{i}".encode(),
                             headers={"if-none-match": "*"})[0])
    wins = sum(1 for s in res if s in (200, 204))
    denied = sum(1 for s in res if s == 412)
    check("[A] exactly one create wins", wins == 1, f"wins={wins} of {N}")
    check("[A] every other create is rejected 412 (no second creator)", denied == N - 1, f"412={denied}")
    check("[A] no 5xx under contention", not any(s >= 500 for s in res), f"{sorted(set(res))}")

    print(f"\n== B: {N}-way CAS race (If-Match: same etag) — exactly one winner ==", flush=True)
    req("PUT", f"/{BUCKET}/cas", body=b"seed")
    _, _, etag = req("GET", f"/{BUCKET}/cas")
    res = race(lambda i: req("PUT", f"/{BUCKET}/cas", body=f"cas-{i}".encode(),
                             headers={"if-match": etag})[0])
    wins = sum(1 for s in res if s in (200, 204))
    denied = sum(1 for s in res if s == 412)
    check("[B] exactly one compare-and-swap wins", wins == 1, f"wins={wins} of {N}")
    check("[B] every stale CAS is rejected 412 (no lost update)", denied == N - 1, f"412={denied}")
    st, body, _ = req("GET", f"/{BUCKET}/cas")
    check("[B] final body is the single winner's write", body.startswith(b"cas-") and body != b"seed", f"{body!r}")

    print(f"\n== C: {N}-way last-writer-wins — final object is exactly one write, never torn ==", flush=True)
    bodies = {f"lww-{i}".encode() for i in range(N)}
    res = race(lambda i: req("PUT", f"/{BUCKET}/lww", body=f"lww-{i}".encode())[0])
    check("[C] every unconditional write succeeds", all(s in (200, 204) for s in res), f"{sorted(set(res))}")
    st, body, etag = req("GET", f"/{BUCKET}/lww")
    check("[C] final body is exactly one of the writes (no torn/mixed content)", body in bodies, f"{body!r}")
    check("[C] final object has a valid ETag", st == 200 and bool(etag), f"{st} {etag}")
finally:
    cleanup()

print(f"\n== RESULT: {len(PASS)} passed, {len(FAIL)} failed ==", flush=True)
if FAIL:
    print("FAILED:", ", ".join(FAIL)); sys.exit(1)
print("ALL CONCURRENCY CHECKS PASSED")
