#!/usr/bin/env python3
"""Replication chaos / limit regression harness (ARCH 20).

Unlike soak.py (happy-path: byte-identical verify under sustained load), this driver deliberately
BREAKS replication and asserts it degrades safely — no data loss, bounded behaviour, eventual
convergence. It owns both node processes so it can inject faults (start the target late, crash and
restart the source, hold the target down):

  S1  target unreachable at write time, then recovers   -> every object converges, no loss
  S2  source crashes (SIGKILL) mid-replication, restarts -> outbox is durable, converges
  S3  target permanently down                            -> source stays healthy & readable; the
                                                            outbox is bounded (pending/failed), not lost
  S4  rapid overwrite of one key                         -> the replica converges to the LAST write

Config via env: BIN, DATA (temp root), PORT1 (target), PORT2 (source), REPL_INTERVAL.
Requires boto3 (same as soak.py).
"""
import os, signal, subprocess, sys, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

BIN = os.environ.get("BIN", "target/debug/cairn")
ROOT = os.environ["DATA"]
PORT_T = int(os.environ.get("PORT1", "9085"))  # target (mirror)
PORT_S = int(os.environ.get("PORT2", "9086"))  # source
REPL_INTERVAL = os.environ.get("REPL_INTERVAL", "1")
REGION = "us-east-1"
BUCKET = "chaos"
KEY_T = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
KEY_S = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
DATA_T = os.path.join(ROOT, "target")
DATA_S = os.path.join(ROOT, "source")

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)
def note(m): print(f"  {m}", flush=True)

def node_env(data, key, port, repl_to=None):
    e = dict(os.environ)
    for k in list(e):
        if k.startswith("CAIRN_"):
            del e[k]
    e.update({
        "CAIRN_DATA_DIR": os.path.join(data, "data"), "CAIRN_DB_PATH": os.path.join(data, "data/cairn.db"),
        "CAIRN_LISTEN_ADDR": f"127.0.0.1:{port}", "CAIRN_UI_ADDR": "off",
        "CAIRN_MASTER_KEY": key, "CAIRN_REGION": REGION,
        "CAIRN_LOG_LEVEL": os.environ.get("CAIRN_LOG_LEVEL", "error"),
    })
    if repl_to:
        ep, ak, sk = repl_to
        e.update({
            "CAIRN_REPLICATION_ENDPOINT": ep, "CAIRN_REPLICATION_ACCESS_KEY": ak,
            "CAIRN_REPLICATION_SECRET": sk, "CAIRN_REPLICATION_REGION": REGION,
            "CAIRN_REPLICATION_INTERVAL_SECS": REPL_INTERVAL,
        })
    return e

def bootstrap(env):
    out = subprocess.run([BIN, "bootstrap"], env=env, capture_output=True, text=True)
    akid = secret = None
    for line in out.stdout.splitlines():
        if "Access Key Id" in line: akid = line.split()[-1]
        if "Secret Access Key" in line: secret = line.split()[-1]
    if not akid or not secret:
        raise RuntimeError(f"bootstrap parse failed: {out.stdout}\n{out.stderr}")
    return akid, secret

PROCS = {}
def serve(name, env, port):
    log = open(os.path.join(ROOT, f"{name}.log"), "w")
    PROCS[name] = subprocess.Popen([BIN, "serve"], env=env, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(150):
        if PROCS[name].poll() is not None:
            return False
        try:
            import http.client
            c = http.client.HTTPConnection("127.0.0.1", port, timeout=2); c.request("GET", "/healthz")
            r = c.getresponse(); r.read(); c.close()
            if r.status == 200: time.sleep(0.3); return True
        except Exception:
            time.sleep(0.2)
    return False
def kill(name, sig=signal.SIGKILL):
    p = PROCS.get(name)
    if p and p.poll() is None:
        p.send_signal(sig)
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill(); p.wait()
    PROCS.pop(name, None)
def kill_all():
    for n in list(PROCS): kill(n)

def client(port, akid, secret):
    return boto3.client(
        "s3", endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id=akid, aws_secret_access_key=secret, region_name=REGION,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )

def setup_replication(src):
    src.create_bucket(Bucket=BUCKET)
    src.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
    src.put_bucket_replication(
        Bucket=BUCKET,
        ReplicationConfiguration={
            "Role": "arn:aws:iam::cairn:role/chaos",
            "Rules": [{"ID": "chaos-rule", "Status": "Enabled", "Prefix": "",
                       "Destination": {"Bucket": f"arn:aws:s3:::{BUCKET}"}}],
        },
    )

def get_body(cl, key):
    try:
        return cl.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except (ClientError, EndpointConnectionError):
        return None

def converged(tgt, expected, timeout=90):
    """Poll the target until every (key -> body) in `expected` matches, or timeout. Returns the
    number of keys that converged."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        got = sum(1 for k, v in expected.items() if get_body(tgt, k) == v)
        if got == len(expected):
            return got
        time.sleep(1)
    return sum(1 for k, v in expected.items() if get_body(tgt, k) == v)

def healthy(port):
    try:
        import http.client
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=2); c.request("GET", "/healthz")
        r = c.getresponse(); r.read(); c.close(); return r.status == 200
    except Exception:
        return False

# =====================================================================
print(f"=== replication chaos/limit regression (BIN={BIN}) ===", flush=True)
os.system(f"rm -rf {DATA_T} {DATA_S} && mkdir -p {DATA_T} {DATA_S}")
T_AK, T_SK = bootstrap(node_env(DATA_T, KEY_T, PORT_T))
S_AK, S_SK = bootstrap(node_env(DATA_S, KEY_S, PORT_S))
TGT_ENV = node_env(DATA_T, KEY_T, PORT_T)
SRC_ENV = node_env(DATA_S, KEY_S, PORT_S, repl_to=(f"http://127.0.0.1:{PORT_T}", T_AK, T_SK))
src = client(PORT_S, S_AK, S_SK)
tgt = client(PORT_T, T_AK, T_SK)

def write(prefix, n):
    objs = {f"{prefix}/obj{i}.bin": (f"{prefix}-payload-{i}".encode() * 64) for i in range(n)}
    for k, v in objs.items():
        src.put_object(Bucket=BUCKET, Key=k, Body=v)
    return objs

try:
    # The destination bucket must pre-exist (as in any real deployment): bring the target up, create
    # it, and set up the source's replication rule — all before injecting any fault. A 404 from a
    # bucketless target would be a *terminal* replication error, so this ordering matters.
    if not serve("target", TGT_ENV, PORT_T):
        raise RuntimeError("target failed to boot during setup")
    tgt.create_bucket(Bucket=BUCKET)
    if not serve("source", SRC_ENV, PORT_S):
        raise RuntimeError("source failed to boot during setup")
    setup_replication(src)

    # ---------- S1: target unreachable at write time, then recovers ----------
    print("\n== S1: target down at write time, comes up later — no data loss ==", flush=True)
    kill("target")
    objs = write("s1", 15)
    note("15 objects written while the target is DOWN")
    time.sleep(3)
    check("[S1] target is genuinely unreachable while down", get_body(tgt, "s1/obj0.bin") is None)
    note("bringing the target back UP")
    check("[S1] target recovers", serve("target", TGT_ENV, PORT_T))
    got = converged(tgt, objs, timeout=120)
    check("[S1] every object converged after recovery (no loss despite down-at-write)", got == len(objs), f"{got}/{len(objs)}")

    # ---------- S2: source crashes mid-replication, restarts ----------
    print("\n== S2: source SIGKILL mid-replication, restart — outbox durable ==", flush=True)
    objs2 = write("s2", 15)
    note("15 objects written; SIGKILL the source immediately (before it can drain)")
    kill("source", signal.SIGKILL)
    check("[S2] source restarts on the same data dir", serve("source", SRC_ENV, PORT_S))
    src = client(PORT_S, S_AK, S_SK)
    got = converged(tgt, objs2, timeout=120)
    check("[S2] outbox survived the crash; every object converged", got == len(objs2), f"{got}/{len(objs2)}")

    # ---------- S3: target permanently down — source stays healthy, isolated ----------
    print("\n== S3: target down under sustained writes — source healthy & readable ==", flush=True)
    kill("target")
    objs3 = write("s3", 10)
    time.sleep(6)  # let several failed replication passes run
    check("[S3] source stays healthy despite the target being unreachable", healthy(PORT_S))
    src_ok = sum(1 for k, v in objs3.items() if get_body(src, k) == v)
    check("[S3] objects remain fully readable on the source (failure is isolated)", src_ok == len(objs3), f"{src_ok}/{len(objs3)}")
    check("[S3] source process did not die under sustained replication failure", PROCS.get("source") and PROCS["source"].poll() is None)

    # ---------- S4: rapid overwrite of one key — replica converges to the last write ----------
    print("\n== S4: rapid overwrite ordering — replica converges to the LAST write ==", flush=True)
    check("[S4] target back up", serve("target", TGT_ENV, PORT_T))
    last = b""
    for i in range(12):
        last = f"version-{i:02d}".encode() * 64
        src.put_object(Bucket=BUCKET, Key="s4/hot.bin", Body=last)
    note("12 rapid overwrites of one key")
    got = converged(tgt, {"s4/hot.bin": last}, timeout=120)
    check("[S4] replica converged to the final version (ordering preserved)", got == 1, "target body != last write")
    # And the writes made during the S3 outage must also eventually land (no loss).
    got3 = converged(tgt, objs3, timeout=120)
    check("[S4] writes from the earlier outage also converged after recovery", got3 == len(objs3), f"{got3}/{len(objs3)}")
finally:
    kill_all()

print(f"\n== RESULT: {len(PASS)} passed, {len(FAIL)} failed ==", flush=True)
if FAIL:
    print("FAILED:", ", ".join(FAIL)); sys.exit(1)
print("ALL REPLICATION CHAOS CHECKS PASSED")
