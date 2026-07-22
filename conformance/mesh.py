#!/usr/bin/env python3
"""5-node MESH bucket-replication harness (ARCH 20).

Stands up FIVE independent Cairn nodes and wires a FULL mesh: every node replicates a bucket to the
other four (5x4 = 20 directed target+rule pairs). Because a received replica enqueues nothing
(cascading is off), a full mesh is the only topology under which a write to ANY node reaches ALL
nodes. The harness then drives the data-resiliency / corruption / throughput / bottleneck scenarios.

Stdlib only (no boto3): a minimal SigV4 signer for the S3 data plane + Bearer for the control API.
The harness owns all five node processes so it can inject faults (crash + restart) and tears them
down on exit.

Scenarios:
  1  full-mesh convergence        — write distinct objects to each node -> all 5 hold all objects
  2  fan-out latency              — one write -> reaches the other 4 well within the interval
  3  version-id identity          — a version has the SAME id on every node, no duplicate versions
  4  concurrent same-key writes   — blob converges across the mesh
  5  crash resiliency             — SIGKILL a node mid-bulk-replication, restart -> converges, no dup
  6  delete-marker mesh           — a delete propagates without looping
  7  throughput / bottleneck      — bulk write to one node, time full-mesh convergence + capture gauges
  8  no-cascade                   — a partial ring does NOT converge transitively
  9  integrity                    — cross-node version-set equality + `cairn integrity` clean

Env: BIN (cairn binary), DATA (temp root), BASE_PORT (default 7500), REPL_INTERVAL (default 2).
"""
import os, re, sys, time, json, signal, subprocess, hashlib, hmac, datetime
import urllib.request, urllib.error, urllib.parse
import http.client

BIN = os.environ.get("BIN", "target/debug/cairn")
ROOT = os.environ["DATA"]
BASE = int(os.environ.get("BASE_PORT", "7500"))
REPL_INTERVAL = os.environ.get("REPL_INTERVAL", "2")
REGION, SERVICE = "us-east-1", "s3"
AKID, SECRET = "cairn", "cairnadmin"
BUCKET = "mesh"
N = 5

# node i: S3 port = BASE + i*10 + 1, UI/control port = BASE + i*10 + 2
def s3_port(i): return BASE + i * 10 + 1
def ui_port(i): return BASE + i * 10 + 2
def s3_url(i): return f"http://127.0.0.1:{s3_port(i)}"
def ui_url(i): return f"http://127.0.0.1:{ui_port(i)}"
NAMES = [chr(ord("A") + i) for i in range(N)]

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    tag = "PASS" if cond else "FAIL"
    # Detail is shown only on failure (matches the other conformance harnesses); informational
    # numbers on a passing check go through note().
    print(f"  [{tag}] {name}" + (f" — {detail}" if detail and not cond else ""), flush=True)
def note(m): print(f"  {m}", flush=True)
def banner(m): print(f"\n=== {m} ===", flush=True)

# ---------------------------------------------------------------- SigV4 signer (stdlib)
def _sign(key, msg): return hmac.new(key, msg.encode(), hashlib.sha256).digest()
def _signing_key(secret, ds):
    k = _sign(("AWS4" + secret).encode(), ds)
    for p in (REGION, SERVICE, "aws4_request"): k = _sign(k, p)
    return k
def _enc(path): return "/".join(urllib.parse.quote(s, safe="") for s in path.split("/"))
def _cq(q): return "&".join(f"{urllib.parse.quote(k,safe='')}={urllib.parse.quote(v,safe='')}"
                            for k, v in sorted(q, key=lambda kv: kv[0]))

def s3req(endpoint, method, path, query=None, body=b"", content_type=None, extra=None):
    """Signed S3 request. Returns (status, headers_dict_lowercased, body_bytes)."""
    query = query or []
    if isinstance(body, str): body = body.encode()
    host = urllib.parse.urlparse(endpoint).netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, ds = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    ph = hashlib.sha256(body).hexdigest()
    headers = {"host": host, "x-amz-content-sha256": ph, "x-amz-date": amz}
    if content_type: headers["content-type"] = content_type
    for k, v in (extra or {}).items(): headers[k.lower()] = v
    sh = ";".join(sorted(headers))
    ch = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    cr = "\n".join([method, _enc(path), _cq(query), ch, sh, ph])
    scope = f"{ds}/{REGION}/{SERVICE}/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(cr.encode()).hexdigest()])
    sig = hmac.new(_signing_key(SECRET, ds), sts.encode(), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={AKID}/{scope}, SignedHeaders={sh}, Signature={sig}"
    url = endpoint.rstrip("/") + _enc(path) + (("?" + _cq(query)) if query else "")
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", auth)
    for k, v in headers.items():
        if k != "host": req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read()
    except (urllib.error.URLError, ConnectionError) as e:
        return 0, {}, str(e).encode()

def ctl(i, method, path, body=None):
    """Control-API call (Bearer auth) against node i's UI port. Returns (status, json_or_bytes)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ui_url(i).rstrip("/") + "/api/v1" + path, data=data, method=method)
    req.add_header("Authorization", f"Bearer {AKID}.{SECRET}")
    if data is not None: req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            try: return r.status, json.loads(raw)
            except Exception: return r.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read()
        try: return e.code, json.loads(raw)
        except Exception: return e.code, raw
    except Exception as e:
        return 0, str(e)

# ---------------------------------------------------------------- node lifecycle
PROCS = {}
def node_env(i, master_key):
    e = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_")}
    d = os.path.join(ROOT, NAMES[i])
    os.makedirs(os.path.join(d, "data"), exist_ok=True)
    e.update({
        "CAIRN_DATA_DIR": os.path.join(d, "data"),
        "CAIRN_DB_PATH": os.path.join(d, "data/cairn.db"),
        "CAIRN_LISTEN_ADDR": f"127.0.0.1:{s3_port(i)}",
        "CAIRN_UI_ADDR": f"127.0.0.1:{ui_port(i)}",
        "CAIRN_MASTER_KEY": master_key,
        "CAIRN_ROOT_ACCESS_KEY": AKID, "CAIRN_ROOT_SECRET_KEY": SECRET,
        "CAIRN_REGION": REGION, "CAIRN_ALLOW_INSECURE": "true",
        # The mesh wires replication targets through the management API, which enforces the
        # cairn-net SSRF guard; the loopback topology needs the internal-endpoint escape hatch
        # (soak.sh avoids this by using the operator-trusted CAIRN_REPLICATION_ENDPOINT config path).
        "CAIRN_ALLOW_INTERNAL_ENDPOINTS": "true",
        "CAIRN_LOG_LEVEL": os.environ.get("CAIRN_LOG_LEVEL", "warn"),
        "CAIRN_REPLICATION_INTERVAL_SECS": REPL_INTERVAL,
        "CAIRN_REPLICATION_WORKER_CONCURRENCY": os.environ.get("WORKERS", "4"),
    })
    return e

KEYS = {}
def bootstrap(i):
    KEYS.setdefault(i, hashlib.sha256(f"mesh-key-{i}".encode()).hexdigest())
    env = node_env(i, KEYS[i])
    subprocess.run([BIN, "bootstrap"], env=env, capture_output=True, text=True)

def serve(i):
    env = node_env(i, KEYS[i])
    log = open(os.path.join(ROOT, f"{NAMES[i]}.log"), "a")
    PROCS[i] = subprocess.Popen([BIN, "serve"], env=env, stdout=log, stderr=subprocess.STDOUT)
    for _ in range(150):
        if PROCS[i].poll() is not None: return False
        try:
            c = http.client.HTTPConnection("127.0.0.1", s3_port(i), timeout=2)
            c.request("GET", "/readyz"); r = c.getresponse(); r.read(); c.close()
            if r.status == 200: time.sleep(0.2); return True
        except Exception: time.sleep(0.2)
    return False

def kill(i, sig=signal.SIGKILL):
    p = PROCS.get(i)
    if p and p.poll() is None:
        p.send_signal(sig)
        try: p.wait(timeout=15)
        except subprocess.TimeoutExpired: p.kill(); p.wait()
    PROCS.pop(i, None)
def kill_all():
    for i in list(PROCS): kill(i)

# ---------------------------------------------------------------- S3 helpers
def put(i, key, body, bucket=BUCKET, extra=None):
    st, h, _ = s3req(s3_url(i), "PUT", f"/{bucket}/{key}", body=body,
                     content_type="application/octet-stream", extra=extra)
    return st, h.get("x-amz-version-id")
def get(i, key, bucket=BUCKET):
    st, h, b = s3req(s3_url(i), "GET", f"/{bucket}/{key}")
    return (b if st == 200 else None), h.get("etag")
def get_version(i, key, vid, bucket=BUCKET):
    st, _, _ = s3req(s3_url(i), "GET", f"/{bucket}/{key}", query=[("versionId", vid)])
    return st
def delete(i, key, bucket=BUCKET, extra=None):
    st, h, _ = s3req(s3_url(i), "DELETE", f"/{bucket}/{key}", extra=extra)
    return st, h.get("x-amz-version-id")
def make_bucket(i, bucket=BUCKET):
    s3req(s3_url(i), "PUT", f"/{bucket}")
    s3req(s3_url(i), "PUT", f"/{bucket}", query=[("versioning", "")],
          body="<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
          content_type="application/xml")

def list_versions(i, bucket=BUCKET):
    """Return {key: [version_id,...]} from ListObjectVersions (versions + delete markers)."""
    st, _, b = s3req(s3_url(i), "GET", f"/{bucket}", query=[("versions", "")])
    out = {}
    if st != 200: return out
    xml = b.decode("utf-8", "replace")
    # crude but adequate: pair each <Key> with the following <VersionId> within the same element
    for block in re.findall(r"<(?:Version|DeleteMarker)>.*?</(?:Version|DeleteMarker)>", xml, re.S):
        k = re.search(r"<Key>(.*?)</Key>", block, re.S)
        v = re.search(r"<VersionId>(.*?)</VersionId>", block, re.S)
        if k and v:
            out.setdefault(k.group(1), []).append(v.group(1))
    return out

def summary(i):
    st, j = ctl(i, "GET", "/replication/summary")
    return j if st == 200 and isinstance(j, dict) else {}
def metric(i, name):
    try:
        c = http.client.HTTPConnection("127.0.0.1", s3_port(i), timeout=3)
        c.request("GET", "/metrics"); r = c.getresponse(); body = r.read().decode(); c.close()
        for line in body.splitlines():
            if line.startswith(name + " ") or line.startswith(name + "{"):
                try: return float(line.rsplit(" ", 1)[1])
                except Exception: pass
    except Exception: pass
    return None

# ---------------------------------------------------------------- mesh wiring
def wire_full_mesh():
    """On each node: versioned bucket + 4 targets (the other nodes) + 4 rules naming each target ARN."""
    for i in range(N):
        make_bucket(i)
    for i in range(N):
        rules = []
        for j in range(N):
            if i == j: continue
            st, j_resp = ctl(i, "POST", f"/buckets/{BUCKET}/replication/targets", {
                "endpoint": s3_url(j), "region": REGION, "dest_bucket": BUCKET,
                "access_key": AKID, "secret": SECRET,
            })
            if st not in (200, 201) or not isinstance(j_resp, dict) or "arn" not in j_resp:
                raise RuntimeError(f"target {NAMES[i]}->{NAMES[j]} failed: {st} {j_resp}")
            rules.append((NAMES[i] + NAMES[j], j_resp["arn"]))
        xml = "<ReplicationConfiguration><Role>cairn</Role>"
        for rid, arn in rules:
            xml += (f"<Rule><ID>{rid}</ID><Status>Enabled</Status><Priority>1</Priority>"
                    f"<DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>"
                    f"<Filter><Prefix></Prefix></Filter>"
                    f"<Destination><Bucket>{arn}</Bucket></Destination></Rule>")
        xml += "</ReplicationConfiguration>"
        st, _, _ = s3req(s3_url(i), "PUT", f"/{BUCKET}", query=[("replication", "")],
                         body=xml, content_type="application/xml")
        if st not in (200, 204):
            raise RuntimeError(f"rule set on {NAMES[i]} failed: {st}")

def converged_everywhere(expected, nodes=None, timeout=60):
    """Poll until every (key->body) in expected is present on every node in `nodes`."""
    nodes = nodes if nodes is not None else list(range(N))
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(get(i, k)[0] == v for i in nodes for k, v in expected.items()):
            return True
        time.sleep(0.5)
    return False

# ---------------------------------------------------------------- scenarios
def sc1_convergence():
    banner("Scenario 1: full-mesh convergence (write to each node -> all 5 hold all)")
    expected = {}
    for i in range(N):
        key = f"s1/from-{NAMES[i]}.txt"
        body = f"object-from-{NAMES[i]}".encode()
        st, _ = put(i, key, body)
        expected[key] = body
        check(f"PUT {key} on {NAMES[i]}", st == 200, f"status {st}")
    ok = converged_everywhere(expected, timeout=60)
    check("every object present on every node (body match)", ok)
    # ETag agreement
    if ok:
        agree = True
        for k in expected:
            etags = {get(i, k)[1] for i in range(N)}
            if len(etags) != 1: agree = False
        check("ETag identical across all nodes", agree)

def sc2_fanout_latency():
    banner("Scenario 2: 1->N fan-out latency (write to A -> reaches B,C,D,E)")
    key = "s2/fanout.txt"; body = b"fan-out-payload"
    t0 = time.time()
    put(0, key, body)
    deadline = t0 + 30
    reached = {}
    while time.time() < deadline and len(reached) < N - 1:
        for j in range(1, N):
            if j not in reached and get(j, key)[0] == body:
                reached[j] = time.time() - t0
        time.sleep(0.2)
    check(f"reached all {N-1} peers", len(reached) == N - 1,
          "reached " + ", ".join(f"{NAMES[j]}@{reached[j]:.1f}s" for j in sorted(reached)))
    if reached:
        note(f"fan-out latencies: " + ", ".join(f"{NAMES[j]}={reached[j]:.2f}s" for j in sorted(reached)))

def sc3_version_id_identity():
    banner("Scenario 3: version-id identity (same id on every node, no duplicate versions)")
    key = "s3/identity.txt"; body = b"identity-payload"
    st, vid = put(0, key, body)
    check(f"PUT returned a version id on {NAMES[0]}", bool(vid), f"vid={vid}")
    converged_everywhere({key: body}, timeout=60)
    # The source's version id must exist (200) on every peer.
    same = all(get_version(j, key, vid) == 200 for j in range(1, N)) if vid else False
    check("source version-id resolves on every peer (id preserved)", same,
          "GET ?versionId returns 404 on peers => version-id NOT preserved")
    # No duplicate versions for the key on any node (exactly one version each).
    counts = {NAMES[i]: len(list_versions(i).get(key, [])) for i in range(N)}
    check("exactly one version of the key on every node (no duplicates)",
          all(c == 1 for c in counts.values()), f"per-node version counts: {counts}")
    # The version-id SET is identical across the mesh.
    sets = {tuple(sorted(list_versions(i).get(key, []))) for i in range(N)}
    check("version-id set identical across the mesh", len(sets) == 1, f"distinct sets: {sets}")

def sc4_concurrent():
    banner("Scenario 4: concurrent same-key writes on two nodes (deterministic convergence)")
    key = "s4/contended.txt"
    st, vA = put(0, key, b"written-on-A")
    st, vC = put(2, key, b"written-on-C")
    check("two distinct versions created", bool(vA) and bool(vC) and vA != vC)
    # version-id is time-ordered, so the later write (vC) has the larger id and must be the latest
    # everywhere — that is the whole point of replica version-id ordering.
    winner = b"written-on-C" if vC > vA else b"written-on-A"
    # converge: every node must hold BOTH versions and agree on the latest body.
    deadline = time.time() + 60
    ok = False
    while time.time() < deadline:
        have_both = all(set(list_versions(i).get(key, [])) == {vA, vC} for i in range(N))
        same_latest = len({get(i, key)[0] for i in range(N)}) == 1
        if have_both and same_latest:
            ok = True; break
        time.sleep(0.5)
    check("every node holds both versions", all(set(list_versions(i).get(key, [])) == {vA, vC} for i in range(N)))
    latest = {NAMES[i]: get(i, key)[0] for i in range(N)}
    check("every node agrees on the latest version (deterministic by version id)",
          len(set(latest.values())) == 1, f"latest bodies: { {k: (v or b'').decode() for k,v in latest.items()} }")
    check("latest is the higher-version-id write on every node",
          all(b == winner for b in latest.values()))
    note(f"converged={ok}; winner={winner.decode()}")

def sc5_crash():
    banner("Scenario 5: crash resiliency (SIGKILL a node mid-replication, restart -> converge, no dup)")
    K = 30
    expected = {}
    for n in range(K):
        key = f"s5/obj-{n:03d}.txt"; body = f"crash-payload-{n}".encode()
        put(0, key, body); expected[key] = body
    note(f"wrote {K} objects to A; SIGKILL A immediately (mid-drain)")
    kill(0)
    time.sleep(1)
    check("A restarts cleanly", serve(0))
    ok = converged_everywhere(expected, timeout=90)
    check(f"all {K} objects converge on every node after restart", ok)
    # No duplicate-version bloat: every object has exactly one version on every node.
    dup = []
    for key in expected:
        for i in range(N):
            c = len(list_versions(i).get(key, []))
            if c != 1: dup.append((NAMES[i], key, c))
    check("exactly one version per object on every node (no re-delivery duplicates)", not dup,
          f"{len(dup)} duplicates; e.g. {dup[0] if dup else ''}")

def sc6_delete_marker():
    banner("Scenario 6: delete-marker mesh (propagates, hides everywhere, no loop, id preserved)")
    key = "s6/to-delete.txt"; body = b"delete-me"
    put(0, key, body)
    check("object converges before delete", converged_everywhere({key: body}, timeout=30))
    st, mvid = delete(0, key)
    check("DELETE on A creates a marker", st in (200, 204) and bool(mvid))
    # The object must become hidden (404) on EVERY node as the marker propagates.
    deadline = time.time() + 40
    while time.time() < deadline:
        if all(get(i, key)[0] is None for i in range(N)): break
        time.sleep(0.5)
    hidden = {NAMES[i]: (get(i, key)[0] is None) for i in range(N)}
    check("object hidden (404) on every node", all(hidden.values()), f"{hidden}")
    # The marker's version id is preserved across the mesh (it is in every node's version listing).
    marker_sets = {tuple(sorted(list_versions(i).get(key, []))) for i in range(N)}
    check("marker version-id set identical across the mesh", len(marker_sets) == 1, f"{marker_sets}")
    # No loop: pending replication returns to 0 on the originating node.
    deadline = time.time() + 20
    while time.time() < deadline:
        if summary(0).get("pending", 1) == 0: break
        time.sleep(0.5)
    check("no replication loop (pending returns to 0 on A)", summary(0).get("pending", 1) == 0)

def sc7_throughput():
    banner("Scenario 7: throughput / bottleneck (bulk write to one node, time full-mesh convergence)")
    K = int(os.environ.get("BULK", "100"))
    expected = {}
    t0 = time.time()
    for n in range(K):
        key = f"s7/bulk-{n:04d}.bin"; body = (f"bulk-{n}-" + "x" * 200).encode()
        put(0, key, body); expected[key] = body
    t_write = time.time() - t0
    note(f"wrote {K} objects to A in {t_write:.2f}s ({K/t_write:.0f} obj/s local)")
    # Sample the bottleneck gauges while the mesh drains.
    peak = {"queue": 0.0, "writer": 0.0, "lag": 0.0}
    deadline = time.time() + 120
    converged_at = None
    while time.time() < deadline:
        peak["queue"] = max(peak["queue"], metric(0, "cairn_replication_queue_depth") or 0)
        peak["writer"] = max(peak["writer"], max((metric(i, "cairn_writer_queue_depth") or 0) for i in range(N)))
        peak["lag"] = max(peak["lag"], metric(0, "cairn_replication_lag_seconds") or 0)
        if all(get(i, k)[0] == v for i in range(1, N) for k, v in expected.items()):
            converged_at = time.time() - t0; break
        time.sleep(0.5)
    check(f"all {K} objects reached all 4 peers", converged_at is not None)
    if converged_at:
        # K objects x 4 targets = 4K replications.
        note(f"full-mesh convergence in {converged_at:.1f}s — {4*K/converged_at:.0f} replications/s")
    note(f"peak source outbox queue_depth={peak['queue']:.0f}, peak writer_queue_depth={peak['writer']:.0f}, "
         f"peak lag={peak['lag']:.0f}s")
    # Bottleneck verdict (informational).
    if peak["writer"] >= peak["queue"] and peak["writer"] > 1:
        note("bottleneck signal: metadata WRITER queue dominated (single-writer commit bound)")
    elif peak["queue"] > 1:
        note("bottleneck signal: replication OUTBOX queue dominated (worker/sink throughput bound)")
    else:
        note("no sustained backlog — fan-out kept up with the write rate")

def sc8_no_cascade():
    banner("Scenario 8: no-cascade (a partial chain A->B->C does NOT propagate transitively)")
    rb = "ring"
    for i in range(N): make_bucket(i, rb)
    # A -> B only; B -> C only. (No A->C rule.)
    def one_rule(src, dst):
        st, j = ctl(src, "POST", f"/buckets/{rb}/replication/targets",
                    {"endpoint": s3_url(dst), "region": REGION, "dest_bucket": rb,
                     "access_key": AKID, "secret": SECRET})
        arn = j["arn"]
        xml = ("<ReplicationConfiguration><Role>cairn</Role>"
               f"<Rule><ID>{NAMES[src]}{NAMES[dst]}</ID><Status>Enabled</Status>"
               f"<Filter><Prefix></Prefix></Filter>"
               f"<Destination><Bucket>{arn}</Bucket></Destination></Rule></ReplicationConfiguration>")
        s3req(s3_url(src), "PUT", f"/{rb}", query=[("replication", "")], body=xml, content_type="application/xml")
    one_rule(0, 1)  # A -> B
    one_rule(1, 2)  # B -> C
    key = "chain.txt"; body = b"only-direct-hops"
    put(0, key, body, bucket=rb)
    time.sleep(8)  # ample drain time
    on_b = get(1, key, bucket=rb)[0] == body
    on_c = get(2, key, bucket=rb)[0] is None
    check("A's write reaches B (direct rule)", on_b)
    check("A's write does NOT cascade to C through B (no transitive propagation)", on_c,
          "C received it => cascading is happening (unexpected)")

def sc9_integrity():
    banner("Scenario 9: integrity (cross-node version-set equality + cairn integrity)")
    # Compare the full per-key version-id sets across all nodes.
    allkeys = set()
    pernode = {}
    for i in range(N):
        pernode[i] = list_versions(i)
        allkeys |= set(pernode[i].keys())
    mismatches = []
    for k in sorted(allkeys):
        sets = {tuple(sorted(pernode[i].get(k, []))) for i in range(N)}
        if len(sets) != 1:
            mismatches.append((k, {NAMES[i]: pernode[i].get(k, []) for i in range(N)}))
    check("every key's version-id set is identical on every node", not mismatches,
          f"{len(mismatches)} divergent keys" + (f"; e.g. {mismatches[0]}" if mismatches else ""))
    if mismatches:
        for k, detail in mismatches[:5]:
            note(f"  divergent {k}: {detail}")
    # cairn integrity on node A (stop it first so the store is quiesced).
    kill(0)
    env = node_env(0, KEYS[0])
    out = subprocess.run([BIN, "integrity"], env=env, capture_output=True, text=True)
    line = (out.stdout + out.stderr).strip().splitlines()
    note(f"cairn integrity (node A): {line[-1] if line else '(no output)'}")
    check("cairn integrity ran on node A", out.returncode == 0)
    serve(0)

def main():
    only = sys.argv[1:] or None
    print(f"Spinning {N} nodes on ports {s3_port(0)}..{s3_port(N-1)} (UI {ui_port(0)}..{ui_port(N-1)})", flush=True)
    try:
        for i in range(N): bootstrap(i)
        for i in range(N):
            if not serve(i): raise RuntimeError(f"node {NAMES[i]} failed to start")
        note(f"all {N} nodes ready")
        wire_full_mesh()
        note("full mesh wired (20 target+rule pairs)")
        scenarios = {"1": sc1_convergence, "2": sc2_fanout_latency, "3": sc3_version_id_identity,
                     "4": sc4_concurrent, "5": sc5_crash, "6": sc6_delete_marker,
                     "7": sc7_throughput, "8": sc8_no_cascade, "9": sc9_integrity}
        for tag, fn in scenarios.items():
            if only and tag not in only: continue
            fn()
    finally:
        kill_all()
    banner(f"RESULT: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED: " + ", ".join(FAIL), flush=True)
        sys.exit(1)
    print("ALL PASSED", flush=True)

if __name__ == "__main__":
    main()
