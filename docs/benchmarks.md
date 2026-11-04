# Benchmarks and macro load profiles

This document covers Cairn's layers of performance verification:

1. a **criterion micro-benchmark** of the SigV4 streaming chunked decoder (the hottest pure-CPU
   stage on the ingest path) — 1;
2. a **boto3 macro load harness** (`conformance/load_profile.py`) that drives a real `cairn`
   binary with concurrent clients to characterize large-object bandwidth, small-object rate, and
   — the point of ARCH 30.2 — the **single-writer ceiling** — 2;
3. the **real MinIO `warp` macro benchmark** (`conformance/warp.sh`), the industry-standard S3
   load tool, run against a fresh Cairn — 3; and
4. a **multi-host replication soak** (`conformance/soak.sh`): two Cairn nodes, a sustained PUT
   workload, byte-for-byte replication verification, and a source-RSS leak check — 4.

The boto3 harness (2) and `warp` (3) overlap deliberately: 2 is the always-available,
dependency-free generator that the single-writer-ceiling analysis is built on; 3 is the external
ground-truth tool. Where `warp` can run, it corroborates 2; see 3 for an important caveat.

All numbers below were recorded on this host; they are illustrative of *shape*, not a hardware
spec sheet. Re-run on the target hardware for absolute figures.

---

## 1. Chunked-decoder micro-benchmark (criterion)

The streaming chunked decoder de-frames `aws-chunked` upload bodies. ARCH 30.1 wants the byte
path bound by hardware, not by framing CPU, so the de-framer must run well above device bandwidth.

`crates/cairn-protocol/benches/decode.rs` decodes an 8 MiB unsigned body in 64 KiB chunks:

```sh
cargo bench -p cairn-protocol --bench decode
```

**Observed** (this host):

```
chunked_decode/8MiB_64KiB_chunks
    time:   [7.7463 ms  8.0061 ms  8.3208 ms]
    thrpt:  [961.44 MiB/s  999.23 MiB/s  1.0085 GiB/s]
```

So **~1 GiB/s** through the de-framer. That is comfortably above the large-object PUT bandwidth a
single durable-commit write path sustains (see below), confirming the de-framer is not the ingest
bottleneck — exactly the 30.1/29.6 expectation.

---

## 2. Macro load profiles (boto3 concurrent harness)

### How to run

`conformance/load_profile.sh` bootstraps a fresh temp store, starts `cairn serve`, runs the
profiles, prints the report, and tears everything down. It exits non-zero on any error.

```sh
# default profile (~1-2 min): 512 MiB large-object phase + 5000 small-object PUTs
BIN=target/debug/cairn PY=/tmp/cairnvenv/bin/python conformance/load_profile.sh

# smoke run (smaller sizes/counts)
QUICK=1 BIN=target/debug/cairn PY=/tmp/cairnvenv/bin/python conformance/load_profile.sh
```

`PY` must point at a Python with `boto3` importable (here `/tmp/cairnvenv/bin/python`). To drive
an already-running server directly:

```sh
/tmp/cairnvenv/bin/python conformance/load_profile.py ACCESS_KEY SECRET_KEY http://127.0.0.1:9081
```

### What it measures

**Profile (a) — large-object bandwidth.** N concurrent workers each PUT then GET a large object
(default 32 MiB, 16 objects, 8 workers). Reports aggregate up/down MiB/s. Per ARCH 30.1 this is
bound by disk and network bandwidth plus the two durable-commit fsyncs (fsync file, fsync dir),
**not** by the writer — every worker stages and fsyncs its own blob in parallel before the short
metadata commit.

**Profile (b) — small-object rate.** N concurrent workers PUT many 4 KiB objects. Reports ops/s
and p50/p99/p999 PUT latency, swept across concurrency **1, 4, 16**. Per ARCH 30.1 the binding
constraint here is the staging fsync and the directory barrier; the writer bounds conditional and
control-plane writes. The sweep makes the ceiling visible.

Between phases the harness GETs `/metrics` and extracts the `cairn_*` gauges and the
`cairn_request_duration_seconds` summary, so the server's own view sits alongside the client-side
numbers.

### Observed results (default profile, this host)

> Historical: recorded before storage protocol 3, kept as the shape of the harness output. §8 has
> the current numbers.

**Profile (a) — large-object bandwidth** (16 × 32 MiB = 512 MiB, 8 concurrent workers):

| direction | throughput | wall |
|-----------|-----------:|-----:|
| PUT (up)  | **59.6 MiB/s** | 8.60 s |
| GET (down)| **1152.5 MiB/s** | 0.44 s |

GET is served from page cache at memory speed (the objects were just written), so it reflects the
zero-copy-ish read path rather than cold disk; PUT carries the full durable-commit cost (stage,
fsync file, rename, fsync dir, metadata commit) and lands near the host's synchronous sequential
write rate. Download integrity (every byte read back equals every byte written) was verified `OK`.

**Profile (b) — small-object rate (4 KiB PUTs), concurrency sweep:**

| concurrency | objects | ops/s | p50 | p99 | p999 |
|------------:|--------:|------:|----:|----:|-----:|
| 1  |  500 | **165.6** |  5.31 ms |  13.76 ms |  18.31 ms |
| 4  | 1500 | **279.1** | 13.67 ms |  27.15 ms |  50.38 ms |
| 16 | 3000 | **275.4** | 53.49 ms | 136.22 ms | 198.24 ms |

`cairn_*` gauges after the sweep: `cairn_objects` 4784, `cairn_versions` 4784,
`cairn_logical_bytes` ≈ 556 MB, `cairn_physical_bytes` ≈ 556 MB (compression ratio 1.0 — the
incompressible-ish payload is stored raw, as expected). The server-side
`cairn_request_duration_seconds` PUT summary tracked the client view (p50 ≈ 20 ms cumulative,
with multi-second p999 outliers from the cold-start window — see below).

### Interpreting the single-writer ceiling (ARCH 8.3 / 30.2)

> Historical: these figures describe the writer-bound regime that storage protocol 3 replaced. The
> current figures are in §8.

The defining number: **concurrency 1 → 16 (16×) moved ops/s only 165.6 → 275.4 (1.66×) while p999
latency grew 18.3 ms → 198.2 ms (10.8×).** That is the single-writer ceiling, made visible.

Why it happens, per the spec:

- A **single group-committing writer** owns the one write connection (ARCH 7.2/11.6). Every
  conditional, quota-governed or Object-Locked PUT must pass through it; an unconditional PUT does
  not.
- **Group commit** (ARCH 8.3) coalesces the mutations that arrive during one durability barrier
  into a single COMMIT and a single fsync, then acks every member of the batch after that barrier.
  This is why ops/s *rises at all* as concurrency goes 1 → 4: more concurrent arrivals mean a
  larger batch factor, so the effective rate climbs above the bare synchronous-commit rate while
  per-write durability is preserved.
- But the writer + fsync rate is a hard ceiling. Once concurrency saturates the batch (here by
  ~4 workers), adding more concurrency (16) does **not** raise ops/s further — it just deepens the
  queue of PUTs waiting behind the in-flight batch, which is exactly why p50/p99/p999 climb
  steeply (5 → 53 ms p50; 18 → 198 ms p999) while throughput plateaus.

This is the 30.1 prediction stated plainly in 30.2: **ops/s scaling well below the concurrency
multiple, together with growing tail latency, is the operator-visible signature that the single
writer and the fsync rate are the binding constraint** for small-object writes. The operator
levers 30.2 lists follow directly from this curve: enlarge the group-commit linger (bigger
batches, a little more latency), relax the `synchronous`/blob-fsync durability setting if the
workload tolerates it (cheaper barrier), or accept the ceiling as the honest limit of one node and
scale out with replication.

#### A note on the multi-second p999 at concurrency 1

The concurrency-1 run shows a PUT p999 of several seconds in the server-side summary even though
its client-side p999 is 18 ms. That tail is the **cold-start window**: the first writes pay
one-time costs (initial WAL growth, the first checkpoint, page-cache warming) that a freshly
bootstrapped store incurs before steady state. It is a startup artifact, not the steady-state
small-write latency; the steady-state picture is the monotonic p50/p99 growth across the sweep.

#### The write-queue-depth gauge

ARCH 26/30.2 names the **write-queue-depth** metric as *the* server-side window onto this
ceiling — a depth that grows under load is the early-warning signal for write saturation before it
becomes latency. It is published as the `cairn_writer_queue_depth` gauge (alongside
`cairn_requests_total`, `cairn_request_duration_seconds`, the store/byte gauges, and the
WAL/replication series). This harness reads the ceiling from three windows: that gauge, the
server-side `cairn_request_duration_seconds` summary, and the client-side
tail-latency-versus-concurrency curve above. All show the same thing — the writer saturating —
which is what 30.2 asks an operator to be able to see.

### Caveats

- Numbers are host- and load-shape-dependent; the harness fixes the *method*, not the figures.
- Large-object GET reads back just-written objects, so it measures the page-cache-warm read path.
- The payload is a cheap LCG fill chosen to be not-trivially-compressible, so throughput reflects
  real bytes moved rather than the compressor's view of a run of zeros (hence compression ratio
  1.0 in the gauges).
- `--quick` / `QUICK=1` shrinks the profile for a fast smoke check; use the default profile for a
  representative ceiling curve.

---

## 3. Real MinIO `warp` macro benchmark

`conformance/warp.sh` downloads the upstream MinIO **`warp`** binary (the canonical S3 macro
benchmark) from `github.com/minio/warp/releases`, bootstraps a throwaway Cairn the same env-only
way `conformance/run.sh` does, creates a bucket, and runs `warp put`, `warp get`, and `warp mixed`
against it with path-style addressing, then tears everything down. It finishes in ~2-3 minutes at
the default (modest) size/duration.

### How to run

```sh
# default: 1 MiB objects, concurrency 4, 20s per phase
BIN=target/debug/cairn conformance/warp.sh

# tune the load; reuse an installed warp instead of downloading
WARP=/usr/local/bin/warp DURATION=30s OBJ_SIZE=512KiB CONCURRENT=8 conformance/warp.sh
```

The script pins **warp v1.0.0** — the GitHub releases assets use un-versioned names
(`warp_Linux_x86_64.tar.gz`), and v1.5.0+ ship only on `dl.min.io`, so v1.0.0 is the newest
tarball actually hosted on the GitHub releases page and gives a stable, scriptable URL. warp
v1.0.0 auto-detects path-style addressing for an `IP:port` `--host`, so no `--path-style`/`--lookup`
flag is needed (and v1.0.0 does not accept one; newer warp would take `--lookup path`).

### How `warp` surfaced (and we fixed) a real SigV4 bug

Running `warp` **uncovered a real server bug** and then verified its fix. `warp`'s object-name
generator deliberately puts `(` and `)` in keys (alphabet `…1234567890()`) to stress URL-encoding.
Cairn's SigV4 canonical request was **double-encoding** an already-percent-encoded request path: the
server canonicalized `req.uri().path()` (already `…/with%28paren%29`) by running `uri_encode` over it
again, turning `%28` into `%2528`, so **every key with a reserved sub-delim** (`(`, `)`, space, …)
failed `SignatureDoesNotMatch` — reproducible with plain boto3 (`Key="a(1).rnd"` → fail). **Fixed**
in `cairn-auth` (`sigv4.rs`): the canonical URI now decodes the wire path and encodes exactly once,
`uri_encode(percent_decode(path))` (regression test in `crypto_util.rs`; verified via boto3 across
`()`, spaces, `+`, and unicode keys). `warp.sh` now runs get/put/mixed **strict** — any operation
error fails the run.

### Observed results (this host, all phases error-free)

1 MiB objects, concurrency 4, 8s (CI uses 20s):

| phase | average throughput |
|-------|------:|
| `warp put` | **~38 MiB/s** (38 obj/s) |
| `warp get` | **~450 MiB/s** (read path, cached) |
| `warp mixed` | **~98 MiB/s** total (163 obj/s) |
| errors | **0** |

These `warp put` figures sit in the same band as the boto3 2(a) large-object PUT path once you
account for object size and concurrency — both are bounded by the durable-commit write cost, not
by client framing — so `warp` corroborates the boto3 harness where it can run.

---

## 4. Multi-host replication soak

`conformance/soak.sh` stands up **two** Cairn nodes and exercises asynchronous bucket replication
(ARCH 20) under sustained load while watching for two failure modes the spec cares about:
replication *correctness* (every replicated object is byte-identical) and a *memory leak* on the
busy source.

Topology (the single-target node→node shape from `docs/operations.md` 2):

- **node-1 = replication TARGET** — a plain Cairn mirror.
- **node-2 = SOURCE** — started with `CAIRN_REPLICATION_ENDPOINT` pointed at node-1, so its
  replication worker ships the source bucket's versions to node-1. (Each node's `bootstrap` and
  `serve` share one master key, or the sealed SigV4 secret cannot be unsealed at serve time.)

The boto3 driver (`conformance/soak.py`, run with the `/tmp/cairnvenv` python) enables versioning
+ an enabled replication rule on the source bucket (replication requires both, ARCH 20), then for
`DURATION` seconds:

- runs a continuous multi-worker PUT workload against the **source** (the soak focuses on
  replication and durability under sustained load);
- every few seconds reads a random sample of already-PUT objects back from the **target** and
  compares them **byte-for-byte** against what was written — any mismatch (or non-arrival past a
  grace window) is counted;
- samples the **source** process RSS (`/proc/<pid>/status` `VmRSS`) throughout and, after dropping
  the warm-up window, checks steady-state RSS did not grow past a threshold (default 50%).

It exits non-zero unless replication mismatches are **0** and the source RSS stayed flat.

### How to run

```sh
# default DURATION=120 (the CI value)
BIN=target/debug/cairn PY=/tmp/cairnvenv/bin/python conformance/soak.sh

# shorter local run
DURATION=60 BIN=target/debug/cairn PY=/tmp/cairnvenv/bin/python conformance/soak.sh
```

### Observed results (this host, `DURATION=60`)

| metric | value |
|--------|------:|
| source PUTs (60s) | **5023** (0 errors) |
| objects verified byte-identical on the target | **93** |
| replication mismatches | **0** |
| source RSS (steady state) | **23.8 MiB → 26.3 MiB (+10.6%)**, under the 50% leak threshold |

Verdict: **PASS** — replication landed every sampled object byte-for-byte (0 mismatches), and the
source's resident set stayed essentially flat across ~5k PUTs (the small rise is pool/cache warm-up,
not a monotonic climb). At the CI default `DURATION=120` the PUT and verified counts roughly double
while the RSS picture is unchanged.

## 5. Metadata sharding (the write-throughput escape valve)

A single Cairn store commits through **one** group-committing writer (ARCH 8.3 / 30.2) — the binding
ceiling for small-object, write-heavy load. `CAIRN_META_SHARDS=N` partitions the metadata across N
databases by bucket name (`shard.rs`), so writes to **disjoint buckets** commit through N parallel
single-writers. It is an init-locked, one-time decision with a real trade-off (user quota becomes
eventually-consistent; a single hot bucket still lands on one shard). The decision guide is
[`scaling-limits.md`](./scaling-limits.md) §3; this is the supporting measurement.

### How to run

```sh
# from the repo root (an ext4 working dir, NOT a tmpfs) — release mode
BENCH_SECS=4 BENCH_SHARDS="1,2,4,8" cargo run --release --example bench_sharded -p cairn-meta
```

The example spreads concurrent `PutObjectVersion`s across many buckets through the
`ShardedMetadataStore` and exercises the full write path (routing → quota check → upsert → roll-up
counters), reporting puts/s and the speedup vs the single-shard baseline.

### Observed results (this host — **2 cores**, indicative only)

| shards | puts/s | speedup |
|-------:|-------:|--------:|
| 1 | 7488 | 1.00× |
| 2 | 9853 | 1.32× |
| 4 | 10474 | 1.40× |
| 8 | 8170 | 1.09× |

Read the **shape**, not the absolute multiplier: sharding lifts write throughput because disjoint
buckets no longer serialize on one writer, but the parallelism is bounded by **CPU cores** — on this
2-core box it saturates near 2× and then *regresses at 8 shards* (more writer threads than cores =
contention). On a real multi-core host the curve scales much further (toward near-linear up to the
core count for disjoint-bucket load); the operational lesson that travels is **do not configure more
shards than you have cores**, and shard only when load actually spreads across many buckets. Re-measure
on your hardware before committing — sharding is init-locked and cannot be changed without a rebuild
([`upgrade-rollback.md`](./upgrade-rollback.md) §4).

## 6. Stress + stability harness (`conformance/stress.sh`)

The single **"is it still fast AND stable?"** check to run after a change. It drives `warp` against a
throwaway Cairn and, in one pass, reports peak write/read/mixed throughput, ramps concurrency to prove
the server bends-not-breaks past the single-writer ceiling, and samples the **server process** for a
leak/stability verdict. Unlike `warp.sh` (which only gates on the error count) it parses warp's
throughput numbers and watches RSS + the writer queue, so the output is a benchmark *and* an assertion.

### How to run

```sh
cargo build --release --bin cairn
BIN=target/release/cairn conformance/stress.sh                 # default profile (~4 min)
CONCURRENT=16 OBJ_SIZE=4MiB DURATION=15s conformance/stress.sh  # tune the load
STRESS_OUT=/tmp/base.json conformance/stress.sh                 # write a results JSON
BASELINE=/tmp/base.json   conformance/stress.sh                 # compare vs a prior run (regression warn)
```

### What it asserts (the PASS/FAIL verdict)

- **Zero operation errors** across every warp phase (put/get/mixed) and every escalation level.
- **Liveness** — the server answers `/healthz` after the full concurrency ramp.
- **No runaway memory** — peak RSS stays under `RSS_CEILING_KIB` (default 1 GiB), the hard gate: a
  real leak grows with request count and blows unbounded past it, while Cairn's byte-budgeted caches
  plateau well under. Steady-state RSS growth (last third vs middle third, cold-start excluded) is
  reported and warned above `LEAK_PCT`, but is **advisory, not fatal** — on fast hardware the cache
  fills faster than a short run can plateau, so a high % is usually warm-up, not a leak (e.g. a 4-core
  runner showed 60% growth while RSS peaked at 168 MiB then *fell* to 119 MiB — clearly not leaking).
  Sensitive leak detection is the job of the long-running `soak.sh`.

### Observed results (this host — **2 cores**, indicative only)

| phase | obj/s | MiB/s |
|-------|------:|------:|
| WRITE (PUT, 1 MiB, conc 8) | ~55 | ~55 |
| READ (GET, 1 MiB, conc 8) | ~50 | ~50 |
| MIXED (get/put/stat/delete, 1 MiB, conc 8) | ~211 | ~126 |

On a **4-core CI runner** (closer to dedicated hardware) the same harness measured ~97 WRITE, **~1760
READ**, ~580 MIXED obj/s, and **~1100 obj/s** under the 64 KiB write ramp to concurrency 256 — 45k
requests, **zero errors**, writer-queue peak **1**, RSS peaking ~168 MiB then settling. Throughput
scales with cores once `warp` is not starved, and the writer is not the bottleneck at this scale.

Escalation (64 KiB writes, concurrency 4→64): alive with **zero errors at every level**; throughput
stays in the ~240–700 obj/s band (noisy, not a clean monotonic plateau, because `warp` and `cairn`
contend for the same 2 cores). RSS peaked at **~63 MiB** and settled lower (no leak); the writer queue
depth peaked in the single-to-low-double digits — i.e. on this box the bottleneck is **CPU contention
with the load generator, not Cairn's single writer**. Read the **shape and the zero-error/RSS-stability
signals**, not the absolute obj/s: warp eats a core here, so these are a floor. Re-measure on real,
dedicated hardware (e.g. the arm64 testbed) for representative throughput.

> **On the `BASELINE` regression check:** run-to-run variance on a shared/contended box is large
> (≈±30% observed here — e.g. READ swung 50→38 obj/s between back-to-back runs with no code change), so
> the default 20% regression threshold (`REGRESS_PCT`) will false-positive on this hardware. The check
> is only trustworthy on a **dedicated** machine, or when averaging several runs; on a noisy box raise
> `REGRESS_PCT` or treat single-run swings as noise. The error-count / liveness / RSS-leak assertions
> are robust regardless and are what the PASS/FAIL verdict hinges on.

## 7. Head-to-head vs MinIO (`conformance/bench_compare.sh`, manual)

Answers one question on demand: *for each S3 operation, how does Cairn compare to MinIO on the same
machine?* The harness boots **both** servers on one host — Cairn from the built binary, MinIO from a
**pinned** release binary (`RELEASE.2025-09-07T16-13-09Z`, `dl.min.io`) — single-node/single-drive,
plaintext HTTP, and drives an identical `warp` v1.0.0 matrix against each **sequentially** (only one
server under load at a time; `warp` itself burns ~1 core). Default matrix (~15-20 min): `PUT`/`GET` at
4 KiB + 8 MiB, and `STAT`/`DELETE`/`LIST`/`MIXED` at one representative size. Env-tunable (`DURATION`,
`CONCURRENT`, `REPEATS`, and `CELLS_ENV` to replace the whole matrix); a longer run uses `REPEATS=3`
for a median. The encrypted 2 GiB/5 GiB replication regression remains in full PR validation.

Output: a **job-summary markdown table** plus `bench.csv` / `bench.json` for over-time tracking. The
parser reads the **measured** operation, not warp's prepare-PUT (a subtle trap: `get`/`stat`/`delete`
upload first, so the first `* Average:` line is the prepare, not the result).

**Report, not gate.** The meaningful signal is the **Cairn/MinIO ratio per op**, not the absolute
obj/s — the run-to-run variance documented in §6 means a throughput gate would false-positive
constantly. The script therefore **fails only on warp operation errors** (a correctness/liveness
signal robust to noise), emits a non-fatal ⚠️ if Cairn is >2.5× slower on an op, and never fails on
who is faster. Compare ratios across runs, not obj/s.

> **Baseline (local 2-core VM, 2026-07):** reproducible signals were MinIO faster at reads/STAT,
> DELETE (~3×), and LIST; Cairn faster at small PUT; mixed/medium within noise. The LIST result was
> initially a warp interop failure (empty `delimiter=` handled as a real delimiter) — since fixed;
> `bench_compare.sh` now measures LIST on both.

## io_uring blob path (`--features io-uring`, experimental, Linux-only)

The staging write path can run through a dedicated `tokio-uring` reactor (off by default). Measured
in release on this NVMe sandbox (`CAIRN_URING_THREADS=4`), staging 1 MiB / 256 KiB objects:

| workload | io_uring | tokio::fs (epoll) | delta |
|---|---:|---:|---:|
| **concurrent** (32 workers × 16 × 256 KiB) | **398 MiB/s** | 382 MiB/s | **+4.2%** |
| serial (200 × 1 MiB, fsync-bound loop) | 162 MiB/s | 282 MiB/s | **−42.5%** |

**Interpretation (honest):** io_uring wins under concurrency (overlapped submission/fsync), which is
the realistic server workload — but the current implementation bridges each file op to the reactor
over a channel, and that per-op hop dominates a serial fsync-bound loop (the worst case), making it
*slower* there. So the feature is appropriately experimental and off by default; a clear across-the-
board win would require batching submissions and removing the per-chunk bridge hop. Both probes are
`#[ignore]` tests in `crates/cairn-blob/tests/blob.rs` (`uring_vs_epoll_*`).

## sendfile fast path (`--features fast-io`, experimental, Linux-only)

The plaintext HTTP/1.1 object-GET fast path (`crates/cairn-server/src/fast_get.rs`) serves a
committed, **uncompressed, unencrypted** object — the full object or a single byte-range — directly
from the page cache to the socket with a single `sendfile(2)`, bypassing hyper's userspace body copy.
Any ineligible request (compressed/encrypted at rest, HTTP/2, TLS, conditional, multi-range, or a
body below `CAIRN_FASTIO_MIN_BYTES`) falls back to the unchanged streamed path byte-for-byte. The win
is **server CPU per GiB sent**, not latency, so the measurement is an A/B of CPU-seconds/GiB at equal
throughput.

The fast path is a small Cairn-owned **HTTP/1.1 keep-alive loop** on the plaintext data-plane
listener: per connection it serves each eligible large GET with one `sendfile(2)` and keeps the
connection open for the next request, handing the connection to hyper (with the already-read bytes
replayed) when it hits a request it cannot serve. **That hand-off is permanent for that connection.**
So the loop engages on every eligible GET of a connection *it still owns* — but a connection whose
**first** request is not an eligible GET is lost to hyper for its whole lifetime, and every later GET
on it bypasses the fast path. The size floor (`CAIRN_FASTIO_MIN_BYTES`, default 256 KiB) still keeps
small objects on the streamed path, where the per-request `sendfile` setup would outweigh the
zero-copy saving.

**This is the load-bearing limitation, and it is why the fast path is still off by default.** A
pooled S3 client (warp, boto3, aws-sdk) opens a few connections, and the first request on each is a
bucket-create, a prepare PUT or a HEAD — never a large GET. Every one of those connections is handed
to hyper at request one and then reused for the entire GET phase, so the fast path serves nothing.
See the measurements below.

### Engagement: measured, and still 0% under a pooled client

The original design **peeked only the first request** of a fresh TCP connection, then force-closed or
handed the socket to hyper. Measured on x86_64 and a real aarch64 Pi (kernel 7.0) under
`warp get --concurrent 4`:

| run | GETs served | served via `sendfile` | engage |
|---|---:|---:|---:|
| x86_64, fast-io (peek-first, old) | 1703 | 0 | **0%** |
| aarch64, fast-io (peek-first, old) | 800 | 0 | **0%** |
| x86_64, fast-io (keep-alive loop, 2026-07) | 8 MiB × 20 s | 0 | **0%** |
| x86_64, fast-io (keep-alive loop) — `curl`, one connection, first request a GET | 20 | 20 | **100%** |

The keep-alive rewrite (`8265689`) lets the loop serve **every** eligible GET on a connection it still
owns — the 100% row, which `conformance/sendfile_keepalive.sh` gates deterministically. **It did not
change the pooled-client outcome.** Re-measured 2026-07 with the keep-alive loop and
`CAIRN_FASTIO_MIN_BYTES=0`, `warp get` still engages **0%**: the counters show only
`cairn_sendfile_fallback_total{reason="not_object"}=3` and `{reason="ineligible"}=3` — six connections,
each handed to hyper on its *first* request (bucket-create, prepare PUT), then reused for the whole
GET phase. `cairn_sendfile_get_total` never appears at all.

So the two tests measure genuinely different things, and only one of them resembles a real client:
`curl` issues an eligible GET as the connection's first request, which the loop keeps; a pooled SDK
never does. **The `sendfile` fast path therefore delivers no measurable benefit to any pooled S3
client today**, which is why it remains off by default and is not shipped in the release binaries.

A CPU-per-GiB A/B is *undefined* while engagement is 0% — there is nothing on the fast path to
measure. (`sendfile_bench.sh` correctly reports "insufficient data for a ratio".) Closing this gap
requires keeping a connection in the loop *after* an ineligible request — i.e. replaying to hyper for
one request and taking the connection back — which is a substantially larger change than the current
loop and is **not** planned.

> **Build note:** `fast-io` is **glibc-Linux-only**. The `ktls` dependency does not cross-compile to
> `aarch64-unknown-linux-musl` (a `cmsghdr`/`msghdr` struct-layout mismatch), so build the fast-io
> path for `aarch64-unknown-linux-gnu` (or x86_64). The shipped release binaries are default-features
> (no `fast-io`), so this does not affect releases. On a kernel where the kTLS ULP probe fails (seen on
> kernel 7.0), TLS connections use userspace rustls. A per-connection offload failure after a
> successful probe closes that connection; it cannot safely resume the consumed rustls stream.
> kTLS record encryption is implemented, but it does not make this plaintext sendfile benchmark
> a measurement of zero-copy HTTPS.

### How to run

`conformance/sendfile_bench.sh` drives a GET-heavy `warp` load, samples the server process's CPU
time (utime+stime from `/proc/<pid>/stat`) and the new sendfile counters from `/metrics` across the
GET phase, and reports CPU/GiB plus the **engage rate** (zero-copy GETs vs fall-backs). Build both
arms and run the A/B:

```sh
cargo build --release --features fast-io --bin cairn && cp target/release/cairn /tmp/cairn-fastio
cargo build --release                    --bin cairn && cp target/release/cairn /tmp/cairn-base
BIN=/tmp/cairn-fastio BASELINE_BIN=/tmp/cairn-base OBJ_SIZE=64MiB DURATION=30s \
  conformance/sendfile_bench.sh
```

### What it measures / caveats

- **`cairn_sendfile_get_total{result,transport}`** — zero-copy GETs served (ok/error, `transport=plain`).
- **`cairn_sendfile_fallback_total{reason}`** — why the fast path declined (`head`, `parse`,
  `ineligible`, `not_object`, `denied`, `not_zerocopy`, `below_floor`) — the engage-rate denominator.
- The objects must be **uncompressed at rest** to engage; the harness uses incompressible random
  bodies, but if the bucket has compression enabled the engage rate is ~0 and the report says so.
- A trustworthy absolute number needs real hardware; on a small/shared box the **A/B ratio** and the
  engage rate are the durable signals. The CPU window brackets only the GET phase (objects are
  pre-staged outside it), so PUT/compression cost does not contaminate the read measurement.
- This path is **plaintext only**; zero-copy over HTTPS needs the unimplemented sendfile-to-kTLS integration, so
  benchmark over `http://`, not `https://`.

## Maintained visible counts: bounded query comparison (2026-09-08)

An in-memory SQLite 3.46.1 comparison used 100,000 current object rows across 1,000 buckets
(24,805,376 database bytes), with eleven warm repetitions per query. Scan and maintained-counter
results matched exactly. These are query costs, excluding API authorization, connection-pool waits,
writer contention, network, and browser rendering.

| Query | Old scan median ms | Maintained counter median ms |
| --- | ---: | ---: |
| Global visible objects | 6.5381 | 0.0620 |
| Per-bucket visible objects | 11.5506 | 0.5523 |

Query plans changed from scanning the current-object covering index to reading `bucket_stats`.
This supports the removal of per-refresh object-cardinality work; it does not establish production
request throughput. The one-time migration still scans current metadata, and writes now maintain
visibility deltas transactionally. No object payloads or persistent test database were created.

## Multipart buffer reuse: bounded comparison (2026-09-08)

A focused A/B/A check compared the per-part 64-KiB allocation against the reused buffer in
`8a5d2ad`. Only buffer placement changed between arms; both retained the new timing instrumentation.
This used Rust 1.97.1's unoptimized test profile on the shared development host and its `/SSD`
ext4 filesystem, not `/tmp` (which is tmpfs). A preliminary tmpfs check was excluded.

Each arm staged 256 plaintext parts of 4 KiB, with part `n` containing byte `n % 251`, then ran
one warm-up and eight sequential durable 1-MiB assemblies. Fixtures and outputs were uncompressed
and unencrypted. Every measured output's logical length, MD5, internal SHA-256 and CRC64NVME
matched a single-object ingest of the concatenated bytes. Output deletion followed each timed
assembly; no server, network, or metadata database participated.

| Arm | Mean assembly ms | Median ms | Min–max ms | Process CPU seconds, eight iterations | Process write bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1: per-part buffer | 109.37 | 107.31 | 102.32–121.98 | 0.73 | 8,388,608 |
| B: reused buffer | 128.61 | 130.55 | 104.94–143.75 | 0.74 | 8,388,608 |
| A2: per-part buffer | 118.10 | 116.09 | 101.91–155.06 | 0.77 | 8,388,608 |

The bounded check **does not demonstrate a latency or CPU improvement**: B was slower in wall
time, controls varied, and ranges overlap. CPU came from `/proc/self/stat` at 100 ticks/second;
CPU and `/proc/self/io` windows include verification and output deletion, while per-assembly wall
times exclude them. Process write accounting excludes previously staged parts and is not a
measurement of total device traffic or complete multipart write amplification.

The code change reduces this fixture's explicit plaintext buffer allocations from 256 to one
per assembly (16 MiB to 64 KiB of requested zero-initialization); allocator calls, peak RSS and
production throughput were not profiled. No reduced disk-write claim follows. The three measured
arms took under five seconds including fixture setup, with a 1-MiB input per arm. Temporary test
code and data were removed after the comparison; this table retains the results without adding a
permanent benchmark or repeating the large load campaign.


## Bidirectional replication and compressed allocation (2026-09-09)

A one-off scale exercise tested release build `000eeb0da612d12ab59dd45bb307b68e68cddcc5`
(binary SHA-256 `1eaaa17cb4091e8701a239ffe6648069d2b6d127d353b731a37de891da704b9d`).
Two processes shared the development host's CPU and ext4 SSD, using separate data/control ports,
independent databases and master keys, SQLite FULL durability, four replication workers per node,
a one-second replication interval and 16 client workers. This is not an isolated-host, WAN or
MinIO comparison, and the console's responsiveness was not tested.

The workload wrote **50,000,000,000 unique logical bytes**, targeting **100,000,000,000 mirrored
logical bytes**: 120,000 small originals (4/16/64 KiB) and 466 large originals, principally
1/32/256 MiB. Compression-off, Zstd and LZ4 buckets used 256-KiB blocks and bidirectional rules.
Initial keys had disjoint origin prefixes. Synthetic content repeated deterministic random or
half-zero blocks, with per-object/per-MiB identity tags; it was not independent random entropy
for every byte. Simultaneous conflicting writes to the same key were not tested.

The fixed run completed within a 60-minute runtime and 100-GB allocated-space budget, with
12 GiB reserved for in-flight work and shutdown. A preliminary port preflight stopped before any
server or object write; the workload then ran once on a fresh port pair. Both nodes shut down
cleanly after verification.

| Result | Measurement |
|---|---:|
| Original data objects / unique bytes | 120,466 / 50,000,000,000 |
| Full HTTP version reads, both nodes | 241,124 |
| Full-body SHA-256 verified bytes, including extra versions | 100,007,768,056 |
| Content mismatches / terminal replication failures | 0 / 0 |
| Final pending / claimed / failed, each node | 0 / 0 / 0 |
| Original population time | 819.785 s |
| Additional baseline convergence time | 735.765 s |
| Full readback and client SHA-256 verification | 850.682 s |
| Entire exercise, including shutdown | 2,542.389 s (42.37 min) |
| Highest sampled whole-run allocation | 59,091,075,072 B |
| Whole-run allocation after shutdown | 57,033,072,640 B |

After baseline convergence, 60 opposite-origin overwrites and 30 delete markers propagated.
One populated peer was stopped while the survivor accepted 18 new objects; catch-up after restart
took 34.813 seconds including the convergence observation window. Another 18 writes propagated
in the reverse direction. Exact current listings, complete version/marker sets, latest-version
flags, marker 404 identities and full bodies matched on both nodes. The final outboxes contained
only the expected 60,298 / 60,294 originating operations, with no replica-origin cascade.
Both metadata databases passed `quick_check`.

### Allocation fix and existing data

Default staging previously retained unwritten preallocation beyond compressed EOF. The fix trims
only to actual descriptor EOF after buffered writes drain and before the existing durability sync;
errors prevent publication and queued jobs retain their I/O ownership (ARCH 7.5, 8.2).
An old-binary fixture stored an 8-MiB logical payload in a 1,052,330-byte file occupying 8,388,608
allocated bytes. An ordinary S3 replacement with the fixed binary had the same file length but
occupied 1,052,672 bytes. Four full readbacks matched; deleting the exact old version reclaimed
its inode through normal cleanup. Upgrading alone does not reclaim old versions' excess allocation;
see [troubleshooting](./troubleshooting.md#compressed-file-length-is-small-but-disk-allocation-remains-large).

Across all 241,124 retained data-version files, EOF lengths summed to 55,931,733,730 bytes and
allocated blocks to 56,572,760,064 bytes. Every file matched its metadata length, and the maximum
allocation above any file's EOF was 7,904 bytes. The earlier incomplete run had about 34 GB of
allocation above EOF. Its different replica population prevents a like-for-like whole-run speed
comparison; no throughput improvement from this fix is claimed.

### Throughput, latency and observation limits

Full readback plus hashing averaged **117.6 MB/s** across the mixed workload. During small-object
readback, ten non-overlapping one-minute windows measured **370–386 requests/s**. The sampled
large-read interval moved about 93.1 GB in 217.2 seconds, approximately **429 MB/s**. These are
client-observed throughput figures with 16 workers, not individual request latency.

The driver did **not** retain per-GET elapsed times or time to first byte, so GET p50/p95/p99 and
consistent tail latency are unproven. Small-upload SDK timings, while replication was active,
were approximately 43 ms median, 95 ms p95 and 150–152 ms p99 across the three compression
settings; these include SDK overhead and exclude separately timed input-hash preparation.

Sampled process RSS peaked at 1,166.6 / 1,069.0 MiB; WAL file lengths at 985,578,192 / 730,743,832
bytes. Pending lag peaked at 359 / 443 seconds. Pending age excludes claimed work, so zero pending
lag alone does not establish convergence. These observations do not establish leak freedom or
attribute a CPU/SQLite bottleneck. No hardware-specific throughput or latency SLO follows.

There were 1,973 resource samples. Four `du` samples encountered the driver's transient SQLite
journal after it disappeared; the final quiescent disk measurement succeeded. An auxiliary raw
`compressed_files` counter counted non-null descriptors, including `Uncompressed`; analysis
corrected it to 80,374 per node from parsed descriptors. It was not used by any pass/fail check.
Raw run artifacts and the one-off driver are retained with the operator's test data, outside Git;
this record does not add that driver to the supported CI harnesses.

The fix also passed the full local gate: 1,470 workspace tests (5 skipped), doctests, both Clippy
variants, formatting, Rust/npm audits, console lint/build and installer checks. Regression coverage
includes trim failure, cancellation ownership and plaintext/encrypted single-object/multipart
containers across the three compression settings.

## 8. Protocol 3 head-to-head vs RustFS

Measured 2026-10-30 on fr38 with strict durability, interleaved same-session pairs through
`warp` (`conformance/warp.sh`) in 23-second arms: Cairn and RustFS take turns on the same host so
that a load swing hits both. The host is shared and swings ±25% run to run, so the ratio is the
number to trust, not the absolute throughput.

| Workload | main before | now | RustFS | Ratio |
|---|---:|---:|---:|---:|
| PUT 4 KiB, c32, host defaults | 1,700 obj/s | 5,062–5,153 (median 5,112) | 4,335–5,564 (median 4,646) | **1.10×** |
| PUT 4 KiB, c32, 8 pinned cores | — | 4,202 | 1,764 | **2.38×** |
| PUT 1 MiB, c16, host defaults | 366–417 obj/s | 418–480 | 423–637 | ~0.95× |
| PUT 1 MiB, c16, 8 pinned cores | — | 466 | 358 | **1.30×** |
| GET 1 MiB | — | — | — | **2.13×** |
| LIST 1,000 keys | — | — | — | **3.76×** |

CPU per 1 MiB object is now ≈40M cycles, parity with RustFS. It was 1.7× before streaming chunk
hashing moved from the pure-Rust `sha2` to `aws-lc-rs`, which is the change that closed it.

The unpinned 1 MiB number is dominated by whatever else runs on the host; the pinned ratio is the
deployment-relevant one, because it holds the core budget fixed the way a dedicated node does.

## 9. Local bare-metal scoreboard (2026-11-03)

Measured on the local 6-core i5-9400T / NVMe host with strict durability, Warp v1.8.2, and
interleaved Cairn/RustFS pairs. The host also runs the homelab services, so absolute numbers move
run to run; the ratio is the stable signal. These are single-node diagnostic runs, not a
durability-equivalence proof.

| Workload | Cairn | RustFS | Ratio |
|---|---:|---:|---:|
| PUT 1 MiB, c16 (5 pairs) | 236.7 obj/s | 196.7 obj/s | **1.20×** |
| Mixed 1 MiB, c16 (3 pairs) | 931.8 obj/s | 592.1 obj/s | **1.57×** |
| GET 4 KiB, c16 (3 pairs) | 14,781 obj/s | 13,299 obj/s | **1.11×** |
| HEAD 4 KiB, c16 (3 pairs) | 20,573 obj/s | 17,511 obj/s | **1.18×** |
| Multipart part GET, 10×5 MiB (3 pairs) | 1,059 obj/s | 388 obj/s | **2.73×** |
| Multipart upload, 10×5 MiB (3 pairs, after §14) | 25.5 obj/s | 23.7 obj/s | **1.08×** |
| DELETE 4 KiB, c16, batch 100† (3 pairs, 100k prefill) | 3,444 obj/s | 2,415 obj/s | **1.43×** |

Notes:

- † **The DELETE rows are not equal-durability comparisons.** RustFS 1.0.0 (commit `d47f54bf`) `strict` mode covers
  PUT/part/container/rename commit points, but its unversioned DELETE path
  (`crates/ecstore/src/disk/local.rs`: `delete_version_inner` →
  `delete_file_with_namespace_owner`/`move_to_trash_with_namespace_owner`) does not consult
  `effective_durability` and performs unsynced unlinks/renames. Cairn ACKs after a durable tombstone
  plus its co-committed audit row. Both the bulk 1.43× and the single-key ratio are therefore
  **as-configured throughput**, not a capacity comparison at equal durability. The versioned
  DELETE-marker arm below is the equal-durability measurement (RustFS strict syncs the marker
  `xl.meta`): **3.55×** over three interleaved pairs.
- DELETE now uses an exact pool: 100,000 × 4 KiB objects created by signed S3 PUTs at 32-way
  concurrency, then `warp delete --list-existing --objects 100000 --batch 100 --concurrent 16
  --duration 12s --noclear`. Both engines started from the same pool; the scored delete excluded
  the prefill, and every arm analyzed at least 9 seconds. Prefill took 85–95 s for Cairn and
  148–155 s for RustFS. Cairn stayed near 3.4k obj/s while RustFS fell across pairs (3,077 →
  2,320 → 1,849 obj/s), so the mean ratio is 1.43× and the per-pair ratios are 1.15, 1.47 and
  1.84; the spread is host-state sensitive. The method lives in `conformance/rustfs_compare.py`.
- Multipart upload was 0.59× over three pairs before #158; the part-hash fix raised the
  single-pair result to 0.69×. The assembly MD5 elision in #168 then measured 0.77× over three
  pairs (49.7 vs 64.4 obj/s; per-pair ratios 0.74, 0.81 and 0.76). The host is noisy, so treat the
  direction as the signal, not the third digit. The measured split and remaining I/O/durability
  work are in
  [`multipart-upload-performance-plan-2026-11.md`](multipart-upload-performance-plan-2026-11.md).
  A 2026-11-04 follow-up, measured before the section 11 completion pipeline, found the part
  handler itself already faster than RustFS's (p50 171 vs 180 ms) and attributed the remaining
  ratio to the per-upload `CompleteMultipartUpload` tail; see that plan's "Where the scored
  multipart-put ratio actually goes".
- **The multipart `obj/s` figures above are Warp `PUTPART` operation rates, not completed
  objects.** Each `PUTPART` is one 5 MiB part, so 49.7 parts/s = 248 MiB/s = **4.97 completed
  objects/s** (RustFS 64.4 parts/s = 6.44 objects/s). The ratio is unchanged while every object
  has ten parts, but the absolute object rate is one tenth of the displayed figure and the summary
  gives the completion tail no line of its own. Section 12 measures that tail directly with an
  exact completed-object count; see also
  [`multipart-followup-decisions-2026-11.md`](multipart-followup-decisions-2026-11.md). The
  current section 14 row is 25.48 vs 23.65 `PUTPART` ops/s = **2.55 vs 2.37 completed objects/s**,
  consistent with section 14.2's completed-object rates.
- GET 1 MiB (2.13×) and LIST 1,000 (3.76×) in Section 8 are the fr38 measurements and were not
  re-run on this host.

### Single-key DELETE — separate measurement, not the bulk ratio

The `DELETE 4 KiB` row above is a Warp multi-object delete (`--batch 100`); it does not describe a
client that sends one DELETE per key. `conformance/delete_single_key.py` measures that shape
separately. For each arm it starts a fresh single-node store, creates exactly 12,000 distinct
4 KiB keys through signed S3 PUTs, then issues exactly one signed `DELETE /bucket/key` per key at
16-way client concurrency. The scored interval is the full pool depletion; prefill is timed but
excluded. An arm passes only when all 12,000 DELETEs return 200/204, the driver records no error,
HEAD of the first/middle/last key returns 404, a bounded `list-type=2` request proves the bucket is
empty, and (for RustFS) the bucket-durability readback is `strict`. The driver records client CPU
and the exact pool count. Cairn pins `CAIRN_META_SYNCHRONOUS=full`, `CAIRN_META_SHARDS=1` and
`CAIRN_META_GROUP_COMMIT_LINGER_MICROS=0`; RustFS uses `RUSTFS_DURABILITY_MODE=strict`,
`RUSTFS_NEW_BUCKET_DURABILITY_MODE=strict` and `RUSTFS_DRIVE_SYNC_ENABLE=true`, the same settings
as `rustfs_compare.py`. As the note above explains, that RustFS mode does **not** fsync the
unversioned delete path; the single-key ratio is as-configured, not equal-durability.

Two independent five-pair batches were run with the pair order reversed on every other pair; the
second used the same command with `--root /var/tmp/cairn-performance-delete-single-key-12k-b`:

```sh
python3 conformance/delete_single_key.py \
  --root /var/tmp/cairn-performance-delete-single-key-12k \
  --cairn target/release/cairn \
  --rustfs /var/tmp/cairn-perf-tools/rustfs \
  --pairs 5 --objects 12000 --concurrency 16
```

The client is Python 3 standard library only (minimal SigV4, one reused connection per worker).

**Before #179/#180 (10 pairs, 20 arms, 120,000 deletes per engine):**

| Metric (10 pairs, 20 arms, 120,000 deletes per engine) | Cairn | RustFS |
|---|---:|---:|
| Throughput, mean of arm rates | 813.4 obj/s | 1,680.3 obj/s |
| Arm range | 715.9–1,059.4 obj/s | 1,549.9–1,739.1 obj/s |
| Wall time per arm, mean | 15.0 s | 7.2 s |
| p50 / p90 / p99 latency, median arm | 15.6 / 35.9 / 92.3 ms | 8.7 / 13.5 / 21.9 ms |
| Prefill (excluded from score), mean | 9.3 s | 17.2 s |
| Server peak RSS, mean | 40.3 MiB | 299.5 MiB |

The ten per-pair Cairn/RustFS ratios were 0.646, 0.504, 0.440, 0.415, 0.439, 0.630, 0.434, 0.462,
0.462 and 0.429: mean **0.49×**, median 0.45×, ratio of arm means 0.48×. RustFS was faster in all
ten pairs. Every arm deleted 12,000/12,000 keys with zero driver errors, all sampled keys returned
404, and RustFS's strict readback held after each arm.

**After #179/#180 (3 hardened pairs, 2026-11-04):** #179 co-commits the audit row with the delete
savepoint (one `synchronous=FULL` commit instead of delete-then-audit); #180 batches cleanup-debt
retirement after the physical unlink/namespace phase. The hardened driver adds the exact
bucket-empty LIST check and records client CPU.

```sh
python3 conformance/delete_single_key.py --root /var/tmp/cairn-performance-delete-single-key-post2   --cairn target/release/cairn --rustfs /tmp/cairn-delete-post/rustfs   --pairs 3 --objects 12000 --concurrency 16
```

| Metric (3 pairs, 6 arms) | Cairn | RustFS |
|---|---:|---:|
| Throughput, mean of arm rates | 1,370.2 obj/s | 1,655.9 obj/s |
| Per-pair rates | 1,612.5 / 1,339.4 / 1,158.8 | 1,627.4 / 1,704.9 / 1,635.3 |
| p50 / p90 / p99 latency, median arm | 9.35 / 23.07 / 47.54 ms | 8.90 / 13.94 / 23.41 ms |
| Client CPU per object, mean | 0.322 ms | 0.371 ms |
| Bucket empty after every arm | yes | yes |

The three per-pair ratios were 0.991, 0.786 and 0.709; mean **0.83×**. This is not a controlled
capacity comparison: the host was loaded (load average ~12), Cairn fell across the three pairs while
RustFS stayed near 1.65k obj/s, and the pre-change 0.49× came from a quieter, larger 10-pair run.
The paired before/after diagnostic for #179 is the controlled evidence: 5,000 × 4 KiB, c16,
13.5 → 7.4 ms p50 and 1,154 → 2,003 obj/s (+74%). #180's cleanup diagnostic removed 24,000 retire
mutations and ~4,300 commits from a 12k-delete arm, but that work was already outside the scored
critical path; the scored gain is the delete+audit co-commit. Raw evidence:
[`performance-evidence/2026-11/delete-single-key-post-co-commit-2026-11-04.json`](performance-evidence/2026-11/delete-single-key-post-co-commit-2026-11-04.json).

Caveats:

- This is a **single-key** measurement and is **not** a replacement for, or an input to, the
  `delete_4k` **1.43×** bulk ratio. The two shapes measure different server work — one request per
  key here, one request carrying up to 100 keys there — and must not be pooled. The single-key
  shape favors RustFS while the bulk shape favors Cairn; that disagreement is the reason to report
  both, not evidence of a regression in either. Neither ratio is equal-durability: RustFS's strict
  mode does not fsync the unversioned delete path (see the † note above). The versioned arm below is
  the equal-durability measurement, and there Cairn is faster (3.55×).
- The 12,000-key pool was chosen after a 3,000-key screening (943 Cairn / 656 RustFS obj/s): it puts
  the slower Cairn arm at ~15 s and the faster RustFS arm near 7 s; a larger pool only lengthens the
  Cairn arm. Two earlier 10,000-key batches measured 0.65× and 0.70× while RustFS ranged
  699–1,776 obj/s, and a 3-pair pilot measured 1.25×; those host-load-affected pilots are not
  pooled. The two 12,000-key batches agreed at 0.489× and 0.484×, with RustFS inside
  1,550–1,739 obj/s across all ten arms.
- Binaries: the pre-change Cairn build was `7175e2ba…`; the post-change build in the hardened run
  was `f1896419f6c58aeded63a7832917b8ca05c3411d26cde981158ff7f4a5810292`. RustFS 1.0.0 executable
  SHA-256 `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` (publisher ZIP
  `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42`). Warp is not involved in
  this measurement. Method and offline checks live in `conformance/delete_single_key.py` and
  `conformance/test_delete_single_key.py`; the writer attribution below uses
  `conformance/delete_single_key_cairn.py` and `conformance/test_delete_single_key_cairn.py`.

#### Writer attribution for the single-key shape (2026-11-04)

`conformance/delete_single_key_cairn.py` is the diagnostic companion to that harness. It imports
`delete_single_key.py`'s client unchanged — the same exact 12,000-key 4 KiB pool and the same one
signed `DELETE` per key at bounded concurrency — and additionally scrapes `/metrics` one full
15-second store-metrics tick apart on both sides of the scored delete window. The writer's
per-stage sample rings (admission, queue, begin, apply, commit, checkpoint), its per-commit ring
(commit seconds and batch size), the per-process I/O counters and the host load average therefore
describe that window and not the prefill. It measures Cairn only and is not an engine ratio. Arms
were run exclusively — no other benchmark ran while a batch was in flight — and the host was
otherwise shared with unrelated builds, so only within-batch comparisons are drawn.

Control binary `c81ff690…` (commit `433438f`), one arm per row. Across the session the same c16 arm
of that binary ranged 1,176–1,613 obj/s as host load moved, while the commit share stayed 77–84%,
so the absolute rate is not comparable across batches but the split is:

| Concurrency | obj/s | Batch | Commits | COMMIT mean | COMMIT share of arm wall | apply mean/batch | Process write bytes per object |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 949 | 3.98 | 3,126 | 3.400 ms | 84% | 0.583 ms | 70 KiB |
| 16 | 1,548 | 7.03 | 1,768 | 3.536 ms | 81% | 0.951 ms | 63 KiB |
| 32 | 2,257 | 10.78 | 1,154 | 3.545 ms | 77% | 1.417 ms | 58 KiB |

Three properties explain the shape:

- **The durability barrier dominates.** The single `COMMIT` that covers a whole group-commit batch
  is 77–84% of the arm's wall time; the per-batch `apply` of the savepointed mutations accounts for
  most of what is left. At c16 the writer spends 6.25 s of a 7.75 s arm inside `COMMIT` and 0.95 ms
  per ~7-mutation batch applying them.
- **It is barrier latency, not bytes written.** From c8 to c32 the mean batch grew 2.7× while
  `COMMIT` mean moved 3.400 → 3.545 ms (+4%). A `synchronous=FULL` commit on this host costs a
  ~3.5 ms fixed barrier, so writing fewer pages per delete would not make the barrier shorter.
- **Throughput follows the batch, and the batch follows the client.** The batch is roughly the
  requests that arrive while the previous commit is in flight (≈ 1 + rate × commit), so the same
  server delivers 949 / 1,548 / 2,257 obj/s at c8 / c16 / c32. At the harness's fixed c16 that is
  ~7 deletes per commit, i.e. ≈ 3.5/7 + 0.14 ≈ 0.64 ms of writer time per object. The handler's own
  duration says the same thing from the request side: 12,000 DELETEs at 9.301 ms mean in the
  quiet-host arm, of which 8.2 ms is the measured assembly queue plus apply plus commit barrier,
  against 0.44 ms of process CPU per object.

**Rejected: fewer statements per delete.** A candidate derived the `bucket_stats.objects` delta from
the row a delete already reads (its `is_latest`/`is_delete_marker` flags and the successor it
promotes) instead of two `current_visibility` probes, folded that delta into the existing roll-up
upsert, and inserted each storage-cleanup debt row before reading it back. That removes five of
roughly sixteen statements from an unversioned delete, and both SQL backends were mirrored. A
three-pair alternating A/B on the same c16 shape measured 1,513 / 1,571 / 1,613 obj/s for the
control and 1,290 / 1,559 / 1,507 obj/s for the candidate (mean **0.93×**), with `apply` per
object 0.148 ms for the control and 0.144 ms for the candidate — no change beyond noise — so the
change is not merged. The reason is visible in the stage split: `apply` time tracks the statements
that modify a page, not the index probes that read one, and the merged upsert writes the same page
the separate update wrote. Removing read probes changed neither `apply` nor `COMMIT`.

**Rejected: a group-commit linger.** The candidate experiment set
`CAIRN_META_GROUP_COMMIT_LINGER_MICROS=1000` for two diagnostic arms (the official comparison pins
it at 0). The mean batch rose 7.06 → 8.5, but the two pairs measured 1,163 → 1,037 and 1,105 →
1,325 obj/s — no repeatable gain — while p50 rose in one pair (10.7 → 12.4 ms). A wait before the
commit pays back exactly what it amortizes at this arrival rate, so the batcher should not add one
implicitly.

The remaining levers are therefore the host's barrier latency, the number of deletes a commit
covers (client concurrency or a multi-key request — the bulk `delete_4k` row above amortizes one
barrier over up to 100 keys), and the ~0.14 ms/object `apply`. Within `apply`, the two
`storage_cleanups` rows a delete enqueues are a third of the page-modifying statements but about
half of the pages it dirties; the `.meta` absence-barrier row (`P2` in the DELETE review) is still
gated on proving no legacy or partial sidecar can exist, and this attribution shows skipping it
would cut WAL bytes and `apply` time but not the barrier latency, so it is not a throughput win on
this host. None of this contradicts the versioned arm below, which is the durability-equivalent
comparison and measures Cairn ahead there: this section only explains why the *unversioned*
as-configured number is what it is. Raw arms, host load and the rejected candidate's binary are
recorded in
[`performance-evidence/2026-11/delete-single-key-attribution-2026-11-04.json`](performance-evidence/2026-11/delete-single-key-attribution-2026-11-04.json).

### Versioned single-key DELETE — the durability-equivalent arm

The two unversioned DELETE ratios above are as-configured throughput, not equal-durability:
RustFS strict never fsyncs that path. `conformance/delete_versioned_single_key.py` is the fair
companion. It starts a fresh single-node store per arm, enables S3 versioning (`PUT
/bucket?versioning` with `<Status>Enabled</Status>`, read back as `Enabled` on both engines),
creates the same exact 12,000 × 4 KiB pool through signed S3 PUTs outside the scored window,
then issues exactly one signed `DELETE /bucket/key` per key at 16-way concurrency. Every DELETE
must return 200/204 with `x-amz-delete-marker: true`; an arm passes only when all 12,000
markers are acknowledged, HEAD of the first/middle/last key returns 404 with that marker
header, a bounded `list-type=2` request shows no current objects, and a fully paged `?versions`
audit finds exactly 12,000 `<Version>` and 12,000 `<DeleteMarker>` entries covering exactly the
prefilled key set. The RustFS bucket-durability readback must stay `strict` after every arm.

Versioning changes what the DELETE does: the marker is metadata, so neither engine unlinks the
prefilled 4 KiB bodies during the scored interval. The absolute rates are therefore **not**
comparable with the unversioned rates above; only the Cairn/RustFS ratio within this arm is.
RustFS 1.0.0 (commit `d47f54bf`) does apply strict to marker commits: the marker is written by
`crates/ecstore/src/disk/local.rs::write_metadata_with_namespace_owner` (whose comment names
delete markers) through `write_all_meta_with_namespace_owner(..., sync=true)`, which gates its
file and directory fsyncs on `effective_durability(volume).syncs_commit_metadata()` — true only
for `Strict`. `delete_version_inner` likewise commits a rewritten `xl.meta` through the same
gated helper. Cairn ACKs the marker after one `synchronous=FULL` metadata transaction that
co-commits the audit row (`Mutation::DeleteWithActivity`), with no physical data removal and no
cleanup debt for the retained version.

Method:

```sh
python3 conformance/delete_versioned_single_key.py \
  --root /var/tmp/cairn-performance-delete-versioned-12k \
  --cairn target/release/cairn \
  --rustfs /var/tmp/cairn-perf-tools/rustfs \
  --pairs 3 --objects 12000 --concurrency 16
```

**Result (3 interleaved pairs, 6 arms, 36,000 markers per engine):**

| Metric | Cairn | RustFS |
|---|---:|---:|
| Throughput, mean of arm rates | 2,457.3 obj/s | 691.7 obj/s |
| Per-arm rates | 2,453.4 / 2,469.9 / 2,448.7 | 685.1 / 692.2 / 697.6 |
| Wall time per arm, mean | 4.88 s | 17.35 s |
| p50 / p90 / p99 latency, median arm | 5.84 / 8.85 / 13.79 ms | 20.77 / 33.13 / 55.29 ms |
| Client CPU per object, mean | 0.32 ms | 0.66 ms |
| Prefill (excluded from score), mean | 8.03 s | 14.68 s |
| Server peak RSS, mean | 66.1 MiB | 342.0 MiB |
| Delete markers acknowledged | 12,000/12,000 per arm | 12,000/12,000 per arm |
| Version history after the arm | 12,000 versions + 12,000 markers | 12,000 versions + 12,000 markers |

The three per-pair Cairn/RustFS ratios were **3.581, 3.568 and 3.510; mean 3.55×**. Every arm
passed: all 12,000 DELETEs returned 200/204 with `x-amz-delete-marker: true`, the three sampled
HEADs returned 404 with the marker header, `list-type=2` showed no current objects, the paged
`?versions` audit covered the exact key set in 24 pages with zero duplicates, and RustFS's
bucket-durability readback stayed `strict` after every arm. Raw evidence:
[`performance-evidence/2026-11/delete-versioned-single-key-2026-11-04.json`](performance-evidence/2026-11/delete-versioned-single-key-2026-11-04.json).

This is the first equal-durability DELETE number, and it reverses the unversioned ranking. The
unversioned **0.49×** (pre-#179/#180) and **0.83×** (hardened, host load ~12) are as-configured:
RustFS's unsynced unversioned arm ran at ~1,656 obj/s, but when strict covers every marker commit the
versioned arm runs at **691.7 obj/s** (p50 20.8 ms), while Cairn's marker is a single group-committed
metadata transaction and 16 concurrent DELETEs share FULL commits, so it holds **2,457 obj/s** (p50
5.8 ms). Cairn's versioned rate is also higher than its own unversioned 1,370 obj/s because a delete
marker removes no object body and owes no cleanup debt. The comparison isolates the durability
contract, not the data-unlink path.

Caveats:

- Single host, single node, one NVMe, and the host also runs the homelab services; the ratio is the
  stable signal, not the third digit. The campaign ran under the exclusive host-wide benchmark lock
  (`flock /tmp/cairn-deep-perf.lock`); the 1-minute load average was 3.53 immediately before and
  15.31 immediately after (the latter is the campaign's own 6-vCPU saturation, not another
  workload). Other agents' low-parallelism release builds may have overlapped.
- Absolute rates are not comparable across the versioned and unversioned arms: versioning moves the
  scored work from data unlink/cleanup to a metadata marker on both engines.
- "Durable marker" here means the engine's declared strict commit path is engaged and readable
  after the arm; it is not a crash-injection proof at every marker. The RustFS source audit above
  and the strict readback are the evidence that the sync gate is on.
- Binaries: Cairn release `c81ff6903212551143bd74da3db7537b5e94e6589534df4d4e0286fda3d3d620`
  (commit `433438f`); RustFS 1.0.0 executable
  `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` (publisher ZIP
  `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42`). Method and offline checks
  live in `conformance/delete_versioned_single_key.py` and
  `conformance/test_delete_versioned_single_key.py`.

## 10. Multipart assembly MD5 elision (2026-11-03)

A focused A/B measured the full-object MD5 pass that assembly used to run even though a completed
multipart object's ETag is the composite of its part MD5s. Both arms staged ten 5 MiB plaintext
parts once, then assembled the same 50 MiB eight times each, alternating arms after one warm-up
assembly per arm. Both arms wrote the v2 metadata sidecar and computed the internal SHA-256; the
only difference was whether the hasher updated an MD5. Release profile, Rust 1.97.1, local 6-core
i5-9400T, data directory on the XFS NVMe root filesystem, `LocalBlobStore` directly — no server,
network, or metadata database.

| Arm | Mean ms | Median ms | Min–max ms |
| --- | ---: | ---: | ---: |
| MD5 computed | 326.1 | 326.0 | 321.5–330.5 |
| MD5 elided (composite ETag supplied) | 225.2 | 224.9 | 219.9–234.8 |

Elision removed 30.9% of mean assembly time (31.0% of the median); a repeat of the skip arm
measured 221.5 ms mean and 223.0 ms median. The saving is assembly only: it excludes the part
upload, metadata commits and the rest of the S3 request path. A permanent test
(`assembly_with_an_explicit_etag_skips_the_unused_full_object_md5`) checks that the elided arm
still returns the exact bytes, the composite ETag, the internal SHA-256 and a v2 sidecar carrying
the same layout.

On the scored 10×5 MiB multipart case, a three-pair run of the same strict harness measured
Cairn at 49.7 obj/s and RustFS at 64.4 obj/s — **0.77×**, versus the earlier single-pair 0.69×
after #158. That run used Cairn release binary SHA-256
`8cc7c84cd344bbe5c705be3ce780091a4c7b0678f41e26817a05e1d183b36e05`, RustFS 1.0.0 executable
SHA-256 `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`, and Warp v1.8.2
SHA-256 `fc7661a6b952800fbc8422cf0f72ae82726f82aa80065d15f50125f8bf5ca5fa`; per-pair ratios were
0.74, 0.81 and 0.76. The pair count and host load differ from the earlier row, so the ratio moved
for more than one reason; the direction is consistent with the isolated A/B. Section 14 supersedes
this row: the same scored case now measures 25.48 vs 23.65 obj/s (**1.078×**) after the BLAKE3
baseline and the reflink completion fast path.

## 11. Multipart assembly hash/I/O pipeline and CRC64 slicing (2026-11-04)

Profiling the completion assembly showed that the mandatory internal SHA-256 was not only the
largest cost, it was serialized with every part read and every sink write: one 256-KiB blocking
read hop, the hash, then one 256-KiB blocking write hop, 200 times for 50 MiB. A micro-probe on
this host measured SHA-256 (aws-lc-rs, no SHA-NI on the i5-9400T) at **333 MiB/s = 150 ms per
50 MiB**, MD5 at 455 MiB/s, 50 MiB of 256-KiB page-cache reads through `spawn_blocking` at 14 ms,
a 256-KiB copy pass at 5 ms, and the exact read+hash+write shape at 184–190 ms wall / 160–180 ms
CPU. The hash dominates and the I/O has nothing to overlap with.

Assembly now runs one leased blocking job that reads and hashes every part in order and hands each
chunk to the staging sink through a bounded channel (depth 2, recycled 256-KiB buffers), while the
reactor applies the block encoder/encrypter and writes the previous chunk. Order is preserved by
the single producer, so the internal SHA-256 still covers exactly the assembled plaintext. The
bytes, composite ETag, requested checksums, encryption/compression framing and the v2 sidecar are
unchanged; only the scheduling of the same work changed. The encrypted-part branch decrypts in the
same reader job instead of in a separate channel-fed job.

CRC-64/NVME now updates eight bytes per iteration (slice-by-8) instead of one. The byte-at-a-time
table cost 135 ms of CPU per 50 MiB, which is significant next to the SHA-256 a completion already
computes over the same bytes; the eight slice tables are derived from the same polynomial, so the
digest is identical. A permanent test compares every input length and streaming split against the
byte recurrence.

Measured with `crates/cairn-blob/examples/bench_assembly.rs`: ten 5 MiB parts staged once, eight
timed assemblies per arm after one warm-up, interleaved arms, release profile, Rust 1.97.1, data
directory on the XFS NVMe root filesystem, `LocalBlobStore` directly (no server, network or
metadata database), under a host-wide benchmark lock. Each arm reports the whole `assemble()` call,
the `Assembly` stage and the `Durability` stage separately. Host load varied (3–17) because other
workstreams were building on the same host, so pairs are read only against each other.

Plaintext parts, two interleaved pairs:

| Arm | Mean ms | Median ms | Min–max ms | Assembly mean | Durability mean | CPU mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before | 237.8 / 231.7 | 236.4 / 231.6 | 225.6–252.2 | 189.4 / 188.5 | 48.4 / 43.2 | 175.0 / 177.5 |
| Pipeline | 206.3 / 207.7 | 206.3 / 207.9 | 200.2–216.9 | 161.9 / 162.6 | 44.3 / 45.0 | 180.0 / 181.3 |

The pipeline removes 26–31 ms of the ~232–238 ms completion (11–13 %): the assembly stage drops
27.5 ms while the durability stage is unchanged. Total CPU is unchanged within tick granularity —
the win is overlap, not less work. A second pair on a quieter host measured 223.1/230.9 ms before
and 202.0/204.6 ms after.

Other shapes, one interleaved pair each (total mean ms / assembly mean ms):

| Shape | Before | Pipeline | Pipeline + CRC64 slice |
| --- | --- | --- | --- |
| Encrypted parts (AES-256-GCM containers) | 291.8 / 247.0 | 279.8 / 233.1 | — |
| zstd-compressed object | 264.5 / 217.7 | 198.4 / 154.4 | — |
| `CRC64NVME` requested | 359.0 / 311.7 | 334.8 / 290.6 | **229.8 / 185.7** |

Every shape improves; none regresses. With CRC64NVME requested, the slice-by-8 update removes
another ~103 ms: the requested checksum now adds ~24 ms to the 207 ms plain pipeline, where it
added ~130 ms before.

Two candidates were measured and rejected:

- **Chunk size.** 256 KiB, 512 KiB and 1 MiB chunks measured 212.5/213.7, 225.9/212.4 and
  217.3/226.4 ms across interleaved runs — indistinguishable. 256 KiB stays: it is the smallest
  retained footprint (depth 2, under 1 MiB per in-flight assembly including the sink buffer).
- **io_uring sink.** With the same pipeline binary and `--features io-uring`, the default and io_uring
  staging backends measured 206.0/203.8 and 203.3/213.8 ms. Once the sink write runs concurrently
  with the hash it is no longer on the critical path, so the write backend does not matter.

End-to-end, through a strict server (`CAIRN_META_SYNCHRONOUS=full`, one shard, no linger), twelve
sequential `CompleteMultipartUpload` calls of ten 5 MiB parts each, interleaved pairs:

| Arm | Median ms | Mean ms | Min–max ms |
| --- | ---: | ---: | ---: |
| Before | 246.8 / 245.9 | 246.2 / 259.4 | 236.7–374.8 |
| Pipeline + CRC64 slice | 210.2 / 210.9 | 210.4 / 222.7 | 198.3–344.7 |

That is ~36 ms (14.6 %) off a real completion. The scored `warp multipart-put` case reports
`PUTPART` throughput and is dominated by the part-upload path (one completion per ten parts), so
it is not a usable signal for this change; its arms moved within host-load noise (41.8–54.4 obj/s).

Tests: `assembly_pipeline_preserves_hash_and_byte_order_across_unaligned_parts` checks the exact
bytes, the composite ETag and the internal plaintext baseline against the same bytes staged in one pass
for parts straddling the 256-KiB read chunk; `assembly_mid_stream_reader_failure_aborts_without_publishing`
checks that a reader failure after earlier chunks reached the sink fails closed;
`crc64nvme::tests::slice_by_eight_matches_the_byte_recurrence` covers every length and split
against the byte-at-a-time recurrence. The existing mixed plaintext/encrypted, wrong-DEK,
checksum-over-plaintext and preallocation assembly tests all run through the new pipeline.

## 12. Completion-inclusive multipart comparison (2026-11-04)

Section 11 measured the completion in isolation. This section measures the whole object under the
same four-worker shape the scored `multipart_put_5m` case uses, so the completion is on the
critical path: every object is `CreateMultipartUpload` → ten 5 MiB `UploadPart`s at 4-way
concurrency → one synchronous `CompleteMultipartUpload` → the next object, with four object
workers and nothing else pipelined. The output is **completed objects per second**, not Warp's
`PUTPART` operation rate.

**Why the existing row is not an object rate.** Warp's `multipart-put` report block is `PUTPART`,
and its `obj/s` field counts those part operations: 49.7 parts/s × 5 MiB = 248 MiB/s (and RustFS
64.4 × 5 MiB = 318 MiB/s), which is the bandwidth the plan recorded. The completed-object rate in
that run was therefore 4.97 Cairn / 6.44 RustFS objects per second, and the summary gives the
per-upload completion no line of its own.

**Method.** `conformance/multipart_complete.py` starts a fresh strict single-node store per arm
through `rustfs_compare.py`'s helpers, creates one unscored warm-up object, then scores exactly 40
objects (10 × 5 MiB = 50 MiB per object). Each part position has its own deterministic 5 MiB buffer,
generated once, so a reordered or duplicated part fails the byte checks rather than passing on
identical payloads. After the scored window the driver fetches **every** completed object in full
and one rotating `?partNumber` (each part number is checked four times over 40 objects), asserts
the exact SHA-256 and length of both responses, asserts the completion ETag equals the composite
`MD5(concat(part MD5s))-10`, and asserts a bounded `list-type=2` page contains exactly the 40
keys. An arm passes only on 40/40 completions, 40/40 of each verification, an exact listing, zero
driver errors, and (RustFS) a strict bucket-durability readback. Both engines wait the same 16 s
settle window before and after the scored interval; the wait exists because Cairn's multipart stage
histograms are drained by a 15 s background tick, and waiting only on Cairn's arm would give it a
quieter start.

```sh
flock -x /tmp/cairn-deep-perf.lock python3 conformance/multipart_complete.py \
  --root /var/tmp/cairn-performance-mpu-complete \
  --cairn target/release/cairn \
  --rustfs /var/tmp/cairn-perf-tools/rustfs \
  --pairs 3 --objects 40
```

Three interleaved pairs, pair order reversed on even pairs, on the local 6-core i5-9400T / NVMe
host under `flock /tmp/cairn-deep-perf.lock`; the 1-minute load average was 1.22 before and 7.72
after the campaign (the latter is the campaign's own server plus client). Cairn release SHA-256
`c4957da821f948193bc0f1f963ebf2b55eb9bffc39bd1065ef67598ac50efd1e` (0.1.0-dev+g53513027, main
`5351302`), RustFS 1.0.0 `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`.

| Metric (3 pairs, 6 arms, 120 completed objects per engine) | Cairn | RustFS |
|---|---:|---:|
| Completed objects/s, mean of arm rates | 1.88 | 2.89 |
| Arm range | 1.59–2.31 | 2.52–3.16 |
| Parts/s, mean | 18.9 | 28.8 |
| Completed-object MiB/s, mean | 94.2 | 144.3 |
| Wall time per arm, mean | 21.7 s | 14.0 s |
| Object-cycle p50, median arm | 2,277 ms | 1,275 ms |
| Completion p50 / p90 / p99, median arm | **1,026 / 1,478 / 1,634 ms** | **32 / 132 / 204 ms** |
| Part p50, median arm | 345 ms | 397 ms |
| Completion share of object time, mean | **43.2 %** | **4.8 %** |
| Client CPU per object, mean | 19.5 ms | 17.6 ms |
| Server CPU, mean | 22.7 s (≈1.0 core) | 34.6 s (≈2.5 cores) |
| Server peak RSS, mean | 61.7 MiB | 318.2 MiB |
| Verified full object / part / ETag / listing | 40/40 each, every arm | 40/40 each, every arm |

Per-pair Cairn/RustFS completed-object ratios were **0.557, 0.534 and 0.914** (mean **0.668×**;
ratio of arm means 0.653×). The third pair is the outlier: Cairn improved to 2.31 obj/s while
RustFS fell to 2.52, and the pair-3 RustFS completion p50 rose to 75 ms.

The loop arithmetic checks out exactly. Cairn's per-object p50 cycle was 2,277 ms and four workers
give `4 / 2.277 = 1.76` obj/s against 1.76 measured; RustFS `4 / 1.275 = 3.14` against 3.16. The
rate is the worker count divided by the object cycle, so the completion tail is not a rounding
error — at 43 % of Cairn's cycle it is the second-largest component after the three part waves.

### Where the completion tail goes under load

A separate diagnostic (`multipart-complete-tail-control-2026-11-04.json`) ran eight complete
objects per shape on one fresh Cairn store with the same payloads and the same `upload_object`
path: sequentially (one object at a time, four part slots) and then under the scored 4×4 pipeline.
It reports the server-side `cairn_blob_multipart_stage_seconds` stages for each window:

| Shape | Completion p50 | Assembly mean | Durability mean | Part p50 |
|---|---:|---:|---:|---:|
| Sequential, one object at a time | 321 ms | 164 ms | 211 ms | 117 ms |
| Four object workers × four part slots | 704 ms | 179 ms | 744 ms | 697 ms |

The scored arms agree with the pipelined control on the stage means: assembly stayed at 173–197 ms
in all three arms, while durability measured 527–859 ms and permit wait was under 0.001 ms.
**The load-dependent tail is the completed object's durability, not assembly.** The completion
writes a new 50 MiB object file and must fsync it (plus the rename and directory sync) while up to
sixteen part streams are staging and fsyncing their own 5 MiB files, so the completion's fsync
waits behind the part pipeline's dirty data. RustFS completes in 25–76 ms; with strict durability its
parts are already durable, and the measured tail is consistent with a metadata-only completion that
does not rewrite the 50 MiB object. That is a real architectural difference, not a mode mismatch,
since both engines ran strict; this comparison did not instrument RustFS's internal stages, so the
mechanism is inferred from the measured tail, not measured directly.

Section 11's isolated probe measured a 246 → 210 ms completion (assembly 189 → 162 ms, durability
~44 ms) with sequential completions on an otherwise idle host. Those numbers reproduce here for
the sequential shape (321 ms p50, assembly 164 ms, durability 211 ms — the faster assembly now
absorbs the writeback in the fsync, as section 11 notes). Under the four-worker pipeline the tail
is **0.70–1.09 s p50 and 39–47 % of the object cycle**, so the 14.6 % isolated improvement does not
translate into a 14.6 % object-rate improvement; the durable rewrite dominates instead.

### Reading and limits

- **The completed-object ratio is 0.67×**, not the Warp row's 0.77×. With ten parts per object,
  `PUTPART` throughput and completed-object throughput are proportional when every object completes
  inside the window, so the ratio difference is not a unit trick: it comes from conditions. Here
  Cairn's tail share is 43 % (Warp's loop implied ~40 %) while RustFS's is 5 % (Warp's implied
  ~10 %), and the part p50s are 345/397 ms versus Warp's 171/180 ms on the same host. This campaign
  did not isolate why the part wave is slower under this driver; client CPU was only ~19 ms/object,
  so the Python client's own CPU is not the explanation, and the host load plus device writeback
  state were not held equal to the Warp runs.
- The absolute rates are **method-specific** and must not be pooled with the Warp scoreboard. The
  stable signals are the tail share, the stage split, and the completed-object count/verification.
- Every completion is verified before the arm passes, so a throughput number is never reported for
  an object whose bytes, part layout or composite ETag were not checked. `?partNumber` coverage is
  part of the gate, not a sample.
- The Python driver is standard-library only and reuses `delete_single_key.py`'s SigV4 client and
  `rustfs_compare.py`'s strict environment, health, bucket and durability helpers. Offline checks:
  `conformance/test_multipart_complete.py`.

Raw evidence:
[`multipart-complete-2026-11-04.json`](performance-evidence/2026-11/multipart-complete-2026-11-04.json)
and
[`multipart-complete-tail-control-2026-11-04.json`](performance-evidence/2026-11/multipart-complete-tail-control-2026-11-04.json).

## 13. BLAKE3 internal baseline (2026-11-04)

The completion tail hashed every 50 MiB of assembled plaintext with SHA-256 at this host's
AVX-class ceiling. Schema v42 changes that baseline for new objects to BLAKE3; legacy rows keep
SHA-256. Isolated hash throughput on the same host and payload:

| Algorithm | Median | Min | Throughput |
| --- | ---: | ---: | ---: |
| BLAKE3 | 16.8 ms | 16.4 ms | ~3.0 GiB/s |
| SHA-256 (aws-lc-rs, no SHA-NI) | 147.1 ms | 144.0 ms | ~340 MiB/s |

`bench_assembly` (ten 5 MiB parts, eight timed assemblies, release, same XFS data directory,
host-wide benchmark lock) isolates the completion assembly stage:

| Arm | Assembly mean | Assembly range | Total mean (fsync-noisy) |
| --- | ---: | ---: | ---: |
| SHA-256 baseline | 158.1 ms | 151.4–174.9 ms | 313.8 ms |
| BLAKE3 baseline | 24.6 ms | 23.1–27.5 ms | 300.9 ms |

The assembly stage drops by ~133 ms (84 %) and process CPU falls from ~174 ms to ~39 ms per
completion. The total includes this host's durability barrier, which was heavily contended during
the run (25–822 ms per arm); under the 44 ms durability measured in §11 the same saving would put
an otherwise-idle completion at ~77 ms. The final verification pass re-read the assembled bytes and
checked the BLAKE3 baseline and v3 sidecar in both arms.

The completion-inclusive harness from §12 measures the whole object, and its durability tail
dominates under four-worker load. The merged §12 control and a fresh same-host paired run against
the pre-BLAKE3 binary:

| Arm | Completed obj/s | Completion p50 | Assembly mean | Durability mean |
| --- | ---: | ---: | ---: | ---: |
| §12 control (merged, different host load) | 1.88 | 1,026 ms | 173–197 ms | 527–859 ms |
| Paired control, pre-BLAKE3 (this run) | 2.63 | 692 ms | 170.0 ms | 480.9 ms |
| BLAKE3 (this branch, this run) | 2.45 | 593 ms | 35.0 ms | 492.4 ms |

The paired completion p50 falls by 99 ms and the assembly stage by 135 ms; the object rate moves
within the run-to-run noise because the 480–492 ms durability barrier still dominates the cycle.
The stage split is the controlled signal: assembly shrinks by 79 %, while durability and the part
wave are unchanged. Every completion in both arms passed the full-object, `?partNumber`, composite
ETag and exact-listing verification. Raw summary:
[`blake3-baseline-2026-11-04.json`](performance-evidence/2026-11/blake3-baseline-2026-11-04.json).

## 14. Reflink multipart completion (2026-11-04)

A multipart completion whose parts are all known-plaintext, with no compression or encryption, is
exactly the concatenation of the part files. On a reflink-capable filesystem, `cairn-blob` now
shares each aligned part's extents into the assembled file and hashes each source part once,
instead of rewriting the assembled bytes. A 4 KiB `FICLONERANGE` probe runs before any placement;
if the filesystem refuses it, the staging file is untouched and the original streaming pipeline
runs, so ext4/tmpfs stores keep the previous path. Once the probe succeeds, an unaligned range or a
refused clone is copied. No storage format, metadata, or configuration changes; the data fsync,
data rename, sidecar rename and directory barrier are unchanged.

### 14.1 Isolated completion

`crates/cairn-blob/examples/bench_assembly.rs` (10 x 5 MiB, 8 timed assemblies, XFS, host-wide
benchmark lock):

| Arm | Assembly mean | Durability mean | Total median |
| --- | ---: | ---: | ---: |
| BLAKE3 baseline (section 13) | 24.6 ms | ~276 ms | 300.9 ms |
| BLAKE3 + reflink (load ~1.6) | 21.2 ms | 11.8 ms | 33.1 ms |
| BLAKE3 + reflink + overlapping sidecar barrier (final) | 23.6 ms | 8.9 ms | 30.8 ms |

The final run verified the exact 50 MiB bytes, the BLAKE3 baseline and the v3 sidecar. The assembly
stage is the mandatory BLAKE3 read of the parts; the durability stage is now metadata-only clone
commit plus the sidecar/data barriers. A second final run under load 3.4 measured 42.1 ms median
(25.6 ms assembly / 17.4 ms durability), so the idle 30.8 ms is the clean floor.

### 14.2 Completion-inclusive

`conformance/multipart_complete.py`, 5 interleaved strict pairs, 40 x 10 x 5 MiB, 4 object workers
x 4-way part concurrency, host-wide lock. Final binary
`784b5d0f8b002de20249a50dcf8897840d9185133f887e8d1298457861d086a2`; RustFS 1.0.0
`a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`:

| Metric | Cairn | RustFS |
| --- | ---: | ---: |
| Completed objects/s, per pair | 2.50 / 2.74 / 2.74 / 2.58 / 2.94 | 2.26 / 2.62 / 2.66 / 2.85 / 3.27 |
| Completion p50, per pair | 137.4 / 81.6 / 147.8 / 78.6 / 76.1 ms | 31.1 / 36.8 / 35.9 / 31.9 / 28.3 ms |
| Completion share of object cycle | 13.0 / 5.8 / 16.1 / 8.7 / 6.2 % | 3.5 / 3.8 / 5.8 / 3.5 / 3.1 % |
| Assembly stage mean | 28.0 / 26.0 / 26.1 / 25.8 / 25.3 ms | n/a |
| Durability stage mean | 89.6 / 23.2 / 92.2 / 49.9 / 26.6 ms | n/a |

Per-pair completed-object ratios: 1.108, 1.045, 1.031, 0.904 and 0.900 (mean **0.998x**). An
earlier 5-pair run of the same code before the sidecar-barrier overlap (binary
`ecf28fbdb5fa8a4a9dcdf265c93337dbc3510d6de1e8067abe773d26084ecfab`) measured 1.073, 0.925, 0.799,
1.265 and 1.180 (mean **1.048x**); across the ten pairs the mean is **~1.02x**. The pre-change
section 12 control was **0.668x**. The remaining spread is the shared XFS log and the part wave,
not a fixed completion tail: the completion share of the object cycle fell from 43.2 % to 4-16 %.
Every arm passed 40/40 completions, full-object GET, one rotating `?partNumber`, the locally
derived composite ETag and an exact bounded listing.

### 14.3 Warp scoreboard

`conformance/rustfs_compare.py --case multipart_put_5m --pairs 3 --duration 12` with the same final
binary and Warp 1.8.2 (`fc7661a6b952800fbc8422cf0f72ae82726f82aa80065d15f50125f8bf5ca5fa`):

| Arm | Cairn | RustFS | Ratio |
| --- | ---: | ---: | ---: |
| pair 1 | 24.35 obj/s | 22.60 obj/s | 1.077x |
| pair 2 | 26.51 obj/s | 23.58 obj/s | 1.124x |
| pair 3 | 25.58 obj/s | 24.77 obj/s | 1.033x |
| mean | **25.48 obj/s** | **23.65 obj/s** | **1.078x** |

This is the scored `multipart_put_5m` row that previously read 0.77x. Raw evidence:
[`reflink-warp-multipart-2026-11-04.json`](performance-evidence/2026-11/reflink-warp-multipart-2026-11-04.json),
[`reflink-completion-2026-11-04.json`](performance-evidence/2026-11/reflink-completion-2026-11-04.json)
and
[`reflink-completion-pre-overlap-2026-11-04.json`](performance-evidence/2026-11/reflink-completion-pre-overlap-2026-11-04.json).
