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
constraint here is the single group-committing metadata writer and the fsync rate; the sweep makes
the ceiling visible.

Between phases the harness GETs `/metrics` and extracts the `cairn_*` gauges and the
`cairn_request_duration_seconds` summary, so the server's own view sits alongside the client-side
numbers.

### Observed results (default profile, this host)

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

The defining number: **concurrency 1 → 16 (16×) moved ops/s only 165.6 → 275.4 (1.66×) while p999
latency grew 18.3 ms → 198.2 ms (10.8×).** That is the single-writer ceiling, made visible.

Why it happens, per the spec:

- A **single group-committing writer** owns the one write connection (ARCH 7.2/11.6). Every
  small-object PUT must pass through it.
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

## 7. Head-to-head vs MinIO (`conformance/bench_compare.sh`, per-push CI)

Runs on **every push** (the `bench-compare` CI job) to answer one question continuously: *for each S3
operation, how does Cairn compare to MinIO on the same machine?* The harness boots **both** servers on
one host — Cairn from the built binary, MinIO from a **pinned** release binary
(`RELEASE.2025-09-07T16-13-09Z`, `dl.min.io`) — single-node/single-drive, plaintext HTTP, and drives
an identical `warp` v1.0.0 matrix against each **sequentially** (only one server under load at a time;
`warp` itself burns ~1 core). Matrix (CI-sized, ~15-20 min): `PUT`/`GET` at 4 KiB + 8 MiB, and
`STAT`/`DELETE`/`LIST`/`MIXED` at one representative size. Env-tunable (`DURATION`,
`CONCURRENT`, `REPEATS`, and `CELLS_ENV` to replace the whole matrix); a manual/nightly run uses
`REPEATS=3` for a median.

Output: a **job-summary markdown table** plus `bench.csv` / `bench.json` uploaded as an artifact for
over-time tracking. The parser reads the **measured** operation, not warp's prepare-PUT (a subtle trap:
`get`/`stat`/`delete` upload first, so the first `* Average:` line is the prepare, not the result).

**Report, not gate.** The meaningful signal is the **Cairn/MinIO ratio per op**, not the absolute
obj/s — a shared GitHub runner has the same ≈±30% run-to-run variance documented in §6, so a throughput
gate would false-positive constantly. The job therefore **fails only on warp operation errors** (a
correctness/liveness signal robust to noise); it emits a non-fatal ⚠️ if Cairn is >2.5× slower on an
op (far outside the noise band), and never fails on who is faster. Absolute numbers differ from the
local 2-core baseline because the runner is ~4 vCPU — compare ratios across runs, not obj/s.

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

The fast path is now a small Cairn-owned **HTTP/1.1 keep-alive loop** on the plaintext data-plane
listener: per connection it serves each eligible large GET with one `sendfile(2)` and keeps the
connection open for the next request, handing the connection to hyper (with the already-read bytes
replayed) only when it hits a request it cannot serve. So a connection-pooled client engages the
zero-copy path on **every** eligible GET on a connection, not just the first. The size floor
(`CAIRN_FASTIO_MIN_BYTES`, default 256 KiB) still keeps small objects on the streamed path, where the
per-request `sendfile` setup would outweigh the zero-copy saving.

### Engagement: before vs after the keep-alive rewrite

The earlier design **peeked only the first request** of a fresh TCP connection and then force-closed
or handed the socket to hyper, so a pooled client accelerated only its first request. Measured then on
both x86_64 and a real aarch64 Pi (kernel 7.0) under `warp get --concurrent 4`:

| run | GETs served | served via `sendfile` | engage |
|---|---:|---:|---:|
| x86_64, fast-io (peek-first, old) | 1703 | 0 | **0%** |
| aarch64, fast-io (peek-first, old) | 800 | 0 | **0%** |

Because `warp` (like every pooled S3 SDK — boto3, aws-sdk, etc.) opens a few keep-alive connections
whose first request is a prepare/PUT, the peek saw a non-GET → `ineligible` → hyper owned the
connection → every subsequent GET bypassed the fast path. The keep-alive rewrite removes that ceiling:
the loop consumes each request and serves eligible GETs back-to-back without closing. This is now a
**deterministic regression gate** — `conformance/sendfile_keepalive.sh` issues N GETs of a large
object over one keep-alive connection and asserts `cairn_sendfile_get_total{result=ok}` rose by N (it
rose by 1 under the old design); the gated `fast-io-conformance` CI job also runs the full boto3
lifecycle through the fast path. The warp A/B above remains the way to measure the **CPU-per-GiB** win
itself once engagement is non-zero.

> **Build note:** `fast-io` is **glibc-Linux-only**. The `ktls` dependency does not cross-compile to
> `aarch64-unknown-linux-musl` (a `cmsghdr`/`msghdr` struct-layout mismatch), so build the fast-io
> path for `aarch64-unknown-linux-gnu` (or x86_64). The shipped release binaries are default-features
> (no `fast-io`), so this does not affect releases. On a kernel where the kTLS ULP probe fails (seen on
> kernel 7.0), TLS connections fall back to userspace rustls — the designed graceful degradation.

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
- This path is **plaintext only**; zero-copy over HTTPS needs the (not-yet-built) kTLS takeover, so
  benchmark over `http://`, not `https://`.
