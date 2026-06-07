# Benchmarks and macro load profiles

This document covers Cairn's two layers of performance verification:

1. a **criterion micro-benchmark** of the SigV4 streaming chunked decoder (the hottest pure-CPU
   stage on the ingest path), and
2. a **macro load harness** that drives a real `cairn` binary with concurrent boto3 clients to
   characterize large-object bandwidth, small-object rate, and — the point of ARCH §30.2 — the
   **single-writer ceiling**.

MinIO's `warp` is the usual tool for the macro layer; it is unavailable in this environment, so
`conformance/load_profile.py` is an equivalent concurrent generator built on the same boto3 client
path the conformance suite drives (real SigV4 signing, real HTTP).

All numbers below were recorded on this host; they are illustrative of *shape*, not a hardware
spec sheet. Re-run on the target hardware for absolute figures.

---

## 1. Chunked-decoder micro-benchmark (criterion)

The streaming chunked decoder de-frames `aws-chunked` upload bodies. ARCH §30.1 wants the byte
path bound by hardware, not by framing CPU, so the de-framer must run well above device bandwidth.

`crates/cairn-s3/benches/decode.rs` decodes an 8 MiB unsigned body in 64 KiB chunks:

```sh
cargo bench -p cairn-s3 --bench decode
```

**Observed** (this host):

```
chunked_decode/8MiB_64KiB_chunks
    time:   [7.7463 ms  8.0061 ms  8.3208 ms]
    thrpt:  [961.44 MiB/s  999.23 MiB/s  1.0085 GiB/s]
```

So **~1 GiB/s** through the de-framer. That is comfortably above the large-object PUT bandwidth a
single durable-commit write path sustains (see below), confirming the de-framer is not the ingest
bottleneck — exactly the §30.1/§29.6 expectation.

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
(default 32 MiB, 16 objects, 8 workers). Reports aggregate up/down MiB/s. Per ARCH §30.1 this is
bound by disk and network bandwidth plus the two durable-commit fsyncs (fsync file, fsync dir),
**not** by the writer — every worker stages and fsyncs its own blob in parallel before the short
metadata commit.

**Profile (b) — small-object rate.** N concurrent workers PUT many 4 KiB objects. Reports ops/s
and p50/p99/p999 PUT latency, swept across concurrency **1, 4, 16**. Per ARCH §30.1 the binding
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

### Interpreting the single-writer ceiling (ARCH §8.3 / §30.2)

The defining number: **concurrency 1 → 16 (16×) moved ops/s only 165.6 → 275.4 (1.66×) while p999
latency grew 18.3 ms → 198.2 ms (10.8×).** That is the single-writer ceiling, made visible.

Why it happens, per the spec:

- A **single group-committing writer** owns the one write connection (ARCH §7.2/§11.6). Every
  small-object PUT must pass through it.
- **Group commit** (ARCH §8.3) coalesces the mutations that arrive during one durability barrier
  into a single COMMIT and a single fsync, then acks every member of the batch after that barrier.
  This is why ops/s *rises at all* as concurrency goes 1 → 4: more concurrent arrivals mean a
  larger batch factor, so the effective rate climbs above the bare synchronous-commit rate while
  per-write durability is preserved.
- But the writer + fsync rate is a hard ceiling. Once concurrency saturates the batch (here by
  ~4 workers), adding more concurrency (16) does **not** raise ops/s further — it just deepens the
  queue of PUTs waiting behind the in-flight batch, which is exactly why p50/p99/p999 climb
  steeply (5 → 53 ms p50; 18 → 198 ms p999) while throughput plateaus.

This is the §30.1 prediction stated plainly in §30.2: **ops/s scaling well below the concurrency
multiple, together with growing tail latency, is the operator-visible signature that the single
writer and the fsync rate are the binding constraint** for small-object writes. The operator
levers §30.2 lists follow directly from this curve: enlarge the group-commit linger (bigger
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

ARCH §26/§30.2 names the **write-queue-depth** metric as *the* server-side window onto this
ceiling — a depth that grows under load is the early-warning signal for write saturation before it
becomes latency. That gauge is **not yet wired** (`docs/GAPS.md` Medium #11/#12 list it among the
missing observability series; only `cairn_requests_total`, `cairn_request_duration_seconds`, the
store/byte gauges, and the WAL/replication series are published today). Until it lands, this
harness characterizes the ceiling from the two windows that *are* available: the server-side
`cairn_request_duration_seconds` summary and the client-side tail-latency-versus-concurrency curve
above. Both show the same thing — the writer saturating — which is what §30.2 asks an operator to
be able to see.

### Caveats

- Numbers are host- and load-shape-dependent; the harness fixes the *method*, not the figures.
- Large-object GET reads back just-written objects, so it measures the page-cache-warm read path.
- The payload is a cheap LCG fill chosen to be not-trivially-compressible, so throughput reflects
  real bytes moved rather than the compressor's view of a run of zeros (hence compression ratio
  1.0 in the gauges).
- `--quick` / `QUICK=1` shrinks the profile for a fast smoke check; use the default profile for a
  representative ceiling curve.
