# Shared-node adoption campaign resumption — 2026-09-23

The user authorized a **new**, separately metered campaign on the existing shared node,
after declining a quiet window. The preceding 3,600-second campaign ended at
3,592.133 measured seconds; its ledger is not reset. This campaign has its own
**1,200 measured client-load seconds** and **10,000,000,000 bytes of peak task-owned
scratch** cap. Build/test time is outside client-load metering, but every measured
arm, including failed arms, counts. Only the exact private
`/var/tmp/cairn-performance-adoption2-7gRRmk` tree and task-owned processes/images
may be removed afterward.

## Pre-registered protocol

- Cairn source is checkout `f6fde5a68c6a5d5acee54f950ce2be8cda547312`
  plus the uncommitted sampled blob-stage instrumentation already under test. Build
  with pinned Rust 1.97.1 release profile, default features and a real web bundle.
  Record the final binary and source-diff hashes before load.
- Control is the previously pinned RustFS **1.0.0** executable, release ZIP SHA-256
  `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42`;
  client is Warp **1.8.0**, SHA-256
  `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.
  Verify downloads before execution. Run native binaries sequentially, fresh store
  and loopback port per arm. No Docker container participates in measured load.
- Primary: five alternating Cairn/RustFS pairs of Warp `put_1m`, 1 MiB, concurrency
  16, requested 12 seconds/arm. RustFS global/new-bucket strict and drive sync on;
  read back each measured bucket's strict mode before and after. Cairn metadata FULL,
  shard count 1 and Writer linger 0. Same client and payload controls on both.
- If primary is valid and budget permits, two alternating pairs each of `mixed_1m`,
  `get_1m`, `list_4k` (12 seconds/arm), in that order. These are protected descriptive
  checks, not a complete adoption matrix. No optimization is adopted merely because
  a score improves here. A source edit after the primary creates a different binary
  and requires a new baseline; never pool it with prior arms.
- Zero unexpected S3 errors and successful Warp exit are mandatory. Retain per-arm
  host I/O-pressure, disk-counter, CPU/RSS and names-only competing-process census.
  Shared-host drift/overlap is disclosed, not numerically corrected. Stop with an
  inconclusive verdict if controls drift enough to obscure comparison. Never claim
  p99 without 10,000 successful samples per operation and arm.
- Stop before 1,200 actual client-load seconds or 10 GB task-owned peak scratch.
  Preserve compact JSON results in `docs/`; remove only owned scratch and processes
  after measurement. The primary goal is a reliable strict-mode performance ratio,
  not a predetermined win or an unwarranted architectural change.

## Execution and results

Before any measured traffic, the production web bundle was built and the pinned
`rust:1.97.1-bookworm` image (digest
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`)
completed `cargo build --locked --release --bin cairn`. Cairn executable SHA-256:
`f6220d3c9294da51e4e06759bbd4e3cf196f18e0556064adaba7bb3a521c9df6`.
The tracked source diff over `crates/cairn-blob`, `crates/cairn-server` and
`docs/observability.md` hashes to
`ca902b3cb3ee18050c2f9a3972a5adb234eaa68c5012b1f4ed0eb917f94227b0`.
RustFS executable SHA-256 is
`a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`.
The publisher's Warp URL transferred slowly; the version-pinned official container
`quay.io/minio/aistor/warp:v1.8.0` (digest
`sha256:594e582a494a05ff8517b23464b5b94654bee71dc42c47ff432d729dcf4ed823`)
provided `/warp` with **the identical preregistered executable hash**. No container
will serve or drive measured traffic. Build scratch was removed before load; the
retained three binaries occupied about 349 MB in the owned root. The 52 offline
Python benchmark tests passed (with non-fatal `ResourceWarning` from existing
SQLite-backed tests). The new load ledger is **0/1,200 seconds** at this point.

## Completed first matrix: instrumented Cairn versus RustFS

All 22 arms passed: Warp exit 0, zero reported operation errors, no runner limit
hit, no dropped load samples. Every RustFS bucket read back `strict` both before
and after traffic. The first matrix used the *instrumented* Cairn executable,
so it must not be labeled clean-HEAD performance. Full per-arm evidence is kept in
the [PUT](performance-evidence/2026-09/performance-shared-node-put-raw-2026-09.json),
[mixed](performance-evidence/2026-09/performance-shared-node-mixed-raw-2026-09.json),
[GET](performance-evidence/2026-09/performance-shared-node-get-raw-2026-09.json) and
[LIST](performance-evidence/2026-09/performance-shared-node-list-raw-2026-09.json) records.

| Operation (requested 12 s/arm) | Cairn arm scores | RustFS arm scores | Descriptive medians | Interpretation |
| --- | --- | --- | --- | --- |
| Strict PUT, 1 MiB, c16; five pairs | 57.20, 104.46, 86.68, 106.33, 51.71 MiB/s | 54.26, 60.29, 56.15, 71.98, 103.53 MiB/s | 86.68 vs 60.29 MiB/s | Four Cairn pair wins, but severe host drift and sampler overhead unknown; **not** an adoption result. |
| Mixed, 1 MiB, c16; two pairs | 161.03, 224.02 MiB/s | 211.77, 156.17 MiB/s | 192.53 vs 183.97 MiB/s | Pair direction reverses; inconclusive. |
| Hot-pool GET, 1 MiB, c16; two pairs | 2,047.87, 1,647.51 MiB/s | 1,246.99, 942.89 MiB/s | 1,847.69 vs 1,094.94 MiB/s | Both Cairn pair wins; loopback/hot-cache, not cold-device bandwidth. |
| LIST of prepared 4-KiB pool, c16; two pairs | 102,663.91, 102,230.89 objects/s | 16,553.31, 13,681.03 objects/s | 102,447.40 vs 15,117.17 objects/s | Both Cairn pair wins; this case reports objects listed/s, not request/s. |

The first PUT screen's Cairn stage sampler retained 152 completed object samples
with zero drops. Count-weighted means were body 34.47 ms (input wait 11.79,
hash 13.00, sink 9.65), namespace 20.56, file finalize 22.24,
destination-directory sync 19.36, staging create 3.96, permit wait 0.01 ms.
These are **wall waits in concurrent requests**, not exclusive CPU or independent
throughput shares. In the 51.71-MiB/s Cairn arm, namespace and directory-sync
means were about 38.6 and 40.5 ms; in a 106.33-MiB/s arm they were about
13.3 and 13.6 ms. This implicates variable filesystem/scheduling wait, but
does not prove that one specific syscall, hashing algorithm or metadata query is
the root cause. An unrelated VM was present during multiple arms and the
names-only census measured substantial competing CPU, including in the timed
portion. Host I/O pressure also moved. It is not valid to adjust a throughput
score by subtracting the observed competing CPU.

The first matrix charged **555.062741 measured client-load seconds**. Warp
post-benchmark cleanup makes some RustFS client processes much longer-lived than
their analyzed interval, so whole-client CPU and pressure deltas are not
request-aligned per-operation costs. The test does not verify power-loss
equivalence; it verifies the configured strict controls and successful S3 traffic.

### Follow-on instrumentation-overhead screen (declared before load)

The completed primary/protected matrix used the sampled instrumentation build.
Because the one-in-32 stage sampler has **not** passed an overhead gate, build a
second executable from exact clean `HEAD` in a task-owned source archive, with the
same pinned toolchain, release profile and production web bundle. Compare clean
HEAD as `control` against the already measured instrumented executable as
`candidate` in five alternating 12-second `put_1m` Cairn/Cairn pairs using
`conformance/cairn_put_ab.py`, FULL/shard-1/linger-0, fresh store per arm.
This is an **overhead diagnostic**, not a RustFS adoption result or a new
optimization. Retain the exact binary hash, successful-operation/error checks,
host-pressure census and every consumed client-load second. A noisy or contradictory
pair sequence is inconclusive; do not infer that the sampler improves performance.
The cumulative 1,200-second cap still applies. No protected workload is required
for a diagnostic-only instrumentation decision; keep the sampler unadopted unless
its own overhead and full validation gates pass.

The clean `HEAD` archive build completed with the same pinned release toolchain;
its executable SHA-256 is
`20f02a7af80a2888f6dcb4048244316dd80d1edf6e1b05508689921e7a6b61bc`.
The instrumented executable remains
`f6220d3c9294da51e4e06759bbd4e3cf196f18e0556064adaba7bb3a521c9df6`.
No measured traffic for this follow-on had started when these identities were recorded.

The [five-pair overhead evidence](performance-evidence/2026-09/performance-shared-node-overhead-raw-2026-09.json)
has zero errors and no sample drops. Clean-control arm scores were 87.75, 84.00,
82.35, 83.57 and 47.61 MiB/s. Instrumented-candidate scores were 50.39,
91.86, 73.45, 79.68 and 46.97 MiB/s. Candidate/control paired ratios were
0.574, 1.094, 0.892, 0.953 and 0.987 (median 0.953). The first pair's collapse
and subsequent reversal preclude attributing the spread to a one-in-32 timer;
the low-overhead gate is **inconclusive**, not passed. This diagnostic charged
**161.572254 measured seconds**, bringing the cumulative ledger to
**716.634994/1,200 seconds**.

### Clean-HEAD strict control confirmation (declared before load)

The timer-overhead screen did not establish a safe overhead bound (see results
below). Therefore, use the remaining metered allowance for **five alternating
clean-HEAD Cairn/RustFS strict `put_1m` pairs** at 12 seconds requested, concurrency
16, fresh store per arm, FULL/shard-1/linger-0 versus verified RustFS strict.
This is a separate sensitivity check of the shipped checkout revision, not pooled
with the instrumented five-pair screen. Use the same exact Warp binary and capture
the host census. Omit post-load metrics drain because clean HEAD lacks the sampler;
all client-load seconds still count. Stop if the cumulative 1,200-second cap would
be crossed; a partial run is not a five-pair result.

The clean-HEAD confirmation completed all five pairs with zero errors, no sample
drops or cap hits, and strict RustFS mode readback before/after each arm. Exact
[per-arm evidence](performance-evidence/2026-09/performance-shared-node-clean-put-raw-2026-09.json) records
host pressure and process census. In pair order, Cairn scored 60.43, 82.81,
47.91, 52.73 and 46.96 MiB/s; RustFS scored 69.60, 115.72, 58.67, 56.53 and
64.23 MiB/s. Cairn/RustFS paired ratios were 0.868, 0.716, 0.817, 0.933 and
0.731 (median **0.817**, five RustFS pair wins). Arm medians were **52.73 vs
64.23 MiB/s** (Cairn 82.1% of RustFS). This is a remaining strict-mode gap on
this shared node, but nowhere near a stable 1.93x deficit. Absolute control
scores ranged from 56.53 to 115.72 MiB/s, so the exact ratio is not a hardware
constant; an unrelated VM and variable I/O pressure remain visible. The
instrumented and clean screens are different binaries and are **not pooled**.

The clean-HEAD run charged **283.715596 measured seconds**. The complete new
campaign ledger is **1,000.350591/1,200 measured seconds**, leaving
**199.649409 seconds unused**. No candidate optimization was implemented or
adopted in this resumption: the evidence does not isolate a safe change that
clears the 20%/five-pair adoption gate and protected-workload requirements.
Peak sampled server RSS during clean-HEAD PUT was 37.45–39.71 MB for Cairn and
244.76–306.68 MB for RustFS (per-arm sample peaks, not PSS). The median sampled
peaks were 38.22 MB and 269.66 MB respectively. These indicate a smaller Cairn
process footprint for this workload but omit allocator/kernel cache and do not
prove steady-state memory efficiency. Per-live-object bytes/inodes and comparable
CPU-seconds per analyzed PUT remain unavailable; the runner's whole-client-load
CPU and footprint intervals include preparation/cleanup. No p99 claim is made
because per-arm samples do not meet the 10,000-successful-operation threshold.

## Cleanup and decision

After all arms exited, `docker ps -a` showed only the pre-existing `orva`
container. The exact task-owned scratch root
`/var/tmp/cairn-performance-adoption2-7gRRmk`, generated `web/node_modules`,
`web/dist` and `conformance/__pycache__` were removed. The two exact images
pulled for this campaign (`rust:1.97.1-bookworm` and
`quay.io/minio/aistor/warp:v1.8.0`) were removed; no other image or container
was touched. The six compact JSON evidence files and this report remain in
`docs/`. The 199.649409 unspent measured seconds are not carried into another
campaign by implication.

**Decision:** no architecture or production optimization is adopted. The clean
strict PUT gap is real in these five pairs but is smaller than the old
mismatched-durability headline. The stage split warrants a controlled
request-aligned foreground filesystem/Writer experiment before changing
namespace/cleanup scheduling; the current wall-stage samples and whole-client
host counters cannot assign exclusive costs. The sampler itself remains
unadopted because its overhead gate was inconclusive. A future change needs
the full correctness, durability, benchmark and protected-workload gates
specified in the execution tracker.
