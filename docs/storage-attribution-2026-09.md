# Storage attribution — 2026-09-08

Status: **INCONCLUSIVE for complete bottleneck attribution**. The bounded screen produced
usable baseline, Writer timing and heap evidence; CPU stacks and several internal wait splits
remain unavailable. This is Phase 2B of [the approved plan](storage-evolution-plan.md), following
PR #84. Retain the current architecture. The predeclared design below was committed before
execution. [Machine-readable evidence](storage-attribution-2026-09.json) preserves the run
identities, executable hashes, configuration, counters, cycles and selected allocation evidence.

## Questions and declared measurements

Identify which costs are observable in real SQLite Writer/read-pool operations, raw blob
stage/read/delete, and complete signed S3 PUT/GET/DELETE. Keep one hot bucket separate from
16 buckets. Do not subtract rates between layers: the metadata fixture includes LIST while
the blob and S3 fixtures include physical deletion, and their scheduling/caching differ.

Primary observation: successful three-operation transactions per second, with the three
operation latencies reported independently. Protected observations: operation errors,
byte/metadata verification, per-operation latency, anonymous/file memory, descriptor/thread
counts, and device/host pressure. This is attribution, not the five-pair adoption comparison.
Per-operation p99 is unavailable below 10,000 successful samples. Host contention, missing
stacks or missing internal measurements must remain visible limits.

All initial payloads are 4 KiB, generated with seed `0x5eed`. Each invocation has three
3-second load / 2-second idle cycles on one process/backend, an aggregate 300,000-transaction
cap and a 60-second whole-invocation allowance. Rows/keys use bounded worker rings. The cap
is a space/admission bound, not a required request count. A capped interval is inconclusive.

| Layer | Buckets | Concurrency | Mode |
|---|---:|---:|---|
| metadata | 1 | 4 | unprofiled |
| metadata | 16 | 4 | unprofiled |
| blob | 1 | 4 | unprofiled |
| blob | 16 | 4 | unprofiled |
| S3 | 1 | 4 | unprofiled |
| S3 | 16 | 4 | unprofiled |
| metadata | 1 | 32 | unprofiled |
| S3 | 1 | 32 | unprofiled |
| metadata / S3 | 1 | 4 | separate CPU stack attempts |
| metadata / blob / S3 | 1 | 4 | separate heap stack collection |

The profiler and decoding allowance may reduce this screen; no additional time is authorized
by an inconclusive result. The campaign ledger includes preparation, load/idle, profiles,
verification and cleanup. Builds, profiler installation and fixed correctness fixtures are
tracked separately. Artifact decoding must also be admitted and charged before execution.

## Provenance and interpretation

The server is built from production revision `1eed10f` with
`CARGO_PROFILE_RELEASE_DEBUG=2 CARGO_PROFILE_RELEASE_STRIP=false cargo build --locked --release
--bin cairn`. The layer driver uses its separate locked release profile (`opt-level=3`,
`debug=2`, `strip=false`, thin LTO). Final executable hashes and harness revision will be
recorded with the results. Measurements start only after both builds finish.

The metadata fixture explicitly uses FULL durability, eight WAL readers, 8 MiB per SQLite
connection and no mmap. The S3 server pins these same database settings, one shard, eight
Tokio workers, 512 maximum blocking threads, and 64 read/write blob permits. Its bounded
application/auth caches and normal background maintenance remain enabled; update checking
is disabled. Complete nonsecret overrides and source-default references accompany each run.
Raw objects are used: this screen makes no compression/encryption throughput claim.

Linux RSS includes anonymous, file and shared-memory contributions; proportional accounting
and allocation stacks are needed to interpret ownership. Process restart does not clear the
host page cache. [Linux `/proc` documentation](https://docs.kernel.org/filesystems/proc.html)
describes these counters. Heaptrack records allocation calls and backtraces;
[its upstream documentation](https://github.com/KDE/heaptrack) describes the collection and
analysis tools. Outstanding allocations at process exit are not, by themselves, a leak
diagnosis; load/idle ownership and teardown behavior must be examined.

CPU sampling is subject to this host's perf permissions (`perf_event_paranoid=4`). Record
the actual profiler result; do not change global host settings to manufacture availability.
Heaptrack 1.5.0 and its missing runtime libraries were extracted into a task-owned tools
directory from version-pinned Ubuntu packages verified against repository SHA-256 values.
No production dependency or system installation changed.
The task-owned Heaptrack launcher replaces its hard-coded `/tmp/heaptrack_fifo$$` path with
`${TMPDIR:?}/heaptrack_fifo$$`, keeping the FIFO within the campaign directory; this local
launcher adjustment is included in the tool provenance. The coordinator signals the exact
owned server and waits for the profiler wrapper to finish before terminating the group.

## Results

The host has four Intel i5-6500TE cores; the owned data directory is on ext4 `/dev/sdb1`.
Other host services remained running. Neither disk cache nor host contention was controlled.
The server SHA-256 is `ff77c634e160d9ca374d4d281cc81257d6097cf47de65bf686439a2e211a8d66`;
the layer driver, built from `cd5216a`, is
`78030c38148008ade9d30f0d29802d824a86e2617aafca0e2ec0394d80d39791`.

### Unprofiled observations

Rates below are the median of three cycles in **one invocation**, in three-operation
transactions/second. These are not five independent paired comparisons, and the layers have
different operations. All eight completed cases verified their operations without errors.

| Layer | Buckets | Concurrency | Transactions/s | Successful transactions | Idle RSS, cycles 1 → 3 |
|---|---:|---:|---:|---:|---|
| Metadata | 1 | 4 | 2,143 | 19,131 | 10.55 → 11.30 MiB |
| Metadata | 16 | 4 | 2,044 | 18,714 | 10.46 → 11.14 MiB |
| Blob | 1 | 4 | 2,153 | 20,269 | 6.18 → 6.37 MiB |
| Blob | 16 | 4 | 2,438 | 22,711 | 6.13 → 6.43 MiB |
| S3 | 1 | 4 | 472 | 4,321 | 20.44 → 22.34 MiB |
| S3 | 16 | 4 | 392 | 3,790 | 21.62 → 23.50 MiB |
| Metadata | 1 | 32 | 3,832 | 34,464 | 15.67 → 17.31 MiB |
| S3 | 1 | 32 | 736 | 6,764 | 27.59 → 32.43 MiB |

Only metadata/concurrency-32 has at least 10,000 successful samples **per operation per
cycle**: its PUT p99 is 15.93–20.84 ms. Other per-cycle p99 values are unavailable; averaging
quantiles or combining counts after discarding samples would not produce a valid pooled p99.

In the hot metadata/concurrency-4 fixture, sampled Writer batches spend 5.73 s in commit,
2.54 s in apply and 0.13 s in begin across approximately nine seconds of load. Admission is
4.28 ms in aggregate; queued requests accumulate 8.42 s of overlapping wait time. This
correlates a busy serialized Writer transaction path with PUT latency, whose cycle medians
are 1.18–1.24 ms. Commit is wall time including scheduling and durability waits, not a CPU
profile. At concurrency 32, batching increases throughput, the queue reaches 32, and 609
timing samples are dropped: its timing attribution is partial even though all operations
complete. These observations do not establish the capacity of Phase 5's larger workload or
the benefit of another engine. Explicit post-load checkpoints take roughly 23–45 ms.

Blob stage medians are 1.08–1.18 ms, versus 0.077–0.082 ms for reads and 0.086–0.108 ms
for deletes. Stage is the largest measured component; permit, buffering, encoding, file I/O,
rename and directory-sync portions are not independently timed here. S3 includes further
protocol/authentication/metadata work and cannot be obtained by subtracting these layer rates.
The Python S3 client uses about 0.34–0.51 CPU cores at concurrency 4 and 0.65–0.83 at 32;
client saturation is not ruled out by this screen. Server CPU use is about 0.69–1.19 cores.

Whole-interval device observations, including idle and unrelated host work, show roughly
30–91 MiB/s writes, 9–35% busy time, average queue 0.09–0.39 and read/write await
0.06–0.21 ms across these cases. They do not demonstrate saturated device bandwidth;
serialized synchronization latency can still matter. Raw host pressure and device counters,
and S3 metric first/last/maximum samples, are preserved in the JSON evidence.

### Allocation ownership and memory limits

The separate heap runs change scheduling and throughput and must not be compared as baseline
performance. Heaptrack's reported peak tracked heaps are 2.32 MB (metadata), 1.43 MB (blob)
and 8.00 MB (S3), using its decimal units. These are different from RSS and exclude several
classes of process/kernel memory. Approximate phase alignment uses process-start ticks and
timestamped samples, excluding 250 ms at both boundaries to avoid including the adjacent
load or teardown in idle measurements.

| Profile | Tracked live heap late in idle, cycles 1 / 2 / 3 | Observed allocation owners |
|---|---|---|
| Metadata | 1.953 / 2.008 / 2.065 MB | SQLite lookaside/page-cache paths account for 1.76 MB of the tracked peak; Writer commit-sample storage and temporary laboratory latency vectors also appear. |
| Blob | 0.133 / 0.136 / 0.138 MB | Concurrent staging buffers account for about 1.05 MB at peak; latency vectors are transient. Thread TLS and small runtime allocations remain at exit. |
| S3 | 3.066 / 3.749 / 5.685 MB | Prometheus DDSketch backing vectors, metric-rendering copies and SQLite caches appear prominently. |

At server exit, 2.88 MB of the 3.08 MB still-live allocation total comes from Prometheus
DDSketch backing vectors. The recorder is installed globally in
`crates/cairn-server/src/observability.rs:28`; the pinned `metrics` implementation retains
that global recorder with `Box::leak` (`metrics-0.24.6/src/recorder/cell.rs:45`). This explains
the dominant exit owner; it does **not** prove that every memory increase is bounded or
unreachable. The metadata/blob exit totals are about 11/14 KB, including thread TLS.
Idle RSS and live heaps still rise during this short screen. Two-second idle periods do not
establish cache expiration, worker retirement or long-duration stability.

Both `perf` attempts fail because `perf_event_paranoid=4` denies `cpu/cycles/Pu`; no CPU
stacks were recorded and global host settings were unchanged. Application-cache live bytes,
exact SQLite allocator/cache subdivisions at each phase, active tasks, in-flight buffer bytes,
and blob-internal wait splits remain unresolved. The historical multi-gigabyte RSS observation
is neither reproduced nor explained by this tiny overwrite-ring fixture.

### Accounting and reproducibility

The first blob run stopped on an observer race with staging-file unlink; the first S3 heap
run stopped on an auxiliary metrics timeout. Both remain charged and excluded from the
accepted observations. Regressions now tolerate disappeared staging entries, preserve
telemetry gaps, allow profiler flushing, validate CPU/device units and exclude heap-phase
boundaries. An unsuccessful interactive reduction is also recorded as FAIL. No production
optimization was made in this phase.

Collection, decoding, reduction, evidence archival and artifact cleanup consumed
**250.787718 seconds** of the 600-second baseline allowance, including a conservative second
for final report serialization. Recorded peak owned footprint is **219,529,216 bytes**.
Owned data, processes and raw profiles were removed; the persistent ledger and compact
results remain for subsequent phases. **349.212282 seconds** of baseline time carries forward.
Unused time carries forward within the single 3,600-second campaign. Builds, focused
correctness fixtures and CI are separate. Use `analyze.py` to decode traces and
`summarize.py --root CAMPAIGN --device DEVICE` for charged reduction; GNU
`c++filt --format=rust` makes the decoded Rust stack names readable.

Local validation: 21 Python regressions, both real Rust layer fixtures, standalone Clippy,
formatting and shellcheck pass. The final-commit repository CI and review remain the merge gate.
