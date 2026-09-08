# Storage attribution — 2026-09-08

Status: experiment design recorded before execution; results pending. This is Phase 2B of
[the approved plan](storage-evolution-plan.md), following the bounded harness in PR #84.
No architecture adoption decision follows from this diagnostic screen.

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

Pending. Every claimed bottleneck requires correlated observations. Internal blob wait
splits, live SQLite/application cache ownership, runtime active tasks and in-flight buffers
remain unresolved unless the collected evidence identifies them. An RSS plateau alone will
not be presented as evidence that memory is leak-free.
