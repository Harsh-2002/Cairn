# Canonical SQLite metadata capacity evaluation

Phase 5A uses the production `SqliteMetadataStore`, one actual Writer and its WAL read pool.
This is a populated metadata workload, not raw SQL/KV insertion or an empty overwrite ring.
Implementation and fixed-fixture validation are in progress; no capacity measurement has run.
The following matrix and criteria are declared before admission, with the actual source and
prebuilt binary identities recorded by the coordinator.

## Dataset and boundaries

Prepare 100,000 authoritative object-version rows through the canonical Writer: 80,000 current
data rows, 10,000 historical data rows and 10,000 current delete markers. Use nested prefixes and
bounded deterministic keys. Current/history/marker totals are distinct and verified. Separate
small, explicitly counted multipart and mutation fixtures must not be passed off as seed rows.
One hot-bucket database and a second 16-bucket database both use one Writer; bucket distribution
is not database sharding. Stop preparation and report INCONCLUSIVE if the declared seed deadline
expires. Approaching a million rows is optional and requires remaining campaign allowance.

Use `synchronous=FULL`, eight WAL readers, 8-MiB per-connection cache and zero mmap for both
populations. Direct library defaults are insufficient because they select NORMAL. The direct
store excludes the auth-path application cache; label it as absent, not measured at zero.
All payload paths are metadata-only fixtures. Exact journal admission, publication, intent
resolution and cleanup settlement still use real mutations, but no physical object files or
physical recovery evidence are implied.

## Mutation and observation contract

The bounded mix includes versioned overwrite/appends and permanent deletion, marker insertion
and removal, successful and rejected conditions, multipart reservation/publication/replacement/
abort, current/version reads, prefix/delimiter pagination, exact journal settlement and jointly
published replication outbox claim/settlement. Give every family a distinct outcome count and
latency histogram. Expected condition rejections are counted separately from errors. Each
worker owns an eight-slot deterministic ring. Ring keys sort adjacent to deterministic seed keys
across the populated prefix/index range, while leaving every seed version unchanged. Prefix,
delimiter and version pages rotate among populated prefix groups. This is scattered transient
churn in a populated database, not random overwrites of the authoritative seed itself. Fixture
histories and journals must not grow without an operation cap. Read-only seed/list verification uses bounded pages, never a whole database
heap inventory. Keep row, quota, part, intent and cleanup invariants independently observable.

Drain the Writer's bounded stage buffers frequently and at every phase boundary. Any dropped
stage observation prevents a service-demand conclusion. Only begin/apply/commit durations
represent serialized observed occupancy, and those still include I/O and scheduler delays.
Admission/queue durations overlap callers and cannot be added as Writer service. Observe the
actual Writer thread's CPU ticks independently; process CPU is not Writer CPU, and Writer CPU
is not pure SQLite engine CPU. Exact cleanup claims invalidated by concurrent quota linkage are counted; only a later applied
Writer settlement may retire them, and verification still requires zero residual debt.
Record successful-family histogram precision and count; p99
requires at least 10,000 successful observations in that family.

Keep separate database/index bytes, peak WAL, post-checkpoint WAL, page/freelist information,
Writer occupancy, queue observations, request throughput and latency, Writer/process CPU, RSS
and file descriptors. Record `dbstat` unavailability explicitly. The laboratory invokes the same Writer checkpoint control seam at a sampled 64-MiB WAL trigger,
records checkpoint stages separately, and performs a final truncating checkpoint. This explicit
loop replaces the absent server background scheduler; it is not a measurement of that scheduler.
Measure checkpoint and fresh reopen with history/parts and exact expected state; this is database reopen, not full-node
recovery. A process restart is not a cold filesystem-cache trial.

## Budget and decisions

All preparation, workload, observations, verification, checkpoint/reopen, reduction and cleanup
share the existing campaign's 600-second metadata allowance and global 100-GB footprint ceiling.
Reserve both seed populations, histories, journals, parts, outbox, SQLite/WAL, observations and
cleanup headroom before work. Builds, tiny correctness fixtures and normal CI are recorded
separately. No unbounded retry or automatic larger seed follows an incomplete run.

The fixed matrix prepares one hot-bucket database and runs concurrency 4, 32, 128, 32, 4; a
fresh 16-bucket database runs 32, 128, 32. Each phase admits ten seconds of operation bundles,
then joins actual owners and verifies the exact population. Per-family latency is separate
from operation-bundle throughput. The C32 repeats must drift no more than 10%; hot-bucket C4
repeats have the same bound. A Writer-limit finding for a population requires C128 queue
nonempty in at least 80% of samples, begin/apply/commit occupancy at least 80% of elapsed time,
and no more than 20% throughput gain from C32 to C128. Both controls and all stage/family/quota
observations must be complete. A qualifying population permits a conditional alternative
experiment for that workload only. It does not imply the other population or the full S3 stack
has the same bottleneck. Missing attribution, uncontrolled drift or incomplete seed/outcome
verification means INCONCLUSIVE. Keep SQLite absent a demonstrated limiting service demand and
a qualifying semantics-equivalent alternative result. The capability comparison includes
SQLite, libSQL/Turso, redb, Fjall and RocksDB; transactional Fjall is the first conditional
experiment, and RocksDB remains research-only. Production replacement always needs complete
semantic/migration/restore parity and the human architectural decision.

## Fixed-fixture validation

Nine Rust metadata tests and all-target laboratory Clippy pass. Coverage includes the full
mutation-family mix, 128 concurrent owners, a stable 1,024-key scattered ring, exact seed identity,
quota/debt preservation and fresh FULL reopen. All thirteen Python coordinator tests pass, including
the actual 100-row CLI with all eighteen outcome families and no residual SQLite sidecars after
close. The CLI fixture exposed and pinned a reporting-order fix: read-only size observation now
precedes the final database close. It cannot reopen the database and recreate sidecars afterward.
A retained WAL reader regression requires busy checkpoint attempts to remain eligible for retry
and verifies truncation after reader release. Seed failure closes fresh admission and drains all
started owners. The coordinator reads final process CPU after exit and before reaping.
These are correctness checks, not the 100,000-row capacity experiment or a Writer-limit finding.
The integrated full repository and final-head CI gates remain pending.

Process RSS includes the in-process generator and observation buffers as well as SQLite and
the runtime. Each worker has at most eighteen pairs of fixed 2,048-bin histograms (589,824 bytes
of bin storage), plus bounded counters/map entries. It is not a measurement of SQLite-only RSS.
Configured SQLite caches, actual Writer thread CPU, process CPU and database/index/WAL bytes are
reported separately; no process-wide counter is relabeled as pure SQLite engine cost.

SQLite connection-level cache-hit/miss counters are unavailable through the canonical store's
public API and are recorded as unavailable. A new observer connection would describe its own
cache, so it cannot stand in for the Writer/read-pool counters. Cache capacities are controlled
and reported; no hit-rate, cache-cold or cache-only attribution claim follows from them.
