# Conditional metadata comparison — Phase 5B

The preregistration below was committed before alternative performance admission. The recorded
outcome is **INCONCLUSIVE / KEEP SQLite**: Fjall seed preparation reached its fixed deadline,
so none of the ten paired load arms ran. See the recorded result at the end. The isolated adapter and its
fixed correctness fixtures do not register an engine in Cairn or change the production lockfile.
SQLite remains the production source of truth. The capability and migration gaps remain in
[the engine comparison](storage-metadata-capabilities-2026-09.md).

## Trigger and fixed comparison

The [canonical capacity result](storage-metadata-capacity-2026-09.json) qualifies **one hot bucket**:
C128 serialized Writer occupancy is 95.22%, its queue is nonempty in 81.87% of samples, and the
gain over C32 is 10.26%. Its repeated controls pass. The distributed population's 79.77% queue
fraction misses the declared 80% threshold; it does not qualify. These are observed Writer
service limits, including fsync and scheduling, not isolated SQLite instruction costs.

Use one prebuilt optimized `cairn-metadata-comparison-lab`, the same eight Tokio workers, C128,
seed `0x5eed`, and one hot bucket. Prepare **100,000 authoritative versions separately in each
engine**, preserving the 80,000 current / 10,000 historical / 10,000 marker distribution and
the persistent auxiliary multipart session/part. The shared workload and run modules supply
both engines' mutations, reads, exact checks, full five-bundle cycles and bounded histograms.
The cycle has fixed outcome multiplicities, including two version appends, four permanent
deletes, two multipart reservations and eleven journal-settlement observations. Every other
family occurs once; the conditional-reject family must contain only expected rejections.

Prepare SQLite, then Fjall. Preserve each seed database across **five paired confirmations**,
in this order: SQLite/Fjall, Fjall/SQLite, SQLite/Fjall, Fjall/SQLite, SQLite/Fjall. Each of the ten
load arms uses a **fresh process**, verifies the complete seed and independent bucket/principal
quotas, admits five seconds of work, finishes every admitted five-bundle cycle, verifies again,
checkpoints, and closes every database/reader/engine-worker owner. The measured wall includes
the final cycle tail. Run one final fresh-process verification for each engine. No arm reseeds,
rewrites a descriptor, repairs a database, or silently starts an empty store on resume. A resume
descriptor must match the persisted generation as well as the seed recipe.

This choice shares preparation while retaining fresh-process arms. It fits the remaining
metadata allowance; it does not turn five seconds into steady-state, cold-cache or long-soak
evidence. Filesystem cache is uncontrolled. Do not extend a slow arm, change its dataset,
shorten its mix, increase concurrency, or retry after observing a result.

## Durability, indexes and ownership

The exact candidate is `fjall = "=3.1.10"`, with default LZ4 support compiled and the `metrics`
feature enabled. The application row keyspace explicitly disables data/index block compression;
internal engine metadata retains library defaults. SQLite uses FULL, eight WAL readers,
8 MiB per connection, zero mmap and zero group-commit linger. Fjall uses one
`SingleWriterTxDatabase` actor and explicit `PersistMode::SyncAll` on **every batch commit**.
Both use opportunistic batches of at most 256 and a 4,096-slot queue, without an added linger.

Fjall conditions read a transaction snapshot plus prior accepted batch members. A bounded child
overlay stages one member's data, secondary indexes, counters, journals, parts and outbox.
Only a successful member merges into its parent; all accepted members then enter one native
transaction. No success reply precedes its durable commit. Native commit errors close write
admission; checked shutdown joins the Writer and returns its actual error. Cancelling a caller
does not cancel admitted physical work or the owned shutdown task. Read permits are acquired
before blocking work and remain owned by that work; close excludes new admission and drains all
eight read permits before dropping the last database handle and joining native workers.

Keys use order-preserving escaped tuples and descending version components. Current/version,
prefix/delimiter, part/session, due-outbox, path-owner, cleanup-ready and quota-link indexes are
maintained transactionally. Reads use at most 256 rows per page. Child/batch overlays each have
16-MiB and 16,384-edit bounds; keys are capped at 8,192 bytes and encoded values at 32 KiB.
The declared generator additionally bounds owners, fan-out, sessions, part slots and input values.
This is a fixed-trace adapter, not a general untrusted-input metadata service. Other trait
methods and unsupported mutation shapes return errors. Object Lock, tags, authorization,
completion, remote-upload state, imports and other unimplemented surfaces do not acquire parity
from these results.

Fjall's block cache is 72 MiB, its row memtable threshold is 32 MiB, its journal threshold is
64 MiB, its descriptor cache is capped at 64, and it has two engine workers. These thresholds
are not an RSS ceiling. The pinned library's hidden global write-buffer setter is not enforced
by this version and is deliberately unused. Record actual write buffers, sealed memtables,
background work and process memory rather than claiming that setter is a bound.

## Observations and decision gates

The primary metric is acknowledged operation bundles per complete measured wall second.
Require at least **20% median paired improvement**, and no pair with a throughput loss greater
than 10%. Protect paired median per-family mean and observed maximum latency, process RSS,
database open, checkpoint and checked-close latency: none may worsen by more than 10%.
The observed maximum is not called p99. A family needs 10,000 outcomes in an arm before its
2%-wide histogram can report a p99 interval; all families need at least 100 outcomes and the
exact cycle distribution. Record every pair, never only a favorable aggregate.

Each engine's repeated C128 throughput controls must have `max/min - 1 <= 10%`. Missing arms,
ownership/checkpoint failures, unexpected outcomes, lost observations, changed seed/quota state,
or control drift make the comparison INCONCLUSIVE. A complete, controlled comparison can PASS
as an experiment while `candidate_qualified=false`; that is a measured rejection, not adoption.
The paired medians are descriptive; no confidence interval or statistical significance is claimed.

Retain actual Writer TID CPU and admission/queue/begin/apply/commit counters separately from
whole-process CPU. Whole-process CPU/RSS include the generator, histograms, blocking readers
and native workers. Sample SQLite stages and WAL every 20 ms, including the same canonical
checkpoint seam at the sampled 64-MiB trigger. Fjall reports constant-space stage totals,
cache activity, write buffers, journals, current SST counts/bytes and compaction counts/time.
A bounded physical table listing classifies current versus non-current bytes against one
manifest version. Non-current means obsolete/retained **or in-progress output**, not proved
garbage. Report disappearance races; sampled peaks are not continuous maxima. This observation
briefly pins a manifest version, and its cost belongs to the candidate arm.

All physical files, including internal metadata, obsolete tables, compaction output, journals
and logs, count toward the campaign footprint. Linux process I/O counters after engine-worker
join capture total I/O, including compaction; no unavailable per-compaction byte counter is
invented. Raw 50-ms samples retain unavailable `/proc/PID/io`/descriptor readings explicitly;
the driver's own final I/O counters remain required. Record post-close logical/allocated disk
bytes, descriptors, whole-process CPU and CPU per completed bundle as resource comparisons.
No process restart is presented as a cold filesystem-cache trial.

## Correctness evidence and budget

The fixed fixtures cover the full family mix, 128 concurrent owners, exact seed/part/quota
preservation and full close/reopen; byte-order and paged overlay merges; and a late outbox
failure that leaves every persisted index/counter unchanged. An owned subprocess uses native
`RLIMIT_FSIZE`/EFBIG to force a real journal-write failure without a success reply. SIGKILL
immediately before native commit and after durable commit but before replies preserves the
appropriate complete state on fresh reopen. These are process-kill and I/O-error fixtures,
not a power-loss experiment or full production backup/migration validation.

The coordinator first revalidates the canonical hot-bucket prerequisite. Both seeds, all fourteen
processes, observations, reductions and cleanup share the campaign's **600-second cumulative
metadata allowance**, not a new allocation. Reserve 300 seconds for this comparison, including
15 seconds of cleanup headroom; prior metadata measurement/export used 292.003397 seconds.
Retain the remaining allowance for final evidence export. The global 100,000,000,000-byte
combined footprint ceiling remains unchanged. Builds, small correctness fixtures and normal CI
are separate. On interruption or exhaustion, stop owned process groups, retain incomplete
evidence, charge actual time and keep SQLite. No automatic second attempt is authorized.

Production adoption would additionally require complete semantic, migration, backup/restore,
compatibility and operational parity and a human architectural decision. This experiment cannot
change the production metadata ceiling, even if the bounded candidate qualifies.

## Recorded result and Phase 5C decision

[Machine-readable evidence](storage-metadata-alternative-2026-09.json) and the
[bounded raw archive](storage-metadata-alternative-2026-09.raw.tar.gz) retain the exact source
`5b3b95a325f6a6624bc979f6b9574a0f441be7e2`, binary/toolchain hashes, configurations, process
samples and incomplete outcome. Both engines used that same optimized executable.

SQLite completed preparation, full seed/quota verification, checkpoint and checked close in
95.338541 seconds within its 100-second deadline. Fjall's preparation reached its fixed
105-second deadline and returned `metadata workload deadline reached before admission`.
The coordinator observed process exit after 105.441568 seconds. The incomplete candidate has
no final verified seed or checked-close result; its process samples are retained as observations,
not treated as a completed engine comparison. Every owned process exited and all temporary
databases and original artifacts were removed after archive verification.

**Zero of ten load arms and zero of five pairs completed.** There is no paired throughput,
protected-latency, memory-adoption or p99 conclusion. In particular, a preparation deadline is
not evidence that Fjall is universally slower, that its native engine is the cause, or that
SQLite is the optimal engine. No shortened workload or automatic retry followed the result.

Phase 5C therefore concludes **KEEP SQLite**. Phase 5A demonstrated a serialized Writer service
limit for the declared 100,000-row hot-bucket metadata trace. That occupancy includes application
mutation work, fsync and scheduling; it does not isolate SQLite computation or establish the
bottleneck of a full S3 request. The distributed population missed its declared queue criterion.
The conditional replacement experiment supplied no qualifying benefit. Production semantics,
migration, backup/restore, physical recovery and power-loss parity remain unproven for the
laboratory candidate, and production metadata dependencies remain unchanged.

Comparison plus evidence export/cleanup charged **204.772213 seconds**; cumulative metadata
consumption is **496.775609 / 600 seconds**. The complete campaign stands at
**1,415.415082 / 3,600 seconds**, with **1,778,102,272 / 100,000,000,000 bytes** recorded peak
combined footprint and no active reservation. Unused allowance does not authorize a changed
experiment or another attempt.

The full local root gate passed, including both Clippy configurations, 1,466 default and 1,492
all-feature tests, two doctests, web lint/build/audits, dependency audits and installer checks.
The final adapter/coordinator sources pass 98 laboratory Rust tests (one ignored child helper)
and all 104 Python tests with every live fixture enabled. These are correctness results,
separate from the inconclusive bounded performance comparison. Final-commit CI and integration
remain required.
