# Storage evolution implementation plan

Approved 2026-09-08, baseline `e38bc6c`. This is the implementation checklist for five phases,
with sequential small PRs. Check an item only after its final commit passes focused tests,
relevant review and normal CI, and its PR is merged. Each PR updates this record and the owning
specification. Experiments may correctly conclude **retain the current design**.

## Delivery checklist

- [ ] **1A: writer/read format limits** — in progress on `storage/01a-container-limits`.
  Share checked 9-byte-entry/64-MiB-index arithmetic; reject before copying/encoding excessive
  input. Known encoded lengths fail before staging/preallocation, multipart totals use checked
  addition, unknown streams abort safely. Preserve raw-file limits and the existing S3 error.
  Test exact and exceeded limits, overflow, chunk boundaries, synthetic small encoder ceilings,
  cleanup and retryable multipart completion. Document actual encoded-object ceilings.
- [ ] **1B: bounded encoder index spool** — implementation/testing active on
  `codex/storage-01b-index-spool`. Replace the complete index vector/final output copy
  with an owned temporary spool and bounded payload/index sinks. Preserve v1–v3 bytes and block
  identities. Append the spool and existing authentication before the final file's durability
  barrier; no independently committed sidecar. Cover spool failures, ENOSPC, cancellation,
  detached filesystem work and startup cleanup. Input and output batches must also be bounded.
- [ ] **1C: bounded verified index reader**. Keep the 64-MiB cap; use 65,529-byte pages
  (7,281 entries). Stream initial structural/HMAC validation, retaining provisional SHA-256 page
  fingerprints and physical starting offsets. Publish a usable reader only after full validation.
  Reread the same descriptor, verify a whole page before interpreting it, retain trusted geometry,
  and transfer the prepared reader from probe to body instead of parsing twice. Move allocation
  bounds into a blob-owned interface used by replication; retain leases through blocking work.
  Test tampering after validation, swapped pages, boundaries, downgrade/wrong-key refusal and
  bounded memory. Plain v1 gains no new initial authentication. Initial index I/O stays linear;
  this does not enable 5-TiB encoded objects. A new format needs a separate reviewed decision.
- [ ] **2A: bounded diagnostic harness**. Shell launcher/Python coordinator and Rust layer drivers
  under `conformance/storage_lab/`, outside production dependencies. Explicit prebuilt binaries,
  owned `/SSD` directory, persistent campaign ledger, bounded process groups and cleanup.
  Record hashes/revisions/configuration/tools/hardware and PASS/FAIL/INCONCLUSIVE/CANCELLED.
  Missing tools, exhausted budgets or inadequate samples never become passes.
- [ ] **2B: CPU/memory/storage attribution report**. Separate real metadata, blob and S3 work;
  one hot bucket versus many buckets. Separate unprofiled rates from CPU and heap profiling.
  Record live allocations, anonymous/file memory, cache use, tasks/threads/descriptors, writer
  stages, filesystem waits and device/host pressure. Three equal load/idle cycles; no RSS-only
  leak claim. Further tuning needs a separate evidence-backed fix and regression.
- [ ] **3A: compatibility and traversal safeguards**. Future startup rejects unsupported newer
  schemas before application mutation, including async backends. Introduce explicit protocol
  compatibility state; preserve flat reads and add bounded safe nested traversal/pruning and
  parent-directory durability coverage. Full startup reconciliation remains active. Existing
  older binaries cannot retroactively be made to reject newer state: rollback uses a verified
  snapshot, and unsupported old writers/external restore invalidate journal-coverage assumptions.
- [ ] **3B: fanout comparison**. Compare flat paths with lazy
  `bucket/<first-two-UUID-hex-digits>/<uuid>` in the laboratory, with identical counts and mixes.
  Measure directory creation, PUT/GET/delete, reconciliation and directory-fsync coalescing.
  It reduces neither inode count nor total scan work. Failure/inconclusive means no default
  change; success produces a separate promotion PR with offline migration and legacy tests.
- [ ] **3C: complete storage lifecycle accounting**. Retain full scans during implementation.
  Plan every unique attempt/path before creation, reserve through the Writer, await reservation
  before I/O. Publish only after data durability/hash validation; atomically verify exact intent,
  install metadata, retire intent and enqueue superseded paths in one savepoint. Deletes enqueue
  exact debt atomically with authoritative removal. Debt survives bucket/session deletion and
  remains until unlink plus parent sync; multipart bytes remain charged. IDs are never reused.
  Timers do not authorize active-upload deletion. Live cleanup requires backend-I/O quiescence;
  otherwise preserve intent until exclusive restart. Reuse recovery consumers/sweepers. Cover
  PUT/Copy, import/replication, part/complete/abort, lifecycle and admin deletion across all SQL
  backends, doubles, typed results and shard routing. Measure the extra durable PUT admission.
- [ ] **3D: offline baseline and recovery activation**. Under the node lock, classify every legacy
  artifact as live or durable debt before recording coverage. Interrupted scans cannot publish
  completion. Fence old process attempts before readiness; page unfinished work, preserve live
  references and retain exact quota debt. Snapshot/restore journals and use a fresh process
  generation. Full reconcile/integrity remain available. Activate journal boot only after crash,
  coverage, restore and performance gates; otherwise retain full boot scans. Recovery scales with
  unfinished work, not constant time.
- [ ] **4A: isolated hybrid prototype**. No production routing/feature flag. Large or unknown
  lengths use dedicated files. Completed small encoded records use one bounded builder, a
  32-MiB admission budget, and immutable segments sealed at 4 MiB, 256 records or 1 ms. File sync,
  rename and directory sync precede SQLite location publication. Explicit file versus segment
  identity/generation/offset/length locations; existing raw/CRNB framing, hashes and encryption
  interpretation. No heap entry per stored object. Report blob-layer, not S3, rates.
- [ ] **4B: collection/recovery/snapshot prototype**. Select sealed segments at least 50% dead;
  reserve replacement space, copy live records, make replacement durable, and conditionally
  relocate exact old identities/locations. Preserve old segments until metadata references and
  reader pins are gone; durable cleanup debt precedes reclamation. Preserve concurrent overwrites
  and Object Lock. Offline snapshots copy the referenced generation and publish manifest last.
  Fresh restore, survivor hashes, ENOSPC and interrupted collection are mandatory.
- [ ] **4C: hybrid decision**. Screen 1/4/16/64/256 KiB and 1 MiB, then confirm crossover in
  paired trials. Select only the largest contiguous passing range from the smallest tested size.
  Gains must survive overwrite/delete/GC; protect large streaming/ranges. No qualifying threshold
  means retain files. A passing candidate produces a separately reviewed production-format,
  migration/recovery and contract proposal; no automatic production connection.
- [ ] **5A: real metadata capacity**. FULL-durability Writer traces, including adopted journal
  costs. Start at 100,000 rows; approach 1 million only within budget. Versions/markers,
  conditions, parts, prefix/delimiter LIST and cleanup/outbox; hot and distributed buckets.
  Separate DB/index/WAL size, service time, throughput, p99, cache and recovery measurements.
- [ ] **5B: conditional alternative evaluation**. Capability comparison of SQLite, libSQL/Turso,
  redb, Fjall and RocksDB. Only if SQLite's isolated service demand demonstrably limits the
  workload, test pinned transactional Fjall first with equivalent durability, ordered keys and
  conditional semantics. RocksDB is research-only absent a separate allowance. Never compare
  raw KV insertion with complete Cairn transactions.
- [ ] **5C: metadata decision**. KEEP SQLite without a demonstrated bottleneck/qualifying gain.
  Laboratory speed alone cannot authorize replacement: complete semantic, migration/restore
  parity and the human architectural decision are required. Bucket sharding is not a hot-bucket
  solution and does not acquire native backup support through this phase.

## Shared benchmark budget and decision policy

One campaign, **3,600 seconds total runtime and 100,000,000,000 bytes peak combined footprint**,
not per PR. Preparation, requests, profiling, verification and cleanup count; builds, focused
correctness tests and ordinary CI are separately recorded. No 500-GB campaign or full local live
regression rerun. Use `/SSD`, not tmpfs `/tmp`. Count both arms, WAL, staging, compaction copies,
snapshots, logs and profiles; reserve headroom before admitting work. Stop/clean up on budget or
space exhaustion without automatically restarting. Carry unused time forward.

| Experiment group | Seconds |
| --- | ---: |
| Baseline/profiling | 600 |
| Fanout | 480 |
| Journal/recovery/backup | 840 |
| Hybrid/collection | 1080 |
| Metadata | 600 |

Record the primary metric and protected workloads **before** comparisons. Five paired confirmation
runs; at least 20% median primary improvement; no >10% protected latency/throughput/memory
regression; at least 10,000 successful requests per arm before p99 claims. Deterministic seeded
data/overwrite rings, selected concurrency 4/32/128, no Cartesian suite. Process-cold is not
cache-cold. Control drift, client saturation, missing measurements or insufficient samples mean
INCONCLUSIVE. Zero unexpected errors, checksum mismatches, premature reclamation or acknowledged
loss. Correctness fixes use their regressions, not fabricated throughput claims.

## Correctness, delivery and records

Inject failure at admission, create/write, file sync, rename, directory sync, metadata commit,
response loss, relocation, unlink and cleanup retirement. Cover late I/O after cancellation,
stale/duplicate ownership, read/overwrite/GC and multipart races, corruption/wrong keys/truncation,
ENOSPC/sync failure, interrupted migration, journals across restore, history and Object Lock.
Use small fixtures. Label process-kill versus durability-model tests; neither invents power-loss
evidence. Normal CI must pass at each final PR commit, including optional backend parity.

Preserve single-node topology, FULL default durability, exact cleanup ownership, fail-closed crypto,
environment-only configuration and append-only schemas. Read `CONTRACT.md` and owning docs first.
Never edit human-owned ceilings or silently adopt an experimental format/engine.

Each result records revisions, commands, seeds, configuration, outcomes, rejected candidates and
budget consumed. Distinguish shipped changes, laboratory work, inconclusive decisions and residual
limits. Update the owning storage/configuration/recovery/migration/test docs and `AUDIT.md` with
each PR. No billion-object, multi-TiB encoded, leak-free or competitor-superiority claims without
corresponding evidence. Merge only after focused validation/review and final-head green CI. Remove
owned data/processes/profiles/worktrees and merged branches; retain tracked sources/results and
only `main` and `website` at final handoff. Never delete unrelated data or shared installed caches.

## Execution record

2026-09-08: implementation started. Phase 1A active; performance budget used **0 seconds / 0 bytes**.
No benchmark corpus or server has been started.

Phase 1A: [PR #81](https://github.com/Harsh-2002/Cairn/pull/81) opened. Blob tests: 85 default,
88 all-feature tests passed (two benchmarks ignored). The preflight regression fails on baseline
`e38bc6c` and passes with the fix. S3 size-rejection/retry regression passes with the existing
HTTP 400 `EntityTooLarge` response. Final-head CI and review are pending; no merge claimed.

Phase 1B: bounded index spooling implemented in an isolated checkout; 91 default and 94 all-feature blob tests pass (two benchmarks ignored); all-feature blob Clippy passes.
Wire-reference comparison covers plaintext/encrypted and all three algorithms across chunk and
partial-block boundaries. Spool tests cover threshold/roundtrip, cancellation, create/read errors
and real ENOSPC via `/dev/full` without filling a disk. CI, review and merge remain pending.
