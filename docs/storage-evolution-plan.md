# Storage evolution implementation plan

Approved 2026-09-08, baseline `e38bc6c`. This is the implementation checklist for five phases,
with sequential small PRs. Check an item only after its final commit passes focused tests,
relevant review and normal CI, and its PR is merged. Each PR updates this record and the owning
specification. Experiments may correctly conclude **retain the current design**.

## Delivery checklist

### Resumed execution tasks

- [x] Recover local branches/worktrees, preserve existing changes, and inspect final-commit CI
  and review status for the interrupted work.
- [x] Integrate 1A after its correctness checks; update 1B onto merged main, finish its review
  and validation, and integrate it independently.
- [x] Implement 1C, including authenticated page reuse, prepared-reader handoff and reservation
  ownership; validate and integrate its independent PR.
- [x] Complete 2A–2B with the persistent campaign ledger and bounded attribution evidence.
- [ ] Complete 3A–3D in order, obtaining the required architectural decision before changing
  the storage-lifecycle protocol and retaining full reconciliation until activation qualifies.
- [ ] Complete the isolated 4A–4C packing evaluation and record its adoption decision.
- [ ] Complete 5A–5C and publish the supported metadata decision.
- [ ] Verify the final integration and results record; remove owned temporary artifacts and
  merged worktrees/branches, leaving `main` and `website`.

### Phase acceptance

- [x] **1A: writer/read format limits** — merged in PR #81 (`5269e1b`).
  Share checked 9-byte-entry/64-MiB-index arithmetic; reject before copying/encoding excessive
  input. Known encoded lengths fail before staging/preallocation, multipart totals use checked
  addition, unknown streams abort safely. Preserve raw-file limits and the existing S3 error.
  Test exact and exceeded limits, overflow, chunk boundaries, synthetic small encoder ceilings,
  cleanup and retryable multipart completion. Document actual encoded-object ceilings.
- [x] **1B: bounded encoder index spool** — merged in PR #82 (`f092a81`). Replace the complete index vector/final output copy
  with an owned temporary spool and bounded payload/index sinks. Preserve v1–v3 bytes and block
  identities. Append the spool and existing authentication before the final file's durability
  barrier; no independently committed sidecar. Cover spool failures, ENOSPC, cancellation,
  detached filesystem work and startup cleanup. Input and output batches must also be bounded.
- [x] **1C: bounded verified index reader** — merged in PR #83 (`1eed10f`). Keep the 64-MiB cap; use 65,529-byte pages
  (7,281 entries). Stream initial structural/HMAC validation, retaining provisional SHA-256 page
  fingerprints and physical starting offsets. Publish a usable reader only after full validation.
  Reread the same descriptor, verify a whole page before interpreting it, retain trusted geometry,
  and transfer the prepared reader from probe to body instead of parsing twice. Move allocation
  bounds into a blob-owned interface used by replication; retain leases through blocking work.
  Test tampering after validation, swapped pages, boundaries, downgrade/wrong-key refusal and
  bounded memory. Plain v1 gains no new initial authentication. Initial index I/O stays linear;
  this does not enable 5-TiB encoded objects. A new format needs a separate reviewed decision.
- [x] **2A: bounded diagnostic harness** — merged in PR #84 (`0373dc2`). Shell launcher/Python coordinator and Rust layer drivers
  under `conformance/storage_lab/`, outside production dependencies. Explicit prebuilt binaries,
  owned `/SSD` directory, persistent campaign ledger, bounded process groups and cleanup.
  Record hashes/revisions/configuration/tools/hardware and PASS/FAIL/INCONCLUSIVE/CANCELLED.
  Missing tools, exhausted budgets or inadequate samples never become passes.
- [x] **2B: CPU/memory/storage attribution report** — merged in PR #85 (`d83b051`). Separate real metadata, blob and S3 work;
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

2026-09-08 resumed: recovered clean 1A/1B/1C worktrees and verified both existing PR heads had
passing normal CI and no posted review findings. PR #81 head `667b286` passed all 88 blob tests
again (`RUSTUP_TOOLCHAIN=stable CARGO_HOME=/SSD/dev/.cargo cargo test -p cairn-blob --all-features`;
two benchmarks ignored) and was merged as `5269e1b`. The original local AUDIT findings are preserved
in that merge. PR #82 is now based on `main`; its resumed validation also exercises cancellation
before queued spool creation can execute. The 1C worktree contains no unique implementation yet.
No local performance experiment has run: campaign consumption remains **0 seconds / 0 bytes**.

Resumed 1B checks: 95 all-feature blob tests passed (two benchmarks skipped), including
`cancelled_spool_creation_cleans_up_after_detached_work`; all-feature owning-crate Clippy and
workspace formatting passed. Commands: `cargo nextest run -p cairn-blob --all-features`,
`cargo clippy -p cairn-blob --all-targets --all-features -- -D warnings`, and
`cargo fmt --all --check`. Nextest 0.9.140 was checksum-verified against its official release.
The shared blob target was cleaned before the recorded nextest run to prevent reuse of a stale
test binary from the older checkout. Final-commit CI remains the integration gate.

PR #82 final head `ab67eef` passed normal CI and CodeQL, with no posted review findings, and merged
as `f092a81`. The additional local `make check-all` passed: 1,327 workspace tests, two doctests,
both Clippy configurations, formatting, web build and installer checks. Web lint and both npm
audits passed separately (zero vulnerabilities); cargo-audit 0.22.2 passed with the existing
allowed rustls-pemfile maintenance warning. The merged 1A/1B branches and 1B worktree are removed.

Phase 1C resumed from `f092a81`: provisional page summaries, whole-index authentication, verified
page reuse and prepared-reader handoff are implemented. The backend now supplies read allocation
and frame bounds to replication. All 107 all-feature blob tests pass (two benchmarks skipped),
including three-page v1/v2/v3 fixtures, changed/swapped/evicted pages, partial-page offset and MAC
corruption, retained-memory bounds, wrong-key/downgrade regressions, descriptor replacement and
raw/encoded cancellation lease retention. Another 373 protocol/replication/type tests passed,
including backend-owned admission bounds. Owning all-feature Clippy, formatting, web lint/build
and both npm audits passed. The final workspace and CI gates remain pending. No performance experiment has run; campaign consumption remains **0 seconds / 0 bytes**.

Phase 1C commands: `cargo nextest run -p cairn-blob --all-features` (107 passed, two skipped),
`cargo nextest run -p cairn-types -p cairn-replication -p cairn-protocol --all-features`
(373 passed), and `cargo clippy -p cairn-blob -p cairn-types -p cairn-replication -p cairn-protocol
--all-targets --all-features -- -D warnings`. A combined intermediate run passed 479 tests;
the final additional test checks the existing zstd bulk decoder's native workspace against its
fixed allowance. Page fixtures use deterministic seed `0x5eed` and fresh test encryption keys;
no benchmark dataset or server was created.

PR #83 final head `2fc2baeb7217bfa98e609f0998e8d9347c0beb74` passed every CI/CodeQL job
with no posted review findings and merged as `1eed10f`. The full local `make check-all` passed
1,340 workspace tests (three skipped), two doctests, both Clippy configurations, formatting,
the web build and installer checks. Web lint and both npm audits passed separately. The
merged reader branch/worktree are removed. Phase 1 is integrated; its format limits remain.

Phase 2A starts from merged `1eed10f` on `codex/storage-02a-lab`. The standalone laboratory
workspace, Python campaign ledger and gated process launcher are implemented. Correctness
fixtures exercise FULL-durability metadata writes/WAL reads, byte-exact blob round trips and
real signed S3 requests. Operation-capped fixtures deliberately produce INCONCLUSIVE rather
than performance evidence. Fifteen Python regressions and both Rust driver tests pass, as do standalone Clippy,
formatting, shellcheck, release-policy tests, actionlint 1.7.12 (verified release checksum),
and the lab lockfile audit (one allowed pre-existing yanked-package warning). Full final-commit
CI passed on final commit `cf5ec69`, with no posted review findings; PR #84 merged as `0373dc2`.
Production Rust and console sources are unchanged. The merged branch/worktree are removed.
Phase 2B's predeclared screen and later results live in
[`storage-attribution-2026-09.md`](storage-attribution-2026-09.md). Compilation, tool installation
and fixed correctness fixtures are tracked separately from experiments.

Phase 2B recorded its experiment design at `cd5216a` and completed eight unprofiled baseline
cases plus three decoded heap traces. CPU sampling was denied by host perf permissions;
complete attribution remains INCONCLUSIVE and no production optimization or architecture
change is justified by this screen. The charged report, failed attempts, ownership evidence
and provenance are preserved in `docs/storage-attribution-2026-09.md` and its JSON companion.
After data/profile/process cleanup, the cumulative campaign is **250.787718 seconds** and
**219,529,216 bytes recorded peak**; 349.212282 unused baseline seconds carry forward. The
persistent ledger is `/SSD/dev/cairn-storage-campaign/ledger.json`; it must not be reset.
Local validation passes 21 Python regressions, two Rust driver tests, standalone Clippy,
formatting and shellcheck. Final head `95315011d460e0b40ec74f8a384fa3369a4fc828` passed all CI/CodeQL jobs with no posted review findings; PR #85 merged as `d83b051`. Its branch/worktree are removed.

Phase 3A starts from merged `d83b051`. Append-only v35 records storage compatibility while keeping
flat writes and mandatory full scans. SQLite/libSQL/Turso startup and migration entry points reject
unsupported state before maintenance; sharded startup preflights all files and snapshot validation
checks before target staging. POSIX reconciliation streams buckets and bounded flat/nested pages,
preserves unknown layouts/symlinks, and synchronizes parent changes before reporting pruning.
Correctness fixtures are separate from experiments; the campaign ledger is unchanged. All 642
owning-crate tests pass with all features (five skipped), including SQLite/libSQL/Turso admission,
all-shard preflight, snapshot refusal and mixed-layout reconciliation. Web lint/build and both npm
audits pass with zero vulnerabilities; cargo audit passes with allowed rustls-pemfile maintenance
and pre-existing chacha20 yanked-version warnings. The full workspace gate and final-commit CI
remain pending.
