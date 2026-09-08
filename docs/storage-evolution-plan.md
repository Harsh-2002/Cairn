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
- [x] **3A: compatibility and traversal safeguards** — merged in PR #86 (`acd396f`). Future startup rejects unsupported newer
  schemas before application mutation, including async backends. Introduce explicit protocol
  compatibility state; preserve flat reads and add bounded safe nested traversal/pruning and
  parent-directory durability coverage. Full startup reconciliation remains active. Existing
  older binaries cannot retroactively be made to reject newer state: rollback uses a verified
  snapshot, and unsupported old writers/external restore invalidate journal-coverage assumptions.
- [x] **3B: fanout comparison** — merged in PR #87 (`bf17dfe`). Compare flat paths with lazy
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
and pre-existing chacha20 yanked-version warnings. The full local gate passed 1,350 workspace
tests (three skipped), two doctests, both Clippy configurations, web and installer checks. Final
head `82471d9747814c8388ca6102e31ec21c76da0b65` passed all CI/CodeQL checks without review
findings; PR #86 merged as `acd396f`, and its branch/worktree are removed.

Phase 3B compares flat and lazy fanout only in the laboratory. Its predeclared workload,
measurements and adoption gates are in `storage-fanout-2026-09.md`. The driver exercises actual
BlobStore reads, deletion and reconciliation, with exact generated liveness and survivor checks.
The raw namespace publisher preserves file/rename/directory durability and compiles the production
directory-sync coordinator. It does not measure complete S3 PUT or the future lifecycle protocol.
All 30 measured arms passed exact operation/survivor checks. The comparison is INCONCLUSIVE
because controls drifted beyond the predeclared 20% span; KEEP flat. No default placement changes
or promotion PR follow. The shared campaign, including reduction and cleanup, has consumed
390.191882 seconds (6m30s), with 542,654,464 bytes peak and no active process/data reservation.
Raw owned run artifacts are removed; compact evidence is in `storage-fanout-2026-09.json`.
Phase 3B final head `6f87421a3312cda7976f522fc1da0b4a506504bc` passed all 54 checks, with no
posted review findings or open branch scanning alerts. PR #87 merged as `bf17dfe`; its branch
and worktree are removed.

The operator approved the exact `storage-lifecycle-proposal.md` protocol-2 proposal on
2026-09-08. Phase 3C implementation is authorized: durable pre-stage Writer admission, exact
cleanup debt and backend-I/O quiescence, while retaining full startup scans. Phase 3D activation
remains gated by the approved coverage, crash/restore and performance checks. CONTRACT.md is
unchanged.

Phase 3C foundation: v36 journal schema, strict protocol-2 preflight, typed admission/cleanup
transactions, retained I/O ownership types and backend/shard parity are implemented. Fifteen focused
migration, ownership and journal tests pass across SQLite, libSQL, Turso, doubles and shards;
owning all-feature Clippy passes. Blob I/O, strict publication, v26 quota retirement and recovery
consumer integration are still required before this phase or its PR can be considered complete.
No new performance experiment has run; the cumulative campaign remains 390.191882 seconds.

Phase 3C integration: all ordinary/import/replica PUT and Copy paths, multipart part/completion
admission and publication, and deletion/lifecycle/control paths now use exact durable ownership.
v37 additionally retains multipart scratch-alias ownership so quota cannot retire before all
aliases are durably absent. Constructor, staging, blocking work, io_uring, reconciliation and
cleanup retain node/I/O lifetime; Linux descriptor-anchored traversal rejects descendant mounts
and symlinks. Focused protocol/blob/backend and five real-filesystem startup integration tests
pass, including across all four metadata configurations. The actual io_uring SIGKILL fixture
passes with the limits stated in `storage-journal-2026-09.md`. Review identified an import recovery
shutdown-sentinel race; its fix and deterministic producer-order regression pass. Exact cleanup
now runs promptly in the existing sweeper independently of the configured stale-upload interval;
three regressions cover multi-page progress, shutdown and locked-file debt retention. The final
local gate passes 1,388 default-feature and 1,413 all-feature tests, both Clippy configurations,
formatting and two doctests. Web, npm audits, cargo audits and installer checks pass. Final-head CI,
the admission-cost comparison, final review and merge remain pending.
Phase 3C is not complete and Phase 3D is not activated. Campaign consumption is unchanged.

PR #91 opened at `537c2b3`. Initial CI passed 52 checks and exposed two integration gaps: metadata
admission on a full filesystem returned 500 instead of 507, and cleanup could prune an empty bucket
directory held by pending multipart assembly. Typed capacity propagation and directory lifetime
fences now have focused regressions; the mixed-feature harness is being updated to verify exact
deferred cleanup within a deadline. The async Writer review additionally found ignored savepoint
failures; whole-batch abort tests accompany its fix, and the additional issue candidate is pending
operator approval. No comparison ran on the rejected candidate; the ledger remains unchanged.

The CI follow-up revision passes the complete Rust gate: 1,403 default-feature and 1,429
all-feature tests (four/seven skipped), two doctests, formatting and both Clippy configurations.
The directory-lock review also corrected completed coalescer/io_uring descriptor release before
acknowledgement; deterministic response-boundary regressions and the rebuilt privileged
pending-kernel-write SIGKILL fixture pass. Fifteen terminal-cleanup verifier tests pass.
The prior web/audit/installer results apply to their unchanged source and dependency inputs.
New-head CI and the predeclared cost comparison remain pending; no additional experiment ran.

At `0de5b69`, all 54 CI checks completed: 52 passed and two failed. The full-filesystem test
now passes. The standalone laboratory compile exposed crate-specific test helper imports in the
source-included coalescer regression; using standard file-lock methods fixes the test without
adding a dependency. Its local gate passes eight fanout/coalescer tests, two layer-driver fixtures
and 50 Python tests (two optional live fixtures skipped).

The mixed-feature soak had zero operation errors, byte mismatches or leak-shape violations, but
its unchanged 30-second cleanup deadline caught a quota-attachment race: a newly attached quota
owner invalidated the old receipt while leaving its 60-second claim lease in place. Ownership
changes now invalidate that entire claim tuple atomically for immediate retry, while unchanged
ownership preserves valid claims. Shared regressions require retries before lease expiry after
both supersession and terminal abort, preserving the charge until every alias is durably absent.
The focused nine-case backend/topology and unchanged-claim contracts pass, together with all
322 owning tests and both complete workspace Clippy configurations. Full workspace tests and
new-head CI remain pending. Neither rejected candidate was measured; the campaign is unchanged.


The final Phase 3C implementation at `9c3539fd41687bdd194eb00418d6dcf301a3e917` passes all
54 CI checks, including CodeQL, all metadata backends and the original mixed-feature soak
cleanup deadline (584/584 terminal sessions). The complete local gate passes 1,407 default and
1,433 all-feature tests, two doctests, formatting and both Clippy configurations. The paired
cost evidence is now in `storage-journal-2026-09.md` and its JSON: all twelve diagnostic arms
pass, but performance qualification remains INCONCLUSIVE. At 4 KiB, paired PUT p50 increases
75.9%, DELETE p50 increases 45.8%, and transaction throughput declines 36.9%; no p99 claim is
supported. Every candidate's post-idle journal is empty. Full scans remain mandatory and Phase
3D is unactivated. Raw experiment artifacts and processes are removed; cumulative costs and
exact evidence are preserved in the report. Final documentation-head CI and PR #91 integration
remain required before Phase 3D implementation starts.


### Phase 3C integration and Phase 3D review boundaries

PR #91 merged as `a72c98ebb719b6f68b9dc1ea8bc3cccb7f080319` after all 54 checks passed for
final head `e3a296d08625db99a00eb4297430e1223d63a309`. The final documentation run's browser
job initially timed out starting Chrome; its isolated retry passed without source or threshold
changes. PR reviews/comments and branch CodeQL alerts were empty. Findings #88–#90 closed with
the merge, and the merged branch/worktree were removed. The additional async-savepoint finding
is fixed in that merge; permission to file its separate tracking issue is still pending.

Phase 3D is split into two sequential review boundaries to keep integration smaller. The first
prepares restored metadata ownership before publication: canonical staged Writer mutation,
checkpoint/close, a derived image receipt, immutable validation and the existing read-only key
binding preflight. It does not add schema, change snapshot format or enable journal startup.
The second adds the offline baseline operation and legacy coverage/quota holds, with its own
failure and performance evidence. Each starts from the preceding merged main and must pass
final-head CI. The Phase 3C cost record remains INCONCLUSIVE; full startup scans remain mandatory.

The restore-preparation implementation is complete on `codex/storage-03d-restore`, based on
`a72c98e`. Thirteen focused metadata journal tests pass across all engines/doubles/shards;
25 server snapshot/publication/key-gate tests pass, including interruption on both sides of the
rename, changed staging files, failed reset and pinned WAL. Both owning all-target/all-feature
Clippy checks pass. The focused real `recovery_state.py` drill passes encrypted history, Object
Lock, active multipart recovery, exact row/quota transitions, unchanged snapshot fingerprints,
and wrong-key refusal before changing either a fresh or existing target. Twelve Python recovery
helper tests pass. Web lint/build, both npm audits, cargo audit and installer checks pass.
The complete workspace gate and final-head CI remain required before this PR merges. No further
local experiment has run; consumption remains 574.519391 seconds and recorded peak 571,154,432 bytes.
