# Storage lifecycle integration review, 2026-09-08

Scoped review of the approved Phase 3C implementation, against merged main `bf17dfe`.
These are concrete architecture/reliability findings with local fixes and focused regressions.
The complete gate passes for `9c3539f`; PR integration remains pending. The operator approved
filing findings #88–#90. The additional async finding is still awaiting filing approval. No release or
journal-startup activation is claimed. Existing GitHub issues were checked for duplicates.

## Finding #88: initialization follows staging descendants before namespace validation

`LocalBlobStore::open` previously called recursive directory creation for `.staging/multipart`
before safe reconciliation. A `.staging` symlink or descendant bind mount could therefore cause
creation outside the configured data root before startup reported the invalid namespace. This is
a local filesystem/deployment trigger, not an S3 key traversal or remote authorization bypass.

The fix uses `namespace::initialize` (`crates/cairn-blob/src/namespace.rs`) with anchored
`openat2` directory resolution, parent synchronization and no descendant symlink/mount traversal.
The explicitly configured root may itself be a mount. The ordinary initialization regression
checks the external tree is unchanged; the privileged same-filesystem bind-mount fixture checks
the mount case that a device-number-only guard would miss. Both pass locally.

Filed: [#88 — Blob initialization mutates staging descendants before validating the namespace](https://github.com/Harsh-2002/Cairn/issues/88).
Type: architecture. Severity: medium.

## Finding #89: unknown directory-entry types silently omit reconciliation subtrees

The raw directory walker previously treated `DT_UNKNOWN` as neither file nor directory and could
skip a bucket subtree without an error. Filesystems that do not supply entry types could therefore
produce an apparently successful incomplete scan, including before legacy multipart accounting
retirement. The protocol-2 coverage decision must not inherit that omission.

The fix resolves unknown types with descriptor-relative no-follow metadata lookup inside each
bounded page (`crates/cairn-blob/src/reconcile.rs::page_with`). Lookup failure is an error;
unknown entries are never silently certified as scanned. The deterministic two-entry-page
regression forces unknown types for files, directories and symlinks and passes locally.

Filed: [#89 — Reconciliation can skip subtrees when directory entries report unknown types](https://github.com/Harsh-2002/Cairn/issues/89).
Type: architecture. Severity: medium.

## Finding #90: recovery stop marker precedes background import termination

`server.rs::serve` previously sent the storage-recovery FIFO stop marker as soon as HTTP listeners
drained, while ordinary workers still stopped concurrently. `import_dest.rs` invokes the same S3
service from the import worker. An import cancellation could therefore enqueue recovery behind
the marker while its synchronous send reported success; the consumer then exited without resolving
that record and shutdown could report complete. Protocol-2 durable intent preserves the unfinished
work for restart, but does not make that shutdown report true.

`server.rs::drain_storage_producers` now joins both HTTP and ordinary/background producers before
sending the marker, then finalization joins recovery before metrics/counters/WAL. Its regression
uses the actual recovery queue and admitted metadata intent, checks HTTP-first and import-first
termination, and proves the late import's exact paths become cleanup debt before consumer exit.
The all-feature focused test passes locally.

Filed: [#90 — Shutdown can discard import recovery queued after the HTTP drain marker](https://github.com/Harsh-2002/Cairn/issues/90).
Type: architecture. Severity: medium.

The final integration passed formatting, both all-target Clippy configurations, 1,388 default-feature
tests, 1,413 all-feature tests and two doctests. The initial all-feature run stopped at the existing
five-second readiness probe timing assertion under concurrent load; the unchanged assertion passed
in the controlled four-thread full reruns. Web lint/build, both npm audits, cargo audit and installer
checks also passed. The cleanup scheduler's three additional regressions prove progress past a
1,000-path batch with the default hourly stale-upload interval, no claims after a pre-signalled
shutdown, and retained debt while a file lock prevents cleanup. Fifty Python laboratory tests pass
(two optional live fixtures skipped); the standalone laboratory lockfile audit passes with its
pre-existing allowed yanked-package warning. Final-head CI, cost measurements and merge remain pending.

## Additional finding: async Writer ignores savepoint failures

Issue filing approval is pending. The optional async Writer on merged main `bf17dfe` ignores
failed `release` and `rollback_to` results before committing a batch. A deterministic driver fixture
performs a real configuration write, returns an apply error and injects a rollback-to-savepoint
failure. Whole-batch rollback now preserves the original row instead of allowing that partial
mutation to reach commit. Failure-point tests also cover savepoint creation/release, typed capacity
errors and a real commit followed by an acknowledgement error. That last case preserves committed
state and explicitly prevents interpreting an error as proof of nonpublication.

This was found while investigating PR #91's new admission path returning 500 on a full metadata
filesystem. SQLite, libSQL and Turso now retain typed capacity failures through Writer response
fan-out and secondary rollback errors; the canonical mapping returns 507. The protocol regression
checks the body remains unpolled, no file is created, and a later retry round-trips its bytes.
The focused backend/protocol tests pass; final combined validation remains pending.

Initial PR #91 CI also found an integration race where empty-directory pruning invalidated a
pending completion's final directory descriptor. Directory fences now protect prepared namespace
operations and have deterministic creation/cancellation/pruning regressions. The soak's former
immediate cleanup assertion is replaced by bounded exact physical/debt/quota verification;
5xx, byte-integrity and leak-shape thresholds are preserved. No local full soak or performance
comparison was run on the rejected revision.

The final ordering review additionally found that coalesced directory-sync acknowledgements and
io_uring terminal responses could wake callers before releasing completed descriptor locks.
The coalescer now drops every completed directory descriptor before any batch acknowledgement;
the ring writer closes its ring descriptor and releases its original file/namespace descriptors
before terminal responses. Both retain I/O leases through actual task completion. Custom-waker
regressions probe the locks at the response boundary, and an eight-pruner barrier proves finite
competing cleanups release shared child locks before acquiring exclusive pruning ownership.
All 131 enabled all-feature blob tests pass, with four privileged/opt-in fixtures skipped in
that run. The rebuilt privileged pending-kernel-write SIGKILL fixture also passes separately;
its owned loop device, mount and image were removed, and the original loop inventory is unchanged.
This still proves the observed exclusion through process teardown, not physical power-loss
behavior or an observed post-exit kernel-reference window. The combined workspace gate passes 1,403 default-feature and 1,429 all-feature tests, two
doctests, formatting and both all-target Clippy configurations. Final-head CI remains required.

The next CI pass (`0de5b69`) found no operation errors or byte-integrity violations in the mixed
soak, but one spool alias's quota debt exceeded the original 30-second deadline. Attaching a quota
owner correctly invalidated its old acknowledgement, yet preserved the old 60-second claim
lease. The Writer now clears that claim only when ownership changes, so a fresh fenced cleanup
can retry immediately. Regression coverage requires immediate retry after part supersession and
terminal abort, rejects old and duplicate acknowledgements, retains quota until alias cleanup,
and preserves valid claims on unchanged ownership across every backend and the double. The
standalone lab's source-included coalescer regression now uses standard file-lock methods and
passes its compile/tests without a new dependency. Final-head CI remains required.


Final implementation validation at `9c3539f` passes all 54 CI checks, 1,407 local default-feature
and 1,433 all-feature tests, two doctests, formatting and both Clippy configurations. The CI soak
verifies 584/584 terminal multipart cleanups before the unchanged 30-second deadline, with zero
operation errors, byte mismatches or leak-shape violations. All twelve bounded comparison arms
pass correctness checks; the cost qualification remains INCONCLUSIVE and shows substantial
small-object overhead (`storage-journal-2026-09.md`). Final documentation-head CI and merge
remain pending. No journal-only startup or production packing/metadata replacement is enabled.
