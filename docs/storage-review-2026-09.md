# Storage lifecycle integration review, 2026-09-08

Scoped review of the approved Phase 3C implementation, against merged main `bf17dfe`.
These are concrete architecture/reliability findings with local fixes and focused regressions.
The complete gate and PR integration remain pending. The operator approved issue filing. No release or
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
