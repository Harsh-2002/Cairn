# Isolated packing collection and recovery

Phase 4B extends the laboratory from phase 4A. Production remains one file per object with the
canonical SQLite Writer and full startup scans. Implementation and validation are in progress;
no collection performance or adoption result is claimed.

## Collection protocol

Candidate enumeration examines bounded pages of at most 256 artifacts, even when none qualifies.
A sealed segment qualifies only when at least half its physical bytes are dead. The retained
physical length is the 80-byte segment header plus 40 bytes of framing and encoded length for
each referenced record. Every historical and locked row counts as live. SQLite remains the
liveness authority; filenames, age and an in-memory inventory cannot authorize reclamation.

The actor rechecks eligibility and acquires one shared source descriptor. Collection reserves
bounded metadata/scratch memory before loading records and admits the complete replacement
length against a persistent physical budget. Live, pending and retired-but-unreclaimed artifacts
all retain their charge. The replacement file also requires successful physical allocation
before the first source byte is copied. Allocation refusal leaves durable pending ownership
for exact cleanup. Encoded records stream unchanged through 64-KiB scratch with hash verification.

The replacement follows file sync, rename, parent sync and hash/length validation before its
private receipt reaches SQLite. One FULL transaction compares each immutable row ID and its
complete old location, changing only location columns for matching rows. Concurrent overwrites
or deletes cause counted CAS losses. Hash, logical length, encoding declarations, history,
current status and Object Lock metadata do not change. A partial/all-lost copy is accounted
explicitly; an unreferenced replacement is durable cleanup debt. An old segment retires only at
zero SQLite references. Existing shared reader pins delay exclusive physical reclamation.

Cancellation retains source pins, admission and buffers through the actual blocking job and
metadata result. Cleanup retires exact debt and physical charges only after durable absence of
both possible aliases. Uncertain work remains pending/debt for an exclusive fresh generation.
No new filesystem scan establishes liveness.

## Offline snapshots and fresh restore

A typed node gate complements the actual root file lock. One active actor session is retained by
all admissions, receipts, cleanup claims and pinned readers. Closing a Store checks the
TRUNCATE checkpoint result, closes SQLite and joins the actor. An offline proof is available
only after every actual owner drains, and prevents another actor opening on the same node.
Holding an ordinary closed Store clone cannot mint another session or a second proof.
Existing SQLite database/sidecar files must resolve beneath the same mount without symlinks and
have one link before SQLite opens them. This prevents another root lock from sharing their inode.

An offline snapshot copies the exact database image and whole immutable artifacts referenced
by any row, including history. Bounded keyset pages and a streamed catalog avoid an in-memory
artifact inventory. Copied lengths/hashes, schema and relations are verified; files and
directories are synchronized before a fixed-size manifest is published last. Pending and debt
metadata remain exact even when their unreferenced bytes are omitted. Admission reserves payload,
control-file bounds, per-entry allocation/directory overhead and free-space headroom; a reported
filesystem allocation unit above the conservative baseline increases that reservation. This is
an admission bound, not measured allocation on every filesystem.

Restore validates the image/catalog/namespace before target metadata publication, copies and
verifies artifact bytes into a fresh locked target, and publishes the database last. Artifact
generations remain unchanged; reopening mints a fresh process generation and invalidates old
claims. Exact pending/debt recovery and survivor verification complete the fixture. No test
stores raw encryption keys in the database or claims production S3 backup parity.

## Required evidence

Fixed tests must cover the 49%/50% boundary, quota and physical-allocation refusal before copying,
ENOSPC/corruption during copying, publication barriers, partial/all-lost relocation, SQL rollback,
delayed actual reader jobs, cleanup failures, fresh reopen, and encrypted protected history.
Snapshot tests must cover active ownership refusal, interrupted pre-manifest copies, truncation,
wrong hashes, malformed catalog/namespace and target metadata absence on failed validation.
These are correctness fixtures; process-kill or power-loss evidence is claimed only when such an
experiment was actually performed. Phase 4C must measure complete churn/collection costs before
any packing selection. Failure to qualify means retain dedicated files.

## Validation record

All 66 packing tests pass, including physical allocation refusal, bounded candidate-page
admission, actual delayed reader ownership, encrypted protected-history relocation/reopen,
snapshot integrity rebinding, allocation headroom, SQLite hardlink/sidecar refusal and ordinary
owned-WAL recovery. All 76 standalone Rust tests and all 70 Python tests pass with every live
fixture enabled. The full workspace gate passes 1,466 default and 1,492 all-feature tests, both
Clippy configurations and two doctests. These are fixed correctness fixtures; no comparative
packing measurement has run. Final-head CI remains required.
