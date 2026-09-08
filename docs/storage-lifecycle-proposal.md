# Storage lifecycle protocol 2 — architectural proposal

**Status: proposed for human review; not implemented or activated.** This is the concrete
Phase 3C/3D proposal required by `storage-evolution-plan.md` and the approved implementation
plan. `CONTRACT.md` remains human-owned and unchanged. Phase 3B retains flat placement.

## Decision requested

Approve adding a durable Writer admission **before** ordinary object staging, and exact,
durable filesystem cleanup accounting **after** authoritative reference removal. Preserve the
existing file sync → rename → directory sync → checksum validation → metadata publication
ordering. Multipart parts retain their existing in-place file/directory synchronization.
The admission transaction does not publish an object or acknowledge an S3 success.

Phase 3C keeps mandatory full startup scans. Phase 3D may activate journal-based startup only
after the separately specified legacy-coverage, crash, backup/restore and performance gates.
An inconclusive or failed activation gate keeps full scans. This proposal does not authorize
packing, a metadata-engine replacement, online migration, or changing the human-owned contract.

The expected cost is an additional durable admission for ordinary PUT/Copy/ingest and extra
cleanup transactions. Group commit can share barriers but cannot eliminate this dependency.
No write-throughput improvement is promised; the admission cost will be measured explicitly.

For an existing installation, the rollout is offline: stop the old node, take and verify a
backup with a fresh restore drill, then migrate/start the new binary under the node lock with
full scans. Complete the new baseline before considering journal startup. A backup and tested
rollback are operational prerequisites; this is not an online conversion or automatic activation.

## Persisted state and compatibility

Use an append-only migration after v35, through the existing schema machinery. All durable
state lives in the same embedded metadata database and uses its canonical Writer. The
SQLite, libSQL/Turso and in-memory implementations expose the same typed outcomes.

| State | Exact ownership and purpose |
|---|---|
| `storage_recovery_state` | Singleton per physical database: current process generation, coverage identity/state, and last completed baseline. A fresh generation is committed under the exclusive node lock before that database admits requests. |
| `storage_write_intents` | Fresh attempt id, generation, routing bucket, operation kind and exact intended object row/version or upload/part/completion token. Bucket/session deletion cannot cascade this row away. Creation time is diagnostic, never deletion authority. |
| `storage_intent_paths` | At most three validated relative file paths per intent, each with its role: temporary data, final object/part, or optional index spool. Index exact path and attempt id. These bounded rows exist only while work is unfinished. |
| `storage_cleanups` | Exact immutable relative path, routing bucket, fresh cleanup id, optional existing multipart quota-debt id, and exact cleanup claim token/generation/lease. Index pending work for bounded keyset paging. No cascade from bucket/session deletion. |

Object/version and live-part rows remain the authoritative references; there is no new
permanent journal entry or heap map per live object. Journal paths are filesystem identities,
not S3 keys. Path validation rejects absolute paths, traversal, symlinks and unsupported shapes.
New files keep flat placement and the existing raw/CRNB formats. Each new attempt gets fresh
identities; retrying an operation never repurposes a previous attempt's filenames.

Keep v26 multipart reservation/part/cleanup counters as the sole quota accounting. Generic
physical-cleanup rows may reference an existing multipart quota debt; they do not charge the
bytes again. Retire that quota debt only when all associated physical work is durably absent
and every associated writer is quiescent. Existing coarse session-cleanup records are marked
legacy and require full reconciliation/baseline conversion; they cannot establish journal
coverage by themselves. New terminal sessions record exact part/attempt paths transactionally.

Set the storage compatibility minimum reader/writer protocol to 2 while keeping
`write_layout='flat'` and `recovery_mode='full-scan'` in Phase 3C. Implement protocol-2 validation
before that migration is reachable. PR #86 binaries reject this state; already released older
binaries cannot be retroactively fenced. Supported rollback remains a verified pre-upgrade
snapshot into a fresh data directory. An unsupported old writer or external filesystem/database
modification invalidates coverage and requires a new exclusive baseline; it is not automatically
detectable from the journal marker alone.

## Admission, physical ownership and publication

1. Obtain the existing bounded recovery slot before admission. Generate the operation's
   immutable object row identity or multipart attempt/completion token. A pure BlobStore
   planning method selects all possible filenames, including the optional spool name, without
   creating a file or directory.
2. Submit `ReserveStorageWrite` with the exact plan/target and current generation. For parts,
   extend the existing quota reservation transaction to insert the intent and paths too. For
   completion, acquire the existing exact completion claim and assembled-object intent in one
   savepoint. Failed conditions/reservations leave no admitted plan.
3. Only the acknowledged typed admission result may be consumed by the staging/assembly API.
   The creation permit is move-only and used once. A lost admission acknowledgement causes
   no file creation; its persisted intent is resolved by the retained recovery consumer.
4. A blob-owned I/O lease follows every blocking closure, submitted io_uring operation and
   staging/index-spool handle until it actually finishes. Dropping an async future does not
   report that lease quiescent. The lease also retains the node-lock lifetime so shutdown cannot
   release exclusivity while an old backend operation could still create a file.
5. Preserve the existing data durability and checksum checks. Before publication, the durable
   result must match the admitted final path, intended target and generation. The Writer's
   publication savepoint checks exact ownership, existing preconditions/Object Lock, then
   atomically installs the object/part, tags/lock/outboxes/counters, consumes the intent, and
   enqueues superseded paths plus any unreferenced temporary/spool aliases as cleanup debt.
6. Only the publication acknowledgement permits an S3 success. An ambiguous publication
   acknowledgement is resolved through the same Writer behind the original mutation. A live
   reference is preserved; an error remains ambiguous and preserves accounting and bytes.

Temporary aliases may already be absent after rename or immediate spool unlink. Their debt
still requires the relevant directory barrier before retirement. This closes the source-name
resurrection window without moving metadata publication ahead of final-blob durability.
Shared bucket/session directories are derived from the admitted plan; creation syncs their
parents before dependent writes. Cleanup prunes only validated empty parents and synchronizes
each successful namespace removal. Nonempty shared directories are preserved.

## Cancellation, deletion and cleanup

Cancellation first prevents that permit from admitting further physical work. The retained
consumer waits for blob-backend quiescence and then asks the Writer to resolve the exact intent.
If quiescence cannot be established, leave the durable intent and its quota charge for exclusive
restart recovery. A timer, request disconnect, completion-claim timeout or expired cleanup lease
never authorizes deletion of an active writer's paths.

Every mutation that removes/replaces an authoritative object or part reference also inserts
exact cleanup debt in the same savepoint. This includes sentinel replacement by delete markers,
version deletion, replication replacement, multipart supersession/completion/abort, lifecycle,
integrity repair and administrative deletion. Existing Object Lock and compare-and-delete
decisions continue to run before removal. Bucket/session deletion preserves outstanding intent,
debt and routing information; ongoing part attempts become cancelled, not silently reclaimed.

Reuse existing retained recovery consumers and sweepers with bounded pages/concurrency; add no
new service, long-lived cache, thread or production dependency. Claim cleanup through the Writer
under an exact token/generation/lease. Before authorizing unlink, the Writer verifies that no
object/part reference or active non-quiescent intent can own that immutable path. A publication
cannot reintroduce it because it requires its exact still-active intent and paths are not reused.
Unlink is idempotent, but ENOENT alone is insufficient: synchronize the containing namespace,
including the surviving ancestor when directories were removed. Only the matching cleanup claim
may retire the row and release associated quota. Sync errors and stale completions retain debt.

All per-bucket intent/cleanup mutations carry their retained routing bucket. Both exhaustive
shard matches and typed fan-out results are updated. Startup advances every physical database's
generation under the one node lock and binds no listener until every shard succeeds. A partial
startup is retryable; it does not require an atomic transaction across databases.

## Full scans first; offline coverage before activation

Phase 3C continues the complete pre-bind reconciliation gate. It fences prior generations and
resolves their exact unfinished work; the existing full walk still discovers legacy artifacts
that lack intents. Bulk multipart-accounting recovery must not forgive protocol-2 debt merely
because a scan returned success: exact absence/directory-sync proof remains required.

Phase 3D adds the operational command `cairn storage-baseline`, using existing environment
configuration and holding the node lock for the entire operation. Before the walk, mark coverage
incomplete through the Writer. In bounded pages, classify every legacy physical artifact as an
authoritative live reference or exact durable cleanup debt, including multipart attempts and
temporary/spool aliases. Convert legacy coarse cleanup accounting without releasing bytes early.
Unknown names, symlinks, mounts, unclassified artifacts or errors prevent coverage completion;
they are preserved and reported. An interrupted walk cannot publish a completion marker.

After successful complete classification, commit the coverage marker. Journal startup may then
fence old process generations, resolve unfinished intents in bounded pages and retain safely
unreferenced cleanup debt for background reclamation. Readiness does not wait for harmless debt
only when exact ownership and quota accounting make that safe. Work scales with unfinished
entries; neither constant-time startup nor coverage after unsupported external edits is claimed.
Explicit full reconciliation and bidirectional integrity checking remain available.

Snapshots include the journal tables and all authoritative object/history/part files. Pending,
unreferenced files may be absent from a snapshot; their intents/debt remain resolvable as absent
after restore. Restore validates compatibility before target staging and publishes the manifest
last. It never resumes source-process ownership: establish a fresh generation, invalidate the
copied coverage marker, and require the full startup/baseline path before reactivating journal
startup. Preserve the existing fail-closed crypto, exact-state and active multipart restore tests.
Native sharded backup support is not added by this proposal.

## Delivery and acceptance

Implement Phase 3C in reviewable commits within its PR: shared types/schema/backend parity;
planned blob I/O and quiescence; all publication/removal call sites; retained consumers and
crash/restore tests. Full scans stay enabled throughout. No production path may retain an
unaccounted staging or direct post-reference-removal unlink escape hatch at completion.

| Seam or race | Required result |
|---|---|
| Before/after admission commit, including lost acknowledgement | No creation before receipt; orphan intent is recoverable without a live-row change. |
| Create/write/spool/file sync/rename/directory sync failures | No S3 success; all possible names remain owned until durable absence is proven. |
| Cancellation during queued or executing backend I/O | No cleanup/lock release before actual quiescence; late I/O cannot resurrect a retired path. |
| Process death with outstanding optional io_uring work | Validate backend teardown/exclusive-restart assumptions before trusting absence; process generation alone is not proof that kernel I/O has drained. |
| Publication precondition/lock failure or acknowledgement loss | No partial metadata/debt change; exact live references survive ambiguous responses. |
| Stale/duplicate intent, generation, completion or cleanup token | Typed not-applied result; no publication, premature unlink or quota release. |
| Overwrite/delete/abort racing readers or writers | Reader descriptors remain valid; active writers are fenced before reclamation; live versions and Object Lock survive. |
| Unlink, ancestor fsync, or cleanup-retirement failure | Retryable exact debt; multipart bytes remain charged. |
| Interrupted baseline, startup or restore; unsupported old writer | No trusted coverage publication or premature readiness; full-scan fallback remains. |
| ENOSPC, wrong keys, corruption and missing live files | Existing fail-closed semantics and retryability preserved; no manufactured clean result. |

Use deterministic owning-crate and real server crash/restore fixtures; label process-kill versus
durability-model tests accurately. Exercise SQLite, libSQL/Turso, doubles and shard routing. The
normal final-commit CI/full gate remains mandatory. Reuse the shared campaign ledger for the
extra durable PUT admission and recovery/baseline/backup cost measurements, including preparation
and cleanup. Predeclare primary/protected workloads and apply the existing five-pair, sample,
drift and regression gates before journal-startup activation. A failed/inconclusive activation
retains full scans and reports the measured tradeoff; it does not silently relax durability.

Human approval of this proposal authorizes implementation of this protocol and the bounded
activation evaluation. It is not evidence that those correctness/performance gates have passed.
