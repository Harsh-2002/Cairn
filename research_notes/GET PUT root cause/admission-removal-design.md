# Removing pre-write storage admission for ordinary object PUT

Date: 2026-10-28 · Branch: `research/get-put-root-cause` · Status: **design only, no code change**

*Line citations were read on this worktree on 2026-10-28. An unrelated in-progress edit to
`crates/cairn-blob/src/lib.rs` shifts lines after ~1112 by +2 in that one file; all other citations
match the committed tree.*

Scope: `PutObject` (including inbound replica ingest), `CopyObject`, and therefore `cairn-import`
(which drives the real PUT path through `S3Service::handle`,
`cairn-server/src/import_dest.rs:1-7`). All three produce exactly one object blob through the same
stage → publish shape.

Out of scope, unchanged: multipart `UploadPart` (quota reservation), `UploadPartCopy`,
`CompleteMultipartUpload` (completion claim + assembly), `cairn storage-baseline`, restore, backup.

---

## 0. Verdict

The pre-write admission (`Mutation::Storage { Reserve }`) is the single largest removable cost on the
PUT path: **74 ms p50**, measured, versus 50 ms p50 for publication
(`docs/performance-get-put-fix-plan-2026-10.md:55-62`). It is the *first* durable barrier of the
request, it happens **before the body is read**, and no micro-optimisation removes it.

It can be removed for ordinary object PUT without weakening any of the guarantees in ARCH 8.1,
because every invariant it currently protects is re-established by (a) the publication transaction
itself, (b) the single-Writer FIFO ordering between publication and abandonment, and (c) the
mandatory full startup scan, which already understands the exact file grammar an ordinary PUT can
leave behind. Multipart keeps admission unchanged: its intent is not merely a path fence, it is the
joint quota-reservation / completion-claim savepoint (`docs/metadata.md:122-127`).

**This is a storage-protocol-2 text change** (`docs/storage-durability.md:13-18`,
`docs/metadata.md:122-127`, `docs/data-plane.md:70-75`, `docs/configuration.md:26`,
`CLAUDE.md:182-186`, `docs/operations.md:237`). It does **not** cross a `CONTRACT.md` ceiling — the
seven ordered durability steps (`CONTRACT.md:34-36`) are untouched, and no file is ever created
before its plan is fixed — but it changes what "protocol 2" promises, so it needs an explicit human
decision on record before implementation. No feature flag is proposed; a flag would violate
`CONTRACT.md:48-50`.

Headline risk: a visible row must never point at a blob that does not exist, and an unacknowledged
file must never be deleted while a publication for it could still commit. Section 2.3 is the proof
obligation; Section 5 is the test that discharges it.

---

## 1. Current sequence and what the admission protects

### 1.1 Exact current sequence (ordinary PUT)

| # | Step | Code |
|---|---|---|
| 1 | Parse/validate headers, tags, checksums, SSE plan, TTL, Object Lock intent; reject before any I/O | `service.rs:840-967` |
| 2 | Build target `{key, version_id, row_id}` | `service.rs:974-979` |
| 3 | **Acquire a bounded recovery slot**, select all three physical names in memory (`plan_write`, no I/O) | `storage_write.rs:95-101` |
| 4 | **Submit `Mutation::Storage { Reserve }` and await its durable commit** | `service.rs:202-212` → `cairn-meta/src/storage.rs:279-369` |
| 5 | Convert the Writer receipt into a move-only `StorageCreationPermit` (receipt must equal the plan, lease must own attempt+generation and not be cancelled) | `storage.rs:451-466`, `service.rs:999-1003` |
| 6 | Open/prepare `.staging`, the bucket dir and the spool parent; one deduplicated parent fsync per device+inode; create and exclusively lock the staging file | `namespace.rs:265-345`, `99-143`, `203-216` |
| 7 | Stream body → staging, hashing + compress/encrypt inline | `blob/lib.rs:1173-1188` |
| 8 | `fdatasync` file, release preallocation, **rename** to `{bucket}/{attempt}`, fsync destination dir | `staging.rs:87-152`, `blob/lib.rs:1189-1208` |
| 9 | Validate client checksums / Content-MD5 / TTL / replication intent | `service.rs:1014-1156` |
| 10 | Submit `Mutation::PublishStorageWrite { plan, PutObjectVersion, index_spool, activity }` | `service.rs:1158-1181` |
| 11 | Writer: `validate_publication` → `owns_publication` (generation current, stored plan identical, `cancelled=0`) → inner `PutObjectVersion` (precondition, bucket/user quota, Object Lock, tags, outbox) → `consume_published` (reference-checked cleanup debt for unreferenced planned paths) → activity | `apply.rs:74-110`, `storage.rs:240-256`, `416-481` |
| 12 | On `Ok(Put)` only: disarm the guard, then ACK 200 | `service.rs:1187-1245` |
| 13 | Post-ACK: replication pulse, event notification, lifecycle header (best effort) | `service.rs:1192-1245` |

Slot ownership is retained through *all* of the above plus any detached blocking job
(`storage_write.rs:41-54`, `189-196`).

### 1.2 Invariants the admission protects, and what carries each after removal

| # | Invariant today | Enforced by | Re-established after removal by |
|---|---|---|---|
| I1 | No file is created for a plan the Writer has not durably recorded | receipt == plan (`storage.rs:456`) | **deleted deliberately.** Authority becomes: in-process authorization + bounded plan + publication-time bucket fence (§2.3) |
| I2 | Every created file has a *bounded, canonical* name derived from the target; no key-derived path | `plan.validate()` + `expected_paths()` (`storage.rs:169-181, 294-349`) | unchanged (plan still built by `plan_write`) |
| I3 | Attempt tokens are unique; no two live attempts share a path | `reserve` refuses a duplicate attempt id (`storage.rs:291-300`) | unused attempt = fresh UUIDv4 (`storage.rs:19-21`); publication re-checks path availability (§2.2 step 6) |
| I4 | A path owned by a live object/part or by cleanup debt cannot be re-used | `reserve` `referenced()`/intent/cleanup check (`storage.rs:328-335`) | same check, moved into `PublishObjectWrite` |
| I5 | Bucket identity: a plan authorized against bucket *B* cannot publish into a same-named bucket owned by another principal, or into a deleted bucket | `reserve` owner compare (`storage.rs:301-313`) + `cancel_bucket` on delete (`apply.rs:645`) | publication compares `buckets.owner_id` **and** `buckets.created_at` in the same savepoint; `DeleteBucket` is serialized by the single Writer against the publication |
| I6 | Cancellation cannot let a later publication commit after cleanup began | `Cancel` marks the intent; `owns_publication` refuses `cancelled=1` | **Writer FIFO**: a publication is either never enqueued, or enqueued before the abandonment (§2.3) |
| I7 | Cleanup never unlinks a path a visible row or another outstanding owner protects | `claim` excludes object/part/intent paths (`storage.rs:736`) | unchanged; intent clause stays for multipart |
| I8 | Bucket/user quota, Object Lock, versioning, preconditions are evaluated atomically with the upsert | inside `put_version` (`apply.rs:2047-2096`) | unchanged (publication savepoint) — note quota was **never** pre-checked at admission |
| I9 | Superseded blobs become exact cleanup debt in the commit savepoint | `upsert_version` (`apply.rs:2243`) | unchanged |
| I10 | Recovery enumerates crash-leftover ownership and proves I/O quiescence before unlinking | `ListStorageIntents` → `confirm_storage_quiescence` (`stack.rs:691-697, 932-982`) | object leftovers have no journal row: the startup scan reclaims them and its `try_lock_exclusive` probe is the quiescence proof (`reconcile.rs:628-656`) |
| I11 | Unaccounted staging bytes are bounded | recovery-slot semaphore == `concurrency_limit` (default 1024, `config.rs:449`, `multipart_claim_recovery.rs:45-61`) | unchanged — the slot is still acquired before the plan, and still held until the guard/recovery drains |
| I12 | A crash leaves no row without a durable blob | blob durability strictly precedes commit | unchanged (`staging.rs:122-144` then `service.rs:1158`) |

The one invariant that genuinely *disappears* is I1. Everything else is relocated. That is the
whole proposal, and it is why the decision is a protocol-text decision rather than a bug fix.

---

## 2. Proposed sequence without pre-write admission

### 2.1 Sequence

1. Steps 1–2 unchanged.
2. **Plan in memory only**: acquire the recovery slot, call `blob.plan_write(...)`, bind
   `bucket_owner` + `bucket_created_at`, build the I/O watch. No Writer call, no await on SQLite.
   (`StorageWriteRuntime::plan_object`, §4.7.)
3. Produce a move-only `ObjectCreationPermit` (plan + live, non-cancelled lease). No receipt.
4. Staging, hashing, transform, fsync, rename, dir fsync: **byte-for-byte unchanged**
   (`staging.rs`, `blob/lib.rs:1119-1225`).
5. Validate checksums/TTL/replication intent: unchanged.
6. Submit `Mutation::PublishObjectWrite { plan, PutObjectVersion, index_spool, activity }`.
   Inside one Writer savepoint:
   a. `plan.validate()` and `plan.validate_publication(&operation)` — the row must carry exactly
      the plan's bucket/key/version/row-id and `storage_path == final path`.
   b. `current(plan.generation)` — a plan from a superseded generation never publishes.
   c. **bucket fence**: `SELECT owner_id, created_at FROM buckets WHERE name=?1`.
      Absent, or `owner_id != plan.bucket_owner`, or `created_at != plan.bucket_created_at`
      ⇒ `MutationOutcome::StoragePublicationNotApplied`.
   d. `crate::baseline::ensure_unheld(db)?` (same position as `reserve`, `storage.rs:285`).
   e. **path availability** for every planned path: not referenced by `object_versions` or
      `multipart_parts`, and no `storage_cleanups` / `storage_intent_paths` row owns it
      ⇒ otherwise `StoragePublicationNotApplied`. (Defence in depth: attempt tokens cannot collide,
      and a stale abandonment row must never be republished.)
   f. `apply_inner(PutObjectVersion)` — the existing precondition/quota/lock/tags/outbox path.
   g. On `MutationOutcome::Put`: `resolve_paths(db, &plan, index_spool)` — enqueue cleanup debt for
      every still-unreferenced planned path (the temp and, unless `NeverCreated`, the spool; the
      final is now referenced and is skipped by the reference check). Then the activity insert with
      the existing duplicate-id tolerance.
7. On `Ok(Put)`: `write_guard.disarm()`, then ACK. Unchanged.
8. On every other outcome/error: drop the armed guard → `enqueue_recovery` → the retained worker
   submits `Mutation::AbandonObjectWrite { plan, quiescence }` after the same quiescence proof used
   today, which resolves cleanup exactly like `StorageMutation::Resolve` (§3.4).

Two Writer transactions per PUT become **one**. The publication barrier (I12) is untouched.

### 2.2 Crash at each boundary

| Boundary | On-disk after crash | Metadata | Startup behaviour | API after restart |
|---|---|---|---|---|
| Plan only, no file | nothing | nothing | nothing | 404 |
| Parent dirs created (`.staging`, `{bucket}/`) | empty dirs | nothing | scan prunes empty bucket dirs (`reconcile.rs:492-498`) | 404 |
| `.staging/{attempt}.tmp` created (locked) | 0-byte temp | nothing | lock released on exit → scan unlinks (`reconcile.rs:226-232`) | 404 |
| Partial body | partial temp | nothing | same | 404 |
| Body complete, not synced | complete temp | nothing | same | 404 |
| Synced, still temp | synced temp | nothing | same | 404 |
| Renamed to `{bucket}/{attempt}`, dir not synced | temp **or** final (rename may be lost on power failure) | nothing | whichever name exists is unreferenced → unlinked, dir synced | 404 |
| Dir synced (blob durable), pre-publish | final file, no row | nothing | unlinked as an orphan (`staging_safety_margin_secs: 0`, `stack.rs:710-720`) | 404 |
| Aborted after rename (in-process, no crash) | final file | cleanup debt, then absent | — | 404 |
| **Publication committed, pre-ACK** | final file | row | row is live → preserved; scan keeps it | **200, exact bytes** (durable but unacknowledged: allowed) |
| ACK sent | final file | row | preserved | 200, exact bytes |

A crash can therefore produce only two shapes: *file without row* (reclaimed) or *row with file*
(served). Never *row without file* — the rename+dir-fsync still strictly precedes the commit.

### 2.3 The safety argument that replaces `Cancel`

The dangerous interleaving is: the request is cancelled while its publication is in flight, the
abandonment cleans the final path, and the publication then commits — leaving a visible row pointing
at a deleted blob.

`Writer::submit` is `reserve().await; permit.send(Job::Write(..)); ack_rx.await`
(`cairn-meta/src/writer.rs:238-250`). Therefore, for any one publication attempt:

* if the task is cancelled **before** `permit.send`, the mutation is never enqueued and never applies;
* if it is cancelled **at** `ack_rx.await`, the mutation is already enqueued and will apply, in FIFO
  order, before any later mutation on that writer.

The abandonment is always later: the guard is a local declared before the publication expression, so
on cancellation the in-flight publication future is dropped *before* `StorageWriteGuard::drop`
(`storage_write.rs:189-196`) enqueues the recovery record; the retained worker then submits
`AbandonObjectWrite` on the **same** bucket writer. Hence at the moment the abandonment savepoint
runs, either the publication already applied — `referenced(final)` is true and the path is kept — or
it can never apply — and the path is provably unreferenced forever.

Two supporting facts close the rest:

* **No online reconciler exists.** `reconcile` is called only at startup (`stack.rs:710`), at restore
  finalisation (`main.rs:1820`), and by the node-lock-holding `integrity` command
  (`main.rs:340-390`). During serving, the only in-process actor that can unlink a bucket file is the
  exact-cleanup worker, which acts only on claimed debt rows. A future online scan must either hold a
  safety margin larger than the maximum staging duration or adopt the descriptor hold in §7 PR4.
* **Startup is exclusive.** The full walk runs before the listener binds, under the node lock, so an
  unreferenced final file is unambiguous at that point.

This argument must be pinned by a test, not just asserted: see matrix row **P2**.

---

## 3. Recovery rules

### 3.1 How startup distinguishes an orphan final file from a live object

The existing oracle is already sufficient. `live_blobs` returns true if **any** of
`object_versions`, `storage_intent_paths`, `storage_cleanups` references the path
(`cairn-meta/src/lib.rs:234-255`; sharded router at `shard.rs:1491`; lab/async/double mirror it).
So:

* live object (any version, including superseded) → present in `object_versions` → kept;
* a path whose blob is superseded or already replaced → `storage_cleanups` row exists until the
  unlink and namespace sync succeed → kept until claimed;
* multipart part → `multipart_parts` (separate walk under `.staging/multipart`) → kept;
* crash-leftover object file → none of the three → unlinked.

**No new oracle, no new table, no new scan.**

### 3.2 Staging and index-spool aliases

`.staging/<32-hex>.tmp` and `.staging/<32-hex>.index.tmp` are exactly the grammar already recognised
by `reconcile.rs:226-232`, and they are exactly the names `expected_paths` produces
(`storage.rs:331-347`). The spool is created and immediately unlinked by the encoder
(`blob/lib.rs` create-unlink path); a crash can leave or restore the alias, and recovery has always
treated it as materialized (`storage.rs:469-479`). Nothing to add.

### 3.3 Bucket delete / recreate without an intent

Today: `DeleteBucket` re-checks emptiness in the savepoint and calls `cancel_bucket`
(`apply.rs:622-649`), which makes a late publication fail. Without intents:

* same Writer savepoint ordering: if the delete commits first, the publication finds no `buckets`
  row → refuses; if the publication commits first, the delete sees `object_versions` → `NotEmpty`;
* different owner after recreation → `owner_id` mismatch → refuses (cross-tenant fence, I5);
* same owner after recreation → `created_at` mismatch → refuses. This is the only reason to add
  `bucket_created_at` to the plan; without it the write lands in the recreated bucket (same tenant,
  arguably tolerable, but it silently resurrects an object the operator deleted). `created_at` is
  never rewritten by any mutation, and it is millisecond resolution (`cairn-types/src/time.rs:16-26`).

Refusal leaves the staged blob to the guard/scan, which reclaims it.

### 3.4 What exists vs what must be added

| Need | Status |
|---|---|
| Reclaim unreferenced final/temp/spool files at startup | **exists** (`reconcile.rs:117-194, 200-260, 565-668`) |
| Prove I/O quiescence before unlink | **exists** — `try_lock_exclusive` on the reopened file; a still-held kernel description fails the probe and fails startup closed (`reconcile.rs:628-638`) |
| Reference-checked cleanup debt | **exists** (`storage.rs:416-481, 635-676`) |
| Exact claim + unlink + dir barrier + retirement | **exists** (`storage.rs:719-780`, `blob/lib.rs:1308-1341`, `multipart_claim_recovery.rs:194-…`) |
| In-process abandonment of an object plan that has no intent | **must add** `Mutation::AbandonObjectWrite` + `resolve_paths` split (§4) |
| Publication without intent ownership | **must add** `Mutation::PublishObjectWrite` (§4) |
| Bucket-incarnation fence | **must add** `bucket_created_at` compare (§4.1) |
| Worker branch that skips `Cancel` for object plans | **must change** `recover_one` (`multipart_claim_recovery.rs:125-185`) |
| Docs + conformance proof | **must add** (§5, §8) |

`StorageMutation::Resolve` stays for parts/completions: it is the operation that requires an intent,
and that requirement is worth keeping (`storage.rs:129-140`).

---

## 4. Exact type / API changes

### 4.1 `cairn-types/src/storage.rs`

```rust
pub struct StorageWritePlan {
    pub attempt: StorageToken,
    pub generation: StorageToken,
    pub bucket: BucketName,
    pub bucket_owner: Option<UserId>,
    /// Bucket incarnation the request was authorized against. `None` for plans recovered
    /// from an older journal, where the comparison is skipped.
    #[serde(default)]
    pub bucket_created_at: Option<Timestamp>,
    pub target: StorageWriteTarget,
    pub paths: Vec<StorageIntentPath>,
}
```

```rust
/// File-free plan for one ordinary object write. Not a Writer acknowledgement: publication
/// re-establishes bucket identity and path availability in its own savepoint (ARCH 8.2).
#[derive(Debug)]
pub struct ObjectCreationPermit { plan: StorageWritePlan, lease: io::StorageIoLease }

impl PlannedStorageWrite {
    /// Bind the authorized bucket snapshot (owner + incarnation).
    pub fn with_bucket_snapshot(self, owner: UserId, created_at: Timestamp) -> Self;
    /// Consume only for an `Object` target. No Writer receipt: this is not admission.
    pub fn into_object_permit(self, lease: io::StorageIoLease) -> Result<ObjectCreationPermit, BlobError>;
}

impl ObjectCreationPermit {
    pub fn plan(&self) -> &StorageWritePlan;
    pub fn into_parts(self) -> (StorageWritePlan, io::StorageIoLease);
}
```

`validate_publication` is reused verbatim. `validate_admission` is unchanged and stays
parts/completions-only. `validate_unadmitted_mutation` (`storage.rs:494-510`) is unchanged: a bare
`PutObjectVersion` carrying a `storage_path` is still rejected; only the new mutation may carry one.

Also delete `StorageWritePlan::with_bucket_owner` in favour of `with_bucket_snapshot` (one caller).

### 4.2 `cairn-types/src/meta.rs`

```rust
/// Publish a planned, never-journaled ordinary object write. Skips intent ownership and instead
/// verifies the authorized bucket identity and plan path availability in this savepoint.
/// Only `PutObjectVersion` is accepted inside.
PublishObjectWrite {
    plan: Box<StorageWritePlan>,
    operation: Box<Mutation>,
    index_spool: IndexSpoolState,
    activity: Option<Box<ActivityEntry>>,
},

/// Abandon a planned ordinary object write after proven backend quiescence. Object targets only.
/// If an intent exists for the same attempt (mixed state), it is resolved exactly as
/// `StorageMutation::Resolve` would resolve it.
AbandonObjectWrite {
    plan: Box<StorageWritePlan>,
    quiescence: io::StorageQuiescence,
},
```

`apply_inner` must reject both as nested (`apply.rs:148-150`), and `cairn-meta/src/cache.rs:489`
(the "no bucket/config change" catch-all) must list them.

### 4.3 SQLite — `cairn-meta/src/storage.rs` + `apply.rs`

Split the shared resolver:

```rust
/// Enqueue exact cleanup for every planned path that no authoritative row references.
/// No intent required, no intent deleted.
pub(crate) fn resolve_paths(db, plan, index_spool) -> R<()>          // body of today's `resolve` loop
pub fn resolve(db, plan, index_spool) -> R<()> { validate_index(); resolve_paths(); discard_unacknowledged() }

/// One savepoint: owner/incarnation fence, generation, path availability, inner mutation,
/// reference-checked cleanup, activity.
pub fn publish_object(db, plan, operation, index_spool, activity) -> R<MutationOutcome>;
```

`abandon_object(db, plan, quiescence)`:

```
if plan.target is not Object            -> Err(engine("abandon requires an object target"))
if !current(quiescence.generation())    -> StorageUpdated { applied: false }
if quiescence.attempt() != plan.attempt
   || quiescence.generation() != plan.generation
                                        -> Err(engine("quiescence identity mismatch"))
if load(db, bucket, attempt, generation)?.is_some()   // mixed/rollback state
      -> return resolve(db, &stored_plan, Materialized)
resolve_paths(db, plan, Materialized)   // conservative spool state, as recovery always is
-> StorageUpdated { applied: true }
```

Note it does **not** call `ensure_unheld`: abandonment creates no physical reference and must work
while a baseline hold is set.

### 4.4 libSQL / Turso — `cairn-meta-async/src/{storage,apply}.rs`

Byte-identical SQL and outcomes (`storage.rs:106-237` mirrors the SQLite `apply`). Same three
functions, same `ensure_unheld` position. No behavioural divergence is acceptable: the async backend
is compiled by `cargo clippy --workspace` and by the storage contract tests.

### 4.5 Sharded router — `cairn-meta/src/shard.rs`

Three edits, all compiler-forced:

1. the multi-shard activity split (`shard.rs:533-564`) — duplicate the block for `PublishObjectWrite`
   (per-shard publication, best-effort activity to the global shard);
2. the per-bucket routing arm list (`shard.rs:566-595`) — add `PublishObjectWrite` and
   `AbandonObjectWrite`;
3. `mutation_bucket` (`shard.rs:1363-1450`, explicitly exhaustive) — `plan.bucket` for both.

`Storage { bucket, .. }` already routes, so nothing else is needed. `Mutation::ListStorageIntents`
and `ClaimStorageCleanup` are untouched (object plans add no intents; cleanup rows are already global
per shard).

### 4.6 In-memory double — `cairn-types/src/testing/{meta,storage}.rs`

Mirror the two mutations; reuse the existing `enqueue`/`reference` helpers
(`testing/storage.rs:171-256`). The double must also grow one test hook the ordering proof needs:

```rust
pub fn hang_next_publication(&self);   // hold the savepoint until released
pub fn release_hung_publication(&self);
```

(The double already has analogous admission hooks at `testing/meta.rs:479-524`; those stay for the
multipart contract tests.)

### 4.7 `+1` laboratory backend — `conformance/storage_lab/src/metadata_fjall/apply.rs`

`PublishStorageWrite` is implemented at `apply.rs:357-390` and the journal at `journal.rs`
(`publication_allowed`/`resolve`). Add the two new arms. **This crate is a separate workspace**
(`conformance/storage_lab/Cargo.toml:1`), so `cargo clippy --workspace` will **not** catch a missing
arm; it must be a checklist item in the PR and run via the lab's own build.

### 4.8 Blob layer, protocol layer, server

| File | Change |
|---|---|
| `cairn-types/src/traits.rs:44-70` | `stage(&self, permit: ObjectCreationPermit, ...)`; `plan_write` default method unchanged |
| `cairn-blob/src/lib.rs:1119-1225` | `stage` takes `ObjectCreationPermit`; keep the `target is Object` assertion as defence |
| `cairn-blob/src/namespace.rs:265-345` | signature only (`AdmittedPaths::open*` take the new permit) |
| `cairn-protocol/src/storage_write.rs` | add `plan_object()` returning `(ObjectCreationPermit, StorageWriteGuard)`; add `StorageWriteGuard::planned_publication(operation, index_spool, activity) -> Mutation::PublishObjectWrite`; keep `prepare`/`admit`/`publication` for parts and completions |
| `cairn-protocol/src/service.rs:178-223` | replace `admit_object_storage` with `plan_object_storage` (no `meta.submit`); `put_object:984` and `copy_object:3155` call it |
| `cairn-protocol/src/service.rs:1247-1254, 3283-3300` | unchanged: `disarm` on `Ok(Put)`, `recover_object_write` otherwise |
| `cairn-server/src/multipart_claim_recovery.rs:125-185` | `recover_one`: for `Object` targets skip `Cancel`, submit `AbandonObjectWrite`; parts/completions keep the current path (cancel → quiescence → resolve → claim release) |
| `cairn-types/src/testing/blob.rs:284-340` | signature only |

Note `StorageWriteRecovery` needs **no** new field: the branch is on `record.plan.target`, which is
already carried.

### 4.9 Deletions (no dual design)

* `Service::admit_object_storage` and the `StorageMutation::Reserve` call site — **gone**.
* `StorageMutation::Reserve` itself: **keep**. It is the generic journal primitive used by the
  storage contract tests, `cairn-types/src/testing/publication_fixture.rs`, the fanout bench and the
  lab; it no longer has a production request-path caller, which the variant doc must say.
* `StorageWritePlan::with_bucket_owner` — replaced.
* No new `CAIRN_*` variable. No flag. No schema change.

### 4.10 Schema impact

**None.** `storage_write_intents` / `storage_intent_paths` / `storage_cleanups` /
`storage_recovery_state` (`schema.rs:877-941`) are unchanged; object writes simply stop inserting
intent rows. `bucket_created_at` lives only in the plan JSON (intent rows for multipart), guarded by
`#[serde(default)]`, so old intents still deserialize and new binaries never downgrade (v35 gate).

---

## 5. Crash / power-loss test matrix

All fixtures run a `--features failpoints` binary under `/tmp/opencode`. `FAILPOINTS=<seam>=sleep(60000)`
plus a harness that polls for the expected on-disk state and then `SIGKILL`s gives a deterministic
kill at the seam without new kill machinery (the pattern used by `conformance/mesh.py:165`,
`replication_chaos.py:89`). Every row asserts the same three post-restart predicates:

* `P` — every `object_versions.storage_path` exists as a regular file (stat sweep, as `backup` does);
* `O` — no unreferenced file remains in `{bucket}/` or `.staging/`: `cairn integrity` exits 0 with a
  parsed report, and a directory listing of both trees contains no unexpected name;
* `A` — the ACK ledger: every 200 recorded by the client is GETtable with the exact bytes/sha256.

New seams required (all one-liners, `fail::fail_point!`): `blob_after_staging_create`,
`blob_before_finalize`, `blob_before_rename`, `blob_after_rename` (after `commit_sampled`, before
`dir_sync`), `put_after_publish` (protocol, immediately after the `Ok(Put)` arm begins).
`blob_after_durable` already exists (`blob/lib.rs:1215`).

| ID | Boundary | Injection | Expected survivor | Key assertions |
|---|---|---|---|---|
| U1 | staging file created | `blob_after_staging_create=sleep` + kill | 0-byte `.staging/{id}.tmp` | `O`; 404 |
| U2 | partial body | slow client, kill at ~50 % | partial temp | `O`; 404; no `object_versions` row |
| U3 | body complete, unsynced | `blob_before_finalize=sleep` | complete temp | `O`; 404 |
| U4 | synced, still temp | seam before rename | synced temp | `O`; 404 |
| U5 | renamed, dir unsynced | `blob_after_rename=sleep` | temp or final (power-loss arm may lose the rename) | `O`; 404 |
| U6 | durable, pre-publish | `blob_after_durable=sleep` (existing) | final file, no row | `O`; 404 |
| U7 | published, pre-ACK | `put_after_publish=sleep` | row + final file | `P`; 200 + exact bytes; `A` (unacknowledged but durable is allowed) |
| U8 | ACKed | none | row + file | `A`; 200 |
| U9 | abort after rename (client disconnect) | close socket after body, before response | cleanup debt → absent | `O`; 404; no row |
| P1 | 64 concurrent 1 MiB PUTs + kill | kill mid-run | every ACKed object | `A` for all ACKed; `O`; `P` |
| P2 | **cancel while the publication is queued** | double: `hang_next_publication`, cancel the request, release, then run recovery | publication applied → path kept; or never applied → path removed | never a row without its file; `err`/`None` pairing exactly matches §2.3 |
| D1 | delete bucket mid-PUT, recreate as **other** principal, complete body | client A slow body; admin deletes; principal B creates same name | publication refused | 5xx (today also 5xx via cancelled intent); no object under B; `O` |
| D2 | delete + recreate by the **same** principal | as D1 | publication refused via `created_at` | same as D1 |
| D3 | delete mid-PUT, no recreate | as D1 without recreate | refused | 5xx; `O` |
| S1 | orphan final file | plant `{bucket}/{uuid}` offline | reclaimed | `integrity`: `orphans_reclaimed >= 1`; GET 404 |
| S2 | orphan staging + spool alias | plant `.staging/{id}.tmp`, `.staging/{id}.index.tmp` | reclaimed | `staging_cleaned >= 2`; aliases absent |
| S3 | locked orphan | harness holds `flock` on the file, then start | **startup fails closed** | `serve` exits non-zero; release → starts and reclaims |
| S4 | path with cleanup debt | plant file + debt row | kept until claimed | present before drain, absent after |
| S5 | row whose file is missing | delete a live blob offline | row kept (never silently dropped) | GET returns a clear error; `cairn integrity` does **not** delete the row |
| B1 | crash leftover + backup + restore | U6 then `cairn backup`/`restore` | leftover excluded or reclaimed | restored tree has no orphan; `.staging/*.tmp` absent from snapshot |
| B2 | ACK ledger across backup/restore | write N objects, backup, restore | all present | `A`; versioned reads exact |
| B3 | restored DB has no object intents | inspect `storage_write_intents` | only multipart rows | count of object-target rows == 0 |
| Q1 | bucket quota exceeded | `quota_bytes` below object size | 507 | no row; `O`; quota counters unchanged |
| Q2 | exactly at quota | equal size | 200 | `A` |
| V1 | versioned, same key, concurrent PUTs | 2 clients | 2 rows, 2 blobs, one `is_latest` | both version GETs exact; no cleanup of either |
| V2 | unversioned overwrite | 2 sequential PUTs | 1 row, new bytes | superseded path unlinked; `O` |
| V3 | Object Lock retention | PUT with retention, crash pre-publish | no lock row | `O`; a later delete is not blocked by a phantom |
| V4 | TTL | PUT with relative TTL, crash pre-publish | nothing | `O`; no expired row |

Gate placement: U1–U9 + P1 into `conformance/crash_multipoint.sh` (it already builds with
`--features failpoints` and calls `cairn integrity`); P2/D1–D3 into the protocol + meta crate tests;
S1–S5 into `conformance/crash_consistency.sh`'s planted-orphan arm; B1–B3 into
`conformance/backup_restore.sh`; Q/V into `objects.sh`/`object_lock.sh`.

---

## 6. Risks, and the smaller fallback

| # | Risk | Severity | Mitigation |
|---|---|---|---|
| R1 | A visible row points at a deleted blob (the §2.3 race) | fatal | FIFO argument + P2 test; no online reconciler; `claim` re-checks references |
| R2 | Unreferenced final files accumulate between restarts | medium | abandonment cleans them in-process (§3.4); the residual is bounded by aborted-after-rename requests, which the §5 U9/P1 rows measure |
| R3 | A future online/periodic reconcile deletes an in-flight final file | latent | §7 PR4 holds the final descriptor's exclusive lock until commit, so `reclaim_unreferenced`'s existing lock probe skips it regardless of margin |
| R4 | 507 no longer returned *before* the body is read | user-visible | `BlobError::OutOfSpace → 507` still (`cairn-types/src/error.rs:327`); rewrite the two tests that assert "before body or file creation" (`protocol_core.rs:385-397, 1910-1945`) |
| R5 | Cross-tenant delete/recreate | fatal if missed | `owner_id` + `created_at` compare inside the publication savepoint; D1/D2 |
| R6 | A plan is published under a superseded generation | low | `current(generation)` check |
| R7 | Intent-dependent tooling breaks (`storage-baseline` classification, `lab.py` counts at `conformance/storage_lab/lab.py:157`) | low | they already treat the tables as optional; update the lab report expectations |
| R8 | Query-count/benchmark reports shift (`put_timing` stage `storage_admission`) | low | keep the stage label and redefine it as "plan + bounded slot" (`put_timing.rs:17`), or rename and update `root_barrier_campaign.py:44` |
| R9 | A reviewer reads this as a durability relaxation | high | §2.2 table + §5 matrix + explicit protocol-text diff; the ACK path still has exactly one durable barrier |

**Smaller fallbacks, in order of preference if the full change is judged too broad:**

1. **Scope to `PutObject` only** (leave `CopyObject` admitted). One call site, one publication path
   removed; copy keeps `PublishStorageWrite`. Saves ~70 % of the benefit (copy/import are not the
   benchmark path) and ~80 lines. Does not add a flag.
2. **Late admission** — keep the intent, but insert it after the final file is durable and before the
   publication. Preserves every invariant *and* every recovery rule verbatim, removes the Writer
   wait from the pre-body path, but keeps **two** Writer transactions per PUT and therefore most of
   the writer-throughput cost. Only worth it if R1 is judged unacceptable.
3. **Nothing structural**: the earlier PR already removed one directory barrier
   (`perf/put-flush-reduction`). Do not chase per-request micro-optimisations further; the 74 ms is a
   durable barrier, not syscall overhead.

Explicitly **not** acceptable: a config knob (CONTRACT 48-50), a non-durable/best-effort admission
acknowledgement (breaks "an unacknowledged admission cannot authorize file creation" while gaining
nothing durable), or an online orphan sweeper as a substitute for the startup scan.

---

## 7. PR sequence, size, and gates

| PR | Contents | Est. diff | Gate |
|---|---|---|---|
| **1** | Types + all five backends + contract tests. `ObjectCreationPermit`, `bucket_created_at`, `PublishObjectWrite`, `AbandonObjectWrite`, `resolve_paths` split, shard routing, cache arm, double, lab. **No production caller yet.** | ~1 000 prod + ~350 test | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings` and again with `--all-features`; `cargo nextest run --workspace`; `cargo test --workspace --doc`; `cargo audit`; the shared storage contract suite on sqlite/async/double/sharded; `cd conformance/storage_lab && cargo test` (separate workspace, not covered by `--workspace`) |
| **2** | Protocol + blob + recovery worker: `plan_object`, `stage(ObjectCreationPermit)`, `put_object`, `copy_object`, `recover_one` branch, `put_after_publish` seam. Behaviour change lands here. | ~350 prod + ~250 test | PR1 gate **plus** `conformance/crash_multipoint.sh`, `crash_consistency.sh`, `objects.sh`, `multipart.sh`, `encryption.sh`, `object_lock.sh`, `checksums.sh`; paired fr38 PUT/GET run (the stop rule: if PUT does not improve on the paired baseline, revert this PR) |
| **3** | Conformance + docs: the §5 matrix, the ACK-ledger ledgers, `backup_restore.sh` rows, and the documentation edits of §8. | ~450 harness + ~120 docs | PR2 gate + `backup_restore.sh`, `run.sh` smoke, docs link/anchor check |
| **4** (optional) | Publication hold: return the final descriptor from `Staging::commit`, keep it in `StagedBlob`, drop after `disarm()`. Removes R3 permanently. | ~120 | PR2 gate + S3 variant that runs a scan while a PUT is between rename and commit |

Revert unit is PR2 alone (PR1 is inert). No schema migration is ever involved, so a revert needs no
data repair — only the startup scan, which runs anyway.

---

## 8. Documentation to change (same PR as the behaviour change)

| File:line | Change |
|---|---|
| `docs/storage-durability.md:13-18` (8.2) | split the first paragraph: ordinary object plans are in memory; only part/assembly plans are Writer-admitted; state the new crash shapes and the §2.3 FIFO argument |
| `docs/storage-durability.md:42-70` | cancellation paragraph: object plans abandon through `AbandonObjectWrite`; the spool/materialized rule is unchanged |
| `docs/metadata.md:122-127` (11.6) | "Protocol 2 requires a file-free plan before physical creation **for multipart**…"; describe `PublishObjectWrite`'s four checks |
| `docs/metadata.md:144-160` | resolution/cancellation: objects reference-check instead of cancel |
| `docs/metadata.md:212, 228` | physical-ownership surface list |
| `docs/data-plane.md:29` | "Before admission, every physical write captures…" → object writes capture the plan locally; parts capture admission |
| `docs/data-plane.md:70-75` (7.5) | rewrite: one durable Writer barrier per PUT, not two; keep the "group commit cannot remove the pre-I/O dependency" sentence only for multipart |
| `docs/configuration.md:26` | "durable admission and exact cleanup accounting apply to every physical write" → admission applies to multipart; exact cleanup to every write |
| `docs/operations.md:237` | "Every write first commits its exact intended filenames" → multipart only; objects publish once |
| `docs/observability.md:107-110` | redefine the `storage_admission` stage |
| `docs/delivery.md:81` | cancelled object writes |
| `CLAUDE.md:182-186` | protocol-2 paragraph, same split |
| `docs/performance-get-put-fix-plan-2026-10.md:103-107` | mark Phase 3 decided + measured result |

---

## 9. Decisions needed before implementation

1. Approve the protocol-2 text change (I1 is deliberately dropped for ordinary object PUT).
2. Approve `bucket_created_at` in `StorageWritePlan` (same-owner delete/recreate fence) — JSON only,
   no schema.
3. Choose PR4 (descriptor hold) yes/no; it is the only defence against a future online reconciler.
4. Accept the 507-after-body semantic change (R4), or keep a cheap pre-check (e.g. a bucket
   `quota_bytes` read) to preserve an early rejection for the quota case only.
5. Confirm the lab backend (`conformance/storage_lab`) is updated by hand, since CI does not compile
   it.
