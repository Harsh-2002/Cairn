# Removing SQLite: what a custom metadata engine would have to rebuild

**Question.** If Cairn replaced its embedded SQLite metadata store with a purpose-built engine,
what exactly must be rebuilt, and what could break?

**Method.** Static reading only. No code changed, no benchmark run, no git write. Branch
`perf/audit-co-commit`, worktree `/home/dev/Cairn`. Counts were produced with `grep`/`python3`
over the files named in each row; they are reproducible from the working tree. Every number below
is labelled **FACT** (read out of the repo) or **ESTIMATE** (my inference).

**Upstream constraint.** This change cannot be made by an agent at all. `CONTRACT.md:22-25`
fixes the ceiling: *"Metadata — one embedded SQLite database, one writer. No external database …
no second store of record … If something seems to need its own datastore, it needs a human
decision first."* `CONTRACT.md:31-32` removes down-migrations ("append-only … There is no
down-migration story and none is wanted"). `CONTRACT.md:34-36` fixes the durability ordering.
`docs/metadata.md:9` already frames the engine as swappable behind `MetadataStore`; `docs/delivery.md:230`
names "a future metadata engine" as an explicit motivation for the abstraction boundary.

---

## 1. FACT — measured inventory of what exists today

### 1.1 Schema

| Metric | Value | Source |
|---|---|---|
| Append-only migrations | **40** (`v1` … `v40`) | `crates/cairn-meta/src/schema.rs:14` (`const MIGRATIONS`), `:947` (`version: 40`) |
| Distinct `CREATE TABLE` names | **31** app tables + `schema_migrations` ledger = **32 on disk** | dedup within the `MIGRATIONS` block |
| `CREATE TABLE` blocks inside migrations | 33 (2 names re-created after a rename: `object_shares`, `object_locks`) | `schema.rs` |
| Distinct index names | **34** (38 `CREATE INDEX` blocks; 4 re-created, e.g. `idx_ov_latest_cover` 4×) | dedup of `schema.rs` |
| `ALTER TABLE` statements (in migrations) | **53** | `schema.rs` |
| `DROP TABLE` / `DROP INDEX` (in migrations) | 2 renamed-then-dropped (`object_shares_v24`, `object_locks_v16`); 6 index drops | `schema.rs:158,305,603,604,642,721,752,784,970` |
| `FOREIGN KEY` constraints | **5 only** | `schema.rs:115,667,736,900,1803` |
| Columns across the 31 migration tables (max per table, excl. constraints) | **227**, plus 53 `ALTER TABLE` additions | measured |
| `PRAGMA` occurrences in schema | 16 | `schema.rs` |
| Migration SQL body | ≈**964 lines** | `awk` over `MIGRATIONS` |

**Tables** (31 application tables): `users`, `user_stats`, `session_credentials`, `buckets`,
`bucket_config`, `bucket_stats`, `account_config`, `object_versions`, `object_tags`, `object_locks`,
`object_shares`, `share_capability_sanitation`, `multipart_uploads`, `multipart_parts`,
`multipart_part_reservations`, `multipart_staging_cleanups`, `multipart_bucket_stats`,
`multipart_principal_stats`, `replication_outbox`, `replication_uploads`, `events_outbox`,
`import_jobs`, `activity`, `request_metrics`, `key_ring_state`, `rewrap_progress`,
`storage_protocol`, `storage_recovery_state`, `storage_write_intents`, `storage_intent_paths`,
`storage_cleanups`.

Notable schema shapes a non-SQL engine must reproduce by hand:

- **Partial indexes** — 8 `CREATE INDEX … WHERE …` predicates in migrations: `WHERE is_latest = 1`
  (3×, incl. `idx_ov_latest_cover` which carries the internal immutable row id so lifecycle
  compare-and-delete does not need a table lookup — `docs/metadata.md:89`, v28 at `schema.rs:765`),
  `quota_owner_path IS NOT NULL`, `storage_path IS NOT NULL`, `upload_id IS NOT NULL`,
  `reservation_id IS NOT NULL`, `expire_at IS NOT NULL` (v40 TTL).
  `schema.rs:309,788,929,931,940,942,967,974`
- **Covering indexes** — `idx_ov_latest_cover` (`schema.rs`, re-created at `:970` for v40 TTL),
  `idx_object_versions_bkv`, `idx_outbox_bucket_key`, `idx_multipart_cleanups_path`.
- **Upserts** — 7 `ON CONFLICT … DO UPDATE` sites: `apply.rs:1586` (metrics roll-up),
  `:1802` (`object_tags`), `:2334`/`:2344` (`bucket_stats`/`user_stats` deltas),
  `store.rs:494,528,613` (rewrap progress + key-ring active flag).
- **CHECK constraints** as enums — e.g. `role IN ('administrator','member')`, `versioning_state IN
  ('unversioned','enabled','suspended')`, `lock_mode`/`retain_until` pairing, `legal_hold IN (0,1)`,
  `part_number BETWEEN 1 AND 10000`, `legacy_accounting_hold IN (0,1)`.
- **JSON-as-text columns** — ACLs, checksums, user metadata, compression descriptors, storage
  plans, `replica_intent`. `docs/delivery.md:255` describes these as "validated text".

### 1.2 Mutation surface

| Metric | Value | Source |
|---|---|---|
| `Mutation` variants | **78** | `crates/cairn-types/src/meta.rs:82` (brace-matched count) |
| `MutationOutcome` variants | **32** | `crates/cairn-types/src/meta.rs:849` |
| `MetaError` variants | **14** | `crates/cairn-types/src/error.rs` |
| `StorageMutation` variants (inner) | 5 | `crates/cairn-types/src/storage.rs:370` |
| Exhaustive routing arms (`mutation_bucket`) | 78 | `crates/cairn-meta/src/shard.rs:1363` |
| `submit()` call sites in protocol/server/control/background engines | 221 total: `cairn-server` 104, `cairn-protocol` 37, `cairn-control` 34, `cairn-lifecycle` 22, `cairn-replication` 13, `cairn-webhook` 11 | measured |

`Mutation` family groups (all **78**, from `meta.rs:82-845`; the enum ends at `:845`, before
`MutationOutcome` begins at `:849`):

- **Storage protocol-2 + baseline (12)**: `BeginStorageBaseline`, `ClassifyStorageBaseline`,
  `AuthorizeStorageBaselineRelease`, `FinalizeStorageBaselineLegacy`, `CompleteStorageBaseline`,
  `BeginStorageGeneration`, `PrepareStorageRestore`, `Storage`, `AdmitStorageWrite`,
  `PublishStorageWrite`, `ListStorageIntents`, `ClaimStorageCleanup`.
- **Object versions (4)**: `PutObjectVersion`, `ResolveObjectWrite`, `CreateDeleteMarker`,
  `DeleteVersion`.
- **Multipart (13)**: `CreateMultipart`, `ReserveMultipartPart`, `ReleaseMultipartReservation`,
  `RecordPart`, `ResolveMultipartPartWrite`, `ReleaseMultipartCleanup`,
  `ReleaseMultipartUploadCleanups`, `RecoverMultipartStagingAccounting`, `ClaimMultipart`,
  `ReleaseMultipartClaim`, `RecoverMultipartClaims`, `CompleteMultipart`, `AbortMultipart`.
- **Bucket + account config (12)**: `CreateBucket`, `CreateObjectLockBucket`, `DeleteBucket`,
  `SetBucketConfig`, `UpdateObjectLockConfiguration`, `SetObjectRetention`, `SetObjectLegalHold`,
  `SetVersioning`, `SetOwnership`, `SetBucketQuota`, `SetBucketCompression`,
  `SetAccountPublicAccessBlock`.
- **Object attributes (3)**: `PutObjectTags`, `DeleteObjectTags`, `SetObjectAcl`. (Object TTL is a
  column, not a separate mutation — `PutObjectVersion.expire_at`, v40.)
- **Identity: users, keys, sessions (9)**: `CreateUser`, `UpdateUser`, `DeactivateUser`,
  `DeleteUser`, `SetUserPolicy`, `SetUserQuota`, `CreateSessionCredential`,
  `DeleteExpiredSessionCredentials`, `DeleteSessionCredential`.
- **Replication outbox + remote-upload journal (13)**: `ClaimReplicationBatch`,
  `MarkReplicationDone`, `MarkReplicationFailed`, `RetryFailedReplication`,
  `RequeueReplicationVersions`, `EnqueueReplication`, `PruneReplicationOutbox`, `DeferReplication`,
  `RecoverClaimedReplication`, `RenewReplicationClaim`, `ReplicationUpload`,
  `ClaimReplicationUploadCleanup`, `PruneEventsOutbox`.
- **Webhook outbox (4)**: `EnqueueWebhooks`, `ClaimWebhookBatch`, `MarkWebhookDone`,
  `MarkWebhookFailed`.
- **Imports (4)**: `CreateImportJob`, `UpdateImportJobProgress`, `SetImportJobState`,
  `PruneImportJobs`.
- **Activity, metrics, shares (4)**: `RecordActivity`, `RecordRequestMetrics`, `CreateShare`,
  `RevokeShare`.

`MutationOutcome` (32 variants, `meta.rs:849`) additionally carries the internal
reads-through-the-writer results `StorageBaselineLegacyPage` and `MultipartAccountingHeld`.

### 1.3 Read surface

| Metric | Value | Source |
|---|---|---|
| `MetadataStore` trait methods | **54** | `crates/cairn-types/src/traits.rs:247` |
| Distinct SQL literals in `cairn-meta` write path (`apply.rs`,`storage.rs`,`baseline.rs`,`replication_upload.rs`) | **234** | measured |
| Distinct SQL literals in `cairn-meta` `store.rs` | **79** (mostly `SELECT`; plus the SQLite-only master-key re-wrap CAS/`key_ring_apply_config` writes at `store.rs:270-629` executed through `Writer::run_exec`, `writer.rs:297`) | measured |
| Distinct SQL literals, sync backend total | **308** | measured |
| Distinct SQL literals, async backend total | **265** | measured |
| Distinct SQL literals, union of both backends | **463** | measured |
| `prepare_cached(` call sites in `cairn-meta` | 68 (`apply.rs` 32, `store.rs` 31, `baseline.rs` 2, `lib.rs` 2, `storage.rs` 1) | measured |
| `rusqlite` call sites in `cairn-meta` (incl. in-crate tests) | ≈**471** | measured |
| `COUNT(*)`/`SUM(`/`MAX(`/`MIN(` uses | 49 (`apply.rs` 21, `store.rs` 28) | measured |

> Counting caveat: literal-based counting under-counts statements assembled with `format!`/
> concatenation and over-counts SQL appearing only in test modules. Treat 308/265/463 as
> order-of-magnitude; the *ratio* (write path ≈ 3× read path) is the meaningful signal.

### 1.4 Code volume and tests

| Artifact | LOC / count | Source |
|---|---|---|
| `crates/cairn-meta/src/*.rs` (sync engine — the thing being replaced) | **16,251** | `wc -l` |
| `crates/cairn-meta/tests/*.rs` | **6,885** | `wc -l` |
| `crates/cairn-meta-async/src/*.rs` (libSQL/Turso parity backend) | **10,687** | `wc -l` |
| `crates/cairn-types/src/meta.rs` | 2,288 | `wc -l` |
| `crates/cairn-types/src/traits.rs` | 651 | `wc -l` |
| `crates/cairn-types/src/storage.rs` | 635 | `wc -l` |
| `crates/cairn-types/src/testing/meta.rs` (in-memory double) | **3,973** | `wc -l` |
| Shared backend-contract modules | 1,468 (`storage_contract`) + 706 (`storage_baseline_contract`) + 396 (`replication_upload_contract`) = **2,570**, plus a 283-LOC `publication_fixture` builder | `wc -l` |
| Tests directly in `cairn-meta` | **170** | counted both `#[test]` and `#[tokio::test]` markers |
| Tests in `cairn-meta-async` | **130** | same method |
| Tests in `cairn-types` (double + contracts) | **72** | same method |
| Tests in `cairn-protocol` (`cairn-meta` is a **dev-dependency** only, `crates/cairn-protocol/Cargo.toml:39-41`) | 226 | same |
| Tests in `cairn-server` | 348 | same method |
| Python conformance tests (lab, live fixtures) | 105 in `conformance/storage_lab/*.py` | `grep -c 'def test_'` |
| `CAIRN_META_*` knobs | **9**: `BACKEND`, `SHARDS`, `SYNCHRONOUS`, `READ_POOL_SIZE`, `CACHE_BYTES`, `CACHE_BYTES_PER_CONN`, `CACHE_TOTAL_BUDGET_BYTES`, `MMAP_BYTES`, `GROUP_COMMIT_LINGER_MICROS` | `crates/cairn-server/src/config.rs` |
| `ConfigAspect` variants (one `bucket_config` row each) | **12**: policy, ACL, CORS, lifecycle, replication, replication targets, tagging, BPA, encryption, Object Lock, notification, object TTL | `crates/cairn-types/src/bucket.rs:41` |

Production dependency edges: `cairn-meta` is depended on by `cairn-server` (production) and
`cairn-protocol` (**dev-only**). `rusqlite` is used by `cairn-meta` and `cairn-server`
(`crates/cairn-server/src/main.rs`, `background.rs`, `stack.rs`, `config.rs`, `build.rs`).

---

## 2. FACT — the ten subsystems that would have to be rebuilt

### 2.1 The transactional core (`writer.rs`, 960 LOC)

The current contract, verbatim from `writer.rs:1-7`: *"drains its queue, applies every waiting
mutation in one transaction — each wrapped in its own savepoint so a logical failure rolls back only
itself — commits once with a single durability barrier, and only then acknowledges every caller
whose mutation was in that batch."*

Mechanically (`writer.rs:475-600`):

| Step | Code |
|---|---|
| Batch admission, cap 256 | `writer.rs:21` (`const MAX_BATCH`) |
| `BEGIN IMMEDIATE` | `writer.rs:488` |
| `SAVEPOINT sp{idx}` per mutation | `writer.rs:512` |
| `RELEASE sp{idx}` on success | `writer.rs:519` |
| `ROLLBACK TO sp; RELEASE sp` on logical error | `writer.rs:529` |
| Whole-batch `ROLLBACK` when savepoint bookkeeping itself fails (audit #17) | `writer.rs:545-561` (ROLLBACK at `:549`) |
| `COMMIT` = the durability barrier | `writer.rs:565` |
| Checkpoint on the same thread | `writer.rs:447-460`, `PRAGMA wal_checkpoint(TRUNCATE)` at `:452` |
| Serialized control seam (`Checkpoint`/`Probe`/`Exec`) | `writer.rs:128`, `:297` |

**Rebuild required:** nested, per-item rollback inside one durable outer commit; a
durable-before-ack barrier; a truncating checkpoint with *immediate-busy* semantics (the current
code drops `busy_timeout` so a reader-contended checkpoint does not stall every PUT —
`cairn-meta/CLAUDE.md:14-16`, `writer.rs:441`); and a control channel that can run an arbitrary
serialized closure (used by master-key re-wrap, `store.rs:270-629`).

### 2.2 The read pool and snapshot isolation

`r2d2` + `r2d2_sqlite` (`lib.rs:16-17`), per-connection PRAGMAs on open, `wal_autocheckpoint=0`
so the background loop is the *sole* checkpointer (`cairn-meta/CLAUDE.md:9-10`), and
`read_probe` = `SELECT 1` on a checked-out connection (`store.rs:32`, `:972`) — explicitly *not*
`list_buckets`, so probe cost is independent of object count (`docs/metadata.md:89`).

**Rebuild required:** many concurrent readers each seeing a stable snapshot while one writer
commits; a constant-cost liveness probe; and a pool whose sizing knob (`CAIRN_META_READ_POOL_SIZE`)
still means something.

### 2.3 The query layer — the largest single piece of application logic

79 distinct read statements in `store.rs` implement 54 trait methods. The performance-critical
shapes (`docs/metadata.md:87-105`):

- **Current listing**: half-open range seek over the indexed key column, seeking to
  `max(continuation, prefix)`, stopping at the computed prefix upper bound, with
  `is_latest=1 AND NOT delete_marker`, `LIMIT page_size + 1` for truncation detection. Terminates
  early instead of scanning (F-11).
- **Version listing**: same range seek without the latest-only predicate, ordered by key then
  version recency. A page ending inside one key emits a **paired (key, version-id) marker**, and
  the next query applies the ordered-pair predicate *in SQL* before its internal `LIMIT`
  (`docs/metadata.md:89`) — this is the guard against gaps/duplicates/memory spikes on a key with
  thousands of versions.
- **Delimiter grouping** with the empty-delimiter normalisation. `cairn-meta/CLAUDE.md:47-51`
  records the exact bug this prevents: `"".find` is `Some(0)`, so an unguarded empty delimiter
  collapses every key into one prefix and returns zero objects — which broke warp/minio-go.
- **Byte-order primitives**: `range.rs` `successor()` (append `\u{0}`) and
  `prefix_upper_bound()` (increment last code point with carry, `None` for empty/all-`char::MAX`),
  unit-tested against empty/maximal/multibyte inputs. `cairn-meta/CLAUDE.md:46-47`: *"their
  correctness is listing's correctness."*
- **O(1) roll-ups** replacing scans: `bucket_stats.objects` (v34, `schema.rs:832`),
  `bucket_stats.logical_bytes`, `user_stats`, `multipart_bucket_stats`,
  `multipart_principal_stats` (v26). `docs/metadata.md:98-105`: overview work scales with buckets,
  not objects.
- **Bounded keyset pagination** for import history (`(created_at DESC, id DESC)`), scheduler
  selection (`(state, created_at, id)`), retention (`(state, updated_at)`) — v24
  (`schema.rs:556`), `docs/metadata.md:91-96`.
- Aggregates (`COUNT/SUM/MAX`), tag summaries (`list_tag_summary`, `list_objects_by_tag` over
  `idx_object_tags_kv`), and share lookup by token hash.

**Rebuild required:** every one of these as explicit ordered index scans — key encoding that
preserves UTF-8 byte order including `\u{0}`, prefix bounds, descending version components, stable
tie-breakers — plus delimiter skipping, paired-marker pagination, and maintained counters. The
capability review already flags this gap: ordered range APIs give "the scan primitive" but "byte
ordering, prefix upper bounds, continuation seek, marker exclusion and delimiter skipping remain
application work" (`docs/storage-metadata-capabilities-2026-09.md:60`).

### 2.4 Constraint enforcement and the typed conflict taxonomy

14 `MetaError` variants, with `engine_err` (`model.rs`) mapping SQLite constraint violations to
`MetaError::Conflict`. Uniqueness is load-bearing: `users.access_key_id UNIQUE`,
`users.sigv4_access_key_id UNIQUE`, `object_versions(bucket,key,version_id) PK`,
`storage_cleanups.storage_path UNIQUE`, `multipart_part_reservations(upload_id, part_number) UNIQUE`,
`object_versions.id` PK (the immutable row id lifecycle depends on).

**Rebuild required:** unique-key enforcement, upsert semantics (`ON CONFLICT DO UPDATE`, 7 sites),
`CHECK`-as-enum validation, and the same error taxonomy surface (`DeleteNotApplied` vs
`DeleteMarker` vs `Deleted` distinctions at `docs/metadata.md:59` depend on knowing whether a
write actually changed a row).

### 2.5 Writer-authoritative conditional logic

All inside the savepoint, by design (`cairn-meta/CLAUDE.md:26-30`): *"Preconditions are evaluated
**here, inside the savepoint**, so check-and-upsert is atomic."*

| Function | Line | Role |
|---|---|---|
| `put_version` | `apply.rs:2039` | precondition → bucket quota → user quota → Object Lock protection → tag/lock resolution → `upsert_version` → tags → lock → outbox |
| `check_precondition` | `apply.rs:3021` | If-Match/If-None-Match |
| `enforce_bucket_quota` | `apply.rs:2097` | reads `bucket_stats.logical_bytes`, subtracts the row being replaced |
| `enforce_user_quota` | `apply.rs:2151` | same, per owner |
| `replacement_is_protected` | `apply.rs:1862` | COMPLIANCE/GOVERNANCE retention + legal hold |
| `multipart_initial_state` | `apply.rs:1942` | exact-token claim + pinned tags/lock/SSE intent |
| `retire_multipart_session` | `apply.rs:2915` | terminal accounting |
| `upsert_version` / `upsert_version_inner` | `apply.rs:2228`, `:2233` | visibility delta + replica-ordering `MAX(version_id)` rule |
| `update_visibility` | `apply.rs:2210` | `bucket_stats.objects` delta |
| `adjust_stats` | `apply.rs:2323` | byte/version counter deltas |

### 2.6 The protocol-2 storage journal (`storage.rs`, 967 LOC)

Admission → publication → cancellation → quiescence resolution → leased cleanup claim → settlement
(`docs/metadata.md:122-175`, `docs/storage-durability.md:8.2`). The settlement predicate is
9-way (`storage.rs:157`, SQL at `:162`):

```sql
DELETE FROM storage_cleanups
 WHERE id=?1 AND bucket_name=?2 AND storage_path=?3 AND claim_token=?4
   AND claim_generation=?5 AND lease_until=?6 AND lease_until>=?7 AND quota_debt_id IS ?8
```

…and a successful delete additionally retires the linked quota debt (`storage.rs:176`,
`retire_quota` at `:607`). `consume_published` (`storage.rs:274`) atomically consumes the intent and
records cleanup for unreferenced planned paths.

**Rebuild required:** exact-match conditional delete with token + generation + lease + optional
link identity; a normalized reverse index (`storage_intent_paths` unique path → attempt/role) that
must agree with each stored plan JSON (`docs/metadata.md:33-34`).

### 2.7 The migration framework

40 append-only migrations, a `schema_migrations` ledger, and a **fail-closed compatibility
preflight** that runs *before* PRAGMAs, migrations, sanitation or Writer startup
(`docs/metadata.md:15-23`): reject a schema or storage protocol newer than the binary;
`storage_protocol` floors reader/writer = 2 from v36; `write_layout='flat'`;
`recovery_mode='full-scan'`. `latest_schema_version()` (`lib.rs:38`) is what restore uses to refuse
a newer snapshot. Sharded startup validates **every** database file before migrating any shard.

**Rebuild required:** a schema/format version ledger with the same monotonic discipline, the same
"refuse newer" gate, and — because a KV/log engine has no `ALTER TABLE` — an explicit *logical*
migration protocol: fresh destination, bounded ordered extraction, schema/codec identification,
full row and index verification, preserved sealed secrets and version identities, atomic
completion marker, unmodified verified rollback source
(`docs/storage-metadata-capabilities-2026-09.md:95-100`).

### 2.8 Backup, restore and the operator runbook

`VACUUM INTO` at `crates/cairn-meta/src/store.rs:146`; the rest lives in the binary
(`crates/cairn-server/src/main.rs`):

| Function | Line | What it relies on |
|---|---|---|
| `require_canonical_backup_topology` | 1024 | refuses libSQL/Turso/sharded — `docs/backup-restore.md:3-6` |
| `snapshot_sqlite_database` | 1085 | truncating checkpoint then `VACUUM INTO` |
| `validate_snapshot_database` | 1139 | `PRAGMA integrity_check` (`:1155`), `PRAGMA foreign_key_check` (`:1176`) |
| `present_sqlite_sidecars` | 1277 | rejects `-wal`/`-shm`/`-journal` rather than ignoring them |
| `file_fingerprint` | 1303 | 64-hex SHA-256 of the image |
| `build_snapshot_manifest` / `write_snapshot_manifest_last` | 1351 / 1379 | manifest written **last** as the completion marker |
| `verify_snapshot_blob_references` | 1192 | every `object_versions.storage_path` and `multipart_parts.storage_path` must exist |

Restore additionally: prepares a fresh-generation staged image (`Mutation::PrepareStorageRestore`,
`storage.rs:83`), re-validates size + SHA-256, applies key-ring identity/retirement checks,
checkpoints and closes every staged connection, removes exact sidecars, syncs, then publishes by
**atomic rename** — a crash before the rename reopens the old generation, after it the new one
(`docs/backup-restore.md:85-104`).

**Rebuild required:** a coherent point-in-time image of a custom engine (an LSM engine has no
`VACUUM INTO`; the capability review found "no public manifest-producing physical backup/checkpoint
API" for Fjall and "no directly equivalent public online backup/export API" for redb —
`docs/storage-metadata-capabilities-2026-09.md:91-92`), an integrity checker equivalent to
`integrity_check` + `foreign_key_check`, sidecar semantics, and the fresh-generation restore path.
Without this the only supported DR story (`docs/disaster-recovery.md:19-24`) stops existing.

### 2.9 The sharding router (`shard.rs`, 1,676 LOC)

FNV-1a bucket hash (deliberately not `DefaultHasher`, so the mapping is stable across releases —
`cairn-meta/CLAUDE.md:89-91`), an exhaustive `submit` routing match, an exhaustive `mutation_bucket`
classification for all 78 variants (both compile-forced, `docs/metadata.md` 4-site rule +
shard site in `CLAUDE.md`), global-shard rules for account-wide tables, cross-shard fan-out/merge
for reads, per-shard generation broadcast, and a rule that no release change may re-shard
(`docs/scaling-limits.md:83`).

### 2.10 The rest of the wiring

- **Cache decorator** (`cache.rs`, 1,592 LOC): 3 memoised reads, 16 shards, byte budget,
  generation re-check *inside* the shard lock to close the read-install TOCTOU, and
  `invalidate_for(&Mutation)` which pattern-matches every mutation shape (`cache.rs:407`, dispatched from `submit` at `:549`).
  This layer survives an engine swap **only if** `Mutation` stays a typed enum.
- **Async parity backend** (`cairn-meta-async`, 10,687 LOC, 265 distinct SQL, 130 tests): the
  4(+1)-site rule (`crates/cairn-meta-async/CLAUDE.md:39`, `:43`) means every change lands twice today.
  A custom engine either deletes this crate or rebuilds parity across libSQL and Turso.
- **In-memory double** (`testing/meta.rs`, 3,973 LOC) — the engine-neutral object-lifecycle spec,
  though it does not implement the protocol-2 journal contracts. Along with the three shared
  contract modules, this is the one genuinely reusable asset.
- **Server wiring**: `stack.rs` backend selection, `background.rs` checkpoint loop + metrics
  (`:1444-1510`), `key_rewrap.rs` (1,298 LOC of CAS through the writer `Exec` seam),
  `node_lock.rs` (317 LOC), and the `Migrate` / `Integrity` / `Backup` / `Restore` /
  `StorageBaseline` CLI subcommands (`main.rs:75-110`).
- **Observability**: 6 fixed writer stage rings (`admission`, `queue`, `begin`, `apply`, `commit`,
  `checkpoint`; `MAX_STAGE_SAMPLES=1024`), 8,192 commit samples, and the metrics
  `cairn_writer_queue_depth`, `cairn_writer_commit_seconds`, `cairn_writer_batch_size`,
  `cairn_wal_checkpoints_total`, `cairn_wal_checkpoints_busy_total` (`writer.rs:21-60`,
  `background.rs:1504-1510`).
- **Config semantics**: of the 9 `CAIRN_META_*` knobs, at least 6 are SQLite-shaped
  (`SYNCHRONOUS`, `MMAP_BYTES`, `CACHE_BYTES`, `CACHE_BYTES_PER_CONN`, `GROUP_COMMIT_LINGER_MICROS`,
  `SHARDS`) and would need redefining or retiring.

---

## 3. FACT — invariants that depend on a single ACID transaction

These are the correctness properties that a non-transactional engine would have to re-earn. Each
row cites both the invariant and the code that currently guarantees it.

| # | Invariant | Evidence | Failure mode if atomicity is lost |
|---|---|---|---|
| 1 | **Conditional PUT**: precondition check and upsert are inseparable | `apply.rs:2039` → `check_precondition` `:3021`; `docs/metadata.md:113-115` | Two `If-None-Match: *` writers both commit; lost update. `docs/metadata.md:114`: check-and-upsert "inseparable" |
| 2 | **Quota + row**: bucket and user byte quota read, projected, and row insert in one unit | `apply.rs:2097`, `:2151`; counter deltas `:2323` | Quota overshoot; `bucket_stats.logical_bytes` drifts permanently (no reconciliation exists for counters) |
| 3 | **Version demotion + insert**: predecessor `is_latest=0` and new row `is_latest=1` atomic | `upsert_version` `apply.rs:2228`, `update_visibility` `:2210` | Two "latest" rows → GET returns an arbitrary version; version listing duplicates |
| 4 | **Publication + intent consumption + cleanup debt** | `Mutation::PublishStorageWrite` → `storage.rs:274 consume_published`; `docs/metadata.md:129-142` | Either an orphan blob (leak) or a path both referenced and queued for deletion (data loss) |
| 5 | **Bucket delete**: re-check emptiness inside the savepoint, then 5 explicit `DELETE`s plus storage-intent cancellation | `apply.rs:623-673`; comment at `:625-630` names the exact hazard: *"stranded object_versions/multipart rows, corrupted counters, a leaked blob, and — since rows are keyed by bucket name — cross-tenant exposure to a recreated same-name bucket"* | Cross-tenant data exposure |
| 6 | **Multipart completion**: claim token + Object Lock + object upsert + session retirement + staging accounting in one savepoint | `apply.rs:525-580`; `multipart_initial_state` `:1942`; `retire_multipart_session` `:2915`; `docs/metadata.md:153-157` | Double completion, locked object committed unlocked, staging bytes leaked or forgiven while files remain |
| 7 | **Claim fencing** (replication, webhook batch, storage cleanup) | `MarkReplicationDone` `apply.rs:1312` → `owns_replication_claim`; `storage.rs:157`; `docs/metadata.md:246-249` | Two workers ship the same version, or two reclaim the same path |
| 8 | **Lifecycle delete guards** (immutable row id, `updated_at`, sole-marker) | `DeleteVersion` `apply.rs:280`; `docs/metadata.md:59`, `:118-120` | Deleting a version the operator never listed; `DeleteMarker`/`Deleted` outcomes become untruthful, breaking maintenance counters |
| 9 | **Generation + baseline HOLD** | `storage.rs:67` (begin), `:83` (prepare restore), `baseline.rs:87` (`held`), `:96` (`ensure_unheld`) | Physical write admission or quota release during an unfinished baseline |
| 10 | **Best-effort audit inside publication** | `PublishStorageWrite.activity` (`meta.rs:130-137`); duplicate-id ignored, other error fails the publication (`docs/metadata.md:135-139`) | Acknowledging an object from a transaction whose state is untrustworthy |

Note the shape of these: they are all *read-then-write* predicates evaluated against committed
state. An engine with only single-key atomic writes cannot express any of them.

---

## 4. FACT — what backup/restore and crash recovery rely on today

**Split the concern.** Cairn's *blob* crash story is already engine-neutral: stage → fsync file →
rename → fsync dir → validate hashes → commit metadata → reclaim
(`CONTRACT.md:34-36`, `docs/storage-durability.md:8.2`), plus a mandatory full filesystem scan at
startup with intent resolution and cleanup drain (`docs/storage-durability.md:8.5`).
`docs/metadata.md:42`: *"The journal does not authorize a shorter startup scan."* Journal-only
startup was measured and **not** qualified (`docs/backup-restore.md:198-199`).

**What is SQLite-dependent** is narrower but non-negotiable:

1. **Row-level atomicity and durability** — a committed metadata transaction survives power loss;
   a partial transaction is never observable. `docs/storage-durability.md:8.4` ties this to
   `PRAGMA synchronous=FULL` by default, with `NORMAL` as a documented "lose the last few commits,
   never corrupt" relaxation.
2. **A coherent image** — `VACUUM INTO` is the only supported snapshot source
   (`store.rs:146`, `docs/backup-restore.md:27`). The live file is never raw-copied.
3. **A verifier** — `PRAGMA integrity_check` + `PRAGMA foreign_key_check` on the image
   (`main.rs:1155`, `:1176`), then a SHA-256 fingerprint and a manifest written last.
4. **Sidecar discipline** — WAL/SHM/journal presence is a hard refusal, not a warning
   (`main.rs:1277`, `docs/backup-restore.md:73-76`).
5. **Downgrade safety** — schema/protocol floors exist specifically because "released binaries
   without the check remain unsafe downgrade targets" (`docs/metadata.md:22-23`). Combined with
   `CONTRACT.md:31-32` (no down-migrations), a new engine must either gain an equivalent floor or
   accept that rollback means restore-from-snapshot.
6. **Incident response** — `docs/operations.md:671` instructs operators to *"Inspect a consistent
   metadata backup using a read-only SQLite connection"*. `docs/disaster-recovery.md:19` lists
   "the **metadata database** (SQLite)" as a first-class DR artifact. Both would need rewriting.

**The load-bearing conclusion:** the engine is trusted for *transaction semantics and image
coherence*, not for blob recovery. Replace the engine and items 1–6 must be re-proved from scratch;
the full-scan recovery design carries over unchanged.

---

## 5. FACT — existing custom-engine labs and their recorded outcomes

`conformance/storage_lab` is a standalone workspace (16,867 LOC) that already contains a
non-SQLite metadata adapter. Its recorded results are the closest thing to prior art in the repo.

| Phase | Candidate | Scope built | Recorded outcome |
|---|---|---|---|
| **5A** capacity | production SQLite | 100k authoritative versions (80k current / 10k historical / 10k markers), C128, `synchronous=FULL`, 8 WAL readers | One hot bucket **qualifies** as a Writer service limit: 95.22% serialized occupancy, 81.87% nonempty queue, +10.26% C128 over C32. Distributed 16-bucket population **misses** its 80% queue threshold at 79.77%. `docs/storage-evolution-plan.md:467-471`, `docs/storage-metadata-capacity-2026-09.md:12-16` |
| **5B** conditional comparison | **Fjall 3.1.10**, `SingleWriterTxDatabase`, `PersistMode::SyncAll` on every batch commit | ~**3,761 LOC** adapter (`metadata_fjall/`: actor, apply, facade, journal, kv, model, multipart, outbox, reads) + 98 laboratory Rust tests; order-preserving escaped tuple keys, descending version components, transactional indexes for current/version, prefix, part/session, due-outbox, path-owner, cleanup-ready and quota-link, 16 MiB/16,384-edit overlay bounds, 8,192-byte key cap, 32 KiB value cap | **INCONCLUSIVE / KEEP SQLite.** SQLite seed prep finished in 95.34 s of a 100 s deadline; Fjall's prep hit its fixed 105 s deadline (`metadata workload deadline reached before admission`). **0 of 10 load arms, 0 of 5 pairs** completed. `docs/storage-metadata-alternative-2026-09.md:139-158`, `:147-150` |
| **4C** packing (adjacent) | packed blob files | 1,276 LOC builder + measurement | **INCONCLUSIVE / KEEP files.** 11 of 90 arms completed before the first 1 MiB packed arm's fixed 120 s deadline. `docs/storage-evolution-plan.md:450-456` |

**Critical scope caveat, stated by the lab itself** (`docs/storage-metadata-alternative-2026-09.md:64-67`):
the Fjall adapter is *"a fixed-trace adapter, not a general untrusted-input metadata service. Other
trait methods and unsupported mutation shapes return errors. Object Lock, tags, authorization,
completion, remote-upload state, imports and other unimplemented surfaces do not acquire parity
from these results."*

And the decision gate (`:128-130`): *"Production adoption would additionally require complete
semantic, migration, backup/restore, compatibility and operational parity and a human architectural
decision. This experiment cannot change the production metadata ceiling, even if the bounded
candidate qualifies."*

**Interpretation (FACT-backed):** the one measured signal (5A, 95.22% writer occupancy) is a
*service* limit — *"these are observed Writer service limits, including fsync and scheduling, not
isolated SQLite instruction costs"* (`docs/storage-metadata-alternative-2026-09.md:15-16`, echoed at
`docs/storage-evolution-plan.md:474-477`). No repo evidence shows SQLite *computation* is the
bottleneck, so engine replacement is not currently motivated by a measured finding.

---

## 6. ESTIMATE — engineering surface

All figures below are **ESTIMATE**, derived from the FACTs in §1 and the one measured data point
(the 5B adapter). They are reasoning, not measurement.

### 6.1 Lines of code

| Component | Basis | Estimate |
|---|---|---|
| Engine core (storage format, WAL/journal, snapshot readers, compaction, crash recovery, integrity check, backup image) | No basis in-repo — Cairn has never written one. Fjall/redb are external. | **8k–15k LOC** |
| Ordered-index layer + key codec (byte-order tuples, descending versions, prefix bounds, delimiter, paired markers) | `range.rs` is 89 LOC today because SQLite supplies everything else; the 5B adapter spent ~3.8k LOC on a *bounded* subset | **3k–5k LOC** |
| `Mutation` → engine translation (78 variants, 234 distinct write statements) | Same order as `apply.rs` (4,172 LOC) if faithfully re-expressed | **4k–6k LOC** |
| Read surface (54 methods, 79 distinct statements) | `store.rs` is 2,419 LOC | **2k–3k LOC** |
| Storage journal + baseline | `storage.rs` 967 + `baseline.rs` 864 | **1.5k–2k LOC** |
| Migration framework + 40 migrations re-expressed | `schema.rs` 2,253 LOC (sync) + 1,700 (async) | **2k–3k LOC** |
| Backup/verify/restore tooling | `main.rs` snapshot region 1014–1500 ≈ 500 LOC, all SQLite-specific | **1k–2k LOC** |
| Shard router adaptation | `shard.rs` 1,676 LOC, largely reusable if `Mutation` is stable | **0.5k–1k LOC** |
| **Total new/changed engine code** | | **≈22k–37k LOC** |

Plus: **~10.7k LOC** of `cairn-meta-async` deleted or rebuilt, and **~6.9k LOC** of `cairn-meta`
tests plus the 2.9k LOC of shared contract suites re-baselined. Call it **~30k–50k LOC touched**.

### 6.2 Tests

- **FACT:** 170 (`cairn-meta`) + 130 (`cairn-meta-async`) + 72 (`cairn-types`) = **372 metadata tests**
  today. The shared storage/baseline/remote-upload contracts run on **4 implementations**
  (sqlite, sharded sqlite, libSQL, Turso); the in-memory double has its own 17-test behavioural
  suite (`crates/cairn-types/tests/doubles_contract.rs`) plus 7 object-lock contract tests, and does
  **not** implement the protocol-2 journal contracts.
- **ESTIMATE:** the contract suites themselves need only a new implementation to run — maybe
  10–15% rework. The rest are behaviour tests on SQL-specific outcomes; expect **50–70% to need
  re-derivation**, i.e. **~190–260 tests rewritten**, plus new engine-level tests (crash, torn
  write, compaction, format upgrade) that do not exist at all today.
- **ESTIMATE:** 200–400 new engine tests, because none of SQLite's own crash/format guarantees are
  tested by Cairn today — they are inherited.

### 6.3 Effort

- **ESTIMATE:** 6–12 engineer-months for a competent team to reach *feature parity behind the
  trait*, assuming the in-memory double is used as the executable spec. This is not a linear
  function of LOC: the hard parts are the conditional-write invariants (§3, 10 of them), the
  paired-marker pagination, and backup/restore.
- **ESTIMATE:** +3–6 engineer-months for the things that are *not code*: migration protocol, a
  restartable engine-to-engine cutover, a restore drill matrix, and operator documentation that
  currently assumes `sqlite3` exists.
- **FACT + ESTIMATE:** the 5B comparison charged 204.772213 s against a 600 s metadata allowance
  (cumulative 496.775609 / 600 s) and produced **zero** comparative load data. A serious parity+performance campaign is a multi-week,
  separately-budgeted effort, not an extension of that allowance.

### 6.4 Risk ranking (ESTIMATE)

| Rank | Risk | Why it ranks here |
|---:|---|---|
| 1 | **Listing/pagination correctness** | Highest-frequency read path, byte-order and delimiter subtleties, and a documented historical bug (`cairn-meta/CLAUDE.md:47-51`). Bugs are silent (missing or duplicated keys), not crashes. |
| 2 | **Backup/restore parity** | Today's only supported DR path is single-SQLite-only and refuses other backends (`docs/backup-restore.md:3-6`). Replacing it means the DR story is unproven until a full drill matrix passes. |
| 3 | **Conditional-write atomicity** (§3 rows 1–3, 6) | Data-integrity bugs that surface as wrong bytes served to a client, not as errors. |
| 4 | **Storage journal / orphan vs double-free** (§3 row 4) | Either leaks disk forever or deletes a live object. |
| 5 | **Bucket-delete race** (§3 row 5) | Documented cross-tenant exposure path. |
| 6 | **Quota/roll-up counter drift** | Counters are maintained, never reconciled; a lost delta is permanent. |
| 7 | **Migration & downgrade** | No down-migration story exists (`CONTRACT.md:31-32`); rollback would mean restore-from-snapshot only. |
| 8 | **Sharding hash stability** | `CAIRN_META_SHARDS` cannot change post-init (`docs/scaling-limits.md:67-68`, `:83`). |
| 9 | **Observability regression** | Writer stage/commit histograms and checkpoint-busy metrics are the documented "am I at the ceiling?" signal (`docs/scaling-limits.md:23-25`). |
| 10 | **Build/linkage** | Bundled-C SQLite already passes the static musl gate; a Rust engine is "a plausible static candidate, not a tested Cairn build" (`docs/storage-metadata-capabilities-2026-09.md:108-110`). |

---

## 7. FACT — what would *not* need rebuilding (the reusable part)

This is the honest good news, and it is significant:

- **The trait is already the contract.** 54 methods in `MetadataStore` (`traits.rs:247`); protocol,
  control, lifecycle, replication and webhook code call `submit`/reads only — 221 `submit()`
  sites, none of which name SQLite. (`cairn-import` touches no metadata API directly; it is wired
  to the store through `cairn-server`.)
- **The in-memory double already encodes the object-lifecycle semantics.** 3,973 LOC
  (`testing/meta.rs`) with its own behavioural suite covering put/get/list/delete round-trips,
  atomic `If-None-Match`, version history and promotion, delete guards and row-identity guards
  (`crates/cairn-types/tests/doubles_contract.rs`, 17 tests) plus 7 object-lock contract tests.
  It is **not** a full oracle: the shared protocol-2 journal/baseline contracts run only on
  sqlite, sharded sqlite, libSQL and Turso. `docs/metadata.md:263` calls the boundary *"the single
  most important structural decision"* and says a future pure-Rust metadata engine would be
  *"drop-in rather than a rewrite."*
- **Blob durability is engine-neutral.** `docs/storage-durability.md:8.2` ordering is in the blob
  layer; only the metadata commit point moves.
- **The 4(+1)-site rule becomes a 2-site rule** (`cairn-meta` + the double) if `cairn-meta-async` is
  deleted — a net reduction in ongoing cost, at the price of losing the libSQL/Turso parity check
  that today catches divergence between two independent SQL engines.
- **The cache decorator is engine-agnostic** (`cache.rs:518`), because invalidation keys off the
  `Mutation` enum, not off SQL.

---

## 8. FACT — verbatim positions already on record in the repo

- `docs/metadata.md:9`: the engine *"is already swappable, with `sqlite` … `libsql` … and `turso`
  all backing the same `MetadataStore` interface … without any change to protocol or control code."*
- `docs/metadata.md:263`: *"a future pure-Rust metadata engine are drop-in rather than rewrites."*
- `docs/delivery.md:230`: the abstraction boundary exists to enable *"a future metadata engine."*
- `docs/storage-metadata-alternative-2026-09.md:128-130`: adoption *"would additionally require
  complete semantic, migration, backup/restore, compatibility and operational parity and a human
  architectural decision."*
- `docs/storage-metadata-capabilities-2026-09.md:160-163`: *"No verified Writer bottleneck, an
  incomplete alternative adapter, or a nonqualifying gain means **KEEP SQLite** for this phase."*
- `CONTRACT.md:22-25`: the single-SQLite, single-writer rule is a human-only ceiling.

**Net factual answer to "exactly what would have to be rebuilt":** §2's ten subsystems —
transactional core, read pool, query layer, constraint/conflict taxonomy, conditional-write logic,
storage journal, migration framework, backup/restore tooling, shard router, and the surrounding
wiring (cache invalidation contract, async parity backend or its deletion, server CLI/stack/rewrap,
observability, and 6 of 9 `CAIRN_META_*` knobs). **What could break:** the ten invariants in §3,
the only supported DR path, the downgrade story, and the documented operator incident-response
workflow. The measured record contains **no evidence** that SQLite is the throughput bottleneck,
and the one attempted alternative produced **zero** comparative load data.
