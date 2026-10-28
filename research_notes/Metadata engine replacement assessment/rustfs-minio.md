# RustFS 1.0.4 vs MinIO vs Cairn: what the single-node metadata engine actually is

Read-only research note. Sources are pinned checkouts:
RustFS `1.0.4` (commit `46337942`) at `/tmp/opencode/cairn-rustfs-1.0.4`; MinIO `master` clone at
`/tmp/opencode/minio-src` (fetched 2026-10-28, so line numbers are "current master", not a tag);
Cairn at `/home/dev/Cairn`. All `path:line` citations are to those trees.

Scope: single node only (one host, local drives). Multi-node/distributed behaviour is cited only
where it explains a single-node design constraint.

---

## 0. One-paragraph answer

Neither RustFS nor MinIO has a metadata *engine*. Each object's entire version set, tags, Object
Lock state, storage layout and ETags live inside one per-object `xl.meta` file in the same POSIX
directory tree as the object's data shards; configuration (bucket policy/ACL/quota/versioning, IAM)
is a small number of ordinary objects under a reserved bucket (`.rustfs.sys` / `.minio.sys`).
There is no catalog, no schema, no transaction log, no secondary index. Listing is a directory
walk with per-entry quorum metadata reads; conditional writes are a namespace-lock + read-then-write;
quotas, usage and lifecycle are scanner/ledger subsystems layered on top of the object API. Cairn
replaces that whole layer with one SQLite database (32 tables, one group-committing writer, one
linearization point per PUT). The trade is: Cairn buys exact conditional semantics, O(log n) listing,
atomic multi-row metadata edits and a one-file backup, and pays two durable metadata commits per PUT,
a single-writer throughput ceiling, and a database that is itself the recovery unit.

---

## 1. RustFS object metadata representation

Layout, per drive:

```
<drive>/<bucket>/<object-key-nested-dirs>/xl.meta          # metadata, one per object
<drive>/<bucket>/<object-key-nested-dirs>/<data_dir>/part.1 # shard file (non-inline objects)
<drive>/.rustfs.sys/tmp/<uuid>/<data_dir>/part.1           # staging before commit
```

- `STORAGE_FORMAT_FILE = "xl.meta"`, `STORAGE_FORMAT_FILE_BACKUP = "xl.meta.bkp"`
  (`crates/ecstore/src/disk/mod.rs:45-46`), reserved bucket `.rustfs.sys`
  (`crates/ecstore/src/disk/mod.rs:35-39`).
- Object paths are the key split into directories; keys ending in `/` are encoded with a
  `__XLDIR__` suffix (`crates/utils/src/path.rs:48-54`, `is_dir_object` at `:64-67`), and path
  construction goes through `io_get_object_path` (`crates/ecstore/src/disk/local.rs:6298-6300`).
- Format: 4-byte header `XL2 `, file version 1.3, `XL_META_VERSION = 3`
  (`crates/filemeta/src/filemeta.rs:49-61`). Struct:
  `FileMeta { versions: Vec<FileMetaShallowVersion>, data: InlineData, meta_ver: u8 }`
  (`crates/filemeta/src/filemeta.rs:297-300`).
- **The version set is one file.** Each version is a length-delimited msgpack blob plus a fixed
  header (`FileMetaShallowVersion { header, meta: Vec<u8> }`,
  `crates/filemeta/src/filemeta/version.rs:554-557`), parsed on demand
  (`parse_version_meta`, `:566-570`). `add_version` appends and re-sorts by mod time
  (`crates/filemeta/src/filemeta.rs:479`, `sort_by_mod_time` at `:342-346`), with a hard cap that
  returns `MaxVersionsExceeded` (`:648`).
- Per-version payload: `MetaObject { version_id, data_dir, erasure geometry, part numbers/etags/
  sizes/indices, size, mod_time, meta_sys: HashMap<String,Vec<u8>>, meta_user: HashMap<String,String> }`
  (`crates/filemeta/src/filemeta/version.rs:1437-1478`). Delete markers are a sibling variant
  `MetaDeleteMarker` in the same journal (`:617-630`).
- **Tags, Object Lock, storage class, SSE labels live in `meta_user`**, and those key spellings are
  declared part of the on-disk format ("existing xl.meta cannot be rewritten in bulk";
  `crates/filemeta/src/metadata_keys.rs:15-42`, pinned table at `:74-84`). Object Lock retention is
  three keys: `x-amz-object-lock-legal-hold`, `-mode`, `-retain-until-date` (`:28-32`).
- **Small objects are inlined into `xl.meta` itself**, in `FileMeta.data`, keyed by version id
  (`InlineData`, `crates/filemeta/src/filemeta.rs:104-110`; lookup at `:349-360`). The budget is
  per-shard, scaled down as data shards grow: `DEFAULT_INLINE_BLOCK = 128 KiB` and
  `DEFAULT_INLINE_OBJECT_BUDGET = 2 * DEFAULT_INLINE_BLOCK = 256 KiB` per *object*, so the per-shard
  bound is `256 KiB / data_shards` capped at 128 KiB (`crates/ecstore/src/config/storageclass.rs:103-104`,
  `effective_inline_block` at `:265-277`), versioned buckets get 1/8 of it (`should_inline`, `:243-256`).
  Consequence: on EC 8+4 unversioned, shard ≤ 32 KiB inlines, so **1 MiB is never inlined**; on
  EC 2+1, ≤128 KiB per shard inlines.
- **There is no global catalog and no catalog file.** The only "system" state is ordinary objects in
  `.rustfs.sys`: bucket metadata is one document per bucket at `buckets/<bucket>/bucket-metadata`
  (`crates/ecstore/src/store/bucket.rs:4203`; struct with policy/ACL/quota/versioning/Object Lock/
  CORS/lifecycle/… fields at `crates/ecstore/src/bucket/metadata.rs:435-462`), IAM is config objects
  under the IAM prefix (`crates/iam/src/manager.rs:85`), and every config write is literally a
  normal `put_object` into `.rustfs.sys` (`crates/ecstore/src/config/com.rs:809-810`).

So: bucket policy, bucket ACL (`bucket_acl_config_json`, `crates/ecstore/src/bucket/metadata.rs:459`),
versioning, Object Lock config and quota are **one whole-document object per bucket**, rewritten in
full under a namespace lock; per-object tags and Object Lock state are `meta_user` keys inside that
object's own `xl.meta` (`MetaObject` has no per-object ACL field —
`crates/filemeta/src/filemeta/version.rs:1437-1478`). Nothing indexes any of them.

---

## 2. A 1 MiB strict PUT in RustFS: files, renames, fsyncs, overlap

Entry point `SetDisks::put_object_with_old_current_size_inner`
(`crates/ecstore/src/set_disk/ops/object.rs:4093` onwards). For a 1 MiB object on EC 8+4 (shard
128 KiB > 32 KiB inline budget) `is_inline_buffer` is false (`object.rs:4290-4300`), so the real
shard path runs.

**Phase A — stage into the reserved tmp bucket.** `tmp_dir`/`tmp_object` are
`<uuid>/<data_dir>/part.1` under `.rustfs.sys/tmp` (`object.rs:4264-4266`), written with
`create_bitrot_writer(..., RUSTFS_META_TMP_BUCKET, &tmp_obj, ...)` per online disk
(`object.rs:4340-4390`). Data therefore lands at
`<drive>/.rustfs.sys/tmp/<uuid>/<data_dir>/part.1` with a bitrot checksum trailer. The object's own
metadata is not written yet and no namespace lock is held (deliberate: the pre-body check is
"advisory fast-fail only" because holding the lock across client-paced ingestion starved readers —
`object.rs:4172-4183`), though a bucket quota reservation may already have written ledger objects
before staging (`object.rs:5040`).

**Phase B — commit under the object namespace lock** (`acquire_write_lock_diag("put_object_commit")`,
`object.rs:4706-4720`), for each disk, in a single per-disk `rename_data_inner`
(`crates/ecstore/src/disk/local/commit.rs:316`):

1. Read the destination `xl.meta` if present, parse it, and `xlmeta.add_version(fi)` — i.e. **read-
   modify-write of the whole version set** (`commit.rs:520-560`).
2. Marshal the new `xl.meta` and write it to the *staging* path
   `.rustfs.sys/tmp/<uuid>/xl.meta` with `SyncMode::FileOnly` (contents durable, no dir fsync) when
   the durability tier syncs commit metadata (`commit.rs:568-600`).
3. Concurrently `fdatasync` the staged shard files (`os::sync_dir_files_with_limiter(src_data_path)`)
   — the two are **independent and overlapped**:
   `let (tmp_meta_res, shard_sync_res) = tokio::join!(tmp_meta_write, shard_sync);`
   (`commit.rs:606-621`; comment at `:585-600` explains the ordering invariant is unchanged because
   both complete before any rename).
4. `rename` the whole data directory into place: `<obj>/<data_dir>/` (`commit.rs:660-690`,
   `os::rename_all_with_commit_guard`).
5. Write the rollback copy of the old `xl.meta` to `<old_data_dir>/xl.meta.bkp` with
   `SyncMode::FileAndDir` under strict (`commit.rs:717-760`).
6. `rename` the new `xl.meta` over the destination (`commit.rs:792`,
   `os::rename_all_with_prepared_source`) — this is the per-disk commit point.
7. `fsync` the destination object directory, which is what makes both renames durable
   (`commit.rs:836-846`, `os::fsync_commit_directory`) — **only when
   `durability.syncs_commit_metadata()`**, i.e. strict.

The tier is resolved once per commit (`effective_durability`, `crates/ecstore/src/disk/local.rs:1757-1766`);
`DurabilityMode` is `Strict | Relaxed | None | LegacyOff` (`local.rs:1481-1526`) with
`syncs_data_shards = Strict|Relaxed` and `syncs_commit_metadata = Strict` (`:1518-1528`), default
strict (`resolve_durability_mode`, `:1533-1560`). The documented table of what each tier fsyncs is
`docs/operations/durability-modes.md` §"What each mode fsyncs", and it says relaxed is "Aligned with
MinIO's default durability posture" (`crates/ecstore/src/disk/local.rs:1485-1490`).

**Fan-out and ack.** Disks are committed in parallel in one coordinator task: the per-disk futures
are built at `crates/ecstore/src/set_disk/core/io_primitives.rs:4204-4260` and awaited with
`let results = join_all(futures).await;` (`:4346`), then quorum-reduced
(`reduce_write_quorum_errs`, `:4447`). When early-ack is enabled (default `true`,
`io_primitives.rs:3335-3337`) the client is acknowledged once write quorum has committed while the
unresolved tail drains detached (`rename_data_owned_early_ack_with_fence`, `:3762`).

Net for one 1 MiB strict PUT on EC 8+4, per disk in the quorum: 1 staged shard file (fdatasync),
1 staged `xl.meta` write (+fsync of contents), 1 directory rename (data dir), 1 rollback
`xl.meta.bkp` write (+file+dir fsync) when replacing an existing version, 1 file rename (`xl.meta`),
1 parent-dir fsync. There is **no separate metadata service hop** — object bytes and object metadata
are the same commit, on the same disks, behind the same quorum.

---

## 3. LIST and version listing without a global database

- `ECStore::list_path` (`crates/ecstore/src/store/list_objects.rs:4371`) normalizes the request and
  fans out one walk per erasure set (`list_merged`, `:4490-4510`), then k-way merges the streams
  (`spawn_listing_merge`) and applies quorum/`ask_disks` policy.
- The disk side is a **filesystem walk**: `LocalDisk::walk_dir`
  (`crates/ecstore/src/disk/local.rs:11475`) → `scan_dir` (`:8708`), which calls
  `read_dir_entries_with_walk_stall(&dir_path_abs, -1, stall)` — `count = -1` means **read the whole
  directory into memory** (`local.rs:8762`), filters, then `entries.sort()` (`:8889`), and for every
  `xl.meta` entry does `self.read_metadata(...)` and streams the bytes into the metacache wire format
  (`:8827-8845`).
- Consequence: for a flat bucket with N keys in one directory, **each list page re-reads and sorts
  the whole parent directory** and reads one `xl.meta` per returned entry. Page cost is
  O(N_dir + page·xl.meta), not O(page). The test `scan_dir_records_whole_parent_read_dir_before_page_limit`
  (`local.rs:21212`) pins this "whole-parent read before page limit" behaviour.
- Each entry is then resolved across disks: `get_list_quorum` default is `optimal` =
  `(drive_count+1)/2` drives (`list_objects.rs:7170-7177`, `list_objects_quorum_from_env` `:2094-2097`),
  and the merge compares per-disk metadata copies before yielding an entry
  (`docs/operations/cluster-lifecycle-operations.md:104` describes the same `pick_valid_fileinfo`
  quorum requirement for the listing path). So LIST read amplification is roughly
  `ask_disks × (directory entries + xl.meta bytes)`.
- Continuation is a **position in the walk**, not an offset in an index: the marker is parsed into
  `id/pool/set/source/generation` and a `forward_to` key (`list_objects.rs:3476-3505`), so a resumed
  page skips forward inside the directory but still re-enumerates that directory.
- Version listing is cheap *per object* and expensive *per bucket*: every version of a key is already
  in that key's `xl.meta`, so `ListObjectVersions` needs no extra lookups, but it still walks every
  directory and reads every `xl.meta`.
- The code itself documents the scale limit: "Large buckets (millions of objects) can take longer
  than the default 5s walkdir timeout to produce the first page of results"
  (`list_objects.rs:4712-4718`). There is a stall-based liveness bound per walk
  (`RUSTFS_DRIVE_WALKDIR_*`, `docs/operations/drive-timeout-tuning.md:62`).
- No persisted listing index ships in this version: `MetacacheWriter/Reader` is a *stream* format
  (`crates/filemeta/src/metacache.rs:845-912`) and `cache_value/metacache_set.rs` merges disk
  streams; the `create`/`transient` fields that in MinIO mean "load/persist an on-disk cache"
  (`list_objects.rs:321-330`) are vestigial here (set to `false` at `:4421`, `:5844`, `:6789`).

---

## 4. Conditional writes, quotas, policies, locks, versioning, multi-key

**Conditional PUT.** Two evaluations of the same predicate: an advisory pre-body read
(`object.rs:4177-4183`) and the authoritative one *inside* the commit, after the namespace write
lock is held (`object.rs:4724-4728` → `check_write_precondition`,
`crates/ecstore/src/set_disk/core/io_primitives.rs:6401-6440`), which does a quorum `get_object_info`
with `no_lock = true` and compares ETag/`If-None-Match` (`should_prevent_write`, `:6427-6431`).
Atomicity therefore rests on the **namespace lock**, not on a compare-and-swap in a store:
single-node `LocalClient` is an in-memory map of guards
(`docs/architecture/unified-object-generation.md:22-24`). RustFS's own architecture doc is explicit
that this is not a durable distributed CAS and that its per-object transaction epoch is "an opaque
operation/idempotency identifier… not an ordered lock epoch", with counterexample schedules, and
that "an independent service holding every object's full manifest is not selected"
(`docs/architecture/unified-object-generation.md:8-14, 25-40`). Failure modes named there: lock
lease loss, coordinator restart, a rollback erasing a competing committed write, and timeouts that
are not evidence of abort. (Same doc: "**Implementation status:** this document does not implement or
claim distributed generation authority.")

**Object Lock / WORM.** Retention mode, retain-until and legal hold are the three pinned
`meta_user` keys (`crates/filemeta/src/metadata_keys.rs:28-32`); enforcement is a read of those keys
in the PUT/DELETE paths (`crates/ecstore/src/set_disk/ops/locking.rs`), not a database constraint.
A corrupt/renamed key makes a WORM object deletable — called out as the reason the spellings are
frozen (`metadata_keys.rs:18-24`).

**Versioning.** Per-object journal only. Enabling versioning is a bucket-metadata document write
(`bucket-metadata` object). There is no bucket-wide version index, so "list all versions in a bucket"
is a full walk; `DEFAULT_API_OBJECT_MAX_VERSIONS` is effectively unlimited
(`crates/config/src/constants/api.rs:105-109`), which means one object's `xl.meta` can grow without a
practical bucket-level bound (per-write cost is O(versions of that key): full parse + `add_version` +
sort at `filemeta.rs:479`, `:342-346`).

**Quotas.** Not a counter read at write time. `bucket/quota/reservation.rs` implements a
"reservation protocol v2": durable ledger/allocator/shard objects under `.rustfs.sys`, ordered lock
acquisition (operation → bucket metadata (read) → reservation → ledger, comment at
`crates/ecstore/src/bucket/quota/reservation.rs:38-46`), persisted `PersistedReservation` records
(`:64-74`), and usage derived by scanning (`compute_bucket_usage`, `:1342`). It is a transaction
protocol rebuilt out of objects + locks, with orphan probing/reaping (`ORPHAN_MIN_AGE_SECONDS`,
`:56-60`). Failure mode is documented: a zero-length/unreadable `xl.meta` is corruption, usage
snapshots stay incomplete, and "quota admission can fail closed until a complete snapshot is
available" (`docs/operations/durability-modes.md` §"Recovery from corrupt object metadata").

**Bucket policy / ACL.** Whole-document fields of the single bucket-metadata object
(`crates/ecstore/src/bucket/metadata.rs:442-462`), written by a normal PUT into `.rustfs.sys`
(`config/com.rs:809-810`) and cached in memory (`metadata_sys`); concurrent edits serialize on the
namespace lock of that one object, and a policy change is a full object re-PUT (with quorum, fsyncs,
and its own `xl.meta`).

**Multi-key atomicity.** None. `DeleteObjects` is a per-object loop with per-key results
(`delete_objects_with_tier_delete_journal_and_accounting`,
`crates/ecstore/src/store/object.rs:4874-4905`); deletes get their own per-key durable recovery
decision so a set-wide delete converges (`crates/ecstore/src/set_disk/ops/delete_recovery.rs:15-16`),
but there is no "N keys or none". Copy is read-then-write; there is no rename-object primitive that
updates two keys atomically.

**Crash recovery.** Startup deletes `.rustfs.sys/tmp` residue
(`crates/ecstore/src/disk/local.rs:5713-5726`); per-object recovery uses `xl.meta.bkp` plus
heal/scanner convergence; corruption of `xl.meta` is treated as data loss needing heal, not as a
rollback ("The version index cannot be reconstructed from shard data alone" —
`docs/operations/durability-modes.md`). There is no log to replay.

---

## 5. MinIO: same design, same give-ups

- Format family: `xlMetaV2 { versions []xlMetaV2ShallowVersion; data xlMetaInlineData; metaV uint8 }`
  (`cmd/xl-storage-format-v2.go:901-911`), header `XL2 `, version 1.3 (`:55-70`), `AddVersion`
  (`:1592`), `MetaSys`/`MetaUser` per version (same struct literal at `:1620-1645`).
- Commit order in `xlStorage.RenameData` (`cmd/xl-storage.go:2557`): read dst `xl.meta` (`:2633`),
  `AddVersion` (`:2811`), `AppendTo` (`:2821`), `WriteAll` the new `xl.meta` to the **staging** path
  (`:2830`), `renameAll(srcDataPath, dstDataPath)` (`:2860`), write `xl.meta.bkp` into the old data
  dir with `sync=true` (`:2882`), then `renameAll(srcFilePath, dstFilePath)` as the commit
  (`:2897`). Same two-rename, backup-then-commit shape as RustFS.
- Durability defaults differ in the details: `xl.meta` writes go through `openFileSync` with
  `writeMode = O_DSYNC` on Linux (`cmd/xl-storage_noatime_supported.go:33`,
  `cmd/xl-storage.go:1975`), part bytes are `Fdatasync`ed inside `writeAllDirect`
  (`cmd/xl-storage.go:2195`), but **directory entries are not fsynced** unless
  `MINIO_FS_OSYNC=on`, which makes `RenameData` call `globalSync()` = `syscall.Sync()`
  (`cmd/xl-storage.go:2576-2578`, `cmd/os_unix.go:355`, `cmd/common-main.go:745`, default off).
  So MinIO's ack can be lost on power failure even though the file contents were durable — the exact
  gap RustFS closes in `strict`.
- Inline policy is *more* aggressive: `ShouldInline(erasure.ShardFileSize(size), versioned)` with a
  128 KiB **per-shard** default and 16 KiB for versioned buckets
  (`cmd/erasure-object.go:1399`, `internal/config/storageclass/storage-class.go:284-293`), where
  `ShardFileSize = numBlocks*ceil(blockSize/dataBlocks) + ceil(lastBlock/dataBlocks)` and
  `blockSizeV2 = 1 MiB` (`cmd/erasure-metadata.go:54-68`, `cmd/object-api-common.go:37`). On EC 8+4 a
  1 MiB object is exactly one block, so the shard is exactly 128 KiB and **MinIO inlines the whole
  1 MiB into `xl.meta`** on every drive (no `part.1` at all). RustFS deliberately deviated: "Keep the
  historical two-data-shard object budget while preventing wider EC layouts from multiplying the
  maximum inline object size" (`crates/ecstore/src/config/storageclass.rs:270-277`). This is a real
  write-amplification trade in both directions: one 1 MiB O_DSYNC write vs 8×128 KiB shard writes
  plus a small `xl.meta`.
- LIST: `metacache` is persisted on disk — caches live at
  `.minio.sys/buckets/<bucket>/.metacache/<id>/block-N.s2` (`cmd/metacache-set.go:336-345`,
  `:342-345` `objectPath`), written by the walk and re-read to serve later pages
  (`cmd/metacache-set.go:375-395` `forwardTo(Prefix)`, `forwardTo(Marker)`). That is MinIO's answer
  to "no index": memoize the walk into a private append-only s2 stream. `ListObjectVersions` uses the
  same walk with `Versioned: true` (`cmd/erasure-server-pool.go:1396-1404`).
- Conditional PUT: `opts.CheckPrecondFn` (`cmd/object-handlers.go:2020-2034`) →
  `erasureObjects.putObject` takes the namespace lock, reads the object, evaluates the predicate, and
  **holds that lock for the rest of the upload, including client body ingestion**
  (`cmd/erasure-object.go:1261-1284`, `defer ns.Unlock(lkctx)` at `:1269`). That is the design RustFS
  explicitly rejected as a starvation source (`RustFS ops/object.rs:4172-4183`). Correct and simple
  on one node; a slow-client lock-hold hazard under concurrency.
- Quota: best-effort against the scanner's in-memory usage snapshot; if the snapshot is unavailable
  MinIO logs "no reliable usage value available - quota will not be enforced"
  (`cmd/bucket-quota.go:65-83`). Concurrent PUTs can overshoot; there is no reservation ledger.
- No multi-object transactions, no global index, no durable catalog: usage, lifecycle, replication
  backlog and policy views are scanner/`data-scanner` products over the same walk
  (`cmd/data-scanner.go:733`).

**Upstream statements about why files, not a database.** Neither project ships a design doc arguing
the choice; the closest primary evidence is the shape of the code plus RustFS's explicit note that a
central metadata service "is not selected" for object authority
(`docs/architecture/unified-object-generation.md:8-10`) and its `DurabilityMode::Relaxed` doc comment
that leaving metadata to the page cache is "Aligned with MinIO's default durability posture"
(`crates/ecstore/src/disk/local.rs:1485-1490`). **Labelled inference:** the file design buys
(a) per-object independence — one hot key never contends in a shared B-tree, and a lost drive loses
only its own objects; (b) trivially shardable metadata (the key's path *is* the shard); (c) format
compatibility with MinIO (`crates/filemeta/src/test_data.rs:134-175` decodes real MinIO-written
`xl.meta`); and (d) no separate backup/restore unit. It gives up everything in §6's right column.

---

## 6. Cairn's single-SQLite metadata

Engine: one embedded SQLite database in WAL mode, one write connection owned by the group-committing
writer and a read-only pool (`docs/metadata.md:9-13`; PRAGMAs at `crates/cairn-meta/src/lib.rs:132-146`;
`OpenOptions` `synchronous_full` at `:63-87`, server default `full`).

**Schema (32 tables, `crates/cairn-meta/src/schema.rs`).** Identity/config: `users`, `buckets`,
`bucket_config` (one row per `aspect`, `schema.rs:43-48`), `account_config`, `key_ring_state`,
`rewrap_progress`, `session_credentials`. Object plane: `object_versions`, `object_tags`,
`object_locks`, `multipart_uploads`, `multipart_parts`, `multipart_part_reservations`,
`multipart_staging_cleanups`, `multipart_bucket_stats`, `multipart_principal_stats`,
`replication_uploads`, `import_jobs`, `object_shares`, `share_capability_sanitation`. Bulk/observability:
`replication_outbox`, `events_outbox`, `activity`, `request_metrics`, `bucket_stats`, `user_stats`.
Storage protocol: `storage_protocol`, `storage_recovery_state`, `storage_write_intents`,
`storage_intent_paths`, `storage_cleanups`.

`object_versions` is the analogue of `xl.meta`'s version journal, but relational:
`UNIQUE (bucket_name, key, version_id)`, `is_latest`, `is_delete_marker`, sizes, ETag, storage path,
compression descriptor, checksums, `acl`, `replication_status` (`schema.rs:55-80`), with a half-open
range-seek index and, since v11, an **index-only covering index for current listings**:
`idx_ov_latest_cover (bucket_name, key, version_id, is_delete_marker, etag, size_logical, updated_at,
storage_class, owner_id) WHERE is_latest = 1` (`schema.rs:300-312`), which superseded
`idx_object_versions_latest`. Tags and Object Lock are side tables keyed by
`(bucket, key, version_id)` (`schema.rs:84-92`, `:403-420`) so they are replaceable in the same
savepoint as the version.

**Write model.** One writer thread, one transaction per batch, one savepoint per mutation, ack after
`COMMIT` (`crates/cairn-meta/src/writer.rs:1-5`, `:474-520`). Preconditions are evaluated *inside*
the transaction (`crates/cairn-meta/src/apply.rs:1-4`, `check_precondition` `:3022-3055`), quota is
checked in the same transaction (`apply.rs:2092-2100`), and bucket/user byte totals are maintained
roll-ups (`schema.rs:316-330`) so a quota check is O(1).

**PUT.** Under storage protocol 2 the sequence is: durable admission of the exact write plan *before
any file is created* → stream to staging (hashes inline) → `fsync` file → `rename` into the final
per-bucket dir → `fsync` directory → validate hashes → **metadata transaction commit (the single
linearization point)** → ack → exact cleanup debt reclaims superseded blobs
(`docs/storage-durability.md:13-31`). The invariant is "a committed, visible row never references a
blob that is not already durable" (`:33-36`).

**Listing.** Answered from the index, not the filesystem: ordered prefix-range scan over
`idx_ov_latest_cover` with `range::prefix_upper_bound`/`successor`
(`crates/cairn-meta/src/range.rs:13-40`), O(log n + page) and index-only for latest-only listings
(`docs/metadata.md` §11.3 "Object versions"). Version listing uses the same B-tree ordered by
`(bucket, key, version_id)`.

**Conditional/atomic semantics.** `If-Match`/`If-None-Match` are read-and-check in the same savepoint
as the upsert (`apply.rs:3022-3055`), so no lock is held during body ingestion and there is no TOCTOU
window. Multi-row metadata edits (version + tags + lock + outbox + stats) commit or roll back
together; lifecycle/delete guards use exact row identity and return `DeleteNotApplied` on a stale
predicate (`docs/metadata.md:111-121`).

**Backup/restore.** Offline, narrow by design: `cairn backup` verifies the single-SQLite topology and
exclusive node lock, then takes the metadata image with `VACUUM INTO` (never a raw copy), plus the blob
manifest, into a self-describing snapshot with a final manifest (`docs/backup-restore.md:3-45`);
`cairn restore` verifies size+SHA-256 and rejects WAL/SHM sidecars (`:62-75`). Non-single-SQLite
topologies (libSQL/Turso/sharded) are refused rather than partially backed up (`:3-5`).

**Crash recovery.** WAL replay plus the storage-intent journal: an unfinished admitted intent blocks
cleanup regardless of age, and startup recovery is `full-scan` over the blob tree to reconcile orphans
(`docs/metadata.md:35-43`, tables at `schema.rs:861-915`).

**Known relaxation.** With `CAIRN_META_SHARDS > 1` the metadata splits by bucket; cross-bucket reads
(`list_buckets`, aggregate counts, global tag browser) fan out and merge, and **per-user quota across
shards is eventually consistent** ("a documented relaxation versus the single-shard store's exact
enforcement", `crates/cairn-meta/src/shard.rs:18-22`).

---

## 7. Head-to-head on the axes asked

| Axis | RustFS 1.0.4 (files) | MinIO (files) | Cairn (SQLite) |
|---|---|---|---|
| Where version/ACL/tags/locks live | one `xl.meta` per key: version journal + `meta_user`/`meta_sys`; bucket policy/ACL/quota in one bucket-metadata object | same (`xlMetaV2` journal + `MetaUser/MetaSys`); bucket config as `.minio.sys` objects | `object_versions` row + `object_tags`/`object_locks` side rows; `bucket_config` aspect rows |
| Atomic conditional PUT | advisory read, then read+check **under namespace lock** at commit; lock is in-memory/single-node or quorum lock service, not a durable CAS (`unified-object-generation.md:8-40`) | check under namespace lock held across body ingest (`erasure-object.go:1261-1284`) | check + upsert in one savepoint/transaction (`apply.rs:3022-3055`) |
| Multi-key atomicity | none (per-key delete-recovery decisions) | none | any set of rows in one savepoint |
| Listing | dir walk, whole-directory read + sort per page, `xl.meta` read per entry, quorum merge over `(N+1)/2` drives | dir walk memoized into `.metacache/*.s2` streams | B-tree range scan, index-only, O(log n + page) |
| Listing at millions of keys | cost grows with directory size per page; 5s first-page walk timeout is a documented concern (`list_objects.rs:4712-4718`) | same walk, but resumable cached stream amortizes repeat pages | flat |
| Version listing | cheap per key (all versions in one file), full walk per bucket | same | index range over `(bucket,key,version_id)` |
| Per-key write cost vs versions | O(versions of that key) parse + rewrite of `xl.meta` | same | O(1) row insert + index maintenance |
| Quota | reservation ledger objects + ordered locks + scanned usage; can fail closed on incomplete usage | scanner snapshot, explicitly can go unenforced (`bucket-quota.go:65-83`) | `quota_bytes` + maintained totals checked in the commit txn |
| Policy/ACL change cost | one object PUT (quorum + fsyncs) of the whole bucket document | one `.minio.sys` object PUT | one `bucket_config` row update |
| PUT durable waits (single node, strict) | staged shard fdatasync + `xl.meta` write+fsync + data-dir rename + `xl.meta` rename + parent-dir fsync, per disk in quorum | fdatasync shard + O_DSYNC `xl.meta` + 2 renames, **no dir fsync** by default | admission txn commit (fsync) + blob fsync + dir fsync + publish txn commit (fsync) |
| Backup unit | every drive's tree; no consistent global snapshot | same | one DB file via `VACUUM INTO` + blob manifest |
| Crash recovery | tmp cleanup, per-object `xl.meta.bkp`, heal/scanner; corrupt `xl.meta` ≈ loss | same (no backup dir fsync, so a rename can vanish) | WAL replay + intent journal + full-scan reconcile |
| Global queries (usage, "all objects with tag X", orphan blobs) | require scans (`compute_bucket_usage`, scanner) | require scans (`data-scanner`) | SQL |

---

## 8. Concrete trade-offs, stated plainly

**What the file design wins on a single node**

1. No metadata write before the body: RustFS/MinIO create a file and write it; Cairn protocol 2
   commits an admission transaction *before* any file may be created (`docs/storage-durability.md:13-17`).
   That is one extra durable commit (and, with `synchronous=full`, one extra fsync) on every PUT.
2. Independent failure domains: one damaged key is one directory; there is no shared B-tree page to
   corrupt and no whole-store restore to recover one object.
3. Zero cross-key contention: 1M concurrent PUTs to 1M keys touch 1M directories; Cairn serializes
   every mutation through one writer thread.
4. Format compatibility with MinIO (`crates/filemeta/src/test_data.rs:134-175`) and a metadata layer
   that can be healed from EC peers rather than restored from backup.

**What the file design gives up (and Cairn buys)**

1. Exact conditionals without a lock: RustFS holds an object lock across the commit and relies on a
   lock service; MinIO holds it across the client body. Cairn holds nothing during ingest and still
   has no TOCTOU.
2. Listing cost bounded by the page, not by the directory: the file design re-reads and sorts the
   whole parent directory per page (`local.rs:8762`), Cairn walks a B-tree in key order.
3. Global queries and atomic multi-row metadata: tags+lock+outbox+stats+counter updates in one commit
   are impossible without a transactional store; RustFS rebuilds pieces of it as reservation ledgers
   and delete-recovery decisions.
4. Bounded per-key metadata: an object with 10k versions is a 10k-entry `xl.meta` rewritten on every
   PUT; a SQL row insert is flat.
5. A real recovery unit: Cairn can point at one file, verify its hash, and restore
   (`docs/backup-restore.md:27-45`). RustFS/MinIO recovery is heal + scan + hope the surviving
   `xl.meta` copies agree.

**What Cairn gives up**

1. Two durable metadata commits per PUT (admission + publish); on `synchronous=full` that is two
   fsync barriers per object, serialized behind one writer. This is the dominant PUT-latency cost and
   it does not exist in RustFS/MinIO.
2. Single-writer ceiling: group commit amortizes but never parallelizes (`writer.rs:1-5`).
3. The database is a single point of failure/restore; corruption is not healable from object copies.
4. Sharded mode relaxes cross-bucket reads and per-user quota (`shard.rs:18-22`), i.e. scaling
   metadata reintroduces exactly the eventually-consistent behaviour the single DB removed.

---

## 9. Open questions worth verifying before any engine change (labelled as open)

- Whether a Cairn PUT could collapse admission+publish into one commit for the non-versioned,
  non-replicated, no-cleanup-debt case without weakening the "visible row never points at a
  non-durable blob" invariant (`docs/storage-durability.md:33-36`). Not investigated here.
- Whether `synchronous=NORMAL` plus blob-first ordering is an acceptable production posture — the
  library default argues yes, the server default says no (`crates/cairn-meta/src/lib.rs:74-87`).
  Not measured here.
- RustFS `relaxed` on a single node is explicitly discouraged ("Single-node deployments must stay on
  `strict`", `docs/operations/durability-modes.md` §Power-loss guarantees), so its practical
  single-node durability is *at least as strict* as Cairn's `full` — the comparison above assumes
  strict for both.
- MinIO line numbers are from current master, not a release tag; the `xl.meta`/`RenameData` shape has
  been stable for years but exact line offsets will drift.
