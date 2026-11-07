# Unversioned-delete write amplification: remaining plan (2026-11-08)

**Status: proposed.** The next step is a storage-format migration (schema v44) and needs an explicit
go-ahead. Nothing in this document is implemented.

## Where the row stands

| 100k-object 12 s arm | Cairn | Note |
|---|---:|---|
| #219 baseline | 839.8 obj/s | ~66 KiB device writes per delete |
| v43 (drop duplicate `bkv` index) | 910.9 | +8.5% |
| v43 + UUIDv7 `StorageToken` | **1,147.0** | **+36.6%** |
| RustFS, same host | 1,783.5 | as-configured, RustFS does not fsync this path |

The device is still saturated: about **52 KiB written per random-key delete** at the drive's
~57 MB/s sustained rate. Every attempt that only reduced commits (bulk mutation, audit co-commit,
batch submit) failed because bytes, not barriers, are the limit.

## Byte budget

A 100k-row SQLite replica with the real DDL and 100-key transactions measures the WAL cost per
delete (the device cost is roughly twice this because the WAL is later checkpointed):

| Variant | WAL/delete | Approx. pages |
|---|---:|---:|
| Full v42 schema | 32.5 KiB | 8 |
| v43 (no `bkv`) | 28.5 KiB | 7 |
| v43 + UUIDv7 cleanup ids | 23.3 KiB | 6 |
| v43, no cleanup debt | ~20.3 KiB | 5 |

After v43, `object_versions` is five pages: the rowid table, the `id` primary-key index, the
`UNIQUE(bucket_name,key,version_id)` auto-index, the `storage_path` index, and the partial
`latest_cover` index. The exact cleanup-debt row adds about 3 KiB even with monotonic ids.

## The target

Matching RustFS's 1,783.5 obj/s on this device needs at most **~32 KiB of device writes per
delete**. Because device bytes are roughly twice the WAL budget, the WAL must fall to ~15–16 KiB
per delete — four pages total including cleanup.

## Recommended design

Rebuild `object_versions` as `WITHOUT ROWID` with
`PRIMARY KEY (bucket_name, key, version_id)`.

- The table *is* the unique key: the rowid table page and the separate `UNIQUE` auto-index collapse
  into one. `idx_object_versions_bkv` is already gone in v43.
- Drop the `id` primary-key index. Keep `id` as a plain, unique-by-construction column used only by
  the lifecycle compare-and-delete guard.
- Keep `storage_path` (reconcile/ownership) and `latest_cover` (current listing). Secondary indexes
  on a `WITHOUT ROWID` table append the primary key; `latest_cover` already leads with it, so only
  `storage_path` grows modestly.
- Expected: table (1 page) + `storage_path` (1) + `latest_cover` (1) + cleanup (~0.75) ≈ 3.75
  pages, ~15 KiB WAL → ~30 KiB device → **~1,900 obj/s** at the measured device rate.

### Refactors the id-index removal requires

1. `apply.rs` and `cairn-meta-async/src/apply.rs` version promotion
   (`UPDATE object_versions SET is_latest=1 WHERE id=?`): update by the primary key of the promoted
   row, which the preceding `SELECT` already has.
2. `baseline.rs` storage-authority page (`WHERE id>? ORDER BY id LIMIT ?`): change the cursor and
   order to `(bucket_name, key, version_id)`; update `StorageAuthorityCursor` and its state tests.
3. `store.rs` SSE key re-wrap (`UPDATE ... WHERE id=?`): carry the primary key through the job.
4. Schema tests that select by `id='…'`: select by the primary key.
5. The in-memory double already keys by `(bucket, key, version)`, so its contract is unchanged.
6. Mirror the migration and the three query changes in `cairn-meta-async`. Its migrations are
   numbered independently (its v2 already dropped `bkv`), so the new async migration is its v43.

## Migration, risk, and gates

- **Schema v44:** create the new table, copy every row, drop the old table, recreate the two
  secondary indexes, all in one transaction. One-way, like every schema migration; a v43 binary
  refuses the store.
- **Space:** up to 2× the `object_versions` table plus indexes free during the rebuild. Document
  the maintenance window and the free-space requirement in `upgrade-rollback.md`.
- **Crash safety:** ordinary SQLite DDL/DML in one transaction. Extend
  `conformance/upgrade_drill.py` and the crash harness with a v43→v44 arm.
- **Read regression gate:** LIST/GET/HEAD must stay within noise. The 1.0 scoreboard has LIST at
  5.33×, so some read cost is affordable, but it must be measured.
- **Acceptance:** the same 100k-object delete arm must clear RustFS's same-session rate (1,783.5 in
  the current pair) or the migration is reverted before release.

## Explicit non-goals

- No durability relaxation: the durable metadata commit remains the ACK point.
- No `synchronous=NORMAL`, and no deferring WAL checkpoints to hide work outside the 12 s window.
- No dual-write or config selector for the table format.

## Decision

Approve the v44 rebuild, or keep **1,147.0 obj/s (0.64×)** as the durable result on this host. The
delete path already reaches **5,680 obj/s with the device flush cost removed** (tmpfs diagnostic),
so the remaining gap is the consumer QLC drive's write budget, not the delete code.
