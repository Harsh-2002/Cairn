# Backup and Restore

Because the metadata database is the single source of truth and blobs are immutable files named
by opaque identifier, a consistent backup follows a defined order with a clear consistency
argument (ARCH 31.4).

## Backup procedure (default: database-first)

1. **Snapshot the database consistently** using SQLite's online backup / `VACUUM INTO`, which
   yields a single transactionally-consistent file without stopping writes:
   ```sh
   sqlite3 /data/cairn.db ".backup '/backup/cairn.db'"
   ```
2. **Copy the blob directories** (everything under the data root except the database and the
   `.staging` directory):
   ```sh
   rsync -a --exclude 'cairn.db*' --exclude '.staging' /data/ /backup/data/
   ```

**Why this order is consistent.** Taking the database snapshot first and copying blobs second
guarantees the copied blob set is a *superset* of what the snapshot references: every object in
the snapshot existed at snapshot time, its immutable blob existed then, and blobs are never
renamed — so the restore finds a blob for every row, with at most some extra blobs from objects
written after the snapshot, which reconciliation harmlessly reclaims.

**The one residual edge case.** An object deleted in the window between the database snapshot and
the blob copy may have its blob missed by the copy while the snapshot still references it,
leaving a row without a blob on restore. Mitigations, in increasing strength:

- *Accept it* — a single just-deleted object; the lazy per-read integrity check flags it.
- *Copy blobs first, snapshot second* — reconciliation then reclaims the (now larger) set of
  extra blobs, and a read of the one affected row surfaces a clear error.
- *Quiesce writes briefly* during the snapshot for a perfectly consistent backup.

The **master key is deliberately excluded** from the backup that contains the database, so the
backup alone does not disclose the envelope-encrypted secrets. Store it separately.

## Restore

1. Place the snapshot database and the copied blob directories under the data root (same
   filesystem).
2. Provide the master key (`CAIRN_MASTER_KEY`).
3. Start with reconciliation enabled (the default); it reclaims any extra blobs.

```sh
cairn integrity            # on-demand reconciliation (reclaims orphaned blobs)
```
