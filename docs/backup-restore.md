# Backup and Restore

Cairn's supported snapshot format is deliberately narrow: **one local SQLite metadata database**
(`CAIRN_META_BACKEND=sqlite`, `CAIRN_META_SHARDS=1`) and its POSIX blob tree. The command refuses
libSQL, Turso, and sharded SQLite instead of producing a partial backup.

Both operations are **offline**. Stop the server first; `cairn backup`, `cairn restore`, `serve`,
and every other node-local command take the same non-waiting advisory locks over the data root and
database identity. A command fails if another Cairn process owns either half. Also stop any
external SQLite or filesystem tool, because it does not participate in Cairn's locks.

## Backup

Choose an empty destination outside `CAIRN_DATA_DIR`, then run:

```sh
systemctl stop cairn
cairn backup /backup/cairn-2026-07-29
systemctl start cairn
```

The command:

1. verifies the canonical single-SQLite topology and exclusive node lock;
2. performs a non-waiting truncating WAL checkpoint and refuses a pinned reader/writer;
3. creates the metadata image with SQLite `VACUUM INTO`—the live database file is never raw-copied;
4. runs `PRAGMA integrity_check` and `PRAGMA foreign_key_check` on that image;
5. durably copies committed object blobs and active multipart-part files, excluding transient
   `.staging/*.tmp`, SQLite sidecars, and Cairn lock files; and
6. streams every `object_versions.storage_path` and `multipart_parts.storage_path` from the
   snapshot and verifies that the corresponding regular file exists; then
7. writes `manifest.json` last, after all earlier files and directories are synced.

The snapshot has a fixed, self-contained layout:

```text
manifest.json       completion marker; written last
metadata.sqlite3    sidecar-free SQLite image
blobs/              committed buckets plus .staging/multipart
```

Manifest format version 1 records `complete: true`, creation time and Cairn version, the
`sqlite`/one-shard topology, applied schema version, database filename/size/SHA-256, and blob-layout
version. A directory without that final manifest is an incomplete snapshot, even if some files are
present. The manifest does not list multipart reservations or cleanup debt: those v26 bookkeeping
rows intentionally may precede or outlive a file; `multipart_parts.storage_path` remains the
authoritative required-present staging reference.

The destination must be empty so files from an older generation cannot masquerade as part of the
new snapshot. Source and destination trees may not overlap.

The **master key is deliberately excluded**. Store `CAIRN_MASTER_KEY` or the complete
`CAIRN_MASTER_KEY_RING` separately in the secret manager and backup system. A database-and-blob
snapshot without the required key material is intentionally unreadable. Preserve the service's
effective `CAIRN_*` environment separately as well, including any environment-defined replication
targets and their credentials; restoring database rows does not recreate that external configuration.

## Restore

Restore only a directory produced by `cairn backup`, with the server stopped and the original
master-key material available:

```sh
systemctl stop cairn
cairn restore /backup/cairn-2026-07-29
systemctl start cairn
```

Before changing the target, restore requires the final manifest, rejects an unsupported format,
topology, blob layout, or schema newer than the running binary, and verifies the database's exact
size and SHA-256. It rejects snapshot `metadata.sqlite3-wal`, `-shm`, and `-journal` sidecars rather
than silently ignoring them, runs SQLite integrity/foreign-key checks, verifies every referenced
object and multipart-part file, and rejects symlinks, special nodes, and top-level regular files in
`blobs/`.

The database is copied to a synced, target-owned sibling and checked against the manifest again.
If the old target generation has an exact WAL, SHM, or rollback-journal sidecar, Cairn first opens
that generation through its canonical Writer, checkpoints and closes every owned SQLite
connection, syncs the old main file, removes the exact sidecars, and syncs the parent directory.
Only then can the atomic rename publish the staged image. Therefore a crash immediately before the
rename reopens a complete old generation, while a crash immediately after it reopens only the new
generation. Immutable blobs are copied before the metadata rename with no-replace publication:
an existing target path is reused only when a no-follow, byte-for-byte comparison proves it
identical to the snapshot, while a different collision aborts restore without changing the old
inode. A new path is staged and synced, then published with an atomic hard link; an
`AlreadyExists` race is re-opened no-follow and subjected to the same identity rule rather than
replaced. Reconciliation runs while the exclusive node lock remains held; target-side blobs not
referenced by restored metadata are reclaimed. Any non-zero reconciliation error count makes
restore fail even when the reconciliation call itself returned a report, matching the pre-bind
startup gate rather than declaring a partially checked generation ready.

## Recovery verification and durable work

The SQLite image preserves complete metadata rows, including historical versions and delete
markers, tags, ACLs, Object Lock retention/legal hold, sealed object and multipart-part data keys,
and multipart reservations, cleanup records, replication outbox work, and remote multipart journals. These are database state;
there is no second manifest inventory that can replace them. Restoring the database does not restore
the external master-key ring or recreate the outcome of an ambiguous remote request.

Before serving, startup releases interrupted multipart completion ownership and replication claims
through the canonical Writer, then reconciles files. A restored active upload must retain its staged
parts and captured tags/lock/encryption intent so the client can retry Complete. Replication work may
be attempted again: generic S3 destinations can gain another version after an ambiguous prior
success. Isolate an old primary before resuming a restored node to avoid two independent sources
replaying the same backlog.

The live `conformance/backup_restore.sh` gate includes `recovery_state.py`, which writes this state
through the S3 API, interrupts a real replication request, checks every table's complete rows across
snapshot/restore, verifies versioned reads and protection, and completes a restored encrypted upload.
It also rejects backup under a live node lock, sharded topology, a snapshot missing an authoritative
part, and startup with the wrong master key. The failpoints `crash_multipoint.sh` gate additionally
pauses Complete after durable assembly, kills the process with an exact completion claim persisted,
and verifies that restoration/restart makes the upload retryable without leaking the assembled blob.
These checks are distinct from the database-publication unit tests that pin the old/new generations
on either side of the atomic rename.

Remote multipart journals preserve the exact destination identity and any upload ID received before
the crash. On startup, the Writer invalidates abandoned delivery and cleanup ownership; existing
replication workers reclaim known upload IDs using the saved target route. A missing initiation
receipt is different: the destination may already hold an upload whose ID the source never learned.
That journal row remains an explicit orphan incident and requires destination lifecycle cleanup;
a successful retry of the object does not make that old remote upload disappear. Preserve the
original target configuration until known cleanup debt drains.

The additional `conformance/recovery_remote.py` drill uses a real Cairn destination behind an HTTP
proxy. It holds successful initiation, part, and (with `--cleanup-lease`) abort responses, kills the
source, then compares every durable row across offline backup and restore. It checks unknown-ID
incident retention, known-ID cleanup, startup release of abandoned cleanup ownership, and exact
native version identity after redelivery. This harness requires the schema-v33 streaming sender;
all three fault arms passed against the final v33 sender implementation at `5a9a9b1`.

## Database-path upgrade requirement

`CAIRN_DB_PATH` may be a direct child of `CAIRN_DATA_DIR` (the default) or outside it on the same
filesystem. It may **not** be in a deeper directory below the data root. Reconciliation treats
every top-level data-root directory other than `.staging` as a bucket, so a former layout such as
`CAIRN_DATA_DIR=/srv/cairn` with `CAIRN_DB_PATH=/srv/cairn/meta/cairn.db` is unsafe and is now
rejected by `validate-config` and every node-local command.

Before upgrading such a node, stop Cairn and every SQLite tool. Confirm the old binary completed a
non-busy truncating checkpoint and that `cairn.db-wal`, `cairn.db-shm`, and `cairn.db-journal` are
absent; do not move only the main file while a sidecar exists. Move the self-contained database to
`/srv/cairn/cairn.db` (or another path on the same filesystem), remove the now-empty `meta/`
directory so it cannot be interpreted as a bucket, update `CAIRN_DB_PATH`, run
`cairn validate-config`, and retain an out-of-band copy until `cairn integrity` succeeds.

Do not use this format to migrate between metadata engines or shard counts. Export/import through
the S3 surface is the supported cross-topology path.
