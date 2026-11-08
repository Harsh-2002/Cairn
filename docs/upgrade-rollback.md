# Upgrading, rollback, and version compatibility

> Operator guide (not part of the section-numbered spec). See [`operations.md`](./operations.md) for
> deploy shapes and the master-key rotation runbook, and [`backup-restore.md`](./backup-restore.md)
> for the snapshot procedure this guide refers to.

Cairn ships as a single static binary. An "upgrade" is replacing that binary (and the embedded
console) and restarting the process; there is no separate data-plane to migrate. This guide states
what is guaranteed across versions, the supported upgrade and rollback procedures, and the one
decision that is **fixed at first init** and cannot be changed later.

## 1. Versioning and stability

- Cairn is **1.0 as a support status (2026-11-07)**; the crate version in `Cargo.toml` remains
  `0.1.0`, and release identity is the CalVer tag. Pin to a specific tag or commit for production
  and read the release notes before upgrading. The forward-upgrade contract below is the 1.0
  contract; older tags remain forward-compatible only through the append-only migration path.
- The **on-disk contract is the stable part**: object bytes and their per-object metadata are plain
  files, and SQLite holds the object index and the control-plane state. The schema evolves only
  through **append-only migrations**
  (`crates/cairn-meta/src/schema.rs`): a new version adds a migration; an applied migration is never
  edited. A newer binary opens an older database and runs the outstanding migrations forward on
  startup. The wire protocol (S3 SigV4/Bearer) and the `CAIRN_*` configuration surface are intended
  to be stable; additive changes (new endpoints, new optional knobs) are the norm.
- A pre-protocol-3 release **fails closed** on a trailer-bearing plaintext object: it reads the
  declared length from the index and finds a mismatch instead of serving the trailer as payload.
  That is a refusal, not a migration, so rollback still requires the pre-upgrade snapshot.

### Multipart sidecars and the BLAKE3 baseline across versions

Assembled multipart objects keep `<uuid>.meta` beside their data file. The current record is v3: it
carries the ordered part layout and the internal plaintext BLAKE3 baseline, so startup rebuild can
restore both the row and `GET`/`HEAD ?partNumber` after index loss. v1 (no layout, legacy SHA-256)
and v2 (layout, legacy SHA-256) records remain readable. A binary that predates the v3 record still
serves the object from SQLite, but it cannot decode the sidecar, so a later index loss cannot rebuild
that row — and, because it also predates schema v42, it refuses to start against the upgraded
database. Downgrade is therefore unsupported: restore the pre-upgrade snapshot.

New objects write `object_versions.internal_blake3` and leave `internal_sha256` NULL; rows created
before schema v42 keep their SHA-256 value and are still scrubbed with SHA-256. There is no
`CAIRN_*` selector and no dual-write: the migration is one-way. A pre-v42 binary refuses the state
through the `storage_protocol` floor (raised to reader/writer 3 by migration v42) instead of starting
and comparing a BLAKE3 digest as if it were SHA-256.

Objects completed before the multipart sidecar existed, and objects whose sidecar is still v1 even
though the row records the part layout (assembled between the v1 and v2 stages), can be repaired
after stopping the node:

```sh
cairn backfill-sidecars --dry-run
cairn backfill-sidecars
cairn backfill-sidecars --upgrade-v1
```

The command takes the node lock, writes only missing sidecars unless `--upgrade-v1` is given, and is
safe to rerun. Without the flag it never overwrites an existing sidecar. `--upgrade-v1` replaces an
existing v1 record only when the record identity (`row_id`, bucket, key, version id, storage path,
ETag and both sizes) matches the index row; it never rewrites a v2 or v3 record, and reports a
conflict instead of guessing. It writes v3 when the row has a BLAKE3 baseline and preserves v1/v2
for legacy rows. Rerun until the summary reports `candidates=0`.

## 2. Forward upgrade (newer binary, same data)

1. **Snapshot first.** Take a backup per [`backup-restore.md`](./backup-restore.md) (database-first
   snapshot + blob copy). This is your rollback point.
2. Stop the node (SIGTERM — Cairn drains in-flight requests within the grace period, then exits).
3. Replace the binary.
4. Start the node. Startup runs any outstanding schema migrations **before** serving (it does not
   answer `/readyz` until migrations and startup reconciliation have completed), then re-affirms the
   root admin and resumes the background loops (replication drain, lifecycle, WAL checkpoint, scrub).
5. Verify: `cairn validate-config` (or watch the startup log), then `GET /readyz` → `ready`.

**Table-rebuild migrations and space.** Schema v44 rebuilds `object_versions` `WITHOUT ROWID` in
one transaction. Measured on a 1M-row SQLite replica with the v44 DDL: about 66 s, a ~661 MB WAL peak, and a database growth
from 441 MB to 657 MB (the old pages stay on the freelist until a vacuum). Scale roughly linearly:
a 100M-row store is on the order of two hours and tens of gigabytes of transient space. Keep free
space of at least three times the `object_versions` table (plus the pre-upgrade snapshot) and plan a
maintenance window. The rebuild is atomic: a crash rolls it back, leaving the pre-upgrade state.
There is no `CAIRN_*` selector to skip it. Startup logs
`metadata schema migration starting` with the database size and unprivileged free bytes, warns when
free space is below the 3× rule, and logs `metadata schema migration complete` with the elapsed
seconds and WAL size. If startup still runs out of space, SQLite returns `SQLITE_FULL`, the
migration transaction rolls back, and the node does not become ready.

**Migrations are forward-only.** There is no automatic down-migration. Starting with the
storage-protocol v35 safeguard, startup rejects unsupported newer schemas or protocol state before
Cairn changes PRAGMAs, runs migrations/sanitation or starts its Writer. SQLite, libSQL and Turso
perform this check; sharded SQLite preflights every existing shard before migrating any shard.
Snapshot validation also checks compatibility before staging target data.

**Already released older binaries may open and mutate newer data.** The new guard cannot retrofit
protection into them. Use a verified pre-upgrade snapshot for downgrade; do not try the previous
binary against upgraded data to see whether it refuses. Schema v35 records minimum reader/writer
protocol 1, flat writes and full-scan recovery; v36 raises the floor to 2; schema v42 raises it to 3
when it adds the BLAKE3 baseline. Missing, malformed or unsupported state fails closed. The floor
does not activate a recovery journal or change object placement.

### Zero-downtime upgrades

A single Cairn node is **not** a rolling-upgrade target on its own: stopping it stops the service for
the restart window (typically sub-second once drained). To upgrade without a serving gap, run **two
nodes with bucket replication** (see [`replication.md`](./replication.md)) behind a load balancer and
upgrade them one at a time, shifting traffic off the node being upgraded. Because replication is
asynchronous (eventually consistent with observable lag), drain replication to the peer (watch
`cairn_replication_unreplicated`) before taking a node down so the peer holds a current copy.

## 3. Rollback

Rollback is **restore-from-snapshot**, not schema downgrade:

1. Stop the node.
2. Restore the pre-upgrade snapshot into the data dir per [`backup-restore.md`](./backup-restore.md)
   (`cairn restore <dir>` places the database + blobs and runs reconciliation).
3. Start the **previous** binary.

An unchanged schema number alone does not establish backward compatibility: storage framing and
writer/recovery semantics also matter. The supported downgrade procedure uses a verified
pre-upgrade snapshot. An unsupported older writer or an external restore invalidates any future
journal-coverage assumption; a full reconciliation and a newly completed offline baseline would be
required before trusting journal recovery. Current startup continues to perform the full scan.

## 4. Decisions fixed at first init (cannot be changed in place)

Two choices are locked when the store is first created and have **no in-place migration path**:

- **Metadata backend** (`CAIRN_META_BACKEND`: the default embedded SQLite, or the beta
  libSQL/Turso). Switching backends on a populated store is not supported.
- **Shard count** (`CAIRN_META_SHARDS`). Buckets are routed to a shard by a stable hash of the bucket
  name (`crates/cairn-meta/src/shard.rs`); changing the count would route an existing bucket to a
  shard that does not hold its rows. See [`scaling-limits.md`](./scaling-limits.md) for how to choose
  the shard count up front.

To change either after the fact, stand up a **new** node with the desired configuration and migrate
data across with replication or an S3-level copy (`aws s3 sync`), then cut over.

## 5. Checklist

- [ ] Read the release notes; note any new migration.
- [ ] Take and verify a snapshot (`backup-restore.md`).
- [ ] Stop → swap binary → start; confirm `/readyz` is `ready`.
- [ ] Spot-check a GET/PUT against a real S3 client.
- [ ] If anything is wrong: stop, restore the snapshot, start the previous binary.

## Verified release-to-main upgrade and snapshot rollback (2026-11-05)

The last published release, v2026.09.13 (commit `f6fde5a`, schema v39, protocol 2), was upgraded
to main at `b9472d2` (schema v42, protocol 3) with `conformance/upgrade_drill.py`:

- plaintext, SSE-S3 and completed multipart objects were written by the release binary;
- after the upgrade all three read back byte-for-byte; the legacy multipart object has no recorded
  part layout, so its `?partNumber` read fails closed, as documented above;
- legacy rows keep `internal_sha256` and have `internal_blake3` NULL;
- a new post-upgrade multipart object records BLAKE3, a v3 sidecar, and a working `?partNumber`;
- the released binary refuses the upgraded database (schema/protocol fence, exit 1);
- restoring the pre-upgrade data-directory snapshot lets the released binary start and read every
  object exactly.

The drill runs in `validate.yml` (full profile) against the pinned `v2026.09.13` asset; manual runs
still take `--old-bin`/`--new-bin`. On 2026-11-08 it was extended and re-run from the same pinned
release (schema v39/protocol 2) to main `f885b5a9` (schema v44/protocol 3):

- a GOVERNANCE-retained, legal-held object written by the old binary keeps its exact retention
  mode/date and legal hold after the rebuild, and `PRAGMA foreign_key_check` is empty;
- `object_versions` is clustered (`WITHOUT ROWID`), `idx_object_versions_id` exists and the partial
  `latest_cover` index is absent;
- every old plaintext, SSE-S3 and multipart object still reads byte-for-byte; the legacy
  `?partNumber` read fails closed; new multipart writes are v3/BLAKE3;
- the old binary refuses the v44 database, and the snapshot rollback reads every old object.

The drill does not replace the pre-upgrade snapshot: the old binary cannot open the upgraded
database, so the snapshot is the only rollback path.

## API and console configuration migration

This change has no compatibility aliases. Replace these settings before starting the new binary:

| Removed | Replacement |
|---|---|
| `CAIRN_LISTEN_ADDR` | `CAIRN_API_ADDR` |
| `CAIRN_WEB_ADDR` | `CAIRN_CONSOLE_ADDR` |
| `CAIRN_PUBLIC_BASE_URL` | `CAIRN_API_PUBLIC_URL` |
| `CAIRN_ENDPOINT`, `CAIRN_S3_ENDPOINT` | `CAIRN_API_ENDPOINT` (CLI only) |

Add `CAIRN_CONSOLE_PUBLIC_URL` for public console download links behind ingress. Public URL values
must be origins, without path prefixes. Replace Cairn CLI `--endpoint`/`--s3-endpoint` with one
`--api-endpoint`; replace installer `--expose-s3` with `--expose-api`. Server configuration remains
environment-only. Do not change storage paths, credentials or master keys during this migration.

Native `/api/v1` administration is now exposed on the API listener. Disabling or firewalling the
console does not disable native administration; update proxy access rules where required.
Run `cairn validate-config`, then recreate the container or restart the service with the new
environment. Verify API readiness, native administration, console sign-in, upload and a public
download link. This PR does not change the storage format or create a new downgrade guarantee;
all existing storage rollback restrictions still apply.
