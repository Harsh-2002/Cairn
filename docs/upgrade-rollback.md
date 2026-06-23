# Upgrading, rollback, and version compatibility

> Operator guide (not part of the section-numbered spec). See [`operations.md`](./operations.md) for
> deploy shapes and the master-key rotation runbook, and [`backup-restore.md`](./backup-restore.md)
> for the snapshot procedure this guide refers to.

Cairn ships as a single static binary. An "upgrade" is replacing that binary (and the embedded
console) and restarting the process; there is no separate data-plane to migrate. This guide states
what is guaranteed across versions, the supported upgrade and rollback procedures, and the one
decision that is **fixed at first init** and cannot be changed later.

## 1. Versioning and stability

- Cairn is **pre-1.0** (`Cargo.toml` is `0.x`). Until a tagged `1.0`, minor releases may change
  behaviour; pin to a specific tag or commit for production and read the release notes before
  upgrading.
- The **on-disk contract is the stable part**: object bytes are plain files and all metadata is the
  embedded SQLite database. The schema evolves only through **append-only migrations**
  (`crates/cairn-meta/src/schema.rs`): a new version adds a migration; an applied migration is never
  edited. A newer binary opens an older database and runs the outstanding migrations forward on
  startup. The wire protocol (S3 SigV4/Bearer) and the `CAIRN_*` configuration surface are intended
  to be stable; additive changes (new endpoints, new optional knobs) are the norm.

## 2. Forward upgrade (newer binary, same data)

1. **Snapshot first.** Take a backup per [`backup-restore.md`](./backup-restore.md) (database-first
   snapshot + blob copy). This is your rollback point.
2. Stop the node (SIGTERM — Cairn drains in-flight requests within the grace period, then exits).
3. Replace the binary.
4. Start the node. Startup runs any outstanding schema migrations **before** serving (it does not
   answer `/readyz` until migrations and startup reconciliation have completed), then re-affirms the
   root admin and resumes the background loops (replication drain, lifecycle, WAL checkpoint, scrub).
5. Verify: `cairn validate-config` (or watch the startup log), then `GET /readyz` → `ready`.

**Migrations are forward-only.** There is no automatic down-migration. A newer schema version is not
readable by an older binary — which is why the pre-upgrade snapshot is the rollback mechanism, not a
schema downgrade.

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

Rolling back *without* a snapshot is only safe if the newer binary applied **no** new migration
(check the release notes / `cairn migrate` reports the applied schema version). If a migration ran,
the older binary cannot open the database — restore the snapshot.

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
