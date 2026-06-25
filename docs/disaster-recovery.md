# Disaster Recovery

Operator runbook for recovering a Cairn deployment after data loss or node loss. Cairn is a
**single-node** store with **asynchronous** bucket replication (spec: `replication.md` §20); it is not
synchronous HA and has no automated failover. That makes durability your responsibility to design —
and entirely achievable with the building blocks below. Read `backup-restore.md` and
`operations.md` alongside this.

## 1. The failure model — what can be lost, and what protects it

A Cairn node has two pieces of state on one filesystem (the one-filesystem invariant,
`operations.md` §1):

- the **metadata database** (SQLite) — the single source of truth for the namespace; and
- the **blob files** — the object bytes under opaque IDs.

| Failure | Protection |
|---|---|
| Metadata DB lost or corrupt, disk otherwise intact | Restore the DB from backup, then `cairn integrity --repair` |
| Whole node / disk lost | A **replication destination** (warm copy) or an **off-box backup** (3-2-1) |
| Single blob bit-rots on disk | The background **scrub** flags it (`cairn_scrub_corruption_total`); re-fetch from a replica/backup |
| Accidental or malicious overwrite/delete | **Versioning** + **Object Lock/WORM** (replication and RAID do *not* protect against this) |

The metadata DB is the critical single point of failure: lose it without a backup and you lose the
namespace even though the blobs survive. **A tested backup of the DB is non-negotiable.**

## 2. RPO and RTO — set expectations honestly

- **RPO (data you can lose)** = the replication lag at the instant the primary is lost, or the age of
  your last backup if you have no live replica. With async replication, in-flight writes that had not
  yet shipped are lost. Watch the lag (§3) to know your live RPO.
- **RTO (time to restore service)** = manual. There is no automatic promotion; recovery is a deliberate
  operator action (re-point clients at a destination, or restore a backup to a fresh node). Budget
  minutes, not seconds.

If you need zero-RPO synchronous failover, Cairn is not the right tier for that workload — see the
positioning in `overview.md`. For homelab, edge, dev/staging, small-prod, and backup-target use, the
procedures here give a sound recovery posture.

## 3. Observability — the signals to watch before and during a disaster

Scrape `/metrics` (S3 port). Replication-health gauges (spec: `replication.md` §20.5):

- `cairn_replication_lag_seconds` — age of the oldest pending entry; **this is your live RPO**.
- `cairn_replication_unreplicated` — pending + in-flight + terminally-failed; non-zero whenever any
  object is owed or stuck. Alert if it stays non-zero (lag/queue_depth alone fall to 0 once a backlog
  fails out).
- `cairn_replication_queue_depth` — entries currently due.
- `cairn_replication_failed_total`, `cairn_replication_failed_by_target` — rising = a destination is
  unreachable or rejecting; investigate before it becomes your RPO.
- `cairn_writer_queue_depth` — metadata write pressure (a saturated writer slows everything).

Suggested alerts: `cairn_replication_lag_seconds` above your RPO budget for N minutes;
`cairn_replication_unreplicated > 0` sustained; any increase in `cairn_replication_failed_total`.

## 4. Recovery procedures

### 4.1 Metadata DB lost or corrupt, blobs intact

1. Stop the node.
2. Restore the most recent DB backup into place (`backup-restore.md`, database-first restore).
3. Run `cairn integrity --repair`: it cross-checks rows against blobs and **drops exactly the dangling
   rows** (metadata referencing a blob that no longer exists), reporting the count. Orphan blobs (bytes
   with no row) are reclaimed by reconciliation.
4. Start the node and verify a sample of objects `GET` back byte-identical.

Any object written *after* the restored backup but whose blob still exists on disk is recoverable;
rows lost since the backup are gone unless also present on a replica.

### 4.2 Whole node lost — promote a replication destination

If a destination bucket has been receiving replicas, it holds every object that shipped before the
loss (RPO = the lag at that moment). "Promotion" is operational, not a command:

1. Confirm the destination node is healthy and has drained (its own `cairn_replication_*` if it
   re-replicates; otherwise just that it is serving).
2. **Re-point clients** (DNS/endpoint/load-balancer) at the destination's S3 endpoint. The destination
   is already a full Cairn node — it serves reads and writes immediately.
3. If the destination should now replicate onward (e.g. to a new third site), configure its
   replication target via the management API (`operations.md` §2, Replication targets).
4. When the original site returns, treat it as a fresh destination and **resync** from the new primary
   (management API existing-object backfill) before considering any switch-back — do not assume its
   stale state is safe to serve.

What crosses to the destination: object data and versions, delete markers, and (when enabled)
object ACLs via the replica-ACL header. Confirm bucket-level config (lifecycle, CORS, notification
endpoints, Object Lock defaults) on the destination — these are configured per node, not replicated.

### 4.3 Whole node lost — restore from an off-box backup (no live replica)

Restore the DB backup and the blob tree to a fresh node per `backup-restore.md`, run
`cairn integrity --repair`, then start and verify. RPO is the age of the backup. This is the minimum
viable DR posture and is sufficient for a backup-target deployment.

### 4.4 Detected blob corruption (bit-rot)

The scrub re-reads stored blobs and raises `cairn_scrub_corruption_total` on an ETag mismatch
(`operations.md`, scrub interval `CAIRN_SCRUB_INTERVAL_SECS`). For the affected object, re-fetch the
good copy from a replica or backup and re-`PUT` it; the corrupt blob is superseded and reclaimed.

## 5. Pre-disaster checklist

- **3-2-1**: at least one copy off the box — an async replication destination, an off-box DB+blob
  backup, or both. Replication is *not* backup (it faithfully propagates a bad delete); backups +
  versioning + Object Lock are what protect against logical errors and ransomware.
- **Test the restore**, not just the backup — a backup you have never restored is a hope, not a plan.
  The `conformance/backup_restore.sh` harness exercises the full backup → corrupt → restore →
  `integrity --repair` → byte-identical-verify loop.
- **Monitor replication lag and `unreplicated`** so you know your live RPO at all times.
- For irreplaceable data, enable **versioning + Object Lock** so an accidental or malicious delete is
  recoverable even within a single node.
