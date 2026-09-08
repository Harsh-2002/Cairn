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
| Metadata DB lost or corrupt, disk otherwise intact | Restore a complete offline SQLite snapshot (database and referenced blobs) |
| Whole node / disk lost | A **replication destination** (warm copy) or an **off-box backup** (3-2-1) |
| Single blob bit-rots on disk | The background **scrub** flags it (`cairn_scrub_corruption_total`); re-fetch from a replica/backup |
| Accidental or malicious overwrite/delete | **Versioning** + **Object Lock/WORM** (replication and RAID do *not* protect against this) |

The metadata DB is the critical single point of failure: lose it without a backup and you lose the
namespace even though the blobs survive. **A tested backup of the DB is non-negotiable.**

## 2. RPO and RTO — set expectations honestly

- **RPO (data you can lose)** is the unreplicated write window when the primary is lost, or the age
  of your last backup if you have no live replica. With async replication, in-flight writes that had not
  yet shipped are absent from the replica. Watch lag together with failed/unreplicated work (§3);
  lag alone cannot bound data loss when delivery has failed.
- **RTO (time to restore service)** = manual. There is no automatic promotion; recovery is a deliberate
  operator action (re-point clients at a destination, or restore a backup to a fresh node). Budget
  minutes, not seconds.

If you need zero-RPO synchronous failover, Cairn is not the right tier for that workload — see the
positioning in `overview.md`. For homelab, edge, dev/staging, small-prod, and backup-target use, the
procedures here give a sound recovery posture.

## 3. Observability — the signals to watch before and during a disaster

Scrape `/metrics` (S3 port). Replication-health gauges (spec: `replication.md` §20.5):

- `cairn_replication_lag_seconds` — age of the oldest pending entry; an indicator of delivery
  delay, not a complete RPO bound when failed work exists.
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

1. Stop the node and any external SQLite/filesystem tools. Preserve the damaged data tree before
   any operation that can reclaim files.
2. Restore the latest complete offline snapshot with `cairn restore`, following
   [`backup-restore.md`](./backup-restore.md). The supported format contains the database and its
   referenced committed blobs and multipart parts; a raw database-only copy is not this format.
3. Confirm restore and its reconciliation pass succeed. If the snapshot carries an interrupted
   storage-baseline hold, restore publishes a fresh held image and explicitly requires
   `cairn storage-baseline <empty-backup-dir>` before startup. Complete that operation; do not
   manually clear its accounting rows. Do not treat `integrity --repair` as an
   automatic recovery step: it removes unprotected rows with missing blobs, cannot recreate bytes,
   and refuses to erase retained or legally held metadata.
4. Start the node, then verify historical version IDs, delete markers, object tags/ACLs/locks, and
   encrypted object reads. Resume a saved multipart upload where one exists and check replication
   backlog recovery before returning clients to the node.

Metadata is the namespace authority. A blob written after the snapshot has no recovered key/version
mapping merely because its bytes survive; reconciliation treats it as an orphan and may reclaim it.
Post-snapshot writes need an independent replica or newer complete backup. The original master-key
ring is also required; snapshots deliberately exclude it. Supported native backup/restore is offline
single SQLite only, never a metadata-engine or shard-count migration.

### 4.2 Whole node lost — promote a replication destination

If a destination bucket has been receiving replicas, it holds every object that shipped before the
loss; failed or unfinished source deliveries remain outside that copy. "Promotion" is operational,
not a command:

1. Confirm the destination is healthy and verify the required object versions. Use the last known
   source backlog/failure state to assess the loss window; the destination's outbound replication
   metrics do not prove it received every acknowledged source write.
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

Restore the complete snapshot to a fresh node per `backup-restore.md`, confirm its built-in
validation and reconciliation succeed, then start and verify. An explicit held-baseline response
requires the resume procedure in Section 4.1 before startup. RPO is the age of the backup. This is the minimum
viable DR posture and is sufficient for a backup-target deployment.

### 4.4 Detected blob corruption (bit-rot)

**The scrub is OFF BY DEFAULT.** `CAIRN_SCRUB_INTERVAL_SECS` defaults to `0`, and at `0` no pass ever
runs — "Cairn has a scrub" and "this node is detecting bit rot" are different claims, and only the
first is true out of the box. If you are relying on corruption detection, set the interval (ARCH 28.2)
and alert on the metrics below. Note also that `cairn integrity [--repair]` is a *reconcile* pass
(orphan blobs, rows with missing blobs) — it never re-reads or re-hashes content, so it is not a
substitute.

When enabled, the scrub re-reads stored blobs and raises `cairn_scrub_corruption_total` on a failed
integrity check (`operations.md`; ARCH 8.6 / 26.4). It covers **encrypted objects as well as
plaintext ones** — an at-rest / SSE-S3 / SSE-KMS version is re-read through its own unsealed data key
and its AES-GCM blocks are authenticated — so a node with `CAIRN_ENCRYPT_AT_REST=true` is scrubbed
like any other. For the affected object, re-fetch the good copy from a replica or backup and re-`PUT`
it; the corrupt blob is superseded and reclaimed.

What the scrub does **not** verify is counted, not hidden. Alert on both series:

- `cairn_scrub_objects_total` — versions fully re-read and hash-verified. **A pass with `scanned = 0`
  and a non-zero skipped count means you have lost coverage, not that the store is clean**; the
  per-pass log line (`scanned`, `skipped`, `corrupt`, `walked`) states the same thing, and
  `scanned + skipped` accounts for every version walked.
- `cairn_scrub_skipped_total{reason="key_unavailable"}` — the version's key is not on the ring right
  now (mid-rotation). Deliberately **not** reported as corruption; it is retried on the next pass. A
  sustained non-zero value means part of the store is going unverified — check the master-key ring
  (`operations.md`, rotation runbook).
- `cairn_scrub_skipped_total{reason="composite_etag"}` — a legacy multipart row lacks both an
  internal ingest SHA-256 and a full-object supplementary checksum. Its composite ETag/checksum
  cannot prove whole-content integrity, so it is read and authenticated but counted as unverified.
  New multipart objects carry an internal digest; legacy FULL_OBJECT checksums are also verified.
  Never generate a trusted baseline by scrubbing old bytes: they may already be corrupt.
- `cairn_scrub_skipped_total{reason="io_error"}` — a transient filesystem failure (fd exhaustion, a
  full or flaky disk) prevented the re-read. Like `key_unavailable`, deliberately **not** reported as
  corruption — the bytes are most likely fine and the condition clears — and retried next pass. A
  missing blob file is a different thing and *is* reported corrupt (`cairn_scrub_corruption_total{kind="missing_blob"}`).
- `cairn_scrub_skipped_total{reason="no_blob"|"delete_marker"|"metadata_unavailable"}` — rows with
  nothing to read.
- `cairn_scrub_enumeration_errors_total{stage="buckets"|"versions"}` — a metadata listing failed, so
  part of the store was not walked at all. A pass that hits one still emits its log line and counters
  (it does not abort silently), but its `scanned`/`skipped` cover less than the whole store.

**Cost on an encrypted node.** The scrub now re-reads *and decrypts* every version, so on a
`CAIRN_ENCRYPT_AT_REST` / heavily-SSE store its per-pass CPU and duration scale with stored bytes
(AES-GCM over the whole dataset) where before it did nearly nothing — the earlier behaviour was the
bug, not a feature. Set `CAIRN_SCRUB_BYTES_PER_SEC` to pace logical bytes (zero, the default,
retains unthrottled behavior). The scrub holds one read permit; buffering and decompression may
read ahead, so pacing is not a physical IOPS limit. A full pass remains I/O- and CPU-heavy. Schedule
`CAIRN_SCRUB_INTERVAL_SECS` for quiet periods and size it above a single pass's duration.

## 5. Pre-disaster checklist

- **3-2-1**: at least one copy off the box — an async replication destination, an off-box DB+blob
  backup, or both. Replication is *not* backup (it faithfully propagates a bad delete); backups +
  versioning + Object Lock are what protect against logical errors and ransomware.
- **Test the restore**, not just the backup — a backup you have never restored is a hope, not a plan.
  The `conformance/backup_restore.sh` harness exercises the full backup → corrupt → restore →
  `integrity --repair` → byte-identical-verify loop, plus encrypted history, protected versions,
  interrupted replication claims, and resumable multipart state.
- **Monitor replication lag and `unreplicated`** to assess the current delivery backlog and loss window.
- For irreplaceable data, enable **versioning + Object Lock** so an accidental or malicious delete is
  recoverable even within a single node.
