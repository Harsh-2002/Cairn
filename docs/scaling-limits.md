# Scaling, limits, and capacity planning

> Operator guide. Numbers below are from the harnesses in [`benchmarks.md`](./benchmarks.md) and the
> performance engineering in [`testing-performance.md`](./testing-performance.md) (§30). They are
> shape-and-ceiling figures for sizing decisions, not SLAs — re-measure on your hardware.

Cairn is a **single-node** object store: one process, one filesystem, an embedded SQLite metadata
store fronted by a single group-committing writer and a pool of read connections. That design fixes
where it scales easily and where it has a ceiling. This guide states the limits and the levers.

## 1. The headline ceiling: metadata write rate (single writer)

Every mutation (object create, delete, tag, multipart step) commits through **one** serialized,
group-committing writer thread. That is the binding throughput limit for **small-object, write-heavy**
workloads:

- Measured small-object metadata commit rate on a 2-vCPU host with the database on a real disk:
  **~13k commits/s** with `CAIRN_META_SYNCHRONOUS=full` (per-commit fsync, the default and safest),
  rising to **~37k commits/s** with `=normal` (no per-commit fsync under WAL) at 64 concurrent
  writers — ~2.8×. Reads do not go through the writer and scale with the read pool + page cache.
- Concurrency past ~4 writers raises latency far more than throughput (e.g. ~165→275 ops/s from
  concurrency 1→16, while p999 grows roughly 18→198 ms): the writer's fsync rate, not CPU, saturates.

**The signal to watch:** `cairn_writer_queue_depth` (Prometheus). A persistently rising queue depth
under load means you are at the write ceiling. Throughput-of-bytes (large objects) is a different
regime — bounded by disk/network, not the writer.

### Levers, in order of preference

1. **`CAIRN_META_SYNCHRONOUS=normal`** — ~2.8× the small-write rate. Safe **only** if your filesystem
   is on battery-backed/redundant storage (it relaxes the per-commit fsync; a power loss can lose the
   last few commits). Keep `full` (default) on commodity disks.
2. **Group-commit linger** (`CAIRN_META_GROUP_COMMIT_LINGER_MICROS`, capped at 1 ms) — coalesces
   bursty writers into fewer fsyncs; only helps under `synchronous=full`.
3. **Metadata sharding** (`CAIRN_META_SHARDS=N`) — the only way past the single-writer ceiling.
   Buckets are hash-partitioned across N independent writers, so **disjoint buckets commit in
   parallel**. See §3 — this is a one-time, init-locked decision with a real trade-off.
4. **Replication for read scaling / geo** — see §4.

## 2. Object and bucket limits

- **Max object size:** configurable via `CAIRN_MAX_OBJECT_SIZE` (default 5 TiB). Large objects stream
  to disk with bounded memory; the cost is disk bandwidth, not the writer. Multipart uploads assemble
  parts during a single staging pass.
- **Per-bucket object count:** there is no hard cap, but list/copy/tag operations are index-backed
  SQLite queries — latency grows with the number of versions under a bucket/prefix. For very large
  buckets, paginate listings (the API is always paged) and prefer prefix-scoped queries. If a single
  bucket dominates a sharded deployment, all its load lands on one shard (see §3).
- **Blobs per directory:** bounded by the on-disk layout (`storage-durability.md` §9); not an
  operator concern at homelab/small-prod scale.

## 3. When to shard — and the trade-offs

Set `CAIRN_META_SHARDS=N` **at first init** (it cannot be changed later — see
[`upgrade-rollback.md`](./upgrade-rollback.md) §4; the bucket→shard hash is fixed). Choose sharding
when:

- You expect sustained small-object write rates **above the single-writer ceiling** for your
  `synchronous` mode (watch `cairn_writer_queue_depth` on a single-shard pilot), **and**
- Your write load spreads across **many buckets** (sharding partitions by bucket name; a single
  hot bucket still hits one writer — sharding does not help a single-bucket hotspot).

Trade-offs to accept:

- **Per-user quota becomes eventually-consistent.** A user's byte quota can span buckets on different
  shards, so it is checked per-shard rather than atomically — fine for rate-limiting, not for strict
  accounting. Single-shard (the default) keeps quotas exact.
- **Cross-shard reads fan out and merge** (list-buckets, account aggregates, tag browsing) — a small
  constant cost.
- **No re-sharding.** Pick N for your 12–24-month ceiling; to change it, migrate to a new node.

Rule of thumb: homelab and most small-prod deployments run **single-shard** (atomic quotas, simplest
ops). Reach for sharding only when a single-shard pilot demonstrably saturates the writer.

## 4. Replication for redundancy and read scaling

Bucket replication ([`replication.md`](./replication.md)) is **asynchronous, at-least-once** with
observable lag — not synchronous redundancy. Use it for a second-site copy, read offload, or
zero-downtime upgrades (drain to the peer first). Capacity notes:

- Lag is visible as `cairn_replication_unreplicated` (pending + in-flight + terminally-failed) and the
  oldest-pending age. A growing lag under sustained writes means the worker pool or the destination is
  the bottleneck; tune `CAIRN_REPLICATION_WORKER_CONCURRENCY` / batch size, or check the target.
- A target that is down does not consume the retry budget — work stays pending and ships when it
  returns. Watch `cairn_replication_unreplicated` so a dashboard never reads "healthy" while objects
  are owed.
- **ACLs are replicated** via an admin-gated header; bucket policy / public-access-block / ownership
  are **not** replicated — set those on each destination.

## 5. The replica-ACL wire header (size note)

Replicated object ACLs travel as a base64-encoded, admin-gated `x-amz-meta-cairn-replica-acl` header.
ACLs are tiny in practice; a pathological object with a very large grant list could produce a header
that an intermediary proxy rejects. If you front replication with a proxy, keep its header-size limit
generous, or keep ACLs small (prefer bucket policy for broad rules).

## 6. WAL growth under long-lived readers

The metadata WAL is checkpointed by a background loop. A reader that holds a snapshot open for a long
time (e.g. a very long-running list against a high-write bucket) pins the checkpoint, so the WAL grows
until the reader releases. This is bounded and self-corrects, but if you see the WAL file growing
without bound, look for a stuck long-lived read connection.

## 7. A sizing cheat-sheet

| Workload | Default single-shard, `synchronous=full` | When to change |
|---|---|---|
| Homelab backups / media (rclone, restic) | Comfortable | — |
| Small-team app assets / CI artifacts | Comfortable | Watch `cairn_writer_queue_depth` under CI bursts |
| Write-heavy, many buckets, > writer ceiling | At the ceiling | `synchronous=normal` (if storage is safe) → then shard |
| Need a second-site copy / read offload | — | Add bucket replication |
| Strict per-user quotas | Use single-shard | Sharding relaxes quota to eventual |
