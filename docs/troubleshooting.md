# Troubleshooting

> Operator guide. Symptoms → likely cause → what to check (`/metrics`, logs) → fix. Cairn fails
> **closed** by design (a missing key, bad signature, or tampered blob errors rather than returning
> wrong data), so most "errors" are the system refusing to do the wrong thing — the table below tells
> you which.

Cairn exposes Prometheus metrics on the S3 port at `/metrics`, liveness at `/healthz`, and readiness
at `/readyz`. Logs are structured (one line per request with status + latency). Start any
investigation there.

## Quick reference

| Symptom | Likely cause | Check | Fix |
|---|---|---|---|
| `507 Insufficient Storage` on PUT | Disk full (the data filesystem) | `df` on `CAIRN_DATA_DIR`; `cairn_*` disk metrics | Free space / grow the volume. Cairn refuses the write rather than corrupting — no data is lost. |
| `503 Service Unavailable` / `TooManyRequests` | Concurrency limiter shedding load | request-rate metrics; `cairn_writer_queue_depth` | You're at capacity. See [`scaling-limits.md`](./scaling-limits.md): raise the limit if hardware allows, or shard/replicate. `/healthz` + `/readyz` bypass the limiter, so probes stay green. |
| Writes slow, p99 climbing | At the single-writer ceiling | **`cairn_writer_queue_depth`** rising | The metadata write ceiling ([`scaling-limits.md`](./scaling-limits.md) §1). `synchronous=normal` (if storage is safe), group-commit linger, or shard. |
| `403 AccessDenied` on a valid-looking request | Auth/authz refusing (fail-closed) | the request log line; the principal's policy/ACL | Check SigV4 clock skew (presigned expiry), the bucket policy/ACL, Block-Public-Access, and ownership mode. A deactivated/deleted user is denied immediately. |
| `403` deleting an object | Object Lock / legal hold | `GetObjectRetention` / `GetObjectLegalHold` | COMPLIANCE is immutable until expiry (no bypass). GOVERNANCE yields only to `s3:BypassGovernanceRetention` + the bypass header. Release the legal hold first. |
| GET returns `500`/corruption error, not bytes | Bit-rot / blob/metadata mismatch | `cairn_scrub_corruption_total`; logs | The scrub detected an ETag mismatch and refuses to serve wrong bytes. Restore from backup; run `cairn integrity --repair` to drop rows whose blob is gone. |
| Replica is behind / missing objects | Async replication lag or a down target | **`cairn_replication_unreplicated`**, oldest-pending age, per-target gauges | Expected lag under load (eventually consistent). If it grows without bound: check the target is up, then tune `CAIRN_REPLICATION_WORKER_CONCURRENCY` / batch size. A down target keeps its queued work and resumes automatically. |
| `404`/missing object after a crash | Reconciliation in progress, or a genuinely-uncommitted write | startup log; `cairn integrity` output | A *committed* write survives a crash (durability contract). On startup Cairn reconciles (reclaims orphan blobs) before answering `/readyz`. If a row's blob is missing, `cairn integrity --repair` drops the dangling row. |
| Server won't start: master-key error | Wrong/missing `CAIRN_MASTER_KEY[_RING]`, or an incomplete rotation | startup log; `GET /api/v1/system/crypto-status` | Crypto fails closed: the key that sealed the data must be present. If mid-rotation, do not retire the old key until `retire_eligible=true` (see [`operations.md`](./operations.md) §7). |
| WAL file growing without bound | A long-lived reader pinning the checkpoint | the WAL file size next to the DB | Find and close the stuck long-running read connection; the checkpoint resumes ([`scaling-limits.md`](./scaling-limits.md) §6). |
| Console/web console won't load, S3 works | web console listener off or firewalled | `CAIRN_WEB_ADDR`; the second listener bound | The console + management API are on `:7374` by default; `CAIRN_WEB_ADDR=off` runs headless. |
| A phantom bucket name in Metrics | (fixed) console asset miscounted | — | Resolved in current builds; upgrade if you see it. |

## Diagnostics you can always run

- **Integrity / reconciliation:** `cairn integrity` (reclaim orphan blobs) and `cairn integrity --repair`
  (also drop metadata rows whose blob is missing). Safe to run against a stopped node.
- **Config sanity:** `cairn validate-config` checks the `CAIRN_*` environment (and the insecure-public-bind
  guardrail) without starting the server.
- **Schema version:** `cairn migrate` opens the store, runs outstanding migrations, and prints the
  applied schema version.
- **Crypto/rotation status:** `GET /api/v1/system/crypto-status` (admin) reports key-ring state and
  rotation eligibility.
- **Replication health:** `GET /api/v1/replication/summary` (admin) and the `cairn_replication_*`
  metrics.

## When to restore from backup

Restore (per [`backup-restore.md`](./backup-restore.md)) when blobs are lost/corrupt beyond what
`integrity --repair` can reconcile, or when an upgrade went wrong (see
[`upgrade-rollback.md`](./upgrade-rollback.md) §3). Reconciliation reclaims orphans and drops dangling
rows; it cannot recreate bytes that are gone — that's what the snapshot is for.
