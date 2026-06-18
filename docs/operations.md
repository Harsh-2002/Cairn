# Cairn Operations Guide

This guide covers configuring, deploying, and operating a Cairn node. Cairn is a single-node,
locally-backed, S3-compatible object store: one process, one data filesystem, one SQLite
metadata file. Cross-host redundancy comes from asynchronous bucket replication, not clustering.

## 1. The one-filesystem invariant

The SQLite database file, the staging directory, and the per-bucket blob directories **must
reside on the same filesystem**, because the commit protocol relies on atomic `rename(2)`,
which only works within a filesystem. The staging area lives inside the data root by design.
Violating this (e.g. mounting staging elsewhere) breaks the durability protocol.

Place the data filesystem on **redundant storage** — Cairn does not implement drive redundancy.
A checksumming, redundant filesystem (ZFS) gives redundancy *and* silent-corruption detection;
software/hardware RAID or a cloud block volume with provider redundancy are also fine.

## 2. Configuration

Configuration is **environment-only**: built-in defaults overlaid with `CAIRN_*` environment
variables. There is no configuration file and no `--config` flag — a Cairn host or container is
configured purely by env. It is validated on load; an invalid configuration fails fast. Validate
without starting: `cairn validate-config`.

| Setting | Env var | Default | Meaning |
|---|---|---|---|
| Listen address | `CAIRN_LISTEN_ADDR` | `127.0.0.1:9000` | Where the server binds. |
| Data directory | `CAIRN_DATA_DIR` | `./data` | Root of staging + per-bucket blobs. |
| Database path | `CAIRN_DB_PATH` | `./data/cairn.db` | SQLite metadata file (same FS as data). |
| Region | `CAIRN_REGION` | `us-east-1` | Location label + SigV4 scope. |
| Master key | `CAIRN_MASTER_KEY` | *(dev key)* | 32-byte hex; AEAD key for secrets at rest. **Set in production.** |
| TLS cert / key | `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH` | unset | Enable built-in TLS when both set. |
| Max object size | `CAIRN_MAX_OBJECT_SIZE` | 5 TiB | Hard per-object ceiling. |
| Concurrency limit | `CAIRN_CONCURRENCY_LIMIT` | 1024 | Max in-flight requests. |
| Request timeout | `CAIRN_REQUEST_TIMEOUT_SECS` | 300 | Per-request timeout. |
| Log level / format | `CAIRN_LOG_LEVEL` / `CAIRN_LOG_FORMAT` | `info` / `text` | Verbosity; `text` or `json`. |
| Dev auth | `CAIRN_DEV_AUTH` | `false` | Loopback-only auth bypass (debug builds only). |

> **Master key.** SigV4 secrets and replication credentials are envelope-encrypted under this
> key. Supply it out of band; **keep it out of the backup** that contains the database, so the
> backup alone cannot disclose secrets. Without it, a fixed insecure development key is used.

### Replication targets

The replication worker ships outbox entries to one or more S3-compatible destinations (§20).

* **Single target** (node→node) — set `CAIRN_REPLICATION_ENDPOINT`, `CAIRN_REPLICATION_ACCESS_KEY`,
  and `CAIRN_REPLICATION_SECRET` (optionally `CAIRN_REPLICATION_DEST_BUCKET`,
  `CAIRN_REPLICATION_REGION`). Each source bucket's *destination bucket* is resolved from its own
  replication rule; the endpoint and credentials are shared.

* **Multiple targets** — set `CAIRN_REPLICATION_TARGETS` to a JSON array of named destinations,
  each with its own endpoint, credentials, and TLS trust. Each source bucket routes to the target
  whose `dest_bucket` (or `name`) matches its replication rule; a bucket matching no target falls
  back to the single-target keys above when present.

  ```json
  [
    { "name": "west", "endpoint": "https://s3.west.example", "region": "us-west-2",
      "dest_bucket": "mirror-west", "access_key": "AK...", "secret": "...",
      "ca_path": "/etc/cairn/west-ca.pem" },
    { "name": "lab", "endpoint": "https://s3.lab.internal", "region": "us-east-1",
      "dest_bucket": "mirror-lab", "access_key": "AK...", "secret": "...",
      "insecure_skip_verify": true }
  ]
  ```

  Per-target TLS trust for an `https://` endpoint: `ca_path` trusts a private CA's PEM bundle
  instead of the public webpki roots; `insecure_skip_verify` disables certificate verification
  entirely (**dangerous** — testing only, and logged loudly). The two are mutually exclusive.

## 3. Bootstrapping

On a fresh store, create the first administrator (one-time, loopback-local):

```sh
cairn bootstrap        # prints Bearer + SigV4 credentials ONCE — save them
```

It refuses to run once any user exists. The credentials are shown only once; afterward only the
Bearer hash and the encrypted SigV4 secret remain.

## 4. Deployment shapes

Two first-class shapes:

1. **Standalone with built-in TLS.** Set `CAIRN_TLS_CERT_PATH`/`CAIRN_TLS_KEY_PATH`; Cairn
   terminates TLS itself using rustls (aws-lc-rs).
2. **Behind a terminating reverse proxy** on a trusted interface. The proxy must pass the
   authorization, range, conditional, and S3-specific headers through unchanged and must
   **stream** rather than buffer large bodies (otherwise Cairn's backpressure is defeated). Set
   `CAIRN_PUBLIC_BASE_URL` for correct generated URLs behind ingress.

Never expose the plaintext interface to an untrusted network.

The release artifact is one fully static binary (`musl`) containing the server, the management
UI, and the CLI; it runs in a `scratch`/distroless container.

## 5. Day-two operations

Liveness `/healthz`, readiness `/readyz` (ready only after migrations + reconciliation),
Prometheus metrics `/metrics`. Signals to watch:

- **write-queue depth** — the single-writer ceiling; growth under load means small-object writes
  are the binding constraint (enlarge the group-commit linger, relax the synchronous setting if
  the durability trade is acceptable, or scale out with replication).
- **WAL size** — a log growing without bound indicates long-lived readers starving the
  checkpointer.
- **reconciliation counts** at startup — integrity.
- **out-of-space (507) rate** vs capacity.
- **replication lag and failures** — the health of redundancy.

## 6. Durability guarantee

Cairn guarantees that after any crash it converges, with no manual intervention, to a state
where every visible metadata row references a present, durable blob and no orphan remains. The
commit sequence is: stream to staging → fsync the file → rename into place → **fsync the
directory** → validate hashes → commit the metadata transaction (the single linearization
point) → reclaim superseded blobs. A write is acknowledged only after its metadata commit is
durable. Drive-failure survival is delegated to the storage layer; host-failure survival comes
from bucket replication.

See [`backup-restore.md`](./backup-restore.md) for the backup procedure and its consistency
argument.

## 7. Master-key rotation

Secrets at rest — per-object SSE-S3 data keys, users' SigV4 secrets, and replication-target
secrets — are sealed under a 32-byte master key. A single key is fine for most deployments
(`CAIRN_MASTER_KEY`, 64 hex chars). To rotate the master key without re-encrypting object data,
use a **key ring** and follow this procedure. The whole flow is online; no downtime and no
re-upload of objects.

**Config.** A ring is a JSON array set in `CAIRN_MASTER_KEY_RING`, replacing `CAIRN_MASTER_KEY`:

```
CAIRN_MASTER_KEY_RING=[{"id":1,"key":"<64-hex old key>"},{"id":2,"key":"<64-hex new key>"}]
CAIRN_MASTER_KEY_ACTIVE_ID=2          # which id NEW seals use (default: the highest id)
CAIRN_KEY_REWRAP_INTERVAL_SECS=300    # background re-wrap cadence (0 disables); sqlite backend only
CAIRN_KEY_COUNTER_SYNC_SECS=60        # active-key seal-count flush cadence
```

Each `id` is a small integer (1–65535); keys are never logged. New seals always use the active
id; every other id in the ring stays available to **open** existing data, so nothing becomes
unreadable when you add a key.

**Procedure (rotate id 1 → id 2):**

1. **Add the new key, keep the old.** Deploy with the ring above (`id:1` + `id:2`, active `2`).
   New writes seal under id 2; all existing id-1 data still opens. (A single-key deployment that
   has never used a ring is just `[{"id":1,"key":"<current CAIRN_MASTER_KEY>"}]`.)
2. **Let re-wrap run.** A background worker re-seals existing secrets onto the active key,
   resumably and idempotently. It never deletes or rewrites data it cannot open; it only re-seals.
3. **Wait for completion.** Poll the admin endpoint:

   ```
   GET /api/v1/system/crypto-status        # Bearer admin token
   ```

   It reports the active id, the seal count vs the 75%/95% thresholds, per-key state (with a short
   non-reversible key-hash, never key material), per-stream re-wrap completion, and a
   `retire_eligible` flag per key. **Wait until the old key shows `retire_eligible: true`** (every
   stream re-wrapped onto the active key, on every shard, with no failures).
4. **Retire the old key.** Remove `id:1` from `CAIRN_MASTER_KEY_RING` and redeploy.

> **Do not remove a key before it is `retire_eligible`.** Startup enforces a **retire-gate**: if a
> removed key still has data sealed under it, the server **refuses to start** with a diagnostic
> naming the key id and shard, rather than booting into unreadable data. Restore the key to the
> ring, wait for re-wrap to finish, then retire it.

**Seal-count bound.** Each key uses fresh random 96-bit GCM nonces; the active key's seal count is
tracked (and survives restarts). At 75% of the safe ceiling the server logs a "rotate soon"
warning; at 95% it refuses *new* seals (opens are never blocked) — rotate before then.

**Backend note.** Automatic re-wrap and the durable seal counter are implemented for the default
`sqlite` backend. The libSQL/Turso backends can rotate and read all data (old keys still open
everything), but do not auto-re-wrap or persist the counter, so the retire-gate and
`retire_eligible` are not available there — keep retired keys in the ring on those backends.
