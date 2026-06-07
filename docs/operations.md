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

Configuration layers, highest precedence first: **command-line flags → environment variables
(`CAIRN_*`) → optional TOML file (`--config`) → built-in defaults.** It is validated on load;
an invalid configuration fails fast. Validate without starting: `cairn validate-config`.

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
