# Cairn

**A production-grade, S3-compatible object store you self-host — one static binary, your filesystem,
your data.**

Cairn speaks the S3 API, stores object bytes as plain files on a POSIX filesystem, and keeps all
metadata in an embedded SQLite database. There is nothing else to run: no external database, no
clustering layer, no separate storage daemon to babysit. Point any S3 client at it, or use the
built-in web console. It ships as a single static binary (or a distroless container) for people who
want their own object storage instead of paying for S3/R2 — on a homelab box, a VPS, or a small
production node.

## Why Cairn

- **Genuinely S3-compatible.** Buckets and objects with byte-range and conditional reads, checksums,
  multipart upload, copy, bulk delete, and v1/v2 + version listings; versioning, tagging, CORS,
  bucket policies, lifecycle rules, object lock (WORM), presigned URLs, and SigV4 (header + streaming
  chunked). Real AWS SDKs — `aws` CLI, boto3, the language SDKs — just work against it.
- **A real admin console.** A clean, fast web UI on its own port to create and configure buckets,
  browse/preview/upload/download/share objects, mint S3 access keys scoped by an access policy, and
  watch storage, compression, and replication at a glance — without reading IAM documentation. The
  console is the reason to pick Cairn over a bare storage daemon.
- **Durable and crash-consistent by design.** Every write is staged, fsynced, atomically renamed into
  place, and acknowledged only after a single metadata commit; on restart Cairn reconciles
  automatically, reclaiming any orphaned blob with no manual intervention. Acknowledged writes survive
  power loss by default.
- **Secure at rest and on the wire.** Native TLS (rustls), AES-256-GCM envelope encryption of every
  secret at rest with online master-key rotation, optional per-bucket SSE-S3 for object data, and a
  full authorization model — bucket policy, ACLs, Block Public Access, and Object Ownership.
- **Efficient.** Transparent per-bucket block compression, range reads that touch only the blocks
  they need, and a metadata engine tuned around a single group-committing writer plus a WAL reader
  pool.
- **Operable.** Prometheus metrics, liveness/readiness endpoints, structured logs, asynchronous
  bucket replication to a second node (or any S3 endpoint) for cross-host redundancy, and a CLI for
  bootstrap, config validation, integrity checking, and backup/restore.

## Quickstart

Build the image from the repo and run it (the result is a single static binary on a distroless base):

```sh
docker build -t cairn .

docker run -d --name cairn \
  -p 7373:7373 -p 7374:7374 \
  -v /srv/cairn:/data \
  -e CAIRN_DATA_DIR=/data -e CAIRN_DB_PATH=/data/cairn.db \
  -e CAIRN_MASTER_KEY="$(openssl rand -hex 32)" \
  cairn serve
```

- **S3 API** on `:7373` — point any S3 client at `http://localhost:7373`.
- **Web console** on `:7374` — open `http://localhost:7374` in a browser.
- Default login is `cairn` / `cairnadmin` — override with `CAIRN_ROOT_ACCESS_KEY` /
  `CAIRN_ROOT_SECRET_KEY` before exposing a node. Liveness is at `/healthz`, readiness at `/readyz`,
  and Prometheus metrics at `/metrics` (all on the S3 port).

Smoke-test the S3 API with the AWS CLI (configure it with the access key / secret above; any region):

```sh
aws --endpoint-url http://localhost:7373 s3 mb s3://demo
echo 'hello cairn' | aws --endpoint-url http://localhost:7373 s3 cp - s3://demo/hi.txt
aws --endpoint-url http://localhost:7373 s3 cp s3://demo/hi.txt -        # -> hello cairn
```

> **Set this in production.** `CAIRN_MASTER_KEY` is a 32-byte hex key that seals every secret at rest.
> Generate it once, keep it constant for the life of your data, and keep it **out of the backup** that
> contains the database. Without it, a fixed insecure development key is used.

Prefer not to use Docker? Cairn is a normal Rust workspace — build the `cairn` binary from source and
run `cairn serve` with the same environment. See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the
toolchain and developer workflow.

## Configuration

Configuration is **environment-only**: every setting is a `CAIRN_*` variable, validated on startup
(`cairn validate-config`). There is no config file and there are no flags. The common knobs:

| Variable | Default | Purpose |
|---|---|---|
| `CAIRN_DATA_DIR` | `./data` | Root of staging + per-bucket object files |
| `CAIRN_DB_PATH` | `./data/cairn.db` | SQLite metadata file (same filesystem as the data) |
| `CAIRN_MASTER_KEY` | *(dev key)* | 32-byte hex key sealing secrets at rest — **set in production** |
| `CAIRN_LISTEN_ADDR` | `0.0.0.0:7373` | S3 API listener |
| `CAIRN_UI_ADDR` | `0.0.0.0:7374` | Console + management API; set `off` to run headless |
| `CAIRN_REGION` | `us-east-1` | Region label + SigV4 scope |
| `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH` | unset | Enable built-in TLS when both are set |
| `CAIRN_MASTER_KEY_RING` | unset | Key ring for online master-key rotation |

The full reference for every variable is in [`docs/configuration.md`](./docs/configuration.md). CLI
subcommands: `cairn serve` (default), `validate-config`, `bootstrap`, `integrity [--repair]`,
`backup` / `restore`.

## Deploying and operating

- **Two listeners.** Expose the S3 port (`:7373`) to clients; keep the console + management port
  (`:7374`) on a trusted interface, firewalled, or disabled (`CAIRN_UI_ADDR=off`). Never expose the
  plaintext interface to an untrusted network.
- **TLS.** Terminate TLS at Cairn (set the cert/key paths; `SIGHUP` reloads) or at a reverse proxy in
  front — both are first-class shapes.
- **Redundancy.** Put the data filesystem on redundant storage (ZFS/RAID/replicated block volume);
  Cairn does not implement drive RAID. For cross-host redundancy, run a second node and turn on
  **bucket replication** to it.
- **Backups.** Snapshot database-first, then blobs, with `cairn backup` / `cairn restore` — see
  [`docs/backup-restore.md`](./docs/backup-restore.md).

Operator guides: [`docs/operations.md`](./docs/operations.md) (config, bootstrapping, day-two signals,
master-key rotation), [`docs/deployment-kubernetes.md`](./docs/deployment-kubernetes.md),
[`docs/upgrade-rollback.md`](./docs/upgrade-rollback.md),
[`docs/scaling-limits.md`](./docs/scaling-limits.md), and
[`docs/troubleshooting.md`](./docs/troubleshooting.md).

## Scope — what Cairn is and isn't

Cairn is **single-node by design**: one process, one data filesystem, one metadata database. That is
what makes it simple to run, fast, and easy to reason about. Cross-host and cross-site redundancy come
from **asynchronous bucket replication** (eventually consistent, with observable lag), not from
clustering. Cairn does not implement drive redundancy — that is delegated to the storage beneath it —
and it is not a multi-region distributed store. SSE-S3 object encryption is supported; SSE-KMS is out
of scope. For a homelab or a small-to-mid production workload that wants the S3 API and a real console
without the operational weight of a distributed system, that trade is the whole point.

## Documentation

The engineering specification and operator guides live in [`docs/`](./docs) (start at
[`docs/CLAUDE.md`](./docs/CLAUDE.md) for the index). The [`docs/s3-api-matrix.md`](./docs/s3-api-matrix.md)
lists exactly which S3 operations are supported, partial, or out of scope.

## License & policies

Apache-2.0 ([`LICENSE`](./LICENSE)). Security policy: [`SECURITY.md`](./SECURITY.md). Contributing and
the developer build/test workflow: [`CONTRIBUTING.md`](./CONTRIBUTING.md).
