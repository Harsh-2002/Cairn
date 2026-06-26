# Cairn

Self-hosted, S3-compatible object storage.

Cairn speaks the S3 API, stores object data as plain files on a normal filesystem, and keeps all
metadata in an embedded SQLite database. There is no external database, clustering layer, or separate
storage daemon to run. Point any S3 client at it, or use the built-in web console. It runs as one
binary, and a distroless container image is also published.

Cairn is for people who want to host their own object storage on a homelab machine, a VPS, or a small
production node, with the S3 API and a console but without operating a distributed system.

## Features

- S3 API: buckets and objects, byte-range and conditional reads, checksums (CRC32, CRC32C,
  CRC64NVME, SHA-1, SHA-256), multipart upload, copy, bulk delete, and v1/v2 plus version listings.
  Versioning, tagging, CORS, lifecycle expiration, object lock (WORM), and presigned URLs. SigV4
  (header and streaming-chunked) and Bearer authentication. The aws CLI and the standard AWS SDKs
  work against it.
- Web console: a UI on its own port to manage buckets and users, browse, upload, download and share
  objects, mint access keys scoped by a policy, and view storage, compression, and replication status.
- Access control: bucket policies, ACLs, Block Public Access, Object Ownership, and short-lived
  (STS-style) credentials.
- Durability: writes are staged, fsynced, atomically renamed, and acknowledged after a single
  metadata commit. On restart Cairn reconciles and reclaims any orphaned data. Acknowledged writes
  survive power loss.
- Security: native TLS, AES-256-GCM encryption of secrets at rest with online master-key rotation,
  and optional per-bucket SSE-S3 for object data.
- Storage efficiency: optional per-bucket block compression, with range reads that touch only the
  blocks they need.
- Operations: Prometheus metrics, liveness and readiness endpoints, structured logs, asynchronous
  bucket replication to another node or S3 endpoint, webhook event notifications, and a CLI for
  bootstrap, config validation, integrity checks, and backup and restore.

## Install

The install script sets Cairn up on a host or with Docker, and updates an existing installation when
you run it again:

```sh
curl -fsSL https://raw.githubusercontent.com/Harsh-2002/Cairn/main/install.sh | sudo sh
```

It detects Docker and offers it (or installs the binary with a systemd or OpenRC service), generates
the master key and admin credentials, can enable TLS, and stores data in a Docker named volume or
under `/var/lib/cairn`. The Docker setup lives in `/opt/cairn` so you can edit the compose file. Run
`sh install.sh --help` for options such as `--docker`, `--host`, `--update`, and `--uninstall`.

## Quickstart

To set it up by hand with Docker instead:

```sh
docker build -t cairn .

docker run -d --name cairn \
  -p 7373:7373 -p 7374:7374 \
  -v /srv/cairn:/data \
  -e CAIRN_DATA_DIR=/data -e CAIRN_DB_PATH=/data/cairn.db \
  -e CAIRN_MASTER_KEY="$(openssl rand -hex 32)" \
  cairn serve
```

- S3 API on `:7373`. Point any S3 client at `http://localhost:7373`.
- Web console on `:7374`. Open `http://localhost:7374` in a browser.
- Default login is `cairn` / `cairnadmin`. Override it with `CAIRN_ROOT_ACCESS_KEY` and
  `CAIRN_ROOT_SECRET_KEY` before exposing a node. Liveness is at `/healthz`, readiness at `/readyz`,
  and Prometheus metrics at `/metrics`, all on the S3 port.

Smoke-test with the AWS CLI (configure it with the access key and secret above, any region):

```sh
aws --endpoint-url http://localhost:7373 s3 mb s3://demo
echo 'hello cairn' | aws --endpoint-url http://localhost:7373 s3 cp - s3://demo/hi.txt
aws --endpoint-url http://localhost:7373 s3 cp s3://demo/hi.txt -
```

`CAIRN_MASTER_KEY` is a 32-byte hex key that seals every secret at rest. Generate it once, keep it
constant for the life of your data, and store it outside the database backup. Without it, a fixed
development key is used, which is not safe for production.

To run without Docker, build the `cairn` binary from source and run `cairn serve` with the same
environment. See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the toolchain.

## Configuration

Configuration is environment-only. Every setting is a `CAIRN_*` variable, validated on startup with
`cairn validate-config`. There is no config file and there are no flags. Common variables:

| Variable | Default | Purpose |
|---|---|---|
| `CAIRN_DATA_DIR` | `./data` | Root of staging and per-bucket object files |
| `CAIRN_DB_PATH` | `./data/cairn.db` | SQLite metadata file (same filesystem as the data) |
| `CAIRN_MASTER_KEY` | dev key | 32-byte hex key sealing secrets at rest; set it in production |
| `CAIRN_LISTEN_ADDR` | `0.0.0.0:7373` | S3 API listener |
| `CAIRN_UI_ADDR` | `0.0.0.0:7374` | Console and management API; set `off` to run headless |
| `CAIRN_REGION` | `us-east-1` | Region label and SigV4 scope |
| `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH` | unset | Enable built-in TLS when both are set |
| `CAIRN_MASTER_KEY_RING` | unset | Key ring for online master-key rotation |

The full reference is in [`docs/configuration.md`](./docs/configuration.md). CLI subcommands:
`cairn serve` (default), `validate-config`, `bootstrap`, `integrity [--repair]`, `backup`, and
`restore`.

## Deploying

- Two listeners. Expose the S3 port to clients, and keep the console and management port on a trusted
  interface, firewalled, or disabled with `CAIRN_UI_ADDR=off`. Do not expose a plaintext interface to
  an untrusted network.
- TLS. Terminate TLS at Cairn (set the cert and key paths; `SIGHUP` reloads) or at a reverse proxy in
  front.
- Redundancy. Put the data filesystem on redundant storage such as ZFS, RAID, or a replicated block
  volume. For cross-host redundancy, run a second node and enable bucket replication to it.
- Backups. Use `cairn backup` and `cairn restore`. See [`docs/backup-restore.md`](./docs/backup-restore.md).

Operator guides: [`docs/operations.md`](./docs/operations.md),
[`docs/deployment-kubernetes.md`](./docs/deployment-kubernetes.md),
[`docs/upgrade-rollback.md`](./docs/upgrade-rollback.md),
[`docs/scaling-limits.md`](./docs/scaling-limits.md),
[`docs/disaster-recovery.md`](./docs/disaster-recovery.md), and
[`docs/troubleshooting.md`](./docs/troubleshooting.md).

## Scope

Cairn is single-node by design: one process, one data filesystem, one metadata database. Cross-host
redundancy comes from asynchronous bucket replication, which is eventually consistent with observable
lag, rather than from clustering. Drive redundancy is left to the storage underneath. SSE-S3 object
encryption is supported; SSE-KMS is not yet. The target is homelab and small-to-mid production
workloads that want the S3 API and a console without running a distributed system.

## Roadmap

Planned work, tracked against the architecture in [`docs/delivery.md`](./docs/delivery.md) (Phase 15).
These are additive and do not change the S3 or management API.

- SSE-KMS object encryption and full-blob encryption at rest (SSE-S3 ships today).
- Lifecycle transition to a remote cold tier, with a restore-from-cold workflow.
- Zero-copy reads with kernel TLS, building on the existing sendfile fast path.
- Signed release artifacts (cosign) and SBOM attestation.

## Documentation

The engineering specification and operator guides are in [`docs/`](./docs); start at
[`docs/CLAUDE.md`](./docs/CLAUDE.md) for the index. [`docs/s3-api-matrix.md`](./docs/s3-api-matrix.md)
lists which S3 operations are supported, partial, or out of scope.

## License

Apache-2.0 ([`LICENSE`](./LICENSE)). The license is permanent; there is no enterprise edition or
open-core split. Governance and maintenance are described in [`GOVERNANCE.md`](./GOVERNANCE.md). See
[`SECURITY.md`](./SECURITY.md) for the security policy, [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
for community expectations, and [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the developer workflow.

Releases are CI-gated and publish static `linux/amd64` and `linux/arm64` binaries with a `SHA256SUMS`
manifest, plus a multi-arch image at `ghcr.io/harsh-2002/cairn`. Verify a download with
`sha256sum -c SHA256SUMS`.
