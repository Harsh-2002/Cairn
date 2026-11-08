# Cairn

Self-hosted, S3-compatible object storage with an embedded web console.

Cairn runs as one binary. It stores each object as durable files on the local filesystem — a
plaintext object is one file, its payload followed by a metadata trailer — and keeps its index in
embedded SQLite, rebuildable from those files. No external database or storage service to operate.
It is designed for a single node; asynchronous bucket replication provides copies on other hosts.

[![Cairn web console](https://harsh-2002.github.io/Cairn/screenshots/overview-dashboard.webp)](https://harsh-2002.github.io/Cairn/)

## What it supports

- **S3:** multipart uploads and part reads, range reads, checksums, versioning, presigned URLs,
  tagging, CORS, lifecycle expiration and incomplete-upload abort, Object Lock and per-bucket object
  TTL. See the [S3 compatibility matrix](docs/s3-api-matrix.md).
- **Console:** manage buckets, users and policies; upload, preview and share objects; inspect usage,
  metrics, replication and import jobs.
- **Security:** native TLS, bucket policies, scoped temporary credentials, SSE-S3 and transparent
  object encryption at rest (`CAIRN_ENCRYPT_AT_REST=true`). The `aws:kms` interface uses key labels
  backed by the node's master-key ring; it does not integrate with an external KMS or provide
  independent cryptographic isolation between key IDs.
- **Storage and operations:** block compression, durable writes, asynchronous replication,
  webhooks, S3 import, quotas, object expiry, integrity checks, backup/restore and Prometheus
  metrics.

## Install

Requires Linux 5.6+ on x86_64 or aarch64 with `openat2` support and a local filesystem that
supports durable file and directory synchronization. XFS is recommended; btrfs also supports
reflink completion, and ext4 works through the streaming-copy path. Docker uses the host's kernel.

The installer sets up Docker or a host service, generates credentials and a master key, and can
configure TLS. Running it again updates the installation:

```sh
curl -fsSL https://raw.githubusercontent.com/Harsh-2002/Cairn/main/install.sh | sudo sh
```

See the [operations guide](docs/operations.md) for deployment details and
[release verification](SECURITY.md#verifying-release-artifacts) for signatures, checksums and provenance.

## Try it locally with Docker

Create a master-key file **once** and keep it with this installation. The command refuses to
replace an existing file:

```sh
(umask 077; set -C; printf 'CAIRN_MASTER_KEY=%s\n' "$(openssl rand -hex 32)" > cairn.env)

docker run -d --name cairn \
  -p 127.0.0.1:7373:7373 -p 127.0.0.1:7374:7374 \
  -v cairn-data:/data --env-file ./cairn.env \
  ghcr.io/harsh-2002/cairn:latest
```

Open [the console](http://localhost:7374) and sign in with the local-development credentials
`cairn` / `cairnadmin`. The S3 endpoint is `http://localhost:7373`.

With the AWS CLI installed, try an upload and download:

```sh
export AWS_ACCESS_KEY_ID=cairn AWS_SECRET_ACCESS_KEY=cairnadmin AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:7373 s3 mb s3://demo
printf 'hello cairn\n' | aws --endpoint-url http://localhost:7373 s3 cp - s3://demo/hi.txt
aws --endpoint-url http://localhost:7373 s3 cp s3://demo/hi.txt -
```

Reuse `cairn.env` whenever you recreate the container with `cairn-data`. Keep the master key
outside the database backup and out of version control. For network deployment, set
`CAIRN_ROOT_ACCESS_KEY` and `CAIRN_ROOT_SECRET_KEY` before first startup, configure TLS, and restrict
access to the management port. Follow the [operations guide](docs/operations.md).

## Configuration and operation

Cairn reads `CAIRN_*` environment variables; `cairn validate-config` checks them before startup.
Docker's `--env-file` supplies those variables; Cairn itself does not read a configuration file.

| Variable | Default | Purpose |
|---|---|---|
| `CAIRN_API_ADDR` | `0.0.0.0:7373` | S3 + native management listener |
| `CAIRN_CONSOLE_ADDR` | `0.0.0.0:7374` | Console, browser API and download links; `off` leaves native API available |
| `CAIRN_DATA_DIR` | `./data` | Object storage root |
| `CAIRN_DB_PATH` | `./data/cairn.db` | SQLite metadata/index database |
| `CAIRN_MASTER_KEY` | Development key | Set a persistent 32-byte hex master key |
| `CAIRN_ENCRYPT_AT_REST` | `false` | Encrypt newly written objects without requiring client SSE headers |
| `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH` | Unset | Native TLS certificate and private key |

The container sets its data root and database to `/data` and `/data/cairn.db`.
Health (`/healthz`), readiness (`/readyz`) and metrics (`/metrics`) use the S3 port.

- [All configuration settings](docs/configuration.md)
- [Backup and restore](docs/backup-restore.md) · [Upgrades and rollback](docs/upgrade-rollback.md)
- [Replication](docs/replication.md) · [Import from another S3 store](docs/migration.md)
- [Troubleshooting](docs/troubleshooting.md) · [Scaling limits](docs/scaling-limits.md)

Replication is asynchronous and can lag. Use backups and appropriate underlying disk redundancy
for your durability requirements.

## Build and contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, a local build/run walkthrough and the
required checks. The React console requires Node 26.13.0 or newer; build it before the Rust binary
so it is embedded correctly.

The [engineering specification](docs/CLAUDE.md) describes the architecture and invariants;
[CONTRACT.md](CONTRACT.md) records architectural constraints.
[Open issues](https://github.com/Harsh-2002/Cairn/issues) track work and bug reports.

## Performance and planned work

[Benchmarks and reproduction commands](docs/benchmarks.md) include workload, hardware and
measurement limits. At strict durability on the shared reference host, interleaved paired runs put
Cairn at 1.73× RustFS on 4 KiB PUT, 1.07× on 1 MiB PUT (parity), 1.64× on 1 MiB GET, 5.13× on a
1,000-key LIST, 1.08× on the scored multipart upload, 19.95× on the bulk unversioned DELETE, 16.53×
on the single-key unversioned DELETE and 32.49× on the equal-durability versioned DELETE. RustFS
1.0.2+ journals deletes under strict mode, so the delete rows are no longer the 1.0.0
as-configured case. The host is shared, so the ratio is the signal; see
[the RustFS 1.0.4 scoreboard](docs/benchmarks.md#156-rustfs-104-paired-scoreboard-2026-11-08).
Optional io_uring writes, plaintext sendfile and kTLS encryption offload are experimental and
disabled in standard builds; see
[the I/O reference](docs/data-plane.md#76-the-read-data-path-and-zero-copy).

## Stability

Current status: **1.0** for a single node with backups. The 24-hour correctness run passed with
1,697,128 PUTs, 0 errors and 0 replication mismatches; after the replication-metrics fix, the 2-hour
anonymous-RSS gate passed (155.6 → 153.5 MiB, −1.3%). The full scoreboard, upgrade/rollback drill,
replication baseline, abrupt-process-kill drill and independent review are recorded in
[the stability record](docs/stability-2026-11-05.md). The supported deployment scope and known
limits are in [the support statement](docs/support.md).

[Planned work](docs/delivery.md#32-phased-implementation-roadmap) includes external KMS integration,
remote cold-tier transition/restore and zero-copy HTTPS reads.

## License

[Apache-2.0](LICENSE). See [governance](GOVERNANCE.md), the [security policy](SECURITY.md) and the
[code of conduct](CODE_OF_CONDUCT.md).

### API and console endpoints

Use `http://localhost:7373` for both S3 clients and Cairn administration. The console at
`http://localhost:7374` has its own session API and public download links. Set
`CAIRN_CONSOLE_ADDR=off` for API-only operation. Closing the console port does not restrict
native administrator access on the API port.

Behind a reverse proxy, set both `CAIRN_API_PUBLIC_URL` and `CAIRN_CONSOLE_PUBLIC_URL` to distinct
HTTP(S) origins. See [deployment examples](docs/operations.md#api-and-console-ingress) and the
[configuration migration](docs/upgrade-rollback.md#api-and-console-configuration-migration).
