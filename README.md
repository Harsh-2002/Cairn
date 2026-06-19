# Cairn

A production-grade, **S3-compatible object storage server** written from scratch in pure Rust.
Object bytes are plain files on a POSIX filesystem; all metadata lives in an embedded SQLite
database (the single source of truth). Ships as **one static binary**.

- **S3 API** — buckets and objects (ranges, conditionals, checksums, SSE-S3), multipart, copy,
  bulk delete, listing v1/v2 + versions, versioning, tagging, CORS, policies, lifecycle, and
  replication — with the SigV4 streaming chunked decoder and a full authorization pipeline
  (policy / ACL / public-access-block / ownership).
- **Durable & crash-consistent** — staged writes with file + directory fsync and rename, hash
  validation, and a metadata transaction as the single linearization point; startup
  reconciliation reclaims any orphaned blob with no manual intervention.
- **Built in** — transparent per-bucket block compression, native TLS (rustls + aws-lc-rs),
  AES-256-GCM envelope encryption of secrets at rest with master-key rotation, asynchronous
  bucket replication, an embedded React management console, Prometheus metrics, and a CLI.

The full engineering specification is **[`ARCH.md`](./ARCH.md)**; contributor / AI-agent onboarding
is **[`CLAUDE.md`](./CLAUDE.md)**; operations guides live in **[`docs/`](./docs)**.

## Quickstart

```sh
cargo build --release --bin cairn          # add `(cd ui && npm install && npm run build)` first for the console

export CAIRN_DATA_DIR=/var/lib/cairn CAIRN_DB_PATH=/var/lib/cairn/cairn.db
export CAIRN_MASTER_KEY=$(openssl rand -hex 32)     # a 32-byte key, hex-encoded
./target/release/cairn serve
```

- **S3 API** on `:7373` — point any S3 client (AWS CLI, boto3, s3cmd) at `http://localhost:7373`.
- **Web console + management API** (`/api/v1`) on `:7374` — open `http://localhost:7374`.
- Default credentials are `cairn` / `cairnadmin` (override with `CAIRN_ROOT_ACCESS_KEY` /
  `CAIRN_ROOT_SECRET_KEY`). Health at `/healthz`, readiness at `/readyz`, metrics at `/metrics`.

```sh
# With the AWS CLI configured for the access key / secret above (any region):
aws --endpoint-url http://localhost:7373 s3 mb s3://demo
echo 'hello cairn' | aws --endpoint-url http://localhost:7373 s3 cp - s3://demo/hi.txt
aws --endpoint-url http://localhost:7373 s3 cp s3://demo/hi.txt -        # -> hello cairn
```

## Configuration

Configuration is **environment-only**: every setting is a `CAIRN_*` variable, validated on load
(`cairn validate-config`) — there is no config file and no flags. The full reference is **ARCH 28**.
Common knobs: `CAIRN_LISTEN_ADDR` (S3, default `0.0.0.0:7373`), `CAIRN_UI_ADDR` (console, default
`0.0.0.0:7374`; set `off` for headless), `CAIRN_REGION`, `CAIRN_MASTER_KEY` (or `CAIRN_MASTER_KEY_RING`
for rotation), `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH`, `CAIRN_META_SHARDS`.

CLI subcommands: `cairn serve` (default), `validate-config`, `bootstrap`, `integrity [--repair]`.

## Build & test

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
(cd ui && npm run build)            # builds the console bundle embedded into the binary

# Static, dependency-free musl binary for distroless / scratch containers:
cargo build --release --bin cairn --target x86_64-unknown-linux-musl
```

The workspace is ~14 crates under `crates/` (see **[`CLAUDE.md`](./CLAUDE.md)** for the map and
ARCH 12 for the trait spine). Heavier conformance harnesses — boto3 (real AWS SDK),
crash-consistency, a two-node replication soak, and the MinIO warp benchmark — live in
`conformance/` and run in CI.

## License

Apache-2.0.
