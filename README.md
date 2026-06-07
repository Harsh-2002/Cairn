# Cairn

A production-grade, fully **S3-compatible object storage server** written from scratch in
pure Rust. Object bytes live as plain files on a local POSIX filesystem; all metadata lives
in an embedded SQLite database (the single source of truth). Cairn adds transparent per-bucket
block compression, native TLS, asynchronous bucket replication, an embedded Svelte management
UI, and a CLI — shipped as **one static binary**.

> The full engineering specification is in [`ARCH.md`](./ARCH.md). The build roadmap is in
> [the plan](.). Cairn is a built from its own engineering specification
> `the baseline`.

## Status

All build waves (ARCH §32, Phases 0–14) are complete. **Cairn is a runnable, S3-compatible
server, validated against a real AWS SDK.**

- **Foundations** — the 8-trait spine + in-memory doubles; `cairn-meta` (group-committing SQLite
  writer + WAL read pool, savepoint-isolated batches); `cairn-blob` (durable commit with
  directory fsync + range-friendly block compression + bounded reconcile); `cairn-auth` (SigV4
  header/presigned + Bearer, validated against the AWS `get-vanilla` vector); `cairn-crypto`
  (AES-256-GCM envelope), `cairn-authz` (policy/ACL/BPA/ownership engine), `cairn-xml`.
- **S3 surface** — the SigV4 streaming **chunked decoder** (F-5) with fuzz target; bucket CRUD;
  object PUT/GET/HEAD/DELETE (ranges, conditionals, streaming uploads, checksums); listing
  v1/v2 + versions; multipart; copy; bulk delete; versioning, tagging, CORS, policy, lifecycle,
  replication subresources; the full authorization pipeline.
- **Engines** — lifecycle scanner + multipart sweeper + metrics refresher run in the background;
  outbox-driven replication engine.
- **Control plane** — management JSON API (`/api/v1`) + embedded **Svelte UI** (`/ui/`) + CLI
  (`bootstrap`, `integrity`, `validate-config`, `serve`); **native TLS** (rustls + aws-lc-rs).

**Verification:** 242 unit/integration/property tests + a doctest suite; `clippy -D warnings`
and `rustfmt` clean; a verified static `musl` binary; the chunked decoder benchmarks at
**~1 GiB/s**; and a **boto3 conformance suite** drives the running server through the full object
lifecycle (incl. real SigV4 + aws-chunked streaming + multipart + versioning + tagging).

### Try it

```sh
cargo build --bin cairn
export CAIRN_DATA_DIR=/tmp/cairn CAIRN_DB_PATH=/tmp/cairn/cairn.db
export CAIRN_MASTER_KEY=$(openssl rand -hex 32)
./target/debug/cairn bootstrap          # prints admin credentials once
./target/debug/cairn serve &            # serves on 127.0.0.1:9000
AUTH="Authorization: Bearer <id>.<secret>"   # from bootstrap output
curl -X PUT -H "$AUTH" http://127.0.0.1:9000/my-bucket
curl -X PUT -H "$AUTH" --data-binary "hello cairn" http://127.0.0.1:9000/my-bucket/hi.txt
curl -H "$AUTH" http://127.0.0.1:9000/my-bucket/hi.txt     # -> hello cairn
# then open http://127.0.0.1:9000/ui/ for the management UI
```

Run the AWS-SDK conformance suite: `pip install boto3 && bash conformance/run.sh`.
See [`docs/`](./docs) for the operations guide, the backup/restore procedure, and the S3 API
support matrix.

## Workspace layout

| Crate | Responsibility |
|---|---|
| `cairn-types` | The 8 traits (the spine), domain types, the error tree, and the in-memory doubles (`feature = "testing"`). Depends on no engine. |
| `cairn-meta` | SQLite `MetadataStore`: single group-committing writer + read pool + cache. *(Wave 1)* |
| `cairn-blob` | Local-filesystem `BlobStore`: durable commit + compression + reconciliation. The only crate doing filesystem syscalls. *(Wave 1)* |
| `cairn-crypto` | `Crypto` (AEAD envelope + zeroize), `Clock`, `PublicUrl`. *(Wave 1)* |
| `cairn-auth` | `Authenticator` chain: SigV4 + Bearer + chunked-signature primitives. *(Wave 1)* |
| `cairn-authz` | `AuthorizationEngine`: pure policy/ACL/BPA/ownership evaluation. *(Wave 1)* |
| `cairn-xml` | quick-xml S3 request/response codec. *(Wave 1)* |
| `cairn-s3` | S3 handlers, the 7 request lifecycles, the streaming chunked decoder. *(Wave 2)* |
| `cairn-replication` / `cairn-lifecycle` | Replication engine; lifecycle scanner. *(Wave 3)* |
| `cairn-control` / `cairn-ui` / `cairn-cli` | Management API; embedded Svelte UI; CLI. *(Wave 4)* |
| `cairn-server` | The binary: wires concrete impls, the hyper/rustls stack, middleware, shutdown. |

## Building

```sh
# Development build + tests (host gnu target)
cargo build
cargo nextest run --workspace        # or: cargo test --workspace

# Static, dependency-free binary for distroless/scratch containers
cargo build --release --bin cairn --target x86_64-unknown-linux-musl
ldd target/x86_64-unknown-linux-musl/release/cairn   # -> "statically linked"
```

## Running

```sh
cairn validate-config        # validate configuration and exit
cairn serve                  # run the server (defaults to 127.0.0.1:9000)
```

Configuration layers flags > environment (`CAIRN_*`) > optional TOML file > defaults, and is
validated on load. Liveness at `/healthz`, readiness at `/readyz`, Prometheus metrics at
`/metrics`.

## License

Apache-2.0.
