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

Built wave by wave against ARCH §32. **Cairn already runs as an S3 server.**

- **Wave 0 (seam freeze)** — complete: workspace, the 8-trait spine (`cairn-types`), in-memory
  test doubles, server skeleton (HTTP stack, middleware, health/readiness/metrics, config
  validation, graceful shutdown), CI, and a verified static `musl` binary.
- **Wave 1 (foundations)** — complete: `cairn-meta` (group-committing SQLite writer + WAL read
  pool), `cairn-blob` (durable commit + block compression + reconcile), `cairn-auth` (SigV4 +
  Bearer, AWS `get-vanilla` vector), `cairn-crypto`, `cairn-authz` (policy/ACL/BPA engine),
  `cairn-xml`.
- **Wave 2 (protocol core)** — in progress: the SigV4 streaming **chunked decoder** (F-5) with
  fuzz target; the S3 service (bucket CRUD; object PUT/GET/HEAD/DELETE with ranges, conditionals,
  streaming uploads, listing); wired into the binary with `cairn bootstrap`. **Remaining:**
  multipart, copy, bulk-delete, and the aws-sdk-s3 conformance matrix.

~190 tests pass; `clippy -D warnings` and `rustfmt` are clean.

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
```

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
