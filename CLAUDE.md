# CLAUDE.md — Cairn

Cairn is a production-grade, **S3-compatible object store** written from scratch in pure Rust.
Object bytes are plain files on a POSIX filesystem; all metadata is an embedded SQLite database
(the single source of truth). It ships as one static binary with transparent compression, native
TLS, async bucket replication, an embedded React console, and a CLI.

> The **engineering specification is the single source of truth** and lives in [`docs/`](./docs),
> split into focused, section-numbered reference documents. The
> **section numbers are stable** — code comments say e.g. "ARCH 28"; use the table below to open the
> exact document instead of grepping. Read the relevant doc before any non-trivial change.

## Documentation map — open the doc for what you're touching

| Need | Document | Sections |
|---|---|---|
| Summary, scope, why-Rust, baseline architecture | [`docs/overview.md`](./docs/overview.md) | 0–5 |
| System/node model; concurrency, runtime, I/O | [`docs/data-plane.md`](./docs/data-plane.md) | 6–7 |
| Durability & crash consistency; storage layout; compression | [`docs/storage-durability.md`](./docs/storage-durability.md) | 8–10 |
| Metadata store (writer/WAL/cache); trait spine | [`docs/metadata.md`](./docs/metadata.md) | 11–12 |
| S3 protocol, versioning, tagging, CORS, lifecycle, request lifecycles | [`docs/s3-api.md`](./docs/s3-api.md) | 13, 16–19, 21 |
| Authentication & authorization (SigV4/Bearer, policy/ACL/BPA/ownership) | [`docs/auth.md`](./docs/auth.md) | 14–15 |
| Bucket replication | [`docs/replication.md`](./docs/replication.md) | 20 |
| Management API, web console, CLI | [`docs/control-plane.md`](./docs/control-plane.md) | 22–24 |
| **Configuration reference (`CAIRN_*`)** | [`docs/configuration.md`](./docs/configuration.md) | 28 |
| Error model + security & threat model | [`docs/security-errors.md`](./docs/security-errors.md) | 25, 27 |
| Metrics, logging, audit | [`docs/observability.md`](./docs/observability.md) | 26 |
| Testing, conformance, performance | [`docs/testing-performance.md`](./docs/testing-performance.md) | 29–30 |
| Build/deploy, roadmap, decision log, appendices | [`docs/delivery.md`](./docs/delivery.md) | 31–34 |

Operator runbooks (not the spec): [`docs/operations.md`](./docs/operations.md) (deploy + master-key
rotation), [`docs/backup-restore.md`](./docs/backup-restore.md),
[`docs/s3-api-matrix.md`](./docs/s3-api-matrix.md), [`docs/benchmarks.md`](./docs/benchmarks.md).
UI visual system: [`docs/design.md`](./docs/design.md). Doc index: [`docs/CLAUDE.md`](./docs/CLAUDE.md).

## Build, test, and the gate

The binary is `cairn` (`cargo build --bin cairn`). Treat the following as the **definition of done**;
it mirrors `.github/workflows/ci.yml` and must be green before any change is finished:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings        # also run with --all-features
cargo nextest run --workspace                                # + cargo test --workspace --doc
(cd ui && npm install && npm run build)                      # for any UI change / the embedded console
```

- Toolchain is pinned in `rust-toolchain.toml` (stable). Warnings are denied; `unsafe_code`, `dbg!`,
  and `todo!` are lints — keep them out of committed code.
- `ui/` is excluded from the cargo workspace. The built `ui/dist` is embedded into `cairn-ui` via
  `rust_embed`; without a real `npm run build` the crate compiles against a placeholder that fails
  the `index_referenced_bundles_are_embedded` test.
- The optional `cairn-meta-async` backend (libSQL/Turso) is glibc-only and is excluded from the
  static musl build.

## Workspace layout (`crates/`)

- `cairn-types` — the 9 traits (the spine), domain types, the error tree, in-memory doubles
  (`feature = "testing"`); depends on no engine.
- `cairn-meta` — the default SQLite `MetadataStore`: one group-committing **Writer** + a WAL read
  pool + a read-through cache. Schema/migrations in `schema.rs`; mutation SQL in `apply.rs`.
- `cairn-meta-async` — libSQL/Turso async backend (beta, glibc-only); mirrors `cairn-meta`'s SQL.
- `cairn-blob` — local-filesystem `BlobStore`: durable commit, block compression, reconciliation.
  The only crate doing filesystem syscalls.
- `cairn-crypto` — `Crypto` (AES-256-GCM envelope + zeroize), `Clock`, `PublicUrl`.
- `cairn-auth` / `cairn-authz` — SigV4 + Bearer authenticator chain; pure policy/ACL/BPA/ownership engine.
- `cairn-xml` — the S3 request/response XML codec (quick-xml).
- `cairn-protocol` — S3 handlers, the request lifecycles, the streaming chunked decoder. The S3
  surface lives in `service.rs`.
- `cairn-replication` / `cairn-lifecycle` — outbox-driven replication engine; lifecycle scanner.
- `cairn-webhook` — outbox-driven webhook event-notification delivery engine (mirrors `cairn-replication`).
- `cairn-control` / `cairn-ui` — management JSON API (`/api/v1`); embedded React console (source in `ui/`).
- `cairn-server` — the binary: wires the concrete stack (`stack.rs`), the hyper/rustls server,
  background loops (`background.rs`), config (`config.rs`), and the CLI subcommands (`main.rs`).

## Conventions & invariants (get these right)

- **Configuration is environment-only.** Everything is `CAIRN_*` env vars parsed by strict Figment
  (`deny_unknown_fields`) — no config file, no CLI flags. Add new knobs to
  `crates/cairn-server/src/config.rs` with a doc comment **and** validation (ARCH 28).
- **Two listeners.** S3 data plane on `:7373` (`CAIRN_LISTEN_ADDR`); web console + `/api/v1` on
  `:7374` (`CAIRN_UI_ADDR`; set to `off`/`none` for headless). `/healthz`, `/readyz`, `/metrics` are
  served on the S3 port, ahead of the concurrency limiter.
- **All writes go through the single `Writer`** (group-commit, savepoint-isolated batches) in
  `cairn-meta`; reads use the WAL read pool. Never open ad-hoc write connections.
- **The 4(+1)-site rule.** A new `Mutation` or shared read must be mirrored in
  `crates/cairn-meta/src/apply.rs` **and** `crates/cairn-meta-async/src/apply.rs`, plus the
  in-memory double in `cairn-types`. Schema changes are **append-only** migrations in
  `cairn-meta/src/schema.rs` (never edit an applied migration).
- **Crypto fails closed.** A missing/wrong key or tampered envelope must return an error — never
  plaintext, zeros, or partial data. Secrets are sealed at rest and are never logged, echoed, or
  returned by any endpoint. Master key via `CAIRN_MASTER_KEY` (or a `CAIRN_MASTER_KEY_RING`;
  rotation runbook in `docs/operations.md`).
- **Durability is the contract** (ARCH 8): stage → fsync file → rename → fsync dir → validate hashes
  → commit the metadata transaction (the single linearization point) → reclaim superseded blobs.
  Don't reorder these steps.
- Every fix lands with a regression test in the owning crate, and the full gate is green, before it
  is "done". Match the surrounding code's style, comment density, and idioms.

## Running locally & gotchas

```sh
export CAIRN_DATA_DIR=/tmp/cairn CAIRN_DB_PATH=/tmp/cairn/cairn.db
export CAIRN_MASTER_KEY=$(openssl rand -hex 32)
cargo run --bin cairn -- validate-config     # validate the env and exit
cargo run --bin cairn -- bootstrap           # ensure the root admin; prints credentials once
cargo run --bin cairn -- serve               # S3 on :7373, console on :7374
```

- Default dev credentials are `cairn` / `cairnadmin` — override `CAIRN_ROOT_ACCESS_KEY` /
  `CAIRN_ROOT_SECRET_KEY` in production.
- A restricted sandbox cannot bind listen sockets; run the server with the sandbox disabled for any
  live/e2e test. Rebuilds are slow (heavy C deps: sqlite, zstd) — prefer per-crate `nextest` while
  iterating, and run the full workspace gate before finishing.
- Heavier conformance harnesses live in `conformance/`: `run.sh` (boto3 / real AWS SDK),
  `crash_consistency.sh` (the F-4 durability harness), `soak.sh` (two-node replication), `warp.sh`.
