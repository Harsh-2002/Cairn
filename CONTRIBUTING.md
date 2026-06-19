# Contributing to Cairn

The full project-specific guidance is in **[`CLAUDE.md`](./CLAUDE.md)** (also exposed as `AGENTS.md`
for other agent tools) and the per-folder `CLAUDE.md` files. This is the short human version.

## Build and the gate

Cairn is a Rust workspace; the binary is `cairn`. Before opening a PR, the full gate must pass — it
mirrors [`.github/workflows/ci.yml`](./.github/workflows/ci.yml):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings    # also run with --all-features
cargo nextest run --workspace                             # + cargo test --workspace --doc
(cd ui && npm install && npm run build)                   # for any UI change
```

## Conventions

- **Configuration is environment-only** (`CAIRN_*`); add knobs in `crates/cairn-server/src/config.rs`
  with a doc comment and validation.
- A new `Mutation` or shared read must be mirrored in **both** `cairn-meta/src/apply.rs` and
  `cairn-meta-async/src/apply.rs`, plus the in-memory double in `cairn-types` (the 4(+1)-site rule).
  Schema changes are **append-only** migrations.
- Crypto reads **fail closed**; secrets are never logged.
- Every change lands with a test. Warnings are denied (`unsafe_code`, `dbg!`, `todo!` are lints).
- For a new feature with a failure mode, add a harness under `conformance/` AND a CI job (see
  [`conformance/CLAUDE.md`](./conformance/CLAUDE.md)).

The engineering specification lives in [`docs/`](./docs) (start at
[`docs/CLAUDE.md`](./docs/CLAUDE.md)).

## License

By contributing you agree that your contributions are licensed under Apache-2.0 (see `LICENSE`).
