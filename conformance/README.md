# Conformance and verification harnesses

This directory holds the end-to-end verification the audit found missing (`docs/GAPS.md`
High #8, Medium #11), alongside the boto3 compatibility suite. Everything here drives a real
`cairn` binary; the in-crate unit/property/fuzz tests live next to their sources.

## `run.sh` + `conformance.py` — boto3 compatibility suite

Bootstraps a fresh store, starts the server, and drives it with the boto3 AWS SDK (real SigV4
signing and the default flexible-checksum `aws-chunked` streaming body). See `run.sh` for usage.

```sh
pip install boto3
BIN=target/debug/cairn PY=python3 bash conformance/run.sh
```

## `crash_consistency.sh` — durability crash window (ARCH §29.4, F-4)

Makes the durability ordering claim *real* rather than asserted. The blob store commits a blob
durably (fsync file → rename → fsync dir) **before** the metadata row is committed, so a crash in
that window leaves an **orphan blob**: a durable file no row references. The spec's correctness
claim is that reconciliation reclaims exactly those orphans. The harness:

1. builds `cairn` with `--features failpoints` (arms the `cairn-blob` `fail` seams);
2. bootstraps a fresh temp store;
3. starts the server with `FAILPOINTS=blob_after_durable=panic` — the seam fires *after* blob
   durability but *before* the metadata commit;
4. issues an object `PUT`, crashing the in-flight task in that exact window;
5. stops the server;
6. runs `cairn integrity` (reconcile);
7. asserts `orphans_reclaimed >= 1` and that the object is absent (a fresh `GET` 404s).

It exits non-zero on any assertion failure.

```sh
conformance/crash_consistency.sh                       # build + run
SKIP_BUILD=1 BIN=target/debug/cairn conformance/crash_consistency.sh   # reuse a binary
```

### Runtime arming prerequisite

The `fail` crate honours the `FAILPOINTS` environment variable **only after the process calls
`fail::setup()`** (equivalently `fail::FailScenario::setup()`). The fault seams and the
`failpoints` cargo feature are present in `cairn-blob`/`cairn-server`, but the server's `serve`
entrypoint does not yet make that call, so an env-armed `FAILPOINTS` is currently inert and the
step-4 `PUT` succeeds instead of crashing. To turn this into a live crash test, add — gated on
the feature, with `fail` as a direct dependency of `cairn-server` — at the top of `run_server`
(`crates/cairn-server/src/main.rs`):

```rust
#[cfg(feature = "failpoints")]
let _fail_scenario = fail::FailScenario::setup(); // honours $FAILPOINTS
```

Until that one line exists, the harness detects that the seam did not fire and falls back to a
**dry validation**: it plants an orphan blob directly in the data dir exactly where the durable
commit places one, proves `cairn integrity` reclaims precisely that orphan while preserving every
live (referenced) blob, and then exits **non-zero** with a diagnostic so CI flags the missing
wiring. Once `fail::setup()` is wired, the same script takes the live path unchanged.

## Fuzz targets (ARCH §29.3)

Three `cargo-fuzz` projects, each detached from the workspace and built on nightly:

| crate | target | entry points |
|-------|--------|--------------|
| `crates/cairn-s3/fuzz`    | `chunked_decoder`  | the SigV4 streaming chunked decoder |
| `crates/cairn-xml/fuzz`   | `request_parsers`  | `parse_tagging` / `parse_cors_configuration` / `parse_delete` / `parse_complete_multipart` / `parse_versioning_configuration` |
| `crates/cairn-authz/fuzz` | `parse_policy`     | `cairn_authz::parse_policy` |

Each parser's contract is *total parsing*: malformed input folds to a typed error, never a
panic. Build and run a target:

```sh
cd crates/cairn-xml/fuzz   && cargo +nightly fuzz build
cd crates/cairn-xml/fuzz   && cargo +nightly fuzz run request_parsers -- -max_total_time=20
cd crates/cairn-authz/fuzz && cargo +nightly fuzz run parse_policy    -- -max_total_time=20
```

## Listing property test (ARCH §29.2)

`crates/cairn-meta/tests/listing_oracle.rs` is a proptest that inserts a random key set into an
in-memory store and asserts that draining `list_current` across random `prefix`/`delimiter`/
`max-keys` pagination yields exactly what a naive sorted-filter reference oracle yields — across
every page boundary. Run more cases with `PROPTEST_CASES=2000 cargo test -p cairn-meta`.
