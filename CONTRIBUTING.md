# Contributing to Cairn

Start with an [issue](https://github.com/Harsh-2002/Cairn/issues) for a bug report or proposed change.
The [architecture constraints](CONTRACT.md) and [engineering specification](docs/CLAUDE.md) describe
what an implementation must preserve. Folder-level `CLAUDE.md` files provide context for each crate.

## Build and run locally

You need Linux 5.6+ with `openat2` support, Rust via rustup (the checkout selects its toolchain),
Node.js 22 with npm, a C/C++ build toolchain, CMake, pkg-config and OpenSSL's command-line tool.
SQLite and the compression libraries are built from bundled sources.

```sh
git clone https://github.com/Harsh-2002/Cairn.git
cd Cairn
npm ci --prefix web
npm run build --prefix web
cargo build --bin cairn
```

Build the console before Rust: `web/dist` is embedded into the binary. A Rust-only build can use
a placeholder console, which is insufficient for console tests or a usable installation.

Create a local master key once. The command refuses to overwrite an existing `cairn.env`:

```sh
(umask 077; set -C; printf 'CAIRN_MASTER_KEY=%s\n' "$(openssl rand -hex 32)" > cairn.env)
```

For each development session, load that same key and use loopback listeners:

```sh
export CAIRN_MASTER_KEY="$(sed -n 's/^CAIRN_MASTER_KEY=//p' cairn.env)"
export CAIRN_DATA_DIR="$PWD/data" CAIRN_DB_PATH="$PWD/data/cairn.db"
export CAIRN_API_ADDR=127.0.0.1:7373 CAIRN_CONSOLE_ADDR=127.0.0.1:7374
cargo run --bin cairn -- validate-config
cargo run --bin cairn -- serve
```

The console is at `http://localhost:7374`; the S3 endpoint is `http://localhost:7373`.
Development credentials are `cairn` / `cairnadmin`. Retain `cairn.env` while reusing `data/`;
both are excluded from version control. The [README](README.md#try-it-locally-with-docker) includes
an AWS CLI upload/download example.

## Checks before a pull request

Install the additional Rust check tools with `cargo install --locked cargo-nextest cargo-audit`;
the installer checks also need ShellCheck. Run focused tests in the crate you changed while
iterating, for example `cargo nextest run -p cairn-blob`.

Code, dependency, build and automation changes use the full gate below. Ordinary documentation-only
PRs use link, anchor, shell-example and workflow-policy checks instead. CI verifies matching PR
results after merging; it reruns full validation only when reuse cannot be established. See
[the CI policy](docs/delivery.md#317-ci-validation-and-result-reuse).

The complete repository gate is:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
cargo audit
(cd web && npm install && npm run lint && npm run build \
  && npm audit --omit=dev --audit-level=moderate && npm audit --audit-level=high)
shellcheck -s sh install.sh tests/install.sh && sh tests/install.sh
```

`make check` runs formatting, default-feature Clippy, nextest and doctests. `make check-all` also
runs all-feature Clippy, the web build and installer checks; web lint and dependency audits still
need the commands above. See `make help` for focused targets and
[conformance guidance](conformance/CLAUDE.md) for the live-server CI harnesses.

For automation edits, also run `python3 tests/release_policy.py`,
`python3 -m unittest discover -s tests -p test_ci.py`, ShellCheck on the changed shell scripts,
and the checksum-pinned `actionlint` used by the workflow.

## Making a change

- Read the relevant specification and nearest `CLAUDE.md` before changing a crate. Update the
  affected documentation with behavior changes.
- Keep configuration in validated `CAIRN_*` environment variables.
- Mirror shared metadata operations across both SQL backends and the in-memory double. Classify
  per-bucket mutations in shard routing. Add migrations rather than editing applied migrations.
- Preserve durable publication ordering and fail-closed crypto behavior; never log secrets.
- Add regression coverage for behavior fixes. A new feature with a failure mode needs a
  conformance harness and CI job. Warnings are denied.
- Describe the problem, resulting behavior and validation in the PR. Keep unrelated changes out.

The full conventions and backend update rules are in [CLAUDE.md](CLAUDE.md).

## License

By contributing, you agree that your contributions are licensed under Apache-2.0 (see [LICENSE](LICENSE)).
