# .github

CI and release automation.

## Layout
- `workflows/ci.yml` — runs on every push/PR. Jobs: `lint` (fmt + clippy `-D warnings`, incl.
  `--all-features`), `test` (nextest, gnu + musl; musl excludes `cairn-meta-async`), `test-all-features`,
  `doc` tests, the conformance/regression e2e jobs (`conformance`, `rotation`, `share`, `concurrency`,
  `crash-consistency`, `crash-multipoint`, `soak`, `replication-chaos`, `warp`, `warp-escalate`,
  `blob-limits`), `fuzz-smoke`, `benches` (compile), `coverage`.
- `workflows/release.yml` — release artifacts. `actions/setup/` — the shared toolchain setup.

## Notes
- The e2e/regression jobs run the harnesses under `../conformance/` against a real `cairn` binary.
- When adding a feature with a failure mode, add a conformance harness AND a job here (see the
  load vs fault-injection split in `../conformance/CLAUDE.md`).
- The gate here is the source of truth for "green" — mirror it locally before pushing.
