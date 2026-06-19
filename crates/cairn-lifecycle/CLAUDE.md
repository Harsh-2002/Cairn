# cairn-lifecycle

The background lifecycle scanner: parses an S3 `<LifecycleConfiguration>` into typed rules and
applies them (expiration, noncurrent expiration, abort-incomplete-multipart, delete-marker removal).

## Layout (`src/`)
- `config.rs` — `parse_lifecycle` (XML -> typed rules). `scanner.rs` — the periodic apply pass.
- `lib.rs` / `tests.rs`.

## Notes
- The scanner runs as a background loop spawned by `cairn-server` (`CAIRN_LIFECYCLE_INTERVAL_SECS`).
- Spec: `docs/s3-api.md` (19). See the root `../../CLAUDE.md`.
