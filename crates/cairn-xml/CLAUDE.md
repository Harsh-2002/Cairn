# cairn-xml

The S3-compatible XML request/response codec (quick-xml) — the single place Cairn translates its
domain types to and from the XML wire shapes S3 clients expect. Pure: depends only on `cairn-types`
(no engine); consumed only by `cairn-protocol`.

## Layout (`src/`)
- `lib.rs` — the response **generators**: listings (`list_objects_v2`/`_v1`/`list_object_versions`),
  multipart results, `error_document`, `delete_result`, tagging, versioning, object-lock/retention/
  legal-hold, `get_object_attributes`, copy results. Plus the `new_doc`/`finish`/`leaf`/`etag_leaf`
  writer helpers.
- `parse.rs` — the request **parsers**: complete-multipart, delete, tagging (+ `validate_tags`),
  versioning, retention, legal-hold, object-lock-configuration, CORS, ACL. `CorsRule` is defined here.
- `timefmt.rs` — hand-rolled ISO-8601 UTC formatter/parser (`format_iso8601`/`parse_iso8601`); no
  `chrono`/`time`.
- `tests.rs` — the unit suite (`#[path]`-included from `lib.rs`).
- `fuzz/` — a **detached** cargo-fuzz workspace (its own `Cargo.toml`/lock, nightly toolchain, not in
  the cargo workspace); the `request_parsers` target feeds arbitrary bytes through every parser.

## Notes
- **Total parsing is the contract.** Every malformed body — bad UTF-8, unbalanced/unclosed tags,
  missing fields, out-of-range numbers — MUST fold to `Error::MalformedXml`; parsers **never** panic.
  Keep the protocol layer's error translator total.
- Parsers drive quick-xml through the `Sax`/`drive` helper, **not** `read_event` directly: quick-xml
  treats a body that hits EOF with elements still open as a *clean* EOF, so `drive` tracks element
  depth and rejects an unbalanced body. New parsers MUST go through `drive`.
- `drive` **coalesces** consecutive `Text`/`CData` into one `Sax::Text` per contiguous run — quick-xml
  splits character data around CDATA boundaries, and emitting chunks separately corrupted keys/ETags
  and split CORS origins (audit #24). Don't reintroduce per-chunk text events.
- Generators return owned `String`s (UTF-8, no BOM, each prefixed with `<?xml … ?>`); buffer is
  in-memory so writes are infallible. All character data is escaped via quick-xml — never hand-format
  XML text. ETags render **quoted** (the one quoting point S3 requires); `StorageClass::ColdTier`
  renders as the S3 token `GLACIER`.

## Pointers
- Spec: `docs/s3-api.md` (ARCH 13, plus 21.4 for the request-lifecycle XML). See the root
  `../../CLAUDE.md` for the gate.
