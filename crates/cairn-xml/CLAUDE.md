# cairn-xml

The S3-compatible XML request/response codec (quick-xml) — the single place domain types are
translated to and from S3 XML.

## Layout (`src/`)
- `parse.rs` — the request parsers (tagging, CORS, delete, complete-multipart, versioning, ACL,
  replication, lifecycle). The `request_parsers` fuzz target. SAX text is coalesced across chunk/CDATA
  boundaries (audit #24).
- `lib.rs` — the response generators (listings, errors, etc.). `timefmt.rs` — S3 timestamp formatting.

## Notes
- Contract is **total parsing**: malformed input folds to a typed error, never a panic.
- Spec: `docs/s3-api.md` (13). See the root `../../CLAUDE.md`.
