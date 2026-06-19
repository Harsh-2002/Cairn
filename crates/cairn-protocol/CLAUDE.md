# cairn-protocol

The S3 protocol layer: request dispatch, the 7 request lifecycles, the streaming chunked-upload
decoder, and the total error translator to S3 XML. Handlers depend ONLY on the `cairn-types` traits.

## Layout (`src/`)
- `service.rs` — **the S3 surface**: every operation handler (PUT/GET/HEAD/DELETE, ranges,
  conditionals, multipart, copy, listing, subresources). SSE-S3 seal/open lives here.
- `chunked.rs` — the SigV4 streaming chunked decoder (fuzzed: `chunked_decoder`, 2.1M+ iters; ~1 GiB/s).
- `error_map.rs` — `Error` -> S3 XML (`<Code>`/`<Message>`/status); internal errors are generalised.
- `request.rs` — the parsed `S3Request`. `httpdate.rs` — RFC 1123 dates.

## Notes
- Copy/UploadPartCopy **authorize the source read** before opening it (audit #1, critical).
- SSE DEK open **fails closed**; conditional writes go through the precondition path (412).
- Spec: `docs/s3-api.md` (13, 16-19, 21); auth in `docs/auth.md`. See the root `../../CLAUDE.md`.
