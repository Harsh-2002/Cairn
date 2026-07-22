# cairn-protocol

The S3 protocol layer: request dispatch, the 7 request lifecycles (ARCH 21.1–21.7), the streaming
chunked-upload decoder, and the total `Error`→S3-XML translator. Handlers reach the storage stack
ONLY through the `cairn-types` trait spine (`Arc<dyn MetadataStore/BlobStore/AuthorizationEngine/
Clock/Crypto>`) — never a concrete engine.

## Layout (`src/`)
- `service.rs` — **the entire S3 surface** (~6k lines, one `impl S3Service`): `dispatch` →
  `bucket_op`/`object_op` → per-operation handlers (PUT/GET/HEAD/DELETE, ranges, conditionals,
  multipart, copy, listing, every subresource incl. `?encryption`), plus the free-function helpers
  below the impl. The central `authorize` and all SSE seal/open live here: `resolve_object_encryption`
  (explicit header > bucket default > transparent `AtRest` > plaintext) mints the object DEK across
  `SseMode {SseS3, AtRest, Kms}`; `open_sse_dek`/`seal_part_dek`/`open_part_dek` handle read and
  per-part multipart keys.
- `keyprovider.rs` — the SSE-KMS `KeyProvider` trait + `LocalRingProvider` (v1). Maps a KMS key id
  to DEK-sealing crypto and gates writes via the `CAIRN_KMS_KEY_IDS` allow-list. **Label-only**: every
  DEK is sealed under the same node master ring regardless of key id — the id is a label, not
  cryptographic isolation (removing an id does not lock existing objects). Shaped so a real external
  provider (AWS KMS/Vault) can slot in later without touching the S3 surface.
- `chunked.rs` — the SigV4 streaming `aws-chunked` decoder (`ChunkDecoder`, `ChunkVerifier`,
  `decode_stream`). The single highest-risk ingest component (F-5); fuzzed via the `chunked_decoder`
  target under `fuzz/`, proptested in `mod fuzz_props`.
- `error_map.rs` — `map`/`error_response`: `Error`→(`StatusCode`, S3 code, XML). **Exhaustive
  match, no wildcard arm** — every variant maps explicitly.
- `request.rs` — library-neutral `S3Request`/`S3Response`/`S3Body` (no hyper here; `cairn-server`
  adapts hyper to these, tests build them directly). `httpdate.rs` — RFC 1123 date parse/format.

## Invariants & rules
- **Authorize centrally, before the handler.** `bucket_op`/`object_op` map the request to an
  `Action`, then call `authorize` BEFORE dispatching to the operation. New operations route through
  `bucket_action`/`object_action`; do not add a handler that skips this chokepoint.
- **An unrecognized subresource MUST NOT fall through to a data-plane handler.** A `PUT object?acl`
  must never overwrite the object body — `unhandled_{object,bucket}_subresource` gates this and
  returns `NotImplemented`. Add new `?subresource` arms *above* those guards (the `?encryption`
  Get/Put/Delete arms sit above the bucket guard, exactly like `cors`).
- **Durability ordering is the contract** (ARCH 8/21.1): stage (fsync file+dir) → verify
  Content-MD5 / signed SHA-256 / client checksums → `meta.submit(Mutation::…)` (the single
  linearization point) → reclaim the superseded blob best-effort. Don't reorder.
- **Any failure after `blob.stage` MUST delete the staged blob** before returning (`blob.delete`),
  or you leak an orphan. Every early-return in `put_object`/copy/multipart after staging does this.
- **Crypto fails closed** across every SSE seam. `open_sse_dek`/`open_part_dek` return an error on a
  bad/missing key or tampered envelope — never plaintext. SSE-S3, transparent `AtRest`, and SSE-KMS
  are all **label-only** (one master ring seals every DEK, so open is symmetric on `self.crypto`); a
  KMS key id gates writes via the allow-list but is not isolation. Part-level multipart seals a
  per-part DEK *before* `stage_part` (no fallible step after staging) and `complete_multipart` opens
  every part key before claiming the session (a bad key leaves the upload retryable). Mandatory-SSE
  buckets refuse a plaintext client PUT — transparent `AtRest` satisfies the data goal but NOT the
  client contract, so it is force-upgraded to advertised SSE-S3.
- **Session credentials never short-circuit.** In `authorize`, `is_session` principals are always
  `AuthenticatedMember` — they get no owner/admin bypass (least-privilege STS, ARCH 14).
- **Corrupt security configs fail closed** (ARCH 15.3/15.5): an unparseable BPA/policy/ACL doc
  raises `Internal`, never silently opens access.
- **Copy / UploadPartCopy authorize the SOURCE read** against the *source* bucket's policy/ACL
  (audit #1, critical) — owning only the destination must not let you exfiltrate another tenant.
- **The `x-amz-meta-cairn-replica` marker is Administrator-gated** (audit #16): only a replication
  principal classifies a write as an inbound `Replica` (skips the outbox, preserves source
  version id). A normal member's header is ignored and the write replicates normally.
- **5xx messages are generalized** (audit #28): `error_response` logs the real cause but returns an
  opaque `InternalError` body; client 4xx keep their descriptive S3 message.
- **Version-scoped authz** (audit #33): a `?versionId` request passes that `VersionId` to
  `authorize` so `s3:ExistingObjectTag`/object-ACL conditions evaluate against the named version.

## Contract / how it fits
- Depends on `cairn-auth`/`cairn-authz` (policy), `cairn-xml` (codec), `cairn-replication`/
  `cairn-lifecycle` (filters). Holds no SQL and no filesystem syscalls — those are `cairn-meta`/
  `cairn-blob`. Stays runtime-agnostic: the replication-drain wake is an injected
  `Fn()` callback (`with_replication_wake`), not a tokio handle.
- All writes go through `meta.submit(Mutation::…)`; never open an ad-hoc write path. A new mutation
  obeys the 4(+1)-site rule (see the root `../../CLAUDE.md`).

## Notes
- The request head is `Sync`; the body is passed *separately* to `handle` so it can borrow across
  awaits — only body-consuming ops (object PUT, `?delete`, `complete-multipart`, config PUTs) take
  it. `streaming_body` de-frames SigV4-streaming bodies; a signed sentinel without `chunk_signing`
  context on the principal is `SignatureDoesNotMatch`.
- CORS preflight (`OPTIONS`) is evaluated against stored CORS rules *before* auth — browsers send
  it credential-less (ARCH 18.2).
- `S3Body::ZeroCopy` always carries the portable `stream` too; non-fast paths (TLS, musl, the
  default build) serve byte-identical output. Don't assume the sendfile path engaged.
- **`DeleteObjects` runs its keys bounded-concurrent** (`buffered`), not serially, so the single
  group-committing writer batches the independent per-key mutations into far fewer fsync barriers.
  Each key keeps its own authorize + Object-Lock check + delete-marker/replication logic, and results
  stay in request order. `authorize` loads the object ACL + tags **only** when a bucket/identity
  policy or an enabled ACL can consult them — a default-bucket GET/HEAD/DELETE skips those reads.
- Tests: `tests/protocol_core.rs` (end-to-end against the real SQLite + filesystem backends);
  decoder bench `benches/decode.rs`. service.rs has no inline `#[test]`s.
- Spec: `docs/s3-api.md` (13, 16–19, 21; decoder = 21.7); auth `docs/auth.md` (14–15); errors
  `docs/security-errors.md` (25). See the root `../../CLAUDE.md` for the gate and conventions.
