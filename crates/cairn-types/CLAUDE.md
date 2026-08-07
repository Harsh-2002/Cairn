# cairn-types

The shared domain types, the typed error tree, and the **trait spine** every other crate is written
against. Depends on **no engine** — the protocol/control layers consume only these traits, so
freezing this crate freezes the seams. `#![forbid(unsafe_code)]`.

## Layout (`src/`)
- `traits.rs` — the spine: the **9 traits** `MetadataStore`, `BlobStore`, `ReconcileOracle`,
  `Authenticator`, `AuthorizationEngine`, `Crypto`, `Clock`, `PublicUrl`, `ReplicationSink`. The
  doc comments here are the contracts (e.g. the durable-commit sequence, fail-closed crypto, the
  `submit`-is-the-only-write rule). `MetadataStore::read_probe` is the uncached, constant-row
  readiness seam: every backend exercises a real read-pool connection without enumerating metadata.
  Read the trait doc before changing a method.
  - **`BlobStore` read seam (footgun removed).** There is ONE reader,
    `open_raw(path, range, cipher: BlobCipher, compression, expected_logical_len)`, plus a DEK-free presence probe
    `probe(path) -> BlobProbe`. `BlobCipher` (`blob.rs`, `KnownPlaintext | LegacyV2(DEK) |
    AuthenticatedV3(DEK)`) makes the caller NAME both the cipher and the metadata-backed CRNB
    version. The old default `open` let a caller forget the key and stream ciphertext; the later
    DEK-only seam let a mutable on-disk version byte select the legacy parser. Neither choice is
    representable now: plaintext, legacy v2, and authenticated v3 are distinct declarations, and
    the file must match exactly. `BlobCipher`'s `Debug` is hand-written to redact every key.
    The *write* seam (`stage`/`stage_part`/`assemble`) still takes an optional DEK because every
    encrypted write emits current v3; `PartRef.cipher` carries the persisted version declaration
    when assembly reads an already-staged part.
- `error.rs` — the typed error tree: per-subsystem errors (`BlobError`, `MetaError`, `AuthError`,
  `CryptoError`, `ReplicationError`, `BodyError`, `ConfigError`) **fold into the canonical `Error`**
  via the `From` impls at the bottom. `Error` is the wire-mappable enum the single translator maps
  totally to S3 XML / control JSON (ARCH 25). `AuthError::PolicyUnavailable` is deliberately an
  opaque internal error, not `AccessDenied`: it means valid credentials reached a policy
  read/integrity fault, and callers must stop rather than substitute policy absence.
- `meta.rs` — the largest module: `Mutation` (the write enum), `ListQuery`/`ListPage`, `OutboxEntry`,
  `WebhookEntry`, and the metadata DTOs/rollups returned by `MetadataStore`. `MultipartSession`
  carries the multipart SSE intent pinned at initiate (`sse_requested`, `encrypt_parts`,
  `sse_kms_requested`/`sse_kms_key_id`/`sse_bucket_key_enabled`) and `PartRecord.part_dek` the
  per-part DEK label (ARCH 27), plus initial tags and explicit Object Lock intent consumed by the
  writer at completion. `InitialObjectState` carries the corresponding atomic PUT/Copy inputs;
  `GovernanceBypass` is a trusted authorization result and deliberately has no default. Import
  history has its own
  `ImportJobListQuery`/`ImportJobPage`: every backend enforces the 1,000-row ceiling and the
  `(created_at, id)` keyset cursor.
- `id.rs` — validated newtypes: `BucketName`, `ObjectKey`, `StoragePath`, `VersionId`, `UploadId`,
  `UserId`, `InvalidName`. Validation is S3 wire-correctness, **not** path safety — keys never become
  filesystem paths (that lives in `cairn-blob`).
- `sse.rs` — the persisted `sse_descriptor` (`SseDescriptor`/`SseMode`) and the shared DEK/cipher
  helpers: the ONE definition of the envelope layout and metadata-backed CRNB version selection,
  shared by `cairn-protocol`, `cairn-server`'s re-wrap worker, and `cairn-replication`. An absent
  object format marker or unprefixed multipart `part_dek` is the explicit legacy-v2 signal; current
  writes stamp v3, and unknown explicit versions fail closed. `SseDescriptor.extra`
  (`#[serde(flatten)]`) is load-bearing: it stops an older node's read-modify-write from erasing a
  field a newer node wrote.
- `auth.rs` / `authz.rs` / `object.rs` / `bucket.rs` / `blob.rs` / `crypto.rs` / `notification.rs` /
  `replication.rs` / `time.rs` — the per-domain DTOs; `lib.rs` re-exports the most-used items.
  SSE additions live here too: `blob.rs` `PartRef.cipher` (the staged part's DEK plus expected CRNB
  version), `authz.rs` the `Get`/`PutBucketEncryption` `Action`s.
- `testing/` — **canonical in-memory doubles** behind `feature = "testing"`: `InMemoryMetadataStore`,
  `InMemoryBlobStore`, `StubCrypto`, `TestClock`, `FakeReplicationSink`, `FixedAuthenticator`,
  `AllowAll`/`DenyAll`. Every other crate enables this as a dev-dependency to unit-test without
  disk or SQLite.

## Notes
- **This is the (+1) site of the 4(+1)-site mutation rule.** A new `Mutation` variant (or a new
  shared read on a trait) MUST be handled in `InMemoryMetadataStore` here, **and** in both
  `cairn-meta/src/apply.rs` and `cairn-meta-async/src/apply.rs`. The in-memory double must stay
  behaviorally faithful — downstream tests trust it as the reference engine.
- Multipart terminal outcomes are typed and writer-owned: Complete claims `active -> completing`
  under a fresh persisted `MultipartClaimToken`, Abort removes only `active`, a failed completer
  releases only its exact token, and final completion accepts only that same token. The in-memory
  double must preserve those won/lost outcomes exactly; never turn Abort back into an unconditional
  acknowledgement. Its acknowledgement-loss hooks exist for deterministic downstream cancellation
  tests and must apply the mutation before hanging/failing.
- Ordinary object-write acknowledgement is typed too. `ResolveObjectWrite` is a writer-serialized
  exact `(bucket,key,version,row_id,storage_path)` probe used after PUT/Copy cancellation or a lost
  acknowledgement. The double's object-put acknowledgement hooks must apply the put before failing,
  and resolution must not confuse a newer null-version overwrite with the original row.
- Multipart-part acknowledgement is typed equivalently. `ResolveMultipartPartWrite` probes exact
  `(upload_id,part_number,storage_path)` ownership after UploadPart/UploadPartCopy cancellation or
  lost acknowledgement; the double's part hooks commit before hanging/failing, and same-number
  retry ABA must match only the new attempt path.
- Lifecycle deletion is typed too: `DeleteMarker`/`Deleted` mean the writer changed metadata, while
  `DeleteNotApplied` means the target was absent, the immutable `expected_row_id`,
  `expected_updated_at`, or sole-marker guard lost a race, or a conditional marker no longer names
  the current version. Listing summaries carry that internal row identity with serde skipped, so it
  never becomes S3/control response data. Protocol DELETE keeps unconditional semantics;
  maintenance must count only writer-confirmed changes.
- Object Lock policy is writer-owned too. Every double/backend must strictly read persisted
  bucket/version lock state inside the mutation, reject protected replacement or retention
  weakening, replace initial tags and lock state atomically with the object and outbox, and preserve
  immutable enablement/versioning. Do not move those decisions back to a caller-side read.
- **Keep it engine-free.** NEVER add a dependency on a concrete `cairn-meta`/`cairn-blob`/
  `cairn-crypto`/etc. crate — the dependency arrow points the other way. Only `serde`/`thiserror`/
  `bytes`/`uuid`/`zeroize`-class leaf deps belong here; the `testing` doubles' extras (`md-5`,
  `hex`, `futures-util`) are gated behind the feature.
- Async traits use `#[async_trait]` to stay object-safe (`dyn`-compatible); zero-copy of object
  *bytes* is a `BlobReadHandle` hint, not part of the futures. Don't make a trait non-dyn-safe.
- `Crypto::open` returns `Zeroizing<Vec<u8>>` — secrets zeroize at the source. A wrong/missing key or
  tampered envelope is a hard `CryptoError`, never plaintext (fail-closed). `CryptoError::UnknownKeyId`
  (the key id is simply not on the ring — a rotation window) is deliberately DISTINCT from
  `Decrypt` (tampering): callers classify the first as transient and the second as permanent, and
  conflating them lets one rotation pass stamp whole buckets terminally failed.
- `secret.rs` owns cross-crate plaintext secrets. Use `SecretString` and the non-`Copy`
  `SecretKey32`; both redact `Debug` and zeroize on drop. Plaintext access must go through the
  explicitly named `expose_secret` method. Do not reintroduce owned `[u8; 32]` DEKs or ordinary
  `String` fields for long-lived credentials.
- `notification.rs` stores webhook signing keys as authenticated `WebhookSecret::Sealed`
  envelopes. `LegacyPlaintext(SecretString)` is deserialize-only compatibility state for the
  mandatory pre-bind migration; control-plane writes must never create or preserve it. Opening
  either representation yields one `Zeroizing<Vec<u8>>`.
- `RequesterClass::OwnerOrAdmin(UserId)` deliberately retains identity alongside privilege.
  Resource-policy named-principal Denies need the id even though this class has a baseline allow;
  do not replace it with an identity-less marker.
- `ClientSource` is deliberately tri-state: `Direct`, `Forwarded`, or `Unavailable`. Preserve the
  provenance variant through `RequestView`, `S3Request`, and `RequestContext`; a trusted proxy with
  unusable forwarding metadata must not be collapsed to its own socket address.
- Spec: trait spine + metadata model in `docs/metadata.md` (11–12); error model in
  `docs/security-errors.md` (25). See the root `../../CLAUDE.md` for the gate and workspace-wide rules.
