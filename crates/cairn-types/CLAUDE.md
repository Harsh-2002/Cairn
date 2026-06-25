# cairn-types

The shared domain types, the typed error tree, and the **trait spine** every other crate is written
against. Depends on **no engine** — the protocol/control layers consume only these traits, so
freezing this crate freezes the seams. `#![forbid(unsafe_code)]`.

## Layout (`src/`)
- `traits.rs` — the spine: the **9 traits** `MetadataStore`, `BlobStore`, `ReconcileOracle`,
  `Authenticator`, `AuthorizationEngine`, `Crypto`, `Clock`, `PublicUrl`, `ReplicationSink`. The
  doc comments here are the contracts (e.g. the durable-commit sequence, fail-closed crypto, the
  `submit`-is-the-only-write rule). Read the trait doc before changing a method.
- `error.rs` — the typed error tree: per-subsystem errors (`BlobError`, `MetaError`, `AuthError`,
  `CryptoError`, `ReplicationError`, `BodyError`, `ConfigError`) **fold into the canonical `Error`**
  via the `From` impls at the bottom. `Error` is the wire-mappable enum the single translator maps
  totally to S3 XML / control JSON (ARCH 25).
- `meta.rs` — the largest module: `Mutation` (the write enum), `ListQuery`/`ListPage`, `OutboxEntry`,
  `WebhookEntry`, and the metadata DTOs/rollups returned by `MetadataStore`.
- `id.rs` — validated newtypes: `BucketName`, `ObjectKey`, `StoragePath`, `VersionId`, `UploadId`,
  `UserId`, `InvalidName`. Validation is S3 wire-correctness, **not** path safety — keys never become
  filesystem paths (that lives in `cairn-blob`).
- `auth.rs` / `authz.rs` / `object.rs` / `bucket.rs` / `blob.rs` / `crypto.rs` / `notification.rs` /
  `replication.rs` / `time.rs` — the per-domain DTOs; `lib.rs` re-exports the most-used items.
- `testing/` — **canonical in-memory doubles** behind `feature = "testing"`: `InMemoryMetadataStore`,
  `InMemoryBlobStore`, `StubCrypto`, `TestClock`, `FakeReplicationSink`, `FixedAuthenticator`,
  `AllowAll`/`DenyAll`. Every other crate enables this as a dev-dependency to unit-test without
  disk or SQLite.

## Notes
- **This is the (+1) site of the 4(+1)-site mutation rule.** A new `Mutation` variant (or a new
  shared read on a trait) MUST be handled in `InMemoryMetadataStore` here, **and** in both
  `cairn-meta/src/apply.rs` and `cairn-meta-async/src/apply.rs`. The in-memory double must stay
  behaviorally faithful — downstream tests trust it as the reference engine.
- **Keep it engine-free.** NEVER add a dependency on a concrete `cairn-meta`/`cairn-blob`/
  `cairn-crypto`/etc. crate — the dependency arrow points the other way. Only `serde`/`thiserror`/
  `bytes`/`uuid`/`zeroize`-class leaf deps belong here; the `testing` doubles' extras (`md-5`,
  `hex`, `futures-util`) are gated behind the feature.
- Async traits use `#[async_trait]` to stay object-safe (`dyn`-compatible); zero-copy of object
  *bytes* is a `BlobReadHandle` hint, not part of the futures. Don't make a trait non-dyn-safe.
- `Crypto::open` returns `Zeroizing<Vec<u8>>` — secrets zeroize at the source. A wrong/missing key or
  tampered envelope is a hard `CryptoError`, never plaintext (fail-closed).
- Spec: trait spine + metadata model in `docs/metadata.md` (11–12); error model in
  `docs/security-errors.md` (25). See the root `../../CLAUDE.md` for the gate and workspace-wide rules.
