# cairn-types

The shared domain types, the typed error tree, and the **trait spine** every other crate is written
against (the 8 traits). Depends on no engine — freezing this freezes the seams.

## Layout (`src/`)
- `traits.rs` — the 8 traits: `MetadataStore`, `BlobStore`, `Crypto`, `Authenticator`,
  `AuthorizationEngine`, `Clock`, `ReconcileOracle`, `PublicUrl`.
- `error.rs` — the typed error tree + the cross-error `From` conversions.
- `meta.rs` — `Mutation` (the write enum), `ListQuery`, `OutboxEntry`, and the metadata DTOs.
- `id.rs` — validated ids: `BucketName`, `ObjectKey`, `StoragePath`, `VersionId`, `UploadId`.
- `object.rs` / `bucket.rs` / `authz.rs` / `auth.rs` / `blob.rs` / `crypto.rs` / `replication.rs`.
- `testing/` — **in-memory doubles** (`feature = "testing"`) used by every other crate's tests.

## Notes
- This is the **(+1) site of the 4(+1)-site mutation rule**: a new `Mutation` must be handled in the
  in-memory double here as well as both `apply.rs` files.
- Keep it engine-free; never add a dependency on a concrete store/blob/crypto crate.
- Spec: `docs/metadata.md` (12). See the root `../../CLAUDE.md` for workspace-wide rules.
