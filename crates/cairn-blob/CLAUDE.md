# cairn-blob

The local-filesystem `BlobStore` — the **only crate that performs filesystem syscalls**. It owns the
durable commit sequence and the reconcile (orphan-reclaim) path.

## Layout (`src/`)
- `lib.rs` — `BlobStore`, `reconcile` (orphan reclamation; the safety margin only gates a reconcile
  racing live PUTs — startup/on-demand reconcile uses margin 0). Failpoint seams live here.
- `commit.rs` / `staging.rs` — the durable commit: stage -> fsync file -> rename -> fsync dir.
- `compress.rs` — range-friendly block compression at rest. `hash.rs` — ETag/checksums.
- `raw_io.rs` / `uring.rs` — the I/O backends (`uring.rs` is the optional `io-uring` feature).

## Notes
- **The durability ordering is the contract** (`docs/storage-durability.md` 8) — do not reorder.
- ENOSPC (errno 28) -> `BlobError::OutOfSpace` -> HTTP 507.
- Failpoint seams (`--features failpoints`): `blob_after_durable`, `blob_after_assemble`
  (exercised by `conformance/crash_consistency.sh` and `crash_multipoint.sh`).
- Spec: `docs/storage-durability.md` (8-10). See the root `../../CLAUDE.md`.
