# cairn-blob

The local-filesystem `BlobStore` (`LocalBlobStore`) — **the only crate in the workspace that performs
filesystem syscalls**. It owns the durable commit sequence, the self-describing CRNB block format
(compression + SSE-S3 encryption at rest), and the reconcile (orphan-reclaim) path. Object bytes are
plain files under opaque IDs; metadata is someone else's job (`cairn-meta`).

## Layout (`src/`)
- `lib.rs` — `LocalBlobStore`, the `BlobStore` + `ReconcileOracle` impls, `reconcile_inner`, the
  streaming write/read transforms, `resolve` (path-traversal guard), `check_single_filesystem`. The
  failpoint seams live here.
- `staging.rs` — `Staging`: the backend-agnostic durable single-object write handle (create tmp →
  stream → `commit` / `abort`). One enum dispatching `tokio::fs` vs. the io_uring backend.
- `commit.rs` — `DirSyncCoalescer`: a single coordinator task that batches concurrent same-directory
  fsyncs into one syscall (group-commit for the directory fsync, ARCH 8.2). Shared across store clones.
- `compress.rs` — the CRNB block format: `BlockEncoder` (write) / `CompressedReader` (ranged read),
  per-block zstd/lz4, per-block AES-256-GCM. **`pub` + `#[doc(hidden)]`** only so `fuzz/` can drive it.
- `hash.rs` — `Hashers`: the always-on MD5 (→ ETag) plus requested CRC32/CRC32C/SHA1/SHA256, over
  plaintext, in one streaming pass.
- `raw_io.rs` — safe `fallocate`/`fadvise` placement hints (ARCH 7.5) via `rustix` (keeps `forbid(unsafe_code)`).
- `uring.rs` — the optional `io-uring`-feature staging backend (EXPERIMENTAL, Linux-only, off by default).

## Notes
- **The durability ordering IS the contract** (`docs/storage-durability.md` 8, ARCH 8.2) — do not
  reorder: stream → `sync_data` (fdatasync, *not* `sync_all`) the staged file → rename into the bucket
  dir → fsync that dir (via the coalescer) → only then is the blob durable. `stage` returns *before*
  any metadata row references it; a crash here leaves an orphan that reconcile reclaims — that is by design.
- **A newly-created bucket directory triggers an extra `data_root` fsync** (`ensure_bucket_dir`, F-1):
  the rename is not durable until the parent records the new dir entry. Paid only on the first write
  into a bucket. Don't drop it.
- **Crypto fails closed.** A wrong/missing DEK or a tampered block fails GCM auth → `BlobError::Corruption`
  — never plaintext or zeros. The DEK is supplied by the caller (the master-key envelope lives in
  `cairn-crypto`); `compress.rs` types deliberately do **not** derive `Debug` so a DEK can't be logged.
  Compress-then-encrypt (ciphertext is incompressible); the 12-byte nonce is `HMAC-SHA256(DEK,
  block_index)[..12]` — deterministic, never stored, never reused for a fixed key. See ARCH 27.
- **Never resolve a storage path that escapes `data_root`.** `resolve` rejects absolute paths and any
  `..`/root/prefix component → `BlobError::Io("unsafe storage path")`. Object bytes live under opaque
  IDs, never under the user key, so key-based traversal is structurally impossible — keep it that way.
- **One filesystem.** `data_root`, `.staging`, and every bucket dir must share a filesystem or the
  atomic rename fails with `EXDEV`. `check_single_filesystem` is called at startup to fail fast.
- ENOSPC (errno 28 / `StorageFull`) → `BlobError::OutOfSpace` → HTTP 507. Map it via `io_err`.
- Reconcile safety margin: a blob/staging artifact younger than `staging_safety_margin_secs` is **not**
  reclaimed even if the oracle reports it not-live (it may be an in-flight PUT whose row hasn't
  committed — audit #7). Margin `0` reclaims immediately (the legacy behavior; what tests and on-demand
  reconcile use). Per-bucket reconciles run concurrently; the staging area is reconciled inline.
- Every transfer holds one of `io_permits` (default `DEFAULT_BLOB_IO_CONCURRENCY = 64`, `with_io_pool_size`
  to tune) for the duration of its file I/O — bounds blocking-pool occupancy (ARCH 7.4). Reads use an
  *owned* permit and defer the file open until the body is first polled, so a kernel zero-copy GET that
  drops the body unpolled opens no file and releases the permit immediately (Phase 2.5).

## Contract & pointers
- Depends only on `cairn-types` (the trait spine + domain types) — no other engine crate. Implements
  the `BlobStore` and `ReconcileOracle` traits; the in-memory double lives in `cairn-types`
  (`feature = "testing"`).
- Multipart parts are staged **unencrypted/uncompressed** intermediate artifacts (`fsync_in_place`, no
  rename); SSE-S3 and compression are applied once at `assemble`, mirroring how the assembled object is
  hashed. The MD5/ETag is always computed over plaintext, so it's identical with or without any transform.
- Failpoint seams (`--features failpoints`): `blob_after_durable`, `blob_after_assemble` — exercised by
  `conformance/crash_consistency.sh` and `crash_multipoint.sh`. CRNB-reader fuzz target in `fuzz/`.
- Tests: unit tests in each module; integration in `tests/blob.rs`. Spec: `docs/storage-durability.md`
  (8–10), SSE-S3 in `docs/security-errors.md` 27. Gate: see the root `../../CLAUDE.md`.
