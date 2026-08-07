# cairn-blob

The local-filesystem `BlobStore` (`LocalBlobStore`) — **the only crate in the workspace that performs
filesystem syscalls**. It owns the durable commit sequence, the self-describing CRNB block format
(compression + SSE-S3 encryption at rest), and the reconcile (orphan-reclaim) path. Object bytes are
plain files under opaque IDs; metadata is someone else's job (`cairn-meta`).

## Layout (`src/`)
- `lib.rs` — `LocalBlobStore`, the `BlobStore` + `ReconcileOracle` impls, `reconcile_inner`, the
  streaming write/read transforms, `resolve` (path-traversal guard), `check_single_filesystem`, and
  the safe rustix-backed `open_readonly_nofollow`/`open_lock_file_nofollow` and
  `try_lock_exclusive` syscall seams used by snapshot input and node-local command exclusion. The
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
- **Cancellation before `StagedBlob` return reclaims synchronously.** Single-part staging and
  multipart assembly keep a POSIX unlink-on-drop guard over both their unique `.staging` name and
  final bucket name through every await, including the post-rename directory fsync. The Tokio and
  io_uring create/rename handoffs retain matching ownership until the request acknowledges them, so
  backend work that finishes after request cancellation cannot recreate an orphan behind the guard.
- **A newly-created bucket directory triggers an extra `data_root` fsync** (`ensure_bucket_dir`, F-1):
  the rename is not durable until the parent records the new dir entry. Paid only on the first write
  into a bucket. Don't drop it.
- **Multipart directory creation is durable before part data is accepted.** `open` creates
  `.staging/multipart` and fsyncs each newly-mutated parent; the first `stage_part` for an upload
  creates its session directory and fsyncs `.staging/multipart` before opening the part file. Each
  completed part is then fsynced before the session directory is fsynced. This parent-to-child
  ordering is what keeps a power loss from leaving metadata that names a part whose directory entry
  was never durable.
- **Crypto fails closed.** A wrong/missing DEK or a tampered block fails GCM auth → `BlobError::Corruption`
  — never plaintext or zeros. The DEK is supplied by the caller (the master-key envelope lives in
  `cairn-crypto`); `compress.rs` types deliberately do **not** derive `Debug` so a DEK can't be logged.
  Compress-then-encrypt (ciphertext is incompressible); the 12-byte nonce is `HMAC-SHA256(DEK,
  block_index)[..12]` — deterministic, never stored, never reused for a fixed key. Encrypted CRNB
  v3 also appends a domain-separated HMAC-SHA256 over the complete plaintext index and trailer,
  so their algorithm, compression flags, lengths, offsets, and version are authenticated before
  use. Legacy encrypted v2 remains readable only when trusted metadata explicitly identifies a
  pre-v3 object/part; every new or rewritten encrypted blob is v3. See ARCH 27.
- **The reader seam is
  `open_raw(path, range, cipher: BlobCipher, compression, expected_logical_len)` + `probe(path)` —
  there is NO DEK-less `open`.** `BlobCipher` (in `cairn-types`) is
  `KnownPlaintext | LegacyV2(DEK) | AuthenticatedV3(DEK)`. A caller cannot express an encrypted read
  without naming both the key and trusted metadata format. `open_raw` preserves that declaration
  through the framing decision, probe open, lazy stream, and
  `CompressedReader::open_with_dek` call (the internal method name is stable — do NOT rename it),
  including the trusted `CompressionDescriptor` and expected logical length from the object row's
  `size_logical` or multipart `PartRef.size`. The compressed reader rejects an on-disk version
  that differs from the declaration before returning bytes; the file cannot select its own legacy
  parser. Because v2 does not authenticate its index/trailer, it additionally requires the trailer
  algorithm and block size to exactly match that descriptor (`Uncompressed` means algorithm None
  with the fixed encryption-only block geometry), requires the trailer/index logical total to equal
  that trusted row/part size, and requires each raw/compressed entry's physical length to agree with
  the 16-byte GCM overhead. `probe` answers PRESENCE + physical framing only:
  one `stat`, no body open, no DEK, no decrypt, so a well-formed ENCRYPTED blob probes `Ok` (present),
  NOT `Corruption`; absence is `NotFound`.
- **Framing comes only from the caller's authoritative descriptor + cipher; body sniffing never
  chooses or refuses framing.** `is_container = cipher.is_encrypted() || compressed`. On the
  metadata-declared plaintext/uncompressed branch, the file length must exactly equal the trusted
  object/part logical length. An encrypted-but-uncompressed CRNB file passed without its descriptor
  therefore fails closed because framing adds physical bytes, while a legitimate plaintext object
  whose body is itself a complete CRNB file (for example a data-directory backup stored in S3)
  remains arbitrary data and round-trips byte-for-byte. The server mirrors the cumulative mismatch
  count as `cairn_blob_plaintext_length_mismatch_total`; non-zero means missing/inconsistent
  metadata or truncated local storage, not a content classification.
- **Index allocation is bounded before authentication.** Trailer `index_len` is capped at 64 MiB
  (enough for a 5-GiB object at the minimum supported 1-KiB block size) and its block count must
  match the independently stored logical size and block geometry before allocation. A corrupt large
  file therefore cannot make pre-v3 or pre-MAC parsing allocate in proportion to its physical size.
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
- Blob transfers are bounded by **two SEPARATE permit pools** (both default `DEFAULT_BLOB_IO_CONCURRENCY
  = 64`; `with_read_pool_size` / `with_io_pool_size` to tune) — `read_permits` for GETs and `write_permits`
  for stage/stage_part/assemble (ARCH 7.4). The split is deliberate: a read permit is held for the whole
  *client-paced* transfer, so a flood of slow readers pins only read permits and can never starve writes
  (a read-side slow-loris that once stalled the data plane, audit 2026-07). Reads use an *owned* permit
  and defer the file open until the body is first polled, so a kernel zero-copy GET that drops the body
  unpolled opens no file and releases the permit immediately (Phase 2.5).
- **Small-object GET fast path.** An uncompressed blob at or below `small_read_max` (default `SMALL_READ_MAX
  = 256 KiB`, below the sendfile floor; `with_small_read_max` overrides, `0` forces the streamed path for
  an A/B) is read WHOLE in the single probe `open` and served as one `Bytes` with the range sliced from
  that buffer — no second open, no read permit, no per-chunk `mpsc` streaming channel. Larger objects take
  the streamed read (+ zero-copy hint). Measured ~1.3–2.6× faster in-process for tiny GETs; isolated by
  `cargo run --release --example bench_small_get -p cairn-blob`.

## Contract & pointers
- Depends only on `cairn-types` (the trait spine + domain types) — no other engine crate. Implements
  the `BlobStore` and `ReconcileOracle` traits; the in-memory double lives in `cairn-types`
  (`feature = "testing"`).
- Multipart parts are staged as **uncompressed** intermediate artifacts (`fsync_in_place`, no rename);
  compression is applied once at `assemble`. A part is staged **encrypted** (a CRNB `VERSION_ENCRYPTED`
  blob) when `stage_part` is passed a per-part DEK (SSE / bucket-default / at-rest multipart, ARCH 27),
  so nothing plaintext hits disk; `assemble` decrypts each such part on read via the typed
  `PartRef.cipher` before re-encoding under the object DEK. The MD5/ETag is always computed over
  plaintext (before any transform), so it is identical with or without encryption/compression.
- **The staging write options remain bare DEKs; every read declaration is typed.** `stage`,
  `stage_part`, and the assembled-object `StageOptions` take `encryption: Option<SecretKey32>` because
  current writes always emit v3. `PartRef` is not a write option: it declares how assembly must read
  an already-staged part, so it carries `BlobCipher` and pins legacy v2 versus authenticated v3.
- Failpoint seams (`--features failpoints`): `blob_after_durable`, `blob_after_assemble`,
  `blob_after_multipart_session_dir` — exercised by crate tests,
  `conformance/crash_consistency.sh`, and `crash_multipoint.sh`. CRNB-reader fuzz target in `fuzz/`.
- Tests: unit tests in each module; integration in `tests/blob.rs`. Spec: `docs/storage-durability.md`
  (8–10), SSE-S3 in `docs/security-errors.md` 27. Gate: see the root `../../CLAUDE.md`.
