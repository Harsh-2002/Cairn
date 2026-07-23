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
- **The reader seam is `open_raw(path, range, cipher: BlobCipher, compression)` + `probe(path)` —
  there is NO DEK-less `open`.** `BlobCipher` (in `cairn-types`) is `KnownPlaintext | Dek([u8;32])`;
  a caller cannot express a read without naming the cipher, which is what removes the footgun below.
  `open_raw` translates the cipher to the internal `Option<[u8;32]>` (`KnownPlaintext` => `None`,
  `Dek(k)` => `Some(k)`) at the top, so every downstream check — the framing decision, the refusal
  guard, the `CompressedReader::open_with_dek` call (an unrelated internal method — do NOT rename it)
  — is byte-for-byte unchanged. `probe` answers PRESENCE + physical framing only: one `stat`, no
  body open, no DEK, no decrypt, so a well-formed ENCRYPTED blob probes `Ok` (present), NOT
  `Corruption`; absence is `NotFound`. It is what `cairn integrity --repair` uses to tell a dangling
  row from a healthy encrypted object it holds no key for.
- **Framing comes from the caller's descriptor + cipher, but a `KnownPlaintext` read of an encrypted
  blob is REFUSED.** `is_container = dek.is_some() || compressed`, and staging records only the
  *logical* compression — so an encrypted-but-**uncompressed** blob is `Uncompressed`, and a caller
  passing `KnownPlaintext` (the old `dek: None`) used to get the raw CRNB container bytes streamed as
  if they were the body (compression is off by default, so this was the default configuration; it is
  how replication mirrored ciphertext). `open_raw`'s probe now cross-checks the trailer and returns
  `Corruption("encrypted blob read without a data key")` when the blob is a fully self-consistent
  `VERSION_ENCRYPTED` container. It only ever **refuses** — it never parses as a container — so
  audit #18's rule (no trailer sniffing to decide framing) still holds, and an uncompressed blob
  whose bytes merely end in `CRNB` still reads. The trailer is fetched with ONE positioned
  `read_exact_at` (`#[cfg(unix)]`; a seek/read/rewind fallback elsewhere), so the tuned plaintext
  read path pays no extra `lseek` and nothing downstream depends on an implicit rewind.
- **The guard has a KNOWN false-positive class — it is not a random collision.** The four identity
  checks are satisfied *by construction* by any object whose body is the verbatim bytes of an
  encrypted blob file, i.e. the "back up `CAIRN_DATA_DIR` by `rclone`/`aws s3 sync`-ing it into a
  bucket" workflow. Such an object PUTs fine and then reads back as
  `Corruption("encrypted blob read without a data key")` even though nothing is wrong: the bytes are
  intact on disk and can still be recovered out-of-band (copy the file out of `data_root`); only the
  API read is refused. Do **not** weaken the guard to fix this — it closes a real fail-open (shipping
  ciphertext as a body). The ambiguity is structural: framing is decided from the *caller's*
  descriptor, and the guard's only input is the bytes. The row-keyed reader in a later ADR stage,
  which resolves framing from the version row that owns the blob rather than from the file's
  contents, removes it. Until then the counter below is how an operator tells the two apart.
- **The refusal is counted, not just logged.** `LocalBlobStore::encrypted_without_key_total()` is a
  cumulative count of refused DEK-less encrypted reads; the server mirrors it into
  `cairn_blob_encrypted_without_key_total` on its metrics tick (this crate takes no `metrics`
  dependency — expose state, let `cairn-server` emit, exactly like `writer_queue_depth`). A
  `tracing::error!` is not alertable; a counter is.
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
  so nothing plaintext hits disk; `assemble` decrypts each such part on read (via `PartRef.dek`) before
  re-encoding under the object DEK. The MD5/ETag is always computed over plaintext (before any
  encrypt/compress transform), so it's identical with or without any transform.
- **Known API asymmetry (a decision, not an oversight).** The *read* seam names its cipher
  (`open_raw` + `BlobCipher`), but the *write* seam does not: `stage`/`stage_part`/`assemble` still
  take a bare `encryption: Option<[u8;32]>` / `PartRef.dek`. Stage 3 deliberately closed only the
  read seam, because that is where the leak lived (a DEK-less read streamed ciphertext). Giving the
  write path the same by-name cipher is a later, separate change; until then the write DEK stays an
  `Option`.
- Failpoint seams (`--features failpoints`): `blob_after_durable`, `blob_after_assemble` — exercised by
  `conformance/crash_consistency.sh` and `crash_multipoint.sh`. CRNB-reader fuzz target in `fuzz/`.
- Tests: unit tests in each module; integration in `tests/blob.rs`. Spec: `docs/storage-durability.md`
  (8–10), SSE-S3 in `docs/security-errors.md` 27. Gate: see the root `../../CLAUDE.md`.
