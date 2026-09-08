# cairn-blob

Directory lifetime is part of admitted I/O ownership: `namespace::open_directory` takes a shared
flock and revalidates the linked inode. `AnchoredPath` clones retain that fence through queued
creation/rename/sync work. Exact cleanup shares traversal locks, then releases the child's shared
lock and prunes only after acquiring nonblocking exclusive ownership; its parent stays locked.
Completed syncs drop all directory descriptors before acknowledgements but retain actual job
leases through completion. Do not remove a shared parent merely because it is currently empty;
an admitted multipart completion may already hold its final directory descriptor.

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
- `namespace.rs` — admitted exact names, descriptor-anchored initialization/creation/rename/input
  opens, file-lock quiescence and durable exact cleanup; Linux `openat2` rejects descendant symlinks
  and mounts, including same-device bind mounts.
- `owned_file.rs` — file descriptors, buffers and storage leases retained through queued/executing
  blocking jobs and abandoned results; async cancellation cannot release actual I/O ownership.
- `reconcile.rs` — bounded Linux flat/two-hex-leaf traversal; descriptor-relative no-follow cleanup,
  exact membership counts, conservative unknown-layout handling and parent-fsynced pruning.
  `DT_UNKNOWN` falls back to no-follow metadata lookup and lookup errors fail the scan.
- `timing.rs` — bounded multipart permit/assembly/durability observations, mirrored by the server
  metrics tick; includes interrupted stages and reports sample eviction.
- `staging.rs` — `Staging`: the backend-agnostic durable single-object write handle (create tmp →
  stream → `commit` / `abort`). One enum dispatching retained blocking file jobs vs. the io_uring
  backend; abort stops production and leaves admitted names for exact recovery.
- `commit.rs` — `DirSyncCoalescer`: a single coordinator task that batches concurrent same-directory
  fsyncs into one syscall (group-commit for the directory fsync, ARCH 8.2). Shared across store clones.
- `compress.rs` — the CRNB block format: `BlockEncoder` (write) / `CompressedReader` (ranged read),
  per-block zstd/lz4, per-block AES-256-GCM. **`pub` + `#[doc(hidden)]`** only so `fuzz/` can drive it.
- `encode.rs` — bounded CRNB staging adapter; 64-KiB index buffer spills to the admitted index alias,
  unlinks its newly created inode, and streams through a leased file owner into the final blob.
  The alias still requires exact cleanup/namespace synchronization before its debt can retire.
- `hash.rs` — `Hashers`: the always-on MD5 (→ ETag) and internal SHA-256 plus requested supplementary checksums, over
  plaintext, in one streaming pass.
- `raw_io.rs` — safe `fallocate`/`fadvise` placement hints (ARCH 7.5) via `rustix` (keeps `forbid(unsafe_code)`).
- `uring.rs` — the optional `io-uring`-feature staging backend (EXPERIMENTAL, Linux-only, off by default).

## Notes
- **The durability ordering IS the contract** (`docs/storage-durability.md` 8, ARCH 8.2) — do not
  reorder: stream → `sync_data` (fdatasync, *not* `sync_all`) the staged file → rename into the bucket
  dir → fsync that dir (via the coalescer) → only then is the blob durable. `stage` returns *before*
  an authoritative object row references it; a crash here leaves admitted paths for startup intent
  resolution and exact cleanup.
- **Every physical write requires committed admission.** `stage`, `stage_part` and `assemble`
  consume a `StorageCreationPermit` for the exact Writer-admitted plan and its `StorageIoLease`.
  The plan includes all possible temporary, final and index-spool names before creation; namespace
  operations may not invent more names. Multipart reservation/completion ownership and admission
  share a Writer savepoint. Publication consumes the matching intent atomically with metadata.
- **Cancellation retains ownership; it does not unlink on Drop.** Recovery admission and the exact
  plan guard precede the Writer submission. Blocking jobs, io_uring work, returned descriptors and
  buffers, and coalesced directory-sync requests retain child leases through actual completion.
  Leases retain the bounded recovery slot and exclusive node lifetime. Recovery waits for lease
  quiescence and probes exact file locks before resolving intent; a timeout is not quiescence.
- **Reclamation uses exact Writer claims.** Publication and resolution preserve live references
  and enqueue unreferenced aliases/superseded paths as durable debt. `cleanup_storage` requires the
  matching claim and lease, acquires the file lock, unlinks only that path and synchronizes its
  directory, including when the name is already absent. It prunes only empty supported parents and
  synchronizes their parents. Debt/quota retirement validates generation, token and expiry after
  durable absence. The raw `delete`, `delete_part_attempt` and recursive `delete_session` APIs
  are removed; do not reintroduce an unleased or unclaimed physical deletion seam. Errors and
  ambiguous acknowledgements retain ownership/debt.
- **Store construction requires a maintenance lease.** `LocalBlobStore::open(root, lease)` runs
  initialization inside a retained blocking job. Production callers supply the actual exclusive
  node guard; `fixture_storage_io()` is only for isolated tests/examples. The configured root may
  itself be a mount. Missing roots create only the final component under an existing parent.
- **A new bucket directory requires a `data_root` fsync** (F-1). Anchored directory preparation
  synchronizes the parent before a child can hold admitted file bytes; do not remove that barrier.
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
- **Index memory is paged before authentication.** The 64-MiB format ceiling and metadata-bound
  block count remain. Initial validation uses 65,529-byte pages (7,281 entries), retaining only
  SHA-256 fingerprints and physical starting offsets. Summaries are provisional until the full
  structure and v3 HMAC pass. Later loads verify an entire page on the same descriptor before
  parsing entries; one page and its offsets are cached. Index working allocations stay below
  192 KiB at the ceiling. Initial open still scans the whole index; v1 gains no initial MAC.
- **Writers obey the same index ceiling.** `BlockEncoder::feed` is fallible and rejects an excessive
  logical size before buffering input; finalization also fails after a rejected feed. Known encoded
  lengths fail before staging/preallocation, and multipart totals use checked addition. Effective
  limits depend on block geometry (ARCH 9.3); raw files retain their configured ceiling. This does
  not provide 5-TiB encrypted-object support. Writes drain entries to the bounded `encode.rs` spool;
  readers retain verified pages. See `docs/storage-evolution-plan.md` for subsequent evaluations.
- **Block allocations obey trusted metadata on every CRNB version.** Trailer algorithm, block
  size and logical total must match metadata for v1/v2/v3. Every raw payload is exactly logical
  length and every compressed payload is nonempty and shorter, excluding the encrypted GCM tag.
  Reject violations during open before `read_range` allocates from an index physical length.
- **Never resolve a storage path that escapes `data_root`.** `resolve` rejects absolute paths and any
  `..`/root/prefix component → `BlobError::Io("unsafe storage path")`. Object bytes live under opaque
  IDs, never under the user key, so key-based traversal is structurally impossible — keep it that way.
- **Guarded read reservations follow blocking work.** `open_raw_guarded` keeps the caller's
  `ReadBufferLease` in the probe, its returned `ReadProbe`, the streaming blocking closure and
  response body. The prepared encoded reader transfers into that body; never reopen/reparse it
  after validation. Request cancellation cannot return admission while detached work or its result
  still owns buffers. `BlobStore::read_memory_bound` owns decoder/page/frame accounting and is what
  replication reserves. Include configurable small-read coalescing; raw reads cannot grow their
  allocation past the probed length. Kernel cache and allocator retention are separate costs.
- **One filesystem and no descendant mount crossings.** Linux 5.6+ with `openat2` support for
  `BENEATH | NO_SYMLINKS | NO_XDEV` is required. Initialization, admitted creation, referenced input
  opens and cleanup reject descendant symlinks and mounts, including same-device bind mounts.
  Device/inode equality alone is insufficient. The explicitly configured root mount is allowed;
  unsupported kernels/platforms fail closed. `check_single_filesystem` also runs at startup.
- ENOSPC (errno 28 / `StorageFull`) → `BlobError::OutOfSpace` → HTTP 507. Map it via `io_err`.
- **Full scans preserve journal ownership.** The membership oracle protects authoritative object
  and part references, every intent alias, and all cleanup rows (pending, claimed or expired).
  Root staging aliases fan out across metadata shards. Session absence alone never authorizes
  recursive deletion. Even an unprotected artifact younger than `staging_safety_margin_secs` is
  preserved; margin `0` removes only that age constraint. Bucket workers are bounded; staging is
  scanned inline. A failed/incomplete scan cannot authorize legacy orphan-accounting release.
- Blob transfers are bounded by **two SEPARATE permit pools** (both default `DEFAULT_BLOB_IO_CONCURRENCY
  = 64`; `with_read_pool_size` / `with_io_pool_size` to tune) — `read_permits` for GETs and `write_permits`
  for stage/stage_part/assemble (ARCH 7.4). The split is deliberate: a read permit is held for the whole
  *client-paced* transfer, so a flood of slow readers pins only read permits and can never starve writes
  (a read-side slow-loris that once stalled the data plane, audit 2026-07). Reads use an *owned* permit
  and defer raw streaming's extra open until the body is polled, so a kernel zero-copy GET that
  drops the fallback unpolled releases its permit without another open. Encoded bodies reuse the
  descriptor and reader already prepared by the initial probe.
- **Small-object GET fast path.** An uncompressed blob at or below `small_read_max` (default `SMALL_READ_MAX
  = 256 KiB`, below the sendfile floor; `with_small_read_max` overrides, `0` forces the streamed path for
  an A/B) is read WHOLE in the single probe `open` and served as one `Bytes` with the range sliced from
  that buffer with a read permit but no second open or per-chunk `mpsc` streaming channel. Larger objects take
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
- New `StagedBlob` values always carry `internal_sha256`, computed over logical plaintext in the
  same ingest/assembly pass. It never adds an unrequested S3 checksum. `hash::Hashers` is shared with
  the scrubber so full-object supplementary checksum algorithms use the ingest implementations.
- Tests: unit tests in each module; integration in `tests/blob.rs`. The ignored namespace bind-mount
  regression reexecutes in a private mount namespace. `tests/uring_process_death.rs` requires root,
  private mounts, an owned ext4 loop image and readable kernel stacks: a real ring write remains
  pending during SIGKILL and exclusion holds through process teardown until thaw/reap. This is not
  evidence of a post-exit detached-kernel-reference interval or power-loss durability. Its fixture
  owns and cleans up its mount, loop device and image. Spec: `docs/storage-durability.md` (8–10),
  SSE-S3 in `docs/security-errors.md` 27. Gate: see the root `../../CLAUDE.md`.

New writes remain flat. Linux reconciliation recognizes only the approved nested UUID grammar,
keeps bounded bucket/leaf pages and one leaf cursor per worker, and preserves/reports unknown paths and symlinks.
Bucket enumeration is streamed too. File unlink batches sync their directory; pruning syncs the
parent before reporting success. Full startup scans remain active; intent recovery and exact debt
cleanup do not authorize scan-free startup or nested writes. Phase 3D remains unactivated.
