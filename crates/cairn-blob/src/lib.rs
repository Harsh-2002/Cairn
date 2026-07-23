//! `cairn-blob` — the local-filesystem [`BlobStore`]. This is the ONLY crate that performs
//! filesystem syscalls, and it owns the durable commit sequence (ARCH 8.2): stream to a
//! staging file, fsync the file, rename it into the per-bucket directory, fsync that
//! directory (the F-1 fix), and only then return — so a committed blob is durable before any
//! metadata references it. Object bytes live under opaque identifiers, never under the key, so
//! key-based path traversal is structurally impossible.

#![forbid(unsafe_code)]

mod commit;
// Public only so the fuzz target (an external crate under `fuzz/`) can drive `CompressedReader`
// against arbitrary bytes; `#[doc(hidden)]` keeps it out of the published API surface. Not part of
// the supported interface — internal callers still go through the re-exports below. The reader /
// encoder deliberately do NOT implement `Debug`: they hold a raw DEK that must never be printed, so
// the `missing_debug_implementations` lint (now that the module is public) is suppressed here.
#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub mod compress;
mod crc64nvme;
mod hash;
// Safe file-placement hints (preallocation + access advice) for the write fast path (ARCH 7.5).
mod raw_io;
#[cfg(feature = "io-uring")]
mod uring;
// The staging sink abstracts the durable-write file ops so the default `tokio::fs` path and the
// optional io_uring path are interchangeable (ARCH 8.2). Each backend implements create →
// streamed writes → commit (fsync file → rename → fsync dir) / abort with the same ordering.
mod staging;

use crate::compress::{BlockEncoder, CompressedReader, is_precompressed};
use crate::hash::Hashers;
use crate::staging::Staging;
use async_trait::async_trait;
use bytes::Bytes;
use cairn_types::blob::{
    BlobCipher, BlobProbe, BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts,
    ReconcileReport, StageOptions, StagedBlob, StagedPart, ZeroCopyRead,
};
use cairn_types::bucket::{CompressionAlgorithm, CompressionPolicy};
use cairn_types::error::BlobError;
use cairn_types::id::{BucketName, StoragePath, UploadId};
use cairn_types::object::{ChecksumSet, CompressionDescriptor, ETag};
use cairn_types::time::Timestamp;
use cairn_types::traits::{BlobStore, ReconcileOracle};
use futures_util::StreamExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const STAGING: &str = ".staging";
const READ_CHUNK: usize = 64 * 1024;
/// The logical block size for an encrypted-but-uncompressed object (no bucket policy supplies a
/// block size in that case). 64 KiB keeps the per-block GCM-tag overhead negligible (16 bytes per
/// 65536) while bounding the amount decrypted for a small ranged read.
const DEFAULT_ENCRYPTED_BLOCK_SIZE: u32 = 64 * 1024;

pub(crate) fn io_err(e: std::io::Error) -> BlobError {
    if e.kind() == std::io::ErrorKind::StorageFull || e.raw_os_error() == Some(28) {
        BlobError::OutOfSpace
    } else {
        BlobError::Io(e.to_string())
    }
}

/// The local-filesystem blob store rooted at one data directory. The database, the staging
/// area, and the per-bucket directories must all share this filesystem so atomic rename works.
#[derive(Debug, Clone)]
pub struct LocalBlobStore {
    data_root: Arc<PathBuf>,
    /// Whether the durable single-object staging write path runs through the io_uring executor.
    /// Always `false` unless the `io-uring` feature is compiled in; the field exists
    /// unconditionally so the struct shape is feature-independent, but it can only be set `true`
    /// under the feature (see [`LocalBlobStore::with_io_uring`]).
    use_uring: bool,
    /// Bounds concurrent blob READ transfers (ARCH 7.4). A read holds its permit inside the
    /// spawn_blocking task feeding the response body, so a **slow client** that reads the download
    /// slowly (or not at all) pins the permit for the entire client-paced transfer. This pool is
    /// SEPARATE from `write_permits` on purpose: with a single shared pool, a flood of idle readers
    /// could exhaust it and starve writes (PUT/copy/assemble) too — a read-side slow-loris that
    /// stalled the whole data plane (audit 2026-07). Now slow readers can only bound other reads.
    read_permits: Arc<tokio::sync::Semaphore>,
    /// Bounds concurrent blob WRITE transfers — stage / stage_part / assemble (ARCH 7.4). Held only
    /// for the server-paced write to disk, never for a client read, so it is insulated from slow
    /// readers by living in its own pool (see `read_permits`).
    write_permits: Arc<tokio::sync::Semaphore>,
    /// Coalesces the per-bucket-directory fsync of the commit sequence: concurrent PUTs into the
    /// same bucket share one directory fsync instead of issuing one each (ARCH 8.2). Shared across
    /// clones of the store, so every writer feeds the same coordinator.
    dir_sync: Arc<commit::DirSyncCoalescer>,
    /// Upper size bound (bytes) for the small-object GET fast path: an uncompressed blob at or below
    /// this size is read whole in the single probe open and served as one `Bytes`, skipping the
    /// second file open, the I/O permit, and the per-chunk streaming channel (such a blob is below
    /// the kernel sendfile floor anyway). Defaults to [`SMALL_READ_MAX`]; [`with_small_read_max`]
    /// overrides it (a bench sets it to `0` to force the streaming path for an A/B on one size).
    ///
    /// [`with_small_read_max`]: LocalBlobStore::with_small_read_max
    small_read_max: u64,
    /// Cumulative count of reads REFUSED because the blob is a self-consistent encrypted CRNB
    /// container but the caller supplied no DEK (the fail-closed guard in [`open_raw`]).
    ///
    /// Exposed as state rather than emitted here: `cairn-blob` is an engine crate with no `metrics`
    /// dependency, so the server mirrors this into `cairn_blob_encrypted_without_key_total` on its
    /// metrics tick — the same expose-and-mirror shape as `writer_queue_depth` and the metadata
    /// cache's `(hits, misses)`. A `tracing::error!` alone is not alertable; this is.
    ///
    /// [`open_raw`]: cairn_types::traits::BlobStore::open_raw
    encrypted_without_key: Arc<std::sync::atomic::AtomicU64>,
}

/// Default upper bound (bytes) for the small-object GET fast path — see [`LocalBlobStore`]'s
/// `small_read_max` field. 256 KiB sits below the kernel sendfile floor, so a blob this small would
/// never take the zero-copy path; reading it whole avoids the streaming channel's per-GET overhead.
pub const SMALL_READ_MAX: u64 = 256 * 1024;

/// The default bound on concurrent blob transfers when not overridden (ARCH 7.4). A reasonable
/// general value for SSD/NVMe-backed storage; tune down for spinning disks, up for fast arrays.
pub const DEFAULT_BLOB_IO_CONCURRENCY: usize = 64;

impl LocalBlobStore {
    /// Open (creating the staging area) a blob store rooted at `data_root`.
    ///
    /// When built with the `io-uring` feature, the durable single-object staging write path
    /// (create tmp → write → fsync → rename → fsync dir) runs on the io_uring executor by
    /// default; without the feature it always uses `tokio::fs`. Use [`Self::with_io_uring`] to
    /// override the choice explicitly (e.g. to compare backends in a benchmark).
    ///
    /// # Errors
    /// Returns a [`BlobError`] if the staging directory cannot be created.
    pub async fn open(data_root: impl Into<PathBuf>) -> Result<Self, BlobError> {
        let data_root = data_root.into();
        tokio::fs::create_dir_all(data_root.join(STAGING).join("multipart"))
            .await
            .map_err(io_err)?;
        Ok(Self {
            data_root: Arc::new(data_root),
            use_uring: cfg!(feature = "io-uring"),
            read_permits: Arc::new(tokio::sync::Semaphore::new(DEFAULT_BLOB_IO_CONCURRENCY)),
            write_permits: Arc::new(tokio::sync::Semaphore::new(DEFAULT_BLOB_IO_CONCURRENCY)),
            dir_sync: Arc::new(commit::DirSyncCoalescer::spawn()),
            small_read_max: SMALL_READ_MAX,
            encrypted_without_key: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Cumulative number of reads refused because the blob is an encrypted CRNB container and the
    /// caller passed no data key (see the `encrypted_without_key` field). Monotonic for the life of
    /// the process and shared across clones of the store; the server publishes it as
    /// `cairn_blob_encrypted_without_key_total`.
    ///
    /// A non-zero value means either a caller lost a DEK it should have resolved (the replication
    /// bug this guard closes) **or** the false-positive class documented in this crate's
    /// `CLAUDE.md`: an object whose body is the verbatim bytes of somebody else's encrypted blob
    /// file (an `rclone`/`aws s3 sync` backup of a `CAIRN_DATA_DIR` into a bucket).
    #[must_use]
    pub fn encrypted_without_key_total(&self) -> u64 {
        self.encrypted_without_key
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Override the small-object GET fast-path size bound (see the `small_read_max` field). An
    /// uncompressed blob at or below `bytes` is read whole in the probe open and served inline;
    /// above it, the streamed read (with the zero-copy hint) is used. Defaults to [`SMALL_READ_MAX`].
    /// Primarily for benchmarks and tests: setting it to `0` forces the streaming path so the two
    /// read paths can be A/B-compared on the same object size.
    #[must_use]
    pub fn with_small_read_max(mut self, bytes: u64) -> Self {
        self.small_read_max = bytes;
        self
    }

    /// Override whether the io_uring staging write path is used. Has effect only when the
    /// `io-uring` feature is compiled in; without it the store always uses `tokio::fs` and this
    /// returns the store unchanged. Primarily for benchmarks and tests that want to exercise a
    /// specific backend deterministically.
    #[must_use]
    pub fn with_io_uring(mut self, enabled: bool) -> Self {
        self.use_uring = enabled && cfg!(feature = "io-uring");
        self
    }

    /// Set the bound on concurrent blob **write** transfers — stage/part/assemble (ARCH 7.4). A
    /// value of `0` is treated as `1` so the store always makes progress. Defaults to
    /// [`DEFAULT_BLOB_IO_CONCURRENCY`]. See [`with_read_io_pool_size`](Self::with_read_io_pool_size)
    /// for the separate read pool.
    #[must_use]
    pub fn with_io_pool_size(mut self, permits: usize) -> Self {
        self.write_permits = Arc::new(tokio::sync::Semaphore::new(permits.max(1)));
        self
    }

    /// Set the bound on concurrent blob **read** transfers (ARCH 7.4). Separate from the write pool
    /// so slow-reading clients (which hold a read permit for the whole client-paced download) can
    /// never starve writes (audit 2026-07). A value of `0` is treated as `1`. Defaults to
    /// [`DEFAULT_BLOB_IO_CONCURRENCY`].
    #[must_use]
    pub fn with_read_io_pool_size(mut self, permits: usize) -> Self {
        self.read_permits = Arc::new(tokio::sync::Semaphore::new(permits.max(1)));
        self
    }

    /// Acquire one blob-I/O permit, bounding concurrent transfers (ARCH 7.4). Held by the caller
    /// for the duration of its file I/O. The semaphore is never closed, so this never errors in
    /// practice; the `Result` is for forward-compatibility with a shutdown that closes it.
    async fn acquire_io(&self) -> Result<tokio::sync::SemaphorePermit<'_>, BlobError> {
        self.write_permits
            .acquire()
            .await
            .map_err(|_| BlobError::Io("blob write I/O pool closed".to_owned()))
    }

    /// Acquire an owned blob-I/O permit that can be moved into a spawned read task and dropped when
    /// the transfer finishes (the read body is streamed after the call returns, so the permit must
    /// outlive this function — hence the owned form keyed on the shared `Arc<Semaphore>`).
    async fn acquire_io_owned(&self) -> Result<tokio::sync::OwnedSemaphorePermit, BlobError> {
        self.read_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BlobError::Io("blob read I/O pool closed".to_owned()))
    }

    fn resolve(&self, sp: &StoragePath) -> Result<PathBuf, BlobError> {
        let rel = Path::new(sp.as_str());
        if rel.is_absolute()
            || rel.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(BlobError::Io("unsafe storage path".into()));
        }
        Ok(self.data_root.join(rel))
    }

    /// Verify that the data root and its staging directory live on a single filesystem, as the
    /// commit protocol's atomic rename requires (ARCH 2.4, 9.2): a cross-device rename fails
    /// with `EXDEV` and would break durability. The server calls this at startup so a
    /// misconfiguration (for example a staging directory bind-mounted from another filesystem)
    /// fails fast with a clear diagnostic instead of a generic error at the first write.
    ///
    /// # Errors
    /// Returns [`BlobError`] if either path cannot be stat'd, or [`BlobError::Io`] with a
    /// descriptive message if the two reside on different filesystems.
    #[cfg(unix)]
    pub fn check_single_filesystem(&self) -> Result<(), BlobError> {
        use std::os::unix::fs::MetadataExt;
        let root = &**self.data_root;
        let staging = self.data_root.join(STAGING);
        let root_dev = std::fs::metadata(root).map_err(io_err)?.dev();
        let staging_dev = std::fs::metadata(&staging).map_err(io_err)?.dev();
        if root_dev != staging_dev {
            return Err(BlobError::Io(format!(
                "data root {} (dev {root_dev}) and staging directory {} (dev {staging_dev}) are on \
                 different filesystems; atomic rename requires one filesystem (ARCH 2.4)",
                root.display(),
                staging.display(),
            )));
        }
        Ok(())
    }
}

pub(crate) async fn fsync_dir(dir: &Path) -> Result<(), BlobError> {
    let d = tokio::fs::File::open(dir).await.map_err(io_err)?;
    d.sync_all().await.map_err(io_err)?;
    Ok(())
}

/// Ensure a per-bucket directory exists, fsyncing `data_root` when the directory entry is newly
/// created (F-1, ARCH 8.2 step 4). `create_dir_all` makes the directory durable only once its
/// own parent records the new entry: a power loss after the rename but before `data_root` is
/// fsynced can lose the bucket directory entry, orphaning the committed blob inside it. We detect
/// newness by probing for existence first, so the extra parent fsync is paid only on the rare
/// first write into a bucket rather than on every commit.
async fn ensure_bucket_dir(data_root: &Path, bucket_dir: &Path) -> Result<(), BlobError> {
    let existed = tokio::fs::try_exists(bucket_dir).await.map_err(io_err)?;
    tokio::fs::create_dir_all(bucket_dir)
        .await
        .map_err(io_err)?;
    if !existed {
        // The bucket directory entry now lives in data_root; make that entry durable.
        fsync_dir(data_root).await?;
    }
    Ok(())
}

/// Stream a body into a staging file, applying compression and hashing in one pass. The staging
/// sink abstracts the file backend (default `tokio::fs`, or io_uring under the feature), so this
/// transform is identical on both paths.
async fn write_staged(
    file: &mut Staging,
    mut body: cairn_types::BodyStream,
    opts: &StageOptions,
) -> Result<
    (
        u64,
        u64,
        String,
        Vec<cairn_types::object::ChecksumValue>,
        CompressionDescriptor,
    ),
    BlobError,
> {
    let compress = match opts.compression {
        Some(pol) if !is_precompressed(&opts.content_type) => Some(pol),
        _ => None,
    };
    let mut hashers = Hashers::new(&opts.extra_checksums);
    let mut logical: u64 = 0;
    let mut physical: u64 = 0;

    // The self-describing CRNB block container is needed whenever we compress OR encrypt: SSE-S3
    // encrypts each physical block after compression, so an encrypted-but-uncompressed object still
    // flows through the block encoder with `CompressionAlgorithm::None`. The MD5/ETag is computed
    // over the plaintext (here, via `hashers`) before any transform, so it is identical with or
    // without compression/encryption (ARCH 21.1, 27).
    let block_pol = compress.or_else(|| {
        opts.encryption.map(|_| CompressionPolicy {
            algorithm: CompressionAlgorithm::None,
            block_size: DEFAULT_ENCRYPTED_BLOCK_SIZE,
        })
    });

    if let Some(pol) = block_pol {
        let mut enc = match opts.encryption {
            Some(dek) => BlockEncoder::new_encrypted(pol.algorithm, pol.block_size, dek),
            None => BlockEncoder::new(pol.algorithm, pol.block_size),
        };
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            logical += chunk.len() as u64;
            if logical > opts.size_ceiling {
                return Err(BlobError::SizeExceeded);
            }
            hashers.update(&chunk);
            let phys = enc.feed(&chunk);
            file.write_all(&phys).await?;
            physical += phys.len() as u64;
        }
        let tail = enc.finish()?;
        file.write_all(&tail).await?;
        physical += tail.len() as u64;
        let (md5, checks) = hashers.finalize();
        // The descriptor records the logical compression of the object. Encryption is recorded on
        // the metadata row's sse_descriptor, not here, so an uncompressed-but-encrypted object is
        // still `Uncompressed` to readers that only care about the compression algorithm.
        let descriptor = match compress {
            Some(_) => CompressionDescriptor::Compressed {
                algorithm: pol.algorithm,
                block_size: pol.block_size,
            },
            None => CompressionDescriptor::Uncompressed,
        };
        Ok((logical, physical, md5, checks, descriptor))
    } else {
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            logical += chunk.len() as u64;
            if logical > opts.size_ceiling {
                return Err(BlobError::SizeExceeded);
            }
            hashers.update(&chunk);
            file.write_all(&chunk).await?;
            physical += chunk.len() as u64;
        }
        let (md5, checks) = hashers.finalize();
        Ok((
            logical,
            physical,
            md5,
            checks,
            CompressionDescriptor::Uncompressed,
        ))
    }
}

/// Stream a read of `[offset, offset+len)` logical bytes from a blob file, decompressing (and,
/// when `dek` is supplied, decrypting) only the overlapping blocks. Runs the blocking file work off
/// the reactor and yields chunks.
/// The blocking body of a streamed read: open the file and push chunks of `[offset, offset+len)`
/// into `tx`, decompressing/decrypting per block when `compressed`. A send failure (the consumer
/// dropped the body) ends the transfer early. Factored out of [`read_stream`] so the open + read
/// happen only when the stream is actually polled.
fn stream_blob(
    path: &Path,
    compressed: bool,
    dek: Option<[u8; 32]>,
    offset: u64,
    len: u64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, BlobError>>,
) -> Result<(), BlobError> {
    use std::io::{Read, Seek, SeekFrom};
    if compressed {
        let f = std::fs::File::open(path).map_err(io_err)?;
        let mut reader = CompressedReader::open_with_dek(f, dek)?;
        let bs = reader.block_size();
        let end = offset.saturating_add(len).min(reader.logical_len());
        if bs == 0 || offset >= end {
            return Ok(());
        }
        let first = offset / bs;
        let last = (end - 1) / bs;
        for b in first..=last {
            let bstart = b * bs;
            let lo = offset.max(bstart);
            let hi = end.min(bstart + bs);
            let data = reader.read_range(lo, hi - lo)?;
            if !data.is_empty() && tx.blocking_send(Ok(Bytes::from(data))).is_err() {
                return Ok(());
            }
        }
    } else {
        let mut f = std::fs::File::open(path).map_err(io_err)?;
        f.seek(SeekFrom::Start(offset)).map_err(io_err)?;
        let mut remaining = len;
        let mut buf = vec![0u8; READ_CHUNK];
        while remaining > 0 {
            let want = (remaining as usize).min(READ_CHUNK);
            let n = f.read(&mut buf[..want]).map_err(io_err)?;
            if n == 0 {
                break;
            }
            if tx
                .blocking_send(Ok(Bytes::copy_from_slice(&buf[..n])))
                .is_err()
            {
                return Ok(());
            }
            remaining -= n as u64;
        }
    }
    Ok(())
}

/// The lazy state of a streamed read: the open + read is deferred until the body is first polled,
/// so a request that takes the kernel zero-copy fast path (and drops this body unpolled) performs
/// no file open and releases its I/O permit immediately (Phase 2.5).
enum StreamSrc {
    Pending(
        (
            PathBuf,
            bool,
            Option<[u8; 32]>,
            u64,
            u64,
            tokio::sync::OwnedSemaphorePermit,
        ),
    ),
    Running(tokio::sync::mpsc::Receiver<Result<Bytes, BlobError>>),
}

fn read_stream(
    path: PathBuf,
    compressed: bool,
    dek: Option<[u8; 32]>,
    offset: u64,
    len: u64,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> cairn_types::BlobStream {
    let initial = StreamSrc::Pending((path, compressed, dek, offset, len, permit));
    Box::pin(futures_util::stream::unfold(initial, |state| async move {
        let mut rx = match state {
            StreamSrc::Pending((path, compressed, dek, offset, len, permit)) => {
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, BlobError>>(4);
                tokio::task::spawn_blocking(move || {
                    // The permit is held for the whole transfer so it counts against the blob-I/O
                    // bound (ARCH 7.4), then released when this task ends.
                    let _permit = permit;
                    if let Err(e) = stream_blob(&path, compressed, dek, offset, len, &tx) {
                        let _ = tx.blocking_send(Err(e));
                    }
                });
                rx
            }
            StreamSrc::Running(rx) => rx,
        };
        rx.recv().await.map(|item| (item, StreamSrc::Running(rx)))
    }))
}

/// Feed one plaintext chunk of an assembled part through the shared downstream: enforce the running
/// size ceiling, hash the plaintext (the ETag/checksum basis), then write it through the optional
/// object block encoder/encrypter into the staging sink. Both the plaintext-part (raw-read) and the
/// encrypted-part (decrypt-on-read) branches of `assemble_into` converge here so the ceiling/hash/
/// encode logic is written once (ARCH 27).
async fn feed_assembled_chunk(
    sink: &mut Staging,
    chunk: &[u8],
    hashers: &mut Hashers,
    enc: &mut Option<BlockEncoder>,
    logical: &mut u64,
    physical: &mut u64,
    size_ceiling: u64,
) -> Result<(), BlobError> {
    *logical += chunk.len() as u64;
    // Enforce the ceiling on the actual bytes read, so a part whose on-disk size exceeds its recorded
    // size can't inflate the object past the limit (audit 2026-07).
    if *logical > size_ceiling {
        return Err(BlobError::SizeExceeded);
    }
    hashers.update(chunk);
    match enc {
        Some(e) => {
            let phys = e.feed(chunk);
            sink.write_all(&phys).await?;
            *physical += phys.len() as u64;
        }
        None => {
            sink.write_all(chunk).await?;
            *physical += chunk.len() as u64;
        }
    }
    Ok(())
}

impl LocalBlobStore {
    /// Read each part in order, hashing the plaintext, applying the (optional) block
    /// encoder/encrypter, and streaming the physical bytes into the staging sink. A part staged
    /// encrypted (`PartRef.dek == Some`, ARCH 27) is decrypted on read through the same CRNB reader
    /// GET uses, so `assemble` always sees plaintext before it re-encodes under the object DEK.
    /// Factored out of `assemble` so the caller can `abort` the sink on any error without duplicating
    /// the unlink.
    #[allow(clippy::too_many_arguments)]
    async fn assemble_into(
        &self,
        sink: &mut Staging,
        parts: &[PartRef],
        hashers: &mut Hashers,
        enc: &mut Option<BlockEncoder>,
        logical: &mut u64,
        physical: &mut u64,
        size_ceiling: u64,
    ) -> Result<(), BlobError> {
        use tokio::io::AsyncReadExt;
        for part in parts {
            let part_path = self.resolve(&part.storage_path)?;
            match part.dek {
                // Plaintext / pre-v21 part: raw read, unchanged.
                None => {
                    let mut f = tokio::fs::File::open(&part_path)
                        .await
                        .map_err(|_| BlobError::NotFound)?;
                    let mut buf = vec![0u8; READ_CHUNK];
                    loop {
                        let n = f.read(&mut buf).await.map_err(io_err)?;
                        if n == 0 {
                            break;
                        }
                        feed_assembled_chunk(
                            sink,
                            &buf[..n],
                            hashers,
                            enc,
                            logical,
                            physical,
                            size_ceiling,
                        )
                        .await?;
                    }
                }
                // Encrypted part: decrypt-on-read through the same CRNB reader GET uses, off the
                // reactor via `spawn_blocking` feeding a bounded channel (mirrors `read_stream`). A
                // wrong/tampered/truncated part fails per-block GCM auth inside `stream_blob` and
                // surfaces here as `BlobError::Corruption`, which `assemble` turns into a `sink.abort()`
                // — no orphan, no plaintext, no partial object.
                Some(dek) => {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, BlobError>>(4);
                    let path = part_path.clone();
                    let size = part.size;
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = stream_blob(&path, true, Some(dek), 0, size, &tx) {
                            let _ = tx.blocking_send(Err(e));
                        }
                    });
                    while let Some(item) = rx.recv().await {
                        let chunk = item?;
                        feed_assembled_chunk(
                            sink,
                            &chunk,
                            hashers,
                            enc,
                            logical,
                            physical,
                            size_ceiling,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn stage(
        &self,
        bucket: &BucketName,
        body: cairn_types::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        // Bound concurrent blob *copy* I/O (ARCH 7.4). Held through the data copy and the per-file
        // durability (fdatasync + rename), then released BEFORE the coalesced directory-fsync
        // barrier (Phase 2.4) so a PUT awaiting that barrier no longer occupies blob-I/O concurrency
        // that concurrent GETs need — reads stop queueing behind writers' fsync barriers. The
        // barrier itself is bounded by the coalescer (one fsync per directory per batch), not by
        // this semaphore, and a waiter only parks on a oneshot, holding no blocking thread.
        let copy_permit = self.acquire_io().await?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let staging = self.data_root.join(STAGING).join(format!("{id}.tmp"));
        let bucket_dir = self.data_root.join(bucket.as_str());
        let final_path = bucket_dir.join(&id);
        let storage_path = StoragePath::from_string(format!("{}/{}", bucket.as_str(), id));

        let mut sink = Staging::create(staging, self.use_uring, opts.content_length).await?;
        let outcome = write_staged(&mut sink, body, &opts).await;
        let (logical, physical, md5, checksums, descriptor) = match outcome {
            Ok(v) => v,
            Err(e) => {
                sink.abort().await;
                return Err(e);
            }
        };
        // Create (and fsync the parent of) the bucket directory *before* the rename, so the
        // commit can rename into an already-durable directory entry (F-1, ARCH 8.2 step 4). The
        // commit performs: fdatasync the staged file → rename. The destination-directory fsync that
        // makes the new entry durable is then issued through the coalescer, which batches it with
        // any concurrent PUTs into the same bucket into a single fsync. `sync_dir` resolves only
        // after that fsync completes, so the blob is fully durable before we proceed.
        ensure_bucket_dir(&self.data_root, &bucket_dir).await?;
        sink.commit(&final_path).await?;
        // Release the copy permit before parking on the coalesced directory-fsync barrier.
        drop(copy_permit);
        self.dir_sync.sync_dir(&bucket_dir).await?;
        // The crash window the durability ordering protects: the blob is now durable but no
        // metadata row references it yet. A crash here leaves an orphan that reconcile reclaims.
        fail::fail_point!("blob_after_durable");

        Ok(StagedBlob {
            storage_path,
            size_logical: logical,
            size_physical: physical,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums,
            compression: descriptor,
        })
    }

    async fn open_raw(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        cipher: BlobCipher,
        compression: &CompressionDescriptor,
    ) -> Result<BlobReadHandle, BlobError> {
        // Translate the named cipher to the internal `Option<[u8; 32]>` once, here, so every use of
        // `dek` below (the framing decision, the DEK-less refusal guard, the CompressedReader open)
        // is byte-for-byte what it was: `KnownPlaintext` => `None`, `Dek(k)` => `Some(k)`. The
        // fail-closed semantics MUST NOT change — a `KnownPlaintext` open of an encrypted container
        // still hits the refusal guard and errors, never streams ciphertext.
        let dek = cipher.dek();
        let file_path = self.resolve(path)?;
        // The blob is a self-describing CRNB block container iff it was compressed OR encrypted at
        // write time (audit #18). Decide that from the caller's stored compression descriptor and
        // the DEK — both authoritative — rather than sniffing the 34-byte trailer magic, which an
        // uncompressed object's own bytes can collide with.
        let is_container =
            dek.is_some() || !matches!(compression, CompressionDescriptor::Uncompressed);
        // One open + one fstat handles existence, length, and compression detection together,
        // replacing the prior try_exists + two metadata stats + a separate compression-probe open
        // (Phase 2.5). On the common uncompressed branch the opened fd is handed back to serve the
        // zero-copy sendfile path, so an uncompressed GET no longer reopens the same file.
        // At or below `small_read_max` an uncompressed object is read WHOLE in the single probe open
        // and served as one `Bytes` — skipping the second file open and the per-chunk `mpsc` streaming
        // channel that otherwise dominate a tiny GET (such an object is below the kernel sendfile floor
        // anyway, so it would never take the zero-copy path). This is the small-object GET fast path.
        let small_read_max = self.small_read_max;
        let probe_path = file_path.clone();
        let refused = self.encrypted_without_key.clone();
        let (compressed, logical_len, reuse_file, whole) = tokio::task::spawn_blocking(
            move || -> Result<(bool, u64, Option<std::fs::File>, Option<Bytes>), BlobError> {
                use std::io::{Read, Seek, SeekFrom};
                let mut f = match std::fs::File::open(&probe_path) {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Err(BlobError::NotFound);
                    }
                    Err(e) => return Err(io_err(e)),
                };
                let file_len = f.metadata().map_err(io_err)?.len();
                // Whether this blob is a CRNB block container is authoritative from the caller's
                // stored descriptor + DEK (`is_container`, audit #18), so there is no trailer sniff
                // to misfire on an uncompressed object whose own bytes happen to end in "CRNB".
                let compressed = is_container;
                // Defence in depth: framing is decided from the DEK ARGUMENT, so a caller that
                // forgets the key for an encrypted-but-uncompressed blob would otherwise stream raw
                // ciphertext as if it were the plaintext body — silently, at exactly the right
                // length. (That is precisely how replication shipped ciphertext to mirrors.) Before
                // taking the plaintext branch, cross-check the trailer and REFUSE if the blob is
                // demonstrably an encrypted container. We only ever refuse — never parse as a
                // container — so a plaintext blob that merely ends in the magic still reads (the
                // predicate additionally requires the full layout identity to hold).
                //
                // The trailer is fetched with ONE POSITIONED read (`pread`) rather than
                // seek-to-end / read / seek-back: the plaintext branch is the tuned hot path
                // (including the small-object fast path), so the guard must not cost it two extra
                // `lseek`s per GET — and, more importantly, the rewind was an *unstated* invariant
                // that both the `read_to_end` branch and the reused zero-copy fd silently depended
                // on. A positioned read leaves the file offset untouched, so that dependency is
                // gone rather than merely satisfied. `read_exact_at` is a safe `std` API, so
                // `#![forbid(unsafe_code)]` still holds.
                if !compressed && file_len >= compress::TRAILER_BYTES as u64 {
                    let mut t = [0u8; compress::TRAILER_BYTES];
                    let off = file_len - compress::TRAILER_BYTES as u64;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::FileExt;
                        f.read_exact_at(&mut t, off).map_err(io_err)?;
                    }
                    // Portable fallback (the crate targets unix in practice — `raw_io` keeps the
                    // same shape). Seek/read/rewind, restoring the offset the branches below want.
                    #[cfg(not(unix))]
                    {
                        f.seek(SeekFrom::Start(off)).map_err(io_err)?;
                        f.read_exact(&mut t).map_err(io_err)?;
                        f.seek(SeekFrom::Start(0)).map_err(io_err)?;
                    }
                    if compress::is_encrypted_container_trailer(&t, file_len) {
                        refused.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!(
                            path = %probe_path.display(),
                            "refusing to read an encrypted blob without a data key"
                        );
                        return Err(BlobError::Corruption(
                            "encrypted blob read without a data key".into(),
                        ));
                    }
                }
                if compressed {
                    // Parse the header for the logical length; the fd is consumed by the reader and
                    // not reused (compressed/encrypted blobs never take the kernel fast path).
                    f.seek(SeekFrom::Start(0)).map_err(io_err)?;
                    let logical = CompressedReader::open_with_dek(f, dek)?.logical_len();
                    Ok((true, logical, None, None))
                } else if file_len <= small_read_max {
                    // Small uncompressed object: read it whole here (one open, one read). The range
                    // is sliced from this buffer below — no second open, no streaming channel.
                    let mut buf = Vec::with_capacity(file_len as usize);
                    f.read_to_end(&mut buf).map_err(io_err)?;
                    Ok((false, file_len, None, Some(Bytes::from(buf))))
                } else {
                    // Larger uncompressed object: the file length is the logical length, and the open
                    // fd is reused as the zero-copy source below.
                    Ok((false, file_len, Some(f), None))
                }
            },
        )
        .await
        .map_err(|e| BlobError::Io(e.to_string()))??;

        let (offset, len, content_range) = match range {
            Some(r) => {
                let offset = r.offset.min(logical_len);
                let len = r.length.min(logical_len - offset);
                let cr = ContentRange {
                    start: offset,
                    end: (offset + len).saturating_sub(1).max(offset),
                    total: logical_len,
                };
                (offset, len, Some(cr))
            }
            None => (0, logical_len, None),
        };

        // Small uncompressed object already read in the probe: serve it as a single `Bytes` (the
        // requested range sliced from the in-memory buffer), with no second open and no streaming
        // channel. It still holds a read permit — see below — so a flood of slow readers requesting
        // small objects is bounded exactly like the streamed path (audit 2026-07). Otherwise fall
        // back to the streamed read.
        let (body, zero_copy) = if let Some(bytes) = whole {
            // Clamp the range to the bytes ACTUALLY read: for a normal immutable blob this is exactly
            // [offset, offset+len), but a truncated/corrupted on-disk blob (shorter than its fstat
            // length — fs corruption or a bit-rotted file) must NOT panic the read path. Serve what is
            // present, as the streamed path does; the integrity scrub is what flags the corruption.
            let avail = bytes.len() as u64;
            let start = offset.min(avail) as usize;
            let end = (offset + len).min(avail) as usize;
            let slice = bytes.slice(start..end);
            // Hold a read permit across BOTH polls of this two-state stream: the first poll yields
            // the bytes while still holding the permit in the next state, and only the SECOND poll
            // (the caller checking for end-of-stream, which on a slow/stalled connection may not
            // happen until the first chunk has actually drained to the socket) drops it. Without
            // this, a small-object GET fast path is unbounded — a flood of slow readers of
            // below-floor objects could hold far more transient memory than `read_permits` was
            // sized to allow (a read-side slow-loris, the exact class this pool exists to bound).
            let permit = self.acquire_io_owned().await?;
            enum SmallState {
                Item(Bytes, tokio::sync::OwnedSemaphorePermit),
                Held(tokio::sync::OwnedSemaphorePermit),
            }
            let body: cairn_types::BlobStream = Box::pin(futures_util::stream::unfold(
                SmallState::Item(slice, permit),
                |state| async move {
                    match state {
                        SmallState::Item(b, permit) => Some((Ok(b), SmallState::Held(permit))),
                        SmallState::Held(_permit) => None,
                    }
                },
            ));
            (body, None)
        } else {
            // Hold a blob-I/O permit for the streamed transfer (ARCH 7.4); released when the read
            // task finishes. (The kernel sendfile fast path below is bounded separately by the server.)
            let permit = self.acquire_io_owned().await?;
            let body = read_stream(file_path.clone(), compressed, dek, offset, len, permit);
            // Uncompressed, plaintext blobs may take the kernel file-to-socket fast path, reusing the
            // fd the probe opened. Encrypted blobs are always block-formatted (so `compressed` is
            // true), so `reuse_file` is `None` for them and the kernel never sees ciphertext.
            let zero_copy = reuse_file.map(|f| ZeroCopyRead {
                file: Arc::new(f),
                offset,
                len,
            });
            (body, zero_copy)
        };

        Ok(BlobReadHandle {
            logical_len: len,
            content_range,
            body,
            zero_copy,
        })
    }

    async fn probe(&self, path: &StoragePath) -> Result<BlobProbe, BlobError> {
        // Presence + basic framing only: a single `stat`, no body open, no CompressedReader, no
        // DEK. The physical (on-disk) length is all a container-free probe can honestly report; a
        // healthy encrypted blob therefore probes present (Ok), never Corruption — presence is not
        // decryptability. Absence maps to NotFound (the dangling-row case `--repair` deletes); a
        // real I/O fault surfaces as the corresponding BlobError.
        let file_path = self.resolve(path)?;
        match tokio::fs::metadata(&file_path).await {
            Ok(m) => Ok(BlobProbe {
                physical_len: m.len(),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(BlobError::NotFound),
            Err(e) => Err(io_err(e)),
        }
    }

    async fn delete(&self, path: &StoragePath) -> Result<(), BlobError> {
        let file_path = self.resolve(path)?;
        match tokio::fs::remove_file(&file_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    async fn stage_part(
        &self,
        upload: &UploadId,
        part_number: u16,
        body: cairn_types::BodyStream,
        checksums: ChecksumSet,
        size_ceiling: u64,
        encryption: Option<[u8; 32]>,
    ) -> Result<StagedPart, BlobError> {
        let _permit = self.acquire_io().await?;
        let dir = self
            .data_root
            .join(STAGING)
            .join("multipart")
            .join(upload.as_str());
        tokio::fs::create_dir_all(&dir).await.map_err(io_err)?;
        let id = format!("{part_number:05}-{}", uuid::Uuid::new_v4().simple());
        let path = dir.join(&id);
        // A part is staged as ciphertext when `encryption` is Some (SSE / bucket-default / at-rest
        // multipart, ARCH 27) so nothing plaintext hits disk; otherwise it is a plaintext intermediate
        // artifact (compression is still deferred to `assemble`). Either way the requested
        // supplementary checksums ARE computed here, over the part's plaintext in the same streaming
        // pass as the MD5 (before any encrypt transform), so the caller can validate a client
        // `x-amz-checksum-*` header and persist the per-part digest for composition at
        // CompleteMultipartUpload — the ETag/checksum basis is identical with or without encryption.
        let opts = StageOptions {
            extra_checksums: checksums,
            size_ceiling,
            content_type: String::new(),
            encryption,
            ..StageOptions::default()
        };
        // A part's length is not known to this seam, so no preallocation here; the assembled blob
        // (whose size is the sum of the parts) is preallocated in `assemble`.
        let mut sink = Staging::create(path, self.use_uring, None).await?;
        let (logical, _phys, md5, checks, _desc) = match write_staged(&mut sink, body, &opts).await
        {
            Ok(v) => v,
            Err(e) => {
                sink.abort().await;
                return Err(e);
            }
        };
        sink.fsync_in_place().await?;
        // fsync the session directory so the new part's directory entry is durable. Without this, a
        // part that was acknowledged 200 OK could lose its dirent on power loss even though its bytes
        // were fdatasync'd, so a later CompleteMultipartUpload fails NoSuchUpload — a durability-
        // contract violation (ARCH 8.1) the single-part path already guards against by fsyncing the
        // bucket dir after rename (F-1). Routed through the coalescer so concurrent part uploads into
        // the same session share one fsync (audit 2026-07).
        self.dir_sync.sync_dir(&dir).await?;
        Ok(StagedPart {
            storage_path: StoragePath::from_string(format!(
                "{}/multipart/{}/{}",
                STAGING,
                upload.as_str(),
                id
            )),
            size: logical,
            md5_hex: md5,
            checksums: checks,
        })
    }

    async fn assemble(
        &self,
        bucket: &BucketName,
        parts: &[PartRef],
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        // As in `stage`, the copy permit is released before the coalesced directory-fsync barrier
        // (Phase 2.4) so the assembly does not hold blob-I/O concurrency through its fsync wait.
        let copy_permit = self.acquire_io().await?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let staging = self.data_root.join(STAGING).join(format!("{id}.tmp"));
        let bucket_dir = self.data_root.join(bucket.as_str());
        let final_path = bucket_dir.join(&id);
        let storage_path = StoragePath::from_string(format!("{}/{}", bucket.as_str(), id));

        let compress = match opts.compression {
            Some(pol) if !is_precompressed(&opts.content_type) => Some(pol),
            _ => None,
        };
        // As in `write_staged`, the CRNB block container is used when we compress OR encrypt; an
        // encrypted-but-uncompressed assembly uses `CompressionAlgorithm::None` with a default block
        // size. The multipart ETag is computed from the part MD5s by the caller, not from `hasher`,
        // but the plaintext MD5 here is still computed before any transform.
        let block_pol = compress.or_else(|| {
            opts.encryption.map(|_| CompressionPolicy {
                algorithm: CompressionAlgorithm::None,
                block_size: DEFAULT_ENCRYPTED_BLOCK_SIZE,
            })
        });
        // The assembled object's size is the sum of the parts' plaintext sizes — known up front, so
        // preallocate the staging file to place it contiguously (ARCH 7.5).
        let assembled_len: u64 = parts.iter().map(|p| p.size).sum();
        // Enforce the object-size ceiling on the multipart total, exactly as the single-PUT path does
        // on its streamed bytes (write_staged). Without this a multipart upload of up to ~10000 parts
        // each near the per-part cap bypasses CAIRN_MAX_OBJECT_SIZE — a limit-bypass / disk+memory DoS
        // (audit 2026-07). Checked up front on the recorded sizes; assemble_into also enforces on the
        // running sum in case a part's on-disk size disagrees with its record.
        if assembled_len > opts.size_ceiling {
            return Err(BlobError::SizeExceeded);
        }
        let mut sink = Staging::create(staging, self.use_uring, Some(assembled_len)).await?;
        // Hash the assembled plaintext once, computing the MD5/ETag basis plus any supplementary
        // checksums the caller requested via `opts.extra_checksums` (a whole-object FULL_OBJECT
        // recompute at CompleteMultipartUpload). With no extra checksums this is byte-for-byte the
        // same work as the previous bare-MD5 path, so the default multipart path is unchanged.
        let mut hashers = Hashers::new(&opts.extra_checksums);
        let mut logical: u64 = 0;
        let mut physical: u64 = 0;
        let mut enc = block_pol.map(|p| match opts.encryption {
            Some(dek) => BlockEncoder::new_encrypted(p.algorithm, p.block_size, dek),
            None => BlockEncoder::new(p.algorithm, p.block_size),
        });

        // The assemble write path mirrors `stage`: on any error before commit, unlink the staged
        // tmp via the same backend that created it, then propagate. A small closure keeps the
        // sink's `abort` reachable from each fallible step without a flag dance.
        let assembled = self
            .assemble_into(
                &mut sink,
                parts,
                &mut hashers,
                &mut enc,
                &mut logical,
                &mut physical,
                opts.size_ceiling,
            )
            .await;
        if let Err(e) = assembled {
            sink.abort().await;
            return Err(e);
        }
        let descriptor = if let Some(e) = enc {
            let tail = e.finish()?;
            if let Err(err) = sink.write_all(&tail).await {
                sink.abort().await;
                return Err(err);
            }
            physical += tail.len() as u64;
            // Record `Compressed` only when a compression policy was actually in force; an
            // encryption-only block container leaves the logical compression as `Uncompressed`.
            match compress {
                Some(pol) => CompressionDescriptor::Compressed {
                    algorithm: pol.algorithm,
                    block_size: pol.block_size,
                },
                None => CompressionDescriptor::Uncompressed,
            }
        } else {
            CompressionDescriptor::Uncompressed
        };

        ensure_bucket_dir(&self.data_root, &bucket_dir).await?;
        sink.commit(&final_path).await?;
        // Release the copy permit before parking on the coalesced directory-fsync barrier.
        drop(copy_permit);
        self.dir_sync.sync_dir(&bucket_dir).await?;
        fail::fail_point!("blob_after_assemble");

        let (md5_hex, checksums) = hashers.finalize();
        Ok(StagedBlob {
            storage_path,
            size_logical: logical,
            size_physical: physical,
            etag: ETag::from_md5_hex(md5_hex.clone()),
            md5_hex,
            checksums,
            compression: descriptor,
        })
    }

    async fn delete_session(&self, upload: &UploadId) -> Result<(), BlobError> {
        let dir = self
            .data_root
            .join(STAGING)
            .join("multipart")
            .join(upload.as_str());
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    async fn reconcile(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
    ) -> Result<ReconcileReport, BlobError> {
        // The trait method is frozen, so `now` cannot be a parameter; obtain it once here from
        // the system clock and thread it explicitly into the reconcile core so the staging
        // safety-margin logic stays unit-testable with an injected `now`.
        let now = system_now();
        reconcile_inner(&self.data_root, oracle, opts, now).await
    }
}

/// The wall-clock now as a [`Timestamp`], saturating at the epoch for clocks set before 1970.
fn system_now() -> Timestamp {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    Timestamp::from_secs(secs)
}

/// The reconcile core, taking an explicit `now` so the staging safety margin is testable. It
/// walks the data root once, reconciles the staging area, and reconciles the per-bucket
/// directories with bounded concurrency (`opts.parallelism`), pruning any directories it empties.
async fn reconcile_inner(
    data_root: &Path,
    oracle: &dyn ReconcileOracle,
    opts: ReconcileOpts,
    now: Timestamp,
) -> Result<ReconcileReport, BlobError> {
    let mut report = ReconcileReport::default();
    // Collect bucket directories first (names only — bounded by the bucket count, not the
    // keyspace) so they can be reconciled concurrently while the staging area is handled inline.
    let mut bucket_dirs: Vec<(PathBuf, String)> = Vec::new();
    let mut entries = tokio::fs::read_dir(data_root).await.map_err(io_err)?;
    while let Some(entry) = entries.next_entry().await.map_err(io_err)? {
        let name = entry.file_name().to_string_lossy().to_string();
        if !entry.file_type().await.map_err(io_err)?.is_dir() {
            continue;
        }
        if name == STAGING {
            reconcile_staging(&entry.path(), oracle, opts, now, &mut report).await?;
            continue;
        }
        bucket_dirs.push((entry.path(), name));
    }

    // Reconcile buckets with bounded concurrency. The oracle is a borrowed `&dyn`, so the
    // futures are not `'static` and cannot move into a detached `JoinSet`; a `FuturesUnordered`
    // capped at `parallelism` gives the same bounded-concurrency, bounded-memory behaviour while
    // keeping the borrow. Each bucket still batches its membership checks internally, so the live
    // working set is at most `parallelism * batch_size` paths.
    let parallelism = opts.parallelism.max(1);
    let batch_size = opts.batch_size.max(1);
    let mut inflight: futures_util::stream::FuturesUnordered<_> =
        futures_util::stream::FuturesUnordered::new();
    let mut iter = bucket_dirs.into_iter();
    loop {
        while inflight.len() < parallelism {
            let Some((path, name)) = iter.next() else {
                break;
            };
            inflight.push(reconcile_bucket(
                path,
                name,
                oracle,
                batch_size,
                opts.staging_safety_margin_secs,
                now,
            ));
        }
        let Some(part) = inflight.next().await else {
            break;
        };
        merge_report(&mut report, part?);
    }
    Ok(report)
}

/// Fold a per-bucket reconcile report into the running total. `ReconcileReport` is a frozen type
/// in `cairn-types`, so the accumulation lives here rather than as a method on it.
fn merge_report(into: &mut ReconcileReport, part: ReconcileReport) {
    into.blobs_scanned += part.blobs_scanned;
    into.orphans_reclaimed += part.orphans_reclaimed;
    into.staging_cleaned += part.staging_cleaned;
    into.sessions_cleaned += part.sessions_cleaned;
    into.dirs_pruned += part.dirs_pruned;
    into.errors += part.errors;
}

/// Reconcile one per-bucket directory, reclaiming blobs no metadata row references, then pruning
/// the directory if reconciliation left it empty. Returns its own report so callers can run it
/// concurrently and fold the counts. Memory stays bounded: at most `batch_size` paths are held.
async fn reconcile_bucket(
    dir: PathBuf,
    bucket: String,
    oracle: &dyn ReconcileOracle,
    batch_size: u32,
    margin_secs: i64,
    now: Timestamp,
) -> Result<ReconcileReport, BlobError> {
    let mut report = ReconcileReport::default();
    let mut rd = tokio::fs::read_dir(&dir).await.map_err(io_err)?;
    let mut batch: Vec<(PathBuf, StoragePath)> = Vec::new();
    loop {
        let next = rd.next_entry().await.map_err(io_err)?;
        let is_end = next.is_none();
        if let Some(entry) = next {
            if entry.file_type().await.map_err(io_err)?.is_file() {
                let file = entry.file_name().to_string_lossy().to_string();
                let sp = StoragePath::from_string(format!("{bucket}/{file}"));
                batch.push((entry.path(), sp));
            }
        }
        if (is_end || batch.len() >= batch_size as usize) && !batch.is_empty() {
            report.blobs_scanned += batch.len() as u64;
            let paths: Vec<StoragePath> = batch.iter().map(|(_, sp)| sp.clone()).collect();
            let live = oracle
                .live_blobs(&paths)
                .await
                .map_err(|e| BlobError::Io(e.to_string()))?;
            for ((path, _), is_live) in batch.drain(..).zip(live) {
                if !is_live {
                    // Do not reclaim a blob younger than the safety margin: it may belong to an
                    // in-flight PUT whose metadata row has not yet committed, which the oracle would
                    // report as not-live (audit #7). Mirrors the staging safety margin.
                    if blob_too_young(&path, margin_secs, now).await {
                        continue;
                    }
                    if tokio::fs::remove_file(&path).await.is_ok() {
                        report.orphans_reclaimed += 1;
                    } else {
                        report.errors += 1;
                    }
                }
            }
        }
        if is_end {
            break;
        }
    }
    // Prune the bucket directory if reconciliation emptied it. `remove_dir` only succeeds on an
    // empty directory, so a concurrent write that re-populated it is left untouched.
    if prune_if_empty(&dir).await? {
        report.dirs_pruned += 1;
    }
    Ok(report)
}

/// Remove `dir` if it is empty, reporting whether it was pruned. A non-empty directory, or one a
/// race repopulated, is left in place; a missing directory counts as not pruned.
async fn prune_if_empty(dir: &Path) -> Result<bool, BlobError> {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(io_err(e)),
    };
    if rd.next_entry().await.map_err(io_err)?.is_some() {
        return Ok(false);
    }
    Ok(tokio::fs::remove_dir(dir).await.is_ok())
}

async fn reconcile_staging(
    staging: &Path,
    oracle: &dyn ReconcileOracle,
    opts: ReconcileOpts,
    now: Timestamp,
    report: &mut ReconcileReport,
) -> Result<(), BlobError> {
    let mut rd = match tokio::fs::read_dir(staging).await {
        Ok(rd) => rd,
        Err(_) => return Ok(()),
    };
    while let Some(entry) = rd.next_entry().await.map_err(io_err)? {
        let name = entry.file_name().to_string_lossy().to_string();
        let ft = entry.file_type().await.map_err(io_err)?;
        if ft.is_file() {
            // A leftover single-part staging artifact, possibly from a crash — but possibly an
            // in-flight write from a live process. Only reclaim it once it is older than the
            // safety margin, so an out-of-band reconcile against a live data dir cannot delete a
            // STAGING/{id}.tmp file that a concurrent write is still streaming into (ARCH 8.5).
            if staging_artifact_expired(&entry, opts.staging_safety_margin_secs, now).await? {
                if tokio::fs::remove_file(entry.path()).await.is_ok() {
                    report.staging_cleaned += 1;
                } else {
                    report.errors += 1;
                }
            }
        } else if ft.is_dir() && name == "multipart" {
            let mut sessions = tokio::fs::read_dir(entry.path()).await.map_err(io_err)?;
            while let Some(s) = sessions.next_entry().await.map_err(io_err)? {
                let upload = UploadId::from_string(s.file_name().to_string_lossy().to_string());
                let live = oracle
                    .live_session(&upload)
                    .await
                    .map_err(|e| BlobError::Io(e.to_string()))?;
                if !live && tokio::fs::remove_dir_all(s.path()).await.is_ok() {
                    report.sessions_cleaned += 1;
                }
            }
            // Note: the `multipart` parent itself is left in place. It is recreated on every
            // store open and on each `stage_part`, so pruning it would be pointless churn; only
            // the per-session subdirectories are reclaimed (counted as `sessions_cleaned`).
        }
    }
    Ok(())
}

/// Whether a staging artifact is older than the safety margin and so safe to reclaim. The margin
/// is compared against the file's mtime; an artifact whose mtime cannot be read (or sits in the
/// future relative to `now`) is treated as fresh and preserved, erring toward never deleting a
/// possibly-live in-flight write.
/// Whether `path`'s mtime is within `margin_secs` of `now` — too recently written to safely reclaim
/// as an orphan (it may be an in-flight PUT whose metadata row has not yet committed, ARCH 9). A
/// margin of `0` (tests) or a path that cannot be stat-ed (already deleted, racing) is treated as
/// not-young so the caller proceeds. The inverse of [`staging_artifact_expired`]'s age check.
async fn blob_too_young(path: &Path, margin_secs: i64, now: Timestamp) -> bool {
    if margin_secs <= 0 {
        return false;
    }
    let Ok(meta) = tokio::fs::metadata(path).await else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    let mtime_secs = match modified.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        // mtime predates the epoch: unambiguously old, so not "young".
        Err(_) => return false,
    };
    now.as_secs() - mtime_secs < margin_secs
}

async fn staging_artifact_expired(
    entry: &tokio::fs::DirEntry,
    margin_secs: i64,
    now: Timestamp,
) -> Result<bool, BlobError> {
    let meta = entry.metadata().await.map_err(io_err)?;
    let Ok(modified) = meta.modified() else {
        return Ok(false);
    };
    let mtime_secs = match modified.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        // mtime predates the epoch: unambiguously old, so it is past any non-negative margin.
        Err(_) => return Ok(margin_secs >= 0),
    };
    let age_secs = now.as_secs() - mtime_secs;
    Ok(age_secs >= margin_secs.max(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::testing::SetReconcileOracle;

    /// Audit 2026-07: reads and writes draw from SEPARATE I/O pools, so a flood of slow-reading
    /// clients (which hold a read permit for the whole client-paced download) can exhaust the read
    /// pool without starving writes. Pre-split, a single shared pool meant idle readers stalled the
    /// entire data plane, PUTs included.
    #[tokio::test]
    async fn read_and_write_io_pools_are_independent() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path())
            .await
            .unwrap()
            .with_io_pool_size(3)
            .with_read_io_pool_size(5);
        assert_eq!(store.write_permits.available_permits(), 3);
        assert_eq!(store.read_permits.available_permits(), 5);

        // Exhaust the read pool (as a wall of stalled downloads would) — writes stay fully available.
        let held: Vec<_> = (0..5)
            .map(|_| store.read_permits.clone().try_acquire_owned().unwrap())
            .collect();
        assert_eq!(store.read_permits.available_permits(), 0);
        assert_eq!(
            store.write_permits.available_permits(),
            3,
            "an exhausted read pool must not consume write permits"
        );
        drop(held);
    }

    /// The small-object GET fast path (an uncompressed blob at or below `small_read_max`, served
    /// inline from the probe's single read) must hold a read permit for as long as the streamed path
    /// does — otherwise a flood of slow readers requesting small objects bypasses the exact pool this
    /// audit exists to bound (see `read_and_write_io_pools_are_independent` above). The permit must
    /// still be held after the one chunk is yielded (a slow client that has the bytes queued but
    /// hasn't drained the connection must not free up the pool) and released only once the stream is
    /// fully exhausted.
    #[tokio::test]
    async fn small_object_fast_path_holds_a_read_permit_across_both_polls() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path())
            .await
            .unwrap()
            .with_read_io_pool_size(1);
        let b = BucketName::parse("bkt").unwrap();
        let staged = store
            .stage(
                &b,
                Box::pin(futures_util::stream::once(async move {
                    Ok(Bytes::from(vec![7u8; 128]))
                })),
                cairn_types::blob::StageOptions {
                    compression: None,
                    size_ceiling: 8 * 1024 * 1024,
                    content_type: "application/octet-stream".to_owned(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(store.read_permits.available_permits(), 1);

        let mut handle = store
            .open_raw(
                &staged.storage_path,
                None,
                BlobCipher::KnownPlaintext,
                &staged.compression,
            )
            .await
            .unwrap();
        assert!(
            handle.zero_copy.is_none(),
            "a 128-byte object takes the small-object fast path, not zero-copy"
        );
        assert_eq!(
            store.read_permits.available_permits(),
            0,
            "the fast path must acquire a read permit"
        );

        let first = handle.body.next().await.unwrap().unwrap();
        assert_eq!(first.as_ref(), &[7u8; 128][..]);
        assert_eq!(
            store.read_permits.available_permits(),
            0,
            "the permit must still be held after the one chunk is yielded"
        );

        assert!(handle.body.next().await.is_none());
        assert_eq!(
            store.read_permits.available_permits(),
            1,
            "the permit is released once the stream is exhausted"
        );
    }

    /// The mtime of a freshly created file, as whole epoch seconds.
    async fn file_mtime_secs(path: &Path) -> i64 {
        let modified = tokio::fs::metadata(path).await.unwrap().modified().unwrap();
        modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// A fresh `.staging` artifact (younger than the margin) is preserved while an old one is
    /// reclaimed, so an out-of-band reconcile cannot delete an in-flight write (ARCH 8.5).
    #[tokio::test]
    async fn staging_safety_margin_preserves_fresh_reclaims_old() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path()).await.unwrap();
        let staging = dir.path().join(STAGING);
        let tmp = staging.join("inflight.tmp");
        tokio::fs::write(&tmp, b"streaming...").await.unwrap();
        let mtime = file_mtime_secs(&tmp).await;

        let oracle = SetReconcileOracle::default();
        let opts = ReconcileOpts {
            staging_safety_margin_secs: 3600,
            ..ReconcileOpts::default()
        };

        // `now` only one second past the file's mtime: the artifact is well inside the margin.
        let now_fresh = Timestamp::from_secs(mtime + 1);
        let report = reconcile_inner(&store.data_root, &oracle, opts, now_fresh)
            .await
            .unwrap();
        assert_eq!(report.staging_cleaned, 0, "fresh staging file preserved");
        assert!(tokio::fs::try_exists(&tmp).await.unwrap());

        // `now` two hours past the mtime: the artifact is now older than the 1h margin.
        let now_old = Timestamp::from_secs(mtime + 7200);
        let report = reconcile_inner(&store.data_root, &oracle, opts, now_old)
            .await
            .unwrap();
        assert_eq!(report.staging_cleaned, 1, "stale staging file reclaimed");
        assert!(!tokio::fs::try_exists(&tmp).await.unwrap());
    }

    /// A zero margin reclaims even a brand-new artifact (the legacy unconditional behaviour, now
    /// opt-in via the margin), confirming the comparison is inclusive at the boundary.
    #[tokio::test]
    async fn staging_zero_margin_reclaims_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path()).await.unwrap();
        let tmp = dir.path().join(STAGING).join("orphan.tmp");
        tokio::fs::write(&tmp, b"leftover").await.unwrap();
        let mtime = file_mtime_secs(&tmp).await;

        let opts = ReconcileOpts {
            staging_safety_margin_secs: 0,
            ..ReconcileOpts::default()
        };
        let report = reconcile_inner(
            &store.data_root,
            &SetReconcileOracle::default(),
            opts,
            Timestamp::from_secs(mtime),
        )
        .await
        .unwrap();
        assert_eq!(report.staging_cleaned, 1);
    }

    /// `ensure_bucket_dir` is a no-op-and-still-Ok on an existing directory and creates a missing
    /// one; the durable parent fsync runs only on the create path (F-1, ARCH 8.2 step 4).
    #[tokio::test]
    async fn ensure_bucket_dir_creates_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let bucket = root.join("bkt");
        assert!(!tokio::fs::try_exists(&bucket).await.unwrap());
        // First call creates it (and fsyncs the parent for durability of the new entry).
        ensure_bucket_dir(root, &bucket).await.unwrap();
        assert!(tokio::fs::metadata(&bucket).await.unwrap().is_dir());
        // Second call is a no-op that still succeeds.
        ensure_bucket_dir(root, &bucket).await.unwrap();
        assert!(tokio::fs::metadata(&bucket).await.unwrap().is_dir());
    }

    /// `prune_if_empty` removes only an empty directory, leaves a populated one, and treats a
    /// missing directory as not pruned.
    #[tokio::test]
    async fn prune_if_empty_only_removes_empty() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty");
        tokio::fs::create_dir(&empty).await.unwrap();
        assert!(prune_if_empty(&empty).await.unwrap());
        assert!(!tokio::fs::try_exists(&empty).await.unwrap());

        let full = dir.path().join("full");
        tokio::fs::create_dir(&full).await.unwrap();
        tokio::fs::write(full.join("f"), b"x").await.unwrap();
        assert!(!prune_if_empty(&full).await.unwrap());
        assert!(tokio::fs::try_exists(&full).await.unwrap());

        let missing = dir.path().join("missing");
        assert!(!prune_if_empty(&missing).await.unwrap());
    }

    /// The data root and its in-root staging directory share one filesystem, so the startup check
    /// passes for a normal temp dir (ARCH 2.4, 9.2).
    #[cfg(unix)]
    #[tokio::test]
    async fn single_filesystem_check_passes_for_same_fs() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path()).await.unwrap();
        store.check_single_filesystem().unwrap();
    }
}
