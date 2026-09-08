//! `cairn-blob` — the local-filesystem [`BlobStore`]. This is the ONLY crate that performs
//! filesystem syscalls, and it owns the durable commit sequence (ARCH 8.2): stream to a
//! staging file, fsync the file, rename it into the per-bucket directory, fsync that
//! directory (the F-1 fix), and only then return — so a committed blob is durable before any
//! metadata references it. Object bytes live under opaque identifiers, never under the key, so
//! key-based path traversal is structurally impossible.

#![forbid(unsafe_code)]

mod commit;
mod timing;
pub use timing::{MultipartStage, MultipartTiming};
// Public only so the fuzz target (an external crate under `fuzz/`) can drive `CompressedReader`
// against arbitrary bytes; `#[doc(hidden)]` keeps it out of the published API surface. Not part of
// the supported interface — internal callers still go through the re-exports below. The reader /
// encoder deliberately do NOT implement `Debug`: they hold a raw DEK that must never be printed, so
// the `missing_debug_implementations` lint (now that the module is public) is suppressed here.
#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub mod compress;
mod crc64nvme;
mod encode;
pub mod hash;
// Safe file-placement hints (preallocation + access advice) for the write fast path (ARCH 7.5).
mod raw_io;
#[cfg(unix)]
mod reconcile;
#[cfg(feature = "io-uring")]
mod uring;
// The staging sink abstracts the durable-write file ops so the default `tokio::fs` path and the
// optional io_uring path are interchangeable (ARCH 8.2). Each backend implements create →
// streamed writes → commit (fsync file → rename → fsync dir) / abort with the same ordering.
mod namespace;
mod owned_file;
mod staging;

use crate::compress::{CompressedReader, is_precompressed};
use crate::encode::StagedEncoder;
use crate::hash::Hashers;
use crate::namespace::AdmittedPaths;
use crate::staging::Staging;
use async_trait::async_trait;
use bytes::Bytes;
use cairn_types::SecretKey32;
pub use cairn_types::blob::BlobCipher;
use cairn_types::blob::ReadBufferLease;
use cairn_types::blob::{
    BlobProbe, BlobReadHandle, ByteRange, ContentRange, PartRef, ReconcileOpts, ReconcileReport,
    StageOptions, StagedBlob, StagedPart, ZeroCopyRead,
};
use cairn_types::bucket::{CompressionAlgorithm, CompressionPolicy};
use cairn_types::error::BlobError;
use cairn_types::id::StoragePath;
use cairn_types::object::{ChecksumSet, CompressionDescriptor, ETag};
use cairn_types::storage::{StorageCreationPermit, StorageWriteTarget};
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

/// Open an existing regular file for reading without following a final-component symlink.
///
/// Snapshot/restore code uses this seam after validating every directory component. Keeping the
/// no-follow syscall here preserves `cairn-blob` as the filesystem boundary and closes the
/// check/open race that a separate `symlink_metadata` followed by `File::open` would leave.
#[cfg(unix)]
pub fn open_readonly_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use rustix::fs::{Mode, OFlags};

    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    Ok(fd.into())
}

/// Portable fallback for targets without `O_NOFOLLOW`.
#[cfg(not(unix))]
pub fn open_readonly_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("refusing to follow symlink {}", path.display()),
        ));
    }
    std::fs::File::open(path)
}

/// Open (or create) a node-state lock file without following a final-component symlink.
#[cfg(unix)]
pub fn open_lock_file_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use rustix::fs::{Mode, OFlags};

    let fd = rustix::fs::open(
        path,
        OFlags::CREATE | OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(std::io::Error::from)?;
    Ok(fd.into())
}

/// Portable fallback for targets without `O_NOFOLLOW`.
#[cfg(not(unix))]
pub fn open_lock_file_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("refusing to follow symlink {}", path.display()),
        ));
    }
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

/// Acquire a non-waiting exclusive advisory lock on an already-open node-state lock file.
///
/// Filesystem syscalls remain centralized in `cairn-blob`; the server retains the returned
/// [`std::fs::File`] for the lock's lifetime. Contention maps to
/// [`std::io::ErrorKind::WouldBlock`].
pub fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<()> {
    rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(std::io::Error::from)
}

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
    /// Cumulative count of metadata-declared plaintext reads refused because the file length does
    /// not equal the authoritative object/part logical length.
    ///
    /// Exposed as state rather than emitted here: `cairn-blob` is an engine crate with no `metrics`
    /// dependency, so the server mirrors this into `cairn_blob_plaintext_length_mismatch_total` on its
    /// metrics tick — the same expose-and-mirror shape as `writer_queue_depth` and the metadata
    /// cache's `(hits, misses)`. A `tracing::error!` alone is not alertable; this is.
    ///
    /// [`open_raw`]: cairn_types::traits::BlobStore::open_raw
    plaintext_length_mismatch: Arc<std::sync::atomic::AtomicU64>,
    multipart_timings: Arc<timing::MultipartTimings>,
}

/// Default upper bound (bytes) for the small-object GET fast path — see [`LocalBlobStore`]'s
/// `small_read_max` field. 256 KiB sits below the kernel sendfile floor, so a blob this small would
/// never take the zero-copy path; reading it whole avoids the streaming channel's per-GET overhead.
pub const SMALL_READ_MAX: u64 = 256 * 1024;

/// The default bound on concurrent blob transfers when not overridden (ARCH 7.4). A reasonable
/// general value for SSD/NVMe-backed storage; tune down for spinning disks, up for fast arrays.
pub const DEFAULT_BLOB_IO_CONCURRENCY: usize = 64;

impl LocalBlobStore {
    /// Open a blob store with retained exclusive maintenance ownership. Initialize the staging
    /// area through anchored descriptors, rejecting symlinks and nested mounts before writes.
    ///
    /// When built with the `io-uring` feature, the durable single-object staging write path
    /// (create tmp → write → fsync → rename → fsync dir) runs on the io_uring executor by
    /// default; without the feature it uses retained blocking file jobs. Use [`Self::with_io_uring`] to
    /// override the choice explicitly (e.g. to compare backends in a benchmark).
    ///
    /// # Errors
    /// Returns a [`BlobError`] if the staging directory cannot be created.
    pub async fn open(
        data_root: impl Into<PathBuf>,
        lease: cairn_types::storage::io::StorageIoLease,
    ) -> Result<Self, BlobError> {
        let data_root = data_root.into();
        let root = data_root.clone();
        let operation = lease.try_child()?;
        let (result, _lease) = tokio::task::spawn_blocking(move || {
            let _operation = operation;
            (namespace::initialize(&root).map_err(io_err), lease)
        })
        .await
        .map_err(|error| BlobError::Io(error.to_string()))?;
        result?;
        Ok(Self {
            data_root: Arc::new(data_root),
            use_uring: cfg!(feature = "io-uring"),
            read_permits: Arc::new(tokio::sync::Semaphore::new(DEFAULT_BLOB_IO_CONCURRENCY)),
            write_permits: Arc::new(tokio::sync::Semaphore::new(DEFAULT_BLOB_IO_CONCURRENCY)),
            dir_sync: Arc::new(commit::DirSyncCoalescer::spawn()),
            small_read_max: SMALL_READ_MAX,
            plaintext_length_mismatch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            multipart_timings: Arc::default(),
        })
    }

    /// Drain up to 1,024 recent multipart stage durations for the server metrics tick.
    /// Includes stages ended by errors or cancellation; samples are shared across clones.
    #[must_use]
    pub fn drain_multipart_timings(&self) -> Vec<MultipartTiming> {
        self.multipart_timings.drain()
    }

    /// Cumulative samples evicted when metrics collection falls behind.
    #[must_use]
    pub fn multipart_timings_dropped_total(&self) -> u64 {
        self.multipart_timings.dropped_total()
    }

    /// Cumulative number of metadata-declared plaintext reads refused because the file's physical
    /// length differed from the authoritative logical length. Monotonic for the process lifetime
    /// and shared across clones; the server publishes it as
    /// `cairn_blob_plaintext_length_mismatch_total`.
    ///
    /// This detects a missing encryption descriptor on the ordinary encrypted/uncompressed layout
    /// (whose framing adds bytes), as well as truncation or inconsistent plaintext metadata, without
    /// content-sniffing and rejecting legitimate arbitrary object bytes.
    #[must_use]
    pub fn plaintext_length_mismatch_total(&self) -> u64 {
        self.plaintext_length_mismatch
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

/// Select the actual on-disk transform once for both preflight and streaming. A precompressed
/// content type bypasses compression, but must never bypass required encryption.
fn encoding_policies(
    opts: &StageOptions,
) -> (Option<CompressionPolicy>, Option<CompressionPolicy>) {
    let compress = match opts.compression {
        Some(pol) if !is_precompressed(&opts.content_type) => Some(pol),
        _ => None,
    };
    let block_pol = compress.or_else(|| {
        opts.encryption.as_ref().map(|_| CompressionPolicy {
            algorithm: CompressionAlgorithm::None,
            block_size: DEFAULT_ENCRYPTED_BLOCK_SIZE,
        })
    });
    (compress, block_pol)
}

fn validate_stage_len(opts: &StageOptions, logical_len: Option<u64>) -> Result<(), BlobError> {
    if logical_len.is_some_and(|len| len > opts.size_ceiling) {
        return Err(BlobError::SizeExceeded);
    }
    if let Some(policy) = encoding_policies(opts).1 {
        compress::validate_encoded_len(logical_len.unwrap_or(0), policy.block_size)?;
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
    paths: &AdmittedPaths,
) -> Result<
    (
        u64,
        u64,
        String,
        Vec<cairn_types::object::ChecksumValue>,
        CompressionDescriptor,
        String,
    ),
    BlobError,
> {
    let (compress, block_pol) = encoding_policies(opts);
    validate_stage_len(opts, opts.content_length)?;
    let mut hashers = Hashers::new(&opts.extra_checksums);
    let mut logical: u64 = 0;
    let mut physical: u64 = 0;

    // The self-describing CRNB block container is needed whenever we compress OR encrypt: SSE-S3
    // encrypts each physical block after compression, so an encrypted-but-uncompressed object still
    // flows through the block encoder with `CompressionAlgorithm::None`. The MD5/ETag is computed
    // over the plaintext (here, via `hashers`) before any transform, so it is identical with or
    // without compression/encryption (ARCH 21.1, 27).
    if let Some(pol) = block_pol {
        let mut enc = StagedEncoder::new(
            pol,
            opts.encryption.clone(),
            paths.spool.clone(),
            paths.lease.try_child()?,
        );
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            logical = logical
                .checked_add(chunk.len() as u64)
                .ok_or(BlobError::SizeExceeded)?;
            if logical > opts.size_ceiling {
                return Err(BlobError::SizeExceeded);
            }
            hashers.update(&chunk);
            physical += enc.feed(&chunk, file).await?;
        }
        physical += enc.finish(file).await?;
        let (md5, checks, internal_sha256) = hashers.finalize();
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
        Ok((logical, physical, md5, checks, descriptor, internal_sha256))
    } else {
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            logical = logical
                .checked_add(chunk.len() as u64)
                .ok_or(BlobError::SizeExceeded)?;
            if logical > opts.size_ceiling {
                return Err(BlobError::SizeExceeded);
            }
            hashers.update(&chunk);
            file.write_all(&chunk).await?;
            physical += chunk.len() as u64;
        }
        let (md5, checks, internal_sha256) = hashers.finalize();
        Ok((
            logical,
            physical,
            md5,
            checks,
            CompressionDescriptor::Uncompressed,
            internal_sha256,
        ))
    }
}

/// An encoded read keeps the descriptor and validated geometry obtained by its initial probe.
/// Raw reads still defer their body open so an unpolled zero-copy fallback performs no extra I/O.
enum StreamInput {
    Raw(PathBuf),
    Container(Box<CompressedReader<std::fs::File>>),
}

/// Probe output owns its memory reservation too: a completed blocking task can retain this
/// result after its awaiting request is cancelled. Drop buffers before releasing that lease.
struct ReadProbe {
    logical_len: u64,
    reuse_file: Option<std::fs::File>,
    whole: Option<Bytes>,
    prepared: Option<Box<CompressedReader<std::fs::File>>>,
    _lease: Option<ReadBufferLease>,
}

/// Push a bounded logical range through a bounded channel. An encoded source is already fully
/// validated; page loads use its original descriptor and verified summaries, without re-probing.
fn stream_input(
    input: StreamInput,
    offset: u64,
    len: u64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, BlobError>>,
) -> Result<(), BlobError> {
    use std::io::{Read, Seek, SeekFrom};
    match input {
        StreamInput::Container(mut reader) => {
            let bs = reader.block_size();
            let end = offset.saturating_add(len).min(reader.logical_len());
            if offset >= end {
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
        }
        StreamInput::Raw(path) => {
            let mut f = std::fs::File::open(path).map_err(io_err)?;
            f.seek(SeekFrom::Start(offset)).map_err(io_err)?;
            let mut remaining = len;
            let mut buf = vec![0u8; READ_CHUNK];
            while remaining > 0 {
                let want = remaining.min(READ_CHUNK as u64) as usize;
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
    }
    Ok(())
}

/// Multipart inputs have no GET probe. Validate and stream this already anchored descriptor in
/// the same leased blocking operation, preserving the metadata-pinned CRNB declaration.
fn stream_part(
    file: std::fs::File,
    cipher: BlobCipher,
    expected_logical_len: u64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, BlobError>>,
) -> Result<(), BlobError> {
    let reader = CompressedReader::open_with_dek(
        file,
        cipher,
        &CompressionDescriptor::Uncompressed,
        expected_logical_len,
    )?;
    stream_input(
        StreamInput::Container(Box::new(reader)),
        0,
        expected_logical_len,
        tx,
    )
}

/// The prepared input and reservations stay together through queued blocking work, including
/// cancellation before the body is first polled or before the blocking task starts.
enum StreamSrc {
    Pending(
        (
            StreamInput,
            u64,
            u64,
            (tokio::sync::OwnedSemaphorePermit, Option<ReadBufferLease>),
        ),
    ),
    Running(tokio::sync::mpsc::Receiver<Result<Bytes, BlobError>>),
}

fn read_stream(
    input: StreamInput,
    offset: u64,
    len: u64,
    permits: (tokio::sync::OwnedSemaphorePermit, Option<ReadBufferLease>),
) -> cairn_types::BlobStream {
    let initial = StreamSrc::Pending((input, offset, len, permits));
    Box::pin(futures_util::stream::unfold(initial, |state| async move {
        let mut rx = match state {
            StreamSrc::Pending((input, offset, len, permits)) => {
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, BlobError>>(4);
                tokio::task::spawn_blocking(move || {
                    let (_permit, _lease) = permits;
                    if let Err(e) = stream_input(input, offset, len, &tx) {
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
    enc: &mut Option<StagedEncoder>,
    logical: &mut u64,
    physical: &mut u64,
    size_ceiling: u64,
) -> Result<(), BlobError> {
    *logical = logical
        .checked_add(chunk.len() as u64)
        .ok_or(BlobError::SizeExceeded)?;
    // Enforce the ceiling on the actual bytes read, so a part whose on-disk size exceeds its recorded
    // size can't inflate the object past the limit (audit 2026-07).
    if *logical > size_ceiling {
        return Err(BlobError::SizeExceeded);
    }
    hashers.update(chunk);
    match enc {
        Some(e) => {
            *physical += e.feed(chunk, sink).await?;
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
    /// encrypted (`PartRef.cipher != KnownPlaintext`, ARCH 27) is decrypted on read through the same
    /// metadata-version-pinned CRNB reader GET uses, so `assemble` always sees plaintext before it
    /// re-encodes under the object DEK.
    /// Factored out of `assemble` so the caller can `abort` the sink on any error without duplicating
    /// the unlink.
    #[allow(clippy::too_many_arguments)]
    async fn assemble_into(
        &self,
        sink: &mut Staging,
        parts: &[PartRef],
        hashers: &mut Hashers,
        enc: &mut Option<StagedEncoder>,
        logical: &mut u64,
        physical: &mut u64,
        size_ceiling: u64,
        lease: &cairn_types::storage::io::StorageIoLease,
    ) -> Result<(), BlobError> {
        // Allocate only for the first plaintext part, then reuse across part boundaries. An
        // entirely encrypted upload keeps its existing bounded decoder buffers without this one.
        let mut plaintext_buf = None;
        for part in parts {
            let part_path = part.storage_path.clone();
            let root = self.data_root.clone();
            match part.cipher.clone() {
                // Plaintext / pre-v21 part: raw read, unchanged.
                BlobCipher::KnownPlaintext => {
                    let read_lease = lease.try_child()?;
                    let expected = part.size;
                    let file = tokio::task::spawn_blocking(move || {
                        let file = namespace::read_file(&root, &part_path).map_err(|error| {
                            if error.kind() == std::io::ErrorKind::NotFound {
                                BlobError::NotFound
                            } else {
                                io_err(error)
                            }
                        })?;
                        if file.metadata().map_err(io_err)?.len() != expected {
                            return Err(BlobError::Corruption(
                                "part length does not match metadata".into(),
                            ));
                        }
                        Ok::<_, BlobError>(owned_file::FileOwner::new(file, read_lease))
                    })
                    .await
                    .map_err(|error| BlobError::Io(error.to_string()))??;
                    let mut remaining = part.size;
                    while remaining != 0 {
                        let mut buf = plaintext_buf
                            .take()
                            .unwrap_or_else(|| vec![0u8; READ_CHUNK]);
                        let want = remaining.min(READ_CHUNK as u64) as usize;
                        let (buf, n) = file
                            .run(move |mut file| {
                                use std::io::Read;
                                let n = file.read(&mut buf[..want])?;
                                Ok((buf, n))
                            })
                            .await
                            .map_err(io_err)?;
                        if n == 0 {
                            return Err(BlobError::Corruption(
                                "part truncated during assembly".into(),
                            ));
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
                        remaining -= n as u64;
                        plaintext_buf = Some(buf);
                    }
                }
                // Encrypted part: decrypt-on-read through the same CRNB reader GET uses, off the
                // reactor via `spawn_blocking` feeding a bounded channel (mirrors `read_stream`). A
                // wrong/tampered/truncated part fails per-block GCM auth inside `stream_blob` and
                // surfaces here as `BlobError::Corruption`, which `assemble` turns into a `sink.abort()`
                // — no orphan, no plaintext, no partial object.
                cipher => {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, BlobError>>(4);
                    let size = part.size;
                    let read_lease = lease.try_child()?;
                    tokio::task::spawn_blocking(move || {
                        let _read_lease = read_lease;
                        let result = namespace::read_file(&root, &part_path)
                            .map_err(io_err)
                            .and_then(|file| stream_part(file, cipher, size, &tx));
                        if let Err(error) = result {
                            let _ = tx.blocking_send(Err(error));
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

/// Bound a coalesced read by the length already checked against metadata, even if a local writer
/// grows the file after fstat. A fixed slice also avoids read_to_end's speculative capacity growth.
fn read_small(reader: &mut impl std::io::Read, len: u64) -> Result<Bytes, BlobError> {
    let size = usize::try_from(len)
        .map_err(|_| BlobError::Corruption("small read length exceeds address space".into()))?;
    let mut buf = vec![0u8; size];
    let mut filled = 0;
    while filled < size {
        let n = match reader.read(&mut buf[filled..]) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result.map_err(io_err)?,
        };
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(Bytes::from(buf))
}

impl LocalBlobStore {
    async fn open_raw_with_lease(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        cipher: BlobCipher,
        compression: &CompressionDescriptor,
        expected_logical_len: u64,
        lease: Option<ReadBufferLease>,
    ) -> Result<BlobReadHandle, BlobError> {
        // The named cipher includes the metadata-backed CRNB format expectation. Keep that typed
        // declaration intact through both the probe open and the lazy body stream: reducing it to a
        // bare DEK would let the on-disk version byte choose the legacy parser.
        let file_path = self.resolve(path)?;
        // The blob is a self-describing CRNB block container iff it was compressed OR encrypted at
        // write time (audit #18). Decide that from the caller's stored compression descriptor and
        // the DEK — both authoritative — rather than sniffing the 34-byte trailer magic, which an
        // uncompressed object's own bytes can collide with.
        let is_container =
            cipher.is_encrypted() || !matches!(compression, CompressionDescriptor::Uncompressed);
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
        let plaintext_length_mismatch = self.plaintext_length_mismatch.clone();
        let probe_cipher = cipher.clone();
        let probe_compression = compression.clone();
        let probe_lease = lease.clone();
        let ReadProbe {
            logical_len,
            reuse_file,
            whole,
            prepared,
            _lease: _probe_result_lease,
        } = tokio::task::spawn_blocking(move || -> Result<ReadProbe, BlobError> {
            let _lease = probe_lease;
            let mut f = match std::fs::File::open(&probe_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(BlobError::NotFound);
                }
                Err(e) => return Err(io_err(e)),
            };
            let file_len = f.metadata().map_err(io_err)?.len();
            // Plaintext framing is an authoritative metadata declaration. Validate its one
            // independent physical invariant instead of sniffing the body: plaintext and
            // logical lengths must match exactly. This catches the ordinary missing-DEK case
            // (an encrypted/uncompressed CRNB file has framing overhead), yet preserves S3's
            // arbitrary-byte contract when a legitimate plaintext object's bytes themselves
            // happen to form a complete CRNB file.
            if !is_container && file_len != expected_logical_len {
                plaintext_length_mismatch.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    path = %probe_path.display(),
                    file_len,
                    expected_logical_len,
                    "metadata-declared plaintext blob length mismatch"
                );
                return Err(BlobError::Corruption(
                    "plaintext blob length does not match trusted metadata".into(),
                ));
            }
            let (logical_len, reuse_file, whole, prepared) = if is_container {
                let reader = CompressedReader::open_with_dek(
                    f,
                    probe_cipher,
                    &probe_compression,
                    expected_logical_len,
                )?;
                (reader.logical_len(), None, None, Some(Box::new(reader)))
            } else if file_len <= small_read_max {
                (file_len, None, Some(read_small(&mut f, file_len)?), None)
            } else {
                (file_len, Some(f), None, None)
            };
            Ok(ReadProbe {
                logical_len,
                reuse_file,
                whole,
                prepared,
                _lease,
            })
        })
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
            let input = match prepared {
                Some(reader) => StreamInput::Container(reader),
                None => StreamInput::Raw(file_path.clone()),
            };
            let body = read_stream(input, offset, len, (permit, lease.clone()));
            // Uncompressed, plaintext blobs may take the kernel file-to-socket fast path, reusing the
            // fd the probe opened. Encrypted blobs are always block-formatted (`is_container`), so
            // `reuse_file` is `None` for them and the kernel never sees ciphertext.
            let zero_copy = reuse_file.map(|f| ZeroCopyRead {
                file: Arc::new(f),
                offset,
                len,
            });
            (body, zero_copy)
        };

        let body = match lease {
            Some(lease) => lease.hold_stream(body),
            None => body,
        };
        Ok(BlobReadHandle {
            logical_len: len,
            content_range,
            body,
            zero_copy,
        })
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    fn read_memory_bound(
        &self,
        compression: &CompressionDescriptor,
        encrypted: bool,
        logical_len: u64,
    ) -> Result<cairn_types::blob::ReadMemoryBound, BlobError> {
        if encrypted || !matches!(compression, CompressionDescriptor::Uncompressed) {
            return compress::read_memory_bound(compression, logical_len);
        }
        // The raw path can retain one coalesced small object, or a read buffer plus four queued
        // chunks and the delivered chunk. Account for an explicitly enlarged small-read cutoff.
        let whole = logical_len.min(self.small_read_max);
        Ok(cairn_types::blob::ReadMemoryBound {
            buffer_bytes: whole.max(6 * READ_CHUNK as u64).saturating_add(128 * 1024),
            max_frame_bytes: whole.max(READ_CHUNK as u64),
        })
    }

    async fn stage(
        &self,
        permit: StorageCreationPermit,
        body: cairn_types::BodyStream,
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        validate_stage_len(&opts, opts.content_length)?;
        // Bound concurrent blob *copy* I/O (ARCH 7.4). Held through the data copy and the per-file
        // durability (fdatasync + rename), then released BEFORE the coalesced directory-fsync
        // barrier (Phase 2.4) so a PUT awaiting that barrier no longer occupies blob-I/O concurrency
        // that concurrent GETs need — reads stop queueing behind writers' fsync barriers. The
        // barrier itself is bounded by the coalescer (one fsync per directory per batch), not by
        // this semaphore, and a waiter only parks on a oneshot, holding no blocking thread.
        let copy_permit = self.acquire_io().await?;
        if !matches!(permit.plan().target, StorageWriteTarget::Object { .. }) {
            return Err(BlobError::Io(
                "storage target is not an object write".into(),
            ));
        }
        let paths = AdmittedPaths::open(&self.data_root, permit).await?;
        let storage_path = paths.storage_path.clone();
        let mut sink = Staging::create(
            paths.staging.clone(),
            self.use_uring,
            opts.content_length,
            paths.lease.try_child()?,
        )
        .await?;
        let outcome = write_staged(&mut sink, body, &opts, &paths).await;
        let (logical, physical, md5, checksums, descriptor, internal_sha256) = match outcome {
            Ok(v) => v,
            Err(e) => {
                sink.abort().await;
                return Err(e);
            }
        };
        sink.commit(&paths.final_file).await?;
        drop(copy_permit);
        self.dir_sync
            .sync_file(paths.final_file.parent.clone(), &paths.lease)
            .await?;
        // The crash window the durability ordering protects: the blob is now durable but no
        // metadata row references it yet. A crash here leaves an orphan that reconcile reclaims.
        fail::fail_point!("blob_after_durable");

        let staged = StagedBlob {
            storage_path,
            size_logical: logical,
            size_physical: physical,
            etag: ETag::from_md5_hex(md5.clone()),
            md5_hex: md5,
            checksums,
            internal_sha256,
            compression: descriptor,
        };
        Ok(staged)
    }

    async fn open_raw(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        cipher: BlobCipher,
        compression: &CompressionDescriptor,
        expected_logical_len: u64,
    ) -> Result<BlobReadHandle, BlobError> {
        self.open_raw_with_lease(path, range, cipher, compression, expected_logical_len, None)
            .await
    }

    async fn open_raw_guarded(
        &self,
        path: &StoragePath,
        range: Option<ByteRange>,
        cipher: BlobCipher,
        compression: &CompressionDescriptor,
        expected_logical_len: u64,
        lease: ReadBufferLease,
    ) -> Result<BlobReadHandle, BlobError> {
        self.open_raw_with_lease(
            path,
            range,
            cipher,
            compression,
            expected_logical_len,
            Some(lease),
        )
        .await
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

    async fn confirm_storage_quiescence(
        &self,
        plan: &cairn_types::storage::StorageWritePlan,
        lease: cairn_types::storage::io::StorageIoLease,
    ) -> Result<(), BlobError> {
        plan.validate()
            .map_err(|error| BlobError::Io(error.to_string()))?;
        if !lease.owns(&plan.attempt, &plan.generation) {
            return Err(BlobError::Io("storage recovery ownership mismatch".into()));
        }
        let operation = lease.try_child()?;
        let root = self.data_root.clone();
        let paths = plan.paths.clone();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let _operation = operation;
            let mut locked = Vec::with_capacity(paths.len());
            for path in paths {
                match namespace::read_file(&root, &path.path) {
                    Ok(file) => {
                        try_lock_exclusive(&file).map_err(io_err)?;
                        locked.push(file);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io_err(error)),
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| BlobError::Io(error.to_string()))?
    }

    async fn cleanup_storage(
        &self,
        cleanup: &cairn_types::storage::StorageCleanup,
        lease: cairn_types::storage::io::StorageIoLease,
    ) -> Result<(), BlobError> {
        cairn_types::storage::validate_storage_path(&cleanup.bucket, &cleanup.path)
            .map_err(|error| BlobError::Io(error.to_string()))?;
        if !lease.owns(&cleanup.id, &cleanup.generation) {
            return Err(BlobError::Io("storage cleanup ownership mismatch".into()));
        }
        let operation = lease.try_child()?;
        let root = self.data_root.clone();
        let path = cleanup.path.clone();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let _operation = operation;
            namespace::cleanup(&root, &path).map_err(io_err)
        })
        .await
        .map_err(|error| BlobError::Io(error.to_string()))?
    }

    async fn stage_part(
        &self,
        permit: StorageCreationPermit,
        body: cairn_types::BodyStream,
        checksums: ChecksumSet,
        size_ceiling: u64,
        encryption: Option<SecretKey32>,
    ) -> Result<StagedPart, BlobError> {
        let _permit = self.acquire_io().await?;
        if !matches!(permit.plan().target, StorageWriteTarget::Part { .. }) {
            return Err(BlobError::Io("storage target is not a part write".into()));
        }
        let paths = AdmittedPaths::open(&self.data_root, permit).await?;
        fail::fail_point!("blob_after_multipart_session_dir");
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
        let mut sink = Staging::create(
            paths.staging.clone(),
            self.use_uring,
            None,
            paths.lease.try_child()?,
        )
        .await?;
        let (logical, _phys, md5, checks, _desc, _internal_sha256) =
            match write_staged(&mut sink, body, &opts, &paths).await {
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
        self.dir_sync
            .sync_file(paths.final_file.parent.clone(), &paths.lease)
            .await?;
        Ok(StagedPart {
            storage_path: paths.storage_path.clone(),
            size: logical,
            md5_hex: md5,
            checksums: checks,
        })
    }

    async fn assemble(
        &self,
        permit: StorageCreationPermit,
        parts: &[PartRef],
        opts: StageOptions,
    ) -> Result<StagedBlob, BlobError> {
        // As in `stage`, the copy permit is released before the coalesced directory-fsync barrier
        // (Phase 2.4) so the assembly does not hold blob-I/O concurrency through its fsync wait.
        let permit_timing = self.multipart_timings.start(MultipartStage::PermitWait);
        let copy_permit = self.acquire_io().await?;
        drop(permit_timing);
        let assembly_timing = self.multipart_timings.start(MultipartStage::Assembly);
        let StorageWriteTarget::Completion { upload_id, .. } = &permit.plan().target else {
            return Err(BlobError::Io(
                "storage target is not multipart assembly".into(),
            ));
        };
        let prefix = format!(".staging/multipart/{upload_id}/");
        if parts
            .iter()
            .any(|part| !part.storage_path.as_str().starts_with(&prefix))
        {
            return Err(BlobError::Io(
                "assembly part does not belong to the admitted session".into(),
            ));
        }
        let (compress, block_pol) = encoding_policies(&opts);
        // The assembled object's size is the sum of the parts' plaintext sizes — known up front, so
        // preallocate the staging file to place it contiguously (ARCH 7.5).
        let assembled_len = parts.iter().try_fold(0_u64, |total, p| {
            total.checked_add(p.size).ok_or(BlobError::SizeExceeded)
        })?;
        // Enforce the object-size ceiling on the multipart total, exactly as the single-PUT path does
        // on its streamed bytes (write_staged). Without this a multipart upload of up to ~10000 parts
        // each near the per-part cap bypasses CAIRN_MAX_OBJECT_SIZE — a limit-bypass / disk+memory DoS
        // (audit 2026-07). Checked up front on the recorded sizes; assemble_into also enforces on the
        // running sum in case a part's on-disk size disagrees with its record.
        validate_stage_len(&opts, Some(assembled_len))?;
        let paths = AdmittedPaths::open(&self.data_root, permit).await?;
        let storage_path = paths.storage_path.clone();
        let mut sink = Staging::create(
            paths.staging.clone(),
            self.use_uring,
            Some(assembled_len),
            paths.lease.try_child()?,
        )
        .await?;
        // Hash the assembled plaintext once, computing the MD5/ETag basis plus any supplementary
        // checksums the caller requested via `opts.extra_checksums` (a whole-object FULL_OBJECT
        // recompute at CompleteMultipartUpload). With no extra checksums this is byte-for-byte the
        // same work as the previous bare-MD5 path, so the default multipart path is unchanged.
        let mut hashers = Hashers::new(&opts.extra_checksums);
        let mut logical: u64 = 0;
        let mut physical: u64 = 0;
        let mut enc = block_pol
            .map(|policy| {
                Ok::<_, BlobError>(StagedEncoder::new(
                    policy,
                    opts.encryption,
                    paths.spool.clone(),
                    paths.lease.try_child()?,
                ))
            })
            .transpose()?;

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
                &paths.lease,
            )
            .await;
        if let Err(e) = assembled {
            sink.abort().await;
            return Err(e);
        }
        let descriptor = if let Some(e) = enc {
            match e.finish(&mut sink).await {
                Ok(bytes) => physical += bytes,
                Err(err) => {
                    sink.abort().await;
                    return Err(err);
                }
            }
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

        drop(assembly_timing);
        let durability_timing = self.multipart_timings.start(MultipartStage::Durability);
        sink.commit(&paths.final_file).await?;
        // Release the copy permit before parking on the coalesced directory-fsync barrier.
        drop(copy_permit);
        self.dir_sync
            .sync_file(paths.final_file.parent.clone(), &paths.lease)
            .await?;
        drop(durability_timing);
        fail::fail_point!("blob_after_assemble");

        let (md5_hex, checksums, internal_sha256) = hashers.finalize();
        let staged = StagedBlob {
            storage_path,
            size_logical: logical,
            size_physical: physical,
            etag: ETag::from_md5_hex(md5_hex.clone()),
            md5_hex,
            checksums,
            internal_sha256,
            compression: descriptor,
        };
        Ok(staged)
    }

    async fn reconcile(
        &self,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
        lease: cairn_types::storage::io::StorageIoLease,
    ) -> Result<ReconcileReport, BlobError> {
        // Sample time once; the bounded exclusive walker retains actual maintenance ownership.
        let now = system_now();
        reconcile_inner(&self.data_root, oracle, opts, now, lease).await
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
    lease: cairn_types::storage::io::StorageIoLease,
) -> Result<ReconcileReport, BlobError> {
    #[cfg(unix)]
    {
        reconcile::run(data_root, oracle, opts, now, lease).await
    }
    #[cfg(not(unix))]
    {
        let _ = (data_root, oracle, opts, now, lease);
        Err(BlobError::Io(
            "safe exclusive reconciliation requires POSIX descriptors".into(),
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::BucketName;
    use cairn_types::testing::FixtureBlobStore;
    use cairn_types::testing::SetReconcileOracle;

    async fn reconcile_inner(
        root: &Path,
        oracle: &dyn ReconcileOracle,
        opts: ReconcileOpts,
        now: Timestamp,
    ) -> Result<ReconcileReport, BlobError> {
        let (_watch, lease) = cairn_types::storage::io::StorageIoWatch::new(
            cairn_types::storage::StorageToken::generate(),
            cairn_types::storage::StorageToken::generate(),
            Arc::new(()),
        );
        super::reconcile_inner(root, oracle, opts, now, lease).await
    }

    /// Audit 2026-07: reads and writes draw from SEPARATE I/O pools, so a flood of slow-reading
    /// clients (which hold a read permit for the whole client-paced download) can exhaust the read
    /// pool without starving writes. Pre-split, a single shared pool meant idle readers stalled the
    /// entire data plane, PUTs included.
    #[tokio::test]
    async fn read_and_write_io_pools_are_independent() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
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

    /// Cancellation ends production of bytes; exact admitted artifacts remain for the retained
    /// recovery consumer, which is deliberately absent from this blob-only fixture.
    #[tokio::test]
    async fn canceled_stage_retains_its_admitted_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
            .await
            .unwrap();
        let bucket = BucketName::parse("bkt").unwrap();
        let (polled_tx, polled_rx) = tokio::sync::oneshot::channel();
        let first = futures_util::stream::once(async move {
            let _ = polled_tx.send(());
            Ok::<_, cairn_types::error::BodyError>(Bytes::from_static(b"started"))
        });
        let body: cairn_types::BodyStream = Box::pin(first.chain(futures_util::stream::pending()));

        let task = tokio::spawn(async move {
            store
                .stage_fixture(
                    &bucket,
                    body,
                    StageOptions {
                        size_ceiling: 1024,
                        ..StageOptions::default()
                    },
                )
                .await
        });
        polled_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let files = std::fs::read_dir(dir.path().join(STAGING))
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_file())
            .count();
        assert_eq!(files, 1);
        assert_eq!(
            std::fs::read_dir(dir.path().join("bkt")).unwrap().count(),
            0
        );
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
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
            .await
            .unwrap()
            .with_read_io_pool_size(1);
        let b = BucketName::parse("bkt").unwrap();
        let staged = store
            .stage_fixture(
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
                staged.size_logical,
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
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
            .await
            .unwrap();
        let staging = dir.path().join(STAGING);
        let tmp = staging.join("11111111111111111111111111111111.tmp");
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
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
            .await
            .unwrap();
        let tmp = dir
            .path()
            .join(STAGING)
            .join("22222222222222222222222222222222.tmp");
        tokio::fs::write(&tmp, b"leftover").await.unwrap();
        // Model a process crash in the scratch create/unlink window.
        let index_tmp = dir
            .path()
            .join(STAGING)
            .join("22222222222222222222222222222222.index.tmp");
        tokio::fs::write(&index_tmp, b"index entries")
            .await
            .unwrap();
        let mtime = file_mtime_secs(&tmp)
            .await
            .max(file_mtime_secs(&index_tmp).await);

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
        assert_eq!(report.staging_cleaned, 2);
        assert!(!tmp.exists());
        assert!(!index_tmp.exists());
    }

    #[tokio::test]
    async fn admitted_bucket_preparation_creates_and_reuses_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
            .await
            .unwrap();
        let bucket = BucketName::parse("bkt").unwrap();
        for _ in 0..2 {
            store
                .stage_fixture(
                    &bucket,
                    Box::pin(futures_util::stream::empty()),
                    StageOptions::default(),
                )
                .await
                .unwrap();
            assert!(dir.path().join("bkt").is_dir());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn single_filesystem_check_passes_for_same_fs() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
            .await
            .unwrap();
        store.check_single_filesystem().unwrap();
    }

    /// A cancelled async caller does not cancel an already queued blocking filesystem read.
    /// Keep its external memory reservation until that task actually exits, both for the probe
    /// and for the lazy body reader. A one-thread blocking pool makes the ordering deterministic.
    #[test]
    fn cancelled_probe_and_stream_retain_external_buffer_lease() {
        async fn occupy_blocking_thread()
        -> (std::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
            let (release, wait) = std::sync::mpsc::channel();
            let (ready, started) = tokio::sync::oneshot::channel();
            let task = tokio::task::spawn_blocking(move || {
                ready.send(()).unwrap();
                wait.recv().unwrap();
            });
            started.await.unwrap();
            (release, task)
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store =
                LocalBlobStore::open(dir.path(), cairn_types::testing::fixture_storage_io())
                    .await
                    .unwrap()
                    .with_small_read_max(0);
            for compression in [None, Some(CompressionPolicy::default())] {
                let data = Bytes::from(vec![7; 64 * 1024]);
                let size = data.len() as u64;
                let staged = store
                    .stage_fixture(
                        &BucketName::parse("bkt").unwrap(),
                        Box::pin(futures_util::stream::iter([Ok(data)])),
                        StageOptions {
                            compression,
                            content_type: "text/plain".into(),
                            ..StageOptions::default()
                        },
                    )
                    .await
                    .unwrap();

                for cancel_probe in [true, false] {
                    let owner = Arc::new(());
                    let weak = Arc::downgrade(&owner);
                    let lease = ReadBufferLease::new(owner);
                    let (release, blocker) = if cancel_probe {
                        let blocking = occupy_blocking_thread().await;
                        {
                            let read = store.open_raw_guarded(
                                &staged.storage_path,
                                None,
                                BlobCipher::KnownPlaintext,
                                &staged.compression,
                                size,
                                lease,
                            );
                            futures_util::pin_mut!(read);
                            std::future::poll_fn(|cx| {
                                assert!(std::future::Future::poll(read.as_mut(), cx).is_pending());
                                std::task::Poll::Ready(())
                            })
                            .await;
                        }
                        blocking
                    } else {
                        let mut handle = store
                            .open_raw_guarded(
                                &staged.storage_path,
                                None,
                                BlobCipher::KnownPlaintext,
                                &staged.compression,
                                size,
                                lease,
                            )
                            .await
                            .unwrap();
                        let blocking = occupy_blocking_thread().await;
                        {
                            let next = handle.body.next();
                            futures_util::pin_mut!(next);
                            std::future::poll_fn(|cx| {
                                assert!(std::future::Future::poll(next.as_mut(), cx).is_pending());
                                std::task::Poll::Ready(())
                            })
                            .await;
                        }
                        drop(handle);
                        blocking
                    };
                    // Release before asserting so a failing regression cannot strand the runtime.
                    let retained = weak.upgrade().is_some();
                    release.send(()).unwrap();
                    blocker.await.unwrap();
                    tokio::task::spawn_blocking(|| ()).await.unwrap();
                    assert!(
                        retained,
                        "cancelled read released reservation before blocking task exited"
                    );
                    assert!(weak.upgrade().is_none(), "finished read leaked reservation");
                }
            }
        });
    }

    #[test]
    fn small_read_never_exceeds_the_probed_length_even_when_backing_bytes_grow() {
        let mut file = std::io::Cursor::new(b"trusted appended after fstat");
        assert_eq!(read_small(&mut file, 7).unwrap().as_ref(), b"trusted");
        assert_eq!(file.position(), 7);
        // Preserve the existing short-read handling when an external writer truncates instead.
        let mut short = std::io::Cursor::new(b"short");
        assert_eq!(read_small(&mut short, 20).unwrap().as_ref(), b"short");
    }

    #[test]
    fn small_read_retries_interrupted_io_within_the_same_bound() {
        struct InterruptOnce {
            inner: std::io::Cursor<&'static [u8]>,
            interrupted: bool,
        }
        impl std::io::Read for InterruptOnce {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                std::io::Read::read(&mut self.inner, buf)
            }
        }
        let mut source = InterruptOnce {
            inner: std::io::Cursor::new(b"retry and stop"),
            interrupted: false,
        };
        assert_eq!(read_small(&mut source, 5).unwrap().as_ref(), b"retry");
        assert_eq!(source.inner.position(), 5);
    }
}
