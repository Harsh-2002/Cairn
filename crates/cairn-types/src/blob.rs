//! Value types crossing the [`crate::BlobStore`] boundary: staging options/results, read
//! handles (with an optional zero-copy hint), ranges, and reconciliation reports.

use crate::bucket::CompressionPolicy;
use crate::id::StoragePath;
use crate::object::{ChecksumSet, ChecksumValue, CompressionDescriptor, ETag};
use std::sync::Arc;

/// Options controlling how an object is staged.
#[derive(Debug, Clone)]
pub struct StageOptions {
    /// The bucket compression policy (None = store uncompressed).
    pub compression: Option<CompressionPolicy>,
    /// Supplementary checksum algorithms to compute over the plaintext.
    pub extra_checksums: ChecksumSet,
    /// Hard size ceiling; staging aborts and cleans up if the body exceeds it.
    pub size_ceiling: u64,
    /// The content type, used only by the incompressibility heuristic.
    pub content_type: String,
    /// The raw 32-byte data-encryption key (DEK) for SSE-S3. When `Some`, the blob store
    /// encrypts each physical block with AES-256-GCM *after* compression (compress-then-encrypt),
    /// so ciphertext incompressibility never inflates a compressed block. `None` stores plaintext
    /// blocks as before; the field is additive and defaults to `None` so existing callers are
    /// unaffected (ARCH 27, SSE-S3).
    pub encryption: Option<[u8; 32]>,
    /// The object's declared content length, when known up front (from the `Content-Length` header
    /// on a non-streaming PUT). `Some` lets the write path preallocate the staging file so the
    /// filesystem places it contiguously and an out-of-space condition surfaces immediately (ARCH
    /// 7.5); `None` (streaming/chunked uploads, multipart parts) skips preallocation. It is the
    /// PLAINTEXT length; with compression the physical file may be smaller, which preallocation
    /// tolerates (it reserves blocks without padding the file).
    pub content_length: Option<u64>,
}

impl Default for StageOptions {
    /// Uncompressed, unencrypted, no supplementary checksums, a 5 GiB ceiling, and an
    /// octet-stream content type. Callers override only the fields they care about via the
    /// struct-update syntax, which keeps the SSE/compression additions backward-compatible.
    fn default() -> Self {
        Self {
            compression: None,
            extra_checksums: ChecksumSet::none(),
            size_ceiling: 5 * 1024 * 1024 * 1024,
            content_type: "application/octet-stream".to_owned(),
            encryption: None,
            content_length: None,
        }
    }
}

/// The result of staging a durable blob. The caller validates the computed hashes against
/// any client-supplied checksums; the blob store does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedBlob {
    /// The opaque path of the durable blob.
    pub storage_path: StoragePath,
    /// The plaintext length.
    pub size_logical: u64,
    /// The on-disk length (after any compression).
    pub size_physical: u64,
    /// The plaintext-MD5 entity tag.
    pub etag: ETag,
    /// The raw hex MD5 (for content-MD5 verification).
    pub md5_hex: String,
    /// The computed supplementary checksums.
    pub checksums: Vec<ChecksumValue>,
    /// How the blob was physically stored.
    pub compression: CompressionDescriptor,
}

/// The result of staging one multipart part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedPart {
    /// The opaque path of the part blob.
    pub storage_path: StoragePath,
    /// The plaintext part size.
    pub size: u64,
    /// The part's hex MD5 (its part ETag, used in the multipart ETag formula).
    pub md5_hex: String,
}

/// A reference to one part during assembly, in part-number order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartRef {
    /// The part number.
    pub part_number: u16,
    /// The part blob path.
    pub storage_path: StoragePath,
    /// The plaintext part size.
    pub size: u64,
}

/// A resolved logical byte range (already validated against the object size by the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// The starting logical offset (inclusive).
    pub offset: u64,
    /// The number of logical bytes to transfer.
    pub length: u64,
}

/// The `Content-Range` of a partial response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    /// First byte position (inclusive).
    pub start: u64,
    /// Last byte position (inclusive).
    pub end: u64,
    /// Total object length.
    pub total: u64,
}

/// A zero-copy read hint: a committed, uncompressed blob the server may stream to a socket
/// via a kernel file-to-socket transfer. The portable path ignores this and uses the body
/// stream from [`BlobReadHandle::body`].
#[derive(Debug, Clone)]
pub struct ZeroCopyRead {
    /// The opened blob file.
    pub file: Arc<std::fs::File>,
    /// The starting offset within the file.
    pub offset: u64,
    /// The number of bytes to transfer.
    pub len: u64,
}

/// A handle to a committed blob opened for reading. Always carries a portable [`body`]
/// stream of logical (decompressed) bytes; `zero_copy` is `Some` only on the fast path.
///
/// [`body`]: BlobReadHandle::body
pub struct BlobReadHandle {
    /// The number of logical bytes this handle will yield.
    pub logical_len: u64,
    /// The content range for a partial read, if a range was requested.
    pub content_range: Option<ContentRange>,
    /// The portable stream of logical bytes.
    pub body: crate::BlobStream,
    /// An optional zero-copy fast-path hint.
    pub zero_copy: Option<ZeroCopyRead>,
}

impl std::fmt::Debug for BlobReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobReadHandle")
            .field("logical_len", &self.logical_len)
            .field("content_range", &self.content_range)
            .field("zero_copy", &self.zero_copy.is_some())
            .finish_non_exhaustive()
    }
}

/// Options controlling reconciliation.
#[derive(Debug, Clone, Copy)]
pub struct ReconcileOpts {
    /// Staging artifacts younger than this many seconds are left alone (in-flight writes).
    pub staging_safety_margin_secs: i64,
    /// Bounded parallelism across buckets.
    pub parallelism: usize,
    /// Batch size for membership checks (bounds memory).
    pub batch_size: u32,
}

impl Default for ReconcileOpts {
    fn default() -> Self {
        Self {
            staging_safety_margin_secs: 3600,
            parallelism: 4,
            batch_size: 1024,
        }
    }
}

/// What reconciliation did, reported as counts and surfaced as metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Blobs examined.
    pub blobs_scanned: u64,
    /// Orphaned blobs reclaimed.
    pub orphans_reclaimed: u64,
    /// Stale staging artifacts removed.
    pub staging_cleaned: u64,
    /// Stale multipart session directories removed.
    pub sessions_cleaned: u64,
    /// Emptied directories pruned.
    pub dirs_pruned: u64,
    /// Non-fatal errors encountered.
    pub errors: u64,
}
