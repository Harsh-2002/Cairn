//! Object-version types: the metadata row, checksums, storage class, and the compression
//! descriptor. A version of an object is one row referencing one blob; a delete marker is a
//! row with no storage path.

use crate::authz::Acl;
use crate::bucket::CompressionAlgorithm;
use crate::id::{BucketName, ObjectKey, StoragePath, UserId, VersionId};
use crate::time::Timestamp;
use serde::{Deserialize, Serialize};
use std::fmt;

/// An entity tag, stored unquoted (the single quoting point is the S3 renderer).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ETag(String);

impl ETag {
    /// A single-part ETag: the hex MD5 of the plaintext content.
    #[must_use]
    pub fn from_md5_hex(hex: String) -> Self {
        Self(hex)
    }

    /// A multipart ETag: `<hex>-<part_count>`.
    #[must_use]
    pub fn multipart(hex: String, part_count: usize) -> Self {
        Self(format!("{hex}-{part_count}"))
    }

    /// Construct from a stored/wire string (without surrounding quotes).
    #[must_use]
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// The unquoted value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this is a multipart ETag (contains a `-<count>` suffix).
    #[must_use]
    pub fn is_multipart(&self) -> bool {
        self.0.contains('-')
    }
}

impl fmt::Debug for ETag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ETag({})", self.0)
    }
}

/// A supplementary checksum algorithm S3 supports (beyond the always-computed MD5/ETag).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    /// CRC32 (IEEE).
    Crc32,
    /// CRC32C (Castagnoli).
    Crc32c,
    /// SHA-1.
    Sha1,
    /// SHA-256.
    Sha256,
}

/// A computed checksum value, base64-encoded as S3 carries it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksumValue {
    /// The algorithm.
    pub algorithm: ChecksumAlgorithm,
    /// The base64-encoded digest.
    pub value: String,
}

/// The set of supplementary checksum algorithms to compute over an object's plaintext.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChecksumSet(pub Vec<ChecksumAlgorithm>);

impl ChecksumSet {
    /// An empty set (only the MD5/ETag is computed).
    #[must_use]
    pub fn none() -> Self {
        Self(Vec::new())
    }

    /// Whether any supplementary checksum is requested.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The storage class of an object version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageClass {
    /// Local standard storage.
    Standard,
    /// Transitioned to the remote cold tier.
    ColdTier,
}

/// How a blob is physically stored (recorded on the row so a reader knows the format).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionDescriptor {
    /// Stored byte-for-byte.
    Uncompressed,
    /// Stored in the self-describing block-compressed format.
    Compressed {
        /// The algorithm used.
        algorithm: CompressionAlgorithm,
        /// The logical block size.
        block_size: u32,
    },
}

/// User-defined metadata (`x-amz-meta-*`) carried with an object.
pub type UserMetadata = Vec<(String, String)>;

/// The retention mode of an S3 Object Lock (ARCH 19.6). `Compliance` is immutable until the
/// retain-until date passes — not even an administrator may shorten it or delete the version;
/// `Governance` is the same but may be bypassed by a principal holding
/// `s3:BypassGovernanceRetention` who passes `x-amz-bypass-governance-retention: true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectLockMode {
    /// Bypassable retention (with the bypass permission + header).
    Governance,
    /// Immutable retention until the retain-until date passes.
    Compliance,
}

/// A retention setting on a single object version: a mode and the instant until which it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRetention {
    /// The retention mode.
    pub mode: ObjectLockMode,
    /// The version is protected from deletion/overwrite until this time.
    pub retain_until: Timestamp,
}

/// The full Object Lock state of one version: an optional retention plus an independent legal hold.
/// A version is protected from permanent deletion while retention is active OR a legal hold is on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockState {
    /// The active retention, if any.
    pub retention: Option<ObjectRetention>,
    /// Whether a legal hold is in force (independent of retention; never expires on its own).
    pub legal_hold: bool,
}

impl ObjectLockState {
    /// Whether this version is protected from permanent deletion at `now`: a legal hold is on, or a
    /// retention is set whose retain-until is still in the future.
    #[must_use]
    pub fn is_protected(&self, now: Timestamp) -> bool {
        self.legal_hold || self.retention.is_some_and(|r| r.retain_until > now)
    }
}

/// One version of one object key — the central metadata record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersionRow {
    /// Opaque row id (also the basis of the storage path).
    pub id: String,
    /// The bucket.
    pub bucket: BucketName,
    /// The object key.
    pub key: ObjectKey,
    /// The version id (sentinel `null` for unversioned/suspended single versions).
    pub version_id: VersionId,
    /// Whether a plain GET resolves to this version.
    pub is_latest: bool,
    /// Whether this is a delete marker (carries no blob).
    pub is_delete_marker: bool,
    /// The plaintext length reported to clients.
    pub size_logical: u64,
    /// The on-disk length (operator-visible only).
    pub size_physical: u64,
    /// The entity tag (plaintext MD5 or multipart form).
    pub etag: ETag,
    /// The MIME content type.
    pub content_type: String,
    /// The `Content-Encoding` header to echo on GET/HEAD (None if not supplied).
    pub content_encoding: Option<String>,
    /// The `Cache-Control` header to echo on GET/HEAD (None if not supplied).
    pub cache_control: Option<String>,
    /// The `Content-Disposition` header to echo on GET/HEAD (None if not supplied).
    pub content_disposition: Option<String>,
    /// The `Content-Language` header to echo on GET/HEAD (None if not supplied).
    pub content_language: Option<String>,
    /// The `Expires` header to echo on GET/HEAD (None if not supplied).
    pub expires: Option<String>,
    /// The opaque blob path (None for delete markers).
    pub storage_path: Option<StoragePath>,
    /// How the blob is compressed.
    pub compression: CompressionDescriptor,
    /// The storage class.
    pub storage_class: StorageClass,
    /// The remote locator when transitioned to the cold tier.
    pub cold_locator: Option<String>,
    /// The version owner under the ownership mode.
    pub owner_id: UserId,
    /// User-defined metadata.
    pub user_metadata: UserMetadata,
    /// The object ACL where ownership keeps ACLs in force.
    pub acl: Option<Acl>,
    /// Any client-supplied checksums.
    pub checksums: Vec<ChecksumValue>,
    /// The SSE-S3 descriptor when this version's data is server-side encrypted: a JSON document
    /// `{alg, wrapped_dek_b64, nonce_b64}` recording the algorithm, the data-encryption key wrapped
    /// (sealed) under the master key, and the wrapping nonce. `None` for unencrypted objects
    /// (ARCH 27, SSE-S3). The raw DEK is never stored; only its sealed form lives here.
    pub sse_descriptor: Option<String>,
    /// Replication status for replication-enabled buckets.
    pub replication_status: Option<crate::meta::ReplicationStatus>,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last-update time.
    pub updated_at: Timestamp,
}
