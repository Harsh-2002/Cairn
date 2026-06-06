//! Bucket-level types: the bucket record, versioning state, the per-bucket configuration
//! aspects (each one logical document), and the compression policy.

use crate::id::{BucketName, UserId};
use crate::time::Timestamp;
use serde::{Deserialize, Serialize};

/// A bucket record (the row, without its associated configuration documents).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bucket {
    /// The bucket name (primary key).
    pub name: BucketName,
    /// The owning user.
    pub owner_id: UserId,
    /// Creation time.
    pub created_at: Timestamp,
    /// Versioning state.
    pub versioning: VersioningState,
    /// Object Ownership mode.
    pub ownership_mode: crate::authz::OwnershipMode,
    /// The region label returned by the location operation.
    pub region: String,
    /// The per-bucket compression policy (absent means off).
    pub compression: Option<CompressionPolicy>,
}

/// A bucket's versioning state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningState {
    /// Never versioned; a single sentinel version per key, overwritten in place.
    Unversioned,
    /// Versioning enabled; every put creates a new identified version.
    Enabled,
    /// Versioning suspended; new puts use the sentinel, existing versions retained.
    Suspended,
}

/// Which per-bucket configuration aspect a get/set targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConfigAspect {
    /// The bucket policy.
    Policy,
    /// The bucket ACL.
    Acl,
    /// The CORS configuration.
    Cors,
    /// The lifecycle configuration.
    Lifecycle,
    /// The replication configuration.
    Replication,
    /// The tag set.
    Tagging,
    /// The bucket-level Block Public Access settings.
    PublicAccessBlock,
}

/// An opaque validated configuration document (stored as text/JSON). The typed parse lives
/// in the relevant subsystem; the store treats it as one logical document per bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDoc(pub String);

/// A per-bucket compression policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionPolicy {
    /// The algorithm to use.
    pub algorithm: CompressionAlgorithm,
    /// The logical block size in bytes (independently compressed for range-friendly reads).
    pub block_size: u32,
}

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Zstd,
            block_size: 256 * 1024,
        }
    }
}

/// A compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression.
    None,
    /// Zstandard (default; good ratio/speed balance).
    Zstd,
    /// LZ4 (faster, lower ratio).
    Lz4,
}
