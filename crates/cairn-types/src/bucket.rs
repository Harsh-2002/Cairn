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
    /// The stored replication remote-target descriptors (consumed by the replication engine).
    ReplicationTargets,
    /// The tag set.
    Tagging,
    /// The bucket-level Block Public Access settings.
    PublicAccessBlock,
    /// The default server-side-encryption setting (SSE-S3 applied to new uploads
    /// that do not carry their own `x-amz-server-side-encryption` header).
    Encryption,
    /// The bucket Object Lock configuration: whether object lock is enabled and an optional default
    /// retention (mode + period) stamped onto new object versions.
    ObjectLock,
    /// The bucket event-notification (webhook) configuration: the list of webhook endpoints and
    /// their event/prefix/suffix filters (`crate::notification::NotificationConfig`).
    Notification,
}

/// How long a bucket's default Object Lock retention lasts, as a period from object creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPeriod {
    /// A number of days.
    Days(u32),
    /// A number of years (treated as 365 days each).
    Years(u32),
}

/// A bucket's default Object Lock retention, applied to every new object version on PUT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    /// The retention mode.
    pub mode: crate::object::ObjectLockMode,
    /// The retention period from object creation.
    pub period: RetentionPeriod,
}

impl DefaultRetention {
    /// The retain-until instant for an object created at `now`.
    #[must_use]
    pub fn retain_until(&self, now: Timestamp) -> Timestamp {
        let days = match self.period {
            RetentionPeriod::Days(d) => i64::from(d),
            RetentionPeriod::Years(y) => i64::from(y) * 365,
        };
        Timestamp(now.0 + days * 86_400_000)
    }
}

/// A bucket's Object Lock configuration: whether object lock is enabled and an optional default
/// retention stamped onto new object versions. Stored as JSON under `ConfigAspect::ObjectLock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ObjectLockConfiguration {
    /// Whether object lock is enabled on the bucket (requires versioning).
    pub enabled: bool,
    /// The default retention applied to new versions, if any.
    pub default_retention: Option<DefaultRetention>,
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
