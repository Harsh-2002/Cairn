//! Bucket-level types: the bucket record, versioning state, the per-bucket configuration
//! aspects (each one logical document), and the compression policy.

use crate::id::{BucketName, UserId};
use crate::time::Timestamp;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    /// A positive number of days, at most [`DefaultRetention::MAX_DAYS`].
    Days(u32),
    /// A positive number of years, at most [`DefaultRetention::MAX_YEARS`] (365 days each).
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

/// A default Object Lock retention period or computed deadline is outside Cairn's supported range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DefaultRetentionError {
    /// S3 default retention periods must be positive.
    #[error("default retention period must be greater than zero")]
    Zero,
    /// A day-based period exceeds [`DefaultRetention::MAX_DAYS`].
    #[error("default retention period cannot exceed 36500 days")]
    DaysTooLarge,
    /// A year-based period exceeds [`DefaultRetention::MAX_YEARS`].
    #[error("default retention period cannot exceed 100 years")]
    YearsTooLarge,
    /// Adding the validated period to the object creation time exceeded the timestamp range.
    #[error("default retention date is outside the supported timestamp range")]
    TimestampOverflow,
}

impl DefaultRetention {
    /// Maximum supported day-based default retention (100 365-day years).
    pub const MAX_DAYS: u32 = 36_500;
    /// Maximum supported year-based default retention.
    pub const MAX_YEARS: u32 = 100;
    const MILLIS_PER_DAY: i64 = 86_400_000;

    /// Validate the positive, bounded default-retention period.
    ///
    /// # Errors
    ///
    /// Returns [`DefaultRetentionError`] for a zero or over-limit period.
    pub fn validate(&self) -> Result<(), DefaultRetentionError> {
        match self.period {
            RetentionPeriod::Days(0) | RetentionPeriod::Years(0) => {
                Err(DefaultRetentionError::Zero)
            }
            RetentionPeriod::Days(days) if days > Self::MAX_DAYS => {
                Err(DefaultRetentionError::DaysTooLarge)
            }
            RetentionPeriod::Years(years) if years > Self::MAX_YEARS => {
                Err(DefaultRetentionError::YearsTooLarge)
            }
            RetentionPeriod::Days(_) | RetentionPeriod::Years(_) => Ok(()),
        }
    }

    /// The retain-until instant for an object created at `now`.
    ///
    /// # Errors
    ///
    /// Returns [`DefaultRetentionError`] when the period is invalid or the computed timestamp
    /// cannot be represented. This operation never saturates or wraps: either the exact deadline
    /// is returned or the caller must fail closed.
    pub fn retain_until(&self, now: Timestamp) -> Result<Timestamp, DefaultRetentionError> {
        self.validate()?;
        let days = match self.period {
            RetentionPeriod::Days(d) => i64::from(d),
            RetentionPeriod::Years(y) => i64::from(y)
                .checked_mul(365)
                .ok_or(DefaultRetentionError::TimestampOverflow)?,
        };
        let duration = days
            .checked_mul(Self::MILLIS_PER_DAY)
            .ok_or(DefaultRetentionError::TimestampOverflow)?;
        now.0
            .checked_add(duration)
            .map(Timestamp)
            .ok_or(DefaultRetentionError::TimestampOverflow)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn retention(period: RetentionPeriod) -> DefaultRetention {
        DefaultRetention {
            mode: crate::object::ObjectLockMode::Governance,
            period,
        }
    }

    #[test]
    fn default_retention_accepts_exact_supported_maxima() {
        let expected = Timestamp(3_153_600_000_000);
        assert_eq!(
            retention(RetentionPeriod::Days(DefaultRetention::MAX_DAYS))
                .retain_until(Timestamp::EPOCH),
            Ok(expected)
        );
        assert_eq!(
            retention(RetentionPeriod::Years(DefaultRetention::MAX_YEARS))
                .retain_until(Timestamp::EPOCH),
            Ok(expected)
        );
    }

    #[test]
    fn default_retention_rejects_zero_and_over_limit_periods() {
        for period in [RetentionPeriod::Days(0), RetentionPeriod::Years(0)] {
            assert_eq!(
                retention(period).retain_until(Timestamp::EPOCH),
                Err(DefaultRetentionError::Zero)
            );
        }
        assert_eq!(
            retention(RetentionPeriod::Days(DefaultRetention::MAX_DAYS + 1))
                .retain_until(Timestamp::EPOCH),
            Err(DefaultRetentionError::DaysTooLarge)
        );
        assert_eq!(
            retention(RetentionPeriod::Years(DefaultRetention::MAX_YEARS + 1))
                .retain_until(Timestamp::EPOCH),
            Err(DefaultRetentionError::YearsTooLarge)
        );
    }

    #[test]
    fn default_retention_rejects_timestamp_overflow() {
        assert_eq!(
            retention(RetentionPeriod::Days(1)).retain_until(Timestamp(i64::MAX)),
            Err(DefaultRetentionError::TimestampOverflow)
        );
    }
}
