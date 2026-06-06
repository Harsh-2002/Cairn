//! A minimal timestamp type. Time is always obtained through the [`crate::Clock`] trait
//! so that skew validation, lifecycle expiry, multipart staleness, and replication backoff
//! are tested deterministically with a controllable clock.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A point in time as milliseconds since the Unix epoch (UTC).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// The Unix epoch.
    pub const EPOCH: Timestamp = Timestamp(0);

    /// Milliseconds since the Unix epoch.
    #[must_use]
    pub fn as_millis(self) -> i64 {
        self.0
    }

    /// Whole seconds since the Unix epoch (floored).
    #[must_use]
    pub fn as_secs(self) -> i64 {
        self.0.div_euclid(1000)
    }

    /// Construct from whole seconds since the Unix epoch.
    #[must_use]
    pub fn from_secs(secs: i64) -> Self {
        Self(secs.saturating_mul(1000))
    }

    /// This timestamp advanced by `secs` seconds.
    #[must_use]
    pub fn plus_secs(self, secs: i64) -> Self {
        Self(self.0.saturating_add(secs.saturating_mul(1000)))
    }

    /// Whole seconds elapsed from `earlier` to `self` (negative if `self` precedes it).
    #[must_use]
    pub fn secs_since(self, earlier: Timestamp) -> i64 {
        (self.0 - earlier.0).div_euclid(1000)
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({}ms)", self.0)
    }
}
