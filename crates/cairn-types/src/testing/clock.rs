//! A controllable clock for deterministic time-dependent tests.

use crate::time::Timestamp;
use crate::traits::Clock;
use std::sync::atomic::{AtomicI64, Ordering};

/// A clock whose time is set explicitly and advanced by the test, so skew validation,
/// lifecycle expiry, multipart staleness, and replication backoff are deterministic.
#[derive(Debug)]
pub struct TestClock {
    millis: AtomicI64,
}

impl TestClock {
    /// A clock fixed at `secs` seconds since the epoch.
    #[must_use]
    pub fn at_secs(secs: i64) -> Self {
        Self {
            millis: AtomicI64::new(secs.saturating_mul(1000)),
        }
    }

    /// Set the current time.
    pub fn set(&self, ts: Timestamp) {
        self.millis.store(ts.as_millis(), Ordering::SeqCst);
    }

    /// Advance the clock by `secs` seconds.
    pub fn advance_secs(&self, secs: i64) {
        self.millis
            .fetch_add(secs.saturating_mul(1000), Ordering::SeqCst);
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::at_secs(1_700_000_000)
    }
}

impl Clock for TestClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.millis.load(Ordering::SeqCst))
    }
}
