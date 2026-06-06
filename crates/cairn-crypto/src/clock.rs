//! The production [`Clock`] backed by the operating-system wall clock.

use cairn_types::time::Timestamp;
use cairn_types::traits::Clock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Production [`Clock`]: the current wall-clock time as Unix milliseconds.
///
/// Time governs signature-skew validation, lifecycle expiry, multipart staleness, and
/// replication backoff; tests substitute the controllable `TestClock` double from
/// `cairn-types` so those behaviours are deterministic.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl SystemClock {
    /// Construct a system clock.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        // Clamp to the i64 millis range. A time before the Unix epoch yields a negative
        // duration, which we represent as a negative timestamp; both branches saturate
        // rather than overflow.
        let millis = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
            Err(e) => i64::try_from(e.duration().as_millis())
                .map(|m| -m)
                .unwrap_or(i64::MIN),
        };
        Timestamp(millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_after_a_known_past_epoch() {
        // 2023-11-14T22:13:20Z in millis — the default of the test double.
        let known_past = Timestamp::from_secs(1_700_000_000);
        let now = SystemClock::new().now();
        assert!(
            now > known_past,
            "system clock {now:?} should be after {known_past:?}"
        );
    }

    #[test]
    fn now_is_monotonic_nondecreasing_across_calls() {
        let c = SystemClock;
        let a = c.now();
        let b = c.now();
        assert!(b >= a, "{b:?} should be >= {a:?}");
    }

    #[test]
    fn default_and_new_both_yield_sane_time() {
        // Exercise the `Default` impl through a typed binding so the constructor is covered
        // without tripping the unit-struct lint.
        let via_default: SystemClock = Default::default();
        let a = via_default.now();
        let b = SystemClock::new().now();
        // Both should be sane positive timestamps in the same ballpark.
        assert!(a.as_millis() > 0);
        assert!(b.as_millis() > 0);
    }
}
