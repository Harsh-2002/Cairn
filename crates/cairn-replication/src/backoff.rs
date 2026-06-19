//! Deterministic exponential backoff for replication retries (ARCH 20.4).

/// Compute the backoff delay, in whole seconds, before the next replication attempt.
///
/// The schedule is exponential in the number of attempts already made and capped at `cap`:
/// the delay after `attempts` failed attempts is `base * 2^(attempts - 1)`, clamped to
/// `[base, cap]`. `attempts == 0` (the first try has not yet failed) yields `base`, so the
/// first retry waits `base` and each subsequent retry doubles until it reaches `cap`. The
/// computation saturates rather than overflowing, so a large `attempts` simply pins at `cap`.
///
/// It is a pure function of its arguments, so the retry schedule is fully deterministic and
/// testable with a controllable clock.
#[must_use]
pub fn next_backoff(attempts: u32, base: u64, cap: u64) -> u64 {
    // Treat the very first attempt (none failed yet) the same as one failed attempt: wait
    // `base`. From there each additional failed attempt doubles the previous delay.
    let exponent = attempts.saturating_sub(1).min(63);
    let factor = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let raw = base.saturating_mul(factor);
    raw.clamp(base.min(cap), cap)
}

#[cfg(test)]
mod tests {
    use super::next_backoff;

    #[test]
    fn grows_exponentially_then_caps() {
        let base = 2;
        let cap = 60;
        // 0 and 1 failed attempts both wait the base delay.
        assert_eq!(next_backoff(0, base, cap), 2);
        assert_eq!(next_backoff(1, base, cap), 2);
        // Each further attempt doubles: 2, 4, 8, 16, 32 ...
        assert_eq!(next_backoff(2, base, cap), 4);
        assert_eq!(next_backoff(3, base, cap), 8);
        assert_eq!(next_backoff(4, base, cap), 16);
        assert_eq!(next_backoff(5, base, cap), 32);
        // ... then pins at the cap.
        assert_eq!(next_backoff(6, base, cap), 60);
        assert_eq!(next_backoff(7, base, cap), 60);
        assert_eq!(next_backoff(1_000_000, base, cap), 60);
    }

    #[test]
    fn monotonic_non_decreasing() {
        let base = 1;
        let cap = 1000;
        let mut prev = 0;
        for attempts in 0..40 {
            let d = next_backoff(attempts, base, cap);
            assert!(d >= prev, "backoff must never decrease");
            assert!(d <= cap, "backoff must never exceed the cap");
            prev = d;
        }
    }

    #[test]
    fn cap_below_base_is_respected() {
        // A degenerate config where the cap is smaller than the base still never exceeds cap.
        assert_eq!(next_backoff(0, 100, 10), 10);
        assert_eq!(next_backoff(5, 100, 10), 10);
    }
}
