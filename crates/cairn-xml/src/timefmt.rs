//! A small, dependency-free ISO-8601 UTC formatter for [`Timestamp`] (Unix milliseconds).
//!
//! S3 renders timestamps as e.g. `2026-06-06T21:00:00.000Z` — always UTC, always with
//! millisecond precision and a `Z` suffix. We avoid pulling `chrono`/`time` by computing the
//! civil date from the day count with Howard Hinnant's `civil_from_days` algorithm, which is
//! exact for the full proleptic Gregorian range.

use cairn_types::Timestamp;

/// Format a [`Timestamp`] (milliseconds since the Unix epoch, UTC) as an ISO-8601 string with
/// millisecond precision and a `Z` suffix, e.g. `2026-06-06T21:00:00.000Z`.
///
/// Negative timestamps (before 1970) are handled correctly via Euclidean division.
#[must_use]
pub fn format_iso8601(ts: Timestamp) -> String {
    let millis_total = ts.as_millis();
    // Split into whole days and the millisecond-of-day remainder, flooring toward -inf so
    // pre-epoch instants land on the correct civil day.
    let days = millis_total.div_euclid(86_400_000);
    let ms_of_day = millis_total.rem_euclid(86_400_000);

    let (year, month, day) = civil_from_days(days);

    let hour = ms_of_day / 3_600_000;
    let minute = (ms_of_day % 3_600_000) / 60_000;
    let second = (ms_of_day % 60_000) / 1_000;
    let milli = ms_of_day % 1_000;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{milli:03}Z")
}

/// Howard Hinnant's `civil_from_days`: convert a count of days since 1970-01-01 to the
/// `(year, month, day)` civil date. Correct for the entire `i64` range of interest.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    // Shift the epoch to 0000-03-01 so leap days fall at the end of the 400-year era.
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch() {
        assert_eq!(format_iso8601(Timestamp(0)), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn known_instant() {
        // 2026-06-06T21:00:00.000Z. Compute the expected millis independently:
        // days from 1970-01-01 to 2026-06-06.
        let days = days_from_civil(2026, 6, 6);
        let millis = days * 86_400_000 + 21 * 3_600_000;
        assert_eq!(
            format_iso8601(Timestamp(millis)),
            "2026-06-06T21:00:00.000Z"
        );
    }

    #[test]
    fn millisecond_precision_preserved() {
        let days = days_from_civil(2000, 1, 1);
        let millis = days * 86_400_000 + 123;
        assert_eq!(
            format_iso8601(Timestamp(millis)),
            "2000-01-01T00:00:00.123Z"
        );
    }

    #[test]
    fn leap_day() {
        let days = days_from_civil(2024, 2, 29);
        let millis = days * 86_400_000 + 12 * 3_600_000 + 30 * 60_000 + 45 * 1_000;
        assert_eq!(
            format_iso8601(Timestamp(millis)),
            "2024-02-29T12:30:45.000Z"
        );
    }

    #[test]
    fn pre_epoch() {
        // 1969-12-31T23:59:59.000Z is -1000 ms.
        assert_eq!(format_iso8601(Timestamp(-1000)), "1969-12-31T23:59:59.000Z");
    }

    #[test]
    fn round_trip_many_days() {
        // For every 37th day across ~80 years, formatting then re-parsing the date must
        // reproduce the same civil date (sanity on civil_from_days vs days_from_civil).
        for d in (-10_000..20_000).step_by(37) {
            let (y, m, day) = civil_from_days(d);
            assert_eq!(
                days_from_civil(y, m, day),
                d,
                "round trip failed for day {d}"
            );
        }
    }

    /// The inverse algorithm, used only to derive expected values in tests.
    fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
        let doy = (153 * mp + 2) / 5 + d as i64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }
}
