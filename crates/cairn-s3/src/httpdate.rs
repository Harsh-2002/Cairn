//! RFC 1123 / IMF-fixdate formatting for the `Last-Modified` header, from a millisecond
//! timestamp, without pulling a date library.

use cairn_types::time::Timestamp;

const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Format a timestamp as an HTTP date, e.g. `Wed, 24 May 2013 00:00:00 GMT`.
#[must_use]
pub fn http_date(ts: Timestamp) -> String {
    let secs = ts.as_millis().div_euclid(1000);
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Day of week: 1970-01-01 was a Thursday (index 4).
    let dow = ((days.rem_euclid(7)) + 4).rem_euclid(7) as usize;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        DAYS[dow],
        d,
        MONTHS[(m - 1) as usize],
        y,
        hour,
        min,
        sec
    )
}

/// Inverse of Howard Hinnant's `days_from_civil`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_dates() {
        assert_eq!(http_date(Timestamp(0)), "Thu, 01 Jan 1970 00:00:00 GMT");
        // 2013-05-24T00:00:00Z = 1369353600
        assert_eq!(
            http_date(Timestamp(1_369_353_600_000)),
            "Fri, 24 May 2013 00:00:00 GMT"
        );
    }
}
