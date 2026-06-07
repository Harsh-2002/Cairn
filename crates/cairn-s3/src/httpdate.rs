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

/// Parse an HTTP date in the preferred RFC 1123 / IMF-fixdate form, e.g.
/// `Wed, 24 May 2013 00:00:00 GMT`, into a [`Timestamp`]. Returns `None` for any value that does
/// not parse (the caller then ignores the conditional header, matching S3's lenient handling).
/// Only the fixed-length IMF form is accepted; the obsolete RFC 850 / asctime forms are not used
/// by modern S3 clients and are intentionally not supported.
#[must_use]
pub fn parse_http_date(s: &str) -> Option<Timestamp> {
    let s = s.trim();
    // "Day, DD Mon YYYY HH:MM:SS GMT"
    let rest = s.split_once(", ")?.1; // drop the weekday and ", "
    let mut parts = rest.split(' ');
    let day: i64 = parts.next()?.parse().ok()?;
    let mon_str = parts.next()?;
    let year: i64 = parts.next()?.parse().ok()?;
    let time = parts.next()?;
    let tz = parts.next()?;
    if tz != "GMT" || parts.next().is_some() {
        return None;
    }
    let month = MONTHS.iter().position(|m| *m == mon_str)? as i64 + 1;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut hms = time.split(':');
    let hour: i64 = hms.next()?.parse().ok()?;
    let min: i64 = hms.next()?.parse().ok()?;
    let sec: i64 = hms.next()?.parse().ok()?;
    if hms.next().is_some() || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let days = days_from_civil(year, month as u32, day as u32);
    let secs = days * 86400 + hour * 3600 + min * 60 + sec;
    Some(Timestamp(secs * 1000))
}

/// Howard Hinnant's `days_from_civil`: days since the Unix epoch for a proleptic Gregorian date.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
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

    #[test]
    fn parse_known_dates() {
        assert_eq!(
            parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"),
            Some(Timestamp(0))
        );
        assert_eq!(
            parse_http_date("Fri, 24 May 2013 00:00:00 GMT"),
            Some(Timestamp(1_369_353_600_000))
        );
        // Round-trips against the formatter for an arbitrary instant (whole seconds).
        let ts = Timestamp(1_700_000_123_000);
        assert_eq!(parse_http_date(&http_date(ts)), Some(ts));
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_http_date(""), None);
        assert_eq!(parse_http_date("not a date"), None);
        assert_eq!(parse_http_date("Fri, 24 Foo 2013 00:00:00 GMT"), None);
        assert_eq!(parse_http_date("Fri, 24 May 2013 00:00:00 PST"), None);
        assert_eq!(parse_http_date("Fri, 24 May 2013 25:00:00 GMT"), None);
    }
}
