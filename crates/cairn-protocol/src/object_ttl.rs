//! Parsing and validation for the opt-in `X-Amz-Meta-Object-Expires` extension.
//!
//! The value is either a headed relative duration (`+2h`, `+1.5h`, `+2h45m`) or an RFC3339 UTC
//! instant. Relative values are resolved against the object's commit timestamp; absolute values are
//! rechecked there so a slow upload cannot commit an already-expired object.

use cairn_types::Error;
use cairn_types::time::Timestamp;

/// A parsed TTL request, before it is anchored to an object's creation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TtlSpec {
    /// A positive duration in milliseconds from object creation.
    Relative { millis: i64 },
    /// An absolute deadline.
    Absolute { at: Timestamp },
}

impl TtlSpec {
    /// Resolve the request against the object's commit time and enforce the bucket ceiling.
    ///
    /// # Errors
    /// Returns `InvalidArgument` when the resulting deadline is not in the future or exceeds the
    /// bucket's `max_ttl_secs` ceiling.
    pub(crate) fn resolve(
        self,
        created_at: Timestamp,
        max_ttl_secs: u64,
    ) -> Result<Timestamp, Error> {
        let max_millis = i64::try_from(max_ttl_secs)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000);
        let expires = match self {
            Self::Relative { millis } => {
                if millis <= 0 || millis > max_millis {
                    return Err(Error::InvalidArgument(
                        "object TTL is outside the bucket's allowed range".to_owned(),
                    ));
                }
                Timestamp(created_at.0.saturating_add(millis))
            }
            Self::Absolute { at } => at,
        };
        if expires <= created_at {
            return Err(Error::InvalidArgument(
                "object TTL must be in the future".to_owned(),
            ));
        }
        if expires.0.saturating_sub(created_at.0) > max_millis {
            return Err(Error::InvalidArgument(
                "object TTL exceeds the bucket maximum".to_owned(),
            ));
        }
        Ok(expires)
    }
}

/// Parse an `X-Amz-Meta-Object-Expires` value.
///
/// A leading `+` selects the relative form; everything else must be an RFC3339 UTC instant.
///
/// # Errors
/// Returns `InvalidArgument` for an empty, malformed, zero, or negative relative value and for an
/// unparseable absolute timestamp.
pub(crate) fn parse(value: &str) -> Result<TtlSpec, Error> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Error::InvalidArgument(
            "object TTL must not be empty".to_owned(),
        ));
    }
    if value.starts_with('+') {
        let millis = parse_relative(value).ok_or_else(|| {
            Error::InvalidArgument(
                "object TTL must be a positive duration such as +2h or +1.5h".to_owned(),
            )
        })?;
        return Ok(TtlSpec::Relative { millis });
    }
    let at = cairn_xml::parse_iso8601(value).ok_or_else(|| {
        Error::InvalidArgument(
            "object TTL must be a positive duration or an RFC3339 UTC timestamp".to_owned(),
        )
    })?;
    Ok(TtlSpec::Absolute { at })
}

/// Parse a headed Go-style duration with optional `d`/`w` units into whole milliseconds, rounding
/// sub-millisecond values up so a positive TTL never becomes zero.
fn parse_relative(value: &str) -> Option<i64> {
    let mut rest = value.strip_prefix('+')?;
    if rest.is_empty() {
        return None;
    }
    let mut total_nanos: i128 = 0;
    while !rest.is_empty() {
        let number_end = rest.find(|c: char| !c.is_ascii_digit() && c != '.')?;
        if number_end == 0 {
            return None;
        }
        let number = &rest[..number_end];
        let unit_end = rest[number_end..]
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .map_or(rest.len(), |offset| number_end + offset);
        let unit = &rest[number_end..unit_end];
        let unit_nanos: i128 = match unit {
            "ns" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3_600 * 1_000_000_000,
            "d" => 86_400 * 1_000_000_000,
            "w" => 604_800 * 1_000_000_000,
            _ => return None,
        };
        total_nanos = total_nanos.checked_add(parse_decimal_nanos(number, unit_nanos)?)?;
        rest = &rest[unit_end..];
    }
    if total_nanos <= 0 {
        return None;
    }
    let millis = (total_nanos.checked_add(999_999)?) / 1_000_000;
    i64::try_from(millis).ok()
}

fn parse_decimal_nanos(number: &str, unit_nanos: i128) -> Option<i128> {
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if number.ends_with('.') || fraction.len() > 18 {
        return None;
    }
    let whole: i128 = if whole.is_empty() {
        0
    } else {
        whole.parse().ok()?
    };
    let mut denominator: i128 = 1;
    let fraction: i128 = if fraction.is_empty() {
        0
    } else {
        for _ in 0..fraction.len() {
            denominator = denominator.checked_mul(10)?;
        }
        fraction.parse().ok()?
    };
    let whole_nanos = whole.checked_mul(unit_nanos)?;
    let fraction_nanos = fraction.checked_mul(unit_nanos)? / denominator;
    whole_nanos.checked_add(fraction_nanos)
}

#[cfg(test)]
mod tests {
    use super::{TtlSpec, parse};
    use cairn_types::time::Timestamp;

    #[test]
    fn parses_relative_forms() {
        assert_eq!(
            parse("+2h").unwrap(),
            TtlSpec::Relative { millis: 7_200_000 }
        );
        assert_eq!(
            parse("+1.5h").unwrap(),
            TtlSpec::Relative { millis: 5_400_000 }
        );
        assert_eq!(
            parse("+2h45m").unwrap(),
            TtlSpec::Relative { millis: 9_900_000 }
        );
        assert_eq!(
            parse("+7d").unwrap(),
            TtlSpec::Relative {
                millis: 604_800_000
            }
        );
        assert_eq!(
            parse("+1w").unwrap(),
            TtlSpec::Relative {
                millis: 604_800_000
            }
        );
        assert_eq!(parse("+500us").unwrap(), TtlSpec::Relative { millis: 1 });
    }

    #[test]
    fn rejects_invalid_relative_forms() {
        for value in ["+", "+0s", "+-1h", "+2x", "+2", "+2..h", "2h"] {
            assert!(parse(value).is_err(), "{value:?} should be rejected");
        }
    }

    #[test]
    fn parses_absolute_rfc3339() {
        assert_eq!(
            parse("2026-11-01T00:00:00Z").unwrap(),
            TtlSpec::Absolute {
                at: Timestamp::from_secs(1_793_491_200)
            }
        );
    }

    #[test]
    fn resolve_enforces_future_and_ceiling() {
        let now = Timestamp::from_secs(1_700_000_000);
        assert_eq!(
            TtlSpec::Relative { millis: 3_600_000 }
                .resolve(now, 7_200)
                .unwrap(),
            now.plus_secs(3_600)
        );
        assert!(
            TtlSpec::Relative { millis: 7_200_001 }
                .resolve(now, 7_200)
                .is_err()
        );
        assert!(TtlSpec::Absolute { at: now }.resolve(now, 7_200).is_err());
    }
}
