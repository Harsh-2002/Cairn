//! Condition evaluation against a [`RequestContext`] (ARCH 15.6, Appendix 34.5).
//!
//! An unrecognised condition key makes the statement *not* match (conservative), so an
//! unknown condition can never broaden access.

use crate::matching::wildcard_match;
use cairn_types::authz::{Condition, ConditionOperator, NumericOp};
use cairn_types::{RequestContext, RequesterClass};
use std::net::IpAddr;

/// Evaluate every condition of a statement. All conditions must pass (AND across conditions);
/// within one condition the expected `values` are matched with any-match semantics (OR).
#[must_use]
pub fn conditions_match(
    conditions: &[Condition],
    ctx: &RequestContext,
    requester: &RequesterClass,
) -> bool {
    conditions
        .iter()
        .all(|c| condition_matches(c, ctx, requester))
}

/// A single condition's outcome. Resolves the key's value(s) from the context, then applies
/// the operator. An unrecognised key yields `false`. `if_exists` makes an absent key pass.
#[must_use]
fn condition_matches(c: &Condition, ctx: &RequestContext, requester: &RequesterClass) -> bool {
    // `Null` is the existence operator and is evaluated specially against key presence.
    if c.operator == ConditionOperator::Null {
        return eval_null(c, ctx, requester);
    }

    let actual: Vec<String> = match resolve_key(&c.key, ctx, requester) {
        KeyValue::Unknown => return false, // unknown key => statement does not match
        // Absent recognised key: passes only with the IfExists qualifier.
        KeyValue::Absent => return c.if_exists,
        KeyValue::Single(s) => vec![s],
    };

    apply_operator(c.operator, &actual, &c.values)
}

/// The resolution of a condition key against the request context.
enum KeyValue {
    /// The key is recognised and has a single value.
    Single(String),
    /// The key is recognised but no value is present on this request.
    Absent,
    /// The key is not recognised by Cairn.
    Unknown,
}

/// Resolve a condition key to its context value(s). Tag keys of the form
/// `s3:ExistingObjectTag/<k>` and `aws:RequestTag/<k>` look up `<k>` in the relevant tag set.
fn resolve_key(key: &str, ctx: &RequestContext, requester: &RequesterClass) -> KeyValue {
    if let Some(tag) = key.strip_prefix("s3:ExistingObjectTag/") {
        return lookup_tag(&ctx.existing_tags, tag);
    }
    if let Some(tag) = key.strip_prefix("aws:RequestTag/") {
        return lookup_tag(&ctx.request_tags, tag);
    }

    match key {
        "aws:SourceIp" => KeyValue::Single(ctx.source.to_string()),
        "aws:SecureTransport" => KeyValue::Single(bool_str(ctx.secure_transport)),
        "aws:Referer" => opt(ctx.referer.clone()),
        "aws:UserAgent" => opt(ctx.user_agent.clone()),
        "aws:CurrentTime" => KeyValue::Single(ctx.now.as_secs().to_string()),
        "aws:PrincipalType" => KeyValue::Single(principal_type(requester).to_owned()),
        "s3:prefix" => opt(ctx.prefix.clone()),
        "s3:delimiter" => opt(ctx.delimiter.clone()),
        "s3:max-keys" => match ctx.max_keys {
            Some(n) => KeyValue::Single(n.to_string()),
            None => KeyValue::Absent,
        },
        "s3:x-amz-acl" => opt(ctx.canned_acl.clone()),
        "s3:x-amz-content-sha256" => opt(ctx.content_sha256.clone()),
        "s3:VersionId" => opt(ctx.version_id.as_ref().map(|v| v.as_str().to_owned())),
        _ => KeyValue::Unknown,
    }
}

fn lookup_tag(tags: &[(String, String)], key: &str) -> KeyValue {
    match tags.iter().find(|(k, _)| k == key) {
        Some((_, v)) => KeyValue::Single(v.clone()),
        None => KeyValue::Absent,
    }
}

fn opt(v: Option<String>) -> KeyValue {
    match v {
        Some(s) => KeyValue::Single(s),
        None => KeyValue::Absent,
    }
}

fn bool_str(b: bool) -> String {
    if b {
        "true".to_owned()
    } else {
        "false".to_owned()
    }
}

fn principal_type(requester: &RequesterClass) -> &'static str {
    match requester {
        RequesterClass::Anonymous => "Anonymous",
        RequesterClass::AuthenticatedMember(_) | RequesterClass::OwnerOrAdmin => "User",
    }
}

/// Evaluate the `Null` existence operator: `key Null true` passes when the key is absent;
/// `key Null false` passes when the key is present. An unknown key is treated as absent.
fn eval_null(c: &Condition, ctx: &RequestContext, requester: &RequesterClass) -> bool {
    let present = matches!(resolve_key(&c.key, ctx, requester), KeyValue::Single(_));
    // Any of the expected values: "true" means "must be absent", "false" means "must exist".
    c.values.iter().any(|want| match want.as_str() {
        "true" => !present,
        "false" => present,
        _ => false,
    })
}

/// Apply a comparison operator across the (possibly multi-valued) actual values and the
/// expected `wanted` values. Semantics per operator family.
fn apply_operator(op: ConditionOperator, actual: &[String], wanted: &[String]) -> bool {
    match op {
        ConditionOperator::StringEquals => any_pair(actual, wanted, |a, w| a == w),
        ConditionOperator::StringNotEquals => {
            // Passes when NONE of the actual values equals ANY wanted value.
            !actual.iter().any(|a| wanted.iter().any(|w| a == w))
        }
        ConditionOperator::StringLike => any_pair(actual, wanted, |a, w| wildcard_match(w, a)),
        ConditionOperator::Bool => {
            // Normalise truthiness so "True"/"1" compare equal to "true".
            any_pair(actual, wanted, |a, w| norm_bool(a) == norm_bool(w))
        }
        ConditionOperator::IpAddress => {
            actual.iter().any(|a| wanted.iter().any(|w| ip_match(a, w)))
        }
        ConditionOperator::NotIpAddress => {
            // Passes when the source matches NONE of the listed ranges.
            !actual.iter().any(|a| wanted.iter().any(|w| ip_match(a, w)))
        }
        ConditionOperator::Numeric(cmp) => any_pair(actual, wanted, |a, w| numeric_cmp(a, w, cmp)),
        ConditionOperator::Date(cmp) => any_pair(actual, wanted, |a, w| date_cmp(a, w, cmp)),
        ConditionOperator::Null => false, // handled earlier
    }
}

/// True when some actual value paired with some wanted value satisfies `f`.
fn any_pair(actual: &[String], wanted: &[String], f: impl Fn(&str, &str) -> bool) -> bool {
    actual.iter().any(|a| wanted.iter().any(|w| f(a, w)))
}

fn norm_bool(s: &str) -> bool {
    matches!(s, "true" | "True" | "TRUE" | "1")
}

/// Match an IP `addr` against a `pattern` that is either a bare address or CIDR (`a.b.c.d/n`).
fn ip_match(addr: &str, pattern: &str) -> bool {
    let Ok(ip) = addr.parse::<IpAddr>() else {
        return false;
    };
    if let Some((net, bits)) = pattern.split_once('/') {
        let Ok(net_ip) = net.parse::<IpAddr>() else {
            return false;
        };
        let Ok(prefix) = bits.parse::<u8>() else {
            return false;
        };
        cidr_contains(net_ip, prefix, ip)
    } else {
        match pattern.parse::<IpAddr>() {
            Ok(p) => p == ip,
            Err(_) => false,
        }
    }
}

/// Whether `ip` falls within `net/prefix`. Mixed address families never match.
fn cidr_contains(net: IpAddr, prefix: u8, ip: IpAddr) -> bool {
    match (net, ip) {
        (IpAddr::V4(net), IpAddr::V4(ip)) => {
            if prefix > 32 {
                return false;
            }
            let mask: u32 = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - u32::from(prefix))
            };
            (u32::from(net) & mask) == (u32::from(ip) & mask)
        }
        (IpAddr::V6(net), IpAddr::V6(ip)) => {
            if prefix > 128 {
                return false;
            }
            let mask: u128 = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - u32::from(prefix))
            };
            (u128::from(net) & mask) == (u128::from(ip) & mask)
        }
        _ => false,
    }
}

fn numeric_cmp(a: &str, w: &str, cmp: NumericOp) -> bool {
    let (Ok(a), Ok(w)) = (a.parse::<f64>(), w.parse::<f64>()) else {
        return false;
    };
    compare(a, w, cmp)
}

/// Compare two date values. Accepts an RFC-3339-ish epoch already normalised to seconds in
/// the context (`aws:CurrentTime` resolves to epoch seconds), or a numeric epoch in the
/// policy. Falls back to lexical numeric parse, which is sufficient for the supported key.
fn date_cmp(a: &str, w: &str, cmp: NumericOp) -> bool {
    let a = parse_epoch_secs(a);
    let w = parse_epoch_secs(w);
    match (a, w) {
        (Some(a), Some(w)) => compare(a as f64, w as f64, cmp),
        _ => false,
    }
}

/// Parse either a bare epoch-seconds integer or an RFC-3339 UTC timestamp
/// (`YYYY-MM-DDTHH:MM:SSZ`) into epoch seconds.
fn parse_epoch_secs(s: &str) -> Option<i64> {
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    parse_rfc3339_utc(s)
}

/// A small, dependency-free RFC-3339 (UTC `Z`) parser yielding epoch seconds. Accepts the
/// canonical `2026-06-06T00:00:00Z` form; returns `None` for anything else.
fn parse_rfc3339_utc(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() != 20 || bytes[19] != b'Z' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| -> Option<i64> {
        s.get(range).and_then(|p| p.parse::<i64>().ok())
    };
    if &s[4..5] != "-"
        || &s[7..8] != "-"
        || &s[10..11] != "T"
        || &s[13..14] != ":"
        || &s[16..17] != ":"
    {
        return None;
    }
    let year = num(0..4)?;
    let month = num(5..7)?;
    let day = num(8..10)?;
    let hour = num(11..13)?;
    let min = num(14..16)?;
    let sec = num(17..19)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + min * 60 + sec)
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn compare(a: f64, b: f64, cmp: NumericOp) -> bool {
    match cmp {
        NumericOp::Equals => (a - b).abs() < f64::EPSILON || a == b,
        NumericOp::NotEquals => a != b,
        NumericOp::LessThan => a < b,
        NumericOp::LessThanEquals => a <= b,
        NumericOp::GreaterThan => a > b,
        NumericOp::GreaterThanEquals => a >= b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::{Timestamp, VersionId};
    use std::net::{IpAddr, Ipv4Addr};

    fn ctx() -> RequestContext {
        RequestContext {
            source: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
            secure_transport: true,
            referer: Some("https://example.com/".to_owned()),
            user_agent: Some("aws-cli/2.0".to_owned()),
            now: Timestamp::from_secs(1_700_000_000),
            prefix: Some("photos/".to_owned()),
            delimiter: Some("/".to_owned()),
            max_keys: Some(100),
            canned_acl: Some("private".to_owned()),
            content_sha256: Some("UNSIGNED-PAYLOAD".to_owned()),
            version_id: Some(VersionId::from_string("v1".to_owned())),
            existing_tags: vec![("env".to_owned(), "prod".to_owned())],
            request_tags: vec![("team".to_owned(), "eng".to_owned())],
        }
    }

    fn cond(op: ConditionOperator, key: &str, vals: &[&str]) -> Condition {
        Condition {
            operator: op,
            key: key.to_owned(),
            values: vals.iter().map(|s| (*s).to_owned()).collect(),
            if_exists: false,
        }
    }

    fn anon() -> RequesterClass {
        RequesterClass::Anonymous
    }

    #[test]
    fn string_equals_and_like() {
        assert!(condition_matches(
            &cond(ConditionOperator::StringEquals, "s3:prefix", &["photos/"]),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(ConditionOperator::StringEquals, "s3:prefix", &["docs/"]),
            &ctx(),
            &anon()
        ));
        assert!(condition_matches(
            &cond(
                ConditionOperator::StringLike,
                "aws:UserAgent",
                &["aws-cli/*"]
            ),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn string_not_equals() {
        assert!(condition_matches(
            &cond(ConditionOperator::StringNotEquals, "s3:prefix", &["docs/"]),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(
                ConditionOperator::StringNotEquals,
                "s3:prefix",
                &["photos/"]
            ),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn bool_secure_transport() {
        assert!(condition_matches(
            &cond(ConditionOperator::Bool, "aws:SecureTransport", &["true"]),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(ConditionOperator::Bool, "aws:SecureTransport", &["false"]),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn ip_address_cidr() {
        assert!(condition_matches(
            &cond(
                ConditionOperator::IpAddress,
                "aws:SourceIp",
                &["10.0.0.0/24"]
            ),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(
                ConditionOperator::IpAddress,
                "aws:SourceIp",
                &["10.1.0.0/24"]
            ),
            &ctx(),
            &anon()
        ));
        assert!(condition_matches(
            &cond(
                ConditionOperator::NotIpAddress,
                "aws:SourceIp",
                &["10.1.0.0/24"]
            ),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(
                ConditionOperator::NotIpAddress,
                "aws:SourceIp",
                &["10.0.0.0/24"]
            ),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn numeric_max_keys() {
        assert!(condition_matches(
            &cond(
                ConditionOperator::Numeric(NumericOp::LessThanEquals),
                "s3:max-keys",
                &["100"]
            ),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(
                ConditionOperator::Numeric(NumericOp::LessThan),
                "s3:max-keys",
                &["100"]
            ),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn date_compare_rfc3339() {
        // now = 1_700_000_000s. A date in the past => GreaterThan(now, past) true.
        assert!(condition_matches(
            &cond(
                ConditionOperator::Date(NumericOp::GreaterThan),
                "aws:CurrentTime",
                &["2000-01-01T00:00:00Z"]
            ),
            &ctx(),
            &anon()
        ));
        assert!(!condition_matches(
            &cond(
                ConditionOperator::Date(NumericOp::GreaterThan),
                "aws:CurrentTime",
                &["2100-01-01T00:00:00Z"]
            ),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn tag_keys() {
        assert!(condition_matches(
            &cond(
                ConditionOperator::StringEquals,
                "s3:ExistingObjectTag/env",
                &["prod"]
            ),
            &ctx(),
            &anon()
        ));
        assert!(condition_matches(
            &cond(
                ConditionOperator::StringEquals,
                "aws:RequestTag/team",
                &["eng"]
            ),
            &ctx(),
            &anon()
        ));
        // Absent tag => not present => no match (no IfExists).
        assert!(!condition_matches(
            &cond(
                ConditionOperator::StringEquals,
                "s3:ExistingObjectTag/missing",
                &["x"]
            ),
            &ctx(),
            &anon()
        ));
    }

    #[test]
    fn null_existence() {
        // prefix present => Null true must FAIL, Null false must PASS.
        assert!(!condition_matches(
            &cond(ConditionOperator::Null, "s3:prefix", &["true"]),
            &ctx(),
            &anon()
        ));
        assert!(condition_matches(
            &cond(ConditionOperator::Null, "s3:prefix", &["false"]),
            &ctx(),
            &anon()
        ));
        // version_id absent on a context without it => Null true passes.
        let mut c = ctx();
        c.version_id = None;
        assert!(condition_matches(
            &cond(ConditionOperator::Null, "s3:VersionId", &["true"]),
            &c,
            &anon()
        ));
    }

    #[test]
    fn unknown_key_never_matches() {
        assert!(!condition_matches(
            &cond(ConditionOperator::StringEquals, "aws:Bogus", &["x"]),
            &ctx(),
            &anon()
        ));
        // Even with IfExists, an unknown key does not match.
        let mut c = cond(ConditionOperator::StringEquals, "aws:Bogus", &["x"]);
        c.if_exists = true;
        assert!(!condition_matches(&c, &ctx(), &anon()));
    }

    #[test]
    fn if_exists_on_absent_recognised_key() {
        let mut base = ctx();
        base.referer = None;
        let mut c = cond(ConditionOperator::StringEquals, "aws:Referer", &["x"]);
        // Absent + no IfExists => fail.
        assert!(!condition_matches(&c, &base, &anon()));
        // Absent + IfExists => pass.
        c.if_exists = true;
        assert!(condition_matches(&c, &base, &anon()));
    }
}
