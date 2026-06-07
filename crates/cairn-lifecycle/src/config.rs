//! Lifecycle-configuration types and the S3 `<LifecycleConfiguration>` XML parser.
//!
//! A bucket's lifecycle configuration (ARCH §19.1) is a list of [`LifecycleRule`]s. Each rule
//! carries an identifier, an enabled/disabled status, an optional [`Filter`] selecting which
//! objects it applies to, and one or more [`Action`]s. The parser is a total function over an
//! arbitrary byte slice: any malformed input — invalid UTF-8, unbalanced tags, a number that
//! does not parse, an unrecognized status — folds to [`Error::MalformedXml`], and the parser
//! never panics.
//!
//! The parser drives quick-xml through a small SAX layer ([`drive`]) that tracks element depth
//! so a body which reaches EOF with an element still open is rejected, mirroring the codec in
//! `cairn-xml`.

use cairn_types::Error;
use quick_xml::Reader;
use quick_xml::events::Event;

/// A lifecycle rule: an identifier, a status, an optional filter, and its actions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LifecycleRule {
    /// The rule identifier (`<ID>`); empty if the configuration omitted it.
    pub id: String,
    /// Whether the rule is enabled (`<Status>Enabled</Status>`). A disabled rule is parsed and
    /// retained but the scanner skips it.
    pub enabled: bool,
    /// The object selector. An empty filter (the default) applies to the whole bucket.
    pub filter: Filter,
    /// The actions this rule applies to matching objects.
    pub actions: Vec<Action>,
}

/// The selector that scopes a rule to a subset of a bucket's objects (ARCH §19.1). An empty
/// filter (all fields `None`/empty) matches every object. Multiple constraints combine as a
/// conjunction, matching S3's `<And>` semantics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Filter {
    /// Restrict to keys beginning with this prefix.
    pub prefix: Option<String>,
    /// Restrict to objects carrying all of these tags (key/value equality).
    pub tags: Vec<(String, String)>,
    /// Restrict to objects strictly larger than this many bytes.
    pub object_size_greater_than: Option<u64>,
    /// Restrict to objects strictly smaller than this many bytes.
    pub object_size_less_than: Option<u64>,
}

impl Filter {
    /// Whether this filter constrains nothing (matches the whole bucket).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.prefix.is_none()
            && self.tags.is_empty()
            && self.object_size_greater_than.is_none()
            && self.object_size_less_than.is_none()
    }

    /// Whether an object with the given key, size, and tag set matches this filter.
    #[must_use]
    pub fn matches(&self, key: &str, size: u64, tags: &[(String, String)]) -> bool {
        if let Some(p) = &self.prefix {
            if !key.starts_with(p.as_str()) {
                return false;
            }
        }
        if let Some(gt) = self.object_size_greater_than {
            if size <= gt {
                return false;
            }
        }
        if let Some(lt) = self.object_size_less_than {
            if size >= lt {
                return false;
            }
        }
        for (k, v) in &self.tags {
            if !tags.iter().any(|(tk, tv)| tk == k && tv == v) {
                return false;
            }
        }
        true
    }
}

/// One lifecycle action (ARCH §19.1). A rule may carry several.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Expire the current object after it ages past a threshold.
    Expiration(Expiration),
    /// Expire noncurrent versions after they have been noncurrent for `days`, optionally
    /// preserving the newest `newer_noncurrent_versions` versions.
    NoncurrentVersionExpiration {
        /// Days a version must have been noncurrent before it is eligible for deletion.
        days: u32,
        /// The number of newest noncurrent versions to always retain, if specified.
        newer_noncurrent_versions: Option<u32>,
    },
    /// Abort incomplete multipart uploads `days_after_initiation` days after they began.
    AbortIncompleteMultipartUpload {
        /// Days after initiation before an incomplete upload is aborted.
        days_after_initiation: u32,
    },
    /// Remove a delete marker once it is the only remaining version of its key.
    ExpiredObjectDeleteMarker,
    /// Transition the object to a remote cold tier. v1 is a documented NO-OP placeholder
    /// (ARCH §19.5); the scanner counts it but performs no movement.
    Transition(Transition),
}

/// The threshold for current-object expiration: a number of days from creation, or a specific
/// calendar date (expressed as whole seconds since the Unix epoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    /// Expire `days` days after the object was created.
    Days(u32),
    /// Expire on or after this absolute time (seconds since the Unix epoch).
    Date(i64),
}

/// The threshold for a cold-tier transition (parsed but not acted on in v1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Transition `days` days after creation.
    Days(u32),
    /// Transition on or after this absolute time (seconds since the Unix epoch).
    Date(i64),
}

/// Parse an S3 `<LifecycleConfiguration>` body into a list of [`LifecycleRule`]s.
///
/// An empty configuration (`<LifecycleConfiguration/>`) parses to an empty list. A `<Status>`
/// of `Enabled` or `Disabled` sets [`LifecycleRule::enabled`]; any other status value is
/// malformed. Numeric fields (`Days`, `ObjectSizeGreaterThan`, etc.) must parse to their type.
/// `<Date>` is accepted as either an RFC 3339 / ISO-8601 instant or whole epoch seconds.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed XML, a numeric field does not
/// parse, a `<Status>` is unrecognized, or a `<Date>` cannot be interpreted.
pub fn parse_lifecycle(body: &[u8]) -> Result<Vec<LifecycleRule>, Error> {
    let mut rules: Vec<LifecycleRule> = Vec::new();

    // Per-rule accumulator state.
    let mut in_rule = false;
    let mut rule = LifecycleRule::default();

    // A stack of currently-open element local names, so a parser can disambiguate where a
    // numeric/text leaf belongs (e.g. `Days` under `Expiration` vs `NoncurrentVersionExpiration`).
    let mut stack: Vec<Vec<u8>> = Vec::new();

    // Scratch fields for the rule currently being assembled.
    let mut expiration_days: Option<u32> = None;
    let mut expiration_date: Option<i64> = None;
    let mut transition_days: Option<u32> = None;
    let mut transition_date: Option<i64> = None;
    let mut noncurrent_days: Option<u32> = None;
    let mut noncurrent_keep: Option<u32> = None;
    let mut abort_days: Option<u32> = None;
    let mut has_expired_marker = false;

    // Scratch for the pending tag being read inside a `<Tag>`.
    let mut tag_key: Option<String> = None;
    let mut tag_value: Option<String> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                match name.as_slice() {
                    b"Rule" => {
                        in_rule = true;
                        rule = LifecycleRule::default();
                        expiration_days = None;
                        expiration_date = None;
                        transition_days = None;
                        transition_date = None;
                        noncurrent_days = None;
                        noncurrent_keep = None;
                        abort_days = None;
                        has_expired_marker = false;
                    }
                    b"Tag" => {
                        tag_key = None;
                        tag_value = None;
                    }
                    _ => {}
                }
                stack.push(name);
            }
            Sax::Text(text) => {
                if !in_rule {
                    return Ok(());
                }
                let Some(field) = stack.last() else {
                    return Ok(());
                };
                let parent = stack.get(stack.len().wrapping_sub(2)).map(Vec::as_slice);
                match field.as_slice() {
                    b"ID" => rule.id = text.trim().to_owned(),
                    b"Status" => {
                        rule.enabled = match text.trim() {
                            "Enabled" => true,
                            "Disabled" => false,
                            _ => return Err(malformed()),
                        };
                    }
                    b"Prefix" => {
                        // `<Prefix>` may appear directly under `<Rule>` or nested in `<Filter>`
                        // / `<And>`; treat all positions as the filter prefix.
                        rule.filter.prefix = Some(text.into_owned());
                    }
                    b"Key" if parent == Some(b"Tag") => tag_key = Some(text.into_owned()),
                    b"Value" if parent == Some(b"Tag") => tag_value = Some(text.into_owned()),
                    b"ObjectSizeGreaterThan" => {
                        rule.filter.object_size_greater_than =
                            Some(text.trim().parse::<u64>().map_err(|_| malformed())?);
                    }
                    b"ObjectSizeLessThan" => {
                        rule.filter.object_size_less_than =
                            Some(text.trim().parse::<u64>().map_err(|_| malformed())?);
                    }
                    b"Days" => {
                        let n = text.trim().parse::<u32>().map_err(|_| malformed())?;
                        match parent {
                            Some(b"Expiration") => expiration_days = Some(n),
                            Some(b"Transition") => transition_days = Some(n),
                            Some(b"NoncurrentVersionExpiration") => noncurrent_days = Some(n),
                            _ => {}
                        }
                    }
                    b"NoncurrentDays" => {
                        // S3 spells the noncurrent threshold `<NoncurrentDays>`.
                        noncurrent_days =
                            Some(text.trim().parse::<u32>().map_err(|_| malformed())?);
                    }
                    b"NewerNoncurrentVersions" => {
                        noncurrent_keep =
                            Some(text.trim().parse::<u32>().map_err(|_| malformed())?);
                    }
                    b"DaysAfterInitiation" => {
                        abort_days = Some(text.trim().parse::<u32>().map_err(|_| malformed())?);
                    }
                    b"Date" => {
                        let d = parse_date(text.trim()).ok_or_else(malformed)?;
                        match parent {
                            Some(b"Transition") => transition_date = Some(d),
                            // `<Date>` directly under `<Expiration>` (or as a bare leaf).
                            _ => expiration_date = Some(d),
                        }
                    }
                    b"ExpiredObjectDeleteMarker" => {
                        has_expired_marker = text.trim().eq_ignore_ascii_case("true");
                    }
                    _ => {}
                }
            }
            Sax::Close(name) => {
                let popped = stack.pop();
                debug_assert!(popped.as_deref() == Some(name.as_slice()) || popped.is_none());
                if !in_rule {
                    return Ok(());
                }
                match name.as_slice() {
                    b"Tag" => {
                        if let (Some(k), Some(v)) = (tag_key.take(), tag_value.take()) {
                            rule.filter.tags.push((k, v));
                        } else {
                            return Err(malformed());
                        }
                    }
                    b"Rule" => {
                        // Materialize the accumulated actions in a deterministic order.
                        if let Some(days) = expiration_days {
                            rule.actions
                                .push(Action::Expiration(Expiration::Days(days)));
                        } else if let Some(date) = expiration_date {
                            rule.actions
                                .push(Action::Expiration(Expiration::Date(date)));
                        }
                        if let Some(days) = noncurrent_days {
                            rule.actions.push(Action::NoncurrentVersionExpiration {
                                days,
                                newer_noncurrent_versions: noncurrent_keep,
                            });
                        }
                        if let Some(days) = abort_days {
                            rule.actions.push(Action::AbortIncompleteMultipartUpload {
                                days_after_initiation: days,
                            });
                        }
                        if has_expired_marker {
                            rule.actions.push(Action::ExpiredObjectDeleteMarker);
                        }
                        if let Some(days) = transition_days {
                            rule.actions
                                .push(Action::Transition(Transition::Days(days)));
                        } else if let Some(date) = transition_date {
                            rule.actions
                                .push(Action::Transition(Transition::Date(date)));
                        }
                        rules.push(std::mem::take(&mut rule));
                        in_rule = false;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    })?;

    Ok(rules)
}

/// Parse a `<Date>` value as either RFC-3339 / ISO-8601 (`2026-01-01T00:00:00Z`) or whole
/// seconds since the Unix epoch. Returns `None` if neither interpretation succeeds.
fn parse_date(s: &str) -> Option<i64> {
    if let Ok(secs) = s.parse::<i64>() {
        return Some(secs);
    }
    parse_rfc3339_secs(s)
}

/// A minimal RFC-3339 parser yielding whole epoch seconds. Accepts `YYYY-MM-DDThh:mm:ss` with
/// an optional fractional part and an optional `Z` or `±hh:mm` offset. Returns `None` on any
/// structural problem; the lifecycle scanner only needs day-granularity comparisons.
fn parse_rfc3339_secs(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    if bytes.get(4) != Some(&b'-') {
        return None;
    }
    let month: i64 = s.get(5..7)?.parse().ok()?;
    if bytes.get(7) != Some(&b'-') {
        return None;
    }
    let day: i64 = s.get(8..10)?.parse().ok()?;
    let sep = bytes.get(10)?;
    if *sep != b'T' && *sep != b't' && *sep != b' ' {
        return None;
    }
    let hour: i64 = s.get(11..13)?.parse().ok()?;
    if bytes.get(13) != Some(&b':') {
        return None;
    }
    let minute: i64 = s.get(14..16)?.parse().ok()?;
    if bytes.get(16) != Some(&b':') {
        return None;
    }
    let second: i64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    // Parse an optional trailing timezone offset after any fractional seconds.
    let mut offset_secs: i64 = 0;
    let rest = &s[19..];
    let tz = rest.trim_start_matches(|c: char| c == '.' || c.is_ascii_digit());
    match tz.as_bytes().first() {
        None => {}
        Some(b'Z') | Some(b'z') => {}
        Some(&sign @ b'+') | Some(&sign @ b'-') => {
            if tz.len() < 6 {
                return None;
            }
            let oh: i64 = tz.get(1..3)?.parse().ok()?;
            let om: i64 = tz.get(4..6)?.parse().ok()?;
            let mag = oh * 3600 + om * 60;
            offset_secs = if sign == b'+' { mag } else { -mag };
        }
        _ => return None,
    }

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3600 + minute * 60 + second - offset_secs;
    Some(secs)
}

/// Days from the Unix epoch (1970-01-01) to the given civil date, by Howard Hinnant's
/// `days_from_civil` algorithm. Valid for the proleptic Gregorian calendar.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// ===========================================================================================
// SAX driver (mirrors the well-formedness/balance discipline of cairn-xml::parse)
// ===========================================================================================

/// Map any quick-xml failure into the canonical malformed-XML error.
fn malformed() -> Error {
    Error::MalformedXml
}

/// One decoded SAX event handed to the parser callback.
enum Sax<'a> {
    /// An opening tag with its local name (namespace prefix stripped).
    Open(Vec<u8>),
    /// Decoded, entity-unescaped text content.
    Text(std::borrow::Cow<'a, str>),
    /// A closing tag with its local name.
    Close(Vec<u8>),
}

/// Drive an XML body through a callback, validating well-formedness and element balance. A
/// self-closing tag surfaces as an `Open` immediately followed by a `Close`. A body that ends
/// with any element still open is rejected as malformed.
fn drive<F>(body: &[u8], mut on_event: F) -> Result<(), Error>
where
    F: FnMut(Sax<'_>) -> Result<(), Error>,
{
    let s = std::str::from_utf8(body).map_err(|_| malformed())?;
    let mut reader = Reader::from_str(s);
    let cfg = reader.config_mut();
    cfg.trim_text(true);

    let mut depth: u32 = 0;
    loop {
        match reader.read_event().map_err(|_| malformed())? {
            Event::Start(e) => {
                depth += 1;
                on_event(Sax::Open(local(e.name())))?;
            }
            Event::Empty(e) => {
                let name = local(e.name());
                on_event(Sax::Open(name.clone()))?;
                on_event(Sax::Close(name))?;
            }
            Event::Text(t) => {
                let text = t.unescape().map_err(|_| malformed())?;
                on_event(Sax::Text(text))?;
            }
            Event::CData(t) => {
                let text = t.decode().map_err(|_| malformed())?;
                on_event(Sax::Text(text))?;
            }
            Event::End(e) => {
                depth = depth.checked_sub(1).ok_or_else(malformed)?;
                on_event(Sax::Close(local(e.name())))?;
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if depth != 0 {
        return Err(malformed());
    }
    Ok(())
}

/// The local element name of an event, as owned bytes (namespace prefix stripped).
fn local(name: quick_xml::name::QName<'_>) -> Vec<u8> {
    quick_xml::name::LocalName::from(name).as_ref().to_vec()
}
