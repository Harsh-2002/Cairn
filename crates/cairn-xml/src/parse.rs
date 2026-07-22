//! S3 request-body parsers. Each takes a raw `&[u8]` body and returns a typed result,
//! folding any malformed input — invalid UTF-8, unbalanced tags, missing required fields,
//! out-of-range numbers — to [`Error::MalformedXml`]. Parsers never panic.
//!
//! quick-xml's reader rejects *mismatched* end tags (`check_end_names`) but treats a body
//! that simply ends with elements still open as a clean EOF. To make every parser total we
//! drive the reader through [`Sax`], which tracks element depth and rejects a body that
//! reaches EOF with any element still open.

use cairn_types::{
    Acl, DefaultRetention, Error, Grant, Grantee, ObjectLockConfiguration, ObjectLockMode,
    ObjectRetention, Permission, RetentionPeriod, UserId, VersioningState,
};
use quick_xml::Reader;
use quick_xml::events::Event;

/// Map any quick-xml error into the canonical malformed-XML error.
fn malformed() -> Error {
    Error::MalformedXml
}

/// One decoded SAX event handed to a parser callback.
enum Sax<'a> {
    /// An opening tag with its local name (namespace prefix stripped).
    Open(Vec<u8>),
    /// Decoded, entity-unescaped text content.
    Text(std::borrow::Cow<'a, str>),
    /// A closing tag with its local name.
    Close(Vec<u8>),
}

/// Drive an XML body through a callback, validating well-formedness and element balance.
///
/// The callback sees [`Sax::Open`]/[`Sax::Text`]/[`Sax::Close`] events in document order and
/// may return `Err(MalformedXml)` to reject a semantically invalid body. Self-closing tags
/// surface as an `Open` immediately followed by a `Close`. A body that ends with any element
/// still open is rejected as malformed.
fn drive<F>(body: &[u8], mut on_event: F) -> Result<(), Error>
where
    F: FnMut(Sax<'_>) -> Result<(), Error>,
{
    // Validate UTF-8 up front so the borrowed reader is sound and bad encodings are rejected
    // as malformed rather than surfacing mid-parse.
    let s = std::str::from_utf8(body).map_err(|_| malformed())?;
    let mut reader = Reader::from_str(s);
    let cfg = reader.config_mut();
    cfg.trim_text(true);

    let mut depth: u32 = 0;
    // Coalesce consecutive Text/CData into a single buffer, flushed as ONE `Sax::Text` on the next
    // structural event. quick-xml splits an element's character data into multiple events around
    // CDATA boundaries and the like, so emitting each chunk separately made handlers that store the
    // "current" text keep only the LAST chunk — corrupting keys/ETags and splitting CORS origins
    // across chunks (audit #24). Buffering yields exactly one Text event per contiguous text run,
    // preserving the prior "emit a Text whenever character data was seen" semantics.
    let mut text_buf: Option<String> = None;
    macro_rules! flush_text {
        () => {
            if let Some(s) = text_buf.take() {
                on_event(Sax::Text(std::borrow::Cow::Owned(s)))?;
            }
        };
    }
    loop {
        match reader.read_event().map_err(|_| malformed())? {
            Event::Start(e) => {
                flush_text!();
                depth += 1;
                on_event(Sax::Open(local(e.name())))?;
            }
            Event::Empty(e) => {
                // A self-closing tag is an open immediately followed by a close.
                flush_text!();
                let name = local(e.name());
                on_event(Sax::Open(name.clone()))?;
                on_event(Sax::Close(name))?;
            }
            Event::Text(t) => {
                let text = t.unescape().map_err(|_| malformed())?;
                text_buf.get_or_insert_with(String::new).push_str(&text);
            }
            Event::CData(t) => {
                let text = t.decode().map_err(|_| malformed())?;
                text_buf.get_or_insert_with(String::new).push_str(&text);
            }
            Event::End(e) => {
                // quick-xml's check_end_names guarantees this matches the open, but guard
                // against an underflow regardless.
                flush_text!();
                depth = depth.checked_sub(1).ok_or_else(malformed)?;
                on_event(Sax::Close(local(e.name())))?;
            }
            Event::Eof => {
                flush_text!();
                break;
            }
            _ => {}
        }
    }
    if depth != 0 {
        // The body ended with elements still open.
        return Err(malformed());
    }
    Ok(())
}

/// The local element name of an event, as owned bytes (namespace prefix stripped).
fn local(name: quick_xml::name::QName<'_>) -> Vec<u8> {
    quick_xml::name::LocalName::from(name).as_ref().to_vec()
}

// ===========================================================================================
// CompleteMultipartUpload
// ===========================================================================================

/// Parse a `CompleteMultipartUpload` body into `(part_number, etag)` pairs, in document
/// order. ETags are returned with any surrounding quotes stripped (S3 clients quote them).
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed, a `PartNumber` is missing
/// or not a valid `u16`, or an `ETag` is missing.
pub fn parse_complete_multipart(body: &[u8]) -> Result<Vec<(u16, String)>, Error> {
    let mut out = Vec::new();
    let mut in_part = false;
    let mut cur_field: Option<Vec<u8>> = None;
    let mut part_number: Option<u16> = None;
    let mut etag: Option<String> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name == b"Part" {
                    in_part = true;
                    part_number = None;
                    etag = None;
                    cur_field = None;
                } else if in_part {
                    cur_field = Some(name);
                }
            }
            Sax::Text(text) => {
                if in_part {
                    if let Some(field) = &cur_field {
                        if field == b"PartNumber" {
                            part_number =
                                Some(text.trim().parse::<u16>().map_err(|_| malformed())?);
                        } else if field == b"ETag" {
                            etag = Some(unquote(text.trim()));
                        }
                    }
                }
            }
            Sax::Close(name) => {
                if name == b"Part" {
                    let pn = part_number.take().ok_or_else(malformed)?;
                    let et = etag.take().ok_or_else(malformed)?;
                    out.push((pn, et));
                    in_part = false;
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;
    Ok(out)
}

// ===========================================================================================
// S3 RESPONSE parsers (used by the import engine to read a remote store's listings, ARCH 27)
// ===========================================================================================

/// Parse a `ListBucketResult` (`GET /{bucket}?list-type=2`) response into
/// `(objects, next_continuation_token, is_truncated)`, where each object is `(key, size, etag?)` in
/// document order. Nested `<Owner>`/`<StorageClass>` and other fields are ignored.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed or a `<Size>` is not a valid `u64`.
#[allow(clippy::type_complexity)]
pub fn parse_list_objects_v2(
    body: &[u8],
) -> Result<(Vec<(String, u64, Option<String>)>, Option<String>, bool), Error> {
    let mut objects = Vec::new();
    let mut is_truncated = false;
    let mut next_token: Option<String> = None;
    let mut in_contents = false;
    let mut cur_field: Option<Vec<u8>> = None;
    let mut key: Option<String> = None;
    let mut size: u64 = 0;
    let mut etag: Option<String> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name == b"Contents" {
                    in_contents = true;
                    key = None;
                    size = 0;
                    etag = None;
                    cur_field = None;
                } else {
                    cur_field = Some(name);
                }
            }
            Sax::Text(text) => {
                if let Some(field) = &cur_field {
                    if in_contents {
                        if field == b"Key" {
                            key = Some(text.into_owned());
                        } else if field == b"Size" {
                            size = text.trim().parse::<u64>().map_err(|_| malformed())?;
                        } else if field == b"ETag" {
                            etag = Some(unquote(text.trim()));
                        }
                    } else if field == b"IsTruncated" {
                        is_truncated = text.trim().eq_ignore_ascii_case("true");
                    } else if field == b"NextContinuationToken" {
                        next_token = Some(text.into_owned());
                    }
                }
            }
            Sax::Close(name) => {
                if name == b"Contents" {
                    if let Some(k) = key.take() {
                        objects.push((k, size, etag.take()));
                    }
                    in_contents = false;
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;
    Ok((objects, next_token, is_truncated))
}

/// Parse a `ListAllMyBucketsResult` (`GET /`) response into the list of bucket names, in document
/// order.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed.
pub fn parse_list_all_my_buckets(body: &[u8]) -> Result<Vec<String>, Error> {
    let mut names = Vec::new();
    let mut in_bucket = false;
    let mut cur_field: Option<Vec<u8>> = None;
    let mut name: Option<String> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(n) => {
                if n == b"Bucket" {
                    in_bucket = true;
                    name = None;
                    cur_field = None;
                } else if in_bucket {
                    cur_field = Some(n);
                }
            }
            Sax::Text(text) => {
                if in_bucket {
                    if let Some(f) = &cur_field {
                        if f == b"Name" {
                            name = Some(text.into_owned());
                        }
                    }
                }
            }
            Sax::Close(n) => {
                if n == b"Bucket" {
                    if let Some(nm) = name.take() {
                        names.push(nm);
                    }
                    in_bucket = false;
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;
    Ok(names)
}

// ===========================================================================================
// Delete (multi-object delete request)
// ===========================================================================================

/// Parse a `Delete` body into `(quiet, objects)` where each object is `(key, version_id?)`.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed or an `<Object>` lacks a
/// `<Key>`.
// The `(quiet, [(key, version_id?)])` shape is the operation's wire result and is part of the
// crate's public contract, so it is kept inline rather than aliased.
#[allow(clippy::type_complexity)]
pub fn parse_delete(body: &[u8]) -> Result<(bool, Vec<(String, Option<String>)>), Error> {
    let mut quiet = false;
    let mut objects = Vec::new();
    let mut in_object = false;
    let mut cur_field: Option<Vec<u8>> = None;
    let mut key: Option<String> = None;
    let mut version_id: Option<String> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name == b"Object" {
                    in_object = true;
                    key = None;
                    version_id = None;
                    cur_field = None;
                } else {
                    cur_field = Some(name);
                }
            }
            Sax::Text(text) => {
                if let Some(field) = &cur_field {
                    if field == b"Quiet" && !in_object {
                        quiet = text.trim().eq_ignore_ascii_case("true");
                    } else if in_object && field == b"Key" {
                        key = Some(text.into_owned());
                    } else if in_object && field == b"VersionId" {
                        version_id = Some(text.into_owned());
                    }
                }
            }
            Sax::Close(name) => {
                if name == b"Object" {
                    let k = key.take().ok_or_else(malformed)?;
                    objects.push((k, version_id.take()));
                    in_object = false;
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;
    Ok((quiet, objects))
}

// ===========================================================================================
// Tagging
// ===========================================================================================

/// Parse a `Tagging` body into `(key, value)` tag pairs.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed or a `<Tag>` lacks a
/// `<Key>` or `<Value>`.
pub fn parse_tagging(body: &[u8]) -> Result<Vec<(String, String)>, Error> {
    let mut out = Vec::new();
    let mut in_tag = false;
    let mut cur_field: Option<Vec<u8>> = None;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name == b"Tag" {
                    in_tag = true;
                    key = None;
                    value = None;
                    cur_field = None;
                } else if in_tag {
                    cur_field = Some(name);
                }
            }
            Sax::Text(text) => {
                if in_tag {
                    if let Some(field) = &cur_field {
                        if field == b"Key" {
                            key = Some(text.into_owned());
                        } else if field == b"Value" {
                            value = Some(text.into_owned());
                        }
                    }
                }
            }
            Sax::Close(name) => {
                if name == b"Tag" {
                    let k = key.take().ok_or_else(malformed)?;
                    let v = value.take().ok_or_else(malformed)?;
                    out.push((k, v));
                    in_tag = false;
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;
    Ok(out)
}

/// Maximum tags per object (the S3 limit).
pub const MAX_TAGS_OBJECT: usize = 10;
/// Maximum tags per bucket (the S3 limit).
pub const MAX_TAGS_BUCKET: usize = 50;
const MAX_TAG_KEY_LEN: usize = 128;
const MAX_TAG_VALUE_LEN: usize = 256;

/// Whether a tag key/value character is in the S3-permitted set: Unicode letters and digits,
/// whitespace, and the punctuation `+ - = . _ : / @`.
fn tag_char_ok(c: char) -> bool {
    c.is_alphanumeric()
        || c.is_whitespace()
        || matches!(c, '+' | '-' | '=' | '.' | '_' | ':' | '/' | '@')
}

/// Enforce the S3 tag-set quantitative limits (ARCH 17.1) on an already-parsed tag list: at most
/// `max_tags` entries; each key 1..=128 and value 0..=256 Unicode scalars; only permitted
/// characters; unique keys; and no key carrying the reserved `aws:` prefix. `max_tags` is
/// [`MAX_TAGS_OBJECT`] or [`MAX_TAGS_BUCKET`].
///
/// # Errors
/// Returns [`Error::InvalidTag`] describing the first violation (distinct from `MalformedXml`,
/// matching S3, which returns `InvalidTag` for a well-formed body that breaks a limit).
pub fn validate_tags(tags: &[(String, String)], max_tags: usize) -> Result<(), Error> {
    use std::collections::HashSet;
    if tags.len() > max_tags {
        return Err(Error::InvalidTag(format!(
            "too many tags ({}, max {max_tags})",
            tags.len()
        )));
    }
    let mut seen: HashSet<&str> = HashSet::with_capacity(tags.len());
    for (k, v) in tags {
        if k.is_empty() {
            return Err(Error::InvalidTag("a tag key must not be empty".to_owned()));
        }
        if k.chars().count() > MAX_TAG_KEY_LEN {
            return Err(Error::InvalidTag(format!(
                "tag key longer than {MAX_TAG_KEY_LEN} characters"
            )));
        }
        if v.chars().count() > MAX_TAG_VALUE_LEN {
            return Err(Error::InvalidTag(format!(
                "tag value longer than {MAX_TAG_VALUE_LEN} characters"
            )));
        }
        if k.to_ascii_lowercase().starts_with("aws:") {
            return Err(Error::InvalidTag(
                "tag keys may not begin with the reserved prefix \"aws:\"".to_owned(),
            ));
        }
        if !k.chars().all(tag_char_ok) || !v.chars().all(tag_char_ok) {
            return Err(Error::InvalidTag(
                "a tag key or value contains a disallowed character".to_owned(),
            ));
        }
        if !seen.insert(k.as_str()) {
            return Err(Error::InvalidTag(format!("duplicate tag key {k:?}")));
        }
    }
    Ok(())
}

// ===========================================================================================
// VersioningConfiguration
// ===========================================================================================

/// Parse a `VersioningConfiguration` body into a [`VersioningState`]. An absent or empty
/// `<Status>` maps to [`VersioningState::Unversioned`]; `Enabled`/`Suspended` map directly.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed or `<Status>` carries an
/// unrecognized value.
pub fn parse_versioning_configuration(body: &[u8]) -> Result<VersioningState, Error> {
    let mut state = VersioningState::Unversioned;
    let mut in_status = false;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name == b"Status" {
                    in_status = true;
                }
            }
            Sax::Text(text) => {
                if in_status {
                    state = match text.trim() {
                        "Enabled" => VersioningState::Enabled,
                        "Suspended" => VersioningState::Suspended,
                        "" => VersioningState::Unversioned,
                        _ => return Err(malformed()),
                    };
                }
            }
            Sax::Close(name) => {
                if name == b"Status" {
                    in_status = false;
                }
            }
        }
        Ok(())
    })?;
    Ok(state)
}

// ===========================================================================================
// Object Lock: Retention, LegalHold, ObjectLockConfiguration
// ===========================================================================================

/// Parse an Object Lock mode token (`GOVERNANCE` / `COMPLIANCE`), as used both in the XML
/// `<Mode>` element and the `x-amz-object-lock-mode` header.
///
/// # Errors
/// [`Error::MalformedXml`] for any other value.
pub fn parse_lock_mode(s: &str) -> Result<ObjectLockMode, Error> {
    match s {
        "GOVERNANCE" => Ok(ObjectLockMode::Governance),
        "COMPLIANCE" => Ok(ObjectLockMode::Compliance),
        _ => Err(malformed()),
    }
}

/// Parse a `Retention` body (`<Retention><Mode>…</Mode><RetainUntilDate>…</RetainUntilDate></Retention>`).
///
/// # Errors
/// [`Error::MalformedXml`] if the body is not well-formed or a required field is missing/invalid.
pub fn parse_retention(body: &[u8]) -> Result<ObjectRetention, Error> {
    let mut mode: Option<ObjectLockMode> = None;
    let mut retain_until = None;
    let mut cur: Option<&'static str> = None;
    let mut buf = String::new();
    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                cur = if name == b"Mode" {
                    Some("mode")
                } else if name == b"RetainUntilDate" {
                    Some("date")
                } else {
                    None
                };
                buf.clear();
            }
            Sax::Text(t) => {
                if cur.is_some() {
                    buf.push_str(&t);
                }
            }
            Sax::Close(name) => {
                if name == b"Mode" {
                    mode = Some(parse_lock_mode(buf.trim())?);
                } else if name == b"RetainUntilDate" {
                    retain_until =
                        Some(crate::timefmt::parse_iso8601(buf.trim()).ok_or_else(malformed)?);
                }
                cur = None;
            }
        }
        Ok(())
    })?;
    match (mode, retain_until) {
        (Some(mode), Some(retain_until)) => Ok(ObjectRetention { mode, retain_until }),
        _ => Err(malformed()),
    }
}

/// Parse a `LegalHold` body (`<LegalHold><Status>ON|OFF</Status></LegalHold>`) into the on/off flag.
///
/// # Errors
/// [`Error::MalformedXml`] if the body is not well-formed or the status is not `ON`/`OFF`.
pub fn parse_legal_hold(body: &[u8]) -> Result<bool, Error> {
    let mut status: Option<bool> = None;
    let mut in_status = false;
    let mut buf = String::new();
    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                in_status = name == b"Status";
                if in_status {
                    buf.clear();
                }
            }
            Sax::Text(t) => {
                if in_status {
                    buf.push_str(&t);
                }
            }
            Sax::Close(name) => {
                if name == b"Status" {
                    status = Some(match buf.trim() {
                        "ON" => true,
                        "OFF" => false,
                        _ => return Err(malformed()),
                    });
                    in_status = false;
                }
            }
        }
        Ok(())
    })?;
    status.ok_or_else(malformed)
}

/// Parse an `ObjectLockConfiguration` body (the bucket-level config) into the typed configuration.
///
/// # Errors
/// [`Error::MalformedXml`] if the body is not well-formed or the default retention is partial.
pub fn parse_object_lock_configuration(body: &[u8]) -> Result<ObjectLockConfiguration, Error> {
    let mut enabled = false;
    let mut mode: Option<ObjectLockMode> = None;
    let mut days: Option<u32> = None;
    let mut years: Option<u32> = None;
    let mut cur: Option<&'static str> = None;
    let mut buf = String::new();
    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                cur = if name == b"ObjectLockEnabled" {
                    Some("enabled")
                } else if name == b"Mode" {
                    Some("mode")
                } else if name == b"Days" {
                    Some("days")
                } else if name == b"Years" {
                    Some("years")
                } else {
                    None
                };
                buf.clear();
            }
            Sax::Text(t) => {
                if cur.is_some() {
                    buf.push_str(&t);
                }
            }
            Sax::Close(name) => {
                if name == b"ObjectLockEnabled" {
                    enabled = buf.trim() == "Enabled";
                } else if name == b"Mode" {
                    mode = Some(parse_lock_mode(buf.trim())?);
                } else if name == b"Days" {
                    days = Some(buf.trim().parse().map_err(|_| malformed())?);
                } else if name == b"Years" {
                    years = Some(buf.trim().parse().map_err(|_| malformed())?);
                }
                cur = None;
            }
        }
        Ok(())
    })?;
    let default_retention = match (mode, days, years) {
        (Some(mode), Some(d), None) => Some(DefaultRetention {
            mode,
            period: RetentionPeriod::Days(d),
        }),
        (Some(mode), None, Some(y)) => Some(DefaultRetention {
            mode,
            period: RetentionPeriod::Years(y),
        }),
        (None, None, None) => None,
        _ => return Err(malformed()),
    };
    Ok(ObjectLockConfiguration {
        enabled,
        default_retention,
    })
}

// ===========================================================================================
// CORSConfiguration
// ===========================================================================================

/// One parsed CORS rule. A simplified projection of the S3 `CORSRule` element sufficient for
/// Cairn's preflight/response handling.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CorsRule {
    /// `AllowedOrigin` values.
    pub allowed_origins: Vec<String>,
    /// `AllowedMethod` values.
    pub allowed_methods: Vec<String>,
    /// `AllowedHeader` values.
    pub allowed_headers: Vec<String>,
    /// `ExposeHeader` values.
    pub expose_headers: Vec<String>,
    /// `MaxAgeSeconds`, if present.
    pub max_age_seconds: Option<u32>,
}

/// Parse a `CORSConfiguration` body into a list of [`CorsRule`]s.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed or a `MaxAgeSeconds` is not
/// a valid `u32`.
pub fn parse_cors_configuration(body: &[u8]) -> Result<Vec<CorsRule>, Error> {
    let mut rules = Vec::new();
    let mut in_rule = false;
    let mut cur_field: Option<Vec<u8>> = None;
    let mut rule = CorsRule::default();

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                if name == b"CORSRule" {
                    in_rule = true;
                    rule = CorsRule::default();
                    cur_field = None;
                } else if in_rule {
                    cur_field = Some(name);
                }
            }
            Sax::Text(text) => {
                if in_rule {
                    if let Some(field) = &cur_field {
                        match field.as_slice() {
                            b"AllowedOrigin" => rule.allowed_origins.push(text.into_owned()),
                            b"AllowedMethod" => rule.allowed_methods.push(text.into_owned()),
                            b"AllowedHeader" => rule.allowed_headers.push(text.into_owned()),
                            b"ExposeHeader" => rule.expose_headers.push(text.into_owned()),
                            b"MaxAgeSeconds" => {
                                rule.max_age_seconds =
                                    Some(text.trim().parse::<u32>().map_err(|_| malformed())?);
                            }
                            _ => {}
                        }
                    }
                }
            }
            Sax::Close(name) => {
                if name == b"CORSRule" {
                    rules.push(std::mem::take(&mut rule));
                    in_rule = false;
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;
    Ok(rules)
}

// ===========================================================================================
// ServerSideEncryptionConfiguration (PutBucketEncryption / GetBucketEncryption)
// ===========================================================================================

/// The default server-side-encryption rule of a `ServerSideEncryptionConfiguration` — a projection
/// of the single `Rule > ApplyServerSideEncryptionByDefault` element S3 supports, plus the
/// rule-level `BucketKeyEnabled` flag.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerSideEncryptionRule {
    /// `SSEAlgorithm`: `AES256` (SSE-S3) or `aws:kms` (SSE-KMS).
    pub sse_algorithm: String,
    /// `KMSMasterKeyID`, when present (only meaningful with `aws:kms`).
    pub kms_master_key_id: Option<String>,
    /// `BucketKeyEnabled` (accepted and echoed so a round-trip GET does not drop it).
    pub bucket_key_enabled: bool,
}

/// Parse a `ServerSideEncryptionConfiguration` body into its default-encryption rule.
///
/// The document carries a single `<Rule>` with an `<ApplyServerSideEncryptionByDefault>` naming the
/// `<SSEAlgorithm>` (`AES256` or `aws:kms`) and an optional `<KMSMasterKeyID>`, plus an optional
/// rule-level `<BucketKeyEnabled>` flag.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed, no `<SSEAlgorithm>` is present,
/// or the algorithm is neither `AES256` nor `aws:kms`.
pub fn parse_server_side_encryption_configuration(
    body: &[u8],
) -> Result<ServerSideEncryptionRule, Error> {
    let mut algorithm: Option<String> = None;
    let mut kms_key_id: Option<String> = None;
    let mut bucket_key_enabled = false;
    let mut cur: Option<&'static str> = None;
    let mut buf = String::new();

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => {
                cur = if name == b"SSEAlgorithm" {
                    Some("alg")
                } else if name == b"KMSMasterKeyID" {
                    Some("kms")
                } else if name == b"BucketKeyEnabled" {
                    Some("bke")
                } else {
                    None
                };
                buf.clear();
            }
            Sax::Text(t) => {
                if cur.is_some() {
                    buf.push_str(&t);
                }
            }
            Sax::Close(name) => {
                if name == b"SSEAlgorithm" {
                    algorithm = Some(buf.trim().to_owned());
                } else if name == b"KMSMasterKeyID" {
                    let id = buf.trim();
                    if !id.is_empty() {
                        kms_key_id = Some(id.to_owned());
                    }
                } else if name == b"BucketKeyEnabled" {
                    bucket_key_enabled = buf.trim().eq_ignore_ascii_case("true");
                }
                cur = None;
            }
        }
        Ok(())
    })?;

    let sse_algorithm = match algorithm {
        Some(a) if a.eq_ignore_ascii_case("AES256") => "AES256".to_owned(),
        Some(a) if a.eq_ignore_ascii_case("aws:kms") => "aws:kms".to_owned(),
        _ => return Err(malformed()),
    };
    Ok(ServerSideEncryptionRule {
        sse_algorithm,
        kms_master_key_id: kms_key_id,
        bucket_key_enabled,
    })
}

// ===========================================================================================
// AccessControlPolicy (PutBucketAcl / PutObjectAcl request body)
// ===========================================================================================

/// Parse an S3 `<AccessControlPolicy>` body into an [`Acl`].
///
/// The document carries an `<Owner>` (whose `<ID>` becomes the ACL owner) and an
/// `<AccessControlList>` of `<Grant>` elements. Each grant names a grantee and a permission:
///
/// - A `CanonicalUser` grantee is identified by its `<ID>` and maps to [`Grantee::User`].
/// - A `Group` grantee is identified by its `<URI>`; the three well-known AWS group URIs map
///   to [`Grantee::AllUsers`], [`Grantee::AuthenticatedUsers`] and [`Grantee::LogDelivery`].
///
/// The grantee kind is conveyed on the wire by the `xsi:type` attribute, but the SAX driver
/// strips attributes, so the kind is inferred structurally from whether the grantee carries an
/// `<ID>` (canonical user) or a `<URI>` (group) — which is unambiguous in well-formed S3 ACLs.
///
/// Permissions map from the S3 tokens `FULL_CONTROL`/`READ`/`WRITE`/`READ_ACP`/`WRITE_ACP`.
///
/// # Errors
/// Returns [`Error::MalformedXml`] if the body is not well-formed, the `<Owner>` has no `<ID>`,
/// a `<Grant>` lacks a grantee identity or permission, a group `<URI>` is unrecognized, or a
/// `<Permission>` token is unknown.
pub fn parse_access_control_policy(body: &[u8]) -> Result<Acl, Error> {
    let mut owner_id: Option<String> = None;
    let mut grants: Vec<Grant> = Vec::new();

    // Nesting flags: an ACL is shallow, but a grantee `<ID>` must be told apart from the owner
    // `<ID>`, so track whether we are inside the owner block versus a grant's grantee block.
    let mut in_owner = false;
    let mut in_grant = false;
    let mut in_grantee = false;
    let mut cur_field: Option<Vec<u8>> = None;

    // Per-grant accumulators.
    let mut grantee_id: Option<String> = None;
    let mut grantee_uri: Option<String> = None;
    let mut permission: Option<Permission> = None;

    drive(body, |ev| {
        match ev {
            Sax::Open(name) => match name.as_slice() {
                b"Owner" => in_owner = true,
                b"Grant" => {
                    in_grant = true;
                    grantee_id = None;
                    grantee_uri = None;
                    permission = None;
                }
                b"Grantee" if in_grant => in_grantee = true,
                _ => cur_field = Some(name),
            },
            Sax::Text(text) => {
                if let Some(field) = &cur_field {
                    match field.as_slice() {
                        b"ID" if in_owner && !in_grantee => {
                            owner_id = Some(text.trim().to_owned());
                        }
                        b"ID" if in_grantee => grantee_id = Some(text.trim().to_owned()),
                        b"URI" if in_grantee => grantee_uri = Some(text.trim().to_owned()),
                        b"Permission" if in_grant => {
                            permission = Some(parse_permission(text.trim())?);
                        }
                        _ => {}
                    }
                }
            }
            Sax::Close(name) => {
                match name.as_slice() {
                    b"Owner" => in_owner = false,
                    b"Grantee" => in_grantee = false,
                    b"Grant" => {
                        let grantee = match (&grantee_id, &grantee_uri) {
                            (Some(id), _) => Grantee::User(UserId(id.clone())),
                            (None, Some(uri)) => parse_group_uri(uri)?,
                            (None, None) => return Err(malformed()),
                        };
                        let perm = permission.take().ok_or_else(malformed)?;
                        grants.push(Grant {
                            grantee,
                            permission: perm,
                        });
                        in_grant = false;
                    }
                    _ => {}
                }
                cur_field = None;
            }
        }
        Ok(())
    })?;

    let owner = UserId(owner_id.ok_or_else(malformed)?);
    Ok(Acl { owner, grants })
}

/// Map an S3 permission token to a [`Permission`].
fn parse_permission(token: &str) -> Result<Permission, Error> {
    match token {
        "FULL_CONTROL" => Ok(Permission::FullControl),
        "READ" => Ok(Permission::Read),
        "WRITE" => Ok(Permission::Write),
        "READ_ACP" => Ok(Permission::ReadAcp),
        "WRITE_ACP" => Ok(Permission::WriteAcp),
        _ => Err(malformed()),
    }
}

/// Map a well-known AWS group URI to a group [`Grantee`].
fn parse_group_uri(uri: &str) -> Result<Grantee, Error> {
    match uri {
        "http://acs.amazonaws.com/groups/global/AllUsers" => Ok(Grantee::AllUsers),
        "http://acs.amazonaws.com/groups/global/AuthenticatedUsers" => {
            Ok(Grantee::AuthenticatedUsers)
        }
        "http://acs.amazonaws.com/groups/s3/LogDelivery" => Ok(Grantee::LogDelivery),
        _ => Err(malformed()),
    }
}

/// Strip a single pair of surrounding ASCII double quotes from an ETag wire value.
fn unquote(s: &str) -> String {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
        s[1..s.len() - 1].to_owned()
    } else {
        s.to_owned()
    }
}
