//! Validated identifier newtypes. Keys never become filesystem paths (the storage
//! model uses opaque [`StoragePath`]s), so validation here is about S3 wire correctness
//! and defense-in-depth, not path safety (which lives in `cairn-blob`).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum length of an S3 object key, in bytes (the AWS limit).
pub const MAX_KEY_LEN: usize = 1024;

/// An opaque user identifier.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UserId(pub String);

impl UserId {
    /// Mint a fresh random user id.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }
}

impl fmt::Debug for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UserId({})", self.0)
    }
}
impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated S3 bucket name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BucketName(String);

impl BucketName {
    /// Validate and construct a bucket name per the S3 DNS-compatible naming rules
    /// (3..=63 chars, lowercase letters/digits/hyphens/dots, no leading/trailing hyphen
    /// or dot, no adjacent dots, not an IP address).
    ///
    /// # Errors
    /// Returns [`InvalidName`] describing the first rule violated.
    pub fn parse(s: &str) -> Result<Self, InvalidName> {
        let len = s.len();
        if !(3..=63).contains(&len) {
            return Err(InvalidName::Length);
        }
        if !s
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        {
            return Err(InvalidName::Charset);
        }
        let first = s.as_bytes()[0];
        let last = s.as_bytes()[len - 1];
        if !(first.is_ascii_lowercase() || first.is_ascii_digit())
            || !(last.is_ascii_lowercase() || last.is_ascii_digit())
        {
            return Err(InvalidName::Boundary);
        }
        if s.contains("..") || s.contains(".-") || s.contains("-.") {
            return Err(InvalidName::AdjacentSeparators);
        }
        if s.split('.').all(|seg| seg.parse::<u8>().is_ok()) && s.matches('.').count() == 3 {
            return Err(InvalidName::IpAddress);
        }
        Ok(Self(s.to_owned()))
    }

    /// The borrowed string form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BucketName({})", self.0)
    }
}
impl fmt::Display for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A validated S3 object key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Validate and construct an object key: non-empty, at most [`MAX_KEY_LEN`] bytes,
    /// valid UTF-8 (guaranteed by `&str`), and free of XML-illegal control characters.
    ///
    /// # Errors
    /// Returns [`InvalidName`] if the key is empty, too long, or contains a control character
    /// that XML 1.0 cannot represent.
    pub fn parse(s: &str) -> Result<Self, InvalidName> {
        if s.is_empty() {
            return Err(InvalidName::Empty);
        }
        if s.len() > MAX_KEY_LEN {
            return Err(InvalidName::Length);
        }
        // Reject the C0 control characters XML 1.0 cannot represent — every byte below 0x20 except
        // tab, LF and CR (NUL included) (audit #32). Such a key could never be emitted into a
        // ListObjects/ListVersions XML response, not even as a numeric character reference, so it
        // must not be storable in the first place.
        if s.bytes().any(|b| b < 0x20 && !matches!(b, b'\t' | b'\n' | b'\r')) {
            return Err(InvalidName::Charset);
        }
        Ok(Self(s.to_owned()))
    }

    /// The borrowed string form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectKey({})", self.0)
    }
}
impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An opaque, sortable object-version identifier. The literal `null` is the sentinel
/// for the single version of an object in an unversioned or suspended bucket.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VersionId(String);

impl VersionId {
    /// The sentinel version id used in unversioned/suspended buckets.
    #[must_use]
    pub fn null() -> Self {
        Self("null".to_owned())
    }

    /// Mint a fresh, time-sortable version id (uuid v7: its hex form sorts by creation
    /// time, so `ORDER BY version_id DESC` yields newest-first as S3 requires).
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::now_v7().simple().to_string())
    }

    /// Construct from a stored/wire string without minting.
    #[must_use]
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// Whether this is the unversioned sentinel.
    #[must_use]
    pub fn is_null(&self) -> bool {
        self.0 == "null"
    }

    /// The borrowed string form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VersionId({})", self.0)
    }
}
impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An opaque path to a committed blob within the data directory, of the form
/// `bucket/uuid`. Derived from a fresh UUID, never from the object key.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StoragePath(String);

impl StoragePath {
    /// Mint a fresh opaque blob path under the given bucket directory.
    #[must_use]
    pub fn generate(bucket: &BucketName) -> Self {
        Self(format!(
            "{}/{}",
            bucket.as_str(),
            uuid::Uuid::new_v4().simple()
        ))
    }

    /// Reconstruct from a stored string.
    #[must_use]
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// The borrowed string form (relative to the data root).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StoragePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoragePath({})", self.0)
    }
}
impl fmt::Display for StoragePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A multipart upload session identifier.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UploadId(String);

impl UploadId {
    /// Mint a fresh upload id.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    /// Reconstruct from a wire/stored string.
    #[must_use]
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// The borrowed string form.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UploadId({})", self.0)
    }
}
impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Reasons a [`BucketName`] or [`ObjectKey`] failed validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidName {
    /// The name is empty.
    #[error("name is empty")]
    Empty,
    /// The name violates the allowed length bounds.
    #[error("name length out of bounds")]
    Length,
    /// The name contains characters outside the allowed set.
    #[error("name contains invalid characters")]
    Charset,
    /// The name starts or ends with a disallowed character.
    #[error("name starts or ends with an invalid character")]
    Boundary,
    /// The name contains adjacent separators (`..`, `.-`, `-.`).
    #[error("name contains adjacent separators")]
    AdjacentSeparators,
    /// The name is formatted as an IP address, which S3 forbids.
    #[error("name must not be formatted as an IP address")]
    IpAddress,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_name_rules() {
        assert!(BucketName::parse("my-bucket").is_ok());
        assert!(BucketName::parse("a.b.c").is_ok());
        assert_eq!(BucketName::parse("ab"), Err(InvalidName::Length));
        assert_eq!(BucketName::parse("-bad"), Err(InvalidName::Boundary));
        assert_eq!(BucketName::parse("bad-"), Err(InvalidName::Boundary));
        assert_eq!(
            BucketName::parse("a..b"),
            Err(InvalidName::AdjacentSeparators)
        );
        assert_eq!(BucketName::parse("UPPER"), Err(InvalidName::Charset));
        assert_eq!(
            BucketName::parse("192.168.0.1"),
            Err(InvalidName::IpAddress)
        );
    }

    #[test]
    fn object_key_rules() {
        assert!(ObjectKey::parse("photos/2026/a.jpg").is_ok());
        assert_eq!(ObjectKey::parse(""), Err(InvalidName::Empty));
        assert_eq!(ObjectKey::parse("a\0b"), Err(InvalidName::Charset));
        let long = "x".repeat(MAX_KEY_LEN + 1);
        assert_eq!(ObjectKey::parse(&long), Err(InvalidName::Length));
        // Audit #32: XML-illegal C0 control characters are rejected...
        assert_eq!(ObjectKey::parse("a\u{1}b"), Err(InvalidName::Charset));
        assert_eq!(ObjectKey::parse("a\u{1f}b"), Err(InvalidName::Charset));
        assert_eq!(ObjectKey::parse("a\u{8}b"), Err(InvalidName::Charset));
        // ...but tab, LF and CR (the XML-legal whitespace controls) are allowed.
        assert!(ObjectKey::parse("a\tb\nc\rd").is_ok());
    }

    #[test]
    fn version_id_sorts_by_time() {
        let a = VersionId::generate();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = VersionId::generate();
        assert!(
            a.as_str() < b.as_str(),
            "uuid v7 must sort by creation time"
        );
        assert!(VersionId::null().is_null());
    }
}
