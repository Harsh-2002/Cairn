//! The first-party Bearer scheme (ARCH 14.4): a high-entropy machine token compared against a
//! stored fast hash in constant time.

use crate::crypto_util::sha256_hex;

/// Parse a `Bearer <access_key_id>.<secret>` Authorization header.
#[must_use]
pub fn parse_bearer(header: &str) -> Option<(String, String)> {
    let rest = header.strip_prefix("Bearer ")?.trim();
    let (id, secret) = rest.split_once('.')?;
    if id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((id.to_owned(), secret.to_owned()))
}

/// The stored hash of a Bearer secret (a fast cryptographic hash; these are high-entropy
/// machine tokens, not human passwords).
#[must_use]
pub fn hash_bearer_secret(secret: &str) -> String {
    sha256_hex(secret.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_hashes() {
        let (id, secret) = parse_bearer("Bearer cairn_abc.s3cr3t").unwrap();
        assert_eq!(id, "cairn_abc");
        assert_eq!(secret, "s3cr3t");
        assert_eq!(hash_bearer_secret("s3cr3t").len(), 64);
        assert!(parse_bearer("Bearer nodot").is_none());
        assert!(parse_bearer("Basic x").is_none());
    }
}
