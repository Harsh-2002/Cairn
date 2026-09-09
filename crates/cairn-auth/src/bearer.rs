//! The first-party Bearer scheme (ARCH 14.4): a high-entropy machine token compared against a
//! stored fast hash in constant time.

use crate::crypto_util::sha256_hex;

/// Encode credential strings losslessly. Ordinary machine credentials retain the legacy format;
/// the encoded form contains no dot, so its interpretation cannot collide with a legacy token.
#[must_use]
pub fn encode_bearer_token(id: &str, secret: &str) -> String {
    let token_byte = |b: u8| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b);
    if !id.contains('.') && id.bytes().all(token_byte) && secret.bytes().all(token_byte) {
        format!("{id}.{secret}")
    } else {
        format!("~1~{}~{}", hex::encode(id), hex::encode(secret))
    }
}

/// Parse legacy `Bearer <id>.<secret>` or lossless `Bearer ~1~<hex-id>~<hex-secret>`.
#[must_use]
pub fn parse_bearer(header: &str) -> Option<(String, String)> {
    let rest = header.strip_prefix("Bearer ")?.trim();
    let (id, secret) = if let Some((id, secret)) = rest.split_once('.') {
        (id.to_owned(), secret.to_owned())
    } else {
        let (id, secret) = rest.strip_prefix("~1~")?.split_once('~')?;
        (
            String::from_utf8(hex::decode(id).ok()?).ok()?,
            String::from_utf8(hex::decode(secret).ok()?).ok()?,
        )
    };
    if id.is_empty() || secret.is_empty() {
        return None;
    }
    Some((id, secret))
}

/// The stored hash of a Bearer secret (a fast cryptographic hash; these are high-entropy
/// machine tokens, not human passwords).
#[must_use]
pub fn hash_bearer_secret(secret: &str) -> String {
    sha256_hex(secret.as_bytes())
}

/// The stored hash of an STS session token (`X-Amz-Security-Token`). The token is a high-entropy
/// machine-minted value, so a fast hash suffices; the comparison is constant-time at verify time.
#[must_use]
pub fn hash_session_token(token: &str) -> String {
    sha256_hex(token.as_bytes())
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

    #[test]
    fn credential_strings_round_trip_without_normalization() {
        for (id, secret) in [
            ("cairn", "cairnadmin"),
            ("first.last@example.test", "secret.with.dots"),
            ("'quoted'", "\"double quoted\""),
            (" admin ", " secret "),
            ("管理者@example.test", "sécret;\r\n\t\\$"),
            ("~1~6162~6364", "legacy.secret"),
        ] {
            let token = encode_bearer_token(id, secret);
            assert!(token.bytes().all(|b| b.is_ascii_graphic()));
            assert_eq!(
                parse_bearer(&format!("Bearer {token}")),
                Some((id.to_owned(), secret.to_owned()))
            );
        }
        assert_eq!(
            encode_bearer_token("cairn", "secret.with.dots"),
            "cairn.secret.with.dots"
        );
    }

    #[test]
    fn encoded_credentials_reject_malformed_or_empty_components() {
        for token in [
            "~2~61~62",
            "~1~~62",
            "~1~61~",
            "~1~6~62",
            "~1~gg~62",
            "~1~ff~62",
            "~1~61~62~63",
            ".secret",
            "id.",
        ] {
            assert!(parse_bearer(&format!("Bearer {token}")).is_none());
        }
    }
}
