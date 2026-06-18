//! Per-bucket **remote replication targets** (the MinIO model, ARCH §20.5).
//!
//! A remote target binds a destination endpoint + credentials to a stable **ARN**; a bucket's
//! replication *rules* reference a target by that ARN (`<Destination><Bucket>arn:cairn:…</Bucket>`)
//! rather than carrying the endpoint inline. The destination authenticates inbound replicated
//! writes with SigV4 using a dedicated access key; that secret is **sealed at rest** under the
//! master key — exactly as SigV4 user secrets are — so it never lands on disk in the clear.
//!
//! The stored form is [`RemoteTarget`] (secret as ciphertext + nonce). A target is *minted* from
//! an operator-supplied [`RemoteTargetInput`] with [`seal_target`], which seals the secret and
//! assigns the ARN. The full set of a bucket's targets is the
//! [`ConfigAspect::ReplicationTargets`](cairn_types) document, a JSON array round-tripped by
//! [`parse_targets`] / [`serialize_targets`]. At ship time the engine resolves a rule's ARN to a
//! target with [`resolve_target`] and unseals it with [`open_target`] into an [`OpenTarget`] whose
//! plaintext secret lives only in a [`Zeroizing`](zeroize::Zeroizing) buffer.

use cairn_crypto::SystemCrypto;
use cairn_types::crypto::Nonce;
use cairn_types::error::ReplicationError;
use cairn_types::traits::Crypto;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::Zeroizing;

/// A stored remote replication target: a destination endpoint + credentials behind a stable ARN,
/// with the secret access key **sealed** under the master key (ciphertext + nonce).
///
/// This is the at-rest, serializable form; it is safe to persist because the secret is encrypted.
/// Unseal it with [`open_target`] to obtain the usable plaintext credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteTarget {
    /// The stable target ARN (`arn:cairn:replication:<region>:<uuid>:<dest_bucket>`). Rules
    /// reference a target by this string.
    pub arn: String,
    /// The destination endpoint base URL, e.g. `https://s3.peer.example.com:9000`.
    pub endpoint: String,
    /// The SigV4 signing region for the destination.
    pub region: String,
    /// The destination bucket replicated into (path-style).
    pub dest_bucket: String,
    /// The destination access-key id (not secret; stored in the clear).
    pub access_key_id: String,
    /// The sealed (AES-GCM) secret access key ciphertext, including the AEAD tag.
    pub secret_ciphertext: Vec<u8>,
    /// The AEAD nonce the secret was sealed under.
    pub nonce: Vec<u8>,
}

/// The plaintext creation input for a remote target: what an operator supplies when registering a
/// destination. The secret is sealed by [`seal_target`] before it is ever stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTargetInput {
    /// The destination endpoint base URL.
    pub endpoint: String,
    /// The SigV4 signing region.
    pub region: String,
    /// The destination bucket.
    pub dest_bucket: String,
    /// The destination access-key id.
    pub access_key_id: String,
    /// The destination secret access key, in plaintext. Sealed at rest by [`seal_target`].
    pub secret: String,
}

/// An **opened** remote target: the same connection parameters as [`RemoteTarget`] but with the
/// secret unsealed into a [`Zeroizing`] buffer so it is scrubbed on drop. Build one with
/// [`open_target`] and feed it to [`sink_for_target`](crate::sink_for_target).
#[derive(Debug, Clone)]
pub struct OpenTarget {
    /// The destination endpoint base URL.
    pub endpoint: String,
    /// The SigV4 signing region.
    pub region: String,
    /// The destination bucket.
    pub dest_bucket: String,
    /// The destination access-key id.
    pub access_key_id: String,
    /// The unsealed destination secret access key (scrubbed on drop).
    pub secret: Zeroizing<String>,
}

/// Seal a [`RemoteTargetInput`] into a storable [`RemoteTarget`]: encrypt the secret under the
/// master key and mint a fresh ARN `arn:cairn:replication:<region>:<uuid>:<dest_bucket>`.
///
/// # Errors
/// Returns [`ReplicationError::Terminal`] if sealing the secret fails (a misconfigured master key
/// is an operator-actionable, permanent error, not a transient one).
pub fn seal_target(
    crypto: &SystemCrypto,
    input: RemoteTargetInput,
) -> Result<RemoteTarget, ReplicationError> {
    let sealed = crypto.seal(input.secret.as_bytes()).map_err(|e| {
        ReplicationError::Terminal(format!("sealing replication target secret failed: {e}"))
    })?;
    let arn = format!(
        "arn:cairn:replication:{}:{}:{}",
        input.region,
        Uuid::new_v4().simple(),
        input.dest_bucket
    );
    Ok(RemoteTarget {
        arn,
        endpoint: input.endpoint,
        region: input.region,
        dest_bucket: input.dest_bucket,
        access_key_id: input.access_key_id,
        // CRK1 envelope (audit #29): the nonce is inside the ciphertext; store an empty nonce.
        secret_ciphertext: sealed.ciphertext,
        nonce: Vec::new(),
    })
}

/// Parse the stored `ReplicationTargets` config-aspect document (a JSON array of [`RemoteTarget`])
/// into a typed vector. An empty/whitespace-only document parses to an empty list.
///
/// # Errors
/// Returns [`ReplicationError::Terminal`] if the document is not well-formed JSON for the target
/// list (a corrupt stored document is operator-actionable, not transiently retryable).
pub fn parse_targets(doc: &[u8]) -> Result<Vec<RemoteTarget>, ReplicationError> {
    let trimmed = doc
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .map_or(&[][..], |_| doc);
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_slice(trimmed).map_err(|e| {
        ReplicationError::Terminal(format!("malformed replication-targets document: {e}"))
    })
}

/// Serialize a set of remote targets into the stored `ReplicationTargets` config-aspect document
/// (a pretty-printed JSON array). The secret is already sealed in each [`RemoteTarget`], so this
/// document is safe to persist.
#[must_use]
pub fn serialize_targets(targets: &[RemoteTarget]) -> String {
    // Serialization of a `Vec<RemoteTarget>` cannot fail (all fields are plain owned data), so a
    // failure here is impossible in practice; fall back to an empty array to keep this infallible.
    serde_json::to_string_pretty(targets).unwrap_or_else(|_| "[]".to_owned())
}

/// Resolve the target a rule's ARN refers to, by exact ARN match.
#[must_use]
pub fn resolve_target<'a>(targets: &'a [RemoteTarget], arn: &str) -> Option<&'a RemoteTarget> {
    targets.iter().find(|t| t.arn == arn)
}

/// Unseal a stored [`RemoteTarget`] into an [`OpenTarget`] with the plaintext secret, decrypting
/// the secret under the master key.
///
/// # Errors
/// Returns [`ReplicationError::Terminal`] if the sealed secret cannot be decrypted (wrong key or a
/// tampered/corrupt ciphertext) or is not valid UTF-8.
pub fn open_target(
    crypto: &SystemCrypto,
    t: &RemoteTarget,
) -> Result<OpenTarget, ReplicationError> {
    let plaintext = crypto
        .open(&t.secret_ciphertext, &Nonce(t.nonce.clone()))
        .map_err(|e| {
            ReplicationError::Terminal(format!("unsealing replication target secret failed: {e}"))
        })?;
    // Move the bytes into a zeroizing buffer first so the plaintext is scrubbed even on the UTF-8
    // error path.
    let plaintext = Zeroizing::new(plaintext);
    let secret = String::from_utf8(plaintext.to_vec()).map_err(|_| {
        ReplicationError::Terminal("replication target secret is not valid UTF-8".to_owned())
    })?;
    Ok(OpenTarget {
        endpoint: t.endpoint.clone(),
        region: t.region.clone(),
        dest_bucket: t.dest_bucket.clone(),
        access_key_id: t.access_key_id.clone(),
        secret: Zeroizing::new(secret),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crypto() -> SystemCrypto {
        SystemCrypto::new([9u8; 32])
    }

    fn input() -> RemoteTargetInput {
        RemoteTargetInput {
            endpoint: "https://s3.peer.example.com:9000".to_owned(),
            region: "us-west-2".to_owned(),
            dest_bucket: "mirror".to_owned(),
            access_key_id: "AKIDPEER".to_owned(),
            secret: "super-secret-key".to_owned(),
        }
    }

    #[test]
    fn seal_then_open_round_trips_the_secret() {
        let c = crypto();
        let t = seal_target(&c, input()).expect("seal");
        // The secret never appears in the stored ciphertext.
        assert!(
            !t.secret_ciphertext
                .windows(16)
                .any(|w| w == b"super-secret-key")
        );
        assert_eq!(t.access_key_id, "AKIDPEER");
        assert_eq!(t.dest_bucket, "mirror");

        let open = open_target(&c, &t).expect("open");
        assert_eq!(open.secret.as_str(), "super-secret-key");
        assert_eq!(open.endpoint, "https://s3.peer.example.com:9000");
        assert_eq!(open.region, "us-west-2");
        assert_eq!(open.dest_bucket, "mirror");
        assert_eq!(open.access_key_id, "AKIDPEER");
    }

    #[test]
    fn arn_shape_is_minted_from_region_and_dest_bucket() {
        let t = seal_target(&crypto(), input()).expect("seal");
        assert!(t.arn.starts_with("arn:cairn:replication:us-west-2:"));
        assert!(t.arn.ends_with(":mirror"));
        // The middle segment is a 32-hex-digit simple uuid.
        let parts: Vec<&str> = t.arn.split(':').collect();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[4].len(), 32);
        assert!(parts[4].chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn each_seal_mints_a_distinct_arn() {
        let c = crypto();
        let a = seal_target(&c, input()).expect("seal a");
        let b = seal_target(&c, input()).expect("seal b");
        assert_ne!(a.arn, b.arn, "fresh uuid per target");
    }

    #[test]
    fn parse_and_serialize_round_trip() {
        let c = crypto();
        let targets = vec![
            seal_target(&c, input()).expect("seal 1"),
            seal_target(&c, input()).expect("seal 2"),
        ];
        let doc = serialize_targets(&targets);
        let parsed = parse_targets(doc.as_bytes()).expect("parse");
        assert_eq!(parsed, targets);
    }

    #[test]
    fn empty_document_parses_to_no_targets() {
        assert!(parse_targets(b"").expect("empty").is_empty());
        assert!(parse_targets(b"   \n  ").expect("whitespace").is_empty());
    }

    #[test]
    fn malformed_document_is_terminal() {
        let err = parse_targets(b"{not json").unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }

    #[test]
    fn resolve_target_matches_by_arn() {
        let c = crypto();
        let targets = vec![
            seal_target(&c, input()).expect("seal 1"),
            seal_target(&c, input()).expect("seal 2"),
        ];
        let arn = targets[1].arn.clone();
        assert_eq!(resolve_target(&targets, &arn).map(|t| &t.arn), Some(&arn));
        assert!(resolve_target(&targets, "arn:cairn:replication:x:y:z").is_none());
    }

    #[test]
    fn open_with_wrong_key_is_terminal() {
        let t = seal_target(&crypto(), input()).expect("seal");
        let other = SystemCrypto::new([1u8; 32]);
        let err = open_target(&other, &t).unwrap_err();
        assert!(matches!(err, ReplicationError::Terminal(_)));
    }
}
