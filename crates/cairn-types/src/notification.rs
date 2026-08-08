//! Bucket event-notification (webhook) configuration and the event taxonomy.
//!
//! Cairn's event notifications are **webhook-native**: a bucket carries a list of webhook
//! endpoints (URL + event selectors + optional prefix/suffix filter + optional HMAC secret),
//! and a matching object event enqueues a durable delivery entry that a background worker POSTs
//! as JSON (ARCH 20-style outbox, best-effort at-least-once). This is deliberately *not* the S3
//! SNS/SQS/Lambda `?notification` shape — those target AWS ARNs Cairn has no equivalent for — so
//! the configuration is set through the management API rather than the S3 `?notification`
//! subresource, which stays `NotImplemented`.

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::crypto::{Nonce, Sealed};
use crate::error::CryptoError;
use crate::secret::SecretString;
use crate::traits::Crypto;

/// The kind of object event that occurred, in S3's `s3:Type:Detail` taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    /// A single-request object PUT.
    ObjectCreatedPut,
    /// A server-side copy wrote the destination object.
    ObjectCreatedCopy,
    /// A multipart upload completed into an object.
    ObjectCreatedCompleteMultipartUpload,
    /// A version (or sentinel) was permanently deleted.
    ObjectRemovedDelete,
    /// A delete marker was created over a key in a versioned bucket.
    ObjectRemovedDeleteMarkerCreated,
}

impl EventKind {
    /// The full S3 event name, e.g. `s3:ObjectCreated:Put`.
    #[must_use]
    pub fn s3_name(self) -> &'static str {
        match self {
            EventKind::ObjectCreatedPut => "s3:ObjectCreated:Put",
            EventKind::ObjectCreatedCopy => "s3:ObjectCreated:Copy",
            EventKind::ObjectCreatedCompleteMultipartUpload => {
                "s3:ObjectCreated:CompleteMultipartUpload"
            }
            EventKind::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            EventKind::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
        }
    }

    /// The event category prefix, e.g. `s3:ObjectCreated`.
    #[must_use]
    pub fn category(self) -> &'static str {
        match self {
            EventKind::ObjectCreatedPut
            | EventKind::ObjectCreatedCopy
            | EventKind::ObjectCreatedCompleteMultipartUpload => "s3:ObjectCreated",
            EventKind::ObjectRemovedDelete | EventKind::ObjectRemovedDeleteMarkerCreated => {
                "s3:ObjectRemoved"
            }
        }
    }

    /// Whether this event matches one of an endpoint's selectors. A selector is either an exact
    /// S3 event name (`s3:ObjectCreated:Put`), a category wildcard (`s3:ObjectCreated:*`), or the
    /// catch-all `s3:*`.
    #[must_use]
    pub fn matches_selector(self, selector: &str) -> bool {
        selector == "s3:*"
            || selector == self.s3_name()
            || selector
                .strip_suffix(":*")
                .is_some_and(|cat| cat == self.category())
    }
}

/// One webhook endpoint subscribed to a bucket's object events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    /// A stable, caller-chosen identifier (unique within the bucket).
    pub id: String,
    /// The destination URL the JSON event is POSTed to (`http`/`https`).
    pub url: String,
    /// The event selectors this endpoint subscribes to (e.g. `["s3:ObjectCreated:*"]`).
    pub events: Vec<String>,
    /// An optional object-key prefix filter; only keys with this prefix notify.
    #[serde(default)]
    pub prefix: Option<String>,
    /// An optional object-key suffix filter; only keys with this suffix notify.
    #[serde(default)]
    pub suffix: Option<String>,
    /// An optional sealed HMAC-SHA256 secret; when set, deliveries carry an
    /// `X-Cairn-Signature` header. New writes always use [`WebhookSecret::Sealed`]. The legacy
    /// plaintext variant exists only so the key-rewrap migration can read and replace old config
    /// documents without losing subscriptions.
    #[serde(default)]
    pub secret: Option<WebhookSecret>,
}

/// The envelope stored for one webhook HMAC signing key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedWebhookSecret {
    /// Authenticated ciphertext produced by the node master-key ring.
    pub ciphertext: Vec<u8>,
    /// Legacy detached nonce. Current CRK1 ciphertext embeds its nonce and stores this empty.
    #[serde(default)]
    pub nonce: Vec<u8>,
}

impl std::fmt::Debug for SealedWebhookSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SealedWebhookSecret")
            .field("ciphertext", &"<redacted>")
            .field("nonce", &"<redacted>")
            .finish()
    }
}

/// Stored webhook signing-key representation.
///
/// `LegacyPlaintext` is backward-read-only: the control plane never creates it, the worker holds
/// it in a zeroize-on-drop [`SecretString`], and the master-key rewrap pass replaces it with a
/// [`Sealed`](WebhookSecret::Sealed) envelope.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WebhookSecret {
    /// Current authenticated envelope form.
    Sealed(SealedWebhookSecret),
    /// Pre-sealing JSON string, accepted solely for online migration.
    LegacyPlaintext(SecretString),
}

impl WebhookSecret {
    /// Convert the cryptography trait's sealed output into the persisted representation.
    #[must_use]
    pub fn from_sealed(sealed: Sealed) -> Self {
        Self::Sealed(SealedWebhookSecret {
            ciphertext: sealed.ciphertext,
            nonce: sealed.nonce.0,
        })
    }

    /// Open the signing key into zeroize-on-drop memory.
    ///
    /// Legacy plaintext documents are copied into a temporary zeroizing buffer so callers have
    /// one safe representation regardless of whether the background re-wrap migration has reached
    /// this bucket yet.
    pub fn open<C: Crypto + ?Sized>(&self, crypto: &C) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        match self {
            Self::Sealed(sealed) => crypto.open(&sealed.ciphertext, &Nonce(sealed.nonce.clone())),
            Self::LegacyPlaintext(secret) => {
                Ok(Zeroizing::new(secret.expose_secret().as_bytes().to_vec()))
            }
        }
    }
}

impl std::fmt::Debug for WebhookSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sealed(_) => formatter.write_str("WebhookSecret::Sealed(<redacted>)"),
            Self::LegacyPlaintext(_) => {
                formatter.write_str("WebhookSecret::LegacyPlaintext(<redacted>)")
            }
        }
    }
}

impl WebhookEndpoint {
    /// Whether `event` on `key` should be delivered to this endpoint (selector + prefix/suffix).
    #[must_use]
    pub fn matches(&self, event: EventKind, key: &str) -> bool {
        let event_ok = self.events.iter().any(|s| event.matches_selector(s));
        let prefix_ok = self.prefix.as_deref().is_none_or(|p| key.starts_with(p));
        let suffix_ok = self.suffix.as_deref().is_none_or(|s| key.ends_with(s));
        event_ok && prefix_ok && suffix_ok
    }
}

/// A bucket's event-notification configuration: a list of webhook endpoints. Stored as JSON under
/// [`crate::bucket::ConfigAspect::Notification`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationConfig {
    /// The configured webhook endpoints.
    #[serde(default)]
    pub endpoints: Vec<WebhookEndpoint>,
}

impl NotificationConfig {
    /// Whether any endpoint is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }
}
