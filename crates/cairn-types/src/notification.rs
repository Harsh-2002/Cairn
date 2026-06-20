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
    /// An optional HMAC-SHA256 secret; when set, deliveries carry an `X-Cairn-Signature` header.
    #[serde(default)]
    pub secret: Option<String>,
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
