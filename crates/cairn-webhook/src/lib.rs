//! `cairn-webhook` — the outbox-driven webhook **event-notification** delivery engine.
//!
//! It mirrors the replication engine (ARCH 20): a background loop claims a batch of due
//! [`WebhookEntry`] rows from the metadata store under a lease, resolves each entry's destination
//! against its bucket's [`NotificationConfig`], POSTs the pre-rendered JSON event (optionally HMAC
//! signed), and marks the entry delivered, rescheduled-with-backoff, or terminally failed. The
//! enqueue is best-effort at-least-once: an endpoint that 5xxs is retried with exponential backoff
//! until the attempt budget is exhausted, then parked as `failed` for operator attention.
//!
//! Payload rendering happens at *enqueue* time (in the protocol layer, where the object's size and
//! ETag are in hand) and is stored verbatim on the entry, so this crate is a pure delivery
//! transport — it never reaches back into the object store.

#![forbid(unsafe_code)]

mod sink;

use cairn_types::bucket::ConfigAspect;
use cairn_types::meta::{Mutation, WebhookEntry};
use cairn_types::notification::NotificationConfig;
use cairn_types::traits::{Clock, MetadataStore};
use cairn_types::{MetaError, Timestamp};
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub use sink::{HttpWebhookSink, WebhookError, WebhookSink};

/// Tunables for the delivery engine.
#[derive(Debug, Clone, Copy)]
pub struct WebhookOpts {
    /// Maximum entries claimed per batch.
    pub batch_size: u32,
    /// Total delivery attempts before an entry is parked as terminally failed.
    pub max_attempts: u32,
    /// The first retry delay, in seconds (doubles each attempt up to the cap).
    pub base_backoff_secs: u64,
    /// The retry-delay cap, in seconds.
    pub max_backoff_secs: u64,
}

impl Default for WebhookOpts {
    fn default() -> Self {
        Self {
            batch_size: 64,
            max_attempts: 8,
            base_backoff_secs: 5,
            max_backoff_secs: 900,
        }
    }
}

/// The result of one drain pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebhookReport {
    /// Entries delivered successfully this pass.
    pub delivered: u64,
    /// Entries that failed this pass (rescheduled or parked terminal).
    pub failed: u64,
    /// Entries dropped because their endpoint subscription no longer exists.
    pub dropped: u64,
}

impl WebhookReport {
    /// Whether the pass did no work (the outbox was idle).
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.delivered == 0 && self.failed == 0 && self.dropped == 0
    }
}

/// Exponential backoff (pure): `base * 2^(attempts-1)`, clamped to `[base, cap]`. Mirrors the
/// replication schedule so the two outboxes behave identically under load.
#[must_use]
pub fn next_backoff(attempts: u32, base: u64, cap: u64) -> u64 {
    let exponent = attempts.saturating_sub(1).min(63);
    let factor = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let raw = base.saturating_mul(factor);
    raw.clamp(base.min(cap), cap)
}

/// The hex-encoded HMAC-SHA256 of `body` under `secret` (the `X-Cairn-Signature` value).
#[must_use]
pub fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// The delivery engine.
#[derive(Debug)]
pub struct WebhookEngine {
    opts: WebhookOpts,
}

impl WebhookEngine {
    /// Construct an engine with the given tunables.
    #[must_use]
    pub fn new(opts: WebhookOpts) -> Self {
        Self { opts }
    }

    /// Drain due entries until the outbox is idle or `max_batches` passes have run, whichever comes
    /// first. Each entry is delivered to its endpoint and marked done / rescheduled / failed.
    pub async fn run_until_idle<M, S, C>(
        &self,
        meta: &M,
        sink: &S,
        clock: &C,
        max_batches: u32,
    ) -> Result<WebhookReport, MetaError>
    where
        M: MetadataStore + ?Sized,
        S: WebhookSink + ?Sized,
        C: Clock + ?Sized,
    {
        let mut report = WebhookReport::default();
        for _ in 0..max_batches {
            let now = clock.now();
            let batch = meta.claim_webhook_batch(self.opts.batch_size, now).await?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                self.deliver_one(meta, sink, clock, entry, &mut report)
                    .await?;
            }
        }
        Ok(report)
    }

    async fn deliver_one<M, S, C>(
        &self,
        meta: &M,
        sink: &S,
        clock: &C,
        entry: WebhookEntry,
        report: &mut WebhookReport,
    ) -> Result<(), MetaError>
    where
        M: MetadataStore + ?Sized,
        S: WebhookSink + ?Sized,
        C: Clock + ?Sized,
    {
        // Resolve the live endpoint (URL + secret) from the bucket's notification config. If the
        // subscription was removed since enqueue, drop the entry rather than retry forever.
        let endpoint = match self.resolve_endpoint(meta, &entry).await? {
            Some(ep) => ep,
            None => {
                meta.submit(Mutation::MarkWebhookDone(entry.id.clone()))
                    .await?;
                report.dropped += 1;
                return Ok(());
            }
        };

        let signature = endpoint
            .secret
            .as_deref()
            .map(|s| sign(s, entry.payload.as_bytes()));
        let result = sink
            .deliver(
                &endpoint.url,
                entry.payload.as_bytes(),
                signature.as_deref(),
            )
            .await;

        match result {
            Ok(()) => {
                meta.submit(Mutation::MarkWebhookDone(entry.id.clone()))
                    .await?;
                report.delivered += 1;
            }
            Err(err) => {
                let terminal = matches!(err, WebhookError::Terminal(_))
                    || entry.attempts + 1 >= self.opts.max_attempts;
                let next = if terminal {
                    None
                } else {
                    let delay = next_backoff(
                        entry.attempts + 1,
                        self.opts.base_backoff_secs,
                        self.opts.max_backoff_secs,
                    );
                    Some(Timestamp(clock.now().0 + delay as i64 * 1000))
                };
                if terminal {
                    tracing::warn!(entry = %entry.id, endpoint = %endpoint.id, error = %err, "webhook delivery failed terminally");
                }
                meta.submit(Mutation::MarkWebhookFailed {
                    id: entry.id.clone(),
                    error: err.to_string(),
                    next_attempt_at: next,
                })
                .await?;
                report.failed += 1;
            }
        }
        Ok(())
    }

    /// Look up the entry's endpoint in its bucket's current notification config.
    async fn resolve_endpoint<M>(
        &self,
        meta: &M,
        entry: &WebhookEntry,
    ) -> Result<Option<cairn_types::notification::WebhookEndpoint>, MetaError>
    where
        M: MetadataStore + ?Sized,
    {
        let doc = match meta
            .get_bucket_config(&entry.bucket, ConfigAspect::Notification)
            .await?
        {
            Some(doc) => doc,
            None => return Ok(None),
        };
        let config: NotificationConfig = match serde_json::from_str(&doc.0) {
            Ok(c) => c,
            // A corrupt config is treated as "no subscription" rather than retried forever.
            Err(_) => return Ok(None),
        };
        Ok(config
            .endpoints
            .into_iter()
            .find(|e| e.id == entry.endpoint_id))
    }
}

#[cfg(test)]
mod tests;
