# cairn-webhook

The outbox-driven **webhook event-notification** delivery engine — the event-notification analogue
of `cairn-replication`. A background worker claims due `events_outbox` rows under a lease, resolves
each row's endpoint against its bucket's notification config, and POSTs the pre-rendered S3-event
JSON (optionally HMAC-signed) with retry/backoff.

## Layout (`src/`)
- `lib.rs` — `WebhookEngine` (`run_until_idle`), `WebhookOpts`, `next_backoff`, `sign` (HMAC-SHA256).
- `sink.rs` — the `WebhookSink` trait + `HttpWebhookSink` (hyper/rustls POST; 2xx done, 5xx/408/429
  retry, other 4xx terminal). `tests.rs` — engine tests against the in-memory double + a recording sink.

## Notes
- Pure delivery: payloads are rendered at **enqueue** time in `cairn-protocol` (where size/ETag are
  in hand) and stored on the row, so this crate never touches the object store.
- Best-effort at-least-once: the enqueue rides just after the object commit, not inside it — a crash
  in the gap drops the notification, never the object.
- Webhook-native, NOT S3 SNS/SQS. Per-bucket config is set via the management API
  (`ConfigAspect::Notification`); the S3 `?notification` subresource stays `NotImplemented`.
- Spec: `docs/replication.md` (20.6). See the root `../../CLAUDE.md`.
