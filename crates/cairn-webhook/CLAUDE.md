# cairn-webhook

The outbox-driven **webhook event-notification** delivery engine — the event-notification analogue
of `cairn-replication`. A background worker claims due `events_outbox` rows under a metadata-side
lease, resolves each row's `endpoint_id` against its bucket's live notification config (URL +
signing secret), POSTs the pre-rendered S3-event-record JSON — optionally HMAC-signed — and marks
the row done / rescheduled-with-backoff / terminally failed.

## Layout (`src/`)
- `lib.rs` — `WebhookEngine::run_until_idle` (the drain loop), `WebhookOpts`, `WebhookReport`,
  the pure `next_backoff`, and `sign` (hex HMAC-SHA256 of the body). `resolve_endpoint` reads the
  bucket's `ConfigAspect::Notification` and finds the entry's endpoint by `endpoint_id`.
- `sink.rs` — the `WebhookSink` trait + `HttpWebhookSink` (hyper/rustls POST). Maps the response to
  `WebhookError::Retryable` (5xx/408/429, transport error, timeout) vs `Terminal` (other 4xx, bad
  scheme/URL). `tests.rs` — engine tests over the `cairn-types` in-memory double + a recording sink.

## Notes
- **Pure delivery transport.** Payloads are rendered at *enqueue* time in `cairn-protocol`
  (`emit_event_notifications` → `Mutation::EnqueueWebhooks`, where object size/ETag are in hand) and
  stored verbatim on the row — this crate **never** touches the blob store, and never re-renders.
  Endpoint event/prefix/suffix matching also happens at enqueue; here we only look up URL + secret.
- **Best-effort at-least-once.** The enqueue rides *just after* the object commit, not inside it — a
  crash in the gap drops the notification, never the object; a delivery failure never fails the
  originating S3 op. Re-enqueue is idempotent: the row id is deterministic
  (`{bucket}:{endpoint}:{version}:{event}`).
- **Terminal vs retryable is load-bearing.** The engine parks a row as `failed` on a `Terminal`
  error **or** when `attempts + 1 >= max_attempts`; otherwise it reschedules with `next_backoff`.
  A removed/corrupt subscription → `Dropped` (marked done), not retried forever.
- **Signing.** `sign` returns the bare hex; the sink emits the header as `X-Cairn-Signature:
  sha256=<hex>`. Only present when the endpoint has a `secret`.
- **The 4(+1)-site rule applies.** `EnqueueWebhooks`, `MarkWebhookDone`, `MarkWebhookFailed`, and the
  `claim_webhook_batch` read are mirrored in `cairn-meta/apply.rs`, `cairn-meta-async/apply.rs`, and
  the in-memory double — keep them in lockstep. The claim lease lives in the metadata layer, not here.
- **Webhook-native, NOT S3 SNS/SQS/Lambda.** Per-bucket config is set via the management API
  (`PUT /api/v1/buckets/{name}/notifications`, `ConfigAspect::Notification`); the S3 `?notification`
  subresource stays `NotImplemented`.
- Driven by `webhook_loop` in `cairn-server/background.rs` (`CAIRN_WEBHOOK_INTERVAL_SECS`, default
  15; `max_batches = 50` per tick); within a batch deliveries run `buffer_unordered` at
  `max_concurrency` so one hung endpoint (bounded by the sink's 10s timeout) can't stall the outbox.
- Spec: `docs/replication.md` 20.6. See the root `../../CLAUDE.md` for the gate and conventions.
