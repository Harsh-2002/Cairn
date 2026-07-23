# cairn-replication

The outbox-driven asynchronous bucket-replication engine (ARCH 20): eventually consistent,
at-least-once, idempotent. A durable outbox in the `MetadataStore` records what remains to ship;
this engine drains it. It holds no mutable state — the outbox is the source of truth, so an engine
is cheap to construct and safe to run from many workers at once.

## Layout (`src/`)
- `lib.rs` — `ReplicationEngine`. `run_once` claims a batch of *due* entries, groups by
  `(bucket, key, target_arn)`, ships each via the router, and records the result on the outbox.
  Also `outbox_entry_for`/`backfill_outbox_entries` (pure builders the write/control plane attach to
  a `PutObjectVersion` commit) and `ReplicationOpts` (batch/attempts/backoff tunables).
- `sink.rs` — `HttpS3Sink`: the production sink; SigV4-signed `PUT`/`DELETE` to a remote
  S3-compatible endpoint over http **or** https (one `https_or_http` connector). Owns error
  classification, per-target TLS trust, per-source→dest bucket routing, and `sink_for_target`.
  `stream_object` is the **operator-audit-only** read-back (`cairn replication audit --verify`): the
  drain path never reads what it wrote, and it exists because a corrupt replica is exactly the right
  size and the ETag differs even for a correct one — only the bytes distinguish them. It streams the
  body frame-by-frame into a caller-supplied digest and holds none of it (an audit must never be the
  most likely thing to OOM the node); its `max_bytes` bounds the total bytes **read**, not a buffer,
  so a hostile/endless body still terminates.
- `route.rs` — `BucketRoutedSink` (the engine's sink boundary — threads the *source* bucket into
  every call) and `SinkRouter`/`SingleSink`. A blanket impl makes any `ReplicationSink` (e.g. the
  test double) a `BucketRoutedSink`.
- `target.rs` — `RemoteTarget` (per-bucket remote target, ARN + **sealed** secret) and
  `seal_target`/`open_target`/`parse_targets`/`resolve_target`. The replication secret-at-rest seam.
- `config.rs` — the S3 `<ReplicationConfiguration>` XML **parser** + `ReplicationRule`/`Filter`/
  `Destination` types. NOT env config — the `CAIRN_REPLICATION_*` knobs live in `cairn-server`.
- `backoff.rs` — `next_backoff`: pure deterministic exponential backoff.

## Invariants & rules
- **Loop prevention.** A version whose `replication_status == Replica` (it arrived here *via*
  replication) is **never** re-shipped — drained without touching the sink. On the wire, every ship
  carries `x-amz-meta-cairn-replica: true` so the destination marks it `Replica` too. NEVER remove
  the marker or the status guard — they break the only cycle-breaker.
- **Do NOT skip on version-level `Completed`.** Under 1→N fan-out a version has one outbox entry
  *per target*; the first target to finish stamps the version `Completed`. Per-target idempotency is
  the durable claim's job (a `completed` entry is never re-claimed), not a status check — see the
  long comment at `process_entry`.
- **Per-key, per-target ordering.** A key's versions ship strictly oldest-first to a given target
  (version ids are uuid-v7, so ascending string order is chronological). A stalled earlier version
  blocks later ones **for that target only** — within a batch via the `blocked` flag, *across*
  batches via `has_unreplicated_predecessor`. A *terminal* failure does NOT block successors
  (at-least-once, best-effort); a retry/deferral does.
- **Three error classes drive three outcomes** (`ReplicationError`): `Retryable` → backoff and burn
  one attempt, terminal after `max_attempts`; `Unavailable` (target down: transport error, 5xx,
  408, 429) → reschedule at a bounded cadence **without** burning the attempt budget, so an extended
  outage auto-resumes; `Terminal` (4xx, missing blob/version) → fail immediately for operator
  attention. Mis-classifying down-vs-rejected either exhausts a recoverable queue or retries a dead
  request forever.
- **Ordering deferral is not a failure** — `DeferReplication` reschedules without incrementing
  `attempts`.
- **Ship PLAINTEXT: resolve the version's DEK before reading its body.** `resolve_dek` unseals
  `row.sse_descriptor` via `cairn_types::sse::open_dek` and `put_object` reads through
  `open_with_dek`. Reading an
  encrypted version with `dek: None` returns the stored **ciphertext at exactly the plaintext
  length** — the destination cannot tell (it has no Content-MD5 to check, and a multipart source's
  composite ETag is unverifiable), so the mirror ends up holding intact-looking garbage. Never call
  the DEK-less `open` here. Resolve per read; never cache a DEK across passes (the re-wrap worker
  re-seals descriptors underneath us).
- **A DEK failure is a LOCAL condition and must say so.** DEK resolution happens in `process_entry`
  (not inside `put_object`) precisely so the failure is classified with its cause statically known:
  `reschedule_unavailable` takes an `UnavailableCause`, and the SourceKey arm logs/stamps "source
  data key unavailable on this node's master ring" instead of "target unavailable" — the destination
  is healthy, and blaming it pages the wrong system. Because `Unavailable` never consumes the attempt
  budget, a PERMANENTLY removed key id retries forever and never reaches `failed`; each reschedule is
  counted into `RunReport::dek_resolve_failures`, which `cairn-server` emits as
  `cairn_replication_dek_resolve_failed_total`. That counter is the only signal such objects exist —
  don't drop it.
- **Client-encrypted objects are gated on a plaintext endpoint.** `ReplicatedObject.client_encrypted`
  is true only for `SseMode::SseS3`/`Kms` (client-requested), never for `AtRest` (an operator storage
  property). `HttpS3Sink` refuses such an object on an `http://` endpoint unless
  `allow_plaintext_sse_over_http` (`CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP`, default off).
  The refusal is `Unavailable`, NOT `Terminal` — an operator-fixable config condition must not burn
  the attempt budget or stamp a bucket failed; the object ships by itself once the endpoint is https.
  This exposure is created BY the DEK fix: before it, such an object never replicated or replicated
  as ciphertext.
- **Key-failure classification is part of the fix.** `CryptoError::UnknownKeyId`/`Key` →
  `Unavailable` (a rotation window must not consume the attempt budget and stamp a whole bucket
  failed); a tampered envelope or a malformed descriptor → `Terminal`. `BlobError::Corruption` is
  `Terminal` too — no retry count changes what is on disk. On every one of these paths the sink is
  never contacted, so nothing leaks on the error path.
- **Target secrets are sealed at rest.** `seal_target`/`open_target` go through `Crypto`; the
  plaintext lives only in a `Zeroizing` buffer. NEVER log, persist, or return the unsealed secret.
  Fails closed — wrong key / tampered ciphertext is `Terminal`, never plaintext (#29 sealed site).

## Contract
- Generic over the trait spine (`MetadataStore`, `BlobStore`, `Clock`, sink) — exercised entirely
  against the in-memory doubles in tests. Depends on `cairn-types`/`cairn-auth`/`cairn-crypto`; no
  metadata/blob engine.
- All outbox state changes are `Mutation`s submitted to the store (`MarkReplicationDone`,
  `MarkReplicationFailed`, `DeferReplication`, plus `PutObjectVersion` to stamp status) — they ride
  the single `Writer` and obey the 4(+1)-site rule. This crate adds no mutation of its own here, but
  honour that rule if you do.
- **The outbox is a work queue, not the incident ledger.** `PruneReplicationOutbox` deletes
  `completed` AND `failed` rows past `CAIRN_REPLICATION_RETENTION_SECS` (default 24 h), so the
  failed-entry API answers "all clear" for anything older. The durable per-object ledger is
  `object_versions.replication_status`. Repair of a *wrongly-completed* version therefore needs
  `Mutation::RequeueReplicationVersions` (`cairn-control`'s `resync?force=true`): the deterministic
  `backfill:{rule}:{key}:{version}` id means `EnqueueReplication`'s `INSERT OR IGNORE` cannot
  resurrect a terminal row. That requeue is **key-scoped, never version-scoped** — re-shipping one
  version of a key while its siblings stay settled lands an older version *after* a newer one
  (reverting the mirror's current object) and never re-ships a delete marker (resurrecting a deleted
  object); with the whole key queued, `has_unreplicated_predecessor` restores the ordering. It is
  also **paged by key with a forward cursor**, never by row — a row page is served in index order
  (all `completed` before all `failed`) and would split a key across batches, which is the same
  ordering bug one layer down. Its two halves have different scopes on purpose: the outbox half moves
  every terminal row of a paged key, the **ledger** half only rows that are `is_latest` or still have
  an outbox row. A non-current version whose outbox row was pruned can be shipped by nothing, so
  stamping it `pending` would make the ledger claim queued work no queue holds and pin the audit's
  `repair_pending` gauge above zero forever. `MarkReplicationDone` additionally stamps
  `object_versions.replicated_at` (schema v23) with the clock read at **ship completion**, not at
  batch claim; that stamp is what lets the audit tell a repaired
  version from a pre-fix one, and it is never `updated_at` (the client-visible S3 `LastModified`).
  Detection lives in `cairn-server/src/replication_audit.rs`; the runbook is `docs/operations.md` 8.7.
- **A 404 from the destination is `ReplicationError::NotFound`, not `Terminal`.** Both are terminal
  for the engine; the split exists so the operator audit can tell "the replica never landed" from
  "it landed wrong" *structurally*. The terminal message embeds the destination's response body, so
  any `msg.contains("404")` probe misfires on a 4xx whose XML happens to carry those digits. Never
  reintroduce string-sniffing on these messages.

## Notes
- `HttpS3Sink` buffers the whole body in memory to hash it for the signed-payload PUT; streaming
  `UNSIGNED-PAYLOAD` is a future extension. It does NOT implement `ReplicationSink` (only
  `BucketRoutedSink`) so the route.rs blanket impl stays coherent.
- `backfill_outbox_entries` stamps `BACKFILL_PLACEHOLDER_BUCKET` — the caller **must** substitute
  the real source bucket before committing.
- `insecure_skip_verify` defeats TLS auth (testing only); mutually exclusive with a custom CA.
- Tests: `tests/gate.rs` (engine), `tests/sink_http.rs` (real mock server). The two-node
  `conformance/stress_replication.py` SSE arm is the GATED regression guard for the DEK fix. Fault injection:
  `conformance/replication_chaos.sh`; two-node soak: `conformance/soak.sh`.
- Spec: `docs/replication.md` (20). Env knobs/wiring: `cairn-server` `config.rs`/`background.rs`.
  See the root `../../CLAUDE.md` for the gate.
