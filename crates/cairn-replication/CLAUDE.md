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

## Notes
- `HttpS3Sink` buffers the whole body in memory to hash it for the signed-payload PUT; streaming
  `UNSIGNED-PAYLOAD` is a future extension. It does NOT implement `ReplicationSink` (only
  `BucketRoutedSink`) so the route.rs blanket impl stays coherent.
- `backfill_outbox_entries` stamps `BACKFILL_PLACEHOLDER_BUCKET` — the caller **must** substitute
  the real source bucket before committing.
- `insecure_skip_verify` defeats TLS auth (testing only); mutually exclusive with a custom CA.
- Tests: `tests/gate.rs` (engine), `tests/sink_http.rs` (real mock server). Fault injection:
  `conformance/replication_chaos.sh`; two-node soak: `conformance/soak.sh`.
- Spec: `docs/replication.md` (20). Env knobs/wiring: `cairn-server` `config.rs`/`background.rs`.
  See the root `../../CLAUDE.md` for the gate.
