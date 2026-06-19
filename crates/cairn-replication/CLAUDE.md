# cairn-replication

The outbox-driven asynchronous bucket-replication engine: eventually consistent, at-least-once,
idempotent.

## Layout (`src/`)
- `lib.rs` — the engine: claim a batch of *due* outbox entries (lease), enforce **per-key ordering**
  (an earlier un-replicated version blocks a later one across drains), retry with backoff, mark
  Completed/Failed.
- `sink.rs` — `HttpS3Sink`: a real SigV4-signing S3 client. 5xx = retryable, 4xx = terminal.
- `backoff.rs` — deterministic exponential backoff. `route.rs` — per-source-bucket destination.
- `target.rs` — `RemoteTarget` (the seal/open of replication-target secrets, a #29 sealed site).
- `config.rs` — single (`CAIRN_REPLICATION_*`) and multi-target (`CAIRN_REPLICATION_TARGETS`) config.

## Notes
- A claimed entry whose lease expires is re-claimed (crash recovery).
- Tests: `tests/gate.rs` (engine), `tests/sink_http.rs` (real mock server). Fault injection:
  `conformance/replication_chaos.sh`; happy-path soak: `conformance/soak.sh`.
- Spec: `docs/replication.md` (20). See the root `../../CLAUDE.md`.
