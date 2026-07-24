# cairn-server

The binary (`cairn`, `default-run`). Wires the concrete engine stack, runs the hyper/rustls server
with ordered graceful shutdown and the background loops, owns the `CAIRN_*` config, and carries the
CLI. This is the **only crate that names concrete impls** — everything else is trait-generic.

## Layout (`src/`)
- `main.rs` — entrypoint + the `Command` enum (clap). Node-local commands operate on the data dir
  from config: `serve` (default), `validate-config`, `bootstrap`, `integrity [--repair]`, `migrate`,
  `backup <dir>`, `restore <dir>`. Remote-admin commands (`bucket`/`user`/`replication`/`object`/
  `share`/`overview`, ARCH 24.2) are a thin HTTP client — dispatched **before** `Config::load()`,
  they never touch the local data dir. **One exception**: `replication audit` is NODE-LOCAL (a
  dispatch guard lets it fall through to `Config::load()`), because it reads the durable version-row
  ledger no API exposes; `--verify` additionally re-derives each source plaintext MD5 and GETs the
  replica. See `replication_audit.rs`.
- `replication_audit.rs` — the source-side enumeration of **encrypted + terminally replicated +
  created before an explicit cutoff** versions: the population the pre-release-X plaintext seam
  damaged (ARCH 20.5). Shared by the CLI and by `background.rs`'s suspect gauges. Reads
  `object_versions.replication_status`, NOT the outbox — the outbox is pruned at
  `CAIRN_REPLICATION_RETENTION_SECS` and lies by omission. **The cutoff is mandatory and never
  defaulted** (`--before` / `CAIRN_REPLICATION_AUDIT_BEFORE`, parsed by `parse_cutoff`): a version
  encrypted and replicated *after* the fix is equally encrypted and equally `completed`, so an
  unbounded predicate counts healthy replicas forever and can never converge. The cutoff applies to
  **two** clocks: `created_at` AND `replicated_at` (schema v23, stamped by `MarkReplicationDone`) —
  `created_at` alone cannot converge either, since it is never rewritten, so a REPAIRED version would
  match forever and the gauge would return to its pre-repair value *because* the repair worked. A
  NULL `replicated_at` (never shipped, or a pre-v23 row) counts as suspect: an upgraded node
  over-reports once and re-ships, which is the right side to err on. `repair_pending`
  (in-window `pending`/`claimed`) is reported separately because a forced requeue empties the suspect
  count *before* any byte re-ships. Convergence is `repair_pending == 0` AND `present_and_suspect ==
  non_current_suspect` — NOT both at zero: the requeue deliberately leaves a non-current version
  whose outbox row was pruned stamped `completed` (nothing can ship it, so `pending` would pin this
  gauge above zero forever), and TRAP 2 already says those are unrepairable in place. That floor is
  published as its own gauge so the alert can subtract it. `--verify` skips non-current
  versions: its GET carries no `versionId`, so comparing one against the destination's current object
  manufactures false `Mismatched` verdicts.
- `config.rs` — **the `CAIRN_*` env config** (strict Figment, `default` + `deny_unknown_fields`).
  `Config::default()` overlaid with env; `validate()` fails fast on load. Add a knob here with a doc
  comment AND validation (ARCH 28). `ReplicationTarget`, `LogFormat` live here too. SSE/STS knobs:
  `CAIRN_STS_ENABLED` (opt-out, default on), `CAIRN_ENCRYPT_AT_REST` (opt-in transparent at-rest),
  `CAIRN_KMS_KEY_IDS` (comma-separated SSE-KMS allow-list → `parse_kms_key_ids`; unset = accept-all),
  `CAIRN_REPLICATION_AUDIT_BEFORE` (the encrypted-suspect audit cutoff; unset = the loop is off).
- `stack.rs` — `build()` assembles `AppStack`; `open_meta` honours `CAIRN_META_BACKEND`
  (`sqlite`|`libsql`|`turso`) and `CAIRN_META_SHARDS`; `build_crypto` builds the key ring;
  `enforce_retire_gate` is the #29 startup retire check. Wires the S3 service's SSE `KeyProvider`
  (`cairn_protocol::LocalRingProvider` from `CAIRN_KMS_KEY_IDS`) and `with_encrypt_at_rest`.
- `sts.rs` — the **AWS-STS wire surface** (ARCH 14): `Action=AssumeRole` / `Action=GetSessionToken`
  as a form `POST /` on the S3 data-plane port, returning AWS-STS XML. A dedicated `sts`-scoped
  SigV4 verification (`AuthChain::authenticate_sts`, no dev bypass) mints a `CAIRNTMP…` session over
  `Mutation::CreateSessionCredential`; sessions stay least-privilege (never broader than the caller).
- `server.rs` — the accept/serve loops, the outer middleware (request id, span, concurrency
  `Semaphore`, timeout), graceful shutdown, and `/healthz` `/readyz` `/metrics`. `serve_web` picks a
  listener's role (S3-only vs. console+API).
- `adapter.rs` — hyper ⇄ `S3Request`/`S3Response`; this is where **authentication** runs and
  path-style addressing routes into the S3 service; also the management-API adapter + `crypto-status`.
- `background.rs` — `spawn()` starts the loops: multipart sweeper, lifecycle scanner, webhook,
  integrity scrub, WAL checkpointer, replication worker pool, the #29 re-wrap + counter-sync, and the
  **opt-in, 6-hourly** encrypted-suspect audit pass — spawned only when `CAIRN_REPLICATION_AUDIT_BEFORE`
  is set (unset = the loop never runs, no gauge, no warn, zero cost), and deliberately NOT in
  `metrics_loop`: it is a version-row walk costing one point query per version in a replicating
  bucket, and `replication_status` has no index — a scrape must never trigger it.
- `metrics_agg.rs` — sharded in-process request-metrics aggregator (zero DB I/O on the hot path;
  batched flush through the single writer). `observability.rs` — tracing + Prometheus recorder.
- `key_rewrap.rs` — the #29 re-wrap worker (sqlite-only, one per shard); reseals the DEK onto the
  active key while **flatten-preserving** the SSE descriptor's additive `mode`/`kms_key_id` labels,
  so a rotation never drops them (an at-rest object must not silently start advertising `AES256`).
  `sse.rs` — the console SSE pulse channel + ticket mint. `tls.rs` — TLS load + SIGHUP reload.
- `fast_get.rs` / `sendfile.rs` — the plaintext `sendfile(2)` GET fast path; **`#[cfg(all(feature =
  "fast-io", target_os = "linux"))]` only** (modules absent otherwise).
- `import_run.rs` — the background S3-import worker: a single claimer reclaims orphaned (crashed,
  stale-lease) jobs at startup, then drains pending jobs, running the `cairn-import` engine and
  persisting per-bucket progress/cursors as the resumable checkpoint. `import_dest.rs` —
  `LocalDestWriter`: lands imported objects through the real `S3Service::handle` (a trusted
  root-admin principal, replaying metadata/tags as request headers) so encryption, compression,
  quota, versioning, and events all apply exactly as a normal upload.

## Notes
- **Two listeners.** S3 + `/share/…` shares + `/healthz` `/readyz` `/metrics` on `CAIRN_LISTEN_ADDR`
  (:7373); console + `/api/v1` + SSE on `CAIRN_WEB_ADDR` (:7374; `off`/`none` for headless). Infra
  endpoints answer **ahead of the concurrency limiter** so a probe/scrape never sheds.
- **Crypto fails closed; `unsafe` is forbidden.** `#![forbid(unsafe_code)]` on every build except
  `fast-io`, which relaxes to `deny` for the SAFETY-commented syscall blocks. Never widen this.
- **Startup refuses to bind a public address with insecure dev defaults** (built-in dev master key
  or default root secret) unless `CAIRN_ALLOW_INSECURE=true` — a hurried deploy cannot come up fully
  functional and fully insecure. Keep this guard.
- **Readiness gates real traffic.** `/readyz` stays false until migrations + reconciliation finish
  (the durability ordering in ARCH 8 — don't reorder). Shutdown is signal → stop claiming new
  replication work → drain connections; an aborted in-flight ship is safe (outbox re-leases).
- Two features change the link/build, not behaviour: `meta-async` links the libSQL/Turso backends
  (and triggers a `-z muldefs` workaround in `build.rs` for the dual-bundled-SQLite collision);
  `fast-io` is glibc-Linux-only (`ktls` won't build for aarch64-musl). The **shipped static-musl
  release is default-features** — neither is on.
- Config tests use figment's `Jail` (serialized, hermetic env). A restricted sandbox can't bind
  listen sockets — run live/e2e with the sandbox disabled.

## Pointers
- Spec: `docs/configuration.md` (28), `docs/control-plane.md` (22–24), `docs/data-plane.md` (6–7),
  `docs/delivery.md` (31). #29 key-rotation runbook: `docs/operations.md`.
- See the root `../../CLAUDE.md` for the gate, the 4(+1)-site mutation rule, and workspace invariants.
