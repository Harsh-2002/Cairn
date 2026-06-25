# cairn-server

The binary (`cairn`, `default-run`). Wires the concrete engine stack, runs the hyper/rustls server
with ordered graceful shutdown and the background loops, owns the `CAIRN_*` config, and carries the
CLI. This is the **only crate that names concrete impls** — everything else is trait-generic.

## Layout (`src/`)
- `main.rs` — entrypoint + the `Command` enum (clap). Node-local commands operate on the data dir
  from config: `serve` (default), `validate-config`, `bootstrap`, `integrity [--repair]`, `migrate`,
  `backup <dir>`, `restore <dir>`. Remote-admin commands (`bucket`/`user`/`replication`/`object`/
  `share`/`overview`, ARCH 24.2) are a thin HTTP client — dispatched **before** `Config::load()`,
  they never touch the local data dir.
- `config.rs` — **the `CAIRN_*` env config** (strict Figment, `default` + `deny_unknown_fields`).
  `Config::default()` overlaid with env; `validate()` fails fast on load. Add a knob here with a doc
  comment AND validation (ARCH 28). `ReplicationTarget`, `LogFormat` live here too.
- `stack.rs` — `build()` assembles `AppStack`; `open_meta` honours `CAIRN_META_BACKEND`
  (`sqlite`|`libsql`|`turso`) and `CAIRN_META_SHARDS`; `build_crypto` builds the key ring;
  `enforce_retire_gate` is the #29 startup retire check.
- `server.rs` — the accept/serve loops, the outer middleware (request id, span, concurrency
  `Semaphore`, timeout), graceful shutdown, and `/healthz` `/readyz` `/metrics`. `serve_ui` picks a
  listener's role (S3-only vs. console+API).
- `adapter.rs` — hyper ⇄ `S3Request`/`S3Response`; this is where **authentication** runs and
  path-style addressing routes into the S3 service; also the management-API adapter + `crypto-status`.
- `background.rs` — `spawn()` starts the loops: multipart sweeper, lifecycle scanner, webhook,
  integrity scrub, WAL checkpointer, replication worker pool, and the #29 re-wrap + counter-sync.
- `metrics_agg.rs` — sharded in-process request-metrics aggregator (zero DB I/O on the hot path;
  batched flush through the single writer). `observability.rs` — tracing + Prometheus recorder.
- `key_rewrap.rs` — the #29 re-wrap worker (sqlite-only, one per shard). `sse.rs` — the console SSE
  pulse channel + ticket mint. `tls.rs` — TLS load + SIGHUP reload.
- `fast_get.rs` / `sendfile.rs` — the plaintext `sendfile(2)` GET fast path; **`#[cfg(all(feature =
  "fast-io", target_os = "linux"))]` only** (modules absent otherwise).

## Notes
- **Two listeners.** S3 + `/p/…` shares + `/healthz` `/readyz` `/metrics` on `CAIRN_LISTEN_ADDR`
  (:7373); console + `/api/v1` + SSE on `CAIRN_UI_ADDR` (:7374; `off`/`none` for headless). Infra
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
