# cairn-server

The binary. Wires the concrete engine stack, runs the hyper/rustls server with ordered graceful
shutdown, and carries the node-local CLI commands.

## Layout (`src/`)
- `main.rs` — entrypoint + CLI subcommands: `serve` (default), `bootstrap`, `validate-config`,
  `integrity [--repair]`. `cli_remote.rs` — the remote-admin client subcommands.
- `config.rs` — **the `CAIRN_*` env config** (strict Figment, `deny_unknown_fields`). Add new knobs
  here with a doc comment AND validation.
- `stack.rs` — the ONLY place naming concrete impls: `build()` wires everything, `build_crypto` (the
  key ring), and the #29 startup `enforce_retire_gate`.
- `server.rs` — hyper/rustls, routing, the concurrency limiter, `/healthz` `/readyz` `/metrics`.
- `background.rs` — the background loops (sweeper, lifecycle, WAL checkpointer, replication, #29
  re-wrap + counter-sync). `adapter.rs` — the management-API adapter + `crypto-status`.
- `fast_get.rs` / `sendfile.rs` — the sendfile fast path. `tls.rs` — TLS + SIGHUP reload.
  `key_rewrap.rs` — the #29 re-wrap worker. `metrics_agg.rs` / `observability.rs`.

## Notes
- Two listeners: S3 on `CAIRN_LISTEN_ADDR` (:7373), console+API on `CAIRN_UI_ADDR` (:7374).
- Spec: `docs/configuration.md` (28), `docs/control-plane.md` (22-24), `docs/delivery.md` (31).
- See the root `../../CLAUDE.md` for the gate and workspace-wide rules.
