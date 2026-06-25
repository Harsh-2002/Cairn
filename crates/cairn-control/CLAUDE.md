# cairn-control

The JSON management API (`ControlService`) — the admin-gated control plane, distinct from the S3 data
plane. JSON over HTTP, path-versioned (`/api/v1`), consumed by both the embedded web console and the
CLI. Written purely against the trait spine; no backend of its own (unit-tested vs. `cairn-types`
in-memory doubles).

## Layout (`src/`)
- `service.rs` — `ControlService` and every handler. `handle()` stamps an `x-amz-request-id` then
  delegates to `route()`, a single `(Method, &[segment])` match over the whole `/api/v1` surface
  (overview/system/metrics, buckets + per-bucket config aspects, replication targets, users,
  temporary STS credentials, object-share list/revoke, tag browsing, activity). Also `ControlResponse`
  (status + JSON body) and `SystemInfo` (config snapshot taken once at startup).
- `wire.rs` — the serde request/response DTOs and small parsers (`parse_role`, `parse_versioning`, …);
  the JSON contract lives here, not in `service.rs`.

## Invariants & rules
- **`route()` is the authz choke point.** `/health` is the *only* unauthenticated endpoint; every
  other branch is gated by `is_admin(principal)` (role `Administrator`) **before** the match — a 403
  `forbidden`. A new endpoint added to the match is admin-gated for free; **never** add an auth check
  that bypasses this gate, and don't add a second unauthenticated path.
- **Secrets are write-once or never.** Plaintext SigV4/Bearer/session secrets appear in a response
  **only** at mint time (`create_user`, `rotate_credentials`, `mint_session_credential`). SigV4 secrets
  are sealed via `crypto.seal` (CRK1 envelope — nonce lives inside the ciphertext, store NULL `nonce`,
  audit #29); only the *hash* of a Bearer secret is persisted. GET/list endpoints return a presence
  flag, **never** the value (e.g. notification HMAC secrets). Never log or echo key material.
- **Every mutating handler calls `record_activity(action, bucket, key, principal)`** after the write,
  stamping `actor = principal.access_key_id` — this is the audit trail the console reads. Keep the
  action string and emit it on success.
- **Self-lockout / break-glass guards** (in `delete_user`/`patch_user`): can't delete the identity
  you're signed in as; the root admin (`with_root_access_key`, re-seeded every startup) is undeletable;
  never remove the last active administrator. Preserve all three when touching user mutations.
- **All listing is bounded** by `PAGE_LIMIT` (1000) and `delete_prefix` caps its error list (audit #26)
  — a hostile cursor or huge prefix can never spin forever or OOM. Don't introduce an unbounded loop.

## Contract / how it fits
- Depends on `cairn-types` (traits + domain types), `cairn-auth` (secret hashing), `cairn-authz`
  (`parse_policy`/`parse_user_policy` — bucket and user policy bodies are validated before they hit the
  store), and `cairn-replication` (canned destination policy). It does **not** depend on a metadata or
  blob backend.
- All writes go through `meta.submit(Mutation::…)` — the single `Writer`. Adding a `Mutation` is the
  4(+1)-site rule (see the root `../../CLAUDE.md`); this crate is only the *caller*.
- Object-**share minting** lives in the server adapter (it streams bytes on redemption); only share
  list/get/revoke — pure metadata ops — live here (ARCH 15.8).
- `with_replication_wake` pulses the replication pool after operator actions (resync/retry/target
  edits); `tokio` is pulled only for the `rt` spawn handle used by the resync backfill.

## Notes
- Served on the **web-UI listener** (`CAIRN_UI_ADDR`, :7374), wired in `cairn-server`, **not** the S3
  port. `/health` here is the console probe — distinct from the S3-plane `/healthz`/`/readyz`.
- `rustix` (`cfg(unix)`) gives the `statvfs` disk figures for `GET /system` — the crate forbids
  `unsafe`, so no raw `libc`. `SystemInfo` is a startup snapshot; the service never re-reads config.
- `request_metrics` converts the store's epoch **seconds** to **milliseconds** (`ts_ms`) for the UI.
- Spec: `docs/control-plane.md` (ARCH 22); request-id/error envelope ARCH 25.1, readiness ARCH 26.4.
  Tests: `tests/gate.rs` (the admin-gate, lifecycle, and secret-once contract). See the root
  `../../CLAUDE.md`.
