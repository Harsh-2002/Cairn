# cairn-control

The JSON management API (`ControlService`) — the admin-gated control plane, distinct from the S3 data
plane. JSON over HTTP, path-versioned (`/api/v1`), consumed by both the web console and the CLI.

## Layout (`src/`)
- `service.rs` — the management handlers (overview, bucket/user/replication/activity management,
  share-token minting). `create_user` seals the user's SigV4 secret (a #29 sealed site).
- `wire.rs` — the JSON request/response DTOs. `lib.rs`.

## Notes
- Admin-gated: handlers check the principal's role; never expose key material or plaintext secrets.
- Served on the **web-UI listener** (`CAIRN_UI_ADDR`, :7374), not the S3 port.
- Spec: `docs/control-plane.md` (22). See the root `../../CLAUDE.md`.
