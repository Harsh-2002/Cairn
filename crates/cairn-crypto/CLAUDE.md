# cairn-crypto

The production cryptography facility: AES-256-GCM envelope encryption of secrets at rest (`Crypto`),
plus `Clock` and `PublicUrl`.

## Layout (`src/`)
- `system_crypto.rs` — `SystemCrypto`: the master-key **ring** + the `CRK1` versioned envelope,
  `seal`/`open`, the seal-count bound, and key rotation (audit #29).
- `base64.rs` / `clock.rs` / `public_url.rs`.

## Notes
- **Reads fail closed**: a missing/wrong key or tampered envelope returns an error — never plaintext,
  zeros, or partial data.
- Secrets are sealed at rest and are **never logged, echoed, or returned** by any endpoint.
- The ring/rotation is wired in `cairn-server` (`stack.rs build_crypto`, `key_rewrap.rs`); the
  operator runbook is `docs/operations.md`.
- Spec: `docs/security-errors.md` (27), `docs/configuration.md` (28). See the root `../../CLAUDE.md`.
