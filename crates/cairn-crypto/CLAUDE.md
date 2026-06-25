# cairn-crypto

The production cryptography facility. Realises **three frozen `cairn-types` traits** with vetted
primitives only (no hand-rolled crypto): `Crypto` (AES-256-GCM envelope encryption of secrets at
rest), `Clock` (OS wall clock), `PublicUrl` (HMAC-SHA256 signed public-read URLs). Depends on
`cairn-types` and crypto crates only — **no engine, no I/O, no `unsafe`** (`#![forbid(unsafe_code)]`).

## Layout (`src/`)
- `system_crypto.rs` — `SystemCrypto: Crypto`: the master-key **ring**, the `CRK1` versioned
  envelope, `seal`/`open`, the per-key seal-count bound, the `from_*` constructors, `needs_rewrap`.
- `public_url.rs` — `HmacPublicUrl: PublicUrl`: HMAC over `method "\n" path "\n" expiry-millis`;
  derives its key from the master key via a domain-separated PRF (`from_master_key`), or
  `ephemeral()` per-process when none is configured.
- `clock.rs` — `SystemClock: Clock`: `now()` as saturating Unix-millis `Timestamp`.
- `base64.rs` — a tiny vendored RFC-4648 codec for the `from_base64` key constructor only; the
  workspace deliberately does not pull a base64 crate. `encode` is `cfg(test)`.

## Invariants (get these right)
- **Crypto fails closed.** Missing/wrong key, unknown key id, or any tag/AAD mismatch returns
  `CryptoError::Decrypt` — **never plaintext, zeros, partial data, or a fallback to another key**.
  Tampering the `CRK1` magic routes a blob to the legacy path, where it also fails.
- **`open` returns `Zeroizing<Vec<u8>>`** (F-15): the secret is scrubbed on drop. Don't copy it out
  into a plain `Vec`/`String` — that defeats the scrubbing. The whole `Crypto::open` chain carries
  `Zeroizing` end to end. Likewise key buffers are zeroized once a cipher is built.
- **`CRK1` envelope** = `magic ‖ key_id(2, BE) ‖ nonce(12) ‖ ct‖tag`; `magic ‖ key_id` is bound as
  GCM **AAD**, so repointing the key id fails auth. `seal` uses the **active** key + a fresh random
  96-bit nonce (same plaintext → distinct ciphertexts). `open` routes by magic to the named ring
  key; a legacy (pre-#29, no-magic) blob uses `legacy_id` and the **separate** `nonce`.
- **`Sealed.nonce` is redundant for `CRK1`** (the nonce lives inside `ciphertext`). It's populated
  only for source-compat — **MUST NOT be persisted to a separate column**; store it empty/NULL.
- **Seal bound, not open bound.** Per active key, `seal` warns once at 75% and **hard-stops with
  `KeyRotationRequired` at 95%** of the `2^32` GCM random-nonce ceiling. `open` is **never** blocked.
- **Never log/echo/return secrets or key material.** `Debug` for `SystemCrypto`/`HmacPublicUrl`
  redacts; keep it that way.

## Notes
- The ring + key rotation (audit #29) is wired in `cairn-server`: `stack.rs build_crypto` /
  `prime_seal_count`, `key_rewrap.rs` (re-wrap loop + `needs_rewrap` + durable seal-count sync).
  `SystemCrypto` is `Arc`-shared; `prime_seal_count` takes `&self` so the durable base is set once
  before the listener binds. Single-key dev/test path goes through `SystemCrypto::new` (id 1).
- `HmacPublicUrl` keys off the raw master-key **bytes** via an HMAC PRF, not the hex string — same
  bytes → same signer across restarts; a different key never verifies.
- Tests pair against the `cairn-types::testing` doubles (`StubCrypto`, `StubPublicUrl`, `TestClock`)
  to prove the real impls are object-safe and contract-compatible (`tests/traits.rs`); per-module
  `#[cfg(test)]` units cover the envelope shape, AAD binding, fail-closed paths, and the seal bound.
- Spec: `docs/metadata.md` (12.6), `docs/security-errors.md` (27); config knobs (`CAIRN_MASTER_KEY`,
  `CAIRN_MASTER_KEY_RING`) in `docs/configuration.md` (28); rotation runbook `docs/operations.md`.
  See the root `../../CLAUDE.md` for the gate and the 4(+1)-site / append-only rules.
