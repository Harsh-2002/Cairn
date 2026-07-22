# cairn-auth

Authentication only (ARCH 14): turns a `RequestView` into an `AuthOutcome` (authenticated
`Principal` / `Denied` / `NotApplicable`). It verifies *who* you are; **authorization (what you may
do) is `cairn-authz`** — this crate depends on it only to parse the identity policy it attaches.

## Layout (`src/`)
- `lib.rs` — `AuthChain` (the `Authenticator`). `classify()` is the ordered dispatch (SigV4 header →
  Bearer → SigV4 presigned → dev bypass); every success funnels through the single `authenticate()`
  → `attach_policy()` chokepoint that loads the per-user identity policy. Holds STS session
  *consumption* (`authenticate_session`, the temporary `CAIRNTMP…` credential) and STS *minting* auth
  (`authenticate_sts`) — a **deliberately separate** path: `expected_service = "sts"` (the generic
  chain hard-rejects a non-`s3` scope), the payload hash bound to the buffered form body, no dev
  bypass, no session chaining; returns the long-term principal WITHOUT attaching an identity policy.
- `sigv4.rs` — SigV4 canonicalization, signing, header + presigned verification, and `mint_presigned`
  (the signer reuses the verifier primitives, so a minted URL is what `aws s3 presign` produces).
  Header verification takes an `expected_service` (`"s3"` | `"sts"`) and a `payload_hash_override`
  (the STS path hashes the buffered form body itself).
- `bearer.rs` — `Bearer <id>.<secret>` parse + fast-hash; `hash_session_token`.
- `chunked.rs` — streaming chunk-signature primitives; the rolling chain is **verified by the ingest
  decoder in `cairn-protocol`**, seeded by the `ChunkSigningContext` `verify_header` returns.
- `cache.rs` — `AuthCache`: sealed-credential + parsed-policy memoization, epoch + TTL invalidated.
- `crypto_util.rs` — sha256/hmac/`uri_encode`/amz-date helpers shared by the SigV4 code.

## Invariants
- **Fail closed, always.** Bad/missing signature, unknown/inactive key, expired or unparseable
  session policy → `Denied`/no-grant, **never** a bypass or a silently widened principal.
- **The canonical URI is `uri_encode(percent_decode(path), encode_slash=false)` — keep it exactly.**
  It is what signs keys containing `(`, `)`, space correctly (warp-regression-tested). Don't
  "simplify" the decode-then-reencode.
- **All secret comparisons are constant-time** (`subtle::ct_eq` / `Crypto::ct_eq`). Never compare a
  signature/token/hash with `==`.
- **The plaintext SigV4 secret is never cached or stored** — only the sealed `(ciphertext, nonce)`.
  It is decrypted per request via `Crypto::open` (which returns a `Zeroizing` buffer) and the derived
  `String` is re-wrapped in `Zeroizing` (F-15); the signing key is re-derived each request so the
  verification math is identical whether the credential came from cache or DB.
- **Sessions (STS) are least-privilege and must stay that way.** A session principal carries the
  parent's identity (ownership/audit) but `is_session = true`, `role` capped to `Member`, and
  `attach_policy` **skips the parent-policy load** — a session is governed *solely* by its scoped
  inline policy. Removing either cap lets a session widen to the parent. (The owner/admin
  short-circuit suppression for sessions lives in `cairn-protocol`.)
- **The dev bypass is triple-gated:** compiled only under the `dev-auth` cargo feature (release builds
  omit it), AND `dev_enabled`, AND `view.source.is_loopback()`. Don't loosen any gate.

## Cache coherency (cache.rs)
Entries are tagged with a shared **auth epoch** (`AtomicU64` the metadata layer bumps on every
user-identity mutation) plus a TTL. `observe_epoch()` *before* the fetch, pass it to `put_*`; an
install is refused if the epoch advanced meanwhile (closes the TOCTOU window). So a deactivation /
policy change takes effect on the next request, not after the TTL. `ttl == 0` disables the cache
entirely (every lookup misses). A malformed stored policy is cached as a remembered absence (warn +
fail-closed) so a known-bad doc isn't re-parsed every request.

## Notes
- Pure crate: no filesystem, no DB writes. Reads creds/policy through `MetadataStore` and decrypts
  via `Crypto` — both `dyn` traits from `cairn-types`. New credential lookups land as shared reads
  (the 4(+1)-site rule, see the root `../../CLAUDE.md`).
- SigV4 service must be `s3` and `host` must be signed, else `Malformed`. Skew window is ±900s.
- Validated against the AWS `get-vanilla` vector; `mint_presigned` round-trips through
  `verify_presigned`; the chunk chain matches the documented streaming format (the AWS doc's
  *published* chunk signature is a known erratum — see the `chunked.rs` test note). End-to-end
  streaming is covered by the aws-sdk PUT in `conformance/`.
- Spec: `docs/auth.md` (14–15). Chain tests: `tests/chain.rs`. See the root `../../CLAUDE.md`.
