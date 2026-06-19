# cairn-auth

The authenticator chain (`Authenticator`): an ordered chain of Bearer, SigV4 header, SigV4
presigned, and (debug-only) dev schemes; the first applicable outcome decides.

## Layout (`src/`)
- `sigv4.rs` — SigV4 verification. The canonical URI is `uri_encode(percent_decode(path))` — keep it
  exactly; it is what makes keys with `(`, `)`, space sign correctly (regression-tested by warp).
- `bearer.rs` — the first-party Bearer scheme. `chunked.rs` — streaming chunk-signature primitives.
- `cache.rs` — the auth cache (credential + parsed-policy memoization, epoch-invalidated).

## Notes
- Auth **fails closed**: a bad/missing signature or unknown key is denied, never bypassed.
- Validated against the AWS `get-vanilla` SigV4 vector; the streaming decoder is fuzzed in
  `cairn-protocol`.
- Spec: `docs/auth.md` (14). See the root `../../CLAUDE.md`.
