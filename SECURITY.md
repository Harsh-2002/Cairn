# Security policy

Cairn is an S3-compatible object-storage server that holds production data and secrets, so security
reports are taken seriously.

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue for an unpatched
vulnerability. Use GitHub's private vulnerability reporting on this repository
(**Security → Report a vulnerability**).

Include the affected version/commit, a description, reproduction steps, and the impact. We aim to
acknowledge reports promptly and to coordinate a fix and disclosure.

## Security model

The architecture and threat model are documented in
[`docs/security-errors.md`](./docs/security-errors.md) (sections 25, 27). Load-bearing invariants:

- Secrets at rest — SigV4 secrets, replication credentials, and SSE-S3 data keys — are
  AES-256-GCM envelope-encrypted; **reads fail closed** (a missing/wrong key errors, never returns
  plaintext) and secrets are never logged, echoed, or returned by any endpoint.
- The S3 authorization pipeline (policy / ACL / public-access-block / object-ownership) is
  fail-closed; an object with no ACL is private.
- Master-key rotation and the retire-gate are described in
  [`docs/operations.md`](./docs/operations.md).

## Supported versions

Cairn is pre-1.0; security fixes land on `main`.
