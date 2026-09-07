# Replication and recovery implementation phases

Approved implementation plan, based on main `34c78e3`. Deliver independently gated PRs in this
order, except independent integrity migration v32 lands before remote-cleanup migration v33.
This is a work tracker; entries must not claim completion until validation passes.

| Phase | Deliverable | Status |
|---|---|---|
| 1 | Fail-closed replication intent across writes and marker deletes; cleanup and retry tests | Merged in PR #69 |
| 2 | Exact attempt-token fencing and renewal of active/waiting claims; backend parity | Merged in PR #71 after exact-head CI passed |
| 4A | Required pinned-verifier signature/checksum checks for host and digest-pinned container installs | Merged in PR #70 |
| 3A | Authorized persisted replica multipart intent; identity, loop prevention, encryption and locks | Implemented; authorization regression and live crash/restore passed; full gate and PR CI pending |
| 3B | Reopenable logical ranges, signed two-pass streaming, multipart delivery, durable remote cleanup | Pending |
| 4B | Full-object checksum scrub, persisted internal SHA-256, legacy coverage, opt-in scrub pacing | Implemented in PR #73; 1,282 workspace tests, doctests, both Clippy configurations and live v32 restore passed; exact-head CI pending |
| 4C | Single-SQLite restore/crash coverage for every new durable field and accurate recovery runbooks | Pending |

## Fixed decisions

- Cairn-to-Cairn preserves source version identity; generic S3 remains at-least-once with possible
  duplicate versions after ambiguous completion. Do not promise remote exactly-once ordering.
- Transfer single PUTs up to 64 MiB; larger objects use multipart. Part size is at least 64 MiB and
  ceil(size/10000), rounded up to MiB and validated against destination limits. One active part per
  transfer, with signed logical-range hash/read passes and bounded shared buffer accounting.
- Five-minute replication leases renew every 60 seconds, including waiting batch entries. All
  completion/failure/defer/renew mutations require exact ownership. Existing workers drive renewal.
- Receiver support precedes sender rollout. Failed known uploads retain durable cleanup work;
  unknown initiation outcomes require explicit orphan reporting and destination lifecycle cleanup.
- Require cryptographic installer verification without a bypass, including older artifact rejection.
- Native recovery remains offline single SQLite. No clustering, external store, multi-shard backup,
  down-migrations, releases, or deployments. The user authorized merging each reviewed PR once
  its exact-head CI passes.
- Every shared mutation/read mirrors SQLite, async backends, in-memory doubles, and routing. Schema
  migrations are append-only and numbered from the actual current tip.

## Every-phase gate

Regressions first (demonstrate failure on prior behavior), owning-crate checks, then the complete
root CLAUDE.md gate: fmt; Clippy default/all features; workspace nextest/doctests; cargo audit; web
install/lint/build and both npm audits; installer shellcheck/tests; release-policy tests. New real
conformance tests run in CI per commit. Confirm exact-head green CI before considering merge.
Update ARCH specs, affected configuration/schema references, crate guidance, runbooks, conformance
inventory and AUDIT.md with the implementation, retaining accurate planned/tested/merged status.

Final combined drill covers concurrent encrypted multipart writes, multiple replication targets,
config faults, lease loss, destination stalls, process crashes, backup and restore; verify digest,
version identity, bounded memory, cleanup and ownership. Real AWS tests need explicitly supplied
credentials; do not equate MinIO/mock coverage with an AWS test. Large-size arithmetic is synthetic;
real >2 GiB and >5 GiB transfer tests gate the new transport.

## Phase 1 validation — 2026-09-07

The new regression fails against main's old behavior (unexpected HTTP 200 instead of 500) and
passes with the fix. Workspace nextest: 1,254 passed, one skipped; two doctests passed. Both Clippy
configurations, formatting, web lint/build/both audits, installer regressions and release-policy
checks passed. RustSec found zero vulnerabilities and the existing rustls-pemfile unmaintained /
chacha20 yanked maintenance warnings. Live bucket and multipart conformance passed. Two-node soak:
11,382 PUTs, zero errors, 216 byte-for-byte verified replicas, zero mismatches, RSS within its gate.
