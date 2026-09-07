# Replication and recovery implementation phases

Approved implementation plan, based on main `34c78e3`. Deliver independently gated PRs in this
order, except independent integrity migration v32 lands before remote-cleanup migration v33.
This is a work tracker; entries must not claim completion until validation passes.

| Phase | Deliverable | Status |
|---|---|---|
| 1 | Fail-closed replication intent across writes and marker deletes; cleanup and retry tests | Merged in PR #69 |
| 2 | Exact attempt-token fencing and renewal of active/waiting claims; backend parity | Merged in PR #71 after exact-head CI passed |
| 4A | Required pinned-verifier signature/checksum checks for host and digest-pinned container installs | Merged in PR #70 |
| 3A | Authorized persisted replica multipart intent; identity, loop prevention, encryption and locks | Merged in PR #72 after exact-head CI passed |
| 3B | Reopenable logical ranges, signed two-pass streaming, multipart delivery, durable remote cleanup | PR #74; complete local gate and real plaintext/encrypted >2 GiB and >5 GiB transfers passed; exact-head CI required |
| 4B | Full-object checksum scrub, persisted internal SHA-256, legacy coverage, opt-in scrub pacing | Merged in PR #73 after exact-head CI passed |
| 4C | Single-SQLite restore/crash coverage for every new durable field and accurate recovery runbooks | PR #75; final v33 full-state restore, 20 crash checks and all three remote response-loss drills passed; exact-head CI required |

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

## Integrated local validation — 2026-09-07

The streaming baseline matches snapshot `5a9a9b1` and PR #74 head `32a384b`. Its complete root gate
passed: 1,297 workspace tests (two skipped), two doctests, formatting, both Clippy configurations,
web lint/build/both audits, cargo audit, installer regressions and release-policy checks. Focused
coverage includes 111 replication, 75 blob and eight journal/backend tests.

Real 2,147,483,665-byte and 5,368,709,137-byte objects replicated to Cairn and MinIO with plaintext
and encrypted storage. Every full readback matched SHA-256; Cairn preserved the exact source
version; healthy runs left no remote multipart uploads or cleanup journal entries. Peak source RSS
was 47 MiB plaintext and 52.4 MiB encrypted. These compressible fixtures prove actual logical
transfer sizes and bounded memory, not sustained capacity or incompressible-disk performance.
Timings were collected on a shared host and are not a performance benchmark. Final recovery
checks passed all 20 crash cases and the three real remote response-loss scenarios.

The final review added bounded ListParts confirmation after remote abort and authorized missing-session
cleanup for replication-only credentials (`4b6d012`, `25efbad`). The late-part regression failed
against the earlier sender and passes after the fix; five multipart HTTP tests and 13 protocol
replication tests pass. PR #75 also removes unnecessary proxy XML parsing and rejects forwarded
header line breaks; four wire regressions and all three recovery scenarios pass. Final integrated
workspace and exact-head CI results are required before merging these follow-up changes.

## Deferred final phase: capacity, sustained load and regression campaign

Requested by the user on 2026-09-07. Run only after all implementation phases merge with green CI,
against the resulting main commit, when the user resumes this campaign. Do not treat the current
multi-GiB transfer checks or CI stress fixtures as proof of million-object capacity.

1. Record the exact commit, hardware, filesystem, available disk/inodes, runtime settings and
   durability mode. Select an isolated test data directory and explicit time/storage limits.
   Establish an idle and low-load baseline; retain raw results and a reproducible workload seed.
2. Populate progressively: 100,000, one million, then several million small documents (for example
   1–64 KiB), with varied prefixes and both one large bucket and multiple buckets. Exercise listing,
   pagination, HEAD, GET, overwrite, version history and deletion at each population checkpoint.
3. Add large files from MiB sizes through actual >2 GiB and >5 GiB multipart objects. Include
   incompressible and compressible payloads, plaintext and encrypted storage, range reads, and
   mixed small/large concurrent traffic. Size the corpus relative to RAM to test cold-disk behavior.
4. Increase concurrency in steps, then hold representative mixed traffic for an extended soak.
   Observe saturation and recovery after reducing load. Include replication lag/backlog catch-up,
   scrub contention and destination outages without changing the durability contract.
5. On disposable fixtures, repeat process-crash, restart and offline backup/restore checks with the
   populated dataset. Verify acknowledged data against an independent manifest, namespace/version
   counts, digests, pagination completeness, journal cleanup and multipart recovery.
6. Report throughput, p50/p95/p99 latency, error/retry rates, CPU, RSS and its growth slope, disk
   space/inodes, SQLite/WAL size, writer queue depth, replication lag and recovery time. Compare
   repeat runs on identical hardware/settings; define performance thresholds from that baseline.
   Any unexplained data loss/corruption, accounting mismatch or failure to recover fails the phase.

Publish the tested capacity envelope, bottlenecks, regressions and reproduction commands in the
benchmark documentation. Convert discovered failures into focused regressions before claiming
that the tested load is supported. Hardware, duration and final dataset limits remain to be chosen
when the user starts this deferred phase.
