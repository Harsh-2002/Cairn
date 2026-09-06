# Security and architecture review — 2026-09-06

Review and remediation plan, not a numbered engineering specification. Baseline:
`a5f712730ef1690a341ef418ec5977f419038a54` (local checkout and GitHub `main`). Reviewed by the
primary agent and three independent Sol agents. Source inspection, GitHub alert data flows,
regression tests, and dependency audits are the evidence; no throughput gains are assumed.

The operator subsequently requested submission of the current changes through a pull request.
This review accompanies the fixes; opening that PR does not mean the fixes are merged or deployed.
Separate vulnerability reports follow `SECURITY.md`; new issue/advisory candidates still require
the review step in `AUDIT.md` §6.1.

## Assessment

Cairn has a coherent core for a single-node object store. Metadata is authoritative; blobs become
visible at a single metadata commit; group commit isolates requests with savepoints; the protocol
depends on traits; and cancellation recovery resolves exact immutable identities rather than
guessing whether a write committed. Two listener roles and the separate policy engine are sound
boundaries. These are worth preserving.

The most important weaknesses are incomplete application of those boundaries: service-level S3
operations skipped authorization, the control plane accepted body work before its admin gate, and
replication uses weaker ownership semantics than multipart completion. Future work should first
make those guarantees uniform and make recovery cover the whole supported object range.

Do not add clustering, an external metadata service, or weaken `synchronous=FULL` to claim a
performance improvement. Existing optional metadata sharding is a documented implementation, but
`CONTRACT.md` still states a one-database ceiling: expanding its guarantees needs an explicit human
decision. A real external KMS likewise crosses the current one-ring contract.

## GitHub inventory and disposition

On this run GitHub had ten open CodeQL alerts, no open Dependabot or secret-scanning alerts, no
open repository advisories, no open issues, and no open PRs. Latest scheduled CodeQL run
`33593959178` analyzed the baseline SHA on 2026-09-02.

All ten CodeQL alerts were individually reviewed and dismissed through GitHub's API. This is
triage, not ten code fixes. Seven retain an intentional credential-output contract; three are
false positives. Scanner severity was high for every alert and is not an independent exploitability
assessment.

| Alert | Baseline location | Disposition and evidence |
|---|---|---|
| [26](https://github.com/Harsh-2002/Cairn/security/code-scanning/26) | `cli_remote.rs:1040` | Won't fix: explicit admin user-create result returns new Bearer secret. |
| [27](https://github.com/Harsh-2002/Cairn/security/code-scanning/27) | `cli_remote.rs:1044` | Won't fix: same create result returns the composite Bearer token. |
| [28](https://github.com/Harsh-2002/Cairn/security/code-scanning/28) | `cli_remote.rs:1050` | Won't fix: same create result returns the new S3 secret. |
| [29](https://github.com/Harsh-2002/Cairn/security/code-scanning/29) | `cli_remote.rs:1066` | Won't fix: explicit admin rotation result returns the new Bearer secret. |
| [30](https://github.com/Harsh-2002/Cairn/security/code-scanning/30) | `cli_remote.rs:1070` | Won't fix: same rotation result returns the composite Bearer token. |
| [31](https://github.com/Harsh-2002/Cairn/security/code-scanning/31) | `main.rs:2477` | Won't fix: explicit node-local bootstrap returns configured root Bearer credentials. |
| [32](https://github.com/Harsh-2002/Cairn/security/code-scanning/32) | `main.rs:2484` | Won't fix: same bootstrap returns the configured root S3 secret. |
| [33](https://github.com/Harsh-2002/Cairn/security/code-scanning/33) | `main.rs:205` | False positive: topology precheck prints only backend/shard count. |
| [34](https://github.com/Harsh-2002/Cairn/security/code-scanning/34) | `main.rs:1538` | False positive: same topology diagnostic in backup. |
| [35](https://github.com/Harsh-2002/Cairn/security/code-scanning/35) | `main.rs:1627` | False positive: same topology diagnostic in restore. |

ARCH 24.2/24.3 explicitly define CLI credential delivery. Capturing that output in a CI log still
discloses credentials; dismissal does not remove that operational risk. See the credential-output
hardening work below. For alerts 33–35, SARIF taints the entire `Config` after
`trusted_proxy_allowlist()` validation; `require_canonical_backup_topology` at baseline
`crates/cairn-server/src/main.rs:948` formats only validated `meta_backend` and `meta_shards`.

[PR 66](https://github.com/Harsh-2002/Cairn/pull/66) is already merged and fixed different CodeQL
alerts (23/24). [PR 65](https://github.com/Harsh-2002/Cairn/pull/65),
[PR 62](https://github.com/Harsh-2002/Cairn/pull/62), and
[PR 58](https://github.com/Harsh-2002/Cairn/pull/58) contain earlier dependency/hardening work;
they are not pending fixes for these ten alerts. GitHub records all six Dependabot alerts as fixed.

## Security remediation in this branch

These are separate from the ten existing CodeQL alerts. Severity below is the review assessment,
not an assertion that GitHub reported them. Source locations refer to the baseline unless noted.

| ID | Severity | Trigger and impact | Remediation / verification |
|---|---|---|---|
| S1 | High | `cairn-protocol/src/service.rs:706,753,970`: ListBuckets and CreateBucket require a principal but skip policy evaluation. A scoped temporary credential can list its parent's bucket names and create buckets without those permissions; long-term identity Denies are ignored. | Evaluate both operations through the existing policy engine, including service-wide resource matching; sessions require explicit Allows. Regression covers session denial/allow, explicit identity Deny, anonymous requests, and ordinary baseline access. |
| S2 | Medium | `cairn-server/src/adapter.rs:652`: the console lacks anti-framing headers. A victim administrator can be induced to act inside a framed console; same-site data and control origins make SameSite cookies insufficient. Requires victim interaction. | Deny framing with CSP `frame-ancestors 'none'` and legacy X-Frame-Options. Test actual console response headers. |
| S3 | Medium | `cairn-server/src/adapter.rs:304`: unauthenticated control requests buffer up to 8 MiB before the admin gate. Default 1,024 admitted requests permit roughly 8 GiB of payload storage alone; actual exhaustion depends on deployment limits. | Reject unauthorized routes before polling bodies, retain exact public exceptions, and give login a small independent cap. Test a body that panics if polled and oversize public login requests. |
| S4 | Low (upstream) | Locked `h2 0.4.14` accepts/queues empty HTTP/2 DATA frames without a bound when streams are not drained. Fresh RustSec scan finds RUSTSEC-2026-0258 despite zero open Dependabot alerts. | Update only h2 to 0.4.16, rescan the lockfile, and run the workspace gate. |

Upstream reference for S4:
[GHSA-q83h-524g-xf6h](https://github.com/hyperium/hyper/security/advisories/GHSA-q83h-524g-xf6h).
The advisory database describes severity as low; this report does not inflate it to match the old
CodeQL dashboard labels.

Compatibility note: console login now accepts at most 4 KiB of JSON. Normal credentials fit well
within this limit; deployments using unusually large configured credentials must account for it.
Authenticated administrative routes retain their 8 MiB limit. The new regressions exercise body
admission directly; raw HTTP/1 pipelining and HTTP/2 frame-flood load tests were not added here.

## Prioritized architecture plan

Priority is ordering of work, not security severity. Each item needs a separate reviewable change
and must preserve the writer, crypto, and durability contracts.

### P1 — Fail closed when replication intent cannot be resolved

Evidence: `cairn-protocol/src/service.rs:5198` returns an empty outbox on both a replication-config
read error (`.ok().flatten()`) and a stored XML parse failure. PUT, copy, multipart completion, and
delete-marker paths then commit normally. A transient read failure can therefore produce an
acknowledged local write with no durable replication work and no pending-queue signal, contradicting
ARCH 20.3. Invalid persisted XML is a corruption/legacy-state case, not input accepted by the normal
validated S3 configuration handler. The read-error branch requires no such corruption.

Return `Result<Vec<OutboxEntry>>`, distinguish successful absence from failure, and propagate errors
before any object/marker metadata mutation. Preserve post-stage exact-path recovery. For bulk
delete, resolve the configuration once and return per-key errors without applying affected deletes.
Test injected read errors and malformed persisted configuration across PUT/copy/completion/markers,
as well as valid and absent configuration. Verify neither silent commit nor leaked staged data.
This recommendation follows exact source control flow; the fault-injection regression has not yet
been executed in this review.

### P1 — Fence replication ownership

Evidence: `cairn-meta/src/store.rs:76` uses a 300-second claim lease; replication delivery defaults
to 3,600 seconds (`cairn-replication/src/sink.rs:116`). Claim results and updates have no attempt
token (`cairn-types/src/meta.rs:435`; `cairn-meta/src/apply.rs:1136`). A live slow attempt may be
reclaimed, after which its stale result can overwrite a newer worker's completed state.

Add a fresh claim token or generation. Done/failed/defer/renew must condition on the exact claimed
owner and return a typed stale-owner result. Renew long-running attempts without widening their
authority. Mirror mutations, append-only schema, async backends, routing, and the in-memory double.
Test expiry and re-claim with a fake clock: B completes, A fails late, and B's state must remain
completed. Destination-level ordering also needs validation; database fencing alone cannot undo an
already-sent stale HTTP request to a generic S3 target.

A deterministic reproduction using the production SQL completed in this review: A claimed at
t=0; B reclaimed at t=300001 ms and completed at t=300002; A's stale failure then changed both
outbox and version status from completed to failed while leaving `replicated_at=300002`. This
proves durable ledger corruption. Cairn's destination version ordering limits the data impact;
an old in-flight PUT overtaking a newer version on a generic S3 target is an additional inferred
ordering risk, not a dynamically reproduced data-loss claim. Treat this as high reliability risk,
not a demonstrated critical vulnerability.

### P1 — Replicate the supported object range with bounded memory

Evidence: `cairn-replication/src/sink.rs:108,201` imposes a 2 GiB per-object buffering ceiling;
Cairn's configurable object-size default is 5 TiB. Raising the process-wide memory budget does not
remove the per-object cap. Large valid local objects therefore cannot obtain the configured remote
copy through this sink.

Design streaming or multipart outbound delivery with explicit destination compatibility and retry
semantics. Prototype signed streaming against Cairn and MinIO, and choose multipart when a
destination requires it. Keep the shared admission budget and non-resetting deadlines. Acceptance:
replicate an object above 2 GiB, check plaintext integrity and encryption behavior, interrupt/retry
transfers, and show RSS bounded by concurrency and chunk/part size rather than whole-object size.
Measure CPU/GiB and throughput; promise no speedup before that measurement.

### P1 — Make installation verification fail closed

Evidence: `install.sh:253` continues when SHA256SUMS cannot be fetched or no checksum utility is
available. It does not verify the signatures/provenance that the release workflow already produces.
The installer then installs executable code, commonly under root. This is a supply-chain hardening
gap, not proof of a present remote compromise.

Require the expected exact checksum entry and successful verification before installation. Then
design a pinned verifier bootstrap and verify the release workflow identity/issuer and artifact
subject, avoiding trust in a verifier fetched through the same unverified path. Test missing
manifest/tool, malformed or wrong checksum, wrong signing identity, wrong subject, and success.
Every failure must leave the installed binary unchanged. Cover both host and container delivery.

### P1 — Audit dependencies even when no commits land

Evidence: `.github/workflows/ci.yml:19` triggers on pushes to main and pull requests, with no
schedule. Main's last CI run was 2026-08-09; RUSTSEC-2026-0258 was published on 2026-08-17. The
2026-09-02 CodeQL scan was green and GitHub's Dependabot inventory still had no open alert for h2
when this review's fresh RustSec scan found it.

Add a lightweight scheduled dependency-only workflow for the current default-branch lockfiles,
reusing the pinned RustSec and npm policies and fetching current advisory data. Keep write/OIDC
permissions absent. Acceptance: verify scheduled and manual execution, fresh database use, and
nonzero failure for a known vulnerable fixture. This complements the existing per-commit gate;
it does not replace it or treat CodeQL success as a dependency-health verdict.

### P1 — Complete integrity coverage for multipart data

Evidence: `cairn-server/src/background.rs:2269` skips whole-object digest comparison when the ETag
is composite. AEAD protects encrypted blocks, but plaintext multipart ETags do not provide a
whole-object digest. Existing usable full-object supplementary checksums are not used by this scrub.

First consume any stored FULL_OBJECT checksum. Then design an internal whole-object digest computed
during staging/assembly and persisted with metadata, with an append-only migration and explicit
legacy coverage state. Never call a digest calculated from already-damaged bytes a verified
baseline. Test bit flips across plaintext/compressed/encrypted and multipart/single-part objects;
measure hash cost before making an algorithm mandatory. Add scrub pacing and live-I/O contention
measurements before enabling a more aggressive default schedule.

### P1 decision — Align sharding with lossless recovery

Evidence: `docs/backup-restore.md:3` restricts native backup to single-shard SQLite;
`docs/migration.md:14` imports current objects and does not preserve all version/lock/ACL state.
Sharding improves disjoint-bucket write parallelism but removes the native complete snapshot path.

Decide whether sharding remains an explicitly constrained configuration or gains an offline
multi-shard snapshot/restore protocol. Multi-file publication needs a documented crash-recovery
design and human approval under `CONTRACT.md`; do not quietly substitute several renames for one
atomic commit. Acceptance includes process death at every publication boundary, all object
versions/delete markers/locks/ACLs, the full key ring, and a measured restore-time objective.
Replication is asynchronous and must not be described as a complete backup substitute.

### P2 — Shorten metadata maintenance transactions and restart time

Evidence: reconciliation membership batches still execute one indexed lookup per path
(`cairn-meta/src/lib.rs:214`); replication batch claim does per-entry update/read work
(`cairn-meta/src/apply.rs:2836`, async equivalent `:3004`). These are identifiable execution costs,
not measured bottlenecks on this host.

Use bounded set-based membership lookups and claim updates where backend parity permits. Preserve
aligned membership results, exact claim ownership, parameter limits, ordering, and mandatory
pre-bind reconciliation. Benchmark 100k/1m/10m synthetic blobs and backlogged replication while
recording startup time, DB statements, RSS, writer queue depth, and foreground PUT p50/p99. Reject
changes that improve one metric by weakening durability or starving foreground work.

### P2 — Make credential-output and backend support contracts explicit

CLI stdout and `install.sh:231` intentionally deliver secrets. A safer automation design should
require an explicit secret-output mode or create a no-replace, owner-only credential file, check
output readiness before mint/rotation, and avoid printing duplicate secret representations. Preserve
an explicit machine-readable path; update all bootstrap-consuming conformance scripts together.
This is a contract change to review, not a reason to hide CodeQL's data flow behind another API.

Before further backend proliferation, publish one capability matrix covering backups, rotation,
quotas, and crash recovery, and run the same semantic contract suite against each supported backend.
Separate large protocol modules only around existing authorization/commit seams; do not duplicate
those checks in extracted handlers. Treat external KMS and cold-tier storage as later product
decisions, after replication and recovery guarantees are complete.

## Validation and handoff

Completed locally on branch `audit/security-architecture-2026-09-06`:

- Formatting and Clippy across all targets, both default and all features: passed.
- Workspace nextest: 1,253 passed, one skipped; workspace doctests: two passed.
- Fresh cargo-audit 0.22.2 scan of 427 dependencies: zero reported vulnerabilities; two allowed
  maintenance warnings, `rustls-pemfile 2.2.0` unmaintained and `chacha20 0.10.0` yanked. The final
  run successfully checked the registry index as well as the freshly fetched advisory database.
- Web clean install, lint, production build, production moderate-level audit and full high-level
  audit: passed; both npm audits reported zero vulnerabilities.
- Installer shellcheck and installer regression script: passed. Release-policy checker: passed.
- Live console-session, authorization/tenancy, STS, and bucket conformance: passed. Bucket harness
  reports its existing non-gating GetBucketPolicy content-type deviation.
- Cross-review of the authorization and body-admission fixes: no blocking findings. The service
  authorization regression failed against the old behavior and passed after the fix.

The yanked `chacha20` is reached through `uuid → rand`, not Cairn's AES-GCM envelope implementation.
The [upstream changelog](https://github.com/RustCrypto/stream-ciphers/blob/master/chacha20/CHANGELOG.md)
marks 0.10.0/0.10.1 yanked and records an SSE backend intrinsic correction in 0.10.2. Schedule a
separate patch upgrade and CPU/backend compatibility check; no RustSec vulnerability was reported
for it in this scan. Replacing unmaintained `rustls-pemfile` is likewise follow-up maintenance, not
evidence of a currently reported vulnerability.

No throughput benchmark, full soak, crash-harness campaign, or non-host architecture test was run;
the performance items are measurement plans, not measured gains. All-features lint is not an
all-features runtime test. No release, tag, main-branch push, public vulnerability issue, or deployment
was performed during the review. The ten original alert dispositions are applied on GitHub.
At review completion the fixes were local and uncommitted; the operator subsequently requested
committing and submitting these changes through a PR. Integration and deployment remain pending.
The pre-submission GitHub recheck found zero open CodeQL alerts, zero open Dependabot alerts, and
no existing open PRs. Architecture recommendations below are planned work, not implemented changes.

Candidate tracking entries for operator review under `AUDIT.md` §6.1:

| Candidate | Tracking / scope |
|---|---|
| S1: Enforce identity/session policy on ListBuckets and CreateBucket | Private security report until the fix is integrated. |
| S2: Prevent framing of the authenticated console | Private security report until the fix is integrated. |
| S3: Authorize management requests before buffering their bodies | Private security report until the fix is integrated. |
| S4: Update h2 for RUSTSEC-2026-0258 | Ordinary dependency-fix tracking; advisory is already public. |
| R1: Preserve replication intent on configuration-read failures | Reliability issue; do not bundle with lease ownership. |
| R2: Fence and renew replication claims | Reliability issue with the deterministic stale-owner reproduction. |
| R3: Support replication above 2 GiB without whole-object buffering | Architecture issue for the documented size/replication mismatch. |
| R4: Enforce installer artifact verification before publication | Supply-chain hardening issue; not a claim of a compromised release. |
| R5: Verify whole-object integrity for multipart data | Reliability issue; distinguish legacy coverage and current digest support. |
| S5: Run dependency audits on a schedule | Security maintenance issue backed by the post-CI h2 advisory timeline. |

Sharded recovery, credential-output ergonomics, and measured metadata optimizations remain design
proposals until their scope/acceptance criteria are agreed; they are not speculative security bugs.
