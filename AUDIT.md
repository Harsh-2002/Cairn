# AUDIT.md — security & architecture review playbook

**This is a living, reusable document.** It is not a one-off review report — it is the process an
agent (or human) follows every time Cairn gets a security/architecture audit, and it accumulates a
history at the bottom so the next run builds on the last one instead of re-discovering it. If you run
an audit using this doc, you are expected to **update it**: correct anything that's gone stale, append
your run to the Audit History table, and fold any newly-learned "known state" facts into the Ground
Truth section below.

Do not treat anything in this file as a substitute for reading the actual code. Where this file
describes current behavior, it was true as of the date attached to that claim — **verify it's still
true before relying on it**, the same way you'd distrust a stale comment.

---

## 1. Orientation — read before anything else

1. `CLAUDE.md` (repo root, `AGENTS.md` is a symlink to it) — project map, build/test gate, crate
   layout, invariants.
2. `CONTRACT.md` — the hard architectural ceilings (single-node, one SQLite writer, env-only config,
   append-only schema, fail-closed crypto, etc). A finding that requires *crossing* a ceiling to fix is
   a recommendation for a human decision, not something to patch around.
3. `docs/security-errors.md` (ARCH 25, 27) — the existing error model and threat model. Don't restate
   what's already documented there as a "finding" — confirm the code still matches the doc; flag it
   only if it's *drifted*.
4. `SECURITY.md` — disclosure policy and the release-signing/verification story.
5. Whichever of `docs/auth.md`, `docs/storage-durability.md`, `docs/replication.md`,
   `docs/control-plane.md`, `docs/scaling-limits.md` matches the area you're deep-diving.
6. The per-crate `CLAUDE.md` for every crate you touch (e.g. `crates/cairn-meta/CLAUDE.md`,
   `crates/cairn-net/CLAUDE.md`) — folder-scoped constraints not repeated at the root.

## 2. Context

Cairn is a self-hosted, S3-compatible object storage server written in Rust, single-node by design.
Object bytes are plain files on disk; all metadata (buckets, objects, ACLs, policies, credentials,
lifecycle rules) lives in an embedded SQLite database behind one group-committing `Writer`. Two
listeners: an S3 data-plane port and a web console/management-API port. Auth is SigV4 (header +
streaming-chunked) and Bearer tokens, plus STS-style AssumeRole/GetSessionToken for short-lived scoped
credentials. Secrets at rest are sealed with an AES-256-GCM master key (rotatable via a key ring). It
supports per-bucket SSE-S3, `CAIRN_ENCRYPT_AT_REST`, SSE-KMS (label-only, not real per-tenant
isolation — documented, not a gap), bucket policies/ACLs/Block-Public-Access, Object Lock (WORM), async
cross-node/S3 replication, webhook notifications, and Prometheus metrics.

**This codebase has already been through multiple rounds of adversarial review.** Don't assume any of
it still holds — verify fresh — but the highest-value use of a new audit run is (a) catching
regressions in previously-hardened areas, or (b) covering surface that's newer or was never in scope
before. See Section 5 Ground Truth for what's already known, and check the Audit History (Section 8) for what the
last run already covered.

**Framing:** this is an authorized internal review of our own codebase for defensive hardening — not
adversarial reconnaissance against a third party.

## 3. Constraints

- Do not cut a release, push to `main`, force-push, or edit `CONTRACT.md`. If a fix requires crossing a
  ceiling in `CONTRACT.md`, stop and report it as a decision for a human.
- Read `docs/` before deciding something is a bug — several "gaps" are documented, deliberate v1
  limitations (SSE-KMS label-only, single-node topology, trusted-host Object Lock enforcement). Do not
  flag documented, in-scope design decisions as findings; focus on gaps *within* the stated scope.
- Every proposed fix needs a regression test in the owning crate, and the full gate must stay green:
  `cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features -- -D warnings &&
  cargo nextest run --workspace`.
- Filing a GitHub issue is a visible, shared-state action (see Section 6) — don't mass-file without a
  confirmation step unless the invoking session was explicitly told to auto-file.

## 4. Review scope

For each area: read the real code, not the doc, and not the class/function name. Every finding needs a
file:line, the attacker-reachable trigger, and the actual impact — no finding without a reproduction
path or a precise explanation of why it's reachable.

### 4.1 Authentication & credential handling
- SigV4: canonical request construction, clock-skew window, replay protection, constant-time signature
  comparison. Streaming-chunked SigV4 — chunk-boundary bypass, length-mismatch handling.
- Bearer token validation — format, expiry, revocation.
- STS AssumeRole/GetSessionToken — session token entropy, expiry enforcement, whether a temp credential
  can exceed its granted policy's scope.
- Master key handling — `CAIRN_MASTER_KEY` in memory, zeroization on drop, whether it or derived keys
  ever surface in logs, `/metrics`, error responses, or panics. **Ground truth (2026-07-29):**
  `crates/cairn-crypto` holds key material in `zeroize::Zeroizing` and scrubs on drop (`system_crypto.rs`
  lines 132, 144, 273) — confirm this still holds, and that nothing introduced since bypasses it.
- Master-key-ring rotation — old-key retention window, atomicity against concurrent secret access.
- Root credential handling (`CAIRN_ROOT_ACCESS_KEY`/`SECRET_KEY`) and the documented dev default —
  confirm it's rejected or loudly warned on outside dev.
- **`install.sh` prints the generated access/secret key to stdout on completion** (`install.sh` around
  the `print_access` step) — this lands in shell scrollback/history/CI logs on an automated install.
  Verified present as of 2026-07-29; treat as a standing item to re-check (not yet triaged into an
  issue — do so on the next run if still true).

### 4.2 Authorization
- Bucket policy engine — Allow/Deny precedence, wildcard/condition matching, default-deny fallthrough,
  any parse-error path that fails open.
- ACL + Object Ownership interaction — confirm no path grants unintended public access.
- Block Public Access — checked on every relevant path (PUT bucket policy, PUT ACL, presigned URL
  generation), not just one entry point.
- Presigned URL generation/validation — expiry enforcement, scope binding (bucket/key/method), reuse.
- Version-scoped authorization on versioned objects.

### 4.3 Cryptography
- AES-256-GCM nonce/IV uniqueness — random vs counter, behavior across process restarts and
  replication (nonce reuse under GCM is catastrophic — check this specifically).
- SSE-S3 / `CAIRN_ENCRYPT_AT_REST` — encryption happens before fsync, not just before the response;
  plaintext isn't staged to disk longer than necessary during multipart assembly.
- `aws:kms` label surface — confirm it's still clearly non-cryptographic-isolation internally and
  nothing new treats it as real per-tenant KMS.
- TLS config — supported versions/ciphers, cert reload race conditions, whether a plaintext listener
  can be exposed by misconfiguration without warning.
- **Replication ciphertext — ground truth (2026-07-29):** the original bug (replication shipped raw
  ciphertext for SSE objects with no DEK) is **fixed at the code level** — `cairn-replication` resolves
  the version's DEK and ships logical plaintext bytes before the destination re-seals
  (`crates/cairn-replication/src/lib.rs` ~420-440, documented in `docs/replication.md` "Affected
  versions" callout). **Residual, still-open:** objects already replicated *before* the fix are not
  auto-healed; a `cairn replication audit` detection tool exists but is opt-in
  (`CAIRN_REPLICATION_AUDIT_BEFORE`), not automatic. A future audit should check whether this residual
  risk is adequately surfaced to operators upgrading from an affected version, not re-report the base
  bug as new.

### 4.4 Storage & filesystem layer
- Path handling for object keys — traversal (`../`, absolute paths, null bytes, encoded slashes) from
  S3 key names to on-disk paths. **Highest-value area to check line by line.**
- Stage → fsync → atomic rename write path — true atomicity (no window where a partial file is visible/
  servable), and that restart reconciliation can't resurrect or leak orphaned data across
  tenants/buckets.
- Multipart upload part storage/assembly — temp file permissions, cleanup on abort, resource limits
  (can a client exhaust disk via unbounded/never-completed multipart uploads?).
- SQLite usage — parameterized queries only. **Ground truth (2026-07-29):** `crates/cairn-meta/src/
  apply.rs` and `schema.rs` are fully parameterized via `rusqlite::params![...]`; no string-built SQL
  found. Re-verify on every run — this is exactly the kind of thing a careless future patch regresses.
  Also check WAL/concurrent-writer behavior, busy-timeout handling, and whether `cairn backup` gives a
  consistent snapshot relative to on-disk object files.
- Object Lock/WORM — enforced at the storage layer, not just the API layer. Direct-filesystem-access
  bypass is an inherent limitation given the trusted-host assumption (documented — not a finding).

### 4.5 Network / API surface
- Resource limits on the S3 API — max object size, max header size, max multipart parts, request body
  size caps; missing limits that enable DoS.
- **Rate limiting — ground truth (2026-07-29): none exists.** `cairn-server` has a concurrency cap
  (`Semaphore`-based, `server.rs` `concurrency`/`connection_limiter`) that load-sheds with `503` past a
  fixed in-flight ceiling — that is a concurrency cap, not a request-rate limiter, and infra endpoints
  (`/healthz`, `/readyz`, `/metrics`) bypass it entirely. **No per-IP or per-credential auth-attempt
  throttling/lockout exists anywhere in `cairn-auth`.** This is a real, currently-open gap (brute-force
  / credential-stuffing exposure on the auth path) — a future run should size the risk and either raise
  an issue or record a documented decision that it's accepted.
- Webhook delivery (`cairn-webhook`) — SSRF potential via a configured target URL, retry/backoff under
  a malicious/slow endpoint, payload signing so receivers can verify authenticity. Confirm it and every
  other outbound dialer (replication, import) actually routes through the `cairn-net` SSRF guard
  (`guarded_http_connector`) — look specifically for any caller that builds its own client/connector.
- Replication — credential scope, whether the stream itself is encrypted/authenticated, partial-object
  visibility on the target under partial failure.
- Management API vs S3 API separation — `CAIRN_WEB_ADDR=off` actually removes the attack surface (not
  just hides a route); management endpoints require separate authz from S3 credentials.
- CORS — **ground truth (2026-07-29): scoped correctly.** `crates/cairn-protocol/src/service.rs` only
  echoes `Access-Control-Allow-Origin` when the request matches a bucket's *stored* CORS rule (exact
  match, configured literal `"*"`, or a single-wildcard prefix/suffix pattern); no unconditional
  reflection of `Origin` found. Re-verify if `service.rs`'s CORS matching logic changes.

### 4.6 Rust-specific code quality
- `unsafe` — **ground truth (2026-07-29):** workspace-level lint (`Cargo.toml` `[workspace.lints.rust]`)
  is `unsafe_code = "warn"`, but every crate individually hardens this with `#![forbid(unsafe_code)]` in
  its `lib.rs`, **except `cairn-server`**, whose `main.rs` forbids by default and only relaxes to `deny`
  under the opt-in `fast-io` feature. The only actual `unsafe` blocks in the entire workspace are in
  `crates/cairn-server/src/sendfile.rs` (7 blocks, each behind an explicit `#[allow(unsafe_code)]` with
  a SAFETY comment, for the `libc`/`ktls` sendfile fast path). Re-run `grep -rn unsafe crates/` on every
  audit — a new crate or a change to `main.rs`'s cfg_attr gating would silently widen this.
- `.unwrap()`/`.expect()`/indexing on request-derived data — panic-as-DoS risk on the S3 or console
  listener; a single malformed request shouldn't take the process down.
- Integer arithmetic on client-supplied sizes/offsets/content-length — overflow/underflow, especially
  byte-range and multipart part-number handling.
- Error types — confirm internal errors (DB, IO, path) aren't leaked verbatim into S3 API error
  responses (info disclosure).
- Dependency audit — **ground truth (2026-07-29): no `deny.toml` exists, and `.github/workflows/ci.yml`
  runs neither `cargo audit` nor `cargo deny`.** This is a real, currently-open gap in the CI gate, not
  a hypothetical — either add the check or record why it's deliberately deferred. In the meantime, run
  `cargo audit` by hand on every audit pass.
- Async task handling — unbounded per-request spawns without backpressure.

### 4.7 Container / deployment
- **Ground truth (2026-07-29): two Dockerfiles exist, for different purposes** — the repo-committed
  root `Dockerfile` (Node + Rust builder image, for local/source builds) and a second one generated
  inline in `.github/workflows/release.yml` (`FROM gcr.io/distroless/static-debian12:nonroot`, copies
  prebuilt release binaries) used only for the published release image. Review each against its own
  purpose — the *release* image is the one that needs to be minimal/distroless/non-root; the dev
  Dockerfile having a full toolchain is expected, not a finding.
- `install.sh` — **ground truth (2026-07-29):** fetches the release binary + `SHA256SUMS` over HTTPS
  from GitHub, runs `sha256sum -c`/`shasum -a 256 -c` but only **warns** (does not fail) if the checksum
  tool is unavailable, and performs **no signature verification** of the downloaded binary. Generates
  `CAIRN_MASTER_KEY`/`CAIRN_ROOT_SECRET_KEY` via `openssl rand -hex 32`, writes them to `/etc/cairn/
  cairn.env` (mode `0600`) or a Docker `.env` file — but also **prints the access/secret key to stdout**
  on completion (see Section 4.1). These are standing review items; re-verify current behavior, don't assume
  the above is still accurate by the time you read this.
- systemd/OpenRC — **ground truth (2026-07-29): no unit files are committed to the repo; `install.sh`
  generates them at install time on the target host.** The generated systemd unit sets real hardening
  (`NoNewPrivileges=true`, `ProtectSystem=full`, `ProtectHome=true`, scoped `ReadWritePaths`). **The
  generated OpenRC script has no equivalent hardening directives** — only `command_user`. This asymmetry
  is a real gap worth an issue if OpenRC targets (Alpine, etc.) are a supported deployment path.
- `docs/deployment-kubernetes.md` — check recommended manifests for `runAsNonRoot`, read-only root
  filesystem, resource limits, `NetworkPolicy` guidance.

### 4.8 Performance / architecture gaps
- Lock contention (mutex/rwlock scope) on the hot PUT/GET path that would cap throughput under
  concurrent load; whether the metadata-commit-then-ack design serializes unnecessarily.
- SQLite as sole metadata store — scaling ceiling per `docs/scaling-limits.md` (as of 2026-07-29: single
  writer, headline ~13k commits/s synchronous / ~37k async on a 2-vCPU reference host) — confirm the
  code still matches those numbers before citing them; they drift as the writer is tuned.
- Compression path — range reads only decompress the blocks they need, not whole objects.
- Replication lag — actually observable via `/metrics` as claimed; backpressure if the target falls
  behind.
- Benchmark claims live in two places that should stay consistent: the summary table in root
  `README.md` Performance section, and the full methodology in `docs/benchmarks.md`. If they diverge, that's a
  documentation-accuracy finding in its own right.

## 5. Ground truth register

This is the running "state of the world" the ground-truth notes above are pulled from — a flat index so
you don't have to re-read Section 4 to know what's already known. **Update this table on every audit run**;
delete a row once its status is stable and uncontroversial enough that it belongs in `docs/` instead.

| Date | Area | Status | Detail |
|---|---|---|---|
| 2026-07-29 | Replication ciphertext | Fixed (code) / residual risk (pre-fix data) | See Section 4.3 |
| 2026-07-29 | SQL parameterization (`cairn-meta`) | Clean | See Section 4.4 |
| 2026-07-29 | Master key zeroization | Clean | See Section 4.1 |
| 2026-07-29 | CORS origin reflection | Clean, correctly scoped | See Section 4.5 |
| 2026-07-29 | `unsafe` scope | Narrow, justified (sendfile.rs only) | See Section 4.6 |
| 2026-07-29 | Rate limiting on auth path | **Open gap** — none exists | See Section 4.5 |
| 2026-07-29 | `cargo audit`/`cargo deny` in CI | **Open gap** — neither runs | See Section 4.6 |
| 2026-07-29 | `install.sh` secret-to-stdout | **Open, untriaged** | See Section 4.1 |
| 2026-07-29 | OpenRC unit hardening | **Open gap** — systemd hardened, OpenRC not | See Section 4.7 |

## 6. Filing findings as GitHub issues

**Every finding gets its own issue** — security defect or architecture/performance gap alike. Don't
bundle unrelated findings into one issue; do link related issues to each other.

### 6.1 Before filing
1. `gh issue list --search "<keyword>" --state all` to check whether this finding (or its root cause)
   already has an issue, open or closed. Cross-reference against Section 5 Ground Truth first — most of the
   effort of avoiding duplicates should happen there, before you even reach for `gh`.
2. Ensure the labels below exist; create any that are missing (`gh label create <name> --color <hex>
   --description "..."`) — as of 2026-07-29 this repo only has GitHub's default label set plus
   `dependencies`/`javascript`, so `audit-finding`, `security`, `architecture`, and the four severity
   labels will need creating on first use.
3. **Present the candidate issue list to the operator before filing** — title + one-line summary per
   issue — and get a go-ahead. This mirrors how any other visible, shared-state action (opening PRs,
   posting comments) is handled in this project: one approval doesn't authorize silent mass-filing in a
   future session. Skip this step only if the session invoking this audit was explicitly told to
   auto-file without review.

### 6.2 Labels
- `audit-finding` — umbrella label on every issue this playbook produces, so the whole program is one
  `gh issue list --label audit-finding` query away regardless of severity or type.
- Type: `security` or `architecture` (pick one; use `security` for anything with an attacker-reachable
  trigger, `architecture` for design/performance/scaling gaps with no direct exploit path).
- Severity: `severity:critical` / `severity:high` / `severity:medium` / `severity:low` (impact ×
  likelihood, OWASP-style — critical/high should be rare and mean it).

### 6.3 Issue template

```
Title: [audit] <concise statement of the defect, not the symptom>

## Summary
One or two sentences: what the issue is.

## Scope / affected components
Crate(s), file(s), API surface, or deployment path this touches. Be specific — "cairn-auth,
crates/cairn-auth/src/sigv4.rs:NNN-NNN, the SigV4 header-auth path" not "the auth system."

## Impact
What happens if this goes unaddressed — concretely. Who's affected (which deployment shapes, which
auth path, which client behavior triggers it) and what they can do as a result (read/write/delete
data they shouldn't, DoS the process, bypass a control, etc). No impact statement without a trigger.

## Evidence
File:line citations and, where applicable, the exact request/config/sequence that reaches the code
path. A reproduction beats a description.

## Suggested fix
Code-level, not "add validation" — name the function/check to add or change. If the fix would cross
a CONTRACT.md ceiling, say so explicitly instead of proposing a workaround.

## Found by
Which audit run (date + reference to the Audit History entry below) and whether it's a new finding
or a regression in previously-hardened surface (name the prior fix/PR if it's a regression).
```

### 6.4 After filing
Record the issue number(s) against the corresponding Audit History row (Section 8) — don't let the mapping
between "the run that found it" and "the issue tracking it" live only in GitHub.

## 7. Deliverable for the audit run itself

Beyond the filed issues, produce a short run summary: total findings by severity, how many were
regressions vs. new-surface, the prioritized top-remediation list (risk-to-effort ratio, not just
severity), and a short list of architecture/performance recommendations that are *not* filed as issues
because they're bigger judgment calls for a human (e.g. "add rate limiting" is a real gap but the shape
of the fix is a design decision, not a one-line patch).

## 8. Audit history

Append one row per run. Keep it terse — the detail lives in the issues, not here.

| Date | Scope | Findings filed | Notes |
|---|---|---|---|
| 2026-07-29 | Doc created; fact-verification pass only (no full audit run yet) | none | Verified the ground-truth register in Section 5 against current code as the doc's baseline. First full audit run against this playbook is still pending. |
