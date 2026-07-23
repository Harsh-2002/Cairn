# conformance

End-to-end harnesses that drive a **real `cairn` binary** — bash launcher + (usually) a Python
driver. Unit/property/fuzz tests live next to their sources in each crate; these are the black-box
"does the whole binary behave?" layer. **Almost all of these are CI gates** (`.github/workflows/ci.yml`,
mirrored job-per-script) — editing a script, its asserts, or the behavior it pins can turn the gate
red, so treat a passing local run as load-bearing. Two kinds — keep them distinct.

## e2e / feature (does it work as specified?)
- `run.sh` (+`conformance.py`) — boto3 / real AWS SDK full object lifecycle; the broad smoke gate.
- `listing.sh` (+`.py`) — listing / pagination / versioning (audit B–H): delimiter + CommonPrefixes,
  empty-delimiter, max-keys truncation, the continuation/v1-marker/`start-after` token round-trip
  driven as a real loop, `EncodingType`, delete markers + undelete, version-scoped GET/HEAD/DELETE.
- `multipart.sh` (+`.py`) — full multipart lifecycle via the **low-level** API: out-of-order/undersized
  parts (`InvalidPartOrder`/`EntityTooSmall`), `ListParts`/`ListMultipartUploads` paging, abort/complete
  session-state errors, the `<md5>-<n>` ETag, multipart×versioning. Carries one `known_issue()` (#5).
- `objects.sh` (+`.py`) — payload / headers / range: system-header + `x-amz-meta-*` round-trip,
  CopyObject COPY vs REPLACE directives, response-header overrides, zero-byte objects, range edges
  (416, suffix `bytes=-N`, open `bytes=N-`), URL-encoded / unicode keys, `Content-MD5` `BadDigest`.
- `buckets.sh` (+`.py`) — bucket surface: CreateBucket idempotency + name validation, DeleteBucket
  (`BucketNotEmpty`), HeadBucket, config-subresource PUT→GET→DELETE round-trips with a
  bucket-survives guard after each DELETE (the routing fall-through class), CORS preflight.
- `authz.sh` (+`.py`) — auth / tenancy / credentials: a second identity's tenant isolation,
  `AccessDenied` beats `NoSuchKey`, SigV4 failure codes (`RequestTimeTooSkewed` et al.), presigned-URL
  redeem + expiry rejection. **UI listener ON** (mints the second user via the management API).
- `lifecycle.sh` (+`.py`) — lifecycle scanner **enforcement** (`CAIRN_LIFECYCLE_INTERVAL_SECS=1`):
  an expiration rule actually removes a matching object (polled, no sleep) and leaves a non-match,
  per-bucket scoping, a Disabled rule no-ops, a storage-class transition is rejected not ignored.
- `checksums.sh` (+`.py`) — modern-SDK flexible-checksum round-trip (ARCH 21.1): PUT/GET/HEAD echo the
  stored `x-amz-checksum-<algo>` (+ `x-amz-checksum-type`) so default-on SDKs validate the transfer;
  CRC32/SHA256 always, CRC32C/CRC64NVME when `botocore[crt]` is installed; Range never echoes.
- `encryption.sh` (+`.py`) — real-SDK SSE object-body conformance (ARCH 27), two boot legs: SSE-KMS +
  SSE-S3 wire echo (PUT/GET/HEAD) + byte-exact GET, mandatory-SSE 400, bucket-default silent encrypt
  (AES256/aws:kms), multipart+SSE incl. UploadPartCopy, cross-policy copy (upgrade/downgrade), and the
  on-disk proofs — committed/staged blobs are VERSION_ENCRYPTED CRNB with the plaintext marker absent,
  a tampered ciphertext byte makes GET fail closed, transparent at-rest (leg 2) advertises nothing.
- `share.sh` — object sharing (revocable share tokens + interoperable SigV4 presigned URLs), pure curl.
- `rotation.sh` (+`.py`) — master-key rotation lifecycle (#29), sharded.
- `soak.sh` (+`.py`) — two-node replication, byte-identical verify + RSS leak check (boto3).
- `mesh.sh` (+`.py`) — **5-node FULL mesh** replication (stdlib only): convergence, fan-out latency,
  version-id identity, concurrent same-key, crash resiliency, delete-marker mesh, no-cascade,
  integrity. The driver owns all five node processes and self-tears-down. `mesh.sh [scenario-ids...]`.
- `crash_consistency.sh` — the F-4 durability property at one crash seam (orphan-blob reclaim).
- `scrub.sh` — integrity scrub: corrupt a stored blob on disk, assert the background scrub
  (`CAIRN_SCRUB_INTERVAL_SECS`) flags the ETag mismatch (`cairn_scrub_corruption_total`).
- `object_lock.sh` — Object Lock / WORM: COMPLIANCE immutable, GOVERNANCE yields only to
  `s3:BypassGovernanceRetention` + bypass header, legal hold, bucket default retention echoed on HEAD.
- `notifications.sh` (+`.py`) — webhook event notifications: local sink, bucket endpoint via the
  management API, assert HMAC-signed S3 event records arrive correctly shaped. **UI listener ON.**
- `sts.sh` (+`.py`) — STS temp creds: mint a scoped session, prove an S3 SDK consumes it
  (`X-Amz-Security-Token`) with exactly the granted access, all else denied. **UI listener ON.**
- `console_session.sh` — console httpOnly session-cookie auth (pure curl): `cairn_session` from
  `POST /session` authenticates management API + S3 on the UI port, REJECTED on the S3 port, cleared
  by `DELETE /session`. **UI listener ON.**
- `backup_restore.sh` — backup/restore/integrity (pure curl, Bearer): `cairn backup`, corrupt then
  `cairn restore` into a FRESH dir → byte-identical, `cairn integrity --repair` drops exactly the
  dangling row. Parses each synchronous CLI's stdout counts — **never sleeps.**

## regression / limit (where does it break?)
- `routing.sh` (+`.py`) — **routing fall-through** (audit 2026-07, boto3 + hand-signed raw requests):
  a PRESENT-BUT-INVALID segment must be rejected (400), never collapsed to `None` and re-routed to
  the bucket/root handler — `DELETE /b/<1025-byte key>` destroyed an empty bucket, `GET` of the same
  returned a `ListBucketResult`, `/UPPERCASE` reached ListBuckets. Also pins unhandled
  `?subresource` → 501 (ARCH 13). Asserts exact status codes and that bucket + canary survive.
- `replication_chaos.sh` (+`.py`) — break replication on purpose (target down, source SIGKILL); no loss.
- `crash_multipoint.sh` (+`.py`) — crash at every blob-commit seam (PUT + multipart); reconcile reclaims.
- `concurrency.sh` (+`.py`) — N clients race one key (create / CAS / last-writer); atomic, no corruption.
- `stress.sh` (+`warp`) — the unified **"is it still fast AND stable?"** check to run after a change.
  Parses warp's throughput into a table (peak write/read/mixed obj/s + MiB/s), ramps concurrency to
  prove the server bends-not-breaks past the single-writer ceiling, and samples the **server process**
  every second — RSS, **open fds**, **thread count**, **summed WAL bytes** (shard-aware), **CPU-seconds**
  (and CPU-s/GiB moved) — plus `cairn_writer_queue_depth`, the **HTTP 5xx count**, and the writer's own
  **`cairn_writer_commit_seconds` / `cairn_writer_batch_size`** histograms. The stability gates are
  deliberately **shape/ratio/count** checks (fd·thread·WAL plateau vs monotonic climb; commit tail
  bounded *relative* to the first ramp level; group-commit batching engaged at high concurrency; any 5xx
  at all) so they hold on a contended CI runner — absolute obj/s + MiB/s stay **advisory**. Emits a
  PASS/FAIL verdict; `STRESS_OUT=`/`BASELINE=` write/compare a results JSON (schema is additive, so an
  older baseline still compares). Supersedes `warp.sh`+`warp_escalate.sh` (kept as focused tools).
- `stress_encrypted.sh` (+`.py`, `warp`) — the **encrypted-path** stress A/B: the same warp profile
  twice on one box, leg A plaintext vs leg B `CAIRN_ENCRYPT_AT_REST=true` (the boolean **`true`** —
  Figment rejects `1`), so every committed blob is a VERSION_ENCRYPTED CRNB container. Transparent
  at-rest needs no SSE headers, which is why warp can drive both legs unmodified. Read phases straddle
  the 256 KiB small-object threshold (64 KiB + 1 MiB) because an encrypted object is disqualified from
  **both** GET fast paths (sendfile zero-copy and the inline small-object read) and must fall back to
  the streamed read. The `.py` arm (boto3, threads) is the headline gate: byte-exact round-trips under
  concurrency plus the on-disk proof (CRNB v2 trailer, plaintext marker absent) for transparent at-rest,
  explicit `AES256`, and `aws:kms`. GATES are load-independent — correctness, zero op-errors, zero HTTP
  5xx, liveness, absolute RSS/fd/thread/WAL ceilings (same knobs as `stress.sh`), every phase parsing a
  non-zero throughput, and — **on a release build only** — a *catastrophic* collapse floor
  (`ENC_MIN_RATIO_PCT`, default 10%). On a **debug** binary the enc/plain ratio is **advisory, not
  gated**: debug AES-GCM is unoptimized software crypto (release 65–135% of plaintext vs 0.7–34% for
  the same debug build), so gating it would be pure flake and says nothing about the code — CI drives
  the debug artifact, so there the load-independent gates are what protect the encrypted path.
  Throughput ratios, CPU-s/GiB and the RSS/fd deltas between legs are **advisory**; `STRESS_ENC_OUT=`
  writes them as JSON. `SKIP_SSE_ARM=1` is an **operator-requested** warp-only run (still passes); an
  *automatic* skip because boto3 is missing **fails** the run, so a CI box that lost boto3 can't go
  green without ever proving encryption correctness.
- `warp.sh` — the MinIO `warp` macro benchmark (get/put/mixed); downloads `warp` once. Gates on errors.
- `bench_compare.sh` — **Cairn vs MinIO head-to-head**: boots Cairn AND a pinned MinIO server on one
  host and drives warp against each side-by-side (PUT/GET/STAT/DELETE/LIST/MIXED). Runs per push
  (`bench-compare` CI job → job-summary table + CSV/JSON artifact). **Report-not-gate**: the signal is
  the Cairn/MinIO ratio, and it fails ONLY on warp operation errors (not on who is faster), because a
  contended runner has large throughput variance. Parses the MEASURED op, not warp's prepare-PUT.
- `warp_escalate.sh` — ramp warp concurrency to the single-writer ceiling; alive + zero errors.
- `blob_limits.sh` (+`.py`) — out-of-space 507 on a small tmpfs, huge object, many objects paginated.
- `load_profile.sh` (+`.py`) — throughput methodology, **NOT a gate**; see `../docs/benchmarks.md`.
- `sendfile_keepalive.sh` — `fast-io` keep-alive engagement (pure curl): N GETs over ONE keep-alive
  conn must all engage zero-copy (`cairn_sendfile_get_total{result=ok}` += N). SKIPs on non-`fast-io`.
- `sendfile_bench.sh` — `fast-io` plaintext sendfile A/B (CPU/GiB + engage rate); **NOT a gate.**

## Notes
- Invoke as `BIN=target/debug/cairn PY=python3 bash conformance/<name>.sh` (the CI form). Most
  default `BIN` to `$ROOT/target/debug/cairn`, so they run from any cwd; a few (`run`, `share`,
  `rotation`, `concurrency`, `warp*`) want `target/debug/cairn` relative to the repo root.
- The bash launcher is thin: `mktemp -d` data dir, `CAIRN_UI_ADDR=off` unless the script needs the
  console, `bootstrap`, `serve`, poll `/healthz`, cleanup via `trap`. **A running server needs the
  dev sandbox disabled** (it binds listen sockets). Default config is env-only (ARCH 28) — these set
  `CAIRN_*` directly; mirror that, never invent a config file.
- Python drivers that **restart** the server (rotation, replication_chaos, crash_multipoint,
  blob_limits, mesh) own the process lifecycle themselves; the bash launcher just hands them the env.
- Needs: boto3 (`run`, `soak`, `replication_chaos`); passwordless sudo (`blob_limits`, for a tmpfs —
  CI runners have it); a `--features failpoints` build for the `crash_*` seams (`FAILPOINTS=...`); a
  `--features fast-io` Linux build for `sendfile_*` (else they SKIP); `warp`/`go` for `warp*`.
- Prefer asserting on synchronous CLI stdout or a metric/poll loop over `sleep` — the no-sleep
  harnesses are deliberately deterministic; don't add timing flake.
- **Every real test runs in CI on every push** — the whole point of the harness layer is that each
  commit gets a complete verdict; running locally is only a dev convenience. `mesh.sh` (5-node) and
  `sts_xml.sh` (STS XML surface) are now CI-gated jobs like the rest; `mesh` needs the internal-endpoint
  escape hatch (`CAIRN_ALLOW_INTERNAL_ENDPOINTS=true`, set by `mesh.py`) because it wires targets
  through the management API (SSRF-guarded), unlike `soak`'s config-endpoint path. The remaining
  non-gated items are pure **benchmarks/measurement tools** (`warp.sh`, `warp_escalate.sh`,
  `load_profile.sh`, `sendfile_bench.sh`) whose gating signal is already covered by the CI `stress` and
  `bench-compare` jobs; run them by hand for numbers.
- Spec: replication ARCH 20, durability/storage `docs/storage-durability.md` 8–10, blob limits ARCH 9,
  testing/conformance/perf `docs/testing-performance.md` 29–30. Build/gate: root `../CLAUDE.md`.
