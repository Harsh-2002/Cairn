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
- `soak.sh` (+`.py`) — two-node replication, byte-identical verify + RSS leak check (boto3). For the
  single-node **mixed-feature** soak (and constant-load leak detection generally) see
  `soak_features.sh` below.
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
  bounded *relative* to the first ramp level; any 5xx
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
- `stress_multipart.sh` (+`.py`, boto3) — the **concurrent multipart write path**, which nothing else
  touches: `multipart.sh` is serial, `encryption.sh` proves one SSE multipart, and the warp harnesses
  never issue a Complete at all. Four scenarios on one hot node. **(1)** N SSE-KMS sessions are
  pre-staged and every `CompleteMultipartUpload` is released from ONE `threading.Barrier` while a
  separate pool runs a steady single-part PUT stream — `assemble` holds one of the 64 blob-**write**
  permits across its serial `assemble_into` loop (a decrypt-then-re-encrypt pass per part) and `stage`
  draws from the SAME pool, so this is the starvation shape. **(2)** one session in that same barrier
  has a staged part corrupted on disk first (flipped ciphertext byte, the `encryption.py` technique):
  its Complete must fail CLOSED with no object committed while every sibling still completes
  byte-exact. **(3)** `ClaimMultipart` complete-vs-abort fired from two threads, and a `RecordPart`
  supersede (part re-uploaded while another part uploads). **(4)** `UploadPartCopy` sessions staged
  concurrently with body sessions, with the staged-part on-disk ciphertext proof. GATES are
  load-independent: byte-exact assemblies + byte-exact background PUTs, zero op errors, exact S3 error
  CODES (`InvalidPart` for a superseded ETag; the race loser from a fixed 4xx set), all-or-nothing
  (never a torn object), the upload id RESOLVED with **no staging orphans**, ordinary PUTs making
  progress AT ALL during the barrier (a count > 0, never a rate), `/healthz` never *stopping*
  (a 60 s per-probe **wedge** timeout — probe latency is load, not signal), the same absolute
  RSS/fd/thread/WAL ceilings as `stress.sh`, and a 5xx counter EQUAL to the driver's declared
  fail-closed budget. Complete wall times and background-PUT throughput with vs without the barrier
  are **advisory** (`STRESS_MP_OUT=` writes the JSON) — CI drives the debug artifact, whose AES-GCM is
  unoptimized. Carries one pinned **KNOWN GAP** (reported, not gated): when Abort wins the race after
  Complete already entered `assemble`, `delete_session` pulls the staged bytes and the loser answers
  500 `InternalError` instead of `NoSuchUpload` — fail-closed, wrong code. **Coverage boundary:**
  scenario 2 flips a ciphertext BODY byte, which fails inside `assemble` (i.e. *after* `ClaimMultipart`)
  and so deliberately bricks that session; the complementary invariant — a bad **part DEK** is opened
  *before* the claim so the upload stays **retryable** (audit #14) — is covered in-process by
  `wrong_master_key_fails_complete` (`cairn-protocol/tests/protocol_core.rs`), not here, because
  reaching it needs the sealed DEK envelope corrupted in the metadata DB under a live server. Profile is env-tunable
  (`MP_SESSIONS`, `MP_PART_SIZE`, `MP_BG_WORKERS`, `MP_RACE_ROUNDS`, …); the default is ~2 min because
  a real multi-part Complete must pay S3's 5 MiB non-final-part minimum, so 5 MiB is spent only where
  a multi-part assemble is under test and every other part is a small tail.
- `stress_adversarial.sh` (+`.py`, boto3 + raw sockets) — the **REJECTION paths under CONCURRENCY**,
  which nothing else covers: every other limit/abuse probe in this directory is SERIAL (one bad
  `Content-MD5`, one tmpfs fill, one tampered token, one continuation loop on an idle server). Five
  scenarios on one hot node. **(1)** The SigV4 **aws-chunked streaming decoder**
  (`cairn-protocol/src/chunked.rs`, the F-5 component — get it wrong and objects corrupt *silently*):
  one barrier releases valid signed **and** unsigned streaming uploads together with deliberately
  MALFORMED ones — bad chunk signature, truncated stream, chunk length over- **and** under-declared,
  oversized chunk header, non-hex chunk size, missing per-chunk signature. Driven over **raw sockets
  with hand-rolled SigV4**: no SDK will emit malformed framing, and `http.client` loses the response
  when the server rejects mid-body. **(2)** A per-bucket **quota** (set via the management API) and
  `CAIRN_MAX_OBJECT_SIZE` attacked by concurrent PUTs. **(3)** A real **continuation-token loop**
  while other workers PUT/DELETE inside the very prefix being listed. **(4)** **STS** sessions
  minted / used / tampered / **revoked** concurrently. **(5)** Large-fanout **DeleteObjects** mixing
  present, absent, structurally invalid and 3×-duplicated keys. GATES are load-independent: every
  valid stream byte-exact *beside* the malformed ones (a malformed stream must never corrupt a
  concurrent valid one), EXACT `(status, S3 code)` pairs everywhere — never "any 4xx" — with nothing
  committed for a rejected request; stored bytes **never** exceeding the quota **and** the admitted
  count exactly `quota/size` (a race that let one extra write through fails even when the sum still
  fits), every quota rejection exactly `507 InsufficientStorage`, every oversized PUT exactly
  `400 EntityTooLarge`, and a cleared quota re-admitting writes; every listing pass terminating
  inside a bound derived from the **keyspace** (the churn workers cycle a FIXED key ring, so the
  bound is load-independent), with no duplicate key, strictly increasing keys, every token accepted
  and every key that existed for the whole pass returned; a session refused with exactly
  `AccessDenied` / `SignatureDoesNotMatch` (tampered) / `InvalidArgument` (no token) /
  `InvalidAccessKeyId` (revoked, with `CAIRN_AUTH_CACHE_TTL_SECS=0` pinned so revocation is not a
  race); an exact per-key delete split (an **absent** key is a SUCCESS — S3's delete is idempotent)
  with the one invalid key the only `InvalidArgument` and a control namespace intact and byte-exact;
  `/healthz` never *stopping* (60 s wedge timeout); the `stress.sh` RSS/fd/thread/WAL ceilings **plus
  a sampler-read-nothing gate** (a column reading zero all run FAILS — a zero peak passes every
  ceiling); and a 5xx counter EQUAL to the declared budget. **UI listener ON**; the launcher pins
  `CAIRN_MAX_OBJECT_SIZE` (it is under test) and `CAIRN_REQUEST_TIMEOUT_SECS=600`. Carries four
  pinned **FINDINGS** (reported loudly + declared into the 5xx budget, not gated): the under-declared
  chunk length, oversized chunk header, non-hex chunk size and missing per-chunk signature each
  answer **500 InternalError** instead of a 4xx — fail-closed (nothing is committed, and *that* is
  gated) but a client-caused framing error surfacing as a server fault, because `DecodeError` travels
  as `BlobError::Body(Transport(..))` and only `BodyError::Truncated` has a 4xx arm in
  `impl From<BlobError> for Error` (`cairn-types/src/error.rs`). The other two 5xx here are correct:
  `507` is S3's quota answer and simply lives in the 5xx range. Profile is env-tunable
  (`ADV_CHUNK_ROUNDS`, `ADV_PAGE_KEYS`, `ADV_DEL_PRESENT`, …), every knob that could empty a loop is
  validated at startup, and `STRESS_ADV_OUT=` writes the advisory JSON.
- `stress_replication.sh` (+`.py`, boto3) — **replication under SUSTAINED LOAD across a MASTER-KEY
  boundary**, the gap `soak.sh` leaves (plaintext only, one fixed 64 KiB body, source RSS the only
  signal) and `mesh.sh` leaves (distinct keys, but a handful of objects per scenario, not a load).
  TWO nodes, **different master keys** (derived per node from a label, the `mesh.py` technique), wired
  through the operator-trusted **`CAIRN_REPLICATION_ENDPOINT` config path** — SSRF-guard-exempt, so
  unlike `mesh` this needs **no** `CAIRN_ALLOW_INTERNAL_ENDPOINTS`; the source's UI listener is on
  only so the driver can read `GET /api/v1/replication/summary` (exact outbox counts + true lag). The
  target runs `CAIRN_ENCRYPT_AT_REST=true`, so it re-seals every replica under **its own** key. A
  fixed-size worker pool holds a **CONSTANT** source workload — single-part PUTs, version churn,
  delete markers (both on already-versioned keys and on write-once **tomb** keys), real ≥ 5 MiB
  multipart Completes — plus an SSE-S3/`aws:kms` arm; both nodes are sampled every second.
  **GATED** (all load-independent): every plaintext-leg version read back **from the TARGET by the
  SOURCE's version id** is BYTE-EXACT — the headline, and only possible if the source decrypted to
  logical bytes and the target re-sealed under its own ring; no replica ever carries **wrong bytes**;
  version-id identity incl. whole-**set** equality for a churn key; every delete marker present on
  the target with the same id and every tombstoned key answering **exactly `404 NoSuchKey`**; the
  **on-disk proof** — target blobs are `VERSION_ENCRYPTED` CRNB with the plaintext marker absent
  while the matching **source** blobs are plain and *do* carry it; the **OUTBOX DRAINING** to 0 after
  writes stop, bounded, with a **no-progress stall** detector (the load-independent way to state "the
  backlog is not growing without bound" — the in-run depth is advisory, since source and target share
  one box); zero driver errors; `/healthz` never *stopping* on either node (60 s **wedge** timeout);
  both alive; per-node 5xx EQUAL to the declared budget (0 — the one deliberate rejection here is a
  400); per-node RSS/fd/thread/WAL **ceilings** (same knobs as `stress.sh`) plus a
  **sampler-read-nothing** gate, because a zero peak would pass its ceiling for free. Convergence-
  latency percentiles, replication throughput, the outbox-depth series, peak lag and per-node CPU/RSS
  are **advisory** (`STRESS_REPL_OUT=` writes the JSON); every knob that could empty a loop
  (`REPL_SECS`, `REPL_WORKERS`, `REPL_MP_PART`, `REPL_SSE_EVERY`, …) is validated at startup. The
  launcher **pins** `CAIRN_REQUEST_TIMEOUT_SECS=600` (so a slow runner cannot manufacture a 503) and
  pre-flights all three ports (a stale cairn answering `/healthz` would otherwise make the run
  silently measure someone else's process). Carries **TWO pinned KNOWN GAPS** (reported, NOT gated —
  fixing them is a product change): `ReplicationEngine::put_object` opens the source blob with
  `BlobStore::open`, i.e. **with no DEK**, so an SSE-encrypted source version ships raw **ciphertext**
  — (1) a **single-part** one is refused `400 BadDigest` at the destination and, a 4xx being terminal,
  **never replicates at all** (fail-closed); (2) a **multipart-completed** one carries a composite
  `<md5>-<n>` ETag the destination cannot MD5-verify, so it is **ACCEPTED** and the replica is
  right-sized, `200`-answering **garbage** — silent corruption, the more serious of the two. The
  driver counts all three outcomes and prints `KNOWN GAPS APPEAR FIXED` when they stop happening; the
  fail-closed gate is therefore scoped to the plaintext leg.
- `soak_features.sh` (+`.py`, boto3) — the **mixed-FEATURE soak**, and **the harness where
  constant-load LEAK DETECTION lives**. Everything else either exercises one feature functionally or
  RAMPS load; `soak.sh` is long-running but is two-node replication only (plaintext, one bucket, a
  fixed 64 KiB body, source RSS the only signal). This one holds a **fixed-size worker pool** busy
  for `SOAK_SECS` (default 180 s, env-tunable to a deep run) against ONE node, so **offered load
  never drifts** — which is precisely what makes a steady-state climb gateable here and deliberately
  *not* in `stress.sh` (which ramps 4→64, where a climb only means more load was offered). The mix
  runs concurrently and continuously: SSE-S3 + `aws:kms` single-part churn; a **versioned** bucket
  accumulating per-version DEKs, delete markers and version-scoped deletes; **composite-checksum**
  multipart (`CRC32`/`COMPOSITE`) completed AND deliberately aborted, plus sessions **abandoned** for
  the background sweeper; an **object-lock** bucket whose GOVERNANCE-locked versions are attacked with
  deletes all run; **STS** session credentials minted on a cadence that drive a slice of the traffic;
  and a **1 s lifecycle** scanner with an expiring prefix plus a control prefix it must never touch.
  Workers own private key namespaces (`w<N>/…`) so a self-inflicted 404 can never fire the
  zero-errors gate. Samples every second: RSS, fds, threads, summed WAL (shard-aware), CPU ticks,
  and — every `SOAK_HEAVY_EVERY` ticks — `.staging` bytes and the `session_credentials` row count.
  **GATED, correctness:** zero operation errors, every sampled read byte-exact, WORM unbroken for the
  whole soak, the lifecycle control prefix never touched (and expirations actually observed), a
  tampered session token refused on **every** mint, aborted uploads leaving no staging, `/healthz`
  never *stopping* (60 s per-probe **wedge** timeout — latency is load, not signal), server alive, and
  a 5xx counter EQUAL to the declared budget, which for this mix is exactly **zero** (every
  deliberate rejection here is a 4xx). **GATED, leak shape** (middle-third vs last-third, the
  `stress.sh` windowing, each requiring BOTH a % growth AND a meaningful absolute delta so a +4 fd
  wobble cannot fail a run): RSS, fd and thread counts must not climb; staging bytes and WAL bytes
  must **plateau** — asserted as *last-third minimum vs middle-third maximum*, because a healthy
  sawtooth always comes back down. **GATED, ceilings:** the same RSS/fd/thread/WAL knobs and defaults
  as `stress.sh`. The launcher pins `CAIRN_WAL_CHECKPOINT_INTERVAL_SECS=15` (the 300 s default is
  longer than a CI soak, which would make the WAL climb by *configuration* and the gate unsound) and
  `CAIRN_REQUEST_TIMEOUT_SECS=600` (so a slow runner cannot manufacture a 503). Two assertions need a
  long run and report as **explicitly SKIPPED** rather than silently passing when it is too short:
  the multipart **sweeper** reclaiming abandoned sessions (needs the run to outlast
  `CAIRN_MULTIPART_UPLOAD_LIFETIME_SECS`; it *does* gate at the 180 s default) and an **expired**
  session credential being refused (the server's floor for a session is 900 s, ARCH 14, so it needs
  `SOAK_SECS` > ~930). The `session_credentials` row count is sampled and **reported, never gated**,
  for the same reason: on a sub-900 s soak a rising row count is *correct*. Everything rate-shaped —
  ops/s, Complete wall times, CPU-s/GiB, expirations observed — is advisory (CI drives the debug
  artifact); `SOAK_OUT=` writes it all as JSON.
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
- Needs: boto3 (`run`, `soak`, `soak_features`, `stress_adversarial`, `stress_replication`,
  `replication_chaos`); passwordless sudo (`blob_limits`, for a tmpfs —
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
