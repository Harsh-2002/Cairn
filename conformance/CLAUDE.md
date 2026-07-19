# conformance

End-to-end harnesses that drive a **real `cairn` binary** — bash launcher + (usually) a Python
driver. Unit/property/fuzz tests live next to their sources in each crate; these are the black-box
"does the whole binary behave?" layer. **Almost all of these are CI gates** (`.github/workflows/ci.yml`,
mirrored job-per-script) — editing a script, its asserts, or the behavior it pins can turn the gate
red, so treat a passing local run as load-bearing. Two kinds — keep them distinct.

## e2e / feature (does it work as specified?)
- `run.sh` (+`conformance.py`) — boto3 / real AWS SDK full object lifecycle; the broad smoke gate.
- `checksums.sh` (+`.py`) — modern-SDK flexible-checksum round-trip (ARCH 21.1): PUT/GET/HEAD echo the
  stored `x-amz-checksum-<algo>` (+ `x-amz-checksum-type`) so default-on SDKs validate the transfer;
  CRC32/SHA256 always, CRC32C/CRC64NVME when `botocore[crt]` is installed; Range never echoes.
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
  — RSS over the run (steady-state leak check, last-third vs middle-third, + a 1 GiB ceiling) and peak
  `cairn_writer_queue_depth`. Emits a PASS/FAIL verdict; `STRESS_OUT=`/`BASELINE=` write/compare a
  results JSON for regression tracking. Supersedes `warp.sh`+`warp_escalate.sh` (kept as focused tools).
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
- `mesh.sh` is intentionally **NOT CI-gated** (5 nodes is too heavy/flaky for the shared runner) —
  run it by hand. Same for the two non-gate benchmarks above.
- Spec: replication ARCH 20, durability/storage `docs/storage-durability.md` 8–10, blob limits ARCH 9,
  testing/conformance/perf `docs/testing-performance.md` 29–30. Build/gate: root `../CLAUDE.md`.
