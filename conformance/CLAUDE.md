# conformance

End-to-end verification harnesses that drive a **real `cairn` binary** (the in-crate unit/property/
fuzz tests live next to their sources). Two kinds — keep them distinct:

## e2e / feature (does it work as specified?)
- `run.sh` (+`conformance.py`) — boto3 / real AWS SDK full object lifecycle.
- `share.sh` — object sharing (share tokens + SigV4 presigned URLs).
- `rotation.sh` (+`rotation.py`) — master-key rotation lifecycle (#29), sharded.
- `soak.sh` (+`soak.py`) — two-node replication, byte-identical verify + RSS leak check.
- `warp.sh` — the MinIO warp macro benchmark (get/put/mixed).
- `crash_consistency.sh` — the F-4 durability property at one crash seam.
- `scrub.sh` — integrity scrub: corrupt a stored blob on disk, assert the background scrub
  (`CAIRN_SCRUB_INTERVAL_SECS`) detects the ETag mismatch (`cairn_scrub_corruption_total`).
- `object_lock.sh` — Object Lock / WORM: COMPLIANCE immutable (no delete/shorten, bypass ignored),
  GOVERNANCE yields only to `s3:BypassGovernanceRetention` + the bypass header, legal hold
  blocks/releases, bucket default retention stamped + echoed on HEAD. (Also folded into `run.sh`.)
- `notifications.sh` (+`notifications.py`) — webhook event notifications: stands up a local sink,
  configures a bucket endpoint via the management API (Bearer), drives S3 PUT/DELETE, and asserts
  the sink receives correctly-shaped, HMAC-signed S3 event records. Needs the UI listener ON.
- `sts.sh` (+`sts.py`) — STS temporary credentials: mints a scoped session via the management API,
  then proves a standard S3 SDK consumes it (`X-Amz-Security-Token`) with exactly the granted access
  (scoped GET allowed; ungranted PUT / cross-bucket / tampered / absent-token denied). UI listener ON.

## regression / limit (where does it break?)
- `replication_chaos.sh` (+`.py`) — break replication on purpose (target down, source SIGKILL); assert no loss.
- `crash_multipoint.sh` (+`.py`) — crash at every blob-commit seam (PUT + multipart); reconcile reclaims.
- `concurrency.sh` (+`.py`) — N clients race one key (create / CAS / last-writer); atomic, no corruption.
- `warp_escalate.sh` — ramp warp concurrency to the single-writer ceiling; alive + zero errors.
- `blob_limits.sh` (+`.py`) — out-of-space 507, huge object, many objects paginated.
- `load_profile.sh` (+`.py`) — throughput methodology (not a gate; see `../docs/benchmarks.md`).
- `sendfile_bench.sh` — `fast-io` plaintext sendfile A/B: server CPU/GiB + zero-copy engage rate
  (needs a `--features fast-io` binary; optional non-`fast-io` `BASELINE_BIN`; not a gate).

## Conventions
- Invoke as `BIN=target/debug/cairn PY=python3 bash conformance/<name>.sh`. Each makes a `mktemp -d`
  data dir, bootstraps, serves, waits on `/healthz`, and cleans up via a trap.
- Python drivers that **restart** the server (rotation, replication_chaos, crash_multipoint, blob_limits)
  own the process lifecycle themselves.
- Needs: boto3 (`soak`, `replication_chaos`); passwordless sudo (`blob_limits`, for a tmpfs); a
  `--features failpoints` build (`crash_*`). Running a server needs the dev sandbox disabled.
