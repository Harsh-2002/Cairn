# Stability and release readiness — 2026-11-05

Internal readiness record for a single-node Cairn deployment. This is not a third-party
certification, and it does not replace the pre-upgrade snapshot or a site-specific test.

## Verdict

**Release candidate for a single node with backups; not 1.0 yet.**

Cairn is at parity or better on every measured workload except the intentionally non-durable
unversioned DELETE, the upgrade and rollback path works, and the new storage format survived an
independent review. What remains before a 1.0 support statement is a decision on replication's
foreground cost, a longer soak, and three long-standing audit gaps that have not been decided.

## Verified on 2026-11-04/05

### Full scoreboard

`conformance/rustfs_compare.py --case all`, three interleaved strict pairs, same host and binary.
The host was shared and loaded (0.5 → 15), so the **ratios** are the signal; absolutes are suppressed
by host load. Full table and evidence in
[`benchmarks.md`](benchmarks.md#15-full-scoreboard-after-blake3-and-reflink-2026-11-05).

| Workload | Ratio |
|---|---:|
| PUT 4 KiB | 2.24× |
| PUT 1 MiB | 1.14× |
| GET 4 KiB | 1.10× |
| GET 1 MiB | 1.92× |
| HEAD 4 KiB | 1.12× |
| LIST 1,000 keys | 5.33× |
| Mixed 1 MiB | 1.12× |
| Multipart part GET | 2.64× |
| Multipart upload (scored) | 1.07× |
| DELETE 4 KiB, unversioned† | 0.51× |

† As-configured, not equal durability. The equal-durability versioned delete-marker arm measured
**5.17×** on the same host state (Cairn 1,148 / 911 / 909 vs RustFS 197 / 191 / 185 obj/s).

### Upgrade and rollback

`conformance/upgrade_drill.py`: v2026.09.13 (commit `f6fde5a`, schema v39, protocol 2) → main
(schema v42, protocol 3) **PASS**.

- Plaintext, SSE-S3 and completed multipart objects written by the release read back exactly.
- Legacy rows keep `internal_sha256` and leave `internal_blake3` NULL.
- The legacy multipart object has no recorded part layout; its `?partNumber` read fails closed.
- New multipart writes record BLAKE3, a v3 sidecar, and a working `?partNumber`.
- The released binary refuses the upgraded database (exit 1, schema/protocol).
- The pre-upgrade data-directory snapshot restores, and the released binary reads every object.

### Soak and memory

A 30-minute two-node replication soak on XFS wrote **90,344 objects with 0 errors, 0 replication
mismatches**, and 229 sampled byte-for-byte target reads matched. RSS rose during the load, so the
soak's built-in 50 % check failed; that check compares an early warm-up baseline to the end of a long
run. A separate load-then-idle plateau test (180 s write, then 600 s idle) measured **90.2 MiB at the
end of the load and 92.1 MiB after ten idle minutes (+1.9 %)**, so the growth is cache/workload
bound, not an idle leak. `conformance/soak.py` now supports `--idle-seconds` (`IDLE` in
`soak.sh`), which uses the post-write idle delta as the leak signal; a 30 s smoke run with
`IDLE=30` measured 34.3 → 36.9 MiB (+7.6 %, threshold 10 %) and passed. The 120 s CI run is
unchanged.

### Independent review

A read-only review of #190–#194 (BLAKE3, sidecar v3, reflink, overlapping barriers, CI reflink job)
found no release blocker. Three low-severity hardening gaps were fixed in #198 with tests:

- `encode` now enforces the same generation/baseline rules as `decode`, including 64-hex digests;
- `Staging::run_file_op` rejects a sink with buffered bytes, not only a queued write;
- the v2 sidecar classifier compares the legacy SHA-256 value, not only the layout.

Remaining review follow-ups: a clone-refusal-after-probe test, a multipart data-rename → sidecar-rename
crash seam, and a legacy rebuild/scrub matrix. The audit register and the reflink crate guide were
updated in #199.

### Replication baseline

`conformance/replication_perf.py` (PR #200): two Cairn nodes on one host, versioned bucket with an
enabled rule vs a control bucket, 30 s interleaved Warp arms, FULL durability.

| Workers | Foreground PUT vs control | Shipped during load | Drain | Lag max / p95 |
|---:|---:|---:|---:|---:|
| 2 | **0.794×** | 4.3 obj/s | 25.4 obj/s | 27 / 25.5 s |
| 4 (default) | 0.66–0.70× | 2.7–3.5 obj/s | 31.7–34.6 obj/s | 26 / 24 s |
| 8 | 0.613× | 9.9 obj/s | 49.1 obj/s | 20 / 19 s |

64 MiB objects: foreground **~0.63×**, shipped 0.24–0.27 obj/s during load, drain 1.34–1.56 obj/s
(86–100 MiB/s), lag 9–15 s. Source CPU per shipped MiB was 0.0185 s (1 MiB) and 0.0141 s (64 MiB);
the extra source CPU per PUT was +10.2 ms and +0.46 s. Hashing alone is not the ceiling; the 1 MiB
worker is capped by four in-flight transfers at ~121–126 ms per replica, and the target's durable
write/fsync is the other candidate.

**All arms share six CPUs and one NVMe.** The foreground ratio is a worst-case same-host contention
measurement, not a remote-target claim. No product default was changed: keep 4 as the balance, use 2
when source foreground matters most, use 8 for backlog/drain throughput.

## Known limits and open decisions

1. **Replication optimization is not finished.** The baseline quantifies the cost and the worker
   trade-off; it does not remove it. The next candidates are a single-pass signed upload (chunked
   SigV4 or a persisted payload SHA-256) and target-side write batching. Both are protocol/security
   decisions, not silent tuning.
2. **No multi-day soak or power-loss test.** 30 minutes plus an idle plateau is not a 48-hour soak,
   and no test cuts power. The crash harnesses cover task-level seams and the FAILPOINTS windows.
3. **Reflink is XFS/btrfs only.** ext4, tmpfs and overlayfs take the streaming copy path; the
   relative performance there was not re-measured. A Docker writable layer is overlayfs — mount the
   data directory on XFS/btrfs, not inside the container layer.
4. **The unversioned DELETE ratio is not equal durability** (0.51× here, 1.43× on a faster disk).
   The versioned arm is the number to quote (3.55–5.17×).
5. **Audit gaps not decided:** no rate limiting on the auth path (not requested and not added),
   `install.sh`/bootstrap secret-to-stdout remains an accepted but untriaged item, and OpenRC unit
   hardening is still open (systemd is hardened). These are in `AUDIT.md`'s ground truth register.

## Gates to call it 1.0

1. Decide the replication direction: accept the measured foreground cost and document the worker
   setting, or implement and measure a single-pass/target-side change. Do not weaken durability.
2. Run a 24–48 hour soak with an idle plateau (`DURATION=86400 IDLE=600 OBJ_SIZE=8192`, or 48 h)
   and crash injection on XFS, and keep the result.
3. Decide the three audit gaps (fix, accept with a written rationale, or document as unsupported).
4. Publish a support statement: single node, Linux 5.6+, x86_64/arm64, SQLite, XFS recommended,
   asynchronous replication (RPO = lag, no automatic failover), backups required, no downgrade after
   schema v42/sidecar v3.

Until then: suitable for a single-node deployment with backups and a tested restore, on XFS or
btrfs, with the pre-upgrade snapshot retained.
