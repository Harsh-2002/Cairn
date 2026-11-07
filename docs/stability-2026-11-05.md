# Stability and release readiness — 2026-11-05

Internal readiness record for a single-node Cairn deployment. This is not a third-party
certification, and it does not replace the pre-upgrade snapshot or a site-specific test.

*Updated 2026-11-07 for the 1.0 closure.*

## Verdict

**1.0 for a single node with backups.**

Cairn is at parity or better on every measured workload except the intentionally non-durable
unversioned DELETE, the upgrade and rollback path works, and the new storage format survived an
independent review. The 24-hour correctness run completed with zero PUT errors and zero replication
mismatches; after fixing the source-only replication-metrics scan, the 2-hour anonymous-RSS gate
passed. 1.0 is declared for a single node with backups; power-loss testing remains site-specific.

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

A 24-hour attempt at 8 KiB objects was stopped after 1.6 hours: at 440,066 objects with 214,630
outbox entries still unreplicated, RSS reached ~984 MiB (703 MiB anonymous, 280 MiB file-backed)
with 0 put errors and 0 mismatches. The worker is object-rate limited for tiny objects, so the
outbox backlog grew; the anonymous memory is the SQLite page cache/mmap filling as the database
grew, not a fold failure, and the fixed caches cap it. That shape is not a production proxy and
does not fit the disk for 24 hours. The gate is now 64 KiB at a bounded 25 objects/s, which keeps
replication up and fits the free space (`DURATION=86400 IDLE=600 OBJ_SIZE=65536 RATE=25`).

### 24-hour correctness run and 2-hour memory gate (2026-11-06/07)

The 64 KiB / 25 object/s shape ran the full 24 hours: **1,697,128 PUTs with 0 errors, 0 replication
mismatches and 137,552 byte-identical target reads**. The total-RSS idle check failed
(2,388.6 → 3,553.6 MiB, +48.8%) while the source database was 2.3 GB. The source alone grew: the
completed replication-outbox rows are retained for 24 hours (about 1.7M rows), and the 15-second
metrics tick's `replication_counts` query grouped the whole outbox every time. The fix (#215)
restricts the active counts to pending/claimed/failed (status index) and computes completed with an
index-only count; the counts are unchanged.

A 2-hour run on the fixed binary with a 15-minute idle then passed: **140,653 PUTs, 0 errors, 0
mismatches, 11,504 verified reads**, anonymous RSS **155.6 → 153.5 MiB (−1.3%, threshold 10%)**.
Total RSS grew 469.6 → 771.3 MiB, entirely file-backed SQLite cache/mmap pages. The owner accepted
the 2-hour memory gate; the 24-hour run remains the long-duration correctness evidence. Power-loss
testing remains site-specific.

Logs: [`performance-evidence/2026-11/soak-24h-2026-11-06.log`](performance-evidence/2026-11/soak-24h-2026-11-06.log)
and [`performance-evidence/2026-11/soak-2h-anon-2026-11-07.log`](performance-evidence/2026-11/soak-2h-anon-2026-11-07.log).

### Abrupt process kill

`conformance/kill_recovery.py` SIGKILLed a live node three times under six concurrent PUT
workers (602 acknowledged objects across the rounds). After each restart every acknowledged
object read back byte-for-byte, the completed 10 MiB multipart object kept its full body and
`?partNumber=1/2`, no in-flight object was torn, and offline `cairn integrity` reported
`errors=0` (4-5 orphans and 1-2 staging aliases reclaimed per round). This is a **process
crash**, not a power loss: the kernel page cache survives, so disk-cache loss remains untested.

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

## Known limits

1. **Replication optimization is not finished.** The baseline quantifies the cost and the worker
   trade-off; it does not remove it. The next candidates are a single-pass signed upload (chunked
   SigV4 or a persisted payload SHA-256) and target-side write batching. Both are protocol/security
   decisions, not silent tuning.
2. **No power-loss test.** The 24-hour correctness run and the 2-hour anonymous-RSS run cover
   long-duration correctness and memory, but no test cuts power. The crash harnesses cover
   task-level seams and the FAILPOINTS windows; power-loss testing needs a site-specific plan.
3. **Reflink is XFS/btrfs only.** ext4, tmpfs and overlayfs take the streaming copy path; the
   relative performance there was not re-measured. A Docker writable layer is overlayfs — mount the
   data directory on XFS/btrfs, not inside the container layer.
4. **The unversioned DELETE ratio is not equal durability** (0.51× here, 1.43× on a faster disk).
   The versioned arm is the number to quote (3.55–5.17×).
5. **Rate limiting is out of scope.** Cairn does not implement auth-path rate limiting; deploy
   behind a reverse proxy or network policy. The installer secret-to-stdout item is fixed (0600
   file, path only), and OpenRC is documented as unsupported. These are in `AUDIT.md`'s ground
   truth register.

## 1.0 closure (2026-11-07)

1. **Replication direction:** accepted. The default 4 workers stay; 2 favours source foreground, 8
   favours backlog drain. The measured foreground cost is documented in
   [`benchmarks.md`](benchmarks.md#16-replication-throughput-and-sender-cost-2026-11-05).
2. **Soak:** the 24-hour run passed correctness (0 errors/mismatches); the source-only RSS growth was
   traced to the completed-outbox scan and fixed in #215. The 2-hour anonymous-RSS gate passed on the
   fixed binary. The owner accepted the 2-hour memory gate; power-loss testing remains site-specific.
3. **Audit:** rate limiting is documented as outside the supported scope; the installer secret is
   fixed; OpenRC is unsupported. No open decision blocks 1.0.
4. **Support statement:** [`support.md`](support.md) is published with the supported deployment and
   limits.

1.0 is supported for a single node with backups and a tested restore, on XFS or btrfs, with the
pre-upgrade snapshot retained (no downgrade after schema v42).
