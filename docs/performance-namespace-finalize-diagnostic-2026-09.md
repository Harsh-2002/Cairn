# Shared-node namespace/finalization diagnostic — 2026-09-23

## Preregistration (before client load)

This is a diagnostic campaign, **not** a candidate-adoption test. The user authorized up to 300 seconds of measured client load and 10,000,000,000 bytes of task-owned scratch on this shared node. The runner additionally caps total wall time at 300 seconds, reserves 30 seconds for teardown, and refuses an arm without 1 GB of scratch headroom. Partial completion is reported as partial, without resetting the allowance or retrying failed arms.

Run two alternating pairs of strict 1-MiB PUT: Cairn→RustFS, RustFS→Cairn, 12 seconds of Warp v1.8.0 load per arm, 16 concurrent clients, fresh stores and buckets, one loopback server at a time. Both modes must be strict: Cairn `CAIRN_META_SYNCHRONOUS=full`; RustFS global and newly created bucket strict, verified before and after traffic. The runner records exact Warp-scored intervals, operation errors, 2-second host/PSI and process samples, server metrics after a 16-second drain, and then reaps the process group and removes the exact arm store. Its JSON is retained for audit. A shared VM and unrelated work may invalidate paired throughput comparisons; two pairs cannot establish an adoption win.

Pinned executables in private `/var/tmp/cairn-performance-nsdiag-B5bg63`:

| Executable | SHA-256 |
| --- | --- |
| Instrumented Cairn, pinned Rust 1.97.1 release build | `40cd98e758a1cdb3eeb26623dd837b8031abe151c788f133040aa7f9684ba208` |
| RustFS 1.0.0 | `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` |
| Warp 1.8.0 | `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |

Cairn's staged source diff across blob and server timing files hashes to `e6b9f6f904a013d1dcdfca4827e02eec5f6b6d536fbde4bf71aac6453ef8df7f`. The change is diagnostic-only sampled timers splitting namespace queue/execution and finalization; no durability or storage-layout optimization was adopted. The older `cairn-namespace-only` executable in the same root is **not** used.

Acceptance: all four arms error-free with exact strict-mode readback, scored-interval agreement and available timing counts; report stage means/counts/drops alongside time-aligned host interference and treat nested stages as inclusive. If incomplete or contended, limit conclusions accordingly. Afterward copy the compact result into `docs/`, remove task-owned build and benchmark scratch plus generated web assets, and verify no task-owned servers/clients remain.

## Results

Four arms completed in 173.05 wall seconds. Measured Warp-process intervals summed to **136.55 seconds**, below the authorized 300-second client-load ceiling; these intervals include Warp preparation and teardown, so the actual load is less. The maximum runner-accounted task footprint was **1,380,540,416 bytes**. Each arm exited zero, had zero reported/error-line errors, hit no cap, and had no dropped 2-second host samples. RustFS's measured bucket was read back as strict before and after each control arm. The [raw report](performance-evidence/2026-09/performance-namespace-finalize-diagnostic-raw-2026-09.json) has SHA-256 `469dcf9c792c9f8a4167c3b81fa88b9bf13ae72a56e3050fe94b0acacd4efbba`.

| Order | Engine | Warp 1-MiB PUT, MiB/s | Warp average / p99 request latency | Peak server RSS | Runner-accounted peak bytes |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Cairn | 34.30 | 433.1 / 768.7 ms | 36.8 MB | 849 MB |
| 2 | RustFS | 80.46 | see raw report | 263.0 MB | 1,381 MB |
| 3 | RustFS | 75.12 | see raw report | 291.6 MB | 1,319 MB |
| 4 | Cairn | 51.11 | see raw report | 37.4 MB | 1,105 MB |

The paired Cairn/RustFS ratios are **0.426** and **0.680**; the two-arm median ratio is **0.549** (42.705/77.79 MiB/s). This is a diagnostic result, not an adoption claim. The sampled Cairn stages below are *mean wall milliseconds per sampled object*, 14 samples in arm 1 and 20 in arm 4, with zero dropped timing records. Inclusive stages overlap; they are not additive CPU shares.

| Cairn stage | Arm 1 | Arm 4 |
| --- | ---: | ---: |
| Namespace total | 49.97 | 19.10 |
| Namespace job queue | 0.22 | 0.52 |
| Namespace execution | 47.76 | 13.63 |
| Finalize total | 50.20 | 38.01 |
| Finalize queue | 0.15 | 0.70 |
| Finalize file sync | 40.91 | 15.82 |
| Directory sync | 45.15 | 25.77 |
| Body total | 44.62 | 96.94 |

The namespace timer principally measures execution, **not queueing**. The finalize timer likewise is not dominated by waiting for its job slot; the required file-data sync is material. The directory sync stage is another substantial wall wait. Writer telemetry (not sampled by object) showed queue sums/counts of 58.94 s/2048 and 18.56 s/2048, and commit sums/counts of 18.58 s/529 and 13.54 s/1135, for arms 1 and 4 respectively. These are system-level observations on fresh stores, not per-PUT critical-path decomposition. They suggest the most promising next question is how much durable filesystem and SQLite commit work can be safely coalesced while respecting the storage contract, not increasing namespace worker count.

Interpretation limits are important. Warp reported only 9 analyzed seconds per arm despite 12 requested; `warp_operation_timeline` says `unavailable: missing or oversized Warp benchdata` for all four arms. Thus the 2-second host/CPU/PSI samples cover the broader Warp process, **not provably the exact scored interval**. Unrelated process CPU was observed in every arm (16.82–64.85 CPU-seconds over sampled process lifetimes), and host I/O pressure varied sharply. The 34.30→51.11 MiB/s Cairn swing, simultaneous body-time increase, and unequal RustFS process lengths show that this shared-node run cannot apportion the entire gap causally or validate a code candidate. Fix the raw Warp timeline capture, and repeat under a quieter or time-aligned paired protocol before any architecture decision.

Post-campaign control repair: the runner supplied `warp.csv.zst` as the `--benchdata` *base name* and then incorrectly looked for that exact file. Warp 1.8.0 defaults to an aggregate and appends `.json.zst`; its [upstream issue](https://github.com/minio/warp/issues/476) documents that per-request CSV is not emitted by this release's benchmark command. The first parser fix still expected a full-request `operations` array, but Warp's default [realtime aggregate](https://github.com/minio/warp/blob/v1.8.0/pkg/aggregate/live.go) uses `by_op_type`. The runner now supplies a suffix-free `warp` base, reads `warp.json.zst`, selects the PUT aggregate and requires score parity before marking a PUT arm PASS. Warp v1.8's `liveThroughput.asThroughput` also sets the raw `end_time` four seconds after the measured interval because it uses the untrimmed segment count; the parser validates that exact offset and derives the scored end from `start_time + measure_duration_millis` ([pinned source](https://github.com/minio/warp/blob/v1.8.0/pkg/aggregate/live.go)). This is a source-derived interval, not a per-request CSV trace.

The 28 offline regressions pass. A **no-load real-CLI replay** used the official pinned Warp v1.8.0 image, verified the extracted `/warp` SHA-256 as `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`, compressed a synthetic v2 realtime fixture and ran the exact `warp analyze --json --analyze.op=PUT --analyze.dur=1s` path. The parser returned a source-derived 8-second scored interval and 1.00 MiB/s with score parity. This validates the analyzer/parser path, **not** artifact creation by a live Warp benchmark; that still requires a future metered arm. The initial direct publisher download had stalled and its partial file was deleted. The pinned image, extracted binary, fixture scratch and temporary container were also removed after replay. The old four scores are unchanged and their missing timelines cannot be reconstructed because their arm scratch was correctly removed.

Cleanup: the runner reaped each arm server/client and removed all exact arm stores as it proceeded. The task-owned executables, private root and generated build/web assets are removed after preserving this report; no unrelated process or container is targeted.
