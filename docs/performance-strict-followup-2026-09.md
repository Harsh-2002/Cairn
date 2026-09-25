# Strict-mode performance follow-up — 2026-09-23

This is the next measured revision of the [1-MiB PUT investigation](performance-put-1m-strict-2026-09.md), not a production optimization. It uses a newly built, unmodified Cairn release from the same source revision and adds six protected workloads. The earlier 1.93× PUT claim remains an unverified-durability historical observation; it must not be compared directly with this strict-mode run.

## Method and identity

One 4-vCPU KVM host, loopback HTTP, one engine at a time, fresh data directory and bucket per arm, Warp v1.8.0, 12-second requested arms (9–10 seconds analyzed), three alternating-order pairs per workload. Concurrency was 16 except 4-KiB PUT (32). Each RustFS bucket had its effective mode read back as `strict` before and after traffic. Cairn used `CAIRN_META_SYNCHRONOUS=full`, one metadata shard, and zero Writer linger. All 36 arms passed with zero reported operation errors. The VM and disk were not exclusively reserved; results are a paired diagnostic, not a statistically isolated capacity claim or a proof of equal crash behavior.

| Binary | SHA-256 |
| --- | --- |
| Cairn `f6fde5a6`, default-feature release, Rust 1.97.1, real web bundle | `f9ce0f52354036b6e8a3c9319247aa15c4beb1582a4c8c5221fda89a5fe5e2e9` |
| RustFS 1.0.0 release executable | `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` |
| Warp v1.8.0 | `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |

The Cairn executable differs byte-for-byte from the preceding strict report's rebuild; the source revision is the same. Only within-run comparisons below are paired. Rate is Warp's analyzed-window average. LIST rates count **objects returned**, not requests. Medians are across three arms per engine; the pair-win column counts which engine had a higher rate in the same fresh-store pair.

| Workload | Unit | Cairn arms | RustFS arms | Cairn median | RustFS median | Cairn/RustFS | Cairn pair wins |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| 1-MiB PUT | MiB/s | 79.41, 76.27, 59.43 | 70.75, 89.42, 82.31 | **76.27** | **82.31** | 0.927× | 1/3 |
| Mixed 1-MiB, GET/STAT/PUT/DELETE 45/30/15/10 | MiB/s | 148.06, 126.83, 178.48 | 199.86, 166.13, 181.34 | **148.06** | **181.34** | 0.816× | 0/3 |
| 4-KiB PUT | requests/s | 472.56, 132.60, 460.96 | 351.39, 355.91, 470.30 | **460.96** | **355.91** | 1.295× | 1/3 |
| Warm 1-MiB GET, 64 objects | MiB/s | 2142.89, 2214.46, 2065.22 | 1343.81, 1349.71, 1277.87 | **2142.89** | **1343.81** | 1.595× | 3/3 |
| 4-KiB HEAD | requests/s | 9731.08, 9610.80, 10465.70 | 9326.21, 9461.15, 9779.52 | **9731.08** | **9461.15** | 1.029× | 3/3 |
| LIST over 1,000 4-KiB objects | objects/s | 107281.30, 84475.26, 100876.30 | 17665.16, 17992.30, 16514.87 | **100876.30** | **17665.16** | 5.710× | 3/3 |

Median per-request p50/p99 in milliseconds, across the three arms (Warp output retained while analyzing):

| Operation | Cairn p50 / p99 | RustFS p50 / p99 |
| --- | ---: | ---: |
| Isolated 1-MiB PUT | 269.6 / 453.3 | 219.1 / 417.6 |
| Mixed 1-MiB PUT | 347.3 / 556.6 | 181.9 / 459.0 |
| Mixed 1-MiB GET | 4.2 / 23.7 | 6.4 / 46.8 |
| Mixed STAT | 1.3 / 16.4 | 2.3 / 34.5 |
| Mixed DELETE | 147.0 / 242.5 | 130.5 / 322.7 |
| Isolated 1-MiB GET | 6.7 / 21.7 | 11.7 / 27.1 |
| Isolated 4-KiB HEAD | 1.2 / 7.3 | 1.5 / 6.1 |
| 1,000-object LIST | 8.5 / 31.5 | 55.5 / 94.0 |

Median process peak RSS was 38.0 MiB Cairn versus 282.2 MiB RustFS in 1-MiB PUT, 53.2 versus 286.0 MiB in mixed, and 53.5 versus 270.9 MiB in warm GET. These are process high-water marks sampled across an arm, including preparation; they are not heap-allocation costs or steady-state memory guarantees. The 4-KiB PUT outlier and reversal of the median ranking versus pair wins warn against a claim that Cairn is decisively faster there.

## Bottleneck inference and decisions

The write-side latency is the remaining actionable difference. In all three mixed pairs Cairn's PUT p50 exceeded RustFS's, while Cairn's GET and STAT p50 were lower in all three. [Warp's mixed worker loop](https://github.com/minio/warp/blob/v1.8.0/pkg/bench/mixed.go) uses a fixed concurrency pool and each worker waits for its chosen operation before selecting the next. Therefore slow PUT/DELETE responses consume client slots and depress *all* mixed operation counts, including GET; the mixed GET throughput alone does not identify a GET-server bottleneck. This is an inference from the client implementation and observed latency, not an exclusive causal decomposition of Cairn's write path. The earlier temporary blob timers suggested namespace and fsync waits, but failed the ≤3% overhead gate and were reverted; they do not support an optimization claim.

We screened the existing Writer group-commit linger setting with a same-binary, FULL-durability Cairn A/B: 0 versus 1,000 µs, three alternating 12-second pairs. The zero-linger rates were 96.22, 52.25 and 101.64 MiB/s (median 96.22); 1,000-µs rates were 82.70, 60.40 and 51.44 MiB/s (median 60.40). Linger won only one pair and **was not adopted**. The wide variation means the magnitude should not be interpreted as a stable 37% regression either. Zero remains the default in this campaign.

Next optimization work should first obtain low-overhead, scored-window attribution of namespace barriers, file sync/rename, final directory sync, and Writer admission/commit/cleanup under a quieter host or dedicated disk. Only then select one strict-contract-preserving change, paired-screen it against the exact same binary/settings except for that change, and run crash consistency plus protected workload gates. Neither the namespace batching candidate nor added hot-path timers from the preceding revision passed its adoption gate. No production Rust optimization from this investigation remains in the worktree.

## Bounds, validation and cleanup

The six workload invocations consumed 837.27 seconds, and the linger A/B 107.95 seconds. Together with the prior 2,529.74 seconds, about **3,474.96 of 3,600 measured seconds** were used; about **125.04 seconds remain**. The harness's time/space caps were respected in this follow-up. Python harness unit tests: 14 passed. No Rust source was changed in this follow-up; the prior full-workspace-test and `cargo audit`/`nextest` limitations remain as recorded in the preceding report. Scratch binaries, data directories, web build products, and task-owned Docker images were removed after recording these results; no benchmark process was left running. The exact deleted scratch outputs are not recoverable, while the scripts and score report remain in the repository.
