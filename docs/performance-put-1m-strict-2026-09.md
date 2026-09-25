# Verified strict-durability 1-MiB PUT control — 2026-09-23

Status: **completed strict control and a five-pair optimization screen; no throughput
optimization adopted**. The [compact machine-readable evidence](performance-evidence/2026-09/performance-put-1m-evidence-2026-09.json)
retains arm rates, outcomes, identities, resource peaks and selected stage metrics. Warp's
per-request benchdata was discarded with each private arm; no p95/p99 inference is made.
This corrects the effective-durability ambiguity in the
[earlier broad comparison](performance-current-2026-09.md) and follows W0 of the
[1-MiB PUT plan](performance-put-1m-plan-2026-09.md).

## Tested binaries and conditions

One 4-vCPU KVM host, loopback HTTP, one fresh store and bucket per arm, path-style SigV4,
Warp v1.8.0 `put --obj.size 1MiB --concurrent 16 --duration 12s`. The engine order was
Cairn→RustFS, RustFS→Cairn, Cairn→RustFS. Only one engine received traffic at a time.
Warp's analyzed intervals were shorter than the requested duration; keep its original
summary rates as the scores. The server/client used separate task-owned processes.
Other host processes and the virtual disk were not exclusively controlled.

| Component | Identity |
| --- | --- |
| Cairn | Source revision `f6fde5a6`, default-feature optimized release built with pinned Rust 1.97.1 and a real web bundle; SHA-256 `d0167b6de6f83f5cd8d8ce812f455ca6de335a234e2269d359712e60cb1dcf08` |
| RustFS | 1.0.0, source `d47f54bfb2f39f48bd1adda334bd27e151fe85b8`; executable SHA-256 `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` |
| Warp | v1.8.0 / `13c3b89`; executable SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |

The child environment discarded inherited `CAIRN_*`, `RUSTFS_*`, `AWS_*` and `WARP_*`
settings. Cairn explicitly set `CAIRN_META_SYNCHRONOUS=full`, one metadata shard and
zero Writer linger; its server connects that setting to `synchronous=FULL`. RustFS explicitly
set both `RUSTFS_DURABILITY_MODE=strict` and
`RUSTFS_NEW_BUCKET_DURABILITY_MODE=strict`, with the legacy drive-sync setting true.
Each RustFS arm precreated the measured bucket and read back `{bucket: cairn-benchmark,
mode: strict}` from its authenticated admin API **before and after** Warp. Warp's own
preparation reuses an existing unlocked bucket. Any missing or different readback would
have rejected the arm. The Cairn configuration was verified through its launched environment
and code path; no separate Writer-connection PRAGMA query was captured.

## Result

| Pair | Cairn MiB/s | RustFS MiB/s | RustFS/Cairn |
| --- | ---: | ---: | ---: |
| 1 | 58.48 | 62.35 | 1.066× |
| 2 | 59.46 | 74.77 | 1.257× |
| 3 | 49.50 | 49.72 | 1.004× |
| Median of arms | **58.48** | **62.35** | **1.066× by displayed medians** |

All six arms completed with zero reported Warp operation errors and retained strict RustFS
readback. Median server peak RSS was approximately 38.1 MiB for Cairn and 281.0 MiB for
RustFS. These are process high-water RSS observations, not controlled heap or per-object
memory costs. The arm rates varied enough that 6.6% is not a stable capacity ranking.
The old comparison's 1.93× ratio reflected launched settings whose effective bucket
durability was not recorded. This corrected run cannot decompose how much of that
historical difference came from durability versus host variation or the rebuilt Cairn
binary. It removes the evidence for an asserted near-2× gap under matched strict modes.

The measured invocation consumed **179.44 seconds** of the original 3,600-second campaign,
after the earlier 1,483.89 seconds. Cumulative consumption is **1,663.33 seconds**,
leaving **1,936.67 seconds** at that point. This allowance has not been reset. The rows above
retain the score summary; the task-owned raw stores and executables have been deleted.
No stage-attribution or new production optimization was part of these six arms.

## Diagnostic stage measurement and rejected namespace candidate

An instrumented Cairn release from the same source revision (SHA-256
`9832cc5d0dac234587f099e0b165f45608ad14e8d2d67b2c14af46a5cac132a7`)
ran one additional 20-second c16 PUT arm at 54.99 MiB/s versus 63.12 MiB/s for a
verified-strict RustFS arm. Its process peaked at 41.5 MiB RSS versus RustFS 272.6 MiB.
The 17-second post-arm metrics drain captured 1,157 blob-stage observations and no dropped
samples. The means below include Warp preparation and any post-score activity; they are **not**
exclusive scored-window latency or additive CPU cost.

| Cairn blob stage | Mean wall ms |
| --- | ---: |
| Write-permit wait | <0.01 |
| Namespace preparation | 34.35 |
| Staging creation | 5.46 |
| Body consumption, hash and write | 30.33 |
| File sync and rename | 30.29 |
| Final directory sync wait | 30.83 |

The existing Writer samples in this same arm showed 1,076 commit observations averaging
23.20 ms and a mean batch size of 6.56 mutation samples per commit. These are shared Writer
batch measurements, not a per-PUT critical-path decomposition. The namespace code opens both
`.staging` and the bucket beneath the same root and currently synchronizes the root after each
open, including for existing directories. That observation motivated a narrowly scoped
candidate which retained both directory locks and used one root barrier before any file
creation. It did not alter final file, destination-directory or SQLite durability barriers.

Five fresh-store A/B pairs used the same instrumented control and the candidate release
(SHA-256 `5d6afd6f1594f3e34c5a2c29a7121bd4572034d5ffd1044df776493b85e1c9f5`),
12-second Warp arms, c16, 17-second metrics drain and alternating order. All ten arms passed
with zero reported operation errors.

| Pair, order | Control MiB/s | Candidate MiB/s | Candidate/control | Control namespace mean ms | Candidate namespace mean ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1, C→N | 48.15 | 122.03 | 2.534× | 35.99 | 9.23 |
| 2, N→C | 96.08 | 52.40 | 0.545× | 13.16 | 18.88 |
| 3, C→N | 54.62 | 52.92 | 0.969× | 33.62 | 21.90 |
| 4, N→C | 94.64 | 72.20 | 0.763× | 18.13 | 15.86 |
| 5, C→N | 47.25 | 56.81 | 1.202× | 45.23 | 17.40 |
| Median of arms | **54.62** | **56.81** | **1.040×** | **33.62** | **17.40** |

The candidate reduced the median namespace-stage mean by about 48%, but won only two of five
end-to-end pairs. The throughput median difference was just 4.0%, far below the plan's 20%
adoption threshold, and individual rates ranged from 47.25 to 122.03 MiB/s. High host I/O
pressure and unrelated VM activity were observed during the sequence. A preceding three-pair
A/B screen was similarly unstable and was not combined with this five-pair table. The
namespace candidate was reverted. There is no defensible claim that this change closed an
end-to-end PUT gap.

The instrumentation itself then faced a separate five-pair c16, 12-second PUT A/B against the
preserved uninstrumented binary, with no post-arm metrics drain for either side. All ten arms
passed with zero reported errors. In pair order (uninstrumented / instrumented, MiB/s):
63.88/49.72, 73.51/62.36, 53.91/88.60, 54.71/50.98, and 70.57/49.57. Arm medians were
**63.88** and **50.98 MiB/s**, respectively; the instrumented binary won one of five pairs.
Host variability prevents assigning the full observed difference to the timers, but this
decisively fails the plan's ≤3% measured-overhead gate. The hot-path timers and their metrics
were therefore also reverted; no production Rust change from this screen remains. The saved
diagnostic measurements remain useful for choosing a less intrusive future profiler.

The subsequent measured diagnostic and A/B work consumed 92.32 + 28.67 for an aborted harness
serialization attempt + 209.20 for the initial three pairs + 350.12 seconds for the five-pair
screen. Including the earlier broad campaign and strict control, the original 3,600-second
measurement allowance has consumed about **2,343.64 seconds** and has about **1,256.36
seconds** left. It has not been reset. The aborted attempt's result was excluded and its store
and process were cleaned; all later arms were fresh. This budget counts benchmark invocations,
not compile or validation time. The instrumentation-overhead A/B added **186.1 seconds**,
bringing campaign consumption to about **2,529.74 seconds** and leaving about **1,070.26
seconds**. No more benchmark arm is planned under this allowance until host isolation and a
lower-overhead attribution method are available.

Validation caveat: a full `cargo test --workspace` build was stopped before execution when its
generated debug target approached the task-owned 10-GB footprint cap. Compilation raced the
monitoring interval and the debug directory briefly reached about 11 GB, exceeding that cap;
the exact generated debug directory was deleted, returning the private root to 1.9 GB. The
full-workspace test gate is therefore **not green**. Targeted release tests and the other gate
results must be reported separately; this capacity incident must not be hidden by the later
cleanup.

Validation that completed: `cargo fmt --all --check`, default and all-features workspace
Clippy with warnings denied, the release `cairn-blob` unit/integration/doctest suite, all ten
Python harness tests, web lint/build and both npm audits, and installer ShellCheck/regression
tests passed. The broad workspace test build did not reach execution, `cargo nextest` and
`cargo audit` were not installed, and no crash-consistency or protected-workload matrix was
run for the rejected candidate. All production Rust edits were reverted after their
measurements; the final worktree changes are the benchmark harness and documentation only.

Final cleanup removed the exact private `/var/tmp/cairn-performance-put1m-4Zr517` directory,
task-generated `web/node_modules`, `web/dist` and Python bytecode, and the Rust Docker image
pulled only for this build. No benchmark process or task-owned Docker container remains;
the unrelated existing container was untouched. These generated artifacts are not recoverable
from the workspace, while the compact evidence and scripts remain in this repository.
