# Strict 1 MiB PUT stage-timing diagnostic — 2026-09-25

The user authorized running the diagnostic on this shared node without another approval request. This is a new, non-reusable campaign, separate from earlier closed ledgers. The private task root is `/var/tmp/cairn-performance-timing-ni5ZpdEu`. Cap summed measured Warp-process time at **300 seconds** and task-owned scratch (builds, downloads, temporary stores, reports) at **10,000,000,000 bytes**. Retain compact nonsecret evidence here, then remove the task root and any task-pulled image. Do not alter unrelated data or services.

Use `conformance/root_barrier_campaign.py --profile put_timing`: three alternating 12-second strict 1 MiB PUT control/instrumented Cairn pairs, followed by one strict instrumented Cairn/RustFS pair, with a 16-second metrics drain. Control and instrumented sources must be identical except the PUT protocol-stage timing probe; the blob-stage timing probe remains in both. Every arm requires exact executable hashes, strict bucket durability, zero client errors, live source-derived Warp timeline, loss-free equal-population timing stages and stable host capacity. A failed phase stops the campaign without retry. The shared-node host may introduce substantial variance, so stage fractions diagnose mechanisms but paired throughput is not an adoption claim.

At preflight, `/proc/meminfo` reported `MemTotal: 11024800 kB`, `MemAvailable: 8318316 kB`, and no swap. This differs from the previously observed 12,255,072 kB total; host-capacity drift is an explicit invalidation condition.

The release builds used pinned Rust 1.97.1, the same production web bundle and lockfile SHA-256 `57894a634f9d37c6666bf6febc3a1249401dc260e64f16da43585631fcb24799`. Recursive source comparison found exactly four differing paths: `cairn-protocol/src/lib.rs`, `cairn-protocol/src/service.rs`, `cairn-server/src/background.rs`, and `cairn-server/src/observability.rs`; all differences remove the PUT protocol-stage timing probe from the control. The one-in-32 blob timing probe remains in both. Control's version string lacks the Git suffix because its build source snapshot has no `.git` directory; its executable behavior is source-matched as described.

| Executable | SHA-256 |
| --- | --- |
| Instrumented Cairn | `df20f397c746d1081ab5cdbaa22d2147609bf34d354c2d1ea50255022b0780e6` |
| Control Cairn | `68afac8ba3e6bb2b2cb015a2f60e59fcb4edee5e9326640e820645611cc428c5` |
| RustFS 1.0.0 | `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` |
| Warp 1.8.0 | `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |

The benchmark scripts passed 39 offline tests immediately before load. Outcomes and cleanup evidence will be appended after execution.

## Pre-load correction and separate VM-balloon diagnostic

The first campaign stopped after its first 16.011-second control Warp process. It reported 45.24 MiB/s, zero errors and a source-derived Warp timeline, but `/proc/meminfo` `MemTotal` moved from 9,759,516 to 9,810,632 kB during the arm and to 9,913,032 kB at phase end. Its exact-capacity gate therefore marked the arm/phase **INCONCLUSIVE**. That ledger is closed with 283.989 measured seconds unused; no remaining time is transferred. The raw report and ledger must be retained before cleanup.

Before any second load, the user’s broad node authorization is used for a **new** 300-second measured-client/10-GB *aggregate task-owned* diagnostic at `/var/tmp/cairn-performance-balloon-snRQxHrf`, reusing the already built exact binaries. The revised, opt-in `--allow-ballooning` policy requires identical logical CPUs and swap and at most 10% `MemTotal` drift relative to the campaign baseline and within every arm; actual values remain in the ledger. The default exact-capacity policy and previous ledger remain unchanged. This correction is solely to obtain a stage-attribution diagnostic on a ballooned VM. Even if all arms pass, the shared-node/ballooned-memory scores cannot by themselves support optimization adoption. The updated offline harness checks pass 41 tests.

## Completed balloon-tolerant diagnostic

All eight arms passed with zero reported Warp errors, live source-derived scored timelines, exact binary/durability settings, no task cap hit, and no dropped or interrupted PUT timing samples. RustFS bucket durability read back `strict` both before and after its arm. The ledger charged **157.139/300 measured client-process seconds**, leaving 142.861 seconds closed and unused. Maximum runner-accounted task bytes were 1,332,023,296 in the second root; the shared binary/build root was 2,676,092,370 bytes, so aggregate task-owned scratch stayed below 4.1 GB and the 10-GB cap. `MemTotal` remained within the opt-in 10% range, but the node was visibly ballooning and shared with other work.

| Sequence | Control Cairn MiB/s | Instrumented Cairn MiB/s | Instrumented/control |
| --- | ---: | ---: | ---: |
| Pair 1 | 59.90 | 43.16 | 0.721× |
| Pair 2, reversed order | 64.00 | 94.05 | 1.470× |
| Pair 3 | 82.25 | 40.92 | 0.498× |

The three-pair geometric mean is 0.808×, but the signs reverse and the candidate itself ranges from 40.92 to 94.05 MiB/s. This **does not estimate instrumentation overhead reliably**. The sole strict candidate/RustFS pair was Cairn **93.06** versus RustFS **63.04 MiB/s** (1.476× Cairn) under different sampled host pressure; it does not refute earlier runs where RustFS won or establish a durable ranking. Their sampled peak server RSS was 36.9 versus 286.8 MiB on that pair, not a whole-product footprint comparison.

The following are **sample-count-weighted histogram means**, across 107 completed PUT samples from four instrumented Cairn arms. The exclusive protocol stages reconcile to the inclusive `total`; separate blob substage samples happen to have the same count here, but cannot be assumed request-aligned in general.

| Protocol stage | Mean ms | Share of 219.67-ms handler |
| --- | ---: | ---: |
| Blob stage | 106.53 | 48.5% |
| Durable storage admission | 40.65 | 18.5% |
| Post-publication audit Writer await | 39.63 | 18.0% |
| Authoritative metadata publication | 32.76 | 14.9% |
| Preflight, publication prep, notification combined | 0.10 | <0.1% |

Within the 106.53-ms blob stage, namespace preparation averaged 28.48 ms, body consumption/hash/write 28.96 ms, file finalization 23.73 ms, and final directory sync 22.75 ms (plus a small staging-create remainder). Two root parent sync calls averaged 13.25 ms **per call**, nested inside namespace preparation; file sync averaged 18.71 ms nested inside finalization. The root/file/final-directory durability waits therefore account for roughly 69.94 ms per sampled write when added at their appropriate non-overlapping level. Namespace queue averaged only 0.62 ms, finalize queue 0.29 ms, and permit wait was negligible; enlarging those worker pools is not evidence-led. Raw-body hash averaged 11.98 ms, much smaller than durability and the three Writer awaits. These are wall waits, not CPU-exclusive times.

The slow instrumented arms (43.16 and 40.92 MiB/s) had mean handler waits of 329.74 and 342.37 ms and scored host I/O-full PSI of 1.574 and 1.724 seconds over roughly six seconds of sampled scored window. The fast arms (94.05 and 93.06 MiB/s) had mean waits of 161.22 and 165.14 ms and corresponding I/O-full PSI of 0.612 and 0.611 seconds. All major durability/Writer stages rose together. This is evidence of a **shared storage-latency/host-interference ceiling**, not proof of which exact physical sync RustFS avoids or of how much Cairn could gain on an isolated disk. The source-derived Warp window covers only part of each scored interval.

### Architectural decision

There is no justified twofold throughput fix to adopt from this campaign. The critical path performs durable storage admission, two root-directory barriers, a file sync, a final-directory sync, authoritative metadata publication, and a separate audit mutation. The observed audit await is real, but a safe single-shard [audit co-commit candidate](performance-audit-cocommit-adoption-2026-09.md) was **already implemented, tested, screened in five paired PUT trials with protected workloads, and rejected/reverted**: it won only two PUT pairs and the LIST protected median was 32.9% lower amid host variation. The new stage data do not overturn that adoption result. Simply moving audit to an unbounded detached task would risk lost events and violate the intent of ARCH 26.3. Even a perfect removal of its 18% critical-path share could at most suggest about 1.22× per-request latency improvement under a naive serial model—not a proven 2× throughput gain at c16.

The fixed ARCH 8/CONTRACT order prevents omitting the file or final-directory sync. A prior root-sibling coalescing candidate removed one root sync mechanically but failed its end-to-end adoption gate under the shared-node variation; do not reinstate it as an assumed win. Next use a dedicated or rate-controlled storage lane if one becomes available, or a paired host-I/O pressure blocking design on this node, before inferring a stable Cairn/RustFS ranking. Keep strict durability and exact source/Binary identity in every arm.

Retained nonsecret evidence (SHA-256): [exact-capacity ledger](performance-evidence/2026-09/performance-put-timing-exact-ledger-2026-09.json) `ba25b15105e02ad88725d92bb184223683137b0321e6ed0842ab2978dab894ad`, [single inconclusive raw arm](performance-evidence/2026-09/performance-put-timing-exact-raw-2026-09.json) `7ec8c807355c963c21ffcb03dae0d7056ee5e0f814085a5d6c28ff4e2210c0f4`, [balloon-tolerant ledger](performance-evidence/2026-09/performance-put-timing-balloon-ledger-2026-09.json) `74b7b1d6f6594ff06aa90759ec5bc9347775b979a422413a0a664d9f838031dc`, [six-arm Cairn A/B](performance-evidence/2026-09/performance-put-timing-balloon-ab-raw-2026-09.json) `d3d42274b7b42a097c3c2aaee52b8b3812d27ce41c49b0f6b72da9479ce80fdc`, and [strict Cairn/RustFS pair](performance-evidence/2026-09/performance-put-timing-balloon-rustfs-raw-2026-09.json) `f7a5d672d0fe461c55a24afa6e6e3ef350c655145cef402ee4081a751a045789`.

Cleanup verified: after retaining and hashing the five JSON evidence files, both exact private task roots were deleted, including toolchain, source snapshot, binaries, release archive, Cargo cache and temporary reports. The task-pulled Warp image was removed after confirming it had no dependent container. No benchmark server/client or task container remains; the unrelated `orva` container and other node work were untouched. Generated `web/node_modules`, bundle assets and TypeScript build-info were removed, and the pre-existing `web/dist/index.html` placeholder was restored byte-for-byte (SHA-256 `fcfc54d1b75f253f6946d02939ef20daf688a8d3a154c519d7b3447cffec0b81`). `git status --short web` is empty.
