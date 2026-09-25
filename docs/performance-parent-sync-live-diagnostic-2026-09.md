# Shared-node parent-sync and Warp-timeline diagnostic — 2026-09-23

## Preregistration (before client load)

The user authorized a new, separate diagnostic campaign capped at **300 seconds of measured client-process time** and **10,000,000,000 bytes of task-owned scratch**. This campaign does not claim a quiet host, full S3/durability equivalence with RustFS, or an optimization adoption result. It will stop on a failed arm without retrying or resetting either allowance. Build/download preparation is not client load; the benchmark runners additionally enforce their own wall and scratch bounds. Every server runs on loopback with a fresh task-owned store, one at a time; each arm is reaped and its exact store removed before the next. Keep compact JSON evidence, then remove task-owned binaries/builds, image, generated web assets and any remaining scratch.

The primary paired test is two alternating strict 1-MiB PUT pairs, Cairn→RustFS then RustFS→Cairn, with 12 seconds requested Warp v1.8.0 load, 16 clients, and a 16-second Cairn metrics drain. Both engines use their strict modes; RustFS's global and freshly created bucket settings must be verified before and after its arms. An arm passes only with zero Warp errors, no cap hit, and a live `warp.json.zst` aggregate successfully reduced to the source-derived scored PUT interval with benchmark-score parity. The 2-second process/host samples are then aligned to that interval; incomplete coverage and unrelated CPU/pressure remain explicit confounders.

An auxiliary one-pair A/B compares the current instrumented Cairn build against an unmodified `HEAD` build made with the same pinned Rust toolchain, release profile and embedded web bundle. It screens timing overhead only; one pair on this shared node cannot establish a small overhead bound. A/B and Cairn/RustFS client-process time are added for the same 300-second ledger. Run the live timeline validation first through a scored PUT arm; if it fails, stop, retain evidence and do not spend the remainder on uninterpretable throughput arms.

Exploratory addendum after the first A/B pair, before further client load: that pair scored clean HEAD 108.49 versus instrumented 54.62 MiB/s, while scored-window host I/O `full` pressure was 0.536 versus 1.257 seconds over comparable sampled windows. The instrumented binary previously scored 112.47 MiB/s against RustFS. To test whether the large A/B drop follows binary or order/host state, run exactly one **reversed-order** A/B pair with the same 12-second Warp and strict settings. This is post-hoc diagnostic evidence, not part of a predeclared adoption comparison; preserve the original A/B JSON before the runner overwrites its output and add all client-process seconds to the original 300-second ledger. Do not add more arms after this pair.

| Executable/source | Pinned identity |
| --- | --- |
| Instrumented Cairn release | SHA-256 `fab439182fdf6c49dd81ebf08866ca086b2aa1894cfa5d02ea7d9a6d4154c640` |
| Clean-HEAD Cairn release | SHA-256 `b20cd5ebc7175ea13c4e381711d426856bce0d66848878edd1d1e4e47ccbd086` |
| Cairn HEAD | `f6fde5a68c6a5d5acee54f950ce2be8cda547312` |
| Instrumentation source diff | SHA-256 `2dfc6bf50c60b28beafcf000874e500b19a42be63ed1af8e35669abb8100d3d9` |
| Rust toolchain | `1.97.1-x86_64-unknown-linux-gnu`, project-pinned |
| RustFS 1.0.0 | executable SHA-256 `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`; publisher ZIP SHA-256 `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42` |
| Warp 1.8.0 | executable SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`, extracted from publisher image digest `sha256:594e582a494a05ff8517b23464b5b94654bee71dc42c47ff432d729dcf4ed823` |

## Results

Eight arms completed, all `PASS`: zero Warp operation errors, no cap hit, no dropped 2-second host samples, and a live v2 Warp aggregate with source-derived scored-span/score parity in every arm. RustFS's bucket was strict on both readbacks per arm. The summed measured Warp-process time, **including Warp preparation and teardown**, was **174.45 seconds**, leaving **125.55 seconds** of the authorized 300-second client-process cap unused. Maximum runner-accounted task bytes in an arm were **1,796,816,896** (1.80 GB), below 10 GB. The primary, first A/B, and reverse-order A/B runners took 143.44, 66.72, and 66.5 wall seconds respectively; their wall time is not added to the client-process ledger.

| Order/group | Binary | Strict 1-MiB PUT | Server peak RSS | Scored host-sample coverage | Scored-window I/O `full` PSI |
| --- | --- | ---: | ---: | ---: | ---: |
| Primary 1 | Instrumented Cairn | 80.45 MiB/s | 37.7 MiB | 8.11 / 10 s | 1.524 s |
| Primary 2 | RustFS 1.0.0 | 80.26 MiB/s | 276.9 MiB | 6.31 / 9 s | 1.463 s |
| Primary 3 | RustFS 1.0.0 | 77.14 MiB/s | 306.3 MiB | 6.26 / 10 s | 1.313 s |
| Primary 4 | Instrumented Cairn | 112.47 MiB/s | 40.6 MiB | 6.09 / 9 s | 0.520 s |
| Initial A/B 1 | Clean HEAD | 108.49 MiB/s | 38.6 MiB | 6.10 / 9 s | 0.536 s |
| Initial A/B 2 | Instrumented Cairn | 54.62 MiB/s | 36.3 MiB | 6.05 / 9 s | 1.257 s |
| Reversed A/B 1 | Instrumented Cairn | 90.14 MiB/s | 38.3 MiB | see raw | 0.539 s |
| Reversed A/B 2 | Clean HEAD | 66.88 MiB/s | 39.1 MiB | see raw | 1.148 s |

The primary paired Cairn/RustFS ratios are **1.002** and **1.458**. The first A/B instrumented/clean ratio is **0.503**, but after reversing order it is **1.348**. In each A/B pair the second arm suffered roughly twice the scored-window I/O `full` pressure and lost throughput, regardless of which binary came second. The sampled unrelated-process CPU was nonzero in every scored window (0.72–1.56 CPU-seconds in the four A/B arms). Thus this shared-node screen **does not establish either an instrumentation regression or a performance win**; it does show that the old near-half-speed observation is not a stable binary-specific ceiling. The score and pressure alignment are descriptive correlation, not causal correction. In particular, the 2-second sampling covers only about 6–8 seconds of each 9–10-second scored interval.

The new `namespace_parent_sync` metric resolves the namespace stage. Means below are wall milliseconds per sampled call, with one-in-32 sampled object writes, zero dropped timing records, and **two parent-sync calls per sampled object**. The parent-sync sum is nested inside namespace execution; it must not be added to that stage as an independent cost.

| Instrumented Cairn arm | Namespace queue / execution | Parent sync mean (calls) | Finalize file sync | Post-rename directory sync | Body total |
| --- | ---: | ---: | ---: | ---: | ---: |
| Primary 1, 80.45 MiB/s | 0.32 / 18.97 ms | 9.45 ms (62) | 15.35 ms | 21.99 ms | 29.58 ms |
| Primary 4, 112.47 MiB/s | 0.47 / 17.70 ms | 8.80 ms (80) | 11.12 ms | 12.29 ms | 29.72 ms |
| Initial A/B 2, 54.62 MiB/s | 0.16 / 23.15 ms | 11.06 ms (50) | 21.45 ms | 30.70 ms | 26.26 ms |
| Reversed A/B 1, 90.14 MiB/s | 0.37 / 18.40 ms | 9.17 ms (64) | 14.04 ms | 12.25 ms | 29.87 ms |

Repeated parent-directory durability barriers, not namespace job queueing, account for nearly all of measured namespace execution in these samples. The current `ensure_directory` synchronizes the parent after opening even an existing directory, because an `EEXIST` race does not prove that another creator has completed its parent barrier. That is a concrete design cost, but eliding it requires an exact same-node durable-generation/ownership proof and crash/cancellation coverage; the fixed stage→file sync→rename→directory sync→metadata-commit durability order remains unchanged. Likewise, the separate file and post-rename directory syncs remain material and cannot simply be skipped under `CONTRACT.md`. This campaign made **no storage-format or durability change** and does not qualify an adoption decision.

Compact retained evidence: [primary four-arm JSON](performance-evidence/2026-09/performance-parent-sync-live-primary-raw-2026-09.json) SHA-256 `d13f26f61c098f8c7d451d3a5a3641aeff90838f5843ddd393993d73d5fbcffd`; [initial A/B JSON](performance-evidence/2026-09/performance-parent-sync-live-overhead-raw-2026-09.json) SHA-256 `deca3a6e756f4cd7368f465f1f5327957c51f22e31dcaf1605e69421f9c72436`; [reversed A/B JSON](performance-evidence/2026-09/performance-parent-sync-live-overhead-reversed-raw-2026-09.json) SHA-256 `dd8db8d2dcd7dc12cbc8da2aa9905be24804207856b9c6032dee5685455b6b8d`. No individual object bytes or credentials are retained in these reports.

Cleanup verification: the exact private `/var/tmp/cairn-performance-nextdiag-ukMx4M` root, its arm stores, build caches and binaries, and the task-generated `web/node_modules`/`web/dist` were removed after copying the reports. The pinned task-pulled Warp image was untagged/deleted, its temporary extraction container had been removed, and a process check found no task-owned Cairn, RustFS or Warp process. No unrelated process, store, container or image was removed.
