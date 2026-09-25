# Root-sibling barrier adoption campaign — 2026-09-24

## Preregistration before client load

The user authorized one new shared-node campaign, separate from the closed 300-second diagnostic, capped at **600 seconds of summed measured Warp-process time** and **10,000,000,000 bytes of task-owned scratch**, including build preparation, downloads, binaries, per-arm stores and retained temporary reports. The exact private root is `/var/tmp/cairn-performance-adoption3-SWwE38`. The coordinator persists one ledger and refuses reset after interruption. A failed arm stops the sequence without retry. Every server runs alone on loopback with a fresh store; the runner reaps its process group and removes each exact arm store. Retain compact nonsecret JSON evidence in `docs/`, then remove the campaign root and task-pulled image.

The candidate is the current source snapshot from `f6fde5a68c6a5d5acee54f950ce2be8cda547312`, with the root-sibling preparation change and the already validated instrumentation and corrected lockfile. The control uses the same snapshot, Rust 1.97.1 release profile, production web bundle, and lockfile; its sole runtime difference is removal of the `prepare_root_children` call and helper, restoring two ordinary root barriers. Exact binary hashes must be recorded before the first client process. The RustFS 1.0.0 and Warp v1.8.0 binaries must match the previously pinned publisher artifacts and executable SHA-256 values before admission. No source or build setting changes between arms.

Fixed phase order under `conformance/root_barrier_campaign.py --profile adoption`: five alternating strict 1-MiB PUT Cairn control/candidate pairs, two alternating candidate/RustFS strict PUT pairs, then one Cairn control/candidate pair each of mixed 1 MiB, GET 1 MiB and LIST 4 KiB. Each Warp invocation requests 12 seconds at the fixed case concurrency (PUT/mixed/GET/LIST: 16); mixed weights are GET 45, STAT 30, PUT 15, DELETE 10. The runner retains each PUT's source-derived live scored interval and score parity, two-second host samples, server-process I/O, and fixed-label Cairn stage/Writer metrics after a 16-second drain. Every arm requires zero reported errors, no resource cap hit and exact strict settings; RustFS bucket mode is read back before and after load. The five PUT pairs and protected cases are not automatically an adoption: the gate requires at least 20% median PUT uplift, no contradictory host-drift explanation, zero correctness errors and no material protected-workload/resource regression. Shared-node variation remains a confounder, not a score adjustment.

Build, benchmark and cleanup results are pending. If the preparation root approaches the scratch limit, stop before load rather than treat the cap as measured-store-only. No client-process time from the closed diagnostic is reused.

## Frozen build identities before load

Both binaries completed the default-feature `release` profile with task-owned Rust 1.97.1 and the same production web bundle. The control recompilation removed only the one root-sibling helper and its invocation from the frozen source; the candidate-to-control `namespace.rs` unified diff is SHA-256 `c8485d60d7cc9cb47e3687f13a621aef655b206b0d4d4bbb1681678a3670e27a`. The candidate namespace source is SHA-256 `925969ab037eb6ea5b073fc67332b398d2acfc2b27fb22515f37fa83038f4843`; the shared lockfile is `57894a634f9d37c6666bf6febc3a1249401dc260e64f16da43585631fcb24799`. Both CLI version checks report `cairn 0.1.0-dev`. The 29 offline parser and six campaign-ledger tests pass; this is not a live-client result.

| Executable | SHA-256 |
| --- | --- |
| Candidate Cairn | `41da62b4adf2e92b583966671b1488b3de3a129ef0a8ae4790b13aa2f087d78c` |
| Source-matched control Cairn | `e44f135cbffb7ea4e37fd04595ee9cfb4e9af21fdfa45d3a1a877cef2f981b8a` |
| RustFS 1.0.0 | `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd` |
| Warp v1.8.0 | `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |

The RustFS publisher archive is SHA-256 `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42`; Warp was extracted from the pinned publisher image digest `sha256:594e582a494a05ff8517b23464b5b94654bee71dc42c47ff432d729dcf4ed823`. The [RustFS release listing](https://github.com/RustFS/RustFS/releases) still labels 1.0.0 latest rather than the preview builds on 2026-09-24. Preparation peaked at about 2.9 GB task-owned root scratch before build-cache pruning; the pulled Warp image has reported size 120,449,264 bytes. No Warp load had run at this preregistration point.

## Completed campaign and adoption decision

All **20 arms** in all five fixed phases passed: zero Warp errors, no cap hit, and zero dropped host samples. Every standalone PUT had a live source-derived Warp scored interval with report-score parity; both RustFS arms returned `strict` on both bucket readbacks. The ledger charged **373.833801 of 600 measured Warp-process seconds**, leaving **226.166199 seconds unused and closed**. Maximum runner-accounted task root bytes in any arm were **2,028,818,432** (2.03 GB); preparation root peaked around 2.9 GB before pruning. Even including the separately pulled 120 MB image, this remained below the 10 GB task-owned cap. Warp-process time includes client preparation/teardown, not the server startup or the 16-second Cairn metrics drain. The 2-second host/process samples completely cover only about 6 seconds of each 9–10-second scored PUT interval, so they are diagnostic rather than a full-window adjustment.

| PUT pair | Control MiB/s | Candidate MiB/s | Candidate/control | Control scored I/O full PSI s | Candidate scored I/O full PSI s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 46.46 | 121.66 | 2.619× | 1.240 | 0.346 |
| 2 | 48.15 | 91.98 | 1.910× | 1.489 | 0.836 |
| 3 | 82.48 | 59.98 | 0.727× | 0.852 | 1.204 |
| 4 | 44.58 | 112.64 | 2.527× | 1.377 | 0.454 |
| 5 | 85.17 | 51.22 | 0.601× | 1.042 | 1.448 |

The control/candidate medians across arm scores are **48.15/91.98 MiB/s**, and the median paired ratio is **1.910×**. That favorable descriptive median does **not** qualify the candidate: two of five pairs reverse the result, and in all five pairs the arm with lower sampled host I/O `full` pressure wins regardless of binary. This is not proof that pressure caused the score difference, but it prevents attributing the 91% median uplift to the code change. The sampled ordinary-object metrics do confirm the mechanism in every arm: control has exactly two `namespace_parent_sync` calls per sampled namespace execution and candidate exactly one. On slow candidate arms the remaining root barrier, file sync, final directory sync and SQLite Writer commit all became materially slower. The namespace queue remained small; removing a second barrier did not remove the shared-durability latency.

| Nearby strict PUT pair | Cairn candidate MiB/s | RustFS 1.0.0 MiB/s | Cairn/RustFS |
| ---: | ---: | ---: | ---: |
| 1 | 49.11 | 105.16 | 0.467× |
| 2, reversed order | 50.62 | 108.89 | 0.465× |

The competitor gap is **not closed** in the nearby reference: Cairn delivered about **46.6%** of RustFS's measured strict PUT throughput in both pairs. Candidate Cairn's PUT-arm peak RSS was around 36–42 MiB, versus RustFS's 288–314 MiB in the two reference arms; these are workload-specific process peaks, not feature-equivalent whole-product footprints. Across the five Cairn A/B PUT arms, median server peak RSS was **38.03 MiB control** and **39.15 MiB candidate**. The new process-I/O counters are captured in the raw reports and show the server's own physical write bytes alongside host-wide diskstats/pressure; they are not request-attributed or a device-latency trace.

| Protected workload (one pair each) | Control | Candidate | Candidate/control |
| --- | ---: | ---: | ---: |
| Mixed 1 MiB aggregate MiB/s | 176.79 | 168.61 | 0.954× |
| Warm GET 1 MiB MiB/s | 2,159.31 | 2,144.58 | 0.993× |
| LIST 4 KiB objects/s | 106,395.94 | 108,132.10 | 1.016× |

Mixed aggregate throughput is 4.6% lower, but the single pair's PUT component mean request latency rises from **222.7 to 292.8 ms** (31.5%). The component's scored rate falls from **44.56 to 42.55 objects/s**. This is another protected-workload warning, not a stable causal regression estimate on the shared node. The warm GET/LIST single pairs have no >10% throughput loss. Do not report p99 as a capacity claim: these arms did not establish 10,000 successful requests per operation.

**Decision: do not adopt this candidate as a demonstrated performance fix.** The preregistered ≥20% five-pair uplift cannot be credited amid two contradictory pairs and perfectly aligned sampled host-pressure ordering, mixed PUT latency is worse in its lone pair, and the nearby RustFS strict gap remains roughly twofold. The source change was an unadopted working-tree experiment, not a release claim; the root-sibling helper and its invocation were subsequently removed while retaining independent diagnostic timing. Another load campaign must be separately authorized and should control host I/O interference or use a more diagnostic paired design; unused time from this ledger is not a new allowance.

Compact evidence, copied before cleanup: [ledger](performance-evidence/2026-09/performance-root-barrier-adoption-ledger-2026-09.json) SHA-256 `ea5081b6db18d1affb75eeafa023dbea12ac87e8356f8f63c35ad3d687c2b63e`; [five-pair PUT A/B](performance-evidence/2026-09/performance-root-barrier-adoption-put-ab-raw-2026-09.json) `a38f268c682216425e0e7da9d904977d080bba30ce9fd8fcdf1fc3c49d814e52`; [strict RustFS PUT](performance-evidence/2026-09/performance-root-barrier-adoption-put-rustfs-raw-2026-09.json) `169490b809f878ebad4edd2bfab7dca09ca25ee763dd4e7c6bbdaab354b64ed6`; [mixed](performance-evidence/2026-09/performance-root-barrier-adoption-mixed-raw-2026-09.json) `04c08d47fde9ab20f2de53659a1ae818cbb1a93282c39a61658ffef7d3251043`; [GET](performance-evidence/2026-09/performance-root-barrier-adoption-get-raw-2026-09.json) `eaeb4b999c9d9ff2fb6376371ca009b20c4459da53169048d58d2e1d95b63710`; [LIST](performance-evidence/2026-09/performance-root-barrier-adoption-list-raw-2026-09.json) `4082aeb97405fc84c1bd7b6e3441140d36f3370bc13a947c9b95c219fb528037`.

Cleanup verification: after copying and hashing all six reports, the exact private `/var/tmp/cairn-performance-adoption3-SWwE38` root was removed, including both Cairn builds, the RustFS/Warp binaries, build cache/toolchain/downloads and per-arm stores. The task-pulled Warp image was untagged/deleted; the temporary extraction container had already been removed. Process checks found no task-owned Cairn, RustFS or Warp server/client. No unrelated process, container, image or store was removed. The 226.166199 unused client-process seconds are closed rather than carried forward.
