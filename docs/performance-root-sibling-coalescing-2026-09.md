# Root-sibling directory barrier coalescing — 2026-09-23

## Candidate and proof obligation

The [live parent-sync diagnostic](performance-parent-sync-live-diagnostic-2026-09.md) observed two `data_root` syncs per sampled ordinary PUT, averaging 8.8–11.1 ms each; namespace queue time was below 0.5 ms. The exact ordinary storage plan names `.staging/<attempt>.tmp`, `<bucket>/<attempt>`, and `.staging/<attempt>.index.tmp`. Path preparation formerly opened and synchronized `.staging`, then opened and synchronized the bucket, although both entries are children of the same root.

The candidate opens all distinct first-level admitted directories, retaining each descriptor's linked shared lock, then synchronizes `data_root` once before traversing below any of them. No file bytes or deeper directory are created until this barrier succeeds. An existing entry does **not** skip the barrier: another creator may have returned `EEXIST` before its own parent sync. For multipart, the root barrier still precedes the separate `.staging` and `multipart` parent barriers before opening the upload session. The file sync, rename, destination-directory sync, hash validation, Writer commit and cleanup order are unchanged. This is per-plan sibling coalescing, not a durable-directory cache, new thread, configuration option or storage-format change; it stays within `CONTRACT.md` and ARCH 8.2.

The failure cases remain conservative. If opening a sibling or synchronizing the root fails, path preparation returns an error before staging any file; the admitted intent remains eligible for exact recovery. Cancellation of the async caller does not release its I/O ownership before the retained blocking job finishes. A concurrent pruner cannot remove an opened directory while its shared lock is held. Each concurrent admission still performs its own post-open root barrier, so this change does not infer durability from another request's unacknowledged sync.

## Implementation and validation status

At measurement time, the candidate was the working tree based on `f6fde5a68c6a5d5acee54f950ce2be8cda547312`; it was not committed or adopted. The exact comparison control copied the same instrumented blob/server sources, web bundle, compiler profile and corrected `Cargo.lock`, but removed only the root-sibling preparation call and helper from `namespace.rs`. A source diff verified that sole runtime-code difference. The unadopted helper was subsequently removed from the working tree. Pinned Rust is 1.97.1.

| Binary | SHA-256 |
| --- | --- |
| Candidate, root sibling coalescing | `d64a62e36679f318d2d693286586442842c038198a1a615dd5810b11b50ccfab` |
| Instrumented pre-change control | `cd79c86b61303bb2be52b72b6ddca5d863866abfe07c98fa87b76cebeac41f2a` |
| Corrected lockfile | `57894a634f9d37c6666bf6febc3a1249401dc260e64f16da43585631fcb24799` |

Regression coverage proves one parent-sync sample per ordinary plan (including on an existing bucket), one per racing ordinary admission, and three ancestry barriers for a multipart part. The final-tree pinned gate passes: `cargo fmt --all --check`; both warnings-denied workspace Clippy legs (including `--all-features`); `cargo nextest run --workspace` (**1,506 passed, five skipped**); `cargo test --workspace --doc`; `cargo audit`; web lint/build and both npm audit levels; and shellcheck plus installer regressions. The independent full `cargo test --workspace` also passed. The focused blob suite passed 105 unit tests and 42 integration tests (two mount-namespace tests ignored); failpoint-enabled blob tests passed 43 integration tests. The real `blob_after_durable` crash-consistency harness passed before and after the lockfile correction: an injected post-blob/pre-metadata task panic was recovered with no orphan or visible object. The benchmark parser's 28 offline regressions pass. `cargo audit` initially failed on existing rustls 0.23.40 ([RustSec advisory](https://rustsec.org/advisories/RUSTSEC-2026-0285.html)), so the lockfile was advanced to rustls 0.23.45 with its compatible `aws-lc-rs`, `aws-lc-sys` and `rustls-webpki` versions. The repeat audit exits zero, with an allowed unmaintained `rustls-pemfile` warning. This dependency correction is part of the exact binaries above and must be held identical in A/B.

## Measurement gate

The previous shared-node ratios varied sharply and cannot serve as a candidate score. The user authorized a separate shared-node diagnostic capped at **300 seconds of measured Warp-process time** and **10,000,000,000 bytes of task-owned scratch**. The preregistered sequence was two alternating 12-second strict 1-MiB PUT pairs of exact control/candidate binaries, then two alternating candidate/RustFS 1.0.0 pairs, all at 16 clients. The Python coordinator maintained one persistent allowance, stopped at the first incomplete/failed arm, and refused allowance reset after interruption. Each runner had its own tighter wall and scratch limits. Every PUT arm had to pass live Warp scored-timeline capture. A 16-second Cairn metrics drain enabled root-sync count/latency and stage-timing checks. Host I/O pressure and unrelated CPU are confounders, not corrections to the scores.

## Completed shared-node diagnostic

All eight arms passed with zero Warp errors, no cap hit, zero dropped load samples, a live source-derived Warp scored interval and score parity. Both RustFS arms were strict on the pre- and post-load readbacks. The ledger charged **171.44 seconds** of measured Warp-process time, leaving **128.56 seconds unused**; the maximum runner-accounted task bytes were **2,452,705,280** (2.45 GB). These figures include Warp preparation and teardown in the time charge, but not server-start/metrics-drain wall time. The scored 2-second host samples cover roughly 6–7 seconds of each 9–10-second scoring interval, not the full interval.

| Sequence | Engine/build | PUT MiB/s | Server peak RSS MiB | Scored host I/O full PSI s |
| --- | --- | ---: | ---: | ---: |
| A/B pair 1 | Control | 77.24 | 37.9 | 1.016 |
| A/B pair 1 | Candidate | 123.04 | 40.0 | 0.426 |
| A/B pair 2 | Candidate | 56.22 | 37.5 | 1.388 |
| A/B pair 2 | Control | 90.76 | 39.7 | 0.792 |
| RustFS pair 1 | Candidate | 48.44 | 42.5 | 1.279 |
| RustFS pair 1 | RustFS | 116.01 | 306.4 | 1.241 |
| RustFS pair 2 | RustFS | 61.70 | 284.0 | 2.789 |
| RustFS pair 2 | Candidate | 97.00 | 38.2 | 0.878 |

The candidate/control ratios are **1.593×** then **0.619×** (two-pair geometric mean **0.993×**). Candidate/RustFS ratios are **0.418×** then **1.572×** (geometric mean **0.810×**). These are observations, not stable treatment effects: even the candidate varied from 48.44 to 123.04 MiB/s, and the reversed RustFS pair flipped the winner. In the A/B pairs the lower I/O-full-pressure arm won, regardless of binary; the RustFS first pair had similar sampled pressure yet RustFS was much faster. The limited sample cannot explain that difference or establish a reliable post-change speedup. The candidate's measured Cairn RSS stayed around 38–43 MiB versus RustFS 284–306 MiB under this workload; that is a workload observation, not a whole-product memory-equivalence claim.

The mechanism did change as intended. For the four A/B arms, sampled `namespace_parent_sync` calls divided by sampled `namespace_execution` calls were **2.00, 1.00, 1.00, 2.00** in sequence. Control's mean namespace execution was **19.16/16.67 ms** and candidate's **4.86/15.13 ms**; their respective parent-sync means were **9.54/8.30 ms** and **4.80/15.03 ms**. The two later candidate arms had one root-sync call each per sampled ordinary PUT, but parent-sync means rose to **19.53/9.16 ms** in the changing host conditions. Thus removing one barrier reduced the number of durable syncs, but remaining sync latency and other pipeline costs still dominate under contention. Queue means stayed below **0.46 ms** in A/B, so a larger namespace worker pool is not the evidence-led next move. The sampled sync measurement is nested inside namespace execution and must not be added to it.

The retained metrics also show what remained after root coalescing. These are **means across separately sampled calls**, not an additive per-request critical path. Writer commit covers all mutation families in each arm, including work outside the sampled PUTs.

| Candidate PUT score MiB/s | Root parent sync ms | File sync ms | Final directory sync ms | Writer commit ms |
| ---: | ---: | ---: | ---: | ---: |
| 123.04 | 4.80 | 10.73 | 12.41 | 11.32 |
| 56.22 | 15.03 | 30.51 | 28.66 | 24.90 |
| 48.44 | 19.53 | 30.32 | 35.56 | 29.19 |
| 97.00 | 9.16 | 13.46 | 15.00 | 18.23 |

The large, jointly moving sync latencies are consistent with a shared storage-latency limit, but do not prove which device operation caused the throughput swings. The file sync and post-rename directory sync are mandatory under `CONTRACT.md`; deleting or weakening either is not an optimization option. Writer commit latency includes its own durable SQLite work. The present evidence does not justify enlarging worker pools or adding a cross-request root-sync scheme; the earlier cross-request screen failed its end-to-end gate. A longer paired campaign with host sampling and protected workloads is needed before an adoption decision or another production-path edit.

Compact retained evidence: [one-ledger report](performance-evidence/2026-09/performance-root-barrier-campaign-ledger-2026-09.json) SHA-256 `48f6c358bc8173e76938f5357876c695dbc79a035d673cff15565c9cf67198a9`; [candidate/control raw JSON](performance-evidence/2026-09/performance-root-barrier-put-ab-raw-2026-09.json) SHA-256 `19129e1caef56b2e868776e3a2f293ec9f305ee887d49845e0e8d9d07e6b003b`; [candidate/RustFS raw JSON](performance-evidence/2026-09/performance-root-barrier-put-rustfs-raw-2026-09.json) SHA-256 `9a9fc7e3f03e8e7823fd8ae42b4a90b3fd7d9c62f8535f29b677b67a886a8c68`.

This diagnostic did **not** include mixed/GET/LIST and cannot alone justify adoption. The later,
separately authorized [five-pair adoption campaign](performance-root-barrier-adoption-2026-09.md)
ran those protected checks but failed the performance decision amid contradictory PUT pairs,
host-pressure confounding, mixed PUT-latency warning and the persistent strict RustFS gap. Neither
campaign's unused allowance was silently reused.

Cleanup verification: after copying and hashing the three retained JSON files, the exact private `/var/tmp/cairn-performance-rootbarrier-eHJ2kU` directory and its candidate/control/RustFS/Warp binaries, release archive, and per-arm scratch were deleted. The pinned task-pulled Warp image was untagged/deleted. No task-owned benchmark process or container remained; no unrelated image, process, or store was removed. The 128.56 seconds unused are closed, not carried forward.
