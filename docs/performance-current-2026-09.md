# Current-head Cairn versus RustFS — 2026-09-23

Status: **completed descriptive comparison; INCONCLUSIVE for optimization adoption or bottleneck attribution**. This is the first current-checkout measurement under [the performance task tracker](performance-execution-2026-09.md). It is not a before/after result: no Cairn executable code was changed for this campaign. The tested Cairn source is `f6fde5a6` and the production binary was built before the comparison scripts or this report were added.

**Research correction (2026-09-23): effective durability was not pinned or recorded.** The exact tested RustFS source seeds newly created buckets with a `relaxed` override, despite a global `strict` default. The launchers created fresh stores/buckets and did not set the new-bucket override; they also inherited parent environment settings. Thus default-relaxed is the expected RustFS behavior, but the historical runtime mode cannot now be verified. Its relaxed mode omits commit metadata/directory syncs. The table remains an observation of the launched configurations, not a strict-durability comparison. The [pinned upstream guide](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/docs/operations/durability-modes.md) and [new-bucket default source](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/bucket/durability.rs) establish this configuration concern. The [1-MiB PUT plan](performance-put-1m-plan-2026-09.md) requires explicit settings and runtime verification before drawing a durability-matched ranking; no corrected score has been measured yet.

## Environment and exact executables

One KVM host, four Skylake-exposed vCPUs, 7.8 GiB RAM/no swap, Debian Linux 6.12, one rotational-advertised ext4 virtual disk shared by `/var/tmp` and the checkout. Both engines used loopback HTTP, path-style SigV4, one fresh single-node/single-disk store per arm and no console. Only one server received a workload at a time. Neither host page cache nor unrelated host activity was controlled. Process-cold is not cache-cold. Native binaries avoided Docker storage/network layers during measurement; Docker was used only to build Cairn because the host lacks a C linker.

| Component | Identity |
| --- | --- |
| Cairn | `0.1.0-dev+gf6fde5a6`; SHA-256 `7f239fcd912875addcc385e755462ac6788492b5bc127c77d777a9bd2cb2cd3e`; default-feature optimized release, real embedded web bundle, pinned Rust 1.97.1 |
| RustFS | [1.0.0 release](https://github.com/rustfs/rustfs/releases/tag/1.0.0), commit `d47f54bfb2f39f48bd1adda334bd27e151fe85b8`; executable SHA-256 `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`; publisher archive SHA-256 `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42` |
| Warp | [v1.8.0 Linux/amd64](https://dl.min.io/aistor/warp/release/linux-amd64/), commit `13c3b89`; executable/publisher SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |
| Fallback client | Repository [`rustfs_s3_compare.py`](../conformance/rustfs_s3_compare.py), Python 3 standard library, SigV4, exact response checks, multipart SHA-256 verification |

The Warp matrix used three fresh-store pairs per case and reversed order in pair 2: Cairn→RustFS, RustFS→Cairn, Cairn→RustFS. Each requested a 12-second run at the concurrency and pool sizes preregistered in [P1](performance-execution-2026-09.md#p1-preregistered-current-head-comparison). Warp's `Ran` interval is generally 9–10 seconds after its own analysis exclusions. The Python fallback used the same three-pair order and 12-second interval. Medians below are medians of the three engine arms; the trial triplets retain original pair order. Ratios are **ratio of displayed medians**, not a paired estimator. A minimum of five paired trials, controlled drift and protected-resource checks would be needed to adopt a change; these three-pair values do not pass that decision gate.

## Throughput result

| Workload | Cairn median | RustFS median | Median ratio / interpretation |
| --- | ---: | ---: | --- |
| PUT, 4 KiB, c32 | 357.84 objects/s | 692.42 objects/s | RustFS 1.94× |
| PUT, 1 MiB, c16 | 47.84 MiB/s | 92.17 MiB/s | RustFS 1.93× |
| GET, 4 KiB, c16 | 7,418.14 objects/s | 6,598.32 objects/s | Cairn 1.12× |
| GET, 1 MiB, c16, warm pool | 1,950.80 MiB/s | 1,236.91 MiB/s | Cairn 1.58×; page-cache/client effects matter |
| HEAD, 4 KiB, c16 | 9,687.78 requests/s | 8,600.69 requests/s | Cairn 1.13× |
| LIST, 1,000 × 4-KiB pool, c16 | 96,082.84 listed objects/s | 16,225.86 listed objects/s | Cairn 5.92×; unit is returned objects, not requests |
| DELETE, 4 KiB, c16 | 456.82 objects/s | 488.98 objects/s | RustFS 1.07× by arm medians; noisy, effectively unresolved |
| Mixed, 1 MiB, c16 | 197.06 MiB/s | 219.98 MiB/s | RustFS 1.12× by arm medians; large Cairn drift |
| Complete two-part multipart cycle, 10 MiB, c4 | 46.19 verified MiB/s | 45.93 verified MiB/s | Near tie; different Python client and full-cycle definition |

The 10-MiB multipart unit is initiate → two 5-MiB UploadParts → Complete → whole-object GET/SHA-256 verification → DELETE. Its displayed MiB/s is verified logical object bytes per completed cycle; it is **not** pure upload bandwidth. Standalone DELETE is the separate Python client's 12-second DELETE interval after preparing 12,000 objects; its preparation time is excluded from the reported rate but charged to campaign time. All other rows use Warp. Never pool Warp and Python-client rates as one workload.

### Trial-level rates, in pair order

| Workload and unit | Cairn arms 1 / 2 / 3 | RustFS arms 1 / 2 / 3 |
| --- | --- | --- |
| PUT 4 KiB, objects/s | 297.11 / 426.71 / 357.84 | 692.42 / 756.41 / 384.33 |
| PUT 1 MiB, MiB/s | 97.46 / 44.56 / 47.84 | 114.61 / 92.17 / 87.26 |
| GET 4 KiB, objects/s | 7,619.79 / 6,261.87 / 7,418.14 | 6,833.89 / 6,018.59 / 6,598.32 |
| GET 1 MiB, MiB/s | 1,769.18 / 1,950.80 / 1,998.63 | 572.36 / 1,236.91 / 1,256.40 |
| HEAD 4 KiB, requests/s | 9,687.78 / 9,278.10 / 10,027.15 | 5,935.70 / 8,672.13 / 8,600.69 |
| LIST, listed objects/s | 91,060.26 / 96,082.84 / 97,361.65 | 16,225.86 / 15,993.34 / 16,515.38 |
| DELETE 4 KiB, objects/s | 571.86 / 416.36 / 456.82 | 528.85 / 340.62 / 488.98 |
| Mixed 1 MiB, MiB/s | 345.48 / 197.06 / 139.53 | 195.65 / 225.58 / 219.98 |
| Complete multipart cycle, verified MiB/s | 70.60 / 46.19 / 36.82 | 45.94 / 43.51 / 47.27 |

Mixed Cairn throughput declined 345.48 → 197.06 → 139.53 MiB/s across fresh stores, while RustFS stayed near 196–226 MiB/s. The trial design cannot establish whether that is Cairn cleanup/Writer interference, virtual-disk or host drift, client scheduling, or another cause. The 4-KiB RustFS PUT and 1-MiB Cairn PUT controls also have large spreads. The prior single-node RustFS comparison used different released Cairn code and workload settings; do not splice its numbers into this table or call the difference a post-change regression.

### Latency and footprint diagnostics

| Workload | Cairn median p50 | RustFS median p50 | Cairn median server peak RSS | RustFS median server peak RSS |
| --- | ---: | ---: | ---: | ---: |
| PUT 4 KiB | 114.1 ms | 40.1 ms | 33.2 MiB | 358.2 MiB |
| PUT 1 MiB | 341.0 ms | 146.6 ms | 37.6 MiB | 269.5 MiB |
| GET 4 KiB | 1.8 ms | 2.0 ms | 42.0 MiB | 236.6 MiB |
| GET 1 MiB | 7.1 ms | 12.7 ms | 54.1 MiB | 237.0 MiB |
| HEAD 4 KiB | 1.2 ms | 1.5 ms | 41.3 MiB | 236.4 MiB |
| LIST | 8.8 ms per LIST request | 61.4 ms per LIST request | 38.6 MiB | 246.5 MiB |
| DELETE 4 KiB | 24.2 ms | 29.5 ms | 35.6 MiB | 255.4 MiB |
| Complete multipart cycle | 903.8 ms | 847.3 ms | 36.0 MiB | 240.9 MiB |

Warp p50 values are read from its `Reqs` line for the measured operation; the Python rows time the whole corresponding signed operation/cycle. Peak RSS is `/proc/<pid>/status` `VmHWM`, sampled during the arm; it is not a retained-heap measurement, a memory-per-object normalization, or a full peak over process startup/teardown. The RustFS process used substantially more RSS in these runs, but the stores have different on-disk formats and features, so the table does not establish equivalent work or durability. No comparable disk-bytes/inodes-per-live-object result was captured: the space watcher measured **whole task-root** allocation, including executables/toolchains/builds, and its raw peak must not be presented as an engine's storage amplification. Per-arm SQLite WAL size was also not captured, so there is no WAL-amplification comparison. Maximum observed task-root allocation was 4,174,872,576 bytes, below the 10,000,000,000-byte cap.

No p99 is claimed. Warp's compact report did not retain an exact successful-request count per operation/arm for this gate; the Python DELETE and multipart arms had fewer than 10,000 successes each. Warp reports p90 rather than p95, so a client-consistent p95 was unavailable and is not fabricated. GET pools were newly prepared and therefore warm; neither cold-device bandwidth nor sustained capacity is established. Warp's client used more than one CPU-second per wall-second in several GET/HEAD/LIST arms, especially Cairn LIST (~30 client CPU-seconds per 12-second invocation), so client saturation cannot be excluded. Host device/cache contention was not isolated. These limitations prevent architectural attribution from throughput ratios alone.

## Correctness, failed attempts and budget

The 54 scored arms in the table completed with zero reported Warp errors or Python-client response/hash failures. The Python multipart arms verified 365 completed 10-MiB objects in total across both engines; the DELETE arms acknowledged 33,735 successful deletes. Those are functional checks under load, not power-loss or Object Lock proofs. The complete repository validation gate was **not** run for this documentation and benchmark-harness change; five offline parser/footprint regressions, Python compilation and the documentation link/anchor/shell check passed. The full code gate remains required before integrating any future production optimization.

All diagnostic and failed attempts were retained in the task-owned ledger until this report was reduced: three initial Warp-1.8 parser diagnostics, one successful five-second pair, an invalid too-small DELETE pool, a larger Warp DELETE arm that emitted `Skipping DELETE too few samples`, a mixed-summary parser miss, the Warp multipart object-size mismatch, a tiny successful Python multipart check, and one RustFS startup-readiness 503 before the Python client waited for S3 readiness. None was silently promoted into the scored table. Warp's multipart mismatch (`want: 5,242,880; got: 524,288,000` with 100 × 5-MiB parts) is a failed benchmark-client assertion, **not** evidence that Cairn returned corrupt bytes; its temporary data was discarded. The two Python fallback cells completed after those failures.

The recorded coordinator invocations consumed **1,483.89 seconds** in total, including the failed attempts, preparation, workload, reporting and per-arm cleanup. This is below the 3,600-second allowance; build/tool installation was outside the experiment allowance and recorded separately. The task root stayed below the 10-GB cap. Every server/benchmark process was reaped and every per-arm store deleted. After this compact report and its tests were checked, the remaining task-owned binaries, toolchain, package cache, scripts' result JSON, and built web artifacts were removed; the repository's tracked comparison scripts and reports remain. Do not reuse the consumed one-hour allowance by resetting or deleting this record.

## Engineering conclusion and next measurement

At this revision and on this host, RustFS has the higher observed small/large PUT medians; Cairn has higher observed GET, HEAD and especially LIST medians; DELETE and full-cycle multipart are effectively too close/noisy to distinguish; mixed tilts RustFS by the displayed median but Cairn's drift is unresolved. Cairn used less sampled server RSS, but disk/inode efficiency was not measured. This is a **baseline, not a diagnosed bottleneck or durability-matched ranking**. The next work is to repair the durability/observation controls, then instrument complete **1-MiB PUT** stage waits at c16, using 4-KiB PUT as a control. Correlate Writer, namespace preparation, blob durability, audit, cleanup and host-device measurements on the same workload. The [detailed plan](performance-put-1m-plan-2026-09.md) records candidate changes and their tests. The one-Writer, FULL-durability and exact-cleanup ceilings remain in force.
