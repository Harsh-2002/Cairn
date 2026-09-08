# Protocol-2 lifecycle cost and recovery qualification

Status: correctness gates pass; the predeclared lifecycle-cost comparison is **INCONCLUSIVE**.
The paired medians show substantial added small-object cost. Full startup reconciliation remains mandatory; this record does not authorize Phase 3D
activation. The approved protocol is in `storage-lifecycle-proposal.md`.

## Comparison

Compare optimized default-feature servers at merged protocol-1 main `bf17dfe` and the final
protocol-2 implementation commit, with matching release/debug/strip settings, explicit executable
hashes, identical FULL-durability SQLite configuration and the existing S3 laboratory client.
This measures the incremental complete lifecycle cost: durable PUT admission, exact publication
and deferred cleanup accounting, plus the required namespace/I/O ownership work. It cannot isolate
one SQL statement or establish the metadata engine's intrinsic capacity.

Use five sequential alternating baseline/candidate pairs at concurrency 4, one hot bucket,
4-KiB deterministic raw bodies, seed `0x5eed`; each arm has three equal 3-second load/1-second idle
cycles, maximum 30,000 transactions, and a 45-second complete-case allowance. Each transaction
is signed PUT, full verified GET, DELETE on the existing bounded overwrite ring. No retries.
A single 1-MiB pair with the same configuration screens the streaming control; it cannot qualify
that workload for a stable performance claim. Do not run while builds or other owned tests load
the host. Record host/device pressure and separate Python client CPU from server CPU.

Primary measurement: successful PUT latency (median of per-cycle medians and, where available,
median of sufficient-sample per-cycle p99 values), interpreted as
added cost rather than a promised improvement. Also report complete transaction throughput,
GET/DELETE latency, maximum observed phase-end server anonymous/PSS memory, available Writer stages and errors. No p99 claim
below 10,000 successful operations in every individual cycle. With the declared 30,000-operation
arm cap split across three cycles, a cycle reaching 10,000 is capped and cannot PASS. Qualified
p99 values are therefore necessarily unavailable in this comparison; retain descriptive paired
p50 observations and an overall INCONCLUSIVE verdict without extending the workload to chase
tail samples. Compare each matched pair and report control drift;
more than 20% baseline span, Python client CPU at or above 0.85 of one core in any load cycle (conservative
saturation exclusion), missing observations or insufficient sample size
makes the affected conclusion INCONCLUSIVE. Zero unexpected errors or byte mismatches is required.
After the final idle period, while the owned server is still alive, read committed protocol-2
`storage_write_intents` and `storage_cleanups` counts from its SQLite database using `mode=ro`
(including the live WAL). Poll for at most five seconds, bounded further by the existing work
deadline so the 45-second case allowance still reserves 15 seconds for teardown. Preserve the
observed counts, elapsed time and availability in the result and reduced export. This observes
the candidate's production cleanup worker; it does not invoke cleanup or change its cadence.
Protocol-1 baseline journals are explicitly unsupported. Candidate residual debt, unsupported
journals or a missing/failed observation makes lifecycle cleanup qualification INCONCLUSIVE,
while retaining descriptive paired p50 cost observations. Empty committed journals at this
boundary do not establish steady-state GC cost, and memory/footprint samples from the preceding
load/idle cycles do not include this extra observation period. Phase 3D must separately measure
recovery and debt-draining behavior.

The campaign root remains `/SSD/dev/cairn-storage-campaign`; use `run.sh run --phase recovery
--layer s3` with explicit binary/commit/build settings, `--concurrency 4 --buckets 1 --size 4096
--seconds 3 --idle 1 --max-ops 30000 --allow-seconds 45`. The 1-MiB screen changes only `--size`.
Charge preparation, executable hashing, discovery, startup, load/idle, collection, reduction and
cleanup to the existing persistent ledger. Starting consumption: 390.191882 seconds and
542,654,464 bytes recorded peak. Builds, focused correctness fixtures and normal CI are separate.
No raw experiment is admitted before its required space/time reserve. Preserve all failed or
inconclusive attempts; never reset the campaign allowance.

## Correctness evidence

The production tests exercise admitted plans across every metadata backend and shard routing,
late blocking I/O/cancellation, exact multipart quota debt including scratch aliases, failed
namespace synchronization, symlinks and same-filesystem bind mounts, claimed cleanup scan
protection, publication ambiguity and fresh-generation recovery. The final local Rust gate passed
1,407 default-feature and 1,433 all-feature tests (four/seven skipped), two doctests, formatting and
both all-target Clippy configurations. Web lint/build, both npm audits, cargo audit and installer
checks passed; 50 Python laboratory tests passed with two optional live fixtures skipped. The
cleanup scheduler regressions additionally cover more than one page with the default hourly
stale-upload interval, shutdown before claims and locked-file debt retention. The CI follow-up fixes also pass 15 deterministic soak-cleanup verifier tests and the rebuilt
privileged pending-kernel-write fixture. All 54 CI checks pass for implementation revision
`9c3539fd41687bdd194eb00418d6dcf301a3e917`, including the optional backends and CodeQL.
The mixed-feature soak verified all 584 terminal multipart sessions clean within 30 seconds,
with zero operation errors, byte mismatches or leak-shape violations. Its 29,160 operations
over 181 seconds are advisory CI activity, not performance-comparison evidence.
The measured results follow below; the final documentation-head CI remains required before merge.

The privileged Linux io_uring fixture freezes only its own mounted ext4 image, observes an actual
submitted write pending in the kernel, sends SIGKILL, and confirms node/file exclusion remains
held while process teardown is pending. After thaw and reap, fresh ownership and durable cleanup
succeed. This establishes exclusion through the observed process teardown. It does not establish
an observed post-exit kernel-reference window or simulate power loss. The separate retained-file
lock fixture is a deterministic ownership model, not additional kernel process-kill evidence.

The checked-in `conformance/storage_lab/journal_report.py` charges bounded reduction and exports
paired ratios, control spans, unavailable measurements and exact binary/configuration provenance.
It retains the driver's per-cycle 10,000-sample p99 gate; combining smaller cycles does not invent
a pooled quantile. Its deterministic regressions reject failed runs and preserve drift, client
saturation and insufficient-sample reasons. Measurements remain descriptive costs even if all
availability checks pass; adoption is a separate decision.

## Measured result

Both prebuilt executables used default features, release optimization level 3, fat LTO, one
codegen unit, panic abort, debug level 2 and no stripping. Build-time compiler: Rust 1.97.1
(`8bab26f4f`, 2026-07-14). The experiment's automatic `rustc` probe failed because its toolchain
manifest was unavailable; the build declaration and actual executable hashes are retained.
The coordinator ran from `9c3539f`. Its recorded production-coalescer/source hashes describe that
checkout for both arms; they do not identify the source inside the separately supplied executable.

| Arm | Exact source revision | Executable SHA-256 |
|---|---|---|
| Baseline | `bf17dfe001e754d6e4041b05fc206f16f55d6584` | `26f061e0d37ef9ea342239a17824d7eb14c23de67ca27cec277e7eea1c99cbf5` |
| Candidate | `9c3539fd41687bdd194eb00418d6dcf301a3e917` | `243c5a635382832b72727a4fdf67da829b169bf18b653b344b420d19e4b0e74f` |

Host: four-core Intel i5-6500TE, Linux 7.0.0-29, ext4 on `/dev/sdb1` mounted at `/SSD`,
Python 3.14.4. Disk cache is uncontrolled. Host device and pressure counters include unrelated
activity. No owned build or correctness test ran during these measurements. No CPU/heap profiler
was used. Available server metrics and device observations are retained; internal Writer stage
samples, admission/queue attribution and allocator ownership are unavailable from this S3 run.
No intrinsic metadata-engine bottleneck follows from the lifecycle rates.

The table reports medians across five arms (each arm first reduces three cycle medians). The
change column is the median of the five matched candidate/baseline ratios, so it need not equal
the ratio of the displayed arm medians. Memory values are medians of each arm's maximum sampled
phase-end values, not continuously observed peaks or retained-allocation evidence.

| 4-KiB measure | Baseline | Candidate | Median paired change |
|---|---:|---:|---:|
| PUT p50 | 3.857 ms | 6.745 ms | +75.9% |
| GET p50 | 0.907 ms | 0.957 ms | +8.0% |
| DELETE p50 | 2.497 ms | 3.625 ms | +45.8% |
| PUT/GET/DELETE transactions per second | 496.2 | 311.5 | -36.9% |
| Server anonymous memory | 9,524 KiB | 9,832 KiB | +2.1% |
| Server PSS | 23,053 KiB | 22,108 KiB | -4.0% |

There were 22,451 successful baseline and 13,753 successful candidate transactions, with zero
unexpected errors or byte mismatches in all ten arms. Individual cycles do not meet the p99
sample gate; pooled counts do not reconstruct a pooled quantile. The largest protected baseline
latency/rate control span was 6.35%, and observed Python client load never exceeded 0.537 CPU
cores. Neither the declared drift nor client-saturation threshold tripped. The candidate's
higher DELETE latency and lower throughput exceed the 10% protected-workload limit for a
performance adoption. These are the measured costs of the approved correctness protocol; they
are not an improvement claim or authorization for additional tuning.

The single 1-MiB screening pair recorded PUT p50 27.951 → 31.364 ms (+12.2%), DELETE p50
3.752 → 4.189 ms (+11.7%) and transaction throughput 91.90 → 81.38/s (-11.4%); GET p50
8.995 → 8.942 ms. It completed 800/744 transactions without errors or byte mismatches. One pair
and insufficient per-cycle tail samples cannot establish a stable streaming comparison.

All six candidates observed zero committed intents and cleanup rows on the first post-idle
read, taking 3.67–4.14 ms. This qualifies only the declared post-load cleanup boundary. It does
not measure how long individual cleanups waited, establish steady-state reclamation cost, or
qualify readiness with unfinished debt. Full startup scans remain mandatory. Phase 3D's offline
baseline and restore validation are still required; journal-only startup is not activated.

The rejected implementation revisions `537c2b3` and `0de5b69` were built but never measured.
All twelve diagnostic arms passed; both comparisons retain **INCONCLUSIVE** qualification.
The full [machine-readable evidence](storage-journal-2026-09.json) retains run IDs, every cycle,
configuration, executable/harness provenance, ratios, missing observations and campaign history.
Reduction IDs are `bdfa7098ad6549c8840f7b6086f1d6b1` (five pairs) and
`4249946ad6db44b99c3d74e61ae98e26` (screen). No failed trial was discarded or repeated.
Raw run directories, datasets, logs and samples were purged after export; compact results and
the original cumulative ledger remain. Each arm reaped its owned process groups.

Cumulative charged runtime after reduction, export and cleanup: **574.519391 seconds** of
3,600; recorded campaign peak **571,154,432 bytes** of 100,000,000,000.
This includes the earlier 390.191882 seconds; the allowance was not reset. Builds, fixed
correctness tests and CI remain separately tracked.
