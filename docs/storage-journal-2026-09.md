# Protocol-2 lifecycle cost and recovery qualification

Status: predeclared before measurements. Production implementation and correctness gates are in
progress. Full startup reconciliation remains mandatory; this record does not authorize Phase 3D
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
1,388 default-feature and 1,413 all-feature tests (four/seven skipped), two doctests, formatting and
both all-target Clippy configurations. Web lint/build, both npm audits, cargo audit and installer
checks passed; 50 Python laboratory tests passed with two optional live fixtures skipped. The
cleanup scheduler regressions additionally cover more than one page with the default hourly
stale-upload interval, shutdown before claims and locked-file debt retention. Final-head CI and
the measured revision/results remain pending.

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
