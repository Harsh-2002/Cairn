# Performance execution tracker — 2026-09-23

This is a new work queue, not a revision of the completed [storage evolution campaign](storage-evolution-plan.md). Its purpose is to explain the current Cairn–RustFS gap on this host, improve the shipped single-node design within [CONTRACT.md](../CONTRACT.md), and evaluate a successor design separately. Historical measurements are hypotheses and controls, not measurements of the current revision.

## Decision and scope

Priority: close the current 1-MiB PUT throughput gap and explain mixed-load instability, while balancing CPU, resident memory, and disk/inode footprint. Keep FULL durability, exact storage ownership and cleanup, one embedded SQLite database and Writer, S3 semantics, Object Lock, fail-closed encryption, backup/restore and existing migration guarantees. A packed on-disk format is **research only** until a human explicitly approves a production-format and migration decision. No release, architecture-ceiling change, or durability downgrade is part of this queue.

The [1-MiB PUT research and implementation plan](performance-put-1m-plan-2026-09.md) expands the tasks below with code evidence, candidate designs, observation requirements and correctness tests. Research found that the tested RustFS source defaults new buckets to relaxed durability, while the original comparison did not record effective modes. The [verified-strict control and candidate screen](performance-put-1m-strict-2026-09.md) supersede the historical 1.93× ratio as evidence for a matched-durability gap; the candidate was reverted. The [strict-mode follow-up](performance-strict-followup-2026-09.md) protects six workloads and screens Writer linger without adopting a change. A [diagnostic sync-call probe](performance-sync-probe-2026-09.md) then counted root/staging/other barriers without adding Rust hot-path timers; it is not an adoption benchmark.

The next comparative campaign is capped at **3,600 seconds and 10,000,000,000 bytes of task-owned peak disk footprint**, including both server stores, databases/WAL, profiles, logs and scratch. Builds and correctness tests are recorded separately. The first measured run must declare its exact binaries, tool versions, workload, controls and artifact root. Never reset a consumed campaign budget to gain more measurements. A missing profiler, saturated client, uncontrolled host drift, too few samples, or a failed correctness check makes an arm inconclusive, not a win.

## Tasks

| ID | State | Deliverable and completion gate |
| --- | --- | --- |
| P0 | Done | Reconcile the prior release comparison, storage-lab evidence, current code, specifications and hard ceilings. Record attribution limits; do not call the entire 4-KiB gap a SQLite bottleneck. |
| P1 | Throughput baseline done; footprint/tails inconclusive | Pre-register a current-revision benchmark matrix and resource budget, then run a clean Cairn baseline and a matched RustFS control sequentially on the same host. Report operation errors, integrity, median throughput, available latency and server/client resource observations; mark unavailable WAL, p95/p99 and per-live-object disk/inode fields explicitly rather than inventing them. Protect large PUT/GET, HEAD, LIST, multipart and mixed/churn while optimizing small PUT. Archive compact evidence and clean owned stores/processes. [Results and explicit unavailable fields](performance-current-2026-09.md). |
| P1b | Strict 1-MiB control done | Cairn FULL and RustFS global/new-bucket strict were pinned; measured RustFS buckets were verified before and after traffic. Three pairs gave 58.48 versus 62.35 MiB/s medians, with substantial spread. See the [control report](performance-put-1m-strict-2026-09.md). |
| P2 | Namespace/finalization split measured; critical path still partial | A new four-arm shared-node strict 1-MiB diagnostic split namespace queue/execution and finalization. Namespace queue averaged 0.22/0.52 ms while execution averaged 47.76/13.63 ms across the two Cairn arms; file and directory syncs were material. Cairn was 34.30/51.11 MiB/s versus RustFS 80.46/75.12 MiB/s, but host interference and unavailable exact Warp timelines prohibit an architecture/adoption claim. The runner's Warp v1.8 aggregate-suffix **and schema** bugs are fixed; PUT arms require source-derived timeline/score parity (28 offline regressions and a pinned real-CLI synthetic replay passed; live artifact creation still pending). A subsequent one-in-32 diagnostic stage isolates each mandatory namespace parent-directory sync inside execution (102 blob tests passed, 2 environment-dependent ignored; Clippy passed), but **it has not been benchmarked** and may add sampling overhead. The earlier glibc sync-call probe and Writer telemetry are contextual only, not per-request attribution. Measure authentication/preflight, exact foreground-versus-cleanup, Writer-admission/publication response waits and a 4-KiB control. [New diagnostic](performance-namespace-finalize-diagnostic-2026-09.md); [prior probe](performance-sync-probe-2026-09.md). |
| P3 | 0/1000-µs screen rejected; full sweep pending | A same-binary, FULL-durability three-pair 0-versus-1000-µs screen gave 96.22 versus 60.40 MiB/s arm medians; 1000 µs won one pair. No setting changed. Smaller steps and the five-pair adoption gate remain pending, contingent on quieter-host attribution. No `synchronous=normal` substitution. [Follow-up](performance-strict-followup-2026-09.md). |
| P3b | Same-request and cross-request screens rejected; cleanup batching pending | A one-root-barrier flat-path candidate cut the median namespace-stage mean in the five-pair screen but improved median 1-MiB PUT only 4.0%, with just two pair wins. A later cross-request root-fence candidate cut observed root `fsync` calls per file-data sync from 2.00 to 0.46, but its two PUT pairs contradicted each other (0.729× and 1.899×) on a drifting shared node. Both were reverted. Cross-request root-fence and cleanup batching remain research only; see W2, the [first screen](performance-put-1m-strict-2026-09.md), and the [cross-request screen](performance-root-cross-request-screen-2026-09.md). |
| P3c | Two independent PUT screens rejected; protection not warranted yet | Isolated no-`KEEP_SIZE` and no-`DONTNEED` variants at fixed 1 MiB missed the preliminary consistency/gain test on the shared node. No hint policy changed. Immediate-read, mixed, device-read/cache and memory protection would still be required for any future adoption; see the [screen](performance-placement-hints-screen-2026-09.md) and W3/W5. |
| P4 | Co-commit candidate rejected | A safe single-shard PUT/audit co-commit candidate passed targeted metadata/protocol tests but failed its five-pair strict PUT adoption screen; it was reverted. The earlier audit-omission diagnostic was non-conformant and not an adoption result. See the [candidate report](performance-audit-cocommit-adoption-2026-09.md). |
| P5 | Diagnostic deferral screen inconclusive; attribution pending | A deliberately non-conformant one-hour cleanup deferral won two of three shared-node PUT pairs but missed the 20% gate; control throughput fell 32.6% over the run. No code was adopted. Measure actual foreground/cleanup barriers, debt creation/claim/settlement and host pressure before any safe scheduler/batching design; keep exact ownership and full recovery scans. See the [screen](performance-cleanup-interference-screen-2026-09.md). |
| P6 | Pending | Diagnose 1-MiB mixed-load drift with interleaved, fresh-store controls and time-aligned PUT/GET/DELETE/STAT rates, Writer/cleanup samples and host device pressure. Then analyze GET/HEAD and multipart CPU/I/O paths separately. Compare the optional fast-I/O build against the normal build without mixing configurations, and protect TLS/encryption/fallback correctness. |
| P7 | Pending | Draft a production-format decision record for a hybrid small-object segment design, informed by the already inconclusive isolated packing experiment. Specify immutable record identity, 4-MiB/256-record/1-ms bounds as hypotheses, Writer publication, pinning, conditional GC, recovery, backup/restore, quotas and migration. Do not wire it into production without the human decision and a complete paired matrix. |
| P8 | Pending | Run owning-crate regressions and the applicable full gate for each shipped change; repeat the matched benchmark and protected matrix. Publish a table of before/after Cairn and RustFS, confidence/limits, CPU/RAM/disk cost and any regressions. Clean only task-owned temporary artifacts. |

Latest P2/P3b checkpoint: the [root-barrier adoption campaign](performance-root-barrier-adoption-2026-09.md)
completed five Cairn PUT pairs, two nearby strict RustFS reference pairs and protected
mixed/GET/LIST pairs (20 zero-error arms). The candidate removed one sampled ordinary
root-parent sync, but two of five Cairn pairs reversed and all five winning arms had
lower sampled host I/O full pressure, regardless of binary. The two Cairn/RustFS
strict PUT ratios were 0.467 and 0.465; the gap remains. The candidate is not
adopted; its root-sibling helper was removed after this decision. The next P2
deliverable is sampled *per-request* exclusive response-path
timing across storage admission, blob stage, publication, notification and audit,
plus foreground/cleanup separation and an observation-overhead screen. Existing
blob/Writer histograms cannot be added into that critical path because they are
different samples and some stages are inclusive. This campaign's 600-second
allowance is closed, with 226.166199 seconds unused; all task scratch was cleaned.
The new protocol-level sampler and runner metric capture are implemented and
pass pinned Rust 1.97.1 owning blob/protocol suites and warnings-denied affected-
crate/server all-target Clippy, plus 37 offline Python runner/ledger checks.
The proposed 300-second `put_timing` profile has not been authorized or run.
These remain unmeasured diagnostic source, not an adopted optimization; the
pinned workspace-wide gate and overhead/live attribution checks remained open
at that checkpoint.

Live follow-up (2026-09-25): the user authorized node-local execution without
further approval. The [PUT response-path diagnostic](performance-put-timing-live-2026-09.md)
used exact release binaries and closed two separate ledgers: the exact-capacity
attempt stopped after one zero-error arm because VM `MemTotal` changed; the
predeclared bounded-ballooning retry passed three Cairn A/B pairs plus one strict
Cairn/RustFS pair in 157.139 measured client seconds. Across 107 instrumented
PUTs, mean handler time was 48.5% blob stage, 18.5% storage admission, 18.0%
audit and 14.9% publication. Slow arms tracked higher host I/O-full pressure;
paired throughput signs reversed, so observation overhead and a stable RustFS
ranking remain unresolved. This is P2/P3b/P4 attribution, **not** an adopted
candidate. Both task roots and the task-pulled Warp image were cleaned after
retaining compact evidence.

Offline validation follow-up (2026-09-24): the `put_timing` profile now rejects
interrupted PUT-stage samples and duplicate metric lines in addition to dropped
samples or unequal successful stage counts; its eight ledger tests and the 29
comparison-runner tests pass. Archived Cairn Prometheus output confirms the
expected `stage,result` label ordering, and the 16-second post-load drain exceeds
the normal 15-second metrics refresh interval. On the current diagnostic worktree,
pinned Rust 1.97.1 passed
workspace default- and all-features all-target Clippy with warnings denied,
`cargo test --locked --workspace` (unit, integration and doctests, including
the real embedded-console asset test), and formatting. The production web
bundle built; web lint and both npm audits passed with zero findings; installer
shellcheck and regression tests passed. `cargo-audit` 0.22.2 scanned the lockfile
against 1,267 advisories with no vulnerability failure and one allowed
`rustls-pemfile` unmaintained warning. This used `cargo test`, not the prescribed
`cargo nextest` runner, and is not a production release-build or live timing/
overhead result. The private validation toolchain/build cache (about 16 GB),
generated web assets and newly installed web dependencies were removed; the
pre-existing placeholder `web/dist/index.html` was restored byte-for-byte.

Host-capacity amendment (2026-09-25, **not a benchmark**): the user expanded this
node's memory. `/proc/meminfo` now reports `MemTotal: 12255072 kB` (11.69 GiB),
`SwapTotal: 0 kB`, and four logical CPUs remain visible. This is a new host
epoch relative to the 7.8-GiB campaigns above: more page-cache headroom can
change warm-read, mixed and writeback behavior, so do not splice its future
absolute scores into the old medians as a code-caused gain or regression. The
Python paired runners now record `MemTotal`, `SwapTotal` and logical CPU count
before and after each arm; the one-ledger coordinator also rechecks capacity
after every child and rejects any phase that differs from its admission snapshot.
The 9 ledger and 30 comparison
runner offline tests pass. No fresh client load or revised performance score
follows from the RAM increase alone; a new campaign still needs its own allowance.

P2 update: A bounded one-in-32 object-write blob-stage sampler passed its owning-crate unit tests
and compiled in the pinned release profile. One strict 1-MiB PUT A/B pair on the shared node
observed 108.13 versus 119.90 MiB/s (baseline then sampler), with changed host pressure and no
repeat; this is **not** an adoption or overhead verdict. The candidate-only stage means place body
at 29.08 ms, finalize at 17.19 ms, namespace at 14.49 ms, and directory sync at 11.57 ms;
these are wall waits, not exclusive CPU costs. See the [screen and ledger](performance-object-write-stage-screen-2026-09.md).

Shared-node control amendment after that screen: the Python runner now samples a names-only
`/proc` CPU census every two seconds, excludes the server, client and runner PIDs, and retains
at most five other CPU users per interval. PID start times distinguish reuse; disappearing
processes and unavailable censuses are reported rather than silently read as zero. This is
diagnostic evidence of competing work, not a noise-correction factor or an automatic excuse to
discard an arm. The preceding one-pair screen did **not** have this field and cannot be
retroactively classified by it. The campaign ledger remains unchanged by these offline tests.

P2 instrumentation revision after that screen: the sampled **raw plaintext** body now has
aggregated input-wait, hash, and buffered-sink substage labels, with cancellation recorded in the
active phase. This is a new candidate build, not the binary measured in the preceding screen;
its pinned Rust 1.97.1 blob suite (101 active unit, 42 integration tests), server suite (321
active unit, one integration test), and affected-crate Clippy check pass. The full workspace gate,
release-binary overhead gate and protected workloads remain pending before using it to select an
optimization. The encoding and durability sequence is unchanged. At that checkpoint no new
measured load had run.
The later [single-arm raw-body diagnostic](performance-raw-body-substage-screen-2026-09.md)
charged 8.811 measured seconds and produced 12 loss-free samples per phase, but an unrelated VM
overlapped the load and the Warp analyzed interval was only two seconds. It is attribution-
inconclusive by its preregistered rule; no candidate is adopted. The separately approved ledger
is now **3,592.133/3,600 seconds**, leaving **7.867 seconds**.

The user separately authorized a **new 1,200-measured-second, 10-GB task-scratch
campaign on the shared node**, without a quiet-window reservation. The
[preregistered resumption and results](performance-shared-node-resumption-2026-09.md)
completed 22 instrumented Cairn/RustFS PUT/mixed/GET/LIST arms, a five-pair
clean-HEAD-versus-instrumentation overhead screen, and a five-pair clean-HEAD
versus strict RustFS 1-MiB PUT confirmation. All 42 arms passed their zero-error
and strict-control checks. The clean-HEAD PUT medians were **52.73 versus
64.23 MiB/s** (Cairn/RustFS paired median ratio 0.817, five RustFS pair wins),
but absolute scores and competing VM load varied. The sampler overhead gate was
inconclusive; no optimization was adopted or claimed. This separate campaign
consumed **1,000.350591/1,200 measured seconds**, leaving **199.649409 unused**.
Its task-owned scratch, generated web assets and two pulled images were removed
after evidence closeout; only compact result records and the worktree remain.

Control follow-up after that campaign: the Python runner now records a wall-clock
anchor with each two-second host sample and, for standalone PUT only, reduces
Warp's bounded benchdata through `warp analyze --json --full --analyze.op=PUT`
after client exit. It reads the aggregate's exact active start/end and requires
its recomputed MiB/s to agree within 0.02 MiB/s with the benchmark report before
attributing complete host-census intervals. This is **not** a noise correction;
boundary intervals remain excluded and the host counters are not server-only.
Warp v1.8.0's [analyzer](https://github.com/minio/warp/blob/v1.8.0/cli/analyze.go)
feeds the same operation set into its [aggregate](https://github.com/minio/warp/blob/v1.8.0/pkg/aggregate/aggregate.go);
the pinned official GET fixture reported 3489.26 MiB/s versus 3489.2599 MiB/s
recomputed from its JSON aggregate over 59.9079 seconds. The 55 offline tests
and that no-load fixture integration pass. No completed campaign arm had its
discarded benchdata retroactively reconstructed, and no new S3 load was charged
to the 1,200-second ledger for this control work.

Next attribution revision (unmeasured): one-in-32 sampled object writes now split the
existing inclusive `namespace` wall duration into `namespace_queue` (blocking-job
submission to worker start) and `namespace_execution` (anchored path preparation on
the worker). This does not change any namespace operation or durability barrier.
The owning blob crate passed 101 unit tests (two mount-namespace tests ignored)
and all-target Clippy with warnings denied using the locally installed Rust 1.98.1;
`cargo fmt --all --check` and `git diff --check` passed. The pinned 1.97.1
toolchain, complete workspace gate, sampler-overhead gate and live stage evidence
remain outstanding. No throughput gain or bottleneck verdict follows from this edit.
Pinned-build follow-up: a task-owned Rust 1.97.1 toolchain compiled the current
checkout (`f6fde5a68c6a5d5acee54f950ce2be8cda547312` plus the diagnostic
worktree diff) with `cargo build --locked --release --bin cairn` after a real
`npm ci && npm run build`; the executable SHA-256 is
`d7f33c4dd0a54df6c89cf366003dda760097a4943f014abf4c4c88976407ae52`.
The same pinned compiler passed the blob crate's 101 active unit tests (two
mount-namespace tests ignored) and the blob crate's all-target Clippy check
with warnings denied. The RustFS 1.0.0 ZIP and extracted executable
and Warp 1.8.0 executable match the exact hashes in the earlier campaign.
These are no-load preparation and correctness evidence, not a scored comparison.
The task-owned build/download root is
`/var/tmp/cairn-performance-nsdiag-B5bg63`; it must be removed after the
pending diagnostic decision and any authorized measurement are finished.
After the pinned build/tests, its toolchain, Cargo cache/target, release ZIP and
unused CLI executable were removed, as were the generated web dependencies and
bundle. Only the three hash-verified executables remain in that private root
(349 MB); no benchmark load has run in this follow-up. **The retained Cairn
executable, renamed `cairn-namespace-only`, predates the following source edit**
and must not be used to claim its new finalization substages.

Further diagnostic source revision (not yet release-built or live-measured): the
same sampled ordinary-object path now records residual `finalize_flush`, blocking
`finalize_queue`, mandatory trim-plus-file-`sync_data` as `finalize_sync`, optional
page-release `finalize_advice`, and exact `finalize_rename` inside the existing
inclusive `finalize` wait. The default retained-blocking backend alone emits
these details; io_uring keeps the inclusive stage. The operation sequence and
failure propagation are unchanged. The owning blob crate's 101 active unit tests
and all-target Clippy with warnings denied pass on the installed Rust 1.98.1;
the pinned rebuild and live stage evidence are still pending at this checkpoint.
This is attribution scaffolding, not an optimization.
The owning tests also assert that a 1-MiB-plus staged write records all five
finalization details, and that a failed mandatory trim records interrupted sync
without falsely recording a rename. Generated test build scratch was removed.

## Predeclared acceptance

The primary candidate metric is the median successful **1-MiB PUT MiB/s at concurrency 16** on one hot bucket. The balanced decision also requires no worse than 10% regression in median 4-KiB PUT/GET/HEAD and LIST throughput, 1-MiB GET throughput, multipart and mixed/churn throughput, p95 latency, peak anonymous/PSS memory, CPU-seconds per operation, and physical bytes/inodes per live logical byte/object. The minimum adoption gain is 20% in the primary metric across five interleaved paired trials with zero unexpected S3 errors, digest mismatches, acknowledged data loss or premature reclamation. Closing the full 1.93× current median gap is a stretch target, not a predicted outcome. Concurrency 1/4/16/32/128 describes the scaling curve; avoid a full Cartesian sweep. At least 10,000 successful samples **per arm and operation** are required for p99 claims. Preserve absolute values, not only ratios, and report confidence intervals or trial spread. A competitor win is not itself permission to weaken the contract.

The 60-minute allowance is initially reserved as 10 minutes baseline/control, 8 attribution, 10 reversible tuning, 14 one-change paired confirmation, 10 protected workloads, and 8 verification/cleanup. If startup or builds consume measured time, shrink the matrix and mark missing cells inconclusive. Do not compare a new current-head run with the historical release binaries as if they were one A/B pair.

P1 consumed 1,483.89 seconds. Subsequent strict control, diagnostic and A/B work brought cumulative measured time to about 2,529.74 seconds, leaving about 1,070.26 seconds. The [strict control and screen report](performance-put-1m-strict-2026-09.md) is the current ledger; the original provisional allocation in the detailed plan is historical and does not reset the cap.

## P0 evidence and boundaries

The prior same-node released-binary Warp comparison found RustFS faster at 4-KiB PUT, 1-MiB PUT/GET, HEAD, mixed and multipart; Cairn led LIST. Its 4-KiB PUT median was 57.82 versus 665.97 objects/s, but the trials do not assign causal stage costs. The historical protocol-1→2 comparison measured 4-KiB PUT p50 3.857→6.745 ms and three-operation transaction rate 496.2→311.5/s under paired FULL-durability conditions; this is evidence of journal cost, not a complete RustFS explanation. The layer screen found hot metadata and blob fixture rates around 2,143 and 2,153 transactions/s versus signed S3 around 472, but the fixtures are not identical and cannot be subtracted. CPU stacks were unavailable. The packing experiment produced a promising first pair but its full adoption matrix was inconclusive, so files remain the production format. See [journal](storage-journal-2026-09.md), [attribution](storage-attribution-2026-09.md), [metadata capacity](storage-metadata-capacity-2026-09.md), and [packing measurement](storage-packing-measurement-2026-09.md).

Current `put_object` visibly awaits storage admission, stages a durable blob, awaits publication, then awaits event emission and `RecordActivity`; this is a sequencing observation, not a measured ranking of costs. The Writer already exposes queue/begin/apply/commit timings. Multipart has separate blob stage samples. Ordinary PUT lacks the same complete stage split. P2 must resolve that gap before any causal bottleneck claim or invasive design change.

The current-head 1-MiB PUT baseline is 47.84 versus 92.17 MiB/s by arm medians (Cairn about 52% of RustFS). Cairn's three fresh-store arms were 97.46, 44.56 and 47.84 MiB/s: its first arm nearly reached RustFS's median, so a fixed per-request overhead alone is not an established explanation. Exactly 1 MiB also enters the `raw_io::HINT_THRESHOLD` preallocation/page-release path; this is a candidate to measure, not a finding. The mixed 1-MiB Cairn arms fell from 345.48 to 197.06 to 139.53 MiB/s across fresh stores, so P6 must distinguish Cairn-internal contention from host/client drift before treating its median as a stable capacity estimate.

## P1 host preflight — not benchmark evidence

On 2026-09-23, this workspace was clean before this tracker was added. The node has four KVM-exposed Skylake cores, 7.8 GiB RAM without swap, and one 100-GB rotational-advertised ext4 virtual disk; `/var/tmp` and the repository share that filesystem. About 73 GiB was available. `/SSD` is absent on this node, so the prior laboratory's `/SSD` examples cannot be reused literally. Docker exists, but the paired comparison should use native binaries sequentially to avoid Docker storage-layer differences. Python 3 has no boto3 in the default environment; the existing Python storage laboratory uses its own Rust signed-S3 client, while Warp remains the external macro client. `perf` is not installed; `perf_event_paranoid` is 3, so CPU-stack attribution must be marked unavailable unless a safe local profiler is present. No benchmark server, corpus or timer was started in this preflight, and the new 3,600-second/10-GB allowance remains unused.

Before the first arm, P1 must pin the current Cairn revision and the chosen stable RustFS/Warp releases by exact version and SHA-256, declare a private `/var/tmp` root and a process/space ownership fence, and test cleanup on a tiny fixture. Existing `conformance/bench_compare.sh` is a MinIO CI comparison, not the bounded RustFS campaign: it runs both servers concurrently and its default ratios are not a substitute for the planned paired measurements. The historical RustFS result remains a released-binary reference only.

### P1 build and smoke record

The current Cairn checkout is `f6fde5a6` (`cairn 0.1.0-dev+gf6fde5a6`). A native pinned build first stopped at `linker cc not found`; it produced no executable. A task-owned Rust 1.97.1 toolchain was installed without changing the system rustup directory. The production web bundle was built with `npm ci` and `npm run build`; the first read-only container compile before that bundle existed failed at the expected `rust_embed` folder check. A second build in the official `rust:1.97.1-bookworm` container (image digest `sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`), with the checkout mounted read-only and all Cargo output under the task-owned `/var/tmp/cairn-performance-xUsAMt`, completed the default-feature optimized release profile. The executable SHA-256 is `7f239fcd912875addcc385e755462ac6788492b5bc127c77d777a9bd2cb2cd3e`. Build compiler identity matches the pinned Rust 1.97.1; this is a build record, not a performance observation.

The executable passed `validate-config`, bootstrapped an isolated loopback-only smoke store and returned HTTP 200 `ok` on `/healthz`. The server shut down cleanly and the exact smoke-store directory was deleted. The fixed smoke key and default development credentials were scoped to that removed store; neither is suitable for a measured or exposed node. At this build/smoke milestone, no Warp arm had run and benchmark allowance was still **0 / 3,600 seconds and 0 / 10,000,000,000 measured-data bytes**. The later measured consumption and final cleanup are recorded in [the result](performance-current-2026-09.md#correctness-failed-attempts-and-budget).

Official release discovery on 2026-09-23 found [RustFS 1.0.0](https://github.com/rustfs/rustfs/releases/tag/1.0.0) marked latest and the [MinIO Warp Linux/amd64 directory](https://dl.min.io/aistor/warp/release/linux-amd64/) listing version 1.8.0. These were selected for P1. Pin the actual downloaded asset bytes against their published SHA-256 before execution; record the binary-reported versions as well. Do not substitute a moving `latest` URL after preregistration.

Both candidates were then downloaded to the private task root and verified against the publisher's digest: RustFS `rustfs-linux-x86_64-gnu-v1.0.0.zip` SHA-256 `2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42` (the value shown on its GitHub release), and Warp `warp.v1.8.0` SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` (matching the adjacent `warp.v1.8.0.sha256sum`). The extracted RustFS executable hash is `a2dedb783bdf1ff6c97a25a0a07230e6488b3b63cef4eb80a2ab4c94006002bd`; it reports version 1.0.0, commit `d47f54bfb2f39f48bd1adda334bd27e151fe85b8`, built with Rust 1.98.1. Warp reports v1.8.0, commit `13c3b89`. No load was sent to either executable in this step.

The RustFS single-node/single-disk loopback smoke then returned HTTP 200 from `/minio/health/live`. Its process exited cleanly and its exact temporary volume was deleted. Both server startup paths now pass on this node. No benchmark operation has run; the next deliverable is the bounded Python coordinator and predeclared operation matrix.

### P1 preregistered current-head comparison

The coordinator is [`conformance/rustfs_compare.py`](../conformance/rustfs_compare.py). One diagnostic pair of 4-KiB PUT at concurrency 32 and five seconds per arm will first check client/server interoperability and parser behavior; it is **not** part of the scored matrix. The scored matrix is three fresh-store pairs per case, 12 measured seconds per arm, alternating Cairn→RustFS then RustFS→Cairn then Cairn→RustFS. Each Warp operation uses path-style SigV4, the same random-generated payload policy, one bucket, identical concurrency and object-pool controls. Only one server receives traffic at a time. No source or build change is allowed between arms. The case order is fixed below.

| Case | Warp operation | Size | Concurrency | Prepared pool / part control |
| --- | --- | ---: | ---: | --- |
| `put_4k` | put | 4 KiB | 32 | duration-bound |
| `put_1m` | put | 1 MiB | 16 | duration-bound |
| `get_4k` | get | 4 KiB | 16 | 1,000 objects |
| `get_1m` | get | 1 MiB | 16 | 64 objects |
| `head_4k` | stat | 4 KiB | 16 | 1,000 objects |
| `list_4k` | list | 4 KiB | 16 | 1,000 objects |
| `delete_4k` | delete | 4 KiB | 16 | 12,000 objects; finite deletion may end early |
| `mixed_1m` | mixed | 1 MiB | 16 | 100 objects |
| `multipart_5m` | multipart | 5-MiB parts | 16 | 100 parts; this is a part-upload test, not a complete-multipart lifecycle |

The scored cells are descriptive medians of three arms per engine, **not** the five-pair adoption gate. The source archive, toolchains and build outputs already occupy about 2.5 GB of the 10-GB task-owned cap; the coordinator stops a new arm without 1 GB headroom and kills an active Warp group if the cap is crossed. It leaves 30 seconds of the 3,600-second campaign for teardown. All server data is under one task-owned `/var/tmp` root and removed after each reaped arm. Warp summaries and resource samples are retained in a compact JSON result; unexpected errors, missing measured-operation summaries, time/space exhaustion or prematurely finite DELETE are reported as inconclusive, not silently omitted. The first diagnostic and any failed runs consume the same cumulative one-hour allowance; their elapsed time is recorded before deciding whether the full matrix fits.

Execution amendment: the first scored invocation completed the 36 arms through LIST, then Warp 1.8 rejected the initial DELETE configuration **before measurement** because its default batch/concurrency requires at least 6,400 prepared objects. The failed attempt and full elapsed time are retained. The DELETE pool is raised from 2,000 to 12,000 solely to satisfy that client-side minimum and avoid finishing the finite workload during the 12-second interval. DELETE, mixed and multipart run as separate continuations; their pair order and all other parameters stay fixed. This operational correction is not a selected performance result.

Warp then completed the larger DELETE preparation but emitted `Skipping DELETE too few samples`, so standalone Warp DELETE is **inconclusive**. Its `multipart` command with `--parts 100 --part.size 5MiB` created a 500-MiB object but rejected GET because it expected 5 MiB; the observed client-side size expectation makes that arm unusable as server performance evidence. Both failed arms and their costs stay in the campaign record. A separate, standard-library Python SigV4 client in [`conformance/rustfs_s3_compare.py`](../conformance/rustfs_s3_compare.py) supplies the missing cells with the same three-pair alternating order: 12,000 prepared 4-KiB objects, concurrency 16, then a 12-second standalone DELETE interval; and full initiate→two 5-MiB parts→Complete→byte-verified GET→DELETE cycles, concurrency 4 for 12 seconds. The latter reports completed 10-MiB objects/s and verified MiB/s, not Warp's failed part operation. These cells are labeled by their different client, never pooled with Warp throughput. Exact success responses and complete-object SHA-256 are mandatory. A client-saturated or prematurely exhausted arm is inconclusive.
