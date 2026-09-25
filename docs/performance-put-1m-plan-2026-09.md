# 1-MiB PUT optimization plan — 2026-09-23

Status: live plan. The [verified-strict control and five-pair candidate screen](performance-put-1m-strict-2026-09.md)
are complete. The namespace candidate was reverted because it missed the end-to-end adoption gate;
temporary diagnostic stage timing was also reverted after failing its measured-overhead gate.
The [strict-mode follow-up](performance-strict-followup-2026-09.md) shows a smaller but variable
1-MiB PUT gap, a repeatable warm-GET advantage, a mixed-workload write-latency signal, and
no benefit from a 1000-µs Writer linger screen; no production change was adopted.
The [diagnostic sync-call probe](performance-sync-probe-2026-09.md) observed material root,
staging and other-directory barriers but has not passed an overhead gate; foreground/cleanup
separation is the next attribution requirement, not an excuse to drop any durability barrier.
An isolated [cleanup-deferral upper-bound screen](performance-cleanup-interference-screen-2026-09.md)
missed the PUT adoption threshold and was dominated by control drift; it is not evidence that
leaving exact debt behind would help a conformant design. Foreground/cleanup attribution remains.
The [cross-request root-fence candidate](performance-root-cross-request-screen-2026-09.md)
demonstrably reduced root sync calls but failed a consistent end-to-end PUT screen, and was
reverted. The separately approved campaign has 50.819 measured seconds left; no five-pair
adoption decision fits in that balance.
The separately approved [root-sync coalescing adoption campaign](performance-root-sync-adoption-2026-09.md)
ran five strict PUT pairs plus mixed/GET/LIST protection checks. That candidate was also reverted:
it won only one PUT pair, missed the median uplift gate, and showed a protected warm-GET regression.
Its 710.84 measured seconds were charged to a new 3,600-second ledger; the original campaign's
approximately 22 seconds remaining were preserved.
The later [shared-node resumption](performance-shared-node-resumption-2026-09.md) completed
42 zero-error arms, including five clean-HEAD strict PUT pairs. Cairn's 52.73-MiB/s arm median
trailed RustFS's 64.23 MiB/s, with a 0.817 paired-median ratio, while host contention varied.
The sampled timer's overhead gate was inconclusive and no optimization was adopted. The
resumption consumed 1,000.350591 of its separately authorized 1,200 measured seconds; the
remaining 199.649409 seconds were not silently reassigned to another campaign. The subsequent
offline runner change aligns complete host-census intervals to Warp's JSON aggregate
scored interval for standalone PUT, with recomputed-score parity required. It passed eight live
PUT arms in the later [root-sibling coalescing diagnostic](performance-root-sibling-coalescing-2026-09.md);
its approximately 6–7 seconds of host-sample coverage per 9–10-second scored interval is not a
host-noise correction. That candidate reduced sampled ordinary root-parent barriers from two
to one but won only one of two control pairs; the two candidate/RustFS pairs also reversed
winner. The separate 300-second diagnostic closed after 171.44 measured seconds, with no
throughput adoption claim. Its diagnostic timing remains in the working tree;
the later rejected root-sibling helper was removed.
The later [five-pair root-barrier adoption campaign](performance-root-barrier-adoption-2026-09.md)
also closed without adoption. Its two nearby strict Cairn/RustFS PUT pairs were
49.11/105.16 and 50.62/108.89 MiB/s (Cairn 46.7% and 46.5% of RustFS). The
candidate removed one sampled root-parent sync per ordinary PUT, but lost two of
five Cairn control pairs; the sampled lower-I/O-pressure arm won all five pairs
regardless of binary. One mixed pair's candidate PUT mean latency rose 31.5%.
All 20 arms passed strict/zero-error controls, 373.833801 of 600 measured client
seconds were used, and task scratch was cleaned. The candidate remains unadopted.
The remaining 226.166199 seconds are closed, not available for another run.

This narrows the next action: measure **exclusive per-PUT response-path waits** for
storage admission, blob stage, publication, notification and audit, with one
sampling decision per PUT but no object identifiers in exported metrics. Existing
blob-stage and Writer-batch histograms sample different populations; their means
cannot be added to reconstruct a request. A next diagnostic should also distinguish
foreground PUT from exact-cleanup Writer commits and directory syncs, and record
its observation overhead before ranking candidate changes. It may not reuse any
closed campaign allowance. A successful root-barrier micro-mechanism alone is not
a throughput fix on this host.

Diagnostic source follow-up (not yet load-measured): `cairn-protocol` now selects
one in 32 ordinary PUTs and records fixed-label preflight, storage-admission,
blob-stage, publication-prep, publication, notification, audit and total wall
durations for that same selected population. Error/cancellation drops retain
interrupted-stage evidence; the server drains a bounded ring into metrics, and
the Python comparison runner retains those fixed-label series. This does not
alter object storage, durability or response ordering. Three timer unit tests,
two new real-backend protocol timing tests, all 212 protocol unit/integration
tests, affected-crate/server all-target Clippy, 37 Python runner/ledger tests,
formatting and documentation checks pass. After removal of the rejected
root-sibling helper, pinned Rust 1.97.1 also passed the full owning blob suite
(105 unit, 42 integration, two environment-dependent ignored), all 212
protocol unit/integration tests, and warnings-denied all-target Clippy for the
blob, protocol and server crates. The 300-second `put_timing` Python profile
is prepared but **not authorized or run**: three source-matched control/timed
PUT pairs followed by one timed-Cairn/RustFS strict reference pair, with a
single ledger, exact mode/score checks and loss-free equal-count timing gates.
The later [live stage-timing diagnostic](performance-put-timing-live-2026-09.md)
built both pinned release binaries, then ran three source-matched PUT timing pairs
and one strict RustFS pair on this node. The exact-capacity first attempt stopped
after one arm because VM `MemTotal` ballooned; a separate opt-in bounded-balloon
diagnostic completed eight zero-error arms. Its 107 sampled PUTs place mean
response-path time at 48.5% blob stage, 18.5% storage admission, 18.0% audit
and 14.9% publication. Host I/O pressure tracks the slow arms. Reversed paired
throughput signs cannot establish probe overhead or a new optimization win;
the previous audit co-commit and root-barrier candidates remain rejected.
No throughput gain is claimed.

This expands P2–P6 of the [execution tracker](performance-execution-2026-09.md).
The [current comparison](performance-current-2026-09.md) is the historical, durability-unverified baseline.
Source inspected: Cairn `f6fde5a6`, RustFS `d47f54bfb2f39f48bd1adda334bd27e151fe85b8`, Warp `v1.8.0`.

## Objective and limits of the current evidence

Make durable 1-MiB PUT faster and consistent at concurrency 16, and carry that improvement into
mixed traffic without losing read performance or Cairn's small memory footprint. Preserve the
single database/Writer and every storage ownership and durability obligation in
[CONTRACT.md](../CONTRACT.md), [ARCH 8](storage-durability.md), and [ARCH 11](metadata.md).

| Observation | Meaning for this work |
| --- | --- |
| Historical PUT: Cairn 47.84 versus RustFS 92.17 MiB/s | Cairn delivered 51.9% of the observed competitor median, but this is not a matched-durability capacity estimate. The verified-strict control instead observed 58.48 versus 62.35 MiB/s with substantial variation. |
| Cairn PUT arms: 97.46 / 44.56 / 47.84 MiB/s | Large variation exists before any candidate change. Explain stalls and repeatability as well as average cost. |
| Mixed: 197.06 versus 219.98 MiB/s; Cairn arms 345.48 / 197.06 / 139.53 | A 11.6% median uplift would reach this historical competitor median, but the drift prevents a stable capacity conclusion. |
| Warm GET, HEAD and LIST favored Cairn in this campaign | Protect those paths while improving writes; this does not establish cold-read or sustained-device capacity. |

The two engines' effective durability was not recorded in the original campaign. A newly
identified configuration difference means the 1.93× number cannot establish a gap between
equivalent durable-write configurations. Neither this correction nor a strict RustFS rerun is
a Cairn optimization. Cairn before/after comparisons must use the same durability settings.
Both measured deployments were single-node, so avoiding distributed coordination is not an
advantage unique to Cairn in this experiment. The actual storage work and acknowledgement
contract determine the comparison.

Further pinned-source check: RustFS's default inline block is 128 KiB, and its own
storage-class tests assert that a 1-MiB object uses the **non-inline** path for the
wider erasure layouts. The tested runner clears inherited `RUSTFS_*` overrides, so
an unnoticed 1-MiB inline fast path is not a supported explanation for the gap.
On that non-inline strict commit path, RustFS starts the temporary metadata
write/sync and payload-shard fdatasync concurrently, waits for both, then
publishes the data and metadata renames and synchronizes the destination and,
for a new object, ancestor directories. This is source evidence of overlapping
independent durability prerequisites, **not** a measured explanation for the
throughput difference or proof of identical crash contracts. See the pinned
[storage-class policy](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/config/storageclass.rs),
[non-inline write decision](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/set_disk/ops/object.rs),
and [strict rename commit](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/disk/local/commit.rs).
Cairn cannot overlap its Writer publication with blob durability under `CONTRACT.md`;
its new `publication_prep` measurement can test whether any *independent*
replication/config preparation is worth overlapping with blob staging instead.

## Research result: pin competitor durability before ranking architectures

The exact tested RustFS source defaults **new buckets to `relaxed`**, even though the global
default is `strict`. The new-bucket constructor seeds the override; bucket configuration takes
precedence over the global setting. Relaxed mode syncs non-inline payload data but omits commit
metadata and directory syncs. The project's documentation requires strict mode for single-node
power-loss durability. See the pinned [durability guide](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/docs/operations/durability-modes.md),
[default selection](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/bucket/durability.rs),
and [bucket creation](https://github.com/rustfs/rustfs/blob/d47f54bfb2f39f48bd1adda334bd27e151fe85b8/crates/ecstore/src/store/bucket.rs).

Both Python launchers inherit the parent environment and leave these settings unspecified.
The historical effective bucket mode was not saved, so default-relaxed is the source-based
expectation, not a recovered runtime observation. The earlier stores and logs were cleaned up.

The corrected control explicitly sets the following in each server subprocess environment:

```text
Cairn:
  CAIRN_META_SYNCHRONOUS=full
  CAIRN_META_SHARDS=1
  CAIRN_META_GROUP_COMMIT_LINGER_MICROS=0
RustFS:
  RUSTFS_DURABILITY_MODE=strict
  RUSTFS_NEW_BUCKET_DURABILITY_MODE=strict
  RUSTFS_DRIVE_SYNC_ENABLE=true
```

A fresh bucket's override must be read back through RustFS's authenticated bucket-durability
admin endpoint before scoring; a global startup log alone is insufficient. Record the mode
after preparation too, since the client may recreate the bucket. Either precreate/verify the
actual measured bucket and use verified reuse semantics, or add a preparation/verification
barrier to the runner. A missing verification makes the matched-durability arm inconclusive.
Verify Cairn's parsed effective settings and actual Writer connection mode through its existing
configuration/test seams. Do not infer the Writer's PRAGMA from a separate SQLite connection.

An explicit RustFS relaxed arm can quantify that configuration's cost on this disk, but belongs
in a separately labelled comparison. No relaxed Cairn arm qualifies for production adoption.

## Current PUT costs and candidate changes

These are verified code sequences with unmeasured cost. Their order below is a research priority,
not a claim that a particular stage consumes most of the observed latency.

| Priority | Verified code behavior | Measurement and conditional optimization |
| --- | --- | --- |
| 1 | `namespace::prepare_path` calls `ensure_directory` for `.staging` and the bucket. Each syncs its parent, including on an existing directory. For ordinary flat PUT both parents are the data root. | Time namespace preparation and count parent syncs. Evaluate one shared root barrier after preparing both sibling directories and before creating dependent files. Then consider sharing concurrent barriers through the existing coordinator if it remains material. |
| 2 | Publication resolves three planned paths. The live final path remains referenced; the temporary and optional index-spool aliases become exact cleanup debt even after rename or when never materialized. | Count debts per PUT, cleanup syncs, Writer work and time spent draining. Evaluate batching absence barriers for claimed aliases in the same directory, preserving one exact outcome per claim. |
| 3 | PUT awaits durable storage admission, publication, and a separate best-effort `RecordActivity` mutation. Configured notifications can add another mutation. | Measure each await and Writer commit/batch costs. Screen existing group-commit linger first; consider audit co-commit only if it saves material response time. |
| 4 | At known length ≥1 MiB, staging attempts `fallocate(KEEP_SIZE)` and sequential advice; after file sync it attempts `DONTNEED`. | Separate allocation cost from page-release effects. Test preallocation and cache-release independently at the same payload size; protect immediate reads and memory pressure. |
| 5 | `BufWriter` and `OwnedFile::WRITE_BATCH` are both 256 KiB. Each underlying write copies bytes to an owned vector and submits a blocking job. | Count bytes copied/jobs and split submission wait from execution. Evaluate bounded buffer reuse or larger batches only if CPU/scheduling cost is significant. |
| 6 | Checkpoints run on the Writer; cleanup and read I/O also consume host resources. | Align throughput dips with checkpoints, cleanup batches, disk latency/pressure, CPU steal and client utilization. Tune the implicated shared resource. |

Code anchors: [namespace preparation](../crates/cairn-blob/src/namespace.rs),
[path planning](../crates/cairn-types/src/storage.rs),
[publication cleanup](../crates/cairn-meta/src/storage.rs),
[cleanup execution](../crates/cairn-server/src/multipart_claim_recovery.rs),
[PUT and audit](../crates/cairn-protocol/src/service.rs),
[staging](../crates/cairn-blob/src/staging.rs),
[placement advice](../crates/cairn-blob/src/raw_io.rs), and
[owned blocking writes](../crates/cairn-blob/src/owned_file.rs).

The parent sync is deliberate: `EEXIST` can race another creator whose durability barrier has
not finished. Any batching change must fulfill that obligation before dependent file creation.
The final renamed-file directory barrier already has a coalescer. It does not currently merge
the earlier parent barriers or the cleanup absence barriers. Linux separately requires directory
sync for directory-entry persistence; a file sync alone is insufficient.
[Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html).

The cleanup worker already executes up to eight operations concurrently. It wakes after one
second of idle time, claims at most 1,000 entries, and immediately continues after a fully
successful full batch. It can therefore overlap these short benchmark arms; increasing its
parallelism without measuring contention is not a supported recommendation. See
[worker scheduling](../crates/cairn-server/src/background.rs).

At c16, the default 64 blob-write permits do not by themselves show a need for more permits.
Increasing concurrency cannot remove a serial durability dependency and can increase disk
queueing. The actual permit wait and blocking-job queue wait decide whether to tune these limits.

## Work packages and completion criteria

### W0 — repair experimental controls

Extend the existing Python coordinator and fallback client rather than add another shell runner.
Reuse process/host telemetry from [storage_lab/processes.py](../conformance/storage_lab/processes.py)
and the campaign's existing ownership/space limits where appropriate.

- Sanitize `CAIRN_*` and `RUSTFS_*` in child environments, apply explicit settings, and retain a
  nonsecret allowlist of effective configuration with binary, source and client hashes.
- Implement and test the durability readback barrier described above. Test inherited conflicting
  settings, a bucket recreated by preparation, an unavailable readback and a wrong effective mode.
- Preserve explicit Warp mixed weights: GET 45, STAT 30, PUT 15, DELETE 10. These are the defaults
  in the tested [Warp source](https://github.com/minio/warp/blob/v1.8.0/cli/mixed.go); make them part
  of the result manifest. Record per-operation latency/rate/errors in addition to aggregate MiB/s.
- Remove repeated full-tree footprint walks from the hot observation path. The current runner
  recursively stats the entire task root every 0.5 seconds, including tool/build directories.
  Record static allocation once and budget live growth conservatively from admitted workload
  bytes; keep bounded space checks and a stop margin. Validate observer CPU and I/O overhead.
- Retain raw request records, small time-series samples, successful-operation counts, failures,
  mode evidence and the cumulative budget ledger in a compact evidence bundle before deleting
  stores and executables. Pre/post data-directory allocation counts run outside timed intervals.
- Separate preparation, warmup, scoring, metrics drain, correctness checks, cooldown and cleanup
  in timestamps and budget accounting. Keep the engines sequential and alternate pair order.

Done when a small fixture proves wrong modes fail scoring, cleanup reaps owned processes,
resource limits work and the result schema preserves evidence. This step delivers no speedup.

### W1 — account for end-to-end 1-MiB PUT time

Add fixed-cardinality observations through the existing timing/metrics mechanisms. Keep protocol
timing runtime-independent and retain bounded samples/counters; no key, bucket, request ID or
secret becomes a metric label. No per-chunk log records.

| Layer | Required split |
| --- | --- |
| Server/protocol | Authentication/authorization and preflight, storage-slot admission, Writer storage admission, total blob stage, checksum validation, replication intent, Writer publication, notification and audit, response completion |
| Blob | Write-permit wait, namespace job queue/execution, parent-directory sync, staging creation/preallocation, body wait, hash/transform work, buffered write/flush, file sync, page-release advice, rename, final directory-barrier queue/execution |
| Writer | Existing admission/queue/begin/apply/commit/checkpoint samples plus finite mutation-family counts and batch sizes; distinguish foreground work from cleanup and activity |
| Cleanup | Debts created/claimed/retired/deferred, oldest debt age, exact-cleanup physical duration, directory sync count and settlement wait |
| Host/client | Server/client/observer CPU, CPU steal, anonymous/PSS memory, file cache/dirty/writeback, process I/O, target-device diskstats, CPU/I/O pressure, open descriptors and threads |

Split blocking-job queue delay using a timestamp at submission and one inside the closure.
Completed-stage timing includes scheduling effects unless explicitly separated. Track interrupted
stages and dropped samples. Gather resource deltas in the scored window and during drain so
cost deferred until after load remains visible.

The existing metrics publisher drains Writer samples on a 15-second cadence; the old 12-second
arms cannot reliably expose that detail. Use longer attribution arms and collect a final drain,
or add an explicit bounded diagnostic snapshot using the existing mechanism. Scraping every
second does not make a 15-second source gauge fresh. Timestamp rare events and sample freshness.

Use exclusive phase totals to reconcile per-request wall time; nested blob phases and Writer
batch samples cannot be added to already-inclusive handler waits. Do not add percentiles or
assign one shared commit's duration independently as CPU work to every request. Report unassigned
time and sampling loss. A stage ranking is usable only if representative slow requests are covered.

Run instrumented/uninstrumented controls. Target ≤3% median throughput overhead with no material
tail change; if overhead exceeds this, reduce observation frequency or use sampled traces before
ranking costs. Missing CPU-stack access does not prevent wait attribution, but blocks a specific
CPU-hotspot claim.

### W2 — reduce measured durability coordination work

First screen the existing `CAIRN_META_GROUP_COMMIT_LINGER_MICROS` setting under FULL durability:
0, 100, 250, 500 and 1,000 µs, narrowing after short screens and retaining a c1 latency control.
SQLite FULL WAL commits include a sync; larger batches can amortize that cost, but an already
large batch or a blob-dominated workload may not benefit. See
[SQLite synchronous documentation](https://www.sqlite.org/pragma.html#pragma_synchronous).

If namespace barriers rank highly, implement the smallest candidate first: prepare the two
ordinary-PUT sibling directories, retain their descriptors/locks, sync their common parent once,
then allow file creation. Nested multipart paths must still honor parent-before-child dependency
levels. A cross-request extension may reuse the existing `DirSyncCoalescer`, keyed by device/inode,
with retained leases and error propagation. Measure it separately from same-request deduplication.
No directory cache or additional long-lived worker is part of this design.

If cleanup ranks highly, split exact deletion from its completion barrier so already-claimed
operations in the same directory can share a sync after their unlink/absence checks. Every
settlement must still carry its own exact claim, generation, lease and quota ownership; hold
descriptors until the shared fence completes, then preserve pruning order. A failed barrier
keeps all affected debts retryable. Never acknowledge cleanup merely because a name was absent.
Do not accelerate the benchmark by leaving an ever-growing cleanup backlog.

Required tests: concurrent directory creation with a delayed/failed creator sync, prune/recreate
races, renamed directory identities, descendant symlink/mount rejection, cancellation before and
during blocking work, shared-fsync failure, stale/expired cleanup claims, live-reference protection,
multipart quota aliases, and process crashes around creation/rename/publication/unlink/settlement.
Retain the current storage format and full recovery scans.

### W3 — test allocation and cache policy independently

The initial [independent 1-MiB hint screens](performance-placement-hints-screen-2026-09.md)
rejected both no-`KEEP_SIZE` and no-`DONTNEED` diagnostics: neither met a preliminary consistent
PUT improvement, so no production hint policy was changed.

At fixed 1 MiB, compare baseline against one preallocation change, then independently one
page-release change. Use isolated source variants for experiments, without adding a permanent
production feature flag. Keep actual-length checking, unused-allocation trimming, hashing and
durability identical. Confirm syscall/path engagement so an unsupported hint is not mistaken
for a working candidate.

Use 512 KiB, 1 MiB and 2 MiB as shape controls, not as the causal A/B. Include immediate PUT→GET,
the original mixed distribution and a stable read set under sustained writes. `DONTNEED` attempts
to discard cached pages; retaining them may benefit near-term reads but increase cache pressure.
Count device reads and cache/anonymous memory separately.
[Linux advice semantics](https://man7.org/linux/man-pages/man2/posix_fadvise.2.html).

Required tests: short/mismatched body, size ceiling, failed preallocation, trim/stat failure,
ENOSPC, incompressible/compressed/encrypted data whose physical length differs from declared
plaintext length, and immediate/range reads after success. Choose a threshold/policy based on
the measured workload tradeoff; 1 MiB is not intrinsically the correct boundary.

### W4 — remove a measured post-publication wait

An [eight-pair audit-omission diagnostic](performance-audit-upper-bound-2026-09.md) produced a
1.117× ratio of medians with all pairs favoring the diagnostic build. It omitted the audit
record and is **not adoptable**; its shared-node result justifies testing a safe co-commit.
The subsequent [safe co-commit adoption screen](performance-audit-cocommit-adoption-2026-09.md)
failed its five-pair strict PUT gate and was reverted. The omission result must not be treated as
a predicted production gain.

If audit is material, design a Writer submission that carries the successful publication and
optional activity record through one commit. Preserve the object operation's outcome, generate
activity only for a successful mutation, and isolate a recoverable activity SQL failure with its
own savepoint. Outer transaction/commit failures remain failures; they cannot be swallowed.
No detached audit task or new queue is proposed. Configured notification semantics need their
own measured decision; do not bundle notification changes into the initial audit candidate.
Today notification enqueue precedes the audit submission. A co-commit would make audit visible
earlier relative to notifications and narrow its crash-loss window. Document whether that ordering
is externally promised before implementation; if exact ordering must remain, reject this shape
and retain the current sequence. Best-effort does not automatically authorize changing ordering.

Mirror shared mutation changes in SQLite, libSQL/Turso, the in-memory double and shard routing.
Activity currently routes globally while object mutations can route per bucket, so existing
multi-shard behavior must be explicitly preserved even though the benchmark uses one shard.
If that requires a larger mutation redesign, defer this candidate behind the measured blob fixes.

Required tests: failed conditional PUT creates no success activity, one activity per successful
mutation, anonymous and authenticated actor preservation, optional-audit failure, ambiguous commit
acknowledgement, cancellation, same-key concurrency, notification ordering and backend parity.
Quantify the maximum possible benefit first: removing a stage that occupies 10% of critical-path
time can provide at most about 11% speedup in a simple fixed-work model.

### W5 — optimize blocking writes only when scheduling/copy cost warrants it

`OwnedFile` deliberately owns its buffers and leases across blocking jobs. Preserve that
cancellation safety while evaluating buffer reuse or a bounded batch increase. A 1-MiB raw body
requires at least four underlying 256-KiB write submissions, potentially more for fragmentation
or partial writes. Hashing already computes internal/requested SHA-256 from the same digest;
there is no basis for deleting an integrity check or claiming duplicate SHA work.

Screen 256/512/1,024-KiB batches only if W1 identifies this cost. Account for both the staging
buffer and the retained job buffer: a conservative two-buffer allowance at 64 write permits
would grow from 32 MiB to 128 MiB when moving from 256 KiB to 1 MiB, before other allocations.
Prefer reuse over growth where it delivers comparable gains. Slow senders, partial writes,
cancelled awaits and abandoned results must remain bounded and correct.

Tokio's own guidance recommends reducing blocking-task transitions through batching, but this
does not predict a speedup on this disk. [Tokio file-I/O guidance](https://docs.rs/tokio/latest/tokio/fs/index.html#tuning-your-file-io).
The optional io_uring backend is a later experiment: it also changes preallocation behavior, so
a whole-backend win cannot be attributed solely to syscall batching.

## Experiment order, budgets and adoption gate

1. Complete W0 and W1 plus their correctness fixtures. Rebuild optimized binaries from recorded
   sources; record build time separately. Keep the ordinary default-feature build as baseline.
2. Establish strict Cairn/RustFS controls and representative instrumented Cairn PUT/mixed arms.
   Start with 1 MiB/c16; use c1/c4/c32 only to explain the scaling curve. Retain every trial.
3. Choose the largest measured avoidable cost. Screen at most two candidates with one change
   each, then take one candidate into five interleaved Cairn-before/Cairn-after pairs.
4. Repeat the strict RustFS reference close in time and run protected operations. Publish
   throughput, p50/p95, CPU per successful operation/GiB, anonymous/PSS memory, physical bytes
   and inodes per live object, WAL and cleanup backlog. Include uncertainty and failed arms.
5. Run the owning regression/crash/conformance tests and applicable full repository validation
   gate before calling a production optimization complete. Clean owned stores/processes/tools
   after retaining compact evidence. Do not delete user data or unrelated containers.

At plan creation, the previous campaign had consumed 1,483.89 of 3,600 seconds, leaving
2,116.11 seconds. The subsequent [strict control and screens](performance-put-1m-strict-2026-09.md)
brought cumulative measured time to about **2,529.74 seconds**, leaving about **1,070.26
seconds**. The original provisional allocation of the then-remaining allowance
is 420 seconds for strict controls, 480 for attribution/observer checks, 240 for candidate screening,
600 for five-pair confirmation, 240 for protected-workload screens and 136.11 for verification/drain/
cleanup. Preparation, cooldown and failures count within those slices. Reserve conservative whole
invocation limits before starting, and reduce the matrix when a slice cannot fit. This is a work
budget, not a claim that a complete adoption matrix or endurance test will fit. An exhausted
allowance yields an explicit partial/inconclusive result; never reset the ledger.

Use 30–60-second scored arms where space permits, with identical warmup and preparation.
At 100 MiB/s a 60-second append-only PUT arm creates about 6,000 MiB before metadata and tools;
enforce the existing 10,000,000,000-byte whole-task cap with headroom and shorten both arms before
starting if needed. Do not silently switch append-only PUT into overwrite/churn to fit the disk.
A resource-capped arm is not a scored capacity result. Longer steady-state/endurance work needs
a separately recorded allowance if it cannot fit this one.

The adoption target remains ≥20% median 1-MiB PUT improvement over a freshly measured unchanged
Cairn baseline across five pairs, zero unexpected operation/integrity errors and no >10% regression
in the protected performance/resource metrics listed in the tracker. Report every paired ratio,
trial spread and uncertainty; contradictory pairs or unresolved host drift prevent an adoption
claim. Comparing against 47.84 from the old run alone cannot qualify. Keep p99 unavailable until
at least 10,000 successful samples per arm/operation are available.

The adoption profile in `conformance/root_barrier_campaign.py` reserves five alternating
candidate/control PUT pairs, two nearby candidate/RustFS pairs, then mixed/GET/LIST protections
under one fail-closed ledger. It independently checks each child report's executable hashes,
workload, order, strict modes, zero errors and live PUT timeline before admitting the next phase.
The Python runner now additionally records fixed `/proc/<pid>/io` server-process deltas over the
Warp-process interval and over complete sampled scored-PUT intervals, with unavailable/reset
samples reported as missing rather than zero. These counters distinguish server-attributed
physical I/O from host-wide diskstats/pressure, but remain Linux process counters rather than a
per-request or per-device latency trace. Earlier raw reports predate this field.
Offline fixture tests passed before the separately authorized client-load campaign. A completed
profile is evidence to evaluate against the adoption gate, not an automatic pass of that gate.

That separately authorized profile subsequently [completed](performance-root-barrier-adoption-2026-09.md)
all 20 arms in 373.83 measured client-process seconds under 10 GB, with raw evidence and cleanup.
It **failed adoption**: candidate/control PUT ratios were 2.619, 1.910, 0.727, 2.527 and 0.601;
the lower sampled host I/O-full-pressure arm won all five pairs irrespective of binary. Nearby
strict Cairn/RustFS ratios were 0.467 and 0.465. Mixed throughput was 4.6% lower, and its PUT
component mean latency was 31.5% higher in one protected pair. The 1.910× descriptive PUT
paired median is not a credible isolated code effect, and the competitor gap remains. The
candidate was an unadopted working-tree experiment and its root-sibling helper was
subsequently removed; do not promote its code or figures as a production speedup.

A same-node 90–100 MiB/s durable PUT rate is a useful stretch target suggested by the historical
numbers, not a forecast. Strict RustFS parity is evaluated against its new verified measurement.
Crash tests prove their injected failure model; process-kill success alone is not a power-loss test.

## How much of mixed performance can PUT fix?

PUT optimization is likely high leverage because reads already led in this measured matrix and
writes use shared disk/Writer capacity. However, 15% PUT requests in Warp's mixed distribution
does not mean 15% of occupied time: slower operations can dominate that time.

For a simple fixed-work estimate, let `f` be the fraction of time attributable to PUT and `s` its
speedup. Overall speedup is `1 / ((1 - f) + f / s)`. A 2× PUT improvement would close the historical
11.6% mixed median gap if PUT occupied roughly 21% of time and other costs stayed constant.
That fraction has not been measured. Queueing, cleanup, cache eviction and changing object counts
make the real mixed test more complex, so W1/W3 must verify the transfer of benefit.

Matching the historical 1.93× pure-PUT gap requires eliminating about 48% of baseline elapsed
cost in this simple model. Small CPU micro-optimizations cannot explain that by themselves.
The plan succeeds by identifying and reducing large waits or repeated work while measuring
steady cleanup and resource costs. It does not establish that cold reads, encryption, large
multipart completion, long-run growth or recovery performance are solved by a PUT improvement.
