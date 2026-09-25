# Shared-node object-write stage screen — 2026-09-23

This is a **diagnostic screen, not an optimization adoption result**. The user declined a
reserved quiet window, so all work used the shared node. An unrelated VM was CPU-active
immediately before the benchmark; process snapshots during the two measured arms did not show
it, but the runner has no complete per-arm competing-process census. Host I/O pressure is
recorded below. One pair cannot establish a small overhead bound or a throughput improvement.
The subsequent names-only process-census runner change was made **after** these arms and does
not supply missing competitor data for them.
The later raw-body substage instrumentation was also made **after** these arms; their binary
hashes and six-stage observations refer to the earlier candidate only.

## Build and controls

The baseline is clean `f6fde5a68c6a5d5acee54f950ce2be8cda547312`, binary SHA-256
`35c0e1af2ad6e32dc9ad30addcdee338602798c1e132d71c94cf961c4d79a3d8`.
The candidate is that revision plus the bounded one-in-32 object-write stage sampler,
binary SHA-256 `f3f025f67002644b42ef70631f46a269f6da62a7cc27bcba034640447abe550d`.
Each used a separate Cargo target directory, the same pinned Rust 1.97.1 release profile, and
the same built web assets. Warp v1.8.0 SHA-256 was
`d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.
The Python A/B runner used fresh stores, Cairn FULL durability, 1-MiB PUT, concurrency 16,
12 seconds requested per arm, and order baseline then candidate. No task-owned build ran
concurrently with measured traffic. The runner removed both arm stores after reaping their
processes; both arms reported zero errors. The test is the existing `cairn_put_ab.py` control,
not a comparison against RustFS.

| Measurement | Baseline | Sampler candidate |
| --- | ---: | ---: |
| Warp PUT throughput | 108.13 MiB/s | 119.90 MiB/s |
| Warp analyzed interval | 9 s | 10 s |
| Actual client load interval (budgeted) | 17.088 s | 17.053 s |
| Server CPU over load | 24.41 s | 26.84 s |
| Peak server RSS | 40.81 MB | 40.43 MB |
| Host I/O PSI `some` over load | 4.404 s | 4.239 s |
| Host I/O PSI `full` over load | 2.020 s | 1.717 s |
| Task-owned peak bytes in each fresh arm | 1.363 GB | 1.506 GB |

The candidate's observed throughput is 1.109× baseline, but this is not evidence of a 10.9%
gain: the sampler has no intended throughput optimization, the two analyzed intervals differ,
I/O pressure changed, and there is no repeat or order reversal. Neither an overhead bound nor a
protected-workload conclusion can be drawn. The preliminary acceptance gate remains five
paired trials with protected cases and the predeclared footprint checks.

## Candidate-only stage observation

The existing 15-second metrics task drained 42 successful samples for each fixed stage and
reported **zero** evictions. Durations include scheduling and I/O waits. The six means below
are histogram sums divided by their counts, not CPU cost or complete request latency.

| Blob stage | Mean wall time of 42 samples |
| --- | ---: |
| Permit wait | <0.001 ms |
| Namespace preparation | 14.49 ms |
| Staging create | 3.06 ms |
| Body consume/hash/write | 29.08 ms |
| File finalize, sync and rename | 17.19 ms |
| Final-directory sync wait | 11.57 ms |

This arm's largest measured blob stage is body handling, followed by file finalization,
namespace preparation, and final-directory synchronization. It does **not** establish that
hashing, filesystem write, network receive, or scheduling is the dominant part of body.
The candidate's existing Writer metrics also reported approximately 22.59-ms median queue
wait and 36.61-ms median COMMIT, versus 23.28 ms and 38.65 ms in the baseline arm; these
Writer samples include other mutation kinds and their windows are not request-aligned.
The result therefore points to multiple waits, not a justified single architecture change.

The immediate next attribution experiment should split the sampled `write_staged` body into
request-body await, the always-on MD5 plus SHA-256 update, and `Staging::write_all`/buffered
file-I/O waits, without changing the hashes or their streaming order. Those are the actual
substeps in the plain 1-MiB path; the current body number cannot tell which one is expensive.
The generic ingest microbench statement in `testing-performance.md` predates this paired
diagnostic and does not substitute for current end-to-end attribution on this node.
Measure that against an uninstrumented control and collect CPU/perf samples only after granting
a fresh measured-load allowance. Keep the existing durability and writer fences intact.

The separately approved adoption campaign consumed **34.141 measured seconds** here. Its
ledger moves from 3,549.181 to **3,583.322/3,600 seconds**, leaving **16.678 seconds**.
No RustFS arm or protected GET/mixed/LIST arm was run in this screen. The original campaign's
separate ~22-second balance is unchanged. Build, correctness tests and metrics-drain time are
not counted as measured client load. All task-owned benchmark/build scratch is to be removed
after validation; this document retains the compact result and exact binary identities.

## Validation and cleanup

Pinned Rust 1.97.1 `cargo clippy -p cairn-blob -p cairn-server --all-targets -- -D warnings`
passed. `cargo test -p cairn-blob` passed 99 active unit and 42 integration tests;
`cargo test -p cairn-server` passed 321 active unit and one integration test. Python
`unittest discover` passed 47 conformance tests. `cargo fmt --all --check` and
`git diff --check` passed. The full workspace/feature/security gate was **not** run, and the one-pair
overhead screen does not satisfy the adoption gate; the sampler remains a diagnostic candidate.

After reaping processes, the two exact task-owned `/var/tmp` roots, generated `web/node_modules`,
`web/dist`, Python bytecode cache, and the task-pulled pinned Rust Docker image were removed.
No task-owned benchmark server, Warp client, or Docker container remained. The removals are not
recoverable from the scratch roots; the summarized results above are retained here.
