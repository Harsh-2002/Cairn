# PUT audit co-commit candidate: adoption screen — 2026-09-23

Decision: **reject and revert**. The safe single-shard audit co-commit candidate did not improve
strict 1-MiB PUT in five paired trials: it won two pairs and had an 8.5% lower median. Its LIST
median was 32.9% lower in the protected screen. All 28 arms completed with zero request errors,
but this shared node had large cross-arm variation and an unrelated VM/Go tests. These data do
not establish the candidate caused either slowdown; they do fail the adoption gate. No RustFS
reference was rerun for a rejected Cairn candidate, and no performance improvement is claimed.

The control was pristine commit `f6fde5a68c6a5d5acee54f950ce2be8cda547312` (release
binary SHA-256 `93fc7cba0ded7083a37d577267a0c1ba3e55c8d58c64c4a853875e3535072ba5`).
The candidate was that commit plus the tracked patch SHA-256
`d505153c15fd1735f5dd3ee8851e356772d9cfb8fb10d6e668887f742060cf80` (release binary
SHA-256 `a8db3344ad58655ba6421626519f880cb0bebdb341d5662efece46dcea474cee`). The test
additions were not copied into the release-build source and cannot affect its binary. Both
binaries used the same source archive path, pinned Rust 1.97.1 Docker image digest
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`, default
features, fat-LTO release profile and one real web build. Warp v1.8.0 (`13c3b89`) SHA-256 was
`d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.

The candidate carried the successful `PutObject` activity entry inside a new
`PublishStorageWriteWithActivity` mutation. SQLite and async backends nested a savepoint for the
best-effort activity INSERT inside successful publication, isolating a recoverable audit-row
error from the object commit. Conditional or unowned publication created no activity. The
in-memory double mirrored this behavior. Because account activity is global on shard 0, a
multi-shard router preserved the old separate best-effort global write after successful
per-bucket publication; only a single-shard node could co-commit. This moved activity ahead of
notification emission (previously notification enqueue was awaited first). The specification
does not promise that relative visibility order, but it would need explicit review before a
future adoption. No new schema, dependency, thread, queue, or durability relaxation was made.

The [Python A/B runner](../conformance/cairn_put_ab.py) used one server at a time, fresh stores,
alternating order, path-style SigV4 over loopback, full SQLite synchronous mode, one shard, zero
Writer linger, and a 7-GB per-run scratch cap. PUT used 1 MiB/c16, five pairs, 30-second scored
loads and 17-second metrics drains. Protected mixed used 45/30/15/10 GET/STAT/PUT/DELETE
weights and 100 prepared objects; GET used 64 prepared 1-MiB objects; LIST used 1,000 prepared
4-KiB objects. Protected runs used three pairs and 12-second scored loads. Rates are Warp's
analyzed-window averages, not preparation throughput; LIST uses objects/s, all others MiB/s.

| Workload | Pairs | Control median | Candidate median | Candidate/control | Paired wins | Invocation time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Strict PUT, 1 MiB/c16 | 5 | 67.76 | 62.03 | 0.915× | 2/5 | 544.27 s |
| Mixed, 1 MiB/c16 | 3 | 295.49 | 330.28 | 1.118× | 3/3 | 218.81 s |
| Warm GET, 1 MiB/c16 | 3 | 1,495.16 | 1,476.60 | 0.988× | 2/3 | 203.96 s |
| LIST, 1,000 objects/c16 | 3 | 100,907.19 | 67,741.56 | 0.671× | 0/3 | 239.34 s |

| Workload | Pair 1 control → candidate | Pair 2 control → candidate | Pair 3 control → candidate | Pair 4 control → candidate | Pair 5 control → candidate |
| --- | --- | --- | --- | --- | --- |
| PUT MiB/s | 52.54 → 62.03 | 67.76 → 53.14 | 57.97 → 78.92 | 69.68 → 61.53 | 68.56 → 67.79 |
| Mixed MiB/s | 295.49 → 330.28 | 317.60 → 350.25 | 145.68 → 236.63 | — | — |
| GET MiB/s | 1,922.03 → 1,318.72 | 1,495.16 → 1,692.67 | 1,461.37 → 1,476.60 | — | — |
| LIST objects/s | 69,141.81 → 57,042.56 | 100,907.19 → 67,741.56 | 100,977.53 → 100,406.10 | — | — |

PUT paired candidate/control ratios were 1.181, 0.784, 1.361, 0.883 and 0.989; their median
was 0.989. Control throughput ranged 52.54–69.68 MiB/s and candidate 53.14–78.92 MiB/s.
The second and third pairs reverse one another, so neither a precise slowdown nor a throughput
gain is causally attributable. PUT p50 medians were 301.9 ms control and 251.8 ms candidate,
again showing that request p50 does not predict total throughput on this host. Median PUT peak
RSS was 40.82 versus 41.49 MiB, no footprint improvement; mixed was 60.00 versus 59.75 MiB.
The largest observed per-arm benchmark-root allocation was 2,564,612,096 bytes. The separate
task-owned build tree was about 1.1 GB and generated web dependencies about 228 MB, keeping
the combined footprint far below 10 GB.

The candidate passed the pinned-toolchain compile check, 20 SQLite/libSQL/Turso/sharded storage
contract tests, a dedicated three-shard activity-routing test, and the protocol audit regression
covering a failed conditional PUT. The contract tests also exercised a duplicate activity ID:
the object still published while the activity insert rolled back. These tests verify their
specific fault model, not every ambiguous-commit or notification race. The full workspace gate,
all-features build, and crash/conformance suites were **not** run after this failed performance
screen; the change is not adoptable on partial verification.

The four invocations consumed **1,206.39 measured seconds**. With the prior 1,581.85 seconds,
the separately approved campaign stands at **2,788.24/3,600 measured seconds**, leaving
**811.76 seconds**. The original campaign's approximately 22-second balance remains untouched.
Compilation/download time was outside the measured ledger. The runner cleaned each fresh store
and its client/server processes after each arm. After retaining this report, the task-owned
isolated binaries, Cargo cache, client, scratch JSON, generated web assets/dependencies and pinned
Docker image were removed. No benchmark server/client remains; `git diff --exit-code` and
`cargo fmt --all --check` pass on the reverted tracked source. The untracked benchmark scripts
and reports are retained as the campaign deliverables.
