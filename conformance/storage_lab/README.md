# Storage laboratory

The approved [storage evolution plan](../../docs/storage-evolution-plan.md) gives all local
experiments one **3,600-second / 100,000,000,000-byte** campaign. This harness uses a private,
explicitly created directory below `/SSD`; it never attaches to an existing production node.
The shell launcher invokes a standard-library Python coordinator. Its small Rust binary has a
separate workspace and lockfile, and is absent from the production binary's dependency graph.

Build outside the experiment allowance; retain the commands and revision with the report:

```sh
cargo build --locked --release --manifest-path conformance/storage_lab/Cargo.toml
# Build the console first as described in the repository gate.
CARGO_PROFILE_RELEASE_DEBUG=2 CARGO_PROFILE_RELEASE_STRIP=false cargo build --release --bin cairn
```

Both binaries must be explicit, prebuilt, optimized builds with symbols for stack attribution.
The coordinator records each executable's SHA-256, the supplied revision/build settings, its own
source hash, the nonsecret effective overrides, hardware/filesystem information and tool versions.
The supplied build declaration is not a cryptographic proof that a binary came from that revision.
Do not put credentials in build settings. Production `CAIRN_*` environment variables are not
inherited; S3 nodes receive fresh ephemeral secrets, which are neither arguments nor artifacts.

Create the campaign **once** and reuse this root for every phase:

```sh
sh conformance/storage_lab/run.sh init --root /SSD/cairn-storage-campaign
sh conformance/storage_lab/run.sh run --root /SSD/cairn-storage-campaign \
  --phase baseline --layer meta \
  --binary "$PWD/conformance/storage_lab/target/release/cairn-storage-lab" \
  --commit "$(git rev-parse HEAD)" --build-settings 'release, debug=2, strip=false, thin LTO' \
  --concurrency 4 --buckets 1 --size 4096 --seconds 5 --idle 2 --max-ops 30000 \
  --allow-seconds 60 --primary-metric 'successful transactions per second' \
  --protected 'errors, returned bytes, operation latency, memory'
sh conformance/storage_lab/run.sh status --root /SSD/cairn-storage-campaign
```

Use `--layer blob` with the same layer binary, or `--layer s3 --binary /absolute/path/to/cairn`.
Run `--buckets 1` and distributed buckets separately. `--profile cpu` wraps the target with
`perf record -F 99 --call-graph dwarf`; `--profile heap` wraps it with heaptrack. S3 profiles attach
to the owned server; client resource samples remain separate. Unprofiled throughput must come
from a different invocation. Do not compare profiled and unprofiled rates as an optimization.
The selected profiler launcher must honor `TMPDIR`: Heaptrack 1.5.0's packaged shell script
hard-codes its FIFO under `/tmp` and needs an explicitly recorded local launcher adjustment
before use in this campaign. S3 target shutdown is directed to the exact owned executable
through a pidfd, allowing the wrapper to finish the trace before bounded group teardown.
Process samples identify executables; timestamped phase records allow load/idle alignment.

Three equal-duration load/idle cycles reuse one backend. The seeded byte generator is identical
in both drivers. The metadata driver initializes one storage generation, reserves each exact
object plan through the canonical FULL-durability Writer, then submits its original
`PutObjectVersion` through `PublishStorageWrite`. It reads through the WAL pool and lists the
worker's bounded 16-key overwrite ring. The `admit_publish` time includes both Writer submissions;
publication creates durable cleanup debt for unused aliases and superseded paths, retained until
owned teardown because this metadata-only fixture creates no physical files. It uses eight read
connections, 8 MiB per connection and no mmap. This workload is an attribution fixture, not the
complete Phase 5 metadata evaluation.
Blob transactions use explicitly synthetic fixture creation permits, then stage, fully read/verify
and reclaim an actual raw object using an explicit synthetic cleanup receipt and matching lease;
cleanup synchronizes absence and prunes empty parents. They have no metadata Writer or journal.
S3 transactions issue
signed PUT/GET/DELETE on one persistent connection per worker, without retries. Successful
transactions/second refers to these three-operation units, not individual S3 requests/second.
Every direct-layer cycle records `publication_variant`: `writer_admission_exact_publication_v2`
for metadata and `blob_only_fixture_permit_exact_cleanup_no_metadata_v3` for blobs. The metadata unit now has two
physical Writer commit points. Keep these results separate from historical bare-PUT trials; their
rate difference cannot establish journal-adoption cost or benefit. A valid comparison requires
matched workload/publication semantics and the predeclared paired experiment gates.
Operation sequences rotate across all configured buckets even when there are fewer workers than
buckets; per-bucket success counts are recorded and incomplete coverage cannot pass.
The blob driver's stage time includes encoding/filesystem/durability waits and is not isolated
fsync time. No compression/encryption claim follows from the initial raw-object workload.

Each operation's p99 is null below 10,000 successful samples. Reaching the operation cap early
prevents a PASS because it shortened the load interval. CPU/heap artifact collection alone is
INCONCLUSIVE until stacks, sample sufficiency and correlated measurements have been analyzed.
Missing tools, failed profiler startup, insufficient cycles, missing data and exhausted budgets
are INCONCLUSIVE. Operation errors are FAIL; operator interruption is CANCELLED. These statuses
are diagnostic availability/correctness results, never adoption decisions. A single run cannot
meet the five-paired-run, 20%-gain and protected-workload gates.

`samples.jsonl` records anonymous/file/shared memory, PSS, process I/O, thread/fd counts, CPU
ticks, device counters and host pressure. Server runs also preserve `/metrics` snapshots; direct
metadata runs preserve sampled admission/queue/begin/apply/commit/checkpoint timings, queue depth,
and dropped-sample counts. Stage sums describe sampled wall time, not disjoint CPU service demand.
Application-cache live bytes, SQLite allocator ownership, runtime active tasks, in-flight buffer
bytes and internal blob wait splits are explicitly unavailable in the initial layer driver.
Retained live allocation owners need heap stacks; an RSS plateau does not diagnose a leak.
The Python client/GIL can saturate, so full-S3 rates require a client-saturation check. Restarting
a process does not establish a cold disk cache.

The nonsensitive, fsynced `ledger.json` persists across invocations and holds an exclusive lock.
Time is reserved **before** hashing, environment discovery, dataset creation or process startup.
Preparation, load, idle, collection, verification and teardown all count. A clean run refunds only
unused reserved time; actual overruns are charged. A killed coordinator keeps its complete time
reservation and blocks admissions until `recover` establishes process quiescence and removes data.
Phase allowances are 600/480/840/1080/600 seconds in order; unused time carries forward and phases
cannot move backward. Admissions leave 30 seconds available for recovery/final cleanup.

Space admission includes all retained campaign artifacts plus a conservative whole-case reserve:
`max_ops × (256 KiB + 2 × payload_size)`, outstanding concurrency allowance, 1 GiB of ordinary
artifacts or 8 GiB for profiling, and 1 GB of cleanup headroom. This bounds the fixed workload's
replacement data, metadata/index/WAL amplification, staging, copies, profiles and logs; future
snapshot/packing drivers must supply their own complete reservations before use. Comparison
stores run sequentially; anything retained from an earlier arm stays counted. A footprint watcher
stops the child group at reserved headroom, `TMPDIR` points inside the owned artifact directory
(including profiler FIFO/scratch work), output is capped at 16 MiB per stream, and each
database/profile file is capped at 2 GiB. This is a cooperative lab workload limit, not a filesystem
quota for arbitrary or hostile executables. Symlinks and unexpected mounts refuse admission;
cleanup never follows a symlink or crosses a device.

Decode a completed profile under the same budget (decoding alone remains INCONCLUSIVE):

```sh
python3 conformance/storage_lab/analyze.py --root /SSD/cairn-storage-campaign \
  --run-id COMPLETED_RUN_ID --profile heap --allow-seconds 45
python3 conformance/storage_lab/summarize.py --root /SSD/cairn-storage-campaign --device sdb1
```

Reduction preserves target/client CPU separately, phase memory samples, Writer sample loss,
device counters and decoded heap timelines. Phase heap alignment excludes 250 ms at both
edges because process-start clocks are approximate. Artifact decoding and reduction have
their own bounded admissions; unavailable inputs do not become successful attribution.

Children start in owned process groups behind an EOF-sensitive gate. The coordinator persists
the leader's boot/PID/start identity before allowing exec. Teardown keeps the leader unreaped
until its group is quiescent, bounds TERM/KILL waits, reaps children and checks output threads.
Recovery uses pidfds and refuses an occupied group whose leader identity cannot be established:

```sh
sh conformance/storage_lab/run.sh recover --root /SSD/cairn-storage-campaign
```

Data directories are removed after each run. Results and bounded profiles/logs remain under the
campaign root for analysis and count toward later admissions. Archive the compact evidence into
the repository results record before `purge --root /SSD/cairn-storage-campaign` removes owned run
directories while preserving the ledger and compact results; do not reset or discard the ledger
to gain more runtime. Builds, fixed correctness fixtures below and CI are separately tracked and
do not establish performance evidence:

```sh
shellcheck -s sh conformance/storage_lab/run.sh
LAB_TEST_SERVER="$PWD/target/debug/cairn" LAB_TEST_RECOVERY_SERVER="$PWD/target/debug/cairn" python3 -m unittest discover -s conformance/storage_lab -p 'test_*.py' -v
cargo fmt --manifest-path conformance/storage_lab/Cargo.toml --check
cargo clippy --locked --manifest-path conformance/storage_lab/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path conformance/storage_lab/Cargo.toml
```

## Flat versus directory fanout

`run.sh fanout` runs `fanout.py` with an explicit `cairn-fanout-lab` binary, charging the existing
campaign's fanout allowance. Build with `cargo build --locked --release --manifest-path
conformance/storage_lab/Cargo.toml --bin cairn-fanout-lab` from the repository root. The complete
command, fixed matrix and predeclared gates are in `../../docs/storage-fanout-2026-09.md`.
Single-case or single-pair runs cannot qualify for adoption. Raw-file publication measures
namespace durability; GET/delete/reconciliation use LocalBlobStore. Its current publication
variant is `raw_namespace_descriptor_coalescer_exact_cleanup_fixture_v3`: the actual production directory
coalescer retains directory descriptors and explicit fixture I/O leases, with no metadata admission
or storage journal. Deletes use exact synthetic cleanup receipts and matching leases, including
durable absence and empty-parent pruning. This differs from the earlier v2 raw-delete workload.
This is not S3 throughput or journal-adoption evidence. The current coordinator
has no external fsync measurement hook, so actual coalesced syscall counts/times are explicitly
null with an unavailable reason; directory-sync requests and directory-creation timing remain
separate measurements. Historical pathname-coalescer results retain their original provenance and
cannot be pooled with this variant as one unchanged trial.
`LAB_TEST_FANOUT_DRIVER` supplies the debug binary to tiny deterministic Python fixtures;
`cargo test` also checks cancellation/failure cleanup and directory synchronization.

`fanout_report.py --root <campaign> --run <comparison-token>` charges reduction/export and
produces Markdown plus the full provenance/measurement JSON inside the owned campaign.
The September comparison retained flat placement after predeclared control-drift checks failed.

## Offline baseline and recovery

`run.sh recovery-cost` measures the predeclared 100/1,000-object offline screen in
[`storage-recovery-2026-09.md`](../../docs/storage-recovery-2026-09.md). It uses an explicit prebuilt
Cairn binary, the existing campaign's recovery allowance and GNU time. Source, ordinary/safety
snapshots, restored data, metadata/WAL, command observations and cleanup share one reservation.
Exact row/byte verification and cleanup success are diagnostic results; this descriptive screen
cannot activate journal-only startup. Full scans remain mandatory.
`LAB_TEST_RECOVERY_SERVER` enables its fixed eight-object correctness fixture.
