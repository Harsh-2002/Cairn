# Flat versus directory fanout: September 2026

Status: experiment design recorded before measurement. Production remains flat. This evaluation
implements storage evolution Phase 3B and may conclude KEEP flat or INCONCLUSIVE.

## Baseline and implementation

Compatibility/traversal PR #86 merged as `acd396f` after final head
`82471d9747814c8388ca6102e31ec21c76da0b65` passed all CI/CodeQL checks without review findings.
Its local gate passed 1,350 workspace tests (three skipped), two doctests, both Clippy builds,
web build and installer checks; 642 owning-crate tests passed with all features (five skipped).
New writes remain flat, schema/storage guards reject unsupported state, and full startup scans
remain mandatory.

The standalone `cairn-fanout-lab` binary compares a raw-file namespace publisher under the two
layouts. It performs temporary create/write, file synchronization, rename and directory
synchronization before acknowledging each publication. It compiles Cairn's exact directory-fsync
coordinator source with a laboratory measurement seam. Bucket/leaf creation is lazy and each new
parent entry becomes durable before dependent writes proceed. Names are seeded, UUID-shaped,
collision-free identities; no heap liveness/location map is required.

GET (whole and range), delete and reconciliation call the actual `LocalBlobStore`. The oracle
reconstructs exact expected locations from the seed/index and returns generated liveness; it adds
no SQL lookup cost. These are namespace/blob measurements, **not S3 throughput**. The raw publisher
omits protocol authentication, hashes, metadata publication and encoded staging. Delete measures
the existing BlobStore unlink API, which has no parent-fsync acknowledgement; reconciliation
includes its real directory synchronization and pruning. This is not a prototype of the Phase 3C
lifecycle protocol.

## Predeclared workload and gates

Seed: `0x5eed`. Five paired runs per workload, alternating flat/fanout and fanout/flat order.
Each arm runs in a fresh process against a new owned tree; the previous arm's data is cleaned
before admission of the next. Restart does not establish a cold page cache.

| Workload | Initial files | Bytes/file | Buckets | Concurrency |
|---|---:|---:|---:|---:|
| Hot bucket | 20,480 | 4,096 | 1 | 32 |
| Distributed | 20,480 | 4,096 | 16 | 32 |
| Large-file control | 512 | 1,048,576 | 1 | 4 |

Each arm publishes every file, verifies half with whole GET and half with range GET, runs an
all-live full scan, explicitly deletes half of each bucket's files, then runs a scan that reclaims
one further quarter. Finally it verifies every surviving file's bytes and every removed file's
absence. Range GET uses the middle half of the logical object. Counts and the exact survivor set
must match before an arm is considered correct.

**Primary metric:** all-live full-reconciliation wall time for the hot 4-KiB workload.

**Protected metrics:** publication/read/delete phase wall time (fixed operation counts), their
whole/range p50 and eligible p99 latencies, cleanup-scan wall time and process peak RSS in every
workload. A p99 is absent below 10,000 successful samples of that operation in that arm; the
small-file arms have 20,480 publications, 10,240 whole GETs, 10,240 range GETs and 10,240 deletes.
The 1-MiB control cannot establish p99.

A promotion proposal requires at least 20% median paired primary improvement and no greater
than 10% median paired regression in any protected metric. It also requires all five complete
pairs per declared workload, zero unexpected errors/byte mismatches, and complete measurements.
A flat-control span greater than 20% (max/min minus one) in scan/publication/read/delete wall time
is predeclared significant drift. Fewer than two samples of the actual target executable,
unavailable process/device measurements, or observer CPU at least 0.5 core during sampling makes
that arm INCONCLUSIVE. There is no external S3 client in this fixture; driver CPU includes both
helper and blob work and does not identify an engine capacity limit. Host/device counters can
include unrelated activity. Missing evidence cannot select fanout.

Report directory creation and parent-sync time, coalesced directory-sync request/call counts,
whole/range operation rates/latencies, live and cleanup scan times, RSS/anonymous/file/PSS memory,
threads/descriptors, driver and observer CPU, device counter deltas and host pressure. Cumulative
concurrent wait times are not disjoint CPU service time. RSS is not a retained-allocation or leak
diagnosis. Fanout adds directories; it does not reduce inodes or make startup sublinear.

## Shared budget and commands

Before this phase the persistent `/SSD/dev/cairn-storage-campaign/ledger.json` records
250.787718 seconds and 219,529,216 bytes peak. The 480-second fanout allocation plus unused
baseline time provides 829.212282 seconds; admissions preserve 30 seconds for recovery/cleanup.
The comparison requests at most 720 seconds and reserves **1,948,778,496 bytes**, including both
stores, directory/inode amplification, outstanding work and artifacts, plus the campaign's
separate 1-GB cleanup headroom. Preparation, all arms, reduction, verification and cleanup are
charged. Build and tiny deterministic correctness fixtures are separate.

```sh
cargo build --locked --release --manifest-path conformance/storage_lab/Cargo.toml --bin cairn-fanout-lab
sh conformance/storage_lab/run.sh fanout --root /SSD/dev/cairn-storage-campaign \
  --binary "$PWD/conformance/storage_lab/target/release/cairn-fanout-lab" \
  --commit "$(git rev-parse HEAD)" --build-settings 'release, debug=2, strip=false, thin LTO' \
  --pairs 5 --case all --seed 24301 --allow-seconds 720 --device sdb1
```

No experiment has run at this design revision. Final results, exact binary/source hashes,
commands, remaining budget and decision will be appended after charged measurement/reduction.
No laboratory result connects fanout to production; a qualifying result would need a separate
promotion PR with legacy-path and offline migration tests.
