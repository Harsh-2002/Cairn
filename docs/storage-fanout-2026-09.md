# Flat versus directory fanout: September 2026

Status: **INCONCLUSIVE — KEEP flat**. All 30 arms passed operation/survivor verification, but
control drift exceeded the predeclared limit. Production remains flat; Phase 3B merged in PR #87 (`bf17dfe`).

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

The design was committed as `e5695bc` before measurement; final whole-survivor validation landed
as `fc87758` before the optimized binary was built and run. Binary and source hashes are recorded
in the JSON companion. Export/reduction uses `fanout_report.py` and is separately charged.
No laboratory result connects fanout to production; a qualifying result would need a separate
promotion PR with legacy-path and offline migration tests.

## Measurement result

**INCONCLUSIVE — KEEP flat.**

- hot-4k: flat control drift exceeds predeclared 20% span
- distributed-4k: flat control drift exceeds predeclared 20% span
- large-1m: flat control drift exceeds predeclared 20% span

All ratios are medians of paired candidate/flat measurements; lower is better.

| Workload | Metric | Flat median | Fanout median | Paired ratio |
|---|---|---:|---:|---:|
| distributed-4k | cleanup_scan_seconds | 0.130496 | 1.00228 | 7.6805 |
| distributed-4k | delete_0_p50_seconds | 0.000347703 | 0.000357944 | 0.9557 |
| distributed-4k | delete_0_p99_seconds | 0.00631426 | 0.0056888 | 0.8973 |
| distributed-4k | delete_wall_seconds | 0.200132 | 0.200508 | 0.9876 |
| distributed-4k | live_scan_seconds | 0.0257579 | 0.413662 | 14.6975 |
| distributed-4k | peak_rss_kib | 16776 | 17868 | 1.0739 |
| distributed-4k | publish_0_p50_seconds | 0.00475754 | 0.00450854 | 0.9905 |
| distributed-4k | publish_0_p99_seconds | 0.0162436 | 0.0187905 | 1.1568 |
| distributed-4k | publish_wall_seconds | 3.17759 | 3.24585 | 1.0714 |
| distributed-4k | read_0_p50_seconds | 0.000262133 | 0.000223176 | 0.7697 |
| distributed-4k | read_0_p99_seconds | 0.0061677 | 0.00586535 | 0.9698 |
| distributed-4k | read_1_p50_seconds | 0.000271336 | 0.000225596 | 0.7696 |
| distributed-4k | read_1_p99_seconds | 0.00643288 | 0.00567129 | 0.9009 |
| distributed-4k | read_wall_seconds | 0.356191 | 0.314012 | 0.8885 |
| hot-4k | cleanup_scan_seconds | 0.216269 | 0.561606 | 2.4597 |
| hot-4k | delete_0_p50_seconds | 0.000413633 | 0.000325863 | 0.8079 |
| hot-4k | delete_0_p99_seconds | 0.00601218 | 0.00590444 | 0.9729 |
| hot-4k | delete_wall_seconds | 0.218816 | 0.191617 | 0.8757 |
| hot-4k | live_scan_seconds | 0.0347266 | 0.104963 | 2.7084 |
| hot-4k | peak_rss_kib | 15092 | 15104 | 0.9939 |
| hot-4k | publish_0_p50_seconds | 0.00469604 | 0.00459103 | 0.9708 |
| hot-4k | publish_0_p99_seconds | 0.0170096 | 0.0178099 | 0.9601 |
| hot-4k | publish_wall_seconds | 3.22813 | 3.23274 | 1.0193 |
| hot-4k | read_0_p50_seconds | 0.000247367 | 0.000231683 | 0.9699 |
| hot-4k | read_0_p99_seconds | 0.00584135 | 0.005156 | 0.8805 |
| hot-4k | read_1_p50_seconds | 0.000246468 | 0.00023026 | 0.9724 |
| hot-4k | read_1_p99_seconds | 0.00567734 | 0.00569274 | 1.0167 |
| hot-4k | read_wall_seconds | 0.334641 | 0.31073 | 0.9285 |
| large-1m | cleanup_scan_seconds | 0.0146257 | 0.118305 | 9.1320 |
| large-1m | delete_0_p50_seconds | 0.00025166 | 0.000120909 | 0.6685 |
| large-1m | delete_wall_seconds | 0.0186374 | 0.00919804 | 0.6849 |
| large-1m | live_scan_seconds | 0.0014502 | 0.0277401 | 19.1285 |
| large-1m | peak_rss_kib | 13512 | 12132 | 0.9073 |
| large-1m | publish_0_p50_seconds | 0.0110039 | 0.0111592 | 1.0273 |
| large-1m | publish_wall_seconds | 1.50385 | 1.54213 | 1.0419 |
| large-1m | read_0_p50_seconds | 0.000820864 | 0.000512815 | 0.8991 |
| large-1m | read_1_p50_seconds | 0.000569091 | 0.000330191 | 0.9279 |
| large-1m | read_wall_seconds | 0.102534 | 0.0625483 | 0.8469 |

distributed-4k flat-control spans (max/min − 1): delete_wall_seconds=47.9%, live_scan_seconds=56.6%, publish_wall_seconds=15.4%, read_wall_seconds=71.6%.


hot-4k flat-control spans (max/min − 1): delete_wall_seconds=36.3%, live_scan_seconds=31.0%, publish_wall_seconds=21.5%, read_wall_seconds=15.4%.


large-1m flat-control spans (max/min − 1): delete_wall_seconds=126.7%, live_scan_seconds=181.2%, publish_wall_seconds=60.0%, read_wall_seconds=96.3%.


| Workload | Layout | Publications/s | Reads/s | Deletes/s | Directory sync calls | Created directories | Parent creation/sync seconds | Sync call seconds | Driver CPU cores | Observer CPU cores |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| distributed-4k | flat | 6445.13 | 57497.3 | 51166.1 | 19109 | 16 | 0.0144224 | 8.83355 | 1.34711 | 0.258084 |
| distributed-4k | fanout | 6309.6 | 65220.5 | 51070.4 | 20469 | 4084 | 3.38792 | 13.5564 | 1.20946 | 0.368965 |
| hot-4k | flat | 6344.23 | 61200 | 46797.4 | 5722 | 1 | 0.000266793 | 2.44972 | 1.17541 | 0.26379 |
| hot-4k | fanout | 6335.18 | 65909.3 | 53439.8 | 20230 | 257 | 0.282774 | 10.8463 | 1.23286 | 0.275574 |
| large-1m | flat | 340.46 | 4993.48 | 13735.8 | 367 | 1 | 0.000317039 | 0.68023 | 0.286731 | 0.204086 |
| large-1m | fanout | 332.007 | 8185.68 | 27832 | 511 | 222 | 0.504509 | 0.850552 | 0.373423 | 0.250911 |

Sync times are cumulative across concurrent calls, not disjoint service time. Read rates combine equal whole/range counts; eligible p99 values above are medians of per-arm p99, not a pooled p99.

Comparison `a2e24b6df5b94fb4a919b29dca8501c9` charged 137.368033 seconds. Data/process cleanup completed: True. The JSON companion preserves every arm, memory/descriptor/thread samples, device counters, pressure and exact provenance.

The nominal hot-bucket primary paired ratio is 2.7084, which does not meet the 0.80
adoption threshold. Because the control drift gate failed, these observations do not establish
a stable performance ranking or a general filesystem result. No rerun is used to search for a
passing outcome. Keep flat placement and mandatory full reconciliation.

Validation: 28 Python regressions and nine Rust driver/coordinator tests; standalone Clippy,
formatting and shellcheck. The unchanged production tree passed the full Phase 3A local gate;
all 54 final-head CI checks passed before merge.

After reduction, export and artifact cleanup, the cumulative campaign consumed **390.191882
seconds**, leaving **3,209.808118 seconds** of the original 3,600. Peak footprint is
**542,654,464 bytes**. No active reservation, owned child process or measured dataset remains.
The persistent ledger and compact result/export files remain until final campaign closeout.

Reproduce the charged reduction with:

```sh
python3 conformance/storage_lab/fanout_report.py --root /SSD/dev/cairn-storage-campaign \
  --run a2e24b6df5b94fb4a919b29dca8501c9
```

A final tiny debug fixture rerun failed once before its diagnostics were retained. Twelve
bounded fixture repeats and the complete 28-test Python suite then passed; the initial failure
was not reproduced or attributed, so no root-cause fix is claimed. Failure summaries now include
the recorded reason and bounded driver stderr, and the test retains the result reason in its
assertion. This is a residual laboratory reliability observation, separate from the 30 successful
measured arms. The final-head storage-laboratory CI job and all other checks subsequently passed.
