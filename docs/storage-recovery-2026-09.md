# Offline baseline and recovery cost screen

Predeclared during Phase 3D implementation, before running any local recovery-cost experiment.
The Phase 3C comparison did not qualify journal-only startup: its performance conclusion is
INCONCLUSIVE, with substantial measured 4-KiB PUT/DELETE costs. Full startup scans remain required.
This screen describes the additional offline operation and does not compare a journal-boot candidate.

The primary measurement is `storage-baseline` command wall time, including its mandatory new safety
snapshot, classification, exact orphan cleanup, namespace proof and legacy finalization. Protected
observations are exact live object rows and bytes, backup/restore/full-scan startup time, and command
CPU/memory. Command durations are different operations on explicitly described states; their ratios
are not optimization results. There are no p99 or per-object service-time claims.

Use one prebuilt optimized Cairn binary with FULL SQLite durability, raw 4-KiB objects, one bucket,
four clients and seed 24301. Run 100 objects followed by 1,000 objects, once each. Each case:

1. starts an isolated node, prepares unique objects through signed S3 PUT, and observes the
   production worker drain deferred cleanup within the existing bounded five-second observer;
2. gracefully stops the node and verifies exact row count, size, ingest SHA-256 and empty journals;
3. measures an ordinary offline backup;
4. creates and syncs 64 uniquely named, fixture-owned legacy index aliases in `.staging`;
5. measures `storage-baseline`, including its separate safety snapshot, and requires complete
   coverage, zero pending ownership/debt, unchanged authoritative rows and every orphan absent;
6. measures process start through readiness using the still-mandatory full scan, then GETs and
   byte-verifies every object in the baselined source;
7. restores the earlier ordinary snapshot into a fresh directory, checks fresh incomplete coverage
   and identical authoritative rows, then GETs and byte-verifies every restored object; and
8. stops every owned process and removes source, both snapshots, restored tree and scratch data.

Preparation, observations, verification and cleanup count against the existing campaign. Reserve
180 seconds before hashing/discovery or creating data, with 15 seconds retained for cleanup and
the campaign's independent recovery allowance intact. Space admission includes all four data/DB
copies, SQLite page/index/WAL amplification, pending metadata-copy files, logs and cleanup headroom.
An error stops the screen; no automatic retry or increased allowance. A command failure remains
FAIL even if later cleanup or timing is incomplete.

```sh
sh conformance/storage_lab/run.sh recovery-cost \
  --root /SSD/dev/cairn-storage-campaign \
  --binary /absolute/prebuilt/cairn --commit EXACT_SOURCE_COMMIT \
  --build-settings 'exact recorded release build settings' \
  --seed 24301 --allow-seconds 180
```

The coordinator records binary/source hashes, declared revision/build settings, effective nonsecret
configuration, host/filesystem/tool information, process observations and each command's outcome.
GNU time supplies child CPU and maximum RSS, with elapsed time rounded to 0.01 seconds; coordinator
wall time includes launch, observation and teardown overhead. Startup has a readiness observation,
not a complete lifetime memory peak. Short commands may have sparse sampled anonymous/PSS data.
RSS is not an allocation or leak diagnosis. Processes restart, but filesystem caches are uncontrolled.
The one-shot 100/1,000-object screen cannot support a scaling law, five-pair adoption decision,
large-store recovery bound or power-loss claim. The eight-object fixture is only a correctness test.

## Recorded result

The single screen passed its correctness checks at source
`cc542fd279d5b43ff6c255684e1518a2353fd807`. Both cases preserved every authoritative object row,
GET-verified every object after baseline and again after fresh restore, removed all 64 legacy
aliases, and completed with no pending journal or quota work. The full evidence, command output,
process samples, binary/source hashes and export procedure are in
[`storage-recovery-2026-09.json`](storage-recovery-2026-09.json).

| Objects | Backup CLI wall (s) | Baseline CLI wall (s) | Restore CLI wall (s) | Full-scan start to readiness (s) |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.12 | 0.15 | 0.13 | 0.079 |
| 1,000 | 0.79 | 0.82 | 0.81 | 0.106 |

CLI wall times above are GNU time observations with 0.01-second resolution. The coordinator's
end-to-end baseline wall times were **0.385 seconds** and **1.028 seconds**, including launch,
sampling and teardown overhead as well as the command's mandatory safety snapshot. Baseline
maximum RSS was 16,112 and 18,816 KiB respectively. These operations ran on different explicitly
described states; subtracting or dividing their times does not establish an optimization gain.

Command used the predeclared seed 24301 and 180-second reservation, with the prebuilt default-feature
release binary: `opt-level=3`, fat LTO, one codegen unit, `panic=abort`, `debug=2`, `strip=false`,
Rust 1.97.1 (`8bab26f4f`, 2026-07-14). No retry or increased allowance was used. The screen consumed
16.610133 seconds including finalization; evidence export and owned-artifact removal added
1.016646 seconds. Cumulative campaign consumption is **592.146171 / 3,600 seconds**, with
**571,154,432 bytes recorded peak**. All measurement processes, datasets, snapshots and raw
artifacts are removed; compact evidence and the persistent ledger remain.

The diagnostic result is **PASS**. Activation remains **INCONCLUSIVE; KEEP full startup scans**.
This small, single-run screen does not change the earlier Phase 3C decision or establish a
large-store recovery bound, scaling law, tail latency, allocation bound or power-loss guarantee.
