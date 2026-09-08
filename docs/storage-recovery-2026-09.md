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

No recovery-cost results have been collected yet. Campaign consumption before this screen is
574.519391 seconds, with 571,154,432 bytes recorded peak and no active experiment reservation.
