# Packing measurement preregistration — Phase 4C

This is an isolated raw-record laboratory decision, not a production format, configuration or
S3 performance change. The coordinator is `conformance/storage_lab/packing_measure.py`. No
threshold was selected: the bounded run below is INCONCLUSIVE. Build and fixed correctness tests are separate from the
shared campaign. Run only after the collection/snapshot implementation and this preregistration
have passed review and the campaign is explicitly started.

## Fixed matrix and order

Use one prebuilt optimized `cairn-packing-lab`, concurrency 32, 1,024 initial objects and seed
`0x5eed`. The known-length size ladder is 1, 4, 16, 64 and 256 KiB, then 1 MiB. Unknown-length
4-KiB and 1-MiB workloads protect streamed dedicated-file fallback. A known-length 2-MiB control
also uses streamed dedicated files in both modes because it exceeds the 1-MiB packing ceiling.
These are nine cases, with five files/packed pairs per case: 90 fresh processes/stores in total. Loop pairs first, then the
listed case order; alternate files/packed on even pair indices and packed/files on odd indices.
Keep identical seeds, object identities, payloads, durability and worker limits in each pair.
The 1,024-object count is fixed before the first measurement to avoid extremely short
small-record arms. There is no data-dependent size, concurrency, object-count or seed selection
after observation.
Single-case/one-pair commands are diagnostics and cannot qualify any threshold. If this fixed
matrix is too short, noisy or incomplete, report INCONCLUSIVE; a larger experiment needs a new
preregistration and remaining campaign admission, with prior evidence retained.

Initial append ordinal `i` maps to key index `(i % 4) * (objects / 4) + i / 4` in both modes.
This fixed transpose interleaves key quarters so the declared churn can leave live/dead records
in the same immutable segment. Payload generation still uses the original key index and seed;
there is no adaptive batching or selection based on observed collection outcomes.

Every arm appends the fixed corpus, overwrites original key indices `[0, objects/4)` with
payload fixture index `key_index + objects`, and permanently deletes the disjoint original key
indices `[objects/4, 3*objects/4)`. It verifies the surviving half, reads bounded ranges, collects eligible segments,
and drains exact cleanup debt. Packed known-length ladder arms must actually copy at least
one live record and retire at least one source. Files, unknown-length and known-length 2-MiB
arms execute the same logical mutations with dedicated-file placement. Snapshot, fresh restore, reopened-store and restored survivor verification are
mandatory. The driver's protocol-1 record must report every expected count, zero pending writes
and zero cleanup debts; initial publication counters describe the append phase only.

## Primary and protected measurements

The primary is wall seconds for append + overwrite + delete + collection + exact cleanup,
including deterministic fixture generation, admission, durable filesystem publication, hashes
and SQLite acknowledgement. Require at least a **20% median paired wall-time reduction**
(packed/files ratio <= 0.80). Readback, range reads, snapshot, restore and reopen have separate
protected timers and are outside the primary. Readback sums initial whole-corpus verification
and post-churn survivor verification; the separate range phase reads one at-most-257-byte range
at offset `size/3` for each survivor. Snapshot includes the clean source close, while restore and
reopen each include opening the node, whole-survivor verification and a clean close. These are raw fixture bytes; encoding and complete
S3 request work are excluded identically from both arms.

No protected median paired regression may exceed **10%** in publication, overwrite, delete,
readback, ranges, cleanup, snapshot, restore, reopen, primary wall time, per-request publication mean/maximum or driver peak RSS.
Require these gates in every size proposed for packing and globally in the unknown-length
4-KiB/1-MiB and known-length 2-MiB controls. The known-length 1-MiB packed case belongs to the
size ladder: it must pass only when the selected contiguous threshold reaches 1 MiB. A smaller
threshold would route larger completed records to files, so a regression in an unselected
larger packing policy does not reject that smaller threshold. Collection has no meaningful direct files/packed timing ratio;
its full cost remains in the primary. Report collection copied/retired counts, logical/physical
bytes and memory admission peaks separately; they cannot replace missing timing or correctness.
The known-length 2-MiB and unknown-length 1-MiB controls protect the dedicated streaming path.
No claim extends to objects larger than 2 MiB or concurrent S3 traffic.

For each case, report every paired ratio and their median. A files-control max/min minus one
above **20%** in primary, publication, readback or range time makes the entire decision
INCONCLUSIVE. Require at least two target-executable process observations spanning at least
0.1 seconds, valid RSS/device observations, and coordinator sampling below half one CPU core
in every arm. Cache state is uncontrolled: process restart does not establish a cold cache.
Sample RSS is diagnostic; the protected peak RSS is the driver's whole-process high-water mark.

Publication latency reports count/sum/maximum. This matrix does not reach 10,000 successful
requests per phase/arm, so **p99 is unavailable**; do not pool arms or phases to invent its sample
size. Five paired descriptive medians do not establish a statistical confidence interval or
significance. Errors, data mismatches or leaked ownership fail correctness; missing metrics,
insufficient samples, drift, deadlines and observer saturation are INCONCLUSIVE.

Select only the **largest contiguous qualifying range from 1 KiB**. Stop at the first failed
size; later isolated gains cannot create a threshold. Any protected-control failure or failure
at 1 KiB retains dedicated files. A passing screen permits a separate format/migration/recovery/
contract proposal only. It does not enable production packing.

## Admission, execution and artifacts

Use the existing private `/SSD` campaign; never reset its ledger. The comparison reserves up to
1,080 seconds under the existing packing phase and the shared 3,600-second ceiling. Earlier
unused time carries forward; reserve 15 seconds for teardown within this command and preserve
the campaign's 30-second recovery reserve. Each driver is capped at the smaller of 120 seconds
and remaining work time. No automatic retry follows timeout or incomplete work.

Space admission precedes binary hashing, environment discovery and workload creation. Reserve
four worst-case corpora (original/replacement/snapshot/restored, each including generous
metadata/WAL and inode allocation), the 32-MiB admission budget, a 4-MiB replacement segment,
per-arm capped stdout/stderr/process samples, discovery logs and finalization headroom. Count
retained artifacts and the campaign's 1-GB cleanup reserve against the shared 100-GB ceiling.
Arms run sequentially and each dataset is removed only after its process group is quiescent.
Owned `TMPDIR` scratch, output drains and process identities follow the existing campaign
launcher. Interrupted cleanup leaves the active reservation intact and blocks new admissions.

The report retains declared revision/build settings, executable and source SHA-256, this
preregistration hash, host/filesystem/device information, exact configurations/commands, per-arm
records, bounded process samples, all rejected/incomplete outcomes and actual charged runtime.
No secrets or production configuration are inherited.

```sh
python3 conformance/storage_lab/packing_measure.py \
  --root /SSD/cairn-storage-campaign \
  --binary /absolute/path/to/prebuilt/cairn-packing-lab \
  --commit VERIFIED_COMMIT --build-settings 'release, debug=2, strip=false' \
  --pairs 5 --case all --seed 24301 --allow-seconds 1080 --device sdb1
```

The command is a reproduction template, not a recorded run. Compilation and tiny fixture tests
must pass before any measurement; retain files unless all declared evidence gates pass.

## Fixed correctness validation

The four added Rust tests and all 70 packing tests pass, including the unchanged 4A/4B
regressions. All-target laboratory Clippy and the debug driver build pass. The coordinator tests
include real 16-object files/packed/unknown-length churn, exact survivor/range checks, snapshot,
fresh restore and reopen; these deliberately remain INCONCLUSIVE for qualification because
they are outside the predeclared matrix. Synthetic tests cover missing/drifting/duplicate arms,
protected latency and memory, contiguous threshold selection, admission-before-discovery,
owned-process cancellation, and cleanup failure preserving the admission fence.

These are correctness fixtures, not campaign measurements or evidence of a production capacity.
Complete repository and final-commit CI validation are still required before integration.

The integrated local gate passes 1,466 default and 1,492 all-feature workspace tests, both
Clippy configurations, two doctests, all 80 Rust laboratory tests and all 84 Python tests with
every live fixture enabled. Web build/lint/audits, Rust audits, installer, shell and workflow
checks pass. These validations precede measurements; no threshold has been selected.

## Recorded outcome — retain dedicated files

Run `e651c93553c34d6c98a08835060556ed` used optimized source
`a5f390441a5cebf77f22a127d20bd58915916d2a`, Rust 1.97.1, opt-level 3, thin LTO,
debug information and no stripping or RUSTFLAGS. The admitted discovery confirmed `/SSD` on
`/dev/sdb1`, ext4. The complete machine/build identities, configurations, outcomes and process
observations are retained in [the measurement record](storage-packing-measurement-2026-09.json)
and its hash-verified [raw evidence archive](storage-packing-measurement-2026-09.raw.tar.gz).

**INCONCLUSIVE; KEEP files.** Eleven arms completed. The first 1-MiB packed arm reached its
fixed 120-second driver deadline; its terminal deadline record is preserved. The comparison
stopped as preregistered, before any unknown-length or 2-MiB controls and before any repeated
pairs. No automatic retry, threshold selection or production packing follows this result.
The timeout record does not identify the unfinished phase, so it cannot establish a particular
collection, readback or restore bottleneck.

The completed first-pair values below are descriptive diagnostics only. They do not provide
five-pair medians, control-drift coverage, tail percentiles or a qualifying threshold.

| Known record size | Files primary seconds | Packed primary seconds | Completed pairs |
| --- | ---: | ---: | ---: |
| 1 KiB | 3.3290 | 0.9632 | 1 |
| 4 KiB | 3.3511 | 0.9205 | 1 |
| 16 KiB | 3.5704 | 1.3660 | 1 |
| 64 KiB | 3.9497 | 3.2475 | 1 |
| 256 KiB | 5.7532 | 10.7614 | 1 |
| 1 MiB | 12.7502 | unavailable: arm deadline | 0 |

Every completed arm verified all 1,024 initial objects and 512 final survivors, all 512 ranges,
exact overwrite/delete counts, fresh restored/reopened survivors and zero pending writes/debts.
Each completed packed arm copied live records and retired source segments. These correctness
checks do not turn an incomplete performance matrix into a pass. At 1 KiB, even this single
pair's range time increased from 0.0624 to 0.1515 seconds; isolated primary improvements alone
would not meet the protected-workload policy.

The comparison charged **325.135765 seconds**, including teardown and finalization. Archival,
content-hash verification and removal of the exact owned artifact directory charged another
**1.357537 seconds**. Campaign consumption after this export was **918.639472 / 3,600 seconds**,
with recorded peak footprint **1,778,102,272 / 100,000,000,000 bytes**. Every owned process and
dataset was quiesced/removed; the active reservation is empty. The raw archive preserves all
71 artifact files, including the incomplete arm, before their temporary originals were removed.
Production remains one file per object with full-scan recovery. A future experiment needs a new
preregistration and explicit remaining allowance; it cannot relabel this attempt as successful.
