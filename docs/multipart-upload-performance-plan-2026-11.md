# Multipart upload performance plan — 2026-11-03

**Status:** research complete; phase 2 item 1 implemented and measured (0.64× -> ~0.69× on a
single pair). Items 3-5 remain open. The measurement harness, the `multipart_put_5m` case, and
sampled part/commit timings are in place.

Goal: close the strict multipart-upload gap against RustFS without weakening the UploadPart
acknowledgement. Ordinary PUT/GET/LIST results must not regress.

## Baseline

Local bare metal (6-core i5-9400T, NVMe, strict durability, Warp v1.8.2), 10 × 5 MiB parts per
upload, 4 concurrent uploads × 4 concurrent parts, 8-second arms, three interleaved
Cairn/RustFS pairs:

| Workload | Cairn | RustFS | Ratio |
|---|---:|---:|---:|
| Multipart upload, 5 MiB parts | 37.4 obj/s (187 MiB/s) | 63.6 obj/s (318 MiB/s) | **0.59×** |

An earlier unpaired screen measured 218 MiB/s vs 343 MiB/s (0.64×), so the gap is consistent.

Cairn sampled part-stage medians (18 samples, 1-in-32 sampling, 12-second arm):

| Stage | Median |
|---|---:|
| **Total part handler** | **141.4 ms** |
| Blob stage | 125.3 ms |
| — Body (read + hash + write) | 94.6 ms |
| — — Input wait | 25.7 ms |
| — — Hash | 35.4 ms |
| — — Sink (buffered 256 KiB writes) | 30.1 ms |
| — Finalize (data fsync) | 15.8 ms |
| — Directory sync (coalesced) | 0.3 ms |
| — Namespace | 2.5 ms |
| — Staging create | 0.9 ms |
| Storage admission + reservation commit | 5.6 ms |
| Part-record publication commit | 6.2 ms |

The two Writer commits cost ~12 ms combined (8.5% of the part); commit service time is ~83 µs
with a median batch of 3. The dominant costs are hashing and the staging write/fsync, not the
Writer.

A 64-in-flight screen (8 uploads × 8 part concurrency) shows the limit is per-part service rate,
not client concurrency: Cairn 47.5 obj/s vs RustFS 75.0, with similar ~0.57 s request latency.
Cairn barely improved from 44.6 obj/s at 16 in-flight; RustFS improved from 69.6.

## Code path

Per part (`crates/cairn-protocol/src/service.rs`, `upload_part`):

1. Scope the upload id to the bucket/key (read).
2. `admit_part_storage` → `Mutation::AdmitStorageWrite { ReserveMultipartPart }`: one durable
   Writer commit that admits the storage plan and reserves quota/cardinality.
3. `stage_part` (`crates/cairn-blob/src/lib.rs`): create the staging file, stream the body,
   hash plaintext, `fdatasync`, then the coalesced session-directory fsync.
4. `PublishStorageWrite { RecordPart }`: a second durable Writer commit that inserts the part
   row, consumes the reservation, supersedes an older part at the same number, and resolves the
   exact storage paths.
5. ACK the part.

A completed multipart object is assembled later, in one pass. It now writes a v2 metadata sidecar
with its part layout, so the durable file is authoritative for the object too (Section 2 of
[`multipart-followup-decisions-2026-11.md`](multipart-followup-decisions-2026-11.md)).

## Findings

- **Hashing is the largest single cost** at 35.4 ms for 5 MiB (~148 MiB/s). The part path uses
  the same `Hashers` as an ordinary object, which always computes the internal SHA-256 even
  though a part's `internal_sha256` is discarded at completion (the assembled object gets a new
  baseline). The ETag still requires MD5.
- **The staging sink is the second cost** at 30.1 ms. `Staging` already wraps a 256 KiB
  `BufWriter`, so this is write/flush backpressure rather than syscall count; the finalize fsync
  adds another 15.8 ms. Both are durability-bound and must not be weakened.
- **Input wait is 25.7 ms**, consistent with client pacing; the server is not waiting on the
  network for most of the part.
- **The two Writer commits are not the primary bottleneck** (~12 ms combined), though removing
  one is still a measurable win.
- **Cairn does not scale with client concurrency** (47.5 obj/s at 64 in-flight vs 75.0 for
  RustFS), so the fix must reduce per-part service time or remove a serialization point, not
  just raise concurrency limits.

## Plan

### Phase 1 — measurement (this change)

- Add the `multipart_put_5m` case to `conformance/rustfs_compare.py` (Warp `multipart-put`,
  report `PUTPART`).
- Sample `stage_part` with the existing `ObjectWriteTimings` stages and split the raw body into
  input-wait/hash/sink.
- Sample `upload_part` with the existing `PutStage` labels: `StorageAdmission`, `BlobStage`,
  `Publication`, `Total`.
- Run three interleaved pairs with a 16-second metrics drain so the 15-second writer/put metric
  tick captures the window.

### Phase 2 — fix the measured bottleneck

Measured order, with a re-measure after each step:

1. **Remove the unused part SHA-256.** Build the part hasher without the internal-baseline
   SHA-256 unless the client requested SHA-256 or the signed-payload verification requires it.
   Keep MD5 and every requested checksum. No new dependency.
2. **Evaluate the MD5 SIMD backend.** `md-5` exposes an `asm` feature (`md5-asm`) on x86/x86_64.
   Enable it only if step 1 leaves hashing dominant, measure it, and verify the musl build. This
   is a feature of an existing dependency, not a new architecture.
3. **Do not add a sink buffer blindly.** The sink already has a 256 KiB `BufWriter`; if it stays
   near 30 ms, the cost is disk write/flush backpressure, not per-chunk syscalls. Consider
   `io_uring` or other write-shape changes only after a measured comparison, and never relax the
   file/directory barriers.
4. **Then collapse the two commits into one post-stage publication** (the ordinary-object
   `PublishObjectWrite` pattern). This is a ~6 ms win but also removes a serialization point:
   - plan the part storage in memory (no Writer round-trip);
   - stage under the planned path;
   - submit one `PublishPartWrite` savepoint that revalidates the active session, bucket/key,
     attempt id, exact path ownership, quota/counters, and supersedes an older part at the same
     number;
   - ACK only after that commit. A crash before it leaves an unreferenced staging file that the
     mandatory full scan reclaims; no acknowledged part is lost.
   Keep the part ACK durable; do not fold the part row asynchronously without a rebuild proof.
5. **Re-measure at 16 and 64 in-flight.** The first target is ≥0.95× RustFS at the standard
   4 × 4 harness shape; the second checks that the improvement scales.

### Phase 3 — completion

Only if completion shows up in the benchmark's scored interval. The assembly path already has
its own `MultipartStage` timings; do not conflate assembly with part upload.

## Implementation log

- 2026-11-03: part hashing uses `Hashers::new_for_part`, which skips the internal SHA-256 unless
  the client requested SHA-256 or the signed payload needs verification. `write_staged` now
  distinguishes the part path from the ordinary object path. Unit tests cover both branches.
  Measured: body hash 35.4 -> 14.8 ms; throughput 44.6 -> 48.9 obj/s on a single pair.
- 2026-11-03: enabled the existing `md-5` `asm` feature (SIMD/assembly MD5) as a measured
  candidate. The hash stage stayed at ~14 ms (within noise), so the feature was reverted; it did
  not justify the extra dependency.
- 2026-11-03: after the SHA-256 skip, a single pair measured 48.9 obj/s vs RustFS 71.1 (0.69×),
  up from 44.6 vs 69.6 (0.64×). The remaining measured costs are the staging sink/fsync (~40-55
  ms), input wait (~21-26 ms), and the two commits (~12 ms). The sink/fsync cost is the part
  durability barrier; do not relax it to chase the number.
- 2026-11-03: completion assembly no longer computes the whole-object MD5 when the composite
  ETag is supplied. A 50 MiB A/B (10×5 MiB, release, XFS NVMe, two-file sidecar in both arms)
  measured 326.1 ms mean / 326.0 median with the MD5 and 225.2 / 224.9 without it — a 31%
  assembly reduction. This is outside the part-upload scoreboard and changes no part barrier.

## Remaining gap

The measured residual is not a single missing micro-optimization. Hashing is now ~14 ms; the
staging write plus data fsync is ~40-55 ms; client input wait is ~21-26 ms; the two commits are
~12 ms. The next candidates are the single post-stage publication (about 6 ms) and a direct
comparison of the part fsync shape against RustFS strict mode. If RustFS defers the per-part
data barrier, Cairn's stricter acknowledgement is the remaining difference and should be
documented rather than removed.

## Acceptance

- `multipart_put_5m` reaches at least **0.95×** RustFS on the local harness with three pairs,
  ideally ≥1.0×; record the median and every pair. The 64-in-flight screen must also improve,
  not just the low-concurrency case.
- `put_4k`, `put_1m`, `get_1m`, `list_4k`, and `multipart_5m` do not regress.
- The full validation gate is green, including the storage-lab and conformance suites.
- A crash before the part publication leaves no acknowledged part and no referenced orphan; a
  crash after it leaves the part durable.
- The fix is code-grounded: every optimization cites the measured stage it removes.

## Risks

- Removing the pre-stage reservation changes the bound on staged-but-uncommitted bytes. Keep a
  bounded in-memory admission (the blob I/O pool and declared-size ceiling) and re-check quota
  in the publication savepoint.
- A single-commit design must preserve exact supersession/cleanup debt for re-uploaded parts.
- Do not trade part durability for throughput: the UploadPart ACK must still follow a durable
  part record (or a proven rebuild path).
