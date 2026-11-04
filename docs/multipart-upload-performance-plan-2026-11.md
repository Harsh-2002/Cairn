# Multipart upload performance plan — 2026-11-03

**Status:** research complete; phase 2 item 1 implemented and measured (0.64× -> ~0.69× on a
single pair). Items 3-5 remain open. The measurement harness, the `multipart_put_5m` case, and
sampled part/commit timings are in place. Phase 3 (completion) is implemented and measured:
assembly hashes on a blocking reader concurrently with the sink write, and CRC-64/NVME slices by
eight, cutting a real completion from a 246 ms median to 210 ms (see `benchmarks.md` section 11).
A later part-body pipeline screen measured a real stage win but no repeatable score win and was
reverted; it also isolated the per-upload completion tail as the then-dominant scored cost — see
"Where the scored multipart-put ratio actually goes".

Goal: close the strict multipart-upload gap against RustFS without weakening the UploadPart
acknowledgement. Ordinary PUT/GET/LIST results must not regress.

## Baseline

Local bare metal (6-core i5-9400T, NVMe, strict durability, Warp v1.8.2), 10 × 5 MiB parts per
upload, 4 concurrent uploads × 4 concurrent parts, 8-second arms, three interleaved
Cairn/RustFS pairs:

| Workload | Cairn | RustFS | Ratio |
|---|---:|---:|---:|
| Multipart upload, 5 MiB parts | 37.4 obj/s (187 MiB/s) | 63.6 obj/s (318 MiB/s) | **0.59×** |

Warp's `multipart-put` report is `PUTPART`, so the rate above counts 5 MiB part operations:
37.4 × 5 MiB = 187 MiB/s, i.e. **3.74 completed objects/s** for Cairn. The completion-inclusive
object rate is measured separately in [`benchmarks.md`](benchmarks.md) section 12.

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
not client concurrency: Cairn 47.5 PUTPART ops/s vs RustFS 75.0, with similar ~0.57 s request
latency. Cairn barely improved from 44.6 ops/s at 16 in-flight; RustFS improved from 69.6.

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

- 2026-11-03: idle-host part-path split (release `7104dcf3…`, 15 s `warp multipart-put`,
  10×5 MiB, 4 objects, part concurrency 4): p50 total 78.5 ms, `blob_stage` 75.1 ms,
  `storage_admission` 1.6 ms, `publication` 1.0 ms, Writer `commit` 0.08 ms. The two commits are
  ~3% of a part; collapsing one is not the next win. Assembly p50 340.5 ms and durability 0.77 ms
  under the same load. The body/sink path is the remaining candidate and needs its own A/B.

- 2026-11-03: body/sink buffer A/B (negative). A 1 MiB part-only staging buffer reduced direct
  p50 sink+finalize from 28–32 ms to 20–25 ms, but body p50 stayed 59–76 ms and throughput ~68
  obj/s because client input wait varied 24–32 ms. The 256 KiB buffer stays; the change was
  reverted. Remaining part cost is client input wait (~27 ms), MD5 (~15 ms) and sink (~27 ms).

- 2026-11-04: completion assembly hashes on a leased blocking reader while the reactor writes the
  previous chunk (bounded depth 2, recycled 256-KiB buffers), and CRC-64/NVME updates eight bytes
  per iteration. A 10×5 MiB isolated A/B under a host-wide lock measured 232–238 ms before and
  206–208 ms after (assembly 189 -> 162 ms, durability unchanged); encrypted, compressed and
  checksum-requesting shapes all improve, and CRC64NVME drops from 334 to 231 ms. A real
  `CompleteMultipartUpload` probe measured a median 246 ms before and 210 ms after. Chunk size
  (256 KiB vs 512 KiB vs 1 MiB) and the io_uring sink were measured and rejected. Details and
  tables are in [`benchmarks.md`](benchmarks.md) section 11. The part path is untouched.
- 2026-11-04: **pipelined part body (measured stage win, no repeatable score win — reverted).**
  Measured against `433438f`, before the completion pipeline above.
  Candidate: drive the raw part body reader/hasher and the staging sink as two halves of one
  task (`try_join`), with a bounded read-ahead (tried 4 chunks, 32 chunks, and a 1 MiB byte
  budget), so the mandatory MD5 and the next body frames overlap the blocking part write. The
  sink kept its single owner, chunk order, and the caller's one flush/fdatasync barrier; the
  UploadPart ACK still followed the durable part record, and no reservation/publication rule
  changed.
  - Part-focused screen (16 concurrent one-part uploads, 5 MiB, 10 s arms, alternating on this
    host): `body` p50 96.5 and 64.3 ms in two baseline arms vs 59.6 and 58.0 ms with the 1 MiB
    read-ahead; a 32-chunk variant measured 63.8 and 43.9 ms. `finalize` moved the other way
    (the write lands closer to the fsync), so `body`+`finalize` together improved ~10–20%.
    The stage effect is real in that shape.
  - Scored harness (`multipart_put_5m`, Warp 10×5 MiB, c4, 12 s, RustFS 1.0.4 normalizer,
    alternating baseline/candidate pairs in four batches). Per-pair Cairn/RustFS ratios:
    baseline 0.773/0.737/0.734 vs 4-chunk pipeline 0.966/0.864/0.655; baseline 0.749/0.742/0.787
    vs 32-chunk 0.918/0.781/0.742; baseline 0.772/0.509/0.726 vs 1 MiB budget 0.713/0.490/0.533;
    a final batch with arms spanning load 0.9–9.4 measured 0.710/0.511/0.832 vs
    0.645/0.448/0.448. Mean body p50 fell from 92.3 to 72.2 ms in the 32-chunk batch, but the
    score moved in both directions and never reproducibly: the host was shared, and the same arms
    ranged 13–52 obj/s.
  - A simultaneous two-server A/B (identical wall-clock conditions, both builds on fresh volumes,
    same Warp load) measured the candidate at +5.0%, -0.8%, +14.8% and +2.7% (median +3.9%),
    with absolute rates halved by mutual contention. Not a repeatable score win.
  - Conclusion: reverted; the code change is not merged. The part body is not the scored
    bottleneck. See the completion-tail measurement below. Raw arms, ratios, stage p50s and
    binary hashes are retained in
    [`performance-evidence/2026-11/multipart-part-pipeline-ab-2026-11-04.json`](performance-evidence/2026-11/multipart-part-pipeline-ab-2026-11-04.json).

## Remaining gap

The measured residual is not a single missing micro-optimization. Hashing is now ~15 ms; the
staging write plus data fsync is ~25-32 ms; client input wait is ~24-32 ms; the two commits are
~2.6 ms. The one-commit publication and the 1 MiB sink buffer were both measured and rejected as
not worth their durability/memory cost. The remaining candidates are the part fsync shape against
RustFS strict mode and client-side input pacing, not a server micro-optimization. If RustFS defers the per-part
data barrier, Cairn's stricter acknowledgement is the remaining difference and should be
documented rather than removed.

Completion is no longer the dominant part of the gap: assembly is hash-bound (the mandatory
internal SHA-256 alone is ~150 ms for 50 MiB on a core without SHA-NI) and now overlaps the reads
and writes; the remaining ~45 ms is the data fsync, which is the same durability barrier the part
path pays.

### Where the scored multipart-put ratio actually goes

The scored `multipart_put_5m` throughput is not limited by the part handler. Warp's
`multipart-put` worker is strictly **create multipart upload → upload 10 parts at 4-way
concurrency → synchronous CompleteMultipartUpload → next upload**, four workers in parallel. An
upload therefore pays `ceil(10/4) = 3` part waves plus a serial completion tail, and the next
upload cannot start during that tail.

In the quiet three-pair batch on 2026-11-04 the per-part p50 was **Cairn 171.4 ms vs RustFS
179.9 ms** (Cairn 155.1 vs RustFS 189.9 in another pair, and Cairn's PUTPART TTFB p50 was 98 ms
vs 122 ms), yet Cairn measured 51.4 vs 66.4 PUTPART ops/s. Cairn's part service was already faster;
the missing throughput was the completion tail. A 50 MiB assembly measured p50 ≈ 340 ms before the
completion pipeline above, so Cairn's per-worker loop was ≈ 3 × 171 + 340 = 853 ms, which at four
workers predicts ≈ 4.7 completed objects/s (≈ 47 PUTPART ops/s; measured 51.4). Fitting RustFS's
measured 66.4 PUTPART ops/s to the same loop implies ≈ 3 × 180 + ≈ 60 ms of completion, i.e. a much
smaller tail. This is why part-path changes of 10–20 ms per part (≈ 30–60 ms per upload, ~5%) were
measured at or below the host noise even when the part stage clearly improved.

The completion pipeline in `benchmarks.md` section 11 then cut that real completion from a 246 ms
median to 210 ms on the same host, which validates the accounting: the same four-worker loop now
predicts ≈ 3 × 171 + 210 = 723 ms → ≈ 5.5 completed objects/s (≈ 55 PUTPART ops/s). The remaining
completion tail is still ≈ 150 ms larger than RustFS's, so the part path is ~70% of the loop and
its per-part service is already faster than the comparison; further part micro-optimizations can
recover only a few percent of the score even when they win their own stage. That 210 ms is a
sequential/isolated completion: the completion-inclusive run in `benchmarks.md` section 12 measured a
tail of ~1.0 s under the four-worker pipeline, dominated by the assembled object's fsync.

**Consequence for this workstream:** the part path's remaining durable barrier (write + fdatasync)
and hashing are not the scored lever. The scored ratio needs the residual completion tail closed
and/or a benchmark shape that measures parts without the per-upload completion tail. Do not merge
part-path micro-optimizations on the strength of a stage reduction alone; require a repeatable
scored-ratio improvement.

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
