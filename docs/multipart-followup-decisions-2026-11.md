# Multipart follow-up decisions — 2026-11-03

**Status:** accepted. Two fixes are merged or in review; the remaining work is phased.

Five parallel reviews covered the multipart upload path: replication layout, object-file authority,
upload performance, benchmark methodology, and an independent architecture/security review. This
record is the decision, not a repeat of the analysis.

## 1. Replicated multipart layout — build now

**Decision:** replicate the source's exact part numbers and sizes. This is sender-only and needs no
new wire format, header, sidecar, schema, or destination change.

**Why:** the previous sink sent objects below 64 MiB as one PUT and re-chunked larger objects with
its own `max(64 MiB, ceil(size/10000))` geometry. A replica could therefore return *different bytes*
for the same part number, a different `ObjectParts` list, and a different composite ETag. That is a
silent wrong answer on a DR read path, not just a missing feature.

**Implementation:** `ReplicatedObject` carries the source row's `multipart_parts`; the sink replays
those exact numbers/sizes for every layout-bearing object, including objects below 64 MiB. The
layout is validated before contacting the destination (strictly ascending 1..=10,000, each part
≤5 GiB, every non-final part ≥5 MiB, sizes summing to the object). A malformed layout is terminal;
it is never silently replaced with transfer geometry. A zero-byte object still uses one PUT; a
layout-less large object keeps the old geometry.

**Consequence:** a layout-bearing object ≤64 MiB now uses the multipart path, so its destination
bucket must have versioning enabled. Existing replicas are not healed; `resync` re-ships and
replaces the layout.

**Residual risk:** AWS CRR parity for `?partNumber` on replicas was not independently verified.
The docs now describe Cairn's behavior instead of claiming single-part parity.

## 2. Multipart object-file authority — phase 1, later

**Decision:** extend protocol-3 file authority to new completed multipart objects, but as a
separate storage-format increment after the replication fix. Until then, the part-number path fails
closed when a composite ETag has no retained layout.

**Why:** a completed multipart object has no trailer and no `.meta` sidecar. `CompleteMultipartUpload`
commits at `synchronous=FULL`, so a crash after ACK is safe; the exposure is a lost/corrupt SQLite
database or an older restore. In that window, startup rebuild cannot restore the row, and the
mandatory reconciliation scan treats the headerless object as unreferenced and eventually deletes
it. The object's durable file is therefore not the authority for this object class, contrary to the
protocol-3 contract.

**Design (phase 1):**

- Plaintext assemblies get the inline `CRNI` trailer; compressed/encrypted assemblies get a
  `<uuid>.meta` sidecar because the container trailer must stay last.
- `ObjectMetadataRecord` gains `multipart_parts`; `format_version` becomes 2 only when parts are
  present. The shared inline-record bound rises from 64 KiB to a named 1 MiB (10,000 parts is
  ~400–450 KB); the writer must fail before commit rather than publish a record the rebuilder
  refuses.
- `StorageWritePlan::expected_paths` gives a `Completion` target the sidecar names and accepts the
  legacy 3-name completion plan for pre-upgrade journal rows. The `published_object` predicate in
  `resolve_paths` must include `Completion` in all three metadata backends, or a successful
  completion would enqueue its own sidecar as cleanup debt.
- `ObjectMetadataSeed` carries the layout and the already-resolved object checksums; TTL/`expire_at`
  are captured before `assemble`.
- `assemble` appends the trailer to the same sink for plaintext, or stages the sidecar and overlaps
  its fsync with the data commit for containers, then one directory barrier.
- `sidecar_rebuild` restores `multipart_parts`; the storage-protocol gate must prevent an older
  binary from rebuilding assembled objects as single-part objects.
- Phase 2 (optional): a rate-limited sidecar-only backfill for existing assembled objects; never
  append a trailer in place to a live plaintext object.

**Tests:** completion trailer/sidecar; startup rebuild of plaintext and container assemblies with
parts; corrupt/over-cap record; crash before/after publication; `?partNumber` after wiping the row;
backup/restore; delete/overwrite reclaiming data and sidecar; legacy assembled objects still served
and still not rebuilt.

**Why not now:** this is a storage-format and recovery change, not a request-path tweak. It needs
the protocol gate and a restore drill; it is not safe to bundle into the replication fix.

## 3. Multipart upload performance — measure before optimizing

**Decision:** do not collapse the two part commits or fold the part row asynchronously yet. First
measure completion/assembly and CPU per part on the scored arm.

**Why:** measured part split after the hash fix is ~141 ms: body ~70–95 ms (input wait ~21–26 ms,
MD5 ~14 ms, buffered sink ~23–30 ms), data fsync ~16–32 ms, admission ~5.7 ms, publication ~7.2 ms.
Removing both commits is ~9 % and caps the result near 0.76×. The two commits are not the main gap.
`complete_multipart` assembles by re-reading and re-hashing every part inside the completion ACK;
the existing `cairn_blob_multipart_stage_seconds` labels can measure assembly and durability, and
that is unmeasured.

**Conditions if the single post-stage publication is built later:**

- One durable publication after staging, still `synchronous=FULL`; never add it to the Writer's
  relaxed allow-list. A part has no per-object metadata file, so a relaxed lost WAL tail would be
  unrecoverable.
- Keep a declared-byte staging admission; the blob I/O pool is a permit count, not a byte bound,
  and a declared part can be as large as `CAIRN_MAX_OBJECT_SIZE`.
- Revalidate session, quota, cardinality, path ownership and supersession in the savepoint.
- Reject the async part-row fold: ListParts/Complete would miss acknowledged parts and there is no
  rebuild proof.

**Acceptance if pursued:** `multipart_put_5m` ≥0.95× over three interleaved pairs, at both 4×4 and
64-in-flight; no regression on the other scoreboard cases. If assembly/CPU shows the host is
CPU-bound, stop and document the barrier asymmetry instead.

## 4. DELETE benchmark — not a ratio

**Decision:** treat the previous `>10×` DELETE row as invalid and do not touch the deletion path.

**Why:** it compared a 20,000-object Cairn run with RustFS's 6,400-object minimum, with different
measured windows. Warp requires `objects ≥ batch*concurrent*4` without `--list-existing`, and it
needs at least six one-second buckets; Cairn deletes the minimum pool before it can score. The
single-key and bulk-delete shapes also disagree because Cairn amortizes one Writer transaction per
request while RustFS appears to sync per key.

**Repeatable method:** fresh strict store, prefill the same 100,000 × 4 KiB pool for both engines,
then run `warp delete --list-existing --objects 100000 --batch 100 --concurrent 16 --duration 12s
--noclear`; require an exact prefill, ≥7 scored seconds, and no pool exhaustion. Report the
single-key Python comparison separately. Until that run exists, DELETE is a diagnostic observation,
not a score.

## Order

1. Replicated multipart layout (this change set).
2. Multipart file-authority phase 1, with a restore drill and protocol gate.
3. Measure multipart completion/assembly and CPU per part.
4. Decide on the one-commit part publication and/or body/sink work from that measurement.
5. Implement the paired DELETE methodology and re-run the scoreboard gaps.

The replicated-layout fix and the fail-closed part-number guard are the only code changes accepted
now; the rest are explicitly gated.
