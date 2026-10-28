# Would a custom file-based metadata engine close the 1-MiB PUT gap?

**Answer on the measured evidence: no.** Not because SQLite is fast, but because ~99% of the
time labelled "SQLite" in a PUT is a shared durability barrier that a replacement engine must
also pay, and because the one commit that *is* removable was already removed twice and measured
with no throughput gain.

Analysis date 2026-10-28. Read-only: no code changed, no load run. Stage figures are the c16
1-MiB numbers supplied by the coordinating session, cross-checked against the archived JSON in
`docs/performance-evidence/`. The 0.626x (11 pairs) and 0.670x (5 pairs) ratios are likewise
supplied and are not yet committed in this checkout; the archived values they must be read
against are 0.817 (5 clean pairs, Sept) and the October flush screen.

---

## 1. What has actually been run

Every prior attempt to change the metadata engine or the storage layout, and what it returned.
"Gain" means an end-to-end PUT throughput gain that survived a paired gate.

| # | Experiment | What ran | Outcome | Throughput gain? |
|---|---|---|---|---|
| 1 | Flat vs directory fanout (`storage-fanout-2026-09`) | 30 arms, 20,480 files x 4 KiB, real `LocalBlobStore` reads/deletes, raw publisher with file+rename+dir sync | All arms correct; flat control drift 15-181% > 20% gate → **INCONCLUSIVE, KEEP flat** | No. Hot-bucket primary 2.71x *worse*, publish ratio 1.02 |
| 2 | Small-record packing (`storage-packing-measurement-2026-09`) | 11 of 90 planned arms, 1,024 objects, 5 pairs/case | **INCONCLUSIVE, KEEP files.** 1 KiB 3.329→0.963 s, 4 KiB 3.351→0.921 s, 16 KiB 3.570→1.366 s, 64 KiB 3.950→3.248 s, 256 KiB 5.753→10.761 s; the **1-MiB packed arm hit its 120 s deadline** (files control 12.750 s) | No. Largest sizes regress; the only arm relevant to a 1-MiB PUT never finished |
| 3 | Protocol-2 lifecycle accounting (`storage-journal-2026-09`) | 5 pairs 4 KiB c4 + 1 pair 1 MiB; real S3 | 4 KiB PUT p50 3.857→6.745 ms (**+75.9%**), DELETE +45.8%, throughput 496.2→311.5/s (**-36.9%**). 1 MiB screen 27.951→31.364 ms (+12.2%), 91.90→81.38/s (-11.4%) | No. Direct measurement that *adding* durable metadata work costs PUT latency |
| 4 | SQLite capacity (`storage-metadata-capacity-2026-09`) | 100,000 real rows through the production Writer, C4/32/128, hot + 16-bucket | **PASS as a lab result.** Hot: 157.80 / 266.13 / 293.12 / 265.55 / 155.25 bundles/s; 95.22% serialized occupancy, 81.87% non-empty queue, only **+10.26%** C32→C128. 16-bucket 79.77% queue misses its 80% gate | n/a - metadata-only, no S3 request |
| 5 | Fjall 3.1.10 replacement (`storage-metadata-alternative-2026-09`) | Same 100k-row seed prepared **separately in each engine**, then 10 paired load arms | **0 of 10 arms, 0 of 5 pairs.** SQLite prepared in 95.34 s inside its 100 s deadline; **Fjall hit the 105 s deadline** and returned `deadline reached before admission` | No. The only completed engine datapoint is a *preparation* loss |
| 6 | libSQL / Turso (`cairn-meta-async`) | Adapters exist behind `CAIRN_META_BACKEND`; capability review only | **Never benchmarked.** Upstream single-writer limit retained; Turso pragmas applied best-effort (`apply_turso_pragmas` ignores errors) | No evidence either way |
| 7 | redb / RocksDB (`storage-metadata-capabilities-2026-09`) | Paper comparison only | Research only; no adapter, no on-disk migration, no measurement | None |
| 8 | Sync-call probe (`performance-sync-probe-2026-09`) | glibc `fsync`/`fdatasync` interposer, one Cairn/RustFS pair | Cairn **7.39 calls/op, 29.7 CPU-ms/op, 44.8 MiB/s**; RustFS **42.88 calls/op, 56.8 CPU-ms/op, 67.3 MiB/s** | Diagnostic only |
| 9 | Audit co-commit (`performance-audit-cocommit-adoption-2026-09`) | Implemented, tested, 5 PUT pairs + protected mixed/GET/LIST | PUT 0.915x (2/5 wins), LIST **0.671x**; **rejected and reverted** | **No - measured loss** |
| 10 | Cleanup deferral (`performance-cleanup-interference-screen-2026-09`) | 3 pairs, non-conformant diagnostic | Median 1.137x, but control fell 32.6% first→last; **not adoptable** | No |
| 11 | October flush screen (`performance-put-flush-screen-2026-10`) | 10 pairs, branch vs main, sync composition | Total intercepted calls/fdatasync **-30.3%**, staging calls **-85.1%**; throughput GM 1.139, **t95 0.794-1.635**, 5/10 wins | **Inconclusive; no established change** |
| 12 | Draft stack on clean binary | 5 pairs, audit co-commit + cleanup | **0.670x clean** | No - reported as a loss |

**No experiment in this repository has ever shown a metadata-engine or storage-format change
producing an end-to-end 1-MiB PUT throughput gain.** The single completed engine comparison is a
preparation-stage loss. Two storage-format attempts (fanout, packing) and three durable-work
removals (Fjall, audit co-commit, cleanup deferral) all ended in `KEEP`.

The Fjall preparation numbers are worth keeping in view, because they invert the usual intuition:

| Engine | Wall to prepare 100k rows | CPU-s | Bytes written (`write_bytes`) | Peak RSS |
|---|---:|---:|---:|---:|
| SQLite | **95.34 s** (deadline 100 s) | 66.71 | 6,848,069,632 | 29,856 KiB |
| Fjall 3.1.10 | **105.44 s** (deadline, incomplete) | 102.88 | 163,577,856 | 103,652 KiB |

SQLite wrote **41.9x more bytes**, used **35% less CPU**, and still finished first. Write volume is
not the limiting resource; per-commit sync latency and batching are.

---

## 2. Where the 346 ms actually goes

Exclusive per-request stages, c16, 1 MiB, `synchronous=FULL`, one shard, zero linger, p50.

| Stage | ms | % of 346 | What it is |
|---|---:|---:|---|
| `storage_admission` | 73.7 | 21.3% | Durable `Mutation::Storage{Reserve}` commit (protocol 2) |
| `blob_stage` | 128.7 | 37.2% | Filesystem: namespace **21.6** (incl. parent-dir fsync), body+hash **25.3**, finalize **34.8** (`sync_data` + rename), directory sync **38.0**, staging/permit **9.0** |
| `publication` | 50.4 | 14.6% | Durable `PublishStorageWrite` commit (ARCH 8 linearization point) |
| `audit` | 67.1 | 19.4% | Separate `RecordActivity` commit after the object is already durable |
| residual | 26.1 | 7.5% | HTTP/TLS, SigV4 verify, response, scheduling |
| **total** | **346.0** | **100%** | |

Arithmetic checks against the archived instrumented arms, where the four stages reconcile to the
inclusive total: 24.7+84.5+20.0+25.9 = 155.1 vs total 155.19; 59.2+124.4+39.4+69.2 = 292.1 vs
320.4; 77.4+146.1+54.3+72.6 = 350.3 vs 378.1. The supplied October numbers have the same shape
(residual 26.1 ms).

**Durable-metadata share: 73.7 + 50.4 + 67.1 = 191.2 ms = 55.3% of the request.**
**Filesystem-blob share: 128.7 ms = 37.2%.** Together, **92.5% of a 1-MiB PUT is spent waiting
for some durability barrier**; only 26.1 ms is protocol work.

### The metadata share is not SQLite computation

| Component | Value | Share of one 30.1 ms commit |
|---|---:|---:|
| `COMMIT` p50 (one durability barrier) | 30.1 ms | 100% |
| Median SQL `apply` per batch, PUT-load arm | 0.178 ms | **0.6%** |
| Median SQL `apply` per batch, 100k-row capacity lab | 1.258 ms | 4.2% |
| Measured `fdatasync` mean, same host, adjacent diagnostic arm | 36.5 ms | ~1:1 with commit |

Three waits on a 30.1 ms commit cycle are 90.3 ms (26.1% of the request) of barrier component -
an upper bound, because one physical commit serves several requests; three queue waits at 11.3 ms
= 33.9 ms (9.8%); the remaining 67.0 ms of the three stages is batch-slot wait, ack and
scheduling. **Remove 100% of SQLite's query work and you recover ~0.5 ms of 346 ms (0.15%);
even using the 100k-row apply cost, ~3.8 ms (1.1%).** The engine's *computation* is not the
cost. The barrier is.

The three stages are also not three private fsyncs. Group commit (`writer.rs:357-374`, 256
mutations/batch, linger 0) shares one `COMMIT` across concurrent requests: in the measured arm
1,573 commits carried a mean 6.47 mutations each, so physical commits per PUT are **around one or
fewer**, not three. Each stage wait is "time until my batch's shared 30 ms barrier completes",
not "my own fsync".

---

## 3. Arithmetic: removing one or two SQLite commits

Closed-loop latency arithmetic, `throughput = 16 / latency` at c16. At 346 ms this gives
46.24 MiB/s, within 3% of the 44.8 MiB/s measured on the same pair. **This is arithmetic, not a
forecast** - see the caveat below.

| Scenario | Latency | MiB/s | Speedup | vs RustFS (0.626x base) |
|---|---:|---:|---:|---:|
| Baseline | 346.0 ms | 46.24 | 1.000x | 0.626x |
| Audit commit removed (co-commit) | 278.9 ms | 57.37 | **1.241x** (+24.1%) | 0.777x |
| Audit + publication removed | 228.5 ms | 70.02 | **1.514x** (+51.4%) | 0.948x |
| All three metadata commits removed (physically invalid) | 154.8 ms | 103.36 | 2.235x | 1.399x |

To close a 0.626x gap you need **1.597x**, i.e. latency <= **216.6 ms**, i.e. **129.4 ms must
disappear**. The only removable commit, audit, is **67.1 ms**. Even granting a free *publication*
commit as well - which ARCH 8 forbids, because that commit is the linearization point - the total
is **117.5 ms**, still short of the 129.4 ms required. **Even with publication and audit entirely
free, Cairn lands at 0.948x of RustFS.** That is the headline: the current gap cannot be closed
inside the metadata layer.

Only the physically impossible row clears the bar, and it clears it by assuming a durable
metadata store with zero commit cost.

**Sensitivity.** Against the weaker archived 5-pair clean gap (0.817x), the single removable
commit's ceiling is 0.817 x 1.241 = **1.014x** - parity at best, never a lead. The direction of
the conclusion survives both gap estimates; only its margin moves.

**Why the arithmetic over-predicts.** (a) The removed stages are not private work: they are waits
on a shared 30 ms group-commit cycle, so deleting a submission also removes batching depth, and
the surviving commits get smaller rather than fewer per unit time. (b) The Writer is a shared
serialized service at ~95% occupancy, so per-request latency reduction does not translate 1:1
into requests/s. (c) It was tested. The arithmetic promises +24% for deleting the audit commit;
the measured results were **0.915x** (Sept, 5 pairs, protected LIST 0.671x) and **0.670x clean**
(Oct draft stack, 5 pairs), with the independent October flush screen spanning **0.79-1.63**.
Reality delivered a loss, not +24%.

---

## 4. Code path: does a file engine remove the waits, or move them?

Four durability points sit on a plain PUT, in this order.

**(a) Admission - `crates/cairn-protocol/src/service.rs:965-981`.** `admit_object_storage`
submits `Mutation::Storage{Reserve}` (`service.rs:190-216`). This is the protocol-2 rule: durable
Writer admission *before any new physical write* (`storage_write.rs:88-113`). It is a durable
commit by construction. **No engine removes it; a different engine only changes what it costs.**

**(b) Blob - `crates/cairn-blob/src/lib.rs:1042-1147`.** `AdmittedPaths::open_sampled` (namespace
incl. parent-directory fsync), `Staging::create`, `write_staged` (body + MD5/SHA-256), then
`sink.commit_sampled` -> `sync_staged` (`staging.rs:18-26`, i.e. `file.sync_data()` = fdatasync) ->
rename, then `dir_sync.sync_file(paths.final_file.parent, ...)` at `lib.rs:1127`. The directory
fsync is already coalesced per (device, inode) across concurrent PUTs (`commit.rs:1-15,74-80`),
so it is one shared fsync per batch, not one per request. **None of this is metadata; a metadata
engine touches none of it.**

**(c) Publication - `service.rs:1139-1163` -> `storage_write.rs:148-166`.** One
`Mutation::PublishStorageWrite` carrying the row, preconditions, tags, Object Lock intent,
replication outbox and cleanup debt, committed after the blob is durable. This is the ARCH 8
linearization point. **Not removable.**

**(d) Audit - `service.rs:4954-4969` helper; call site removed by `f553779` (it sat immediately
after the notification in the success arm).** `self.audit("PutObject", ...)` submitted a third
`RecordActivity` commit and awaited it before building the response. This is the **only**
removable commit, and the current branch already folds it into (c) at `service.rs:1152-1161`.

**Writer mechanics - `cairn-meta/src/writer.rs:562-566`.** One `COMMIT` per batch is one
durability barrier covering every mutation in the batch; `PRAGMA synchronous=FULL` is set in
`cairn-meta/src/lib.rs:133-138`; `MAX_BATCH = 256` (`writer.rs:21`).

### Verdict on the "directory fsync might cover it" hypothesis

It cannot, for three independent reasons:

1. **Different inode.** A bucket-directory fsync makes that directory's *entries* durable. It
   says nothing about the contents of a metadata log file, which needs its own
   `fsync`/`fdatasync`. SQLite already pays exactly one such call per commit on its `-wal` file.
2. **Wrong order.** The root/bucket directory barriers happen at (b), *before* the metadata
   commit at (c). They cannot retroactively cover a file written afterwards.
3. **Different directory.** The metadata file lives under the metadata/data root, not the bucket
   directory; a directory fsync is per-inode, so even the entry durability does not transfer.

The one genuine saving available is amortising the metadata log's *directory* fsync by creating
or rotating segments rarely - and that is already true of SQLite's WAL. Segment rotation would
add directory fsyncs, not remove them.

**So a file-based engine moves the wait: 2-3 `fdatasync` calls land on a different inode, on the
same device, at the same ~30 ms.** It removes zero blob barriers, zero admission commits, zero
publication commits. The only commit it could delete is the audit, which is deletable today
without changing engines - and when that was done, throughput did not improve.

---

## 5. Strongest fact-based case FOR replacing SQLite

1. **Metadata is the largest measured block and the only demonstrably saturated service.**
   191.2 ms of 346 ms (55.3%), about 1.5x the entire blob stage and 2.5x its file-sync
   component; the capacity lab shows 95.22% serialized occupancy, 81.87% non-empty queue and only
   +10.26% from C32 to C128. More S3 concurrency does not buy Writer capacity. That is a real
   ceiling, measured on the production Writer.
2. **Cairn's durability rate is 8.7x below what the same host and device sustain.** 7.39 calls/op
   at 44.8 MiB/s = **331 calls/s**; RustFS 42.88 calls/op at 67.3 MiB/s = **2,886 calls/s**.
   RustFS issues *more* durability calls per object and still goes faster, so the device is not
   the limit - Cairn's serialization is. A metadata design that admits concurrent durable
   commits (per-bucket writers, MVCC) attacks the actual limiter. This is the only mechanism in
   the evidence that could plausibly close a 1.6x gap.
3. **If metadata time could be halved, that alone is ~96 ms -> 1.38x.** No other single block is
   large enough to matter.

Caveats that keep this honest: (2) is an argument about *concurrency*, not about file-vs-SQL, and
a per-bucket design does not help a single hot bucket - which is exactly the workload that was
measured. (3) is arithmetic on a block that is 99% barrier.

---

## 6. Strongest fact-based case AGAINST replacing SQLite

1. **The engine's computation is ~0.5 ms of a 346 ms request (0.15%).** `apply` p50 is 0.178 ms
   per batch against a 30.1 ms commit; the commit is indistinguishable from the host's measured
   `fdatasync` (35.1 ms commit p50 vs 36.5 ms fdatasync mean, adjacent arms, same host). There is
   nothing left for a faster engine to win: **99.4% of the "SQLite" time is the barrier.**
2. **The arithmetic ceiling does not reach the gap.** Free publication *and* audit = 0.948x.
   A replacement must therefore beat RustFS on barriers it does not control.
3. **The one removable commit was removed and measured, twice, with no gain**: 0.915x (Sept,
   5 pairs, protected LIST 0.671x) and 0.670x clean (Oct draft stack that also bundles cleanup
   changes, 5 pairs), against a flush-screen interval of 0.79-1.63. The hypothesis "fewer
   metadata commits -> more throughput" has already been falsified on this workload.
4. **The only engine actually built and run never completed a single load arm.** Fjall missed a
   105 s preparation deadline that SQLite met in 95.3 s, while writing 42x fewer bytes. The sole
   completed alternative-engine datapoint is negative.
5. **LIST is Cairn's one decisive win and it is a pure metadata read.** 96,083 vs 16,226 listed
   objects/s (5.92x) and 8.8 ms vs 61.4 ms p50 in the baseline matrix; 102,447 vs 15,117 (6.8x) on
   the shared-node run. It is an ordered range seek over the covering index
   `idx_ov_latest_cover` (`store.rs:872-897`, `schema.rs:785`) with `LIMIT`, not a table scan. A
   replacement puts a 6x win at risk to chase a PUT gain the arithmetic says it cannot deliver.
6. **Replacement is a correctness project, not a performance patch.** Savepoint-per-mutation
   semantics, group commit, shard routing (the 4(+1)-site rule), 40 append-only migrations,
   offline backup/restore (`VACUUM INTO` + manifest), secrets sealing, outbox fencing, quota
   counters and Object Lock all have to be rebuilt. The capability review found no qualifying
   manifest/backup primitive for Fjall or redb, and Cairn's native backup today refuses the
   non-SQLite backends it does have (libSQL, Turso).

---

## 7. What would change this answer

- A commit primitive that is materially cheaper than the device's `fdatasync`. Under FULL
  semantics with one durable barrier per commit, none exists.
- A **measured** PUT gain from reducing commits/PUT. Already tested twice: it lost.
- A LIST implementation that matches >=96k objects/s and ~8.8 ms p50 while the PUT path improves.
- A hot-bucket concurrency mechanism (not a file format) - the only lever the numbers actually
  point at, and the one nobody has measured yet.

---

## 8. Limits of this note

- p50 only. No p99: no arm reached 10,000 successful PUTs. Shared 4-vCPU KVM host, one virtual
  disk, uncontrolled page cache and host I/O pressure.
- One workload: 1 MiB, c16, FULL, single shard, zero linger; RustFS 1.0.4. 4-KiB and mixed
  workloads are not covered by these ratios.
- Stage timers are exclusive per request, but the Writer batch and the directory coalescer are
  shared; the commit/queue decomposition is indicative, not additive device work. Nested blob
  substages are not request-aligned in general.
- The 0.626x (11 pairs) and 0.670x (5 pairs) figures are from the coordinating session and are not
  yet committed here; archived corroboration is 0.817x (5 clean pairs, CI 0.703-0.930) and the
  October flush screen (GM 1.139, CI 0.794-1.635).
- No new load was run, and no code was modified, for this analysis.
