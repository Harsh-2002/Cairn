# Root-sync coalescing candidate: adoption campaign — 2026-09-23

Decision: **reject and revert the candidate**. It did not improve strict 1-MiB PUT in the
five-pair screen (one paired win, 3.4% lower median), and the protected 1-MiB GET median was 12.0%
lower. No production Rust change from this experiment remains in the worktree. This is an
end-to-end result for this host and workload, not a proof that root-directory sync work is free or
that the candidate caused every observed difference. The host showed substantial I/O/CPU drift.

The unchanged control was archived from commit `f6fde5a68c6a5d5acee54f950ce2be8cda547312`;
its release binary SHA-256 was
`87d7aa5c7e6779188ffd20a9e1952e3263dbf5cc330ca7b11e18815f8945290e`.
The candidate was that commit plus the tracked Rust patch SHA-256
`d39564fc4ee4bef4b7a80e0bd59a4177be79cf18882200249a2e2d3ce85f4ebe`;
its release binary SHA-256 was
`5e0a183a460fe944fbe2384381ee09356ee01925c314b869fe995a12b668127e`.
Both used the pinned Rust 1.97.1 Docker toolchain, default features, optimized release profile
with fat LTO, and the same built web bundle. The control source archive lacks `.git`, so its
reported dev version omits the git suffix; the source revision above, rather than that version
string, identifies it. Warp v1.8.0 (`13c3b89`) SHA-256 was
`d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.

The candidate changed only flat-path admission: open/lock the sibling `.staging` and bucket
directories, then await the existing directory-sync coalescer on their common root before
returning any path that can create object bytes. Nested multipart admissions kept their existing
parent-before-child syncs. The intended saving was fewer root `fsync` calls at concurrency 16;
there was no relaxation of metadata FULL, file sync, rename, final-directory sync or admission.
Blob unit/integration tests, protocol-core tests, full workspace default-feature Clippy, the
single-seam crash harness, and all 32 multipoint crash checks passed on the candidate before
benchmarking. These checks cover their injected failure models, not physical power loss.

## Matched protocol and results

The Python [`cairn_put_ab.py`](../conformance/cairn_put_ab.py) runner created a fresh store per
arm, ran only one server at a time, alternated order by pair, used loopback path-style SigV4 and
Warp's fixed workload controls, and required zero errors. Both Cairn variants used SQLite
`synchronous=FULL`, one metadata shard, and zero Writer linger; 1-MiB PUT used 16 clients.
Scored durations were 12 seconds per arm; the PUT run also allowed 17 seconds per-arm metrics
drain. Protected mixed used 45/30/15/10 GET/STAT/PUT/DELETE weights with 100 prepared objects;
GET used 64 prepared 1-MiB objects and LIST used 1,000 prepared 4-KiB objects. All 28 arms passed
the harness's zero-error and resource-limit checks. Reported rates are Warp's analyzed-window
averages, not preparation throughput. The 4-KiB LIST rate is objects/s; others are MiB/s.

| Workload | Pairs | Control median | Candidate median | Candidate/control | Candidate paired wins | Measured invocation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Strict PUT, 1 MiB/c16 | 5 | 63.83 | 61.69 | 0.966× | 1/5 | 344.76 s |
| Mixed, 1 MiB/c16 | 3 | 283.84 | 216.81 | 0.764× | 1/3 | 114.93 s |
| Warm GET, 1 MiB/c16 | 3 | 1,961.72 | 1,727.14 | 0.880× | 1/3 | 99.13 s |
| LIST, 1,000 objects/c16 | 3 | 64,687.67 | 82,087.95 | 1.269× | 2/3 | 152.02 s |

The full paired sequence is retained so a favorable arm cannot hide a reversal:

| Workload | Pair 1 control → candidate | Pair 2 control → candidate | Pair 3 control → candidate | Pair 4 control → candidate | Pair 5 control → candidate |
| --- | --- | --- | --- | --- | --- |
| PUT MiB/s | 63.83 → 61.69 | 59.72 → 45.66 | 50.86 → 94.78 | 79.12 → 49.65 | 95.91 → 67.95 |
| Mixed MiB/s | 283.84 → 216.81 | 298.58 → 146.99 | 149.47 → 267.30 | — | — |
| GET MiB/s | 1,366.59 → 1,431.85 | 1,961.72 → 1,727.14 | 2,076.08 → 1,966.75 | — | — |
| LIST objects/s | 87,046.47 → 82,087.95 | 64,687.67 → 83,588.43 | 55,840.50 → 64,791.89 | — | — |

PUT paired candidate/control ratios were 0.966, 0.765, 1.864, 0.628 and 0.708; their median
was 0.765. This strongly fails the preregistered ≥20% median uplift and consistent-pair adoption
gate. The opposite pair-3 outlier and 50.86–95.91 MiB/s control range make a precise causal
slowdown estimate unsound. Candidate PUT p50 median was 264.8 ms versus 291.2 ms control, while
its throughput median was lower; that mismatch reinforces that a p50 alone cannot explain
end-to-end capacity. Warm GET p50 medians were 8.0 versus 7.3 ms. Median process peak RSS in PUT
was 37.0 MiB candidate versus 38.3 MiB control; mixed was 61.3 versus 61.0 MiB. These small
RSS differences do not establish a memory-footprint gain. The largest observed task-root peak
allocation was 1,433,260,032 bytes, well below the 10,000,000,000-byte cap.

One unrelated VM used much of a CPU during build preparation and exited before the measured run;
unrelated Go compilation overlapped the first PUT pair. Host I/O pressure and throughput then
varied materially across later pairs. Alternating order limits simple time-trend bias but does
not eliminate this noise. No RustFS reference was rerun for a rejected Cairn candidate: the
[recent verified-strict comparison](performance-strict-followup-2026-09.md) remains the reference,
not a claim about this candidate's RustFS parity.

## Ledger, verification boundary, and cleanup

The four bounded invocations consumed **710.84 of the separately approved 3,600 measured
seconds**, leaving **2,889.16 seconds** in this campaign. The original campaign's approximately
22-second balance was not charged or reset. Compilation, tool download and test setup preceded
the measured ledger; each harness invocation included preparation, scored load, teardown and
any requested metrics drain. The Python benchmark/probe unit suite passed all 16 tests. The
candidate code was reverted via patch after the failed gate, and the tracked Rust diff is empty.
No full-workspace test/Clippy-all-features/audit gate is claimed for an unadopted change.

Task-owned benchmark stores, copied binaries, Cargo build/cache, and temporary web build
products are removed after this report is recorded. Raw per-arm Warp logs and scratch JSON were
discarded with that scratch; the table above, hashes, protocol, resource maxima and all per-pair
scores are the retained evidence. No benchmark server or client was left running.

Next work should separate foreground root/staging/final-directory fences from cleanup I/O in a
low-overhead scored-window trace, then test a different contract-preserving candidate on a
quieter or dedicated device. A favorable syscall count without paired throughput and protected
workload wins is not sufficient for adoption.
