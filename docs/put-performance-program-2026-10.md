# PUT performance program — 2026-10-29

Goal: close the strict-harness PUT gap (Cairn 0.688x RustFS at 1 MiB, c16) without relaxing
durability, SigV4 verification, or the integrity-baseline guarantee. Keep LIST (3.76x) and GET
(2.13x) at or above their current values.

## Baseline (main f8ec953, fr38, strict harness, 3 pairs)

| Workload | Cairn / RustFS |
|---|---:|
| PUT 1 MiB | 0.688x |
| GET 1 MiB | 2.134x |
| LIST 1,000 keys | 3.759x |

## Measured budget at c16 (PUT 1 MiB, ~30.6 ms/op)

| Component | Cost | Verdict |
|---|---:|---|
| body (client delivery + inline hash/write) | 24.7 ms | client-paced; hash removal only moved time into input wait |
| storage admission (SQLite commit) | 1.4–2.0 ms | removable (re-verified at publication) |
| SQLite publication wait | 2.2–3.2 ms | removable via overlay + ordered fold |
| finalize + rename + directory sync | 1.1–1.9 ms | partly removable (overlap sidecar) |
| namespace / staging create | 0.2–0.5 ms | keep |

Measured negatives: `CAIRN_META_SYNCHRONOUS=normal` is slower; removing SHA-256 entirely gains only
10–20% (BLAKE3 at 4.5 GB/s does not help because the digest is hidden behind client delivery);
a naive batched hash pipeline regressed (input wait grew).

## Workstreams

### A — P1 completion, then P2 (owner: agent A, worktree `/tmp/opencode/wt-meta-p1`)

**A1. P1 green (draft PR #144).**
- Fix every remaining CI failure on `perf/object-metadata-p1` (conformance scripts and any
  physical-path counts that still assume one file per object).
- Do not merge. Record P1-alone PUT cost (expected ~+1 ms regression).

**A2. No admission + sidecar overlap (branch `perf/p2-admission`, stacked on A1).**
- Ordinary PUT/Copy plan paths in memory; publication re-verifies the storage generation, bucket
  incarnation, absent intent, and that every planned path is unreferenced.
- Failed/cancelled writes reclaimed by the existing recovery scan.
- Stage the sidecar alias before the data rename; fsync data and sidecar concurrently; one
  directory barrier; a sidecar rename failure aborts before ACK.
- Multipart keeps its admitted path.

**A3. Overlay + ordered fold + startup rebuild (branch `perf/p2-authority`, stacked on A2).**
- ACK after the file barrier; insert the exact row into a bounded in-memory overlay; ordered
  background fold into SQLite; fold failure retains the entry and retries.
- Per-key striped serialization for conditional PUT/quota/version order; bucket fence for
  delete/recreate; quota overlay deltas.
- Startup: scan metadata files, validate data/meta identity, insert missing rows and counters
  before readiness.
- Shutdown drains the overlay.
- Tests: two-writer conditional race, quota under concurrency, bucket delete vs in-flight PUT,
  version ordering, overlay bound/drain, crash with lagging index then rebuild.

### B — Read path (owner: agent B, worktree `/tmp/opencode/wt-read`, branch from `origin/main`)

**B1. Socket/read-buffer experiment (measure first).**
- Set an explicit large `SO_RCVBUF` (and `TCP_QUICKACK` if it helps) on accepted data-plane sockets;
  test 256 KiB and 1 MiB hyper read buffers.
- Measure PUT 1 MiB c16 against the current main binary; keep only a measured win.

**B2. Reader/stager decoupling prototype.**
- The read task must only read into pre-allocated buffers and hand them off (bounded ring); a
  stager task writes and a hashing worker digests. No await on hash/write between socket reads.
- Bound in-flight bytes; propagate errors; preserve chunk order and `StagedWrite` results.
- Measure against main; deliver the smallest patch that wins, with numbers.

### C — Integration and acceptance (owner: lead)

1. Rebase B's patch onto A's branch (or extract the hunks if `write_staged` diverged).
2. Full focused gate, then full workspace CI (all features, musl, conformance).
3. Strict harness on fr38, 3 pairs per case, same host/session:
   - PUT 1 MiB must improve and reach >= 0.95x (target > 1.0x).
   - LIST must stay >= 3.0x.
   - GET must not regress below 2.0x.
4. Merge only green CI + accepted benchmark. Otherwise keep the best subset (A2 and/or B1 are
   independently mergeable) and record why.

## Benchmark protocol (shared host)

- Host: `ssh -p 2222 mj@203.153.55.38`, workspace `/srv/project-testing`, rustup 1.97.1 at
  `~/.cargo`, `CC=clang CXX=clang++`.
- Serialize every benchmark with `flock /srv/project-testing/bench.lock` and check that no `cairn`,
  `rustfs`, or `warp` process is running first.
- Disk preflight before any build/test (`df` >= 10 GB free). Release build on fr38 only.
- Harness: `conformance/rustfs_compare.py` with the `/srv/project-testing/cairn-performance-` root
  guard; `--pairs 3 --duration 12 --metrics-drain-seconds 5`.
- Record every result in the PR description; never compare numbers across sessions without a
  same-session control arm.

## Risks

- P2's overlay is the largest correctness risk (conditional/quota/version ordering); every rule
  gets a regression test and the crash matrix is re-run.
- The read path may not move the client floor; B is allowed to conclude "no win" and deliver a
  report instead of a patch.
- P1 alone regresses PUT; it must not merge before A2/A3 unless the benchmark shows no regression.
