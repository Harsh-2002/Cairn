# Performance follow-up

Five sequential PRs address the September 2026 capacity campaign findings. Historical measurements remain in the external Cairn-performance-report.md; they are not post-fix benchmarks. Focused local tests and normal CI gate each phase. The 500 GB campaign and long local regression runs are not repeated.

| Phase | Change | Status |
| --- | --- | --- |
| 1 | TCP_NODELAY on both accepted listeners | [PR #76](https://github.com/Harsh-2002/Cairn/pull/76) merged after green CI; 16 focused server tests passed |
| 2 | Reap completed connection tasks | [PR #77](https://github.com/Harsh-2002/Cairn/pull/77) merged after green CI |
| 3 | Bounded bucket rendering, coalesced refresh, maintained visible counts | [PR #78](https://github.com/Harsh-2002/Cairn/pull/78) merged after green CI |
| 4 | Bounded write-stage diagnostics | [PR #79](https://github.com/Harsh-2002/Cairn/pull/79) merged after green CI |
| 5 | Reuse multipart buffers and measure assembly stages | [PR #80](https://github.com/Harsh-2002/Cairn/pull/80); implementation and focused validation complete; CI-gated delivery |

## Phase 1

The shared accept path configures the socket before either listener enters plaintext/TLS/fast-I/O handling. Setup failure drops the socket and logs the error. Regression coverage asserts the socket option and byte-exact repeated exchanges; optional fast-I/O TLS coverage uses the same accept helper. Timing is advisory rather than a CI threshold. The historical matched comparison removed a recurring approximately 40 ms post-header delay; it is not a new measurement of this commit.

## Phase 2

The live listener selector reaps tasks ahead of accepting more sockets, remains pending with an empty
task set, and gives shutdown priority. Completed, cancelled, and failed task counts carry into the
final shutdown report. Tests cover idle connection waves, released permits, panic/cancellation,
new accepts, pending shutdown and sender closure, and forced-drain accounting. The initial 18
server tests passed; after adding pending-shutdown coverage, all seven focused shutdown tests passed.
No new RSS comparison was run: removal of completed task retention is structurally tested, while
allocator/cache retention and other unexplained memory growth remain separate questions.

## Phase 3

Bucket pages render at most 50 rows, preserve explicit selections across pages, and clamp after
deletions. Refreshes allow one active load plus one queued refresh per dependency generation;
old generations cannot publish or schedule work after disposal. The browser regression runs in CI
with 2,000 mocked buckets at desktop/mobile widths and creates no Cairn data. Startup failures and
forced browser termination also clean up test profiles/listeners.

Schema v34 backfills exact current-visible counters once. Both SQL backends update visibility
inside writer savepoints; overview reads use per-bucket rollups while byte/version meanings stay
unchanged. Focused local validation passed: four counter tests, populated migration/idempotence,
nine sharding tests, console lint/build/audits and browser tests. CI exposed legacy migration
fixtures missing pre-existing rollup tables; those fixtures were corrected in both SQL backends,
and all 23 default-backend migration tests then passed locally. Async-backend parity tests are
included for CI. The bounded 100,000-row query comparison and its limitations are in
[benchmarks.md](benchmarks.md#maintained-visible-counts-bounded-query-comparison-2026-09-08).
Client-side pagination bounds rendering; existing bucket-list API payloads still contain all buckets.

## Phase 4

Six independent 1,024-sample rings distinguish writer admission, queue residence, BEGIN, apply,
COMMIT and checkpoint execution. Fixed labels and dropped-sample accounting bound diagnostics;
rare checkpoint samples cannot be evicted by frequent queue samples. Slow-stage warnings are limited to one per stage per writer per second, preserving queue-stall
evidence even if its sample is evicted while avoiding a per-request log flood. Metrics use the existing
server collection task; COMMIT wall time is not labelled as isolated fsync time.

Reserving queue capacity before incrementing depth also fixes phantom queued mutations when an
admission future is cancelled. Focused tests cover cancellation, buffer bounds/rare-stage retention,
expected mutation rejection, and checkpoint busy-wait prevention. The opt-in real-writer diagnostic
completed 2,048 mutations and attributed a controlled 50 ms blockage to queue wait (maximum
50.596 ms), with maximum COMMIT time 0.004 ms in that in-memory leg. See
[metadata.md](metadata.md#bounded-writer-diagnostic) for method and limitations. The existing dashboard
is regenerated from its source. This closes attribution gaps without claiming the historical
five-second stall was reproduced or fixed; ordinary blob-path profiling remains follow-up work.

## Phase 5

Assembly lazily allocates one 64-KiB plaintext buffer and reuses it across parts. Fully encrypted
part sets retain their existing bounded decoder path. Fixed-label timings separate blob permit
wait, assembly and durability; the existing metrics task drains at most 1,024 recent samples with
explicit eviction accounting. Samples include interrupted/error stages and exclude metadata commit.

Validation passed: 43 blob unit tests, 36 blob integration tests, focused Clippy and formatting.
Integration compilation and 19 server tests passed; the mixed-part checksum/range regression also
passed after switching its test keys to fresh generated values. Cases include mixed plaintext and
encrypted parts, short final reads, compressed/encrypted output, missing/tampered parts, cancellation,
size ceilings and cleanup. The bounded A/B/A comparison's mean times were 109.37 / 128.61 / 118.10 ms:
**no speedup was demonstrated**. One reused allocation replaces 256 explicit allocations for that
fixture, but durable part storage plus final assembly still requires approximately two payload
writes. Full measurements and limitations are in
[benchmarks.md](benchmarks.md#multipart-buffer-reuse-bounded-comparison-2026-09-08).

## Handoff and cleanup

The large capacity campaign was not restarted. The bounded diagnostics created no persistent
metadata corpus; multipart fixtures, temporary benchmark code and browser profiles were removed.
The focused browser harness also verifies cleanup on startup failure and forced browser termination.
Temporary worktrees, merged phase branches and local build outputs are removed at final handoff,
after CI passes; committed implementation, regression tests, documentation and the generated
dashboard are retained. Shared installed tool caches are preserved. The intended final branch set
is `main` and `website`.

## Remaining limitations

The recorded write stall has no uniquely established cause; checkpoint busy-wait prevention already exists. Multipart's approximately 2× payload writes include durable part storage and final assembly. General memory leakage and a CPU leak were not established. These phases do not promise 10,000 successful requests per second or eliminate the disk-bandwidth ceiling.
