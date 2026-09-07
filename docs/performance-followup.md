# Performance follow-up

Five sequential PRs address the September 2026 capacity campaign findings. Historical measurements remain in the external Cairn-performance-report.md; they are not post-fix benchmarks. Focused local tests and normal CI gate each phase. The 500 GB campaign and long local regression runs are not repeated.

| Phase | Change | Status |
| --- | --- | --- |
| 1 | TCP_NODELAY on both accepted listeners | PR #76 merged after green CI; 16 focused server tests passed |
| 2 | Reap completed connection tasks | PR #77 merged after green CI |
| 3 | Bounded bucket rendering, coalesced refresh, maintained visible counts | PR #78 merged after green CI |
| 4 | Bounded write-stage diagnostics | Implemented; focused checks passed; CI gates merge |
| 5 | Reuse multipart buffers and measure assembly stages | Pending |

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
rare checkpoint samples cannot be evicted by frequent queue samples. Only slow transaction or
checkpoint stages produce warnings, avoiding a per-request log flood. Metrics use the existing
server collection task; COMMIT wall time is not labelled as isolated fsync time.

Reserving queue capacity before incrementing depth also fixes phantom queued mutations when an
admission future is cancelled. Focused tests cover cancellation, buffer bounds/rare-stage retention,
expected mutation rejection, and checkpoint busy-wait prevention. The opt-in real-writer diagnostic
completed 2,048 mutations and attributed a controlled 50 ms blockage to queue wait (maximum
50.596 ms), with maximum COMMIT time 0.004 ms in that in-memory leg. See
[metadata.md](metadata.md#bounded-writer-diagnostic) for method and limitations. The existing dashboard
is regenerated from its source. This closes attribution gaps without claiming the historical
five-second stall was reproduced or fixed; ordinary blob-path profiling remains follow-up work.

## Remaining limitations

The recorded write stall has no uniquely established cause; checkpoint busy-wait prevention already exists. Multipart's approximately 2× payload writes include durable part storage and final assembly. General memory leakage and a CPU leak were not established. These phases do not promise 10,000 successful requests per second or eliminate the disk-bandwidth ceiling.
