# Performance follow-up

Five sequential PRs address the September 2026 capacity campaign findings. Historical measurements remain in the external Cairn-performance-report.md; they are not post-fix benchmarks. Focused local tests and normal CI gate each phase. The 500 GB campaign and long local regression runs are not repeated.

| Phase | Change | Status |
| --- | --- | --- |
| 1 | TCP_NODELAY on both accepted listeners | Implemented; validation pending |
| 2 | Reap completed connection tasks | Pending |
| 3 | Bounded bucket rendering, coalesced refresh, maintained visible counts | Pending |
| 4 | Bounded write-stage diagnostics | Pending |
| 5 | Reuse multipart buffers and measure assembly stages | Pending |

## Phase 1

The shared accept path configures the socket before either listener enters plaintext/TLS/fast-I/O handling. Setup failure drops the socket and logs the error. Regression coverage asserts the socket option and byte-exact repeated exchanges; optional fast-I/O TLS coverage uses the same accept helper. Timing is advisory rather than a CI threshold. The historical matched comparison removed a recurring approximately 40 ms post-header delay; it is not a new measurement of this commit.

## Remaining limitations

The recorded write stall has no uniquely established cause; checkpoint busy-wait prevention already exists. Multipart's approximately 2× payload writes include durable part storage and final assembly. General memory leakage and a CPU leak were not established. These phases do not promise 10,000 successful requests per second or eliminate the disk-bandwidth ceiling.
