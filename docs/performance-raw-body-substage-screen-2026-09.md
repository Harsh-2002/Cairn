# Raw-body substage diagnostic — 2026-09-23

## Preregistered before load

Purpose: on the same shared node, determine whether a short strict-durability 1-MiB PUT arm's
sampled raw-body wall time is predominantly input wait, mandatory hash work, or buffered sink
wait. This is **not** an overhead screen, Cairn-versus-RustFS comparison, or architecture-adoption
trial. The preceding one-pair screen used a different six-stage binary and cannot be pooled with
this arm. The existing [strict controls](performance-put-1m-strict-2026-09.md) remain in force.

Use the current worktree's refined sampler, pinned Rust 1.97.1 release build and previously
SHA-verified Warp v1.8.0. One fresh Cairn store, FULL metadata durability, one bucket, path-style
SigV4, 1-MiB objects, concurrency 16, Warp PUT for five requested seconds. `cairn_put_ab.py`
single-candidate mode carries a **11.5-second client-load cutoff** and 7.5-GB task-root cap;
it records the actual client interval, zero-error status, sampled metric count/loss and host
I/O pressure plus a names-only competing-process CPU census. Any timeout/error, metric loss,
fewer than five substage samples, or obvious competing CPU pressure makes attribution
inconclusive. No throughput gain or p99 claim will be made from this arm regardless of score.

The separate adoption ledger starts this arm at **3,583.322/3,600 measured seconds** with
**16.678 seconds remaining**. Client load is charged by its actual elapsed interval even if the
arm fails; build, preparation and 17-second post-load metrics drain are tracked as wall time but
not client load. Stop before exceeding the remaining measured allowance. Keep total task-owned
scratch, including build and store, below 10 GB. Reap all client/server/container processes,
then remove only the exact task-owned scratch and generated web build artifacts. Retain compact
results in this document; no production optimization is selected by this diagnostic.

## Result

The pinned release binary SHA-256 was
`f2c09f93f27736684c92c8eecd3f67c1608ebf89f66fa107d196fe37e3be0ecf`, built from
HEAD `f6fde5a68c6a5d5acee54f950ce2be8cda547312` plus the uncommitted Rust timing changes
(four-file binary-source diff SHA-256
`ce8363eafcd554dd29834b1a2bd124402db6b94ac98934f3c5e1341b5982d1fe`). Warp's
executable SHA-256 was `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.
The build used the official `rust:1.97.1-bookworm` image digest
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`.

The strict settings were `CAIRN_META_SYNCHRONOUS=full`, one metadata shard, and zero Writer
linger. The fresh bucket was created before load. Warp exited 0; the runner reported PASS,
zero operation errors, no time/space limit hit, 63.29 MiB/s over only **two analyzed seconds**,
and an 8.811-second actual client interval. The 11.5-second cutoff was not reached. This is a
functional arm result, **not** a comparable throughput score.

| Sampled raw-object phase | Sum across 12 writes | Mean wall time |
| --- | ---: | ---: |
| Body input wait | 136.119 ms | 11.34 ms |
| Body hash update/finalize | 115.481 ms | 9.62 ms |
| Body buffered sink await | 40.168 ms | 3.35 ms |
| Aggregate body | 291.978 ms | 24.33 ms |
| Namespace preparation | 352.694 ms | 29.39 ms |
| File finalize | 239.293 ms | 19.94 ms |
| Destination-directory sync wait | 248.557 ms | 20.71 ms |

Every listed metric had 12 successful samples and the timing-eviction counter was zero. The
three body substages sum to within about 0.018 ms per sampled write of the aggregate body
measurement; that difference is instrumentation/loop overhead, not an unmeasured operation.
`body_hash` is **wall time**, not exclusive CPU; scheduler preemption can inflate it. The raw
sink awaits do not include the final buffered flush in `finalize`.

This arm was **attribution-inconclusive by the preregistered criterion**: the names-only CPU
census observed an unrelated `libkrun VM` consume about 0.95 CPU-seconds during measured load,
and other unrelated processes together contributed to 3.18 observed CPU-seconds. Processes
that started and exited between two-second censuses may be missed. Host I/O pressure also rose
by 1.828 seconds (`some`) and 1.093 seconds (`full`) over the 8.811-second interval. The
server used 7.21 CPU-seconds and peaked at 37.20 MB RSS; task-owned peak footprint was
1.538 GB. The previous longer candidate arm's namespace and directory-sync means were much
lower, so these values show substantial shared-host drift, not a stable causal bottleneck.
There is no defensible claim that hash work, network input, SQLite, or fsync alone is the
single dominant cause of the Cairn–RustFS gap.

The adoption ledger charges **8.811 seconds**, moving from 3,583.322 to
**3,592.133/3,600 measured seconds** and leaving **7.867 seconds**. No RustFS or protected
mixed/GET/LIST arm was run. A new separately metered campaign is required for paired
attribution, a safe optimization candidate, and the complete adoption matrix.
