# PUT audit-wait upper-bound diagnostic — 2026-09-23

Decision: **do not adopt the diagnostic build**. It deliberately omits the successful PUT's
post-publication activity record, so it violates production behavior. Eight of eight strict
1-MiB PUT pairs favored it, which makes the second Writer round trip a plausible optimization
target, not a measured gain for a safe co-commit. Shared-node CPU and I/O contention remained
uncontrolled; the same result must be reproduced with activity preserved and protected workloads
before claiming a production speedup.

The control was the pristine `f6fde5a68c6a5d5acee54f950ce2be8cda547312` archive, built
with the pinned Rust 1.97.1 Docker image (digest
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`), default
features, release profile and the same built web bundle. Its binary SHA-256 was
`9a10f9b6f435d0b057d085a2f1e3c5031e7838dc05da3d97a9ed86d68296138f`. The diagnostic
binary was identical except that `put_object` skipped its separate, awaited `self.audit` call
after successful publication. Its binary SHA-256 was
`ce5e355012aaea7ebf524bf94e9ed20040d79aa513629f1ee484f48f170109bc`; the patched
`service.rs` SHA-256 was
`76213a45fca8173ab46c190ca0fffaa2ed76302ff0f3df8a249542c84abb42cf`. The change
was made only in an isolated source archive, never in the tracked worktree. Warp v1.8.0 SHA-256
was `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.

Both variants used `CAIRN_META_SYNCHRONOUS=full`, one metadata shard and zero group-commit
linger. The Python [paired runner](../conformance/cairn_put_ab.py) used fresh stores, one server
at a time, alternating arm order, loopback SigV4, Warp PUT at 1 MiB/c16 and zero-error gating.
Each scored load requested 30 seconds and allowed a 17-second metrics drain. The first run had
three pairs; the second had five. Rates are Warp analyzed-window averages, not preparation
throughput.

| Pair | Control MiB/s | Audit omitted MiB/s | Diagnostic/control |
| ---: | ---: | ---: | ---: |
| 1 | 65.54 | 79.60 | 1.215× |
| 2 | 62.02 | 64.43 | 1.039× |
| 3 | 54.34 | 67.06 | 1.234× |
| 4 | 52.77 | 60.38 | 1.144× |
| 5 | 69.48 | 77.51 | 1.116× |
| 6 | 55.19 | 69.93 | 1.267× |
| 7 | 65.98 | 73.90 | 1.120× |
| 8 | 60.57 | 62.65 | 1.034× |

The eight-arm control median was **61.30 MiB/s**, the diagnostic median **68.50 MiB/s**
(1.117× ratio of medians); median paired ratio was **1.132×**. Median PUT p50 was 303.35 ms
control versus 247.85 ms diagnostic. Median server CPU seconds per scored GiB were 22.94 and
22.16 respectively, but this includes work before/after the analyzed interval and is only a
rough efficiency check. Median peak server RSS was 40.71 versus 40.88 MiB; there is no memory
benefit. All 16 arms passed with zero request errors. The largest observed task-root allocation
was 2,724,552,704 bytes, under the 10-GB cap. No RustFS arm was run for this deliberately
non-conformant build.

The shared node hosted unrelated Go tests and a VM. Several first-run control arms overlapped
those Go tests while the corresponding diagnostic arms did not; the VM also ran during the
second run. Alternation and eight same-direction pairs reduce, but cannot eliminate, host-load
bias. The diagnostic also removes the activity SQL itself, not just the response wait, so its
gain is an **upper-bound experimental signal**, not a predicted co-commit uplift. Writer timing
quantiles after the drain are batch-skewed and do not isolate audit latency. A safe candidate
must preserve successful activity, conditional-failure behavior, best-effort audit isolation,
notification ordering and multi-shard semantics; see [W4](performance-put-1m-plan-2026-09.md).

The two bounded invocations consumed 327.39 and 543.62 measured seconds. Added to the prior
710.84 seconds, the separately approved adoption ledger is **1,581.85/3,600 seconds**, leaving
**2,018.15 seconds**. The original campaign's approximately 22-second balance remains untouched.
Build/download time was outside the measured invocations. Task-owned stores were removed by the
runner after each arm. After the table and ledger were retained here, the isolated binaries,
client, scratch JSON, private directory and task-owned pinned Docker image were deleted. No
benchmark process remains. The real worktree has no tracked Rust change from this diagnostic.
