# Diagnostic sync-call attribution — 2026-09-23

This follow-up to the [strict performance comparison](performance-strict-followup-2026-09.md)
narrows the next 1-MiB PUT experiment. It does **not** demonstrate a production speedup or
replace paired, uninstrumented scores. Cairn source remains `f6fde5a6`; there is no production
Rust change. The default-feature release used here was rebuilt with the pinned Rust 1.97.1 and a
real web bundle, SHA-256 `5da61ca38c18ac7ba91faafedcc1e7bd68688c4b7c546dcc67aaac03bde52c5b`.
Warp v1.8.0 was SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.
The [compact machine-readable evidence](performance-evidence/2026-09/performance-sync-probe-evidence-2026-09.json) retains
the exact captured totals after task-owned raw stores are removed.

## Method and limits

A diagnostic-only glibc preload, [source](../conformance/sync_probe.c), intercepts `fsync` and
`fdatasync`, accumulates bounded process-wide counts and wall times in a shared memory-mapped
file, and emits **no paths, descriptors, payloads or keys**. The [Python reader](../conformance/sync_probe.py)
and [benchmark coordinator](../conformance/rustfs_compare.py) reject an empty capture, which a
static binary or bypassed libc symbols could otherwise misreport as zero sync cost. Real-child
tests proved directory `fsync`, regular-file `fsync`, and `fdatasync` engage on this glibc host;
16 Python tests passed. No new production dependency, thread, configuration knob or hot-path
timer was added.

The final probe classifies `fsync` on the data root, paths beneath `.staging`, other
directories, and regular/unknown files; this classification uses one `fstat`/`readlink` before
the measured call. It cannot see direct raw syscalls or an inlined/bypassed libc call. It records
*summed syscall wall time*, which overlaps across threads and cannot be added to request latency.
It does not identify SQLite versus non-SQLite regular files, attribute a call to a particular
request, or isolate Warp's analyzed interval from its preparation and shutdown tail. Interposer
overhead has **not** passed the plan's ≤3% paired gate; its throughput is diagnostic only.

The final Cairn-only arm requested 12 seconds at c16, analyzed 9 seconds, and spent 16.35 seconds
inside the complete Warp load process. The table subtracts probe counters taken immediately
before starting Warp from counters taken after Warp exits, excluding server bootstrap and bucket
creation. All reported operations passed with zero Warp errors; the instrumented rate was 39.10
MiB/s. The VM/disk were not isolated; host I/O PSI `some` advanced 4.54 seconds during that
load interval.

| Intercepted call class | Calls during Warp load | Summed wall seconds | Mean per call |
| --- | ---: | ---: | ---: |
| Data-root `fsync` | 958 | 22.71 | 23.7 ms |
| `.staging` directory `fsync` | 787 | 17.94 | 22.8 ms |
| Other-directory `fsync` | 219 | 6.06 | 27.7 ms |
| Regular/unknown `fsync` | 358 | 11.32 | 31.6 ms |
| `fdatasync` | 479 | 17.47 | 36.5 ms |

The root count is close to twice the `fdatasync` count. This is **consistent with** the verified
flat-PUT path preparing `.staging` and the bucket under the same root, with a parent barrier
after each, including when those directories already exist. It is not an exact per-PUT count:
the interval includes Warp preparation, metadata work, cleanup, and other background activity.
Staging and other-directory barriers are also material. The prior same-request one-root-barrier
candidate reduced its local namespace timer but won only two of five end-to-end pairs, so this
probe does not revive that candidate as a proven optimization. A cross-request barrier-sharing
design or a more selective exact-cleanup barrier could be evaluated **only after** identifying
which calls are foreground and which are cleanup and preserving every creation/absence fence.

Two earlier single-engine probe arms give useful context but not a comparison. A 30-second
requested arm analyzed 27 seconds at 49.30 MiB/s; whole-arm counters (including startup and
preparation) saw 6,302 directory `fsync`, 1,081 other `fsync`, and 1,453 `fdatasync` calls. Its
existing Writer metrics, drained after load, showed 1,573 commits, a median commit of 35.1 ms,
median Writer queue wait of 21.9 ms, median SQL apply of 0.178 ms, and a mean batch size of
6.47 mutations. The metrics span preparation and post-score activity and the writer samples
share batches; they cannot be allocated to one PUT. A second 20-second requested arm analyzed
18 seconds at 73.68 MiB/s. Its load-only counters were 6,494 directory `fsync`, 1,331 other
`fsync`, and 1,507 `fdatasync`; host I/O PSI `some` advanced 7.02 seconds. The large rate and
sync-time variation reinforces the need for a quieter host and paired A/B before adoption.

The first two attempts to launch the probe failed *before traffic*: Cairn's strict Figment
parser correctly rejected a diagnostic variable accidentally named with the `CAIRN_` prefix.
The probe now uses `SYNC_PROBE_FILE`; the failure path retains a bounded server-log tail so a
future startup failure is diagnosable. The attempts consumed about four seconds of the campaign
allowance and were never promoted to scored arms.

## Next decision

The most credible bottleneck is the number and latency of durable directory/metadata syncs on
this virtual disk, not hash CPU or warm-read bandwidth. The Writer's median SQL apply time was
sub-millisecond while commit and queue waits were tens of milliseconds in the diagnostic arm;
the directory probe independently saw many similarly long syncs. This is a **bounded inference**,
not a complete causal decomposition. Next work should classify foreground versus cleanup syncs
in a scored window and screen one cross-request sharing design that retains exact directory
ownership and propagates fsync failure to every waiter. The full crash-race suite in the
[optimization plan](performance-put-1m-plan-2026-09.md) and five paired uninstrumented PUT
trials, plus protected mixed/GET/LIST, remain mandatory before adopting anything.

The prior campaign had about 125.04 of 3,600 measured seconds remaining. These three diagnostic
invocations consumed 54.03 + 26.70 + 17.89 seconds, and the two pre-traffic failures about
four seconds, leaving **roughly 22 seconds**. Compile/test time is excluded as before. No further
performance run under that campaign is justified by the remaining allowance; it must not be
silently reset. All task-owned probe stores, binaries, web build products, bytecode and images
were removed after capturing the numbers above; no benchmark process remains.
