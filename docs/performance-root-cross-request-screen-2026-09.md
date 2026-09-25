# Cross-request root-directory fence screen — 2026-09-23

Decision: **reject and revert**. Source base is Cairn
`f6fde5a68c6a5d5acee54f950ce2be8cda547312`. The candidate changes only ordinary
object-path preparation: open and lock the flat `.staging` and bucket directories, then await
the existing inode-keyed directory-sync coordinator on the common data root before creating
any staged file. Nested multipart preparation, staged-file sync, final rename/directory sync,
hash validation, metadata commit and exact cleanup were unchanged. The intended gain was fewer
root syncs across concurrent PUTs; the previous one-root-sync-per-request candidate reduced
namespace time but failed the end-to-end adoption gate.

The preliminary screen used two alternating same-host pairs of strict 1-MiB PUT/c16 with fresh
stores and 12-second requested loads. The pinned 1.97.1 fat-LTO control and candidate used the
same real web bundle and Warp v1.8.0. The [Python runner](../conformance/cairn_put_ab.py)
pinned SQLite FULL, one shard and zero Writer linger. Every arm passed with zero unexpected
errors. A candidate could advance to five-pair PUT and protected mixed/GET/LIST checks only
with a materially positive paired pattern (at least 20% median, no contradictory pairs) and
correctness gates. These pairs contradicted each other, so it did not advance. Nothing from
this two-pair screen alone could authorize adoption.

The owning `cairn-blob` crate passed 99 unit and 42 integration tests; two mount-namespace
tests were ignored because they require extra privileges. Its all-targets pinned-toolchain
Clippy gate and workspace formatting check passed. Full workspace/conformance verification and
protected benchmarks would be required for production adoption; the rejected candidate did not
receive them.

The control binary SHA-256 was
`d725670f923191d89d8807db9cf2e0dcbe52389fd0735d7f74a72c9082ac7744`; the
candidate was `5c0f05166588d973afe82130bad4305eb674b5899bc9b95233c803efde1eb9c3`.
The exact three-file candidate diff SHA-256 was
`0f17ba76cc6096dff457ed4a91b7c8af33f9317b930c13b021a6b2702d0f4180`.
The control came from `git archive HEAD`, the candidate from the working tree. An initial
candidate invocation falsely reused the control build because both source trees mounted at the
same container path and Cargo's previous fingerprints looked fresh; it was **not benchmarked**.
The changed crate was explicitly cleaned from the task-owned release target and the candidate
rebuilt; the distinct binary hashes above were verified before load. Warp matched its published
SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.

| Pair | Order | Control MiB/s | Candidate MiB/s | Candidate/control | Host full I/O pressure, control/candidate |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Control → candidate | 55.19 | 40.26 | 0.729× | 1.21 / 1.59 s |
| 2 | Candidate → control | 48.32 | 91.75 | 1.899× | 2.61 / 0.82 s |
| Arm median | — | 51.76 | 66.01 | 1.275× | — |

The two-pair ratio of medians looks positive only because one candidate arm was unusually fast;
the other lost 27%. The control's second arm also declined 12% while host I/O pressure rose.
The four load intervals each produced 7–8 bounded process/host/device samples with no drops.
All were on the same shared volume; `/proc/pressure/io` and `/proc/diskstats` are host-wide, not
Cairn-only. No result meets the consistency gate.

A separate one-pair, 12-second **diagnostic-only** glibc sync probe compared the exact same
binaries. Its interposer SHA-256 was
`18f15993d013eb42a3de72bc6a97b90fada75b403b3e1dd900ea8e59f7f64177`.
Control versus candidate throughput was **101.53 versus 56.27 MiB/s**; host full I/O pressure
was 1.25 versus 3.11 seconds. The probe's own overhead was not gated, so these rates are not
combined with the uninstrumented screen. During the load interval, captured data-root `fsync`
calls fell from **2,436** control to **308** candidate, while `fdatasync` calls were 1,218 versus
675. Thus root syncs per captured file-data sync fell from **2.00 to 0.46**. This verifies that
cross-request coalescing engaged; it does **not** establish end-to-end improvement or an exact
per-PUT count because preparation and cleanup also lie in the interval. Other directory/file
syncs and disk contention remained. The protected workload matrix was not run after the
preliminary PUT consistency failure.

The two invocations consumed 69.607 and 33.313 measured seconds. The separately approved
campaign now stands at **3,549.181/3,600 seconds**, leaving **50.819 seconds**. The original
campaign's roughly 22-second balance remains separate. The task-owned build/client tree was
about 3.0 GB, the maximum sampled arm allocation 1.36 GB, and generated web files about
0.23 GB: a conservative combined peak below 4.7 GB, well under 10 GB. Per-arm stores and
children were reaped by the runner. The temporary
build/client/probe artifacts were removed after recording this result. The safe conclusion is
that reducing root syscall count alone did not deliver a stable PUT gain on this shared device;
a further candidate needs foreground stage timing aligned with device pressure.
