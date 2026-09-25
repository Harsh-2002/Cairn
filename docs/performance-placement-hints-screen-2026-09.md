# Independent 1-MiB placement-hint screens — 2026-09-23

Decision: **reject both isolated diagnostic variants**. Neither supports a production change
under the predeclared ≥20% five-pair PUT gate. These were three-pair screens on a shared node,
not adoption campaigns; no protected GET/mixed/footprint matrix or full validation gate was run
for the rejected variants. No tracked Rust source was changed.

Both variants started from pristine Cairn commit
`f6fde5a68c6a5d5acee54f950ce2be8cda547312`, used the same real web bundle, pinned Rust
1.97.1 Docker image (digest
`sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`), default
features and fat-LTO release profile. The unchanged control binary SHA-256 was
`0cd37126bdafad688fbcd3e88200807eb570cf78b9af13e9056ea0ff5b804b68`. Variant A
omitted only `fallocate(KEEP_SIZE)` in `raw_io::preallocate_sequential`, retaining sequential
advice and post-sync `DONTNEED`; binary SHA-256
`df68f762b6e69e944660633838e56e3e2a9377cab75450999de10069780de0ed`. Variant B
restored the control preallocation and omitted only the post-sync `DONTNEED` advice;
binary SHA-256 `f6a4aa0f8879762f3bc983037c18515274bc5a6ff39984231d3b2bac83557bd1`.
The modified `raw_io.rs` SHA-256 hashes were respectively
`d6c811bea9f040abe51a8a3daa5075b4b691e4363ac0ce482b19e9a09b355b70` and
`750bfbec3f1e731614dd6a834cc0da08617b575487210e125f0f64b873492f86`.
Warp v1.8.0 (`13c3b89`) matched publisher SHA-256
`d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.

The [Python paired runner](../conformance/cairn_put_ab.py) used one server at a time, a fresh
store per arm, alternating order, explicit SQLite FULL, one shard, zero Writer linger, path-style
loopback SigV4 and Warp PUT 1 MiB/c16. Loads requested 30 seconds; their analyzed-window
MiB/s rates are below. No metrics drain was requested. All 12 arms passed with zero Warp errors
and remained under the 7-GB per-run cap. The task-owned build tree was about 1.1 GB, so the
whole task stayed below 10 GB. Unrelated VMs overlapped both screens.

| Variant | Pair 1 control → candidate | Pair 2 control → candidate | Pair 3 control → candidate | Control median | Candidate median | Ratio of medians | Paired wins |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| No `KEEP_SIZE` | 44.92 → 46.43 | 43.53 → 46.70 | 39.02 → 37.67 | 43.53 | 46.43 | 1.067× | 2/3 |
| No `DONTNEED` | 55.93 → 45.83 | 43.22 → 54.69 | 48.94 → 42.78 | 48.94 | 45.83 | 0.936× | 1/3 |

For no `KEEP_SIZE`, paired ratios were 1.034, 1.073 and 0.965 (median 1.034); the 6.7%
ratio-of-medians uplift is a different statistic and not a stable capacity gain. Median peak
server RSS was 40.10 MiB control versus 38.65 MiB candidate. The maximum sampled arm allocation
was 1,546,690,560 bytes. For no `DONTNEED`, paired ratios were 0.819, 1.265 and 0.874
(median 0.874); median peak RSS was 39.88 versus 38.41 MiB, and maximum sampled arm allocation
was 1,978,605,568 bytes. These small RSS observations cannot establish page-cache or sustained
footprint effects. No device-read/cache counters or immediate-read protection were collected,
because neither candidate passed the preliminary PUT screen. `rustix` uses its Linux raw backend
for these hints; the existing glibc sync interposer does not prove their syscall engagement.

The two bounded invocations consumed 220.697 and 219.268 measured seconds. Added to the prior
2,788.238 seconds, the separately approved campaign stands at **3,228.203/3,600 seconds**,
leaving **371.797 seconds**. The original campaign's roughly 22-second balance remains separate.
Build and download time are not measured load time. Per-arm stores and processes were removed by
the runner. The remaining isolated binaries/build cache/client and compact scratch JSON were
removed after the subsequent [cleanup-interference diagnostic](performance-cleanup-interference-screen-2026-09.md).
