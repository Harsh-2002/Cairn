# Periodic-cleanup interference screen — 2026-09-23

Decision: **do not adopt cleanup deferral**. A private, deliberately non-conformant diagnostic
binary postponed only the periodic exact-cleanup sweep for one hour. It left cleanup debt
outstanding during each short arm; that violates the normal cleanup-liveness policy and is not
a production candidate. Its three-pair 1-MiB PUT screen missed the predeclared 20% uplift and
five-pair adoption gates. The shared node drifted sharply, so the observed difference cannot
establish a causal cleanup bottleneck. No tracked Rust code was changed.

Both binaries started from Cairn `f6fde5a68c6a5d5acee54f950ce2be8cda547312`, used the
same built web bundle, default features, fat-LTO release profile, and pinned Rust 1.97.1 Docker
image (digest `sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97`).
Control SHA-256: `0cd37126bdafad688fbcd3e88200807eb570cf78b9af13e9056ea0ff5b804b68`.
Diagnostic SHA-256: `bc8fb8febe3bbd7717a7e62747db8b3fc224f542ef20700e860e5b50eea1fd2d`.
The diagnostic `background.rs` SHA-256 was
`c23f23c7e2171df35240d6691c6e752c67b782110a1d3ea658758cba8a466bb9`.
The sole source change replaced the initial one-second `next_cleanup` deadline with one hour;
the tested `raw_io.rs` was byte-identical to HEAD. This did not skip foreground durability
barriers. Warp v1.8.0 (`13c3b89`) matched publisher SHA-256
`d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b`.

The [Python paired runner](../conformance/cairn_put_ab.py) used fresh stores, one server at a
time, alternating order, explicit SQLite FULL, one shard, zero Writer linger, and loopback
path-style SigV4. Warp PUT was exactly 1 MiB at concurrency 16. Loads requested 30 seconds,
without a metrics drain. All six arms passed with zero Warp errors; the maximum task-owned
allocation sampled within an arm was 1,813,884,928 bytes, under the 7-GB run cap.

| Pair | Order | Control MiB/s | Cleanup-deferred MiB/s | Candidate/control |
| --- | --- | ---: | ---: | ---: |
| 1 | Control → diagnostic | 52.93 | 50.48 | 0.954× |
| 2 | Diagnostic → control | 40.53 | 46.10 | 1.137× |
| 3 | Control → diagnostic | 35.68 | 41.73 | 1.170× |
| Median | — | 40.53 | 46.10 | 1.137× |

The diagnostic won two of three pairs, but the control declined 32.6% from its first to last
arm. Thus the 13.7% median difference is at most a noisy upper-bound screen, not an estimate of
what an exact-cleanup scheduling redesign would gain. Median sampled server peak RSS was
38.62 MiB control versus 39.11 MiB diagnostic; median sampled server CPU was 25.14 versus
28.65 seconds. Those samples do not measure deferred debt, post-load cleanup cost, or sustained
footprint. No protected GET/mixed/LIST or crash/cleanup correctness matrix ran because the
preliminary PUT gate failed. A safe batching design must still perform and settle every exact
cleanup with its durability fence; this diagnostic cannot justify omitting or postponing one.

This run consumed 218.058 measured seconds. The separately approved campaign now stands at
**3,446.261/3,600 seconds**, leaving **153.739 seconds**. The original campaign's roughly
22-second balance remains separate. Per-arm stores and child processes were reaped by the
runner. Private build/client and compact scratch JSON were removed after this report; no
diagnostic binary is retained. The next attribution step needs per-request foreground versus
cleanup barrier timing and time-aligned host pressure under less-drifting controls before a
cleanup-specific architecture choice can be made.
