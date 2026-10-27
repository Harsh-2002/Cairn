# PUT flush-reduction screen — 2026-10-27

Status: **mechanism confirmed; throughput inconclusive on this host.** This is not an adoption
claim and not a Cairn-versus-RustFS score.

## Question

Does the unmerged `perf/put-cleanup-debt` + `perf/audit-co-commit` stack reduce the durable work of
a plain 1-MiB PUT, and does that translate into throughput on this node?

## Binaries and client

| Component | Identity |
| --- | --- |
| Control | `main` at `c53aee6` (includes the parent-barrier dedupe), release binary SHA-256 `a2603381e8c4152f793a0a51f12cd3630ac8c0967eaf8729a3fe629bcf71a60a` |
| Candidate | `perf/audit-co-commit` at `f553779`, release binary SHA-256 `be4dc103667df37b6682d52b3a3ee0b26dff9932665425b76575574bc8514ca7` |
| Client | Warp **v1.8.0** extracted from the pinned `quay.io/minio/aistor/warp:v1.8.0` image (digest `sha256:594e582a…`), executable SHA-256 `d41abf5ff9bd61796def9d6ee9856fa25d325617476cd0c34b482cb42c716c8b` |
| Host | 4 vCPU KVM, one 100-GB virtual disk, shared node, `MemTotal` ~8.0 GiB with ballooning |

Every arm used a fresh store, `CAIRN_META_SYNCHRONOUS=full`, one shard, zero Writer linger, and
path-style SigV4. All arms completed with zero reported errors and passed the runner's capacity
policy under `--allow-ballooning`.

## Mechanism: fewer sync calls per PUT

Two alternating control/candidate pairs with the glibc sync interposer, 12-second arms. Counts are
libc `fsync`/`fdatasync` calls during the scored load divided by successful PUTs
(`objects_per_second × analyzed_seconds`).

| Sync class | Control, calls/PUT | Candidate, calls/PUT | Delta |
| --- | ---: | ---: | ---: |
| `fdatasync` (staged file) | 1.48 | 1.42 | −4% |
| `fsync` other (WAL/DB) | 1.11 | 0.90 | −19% |
| `fsync` other directory | 0.86 | 0.68 | −21% |
| `fsync` data root | 1.48 | 1.42 | −4% |
| `fsync` `.staging` (cleanup) | **2.18** | **0.31** | **−86%** |
| **Total** | **7.11** | **4.73** | **−33.5%** |

The staging collapse is the index-spool debt removal plus the coalesced absence barrier: a plain
write no longer retires a name it never created, and concurrent cleanups share one fsync. The small
`fsync other` drop is consistent with removing the separate activity commit. The staged-file and
root barriers are unchanged, as designed.

This is deterministic evidence that the branch performs less durable work. It is not evidence of
higher throughput.

## Noise floor

Three A/A pairs (identical candidate binary on both sides), 12-second arms:
paired ratios 1.360, 0.987, 0.756; **paired log-ratio SD 0.294**; arm range 65.80–99.75 MiB/s.
A 30-second A/A run hit the measured-load cap after five arms and was not used.

At that noise level a five-pair run cannot resolve a 10–15% effect. About 30 pairs would be needed
for a 15% effect at 80% power, and roughly 100 for 10%.

## Throughput

Ten alternating control/candidate pairs across two invocations (the second with control and
candidate swapped), 12-second arms:

| Statistic | Value |
| --- | ---: |
| Pairs | 10 |
| Candidate/main geomean | 1.057 |
| Median paired ratio | 1.133 |
| 95% CI (paired log-ratio) | 0.766 – 1.459 |
| Paired wins | 5 / 10 |
| Paired log-ratio SD | 0.52 |

The interval spans a 23% regression and a 46% improvement. A strong arm-order effect was visible:
in the first invocation the second arm of the pair won four of five pairs, which the alternating
design does not cancel when drift is this large. This run establishes neither a win nor a
regression.

## Interpretation

- The branch does **33.5% fewer libc sync calls per successful PUT**, with the staging barrier
  dropping by 86%. That is the mechanism the changes were built for.
- Throughput on this shared node is **not resolvable** at 10–15%: the A/A floor is 29–53% paired
  SD, and the 10-pair confidence interval covers both directions.
- No merge decision should cite these throughput numbers. The next measurement needs a quiet host
  or a dedicated virtual disk, or a paired design that gates on `/proc/pressure/io`.

## Evidence

Compact, non-secret JSON (heavy per-sample arrays removed):

- [noise floor](performance-evidence/2026-10/performance-flush-aa-2026-10-27.json)
- [mechanism pairs](performance-evidence/2026-10/performance-flush-mech-2026-10-27.json)
- [throughput, invocation A](performance-evidence/2026-10/performance-flush-thr1-2026-10-27.json)
- [throughput, invocation B, swapped](performance-evidence/2026-10/performance-flush-thr2-2026-10-27.json)
