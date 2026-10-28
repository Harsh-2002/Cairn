# PUT flush-reduction screen — 2026-10-27

Status: **captured call mix changed; throughput inconclusive on this host.** This is not an adoption
claim and not a Cairn-versus-RustFS score.

## Recomputed evidence — 2026-10-27

An independent [identity/window cross-check](https://github.com/Harsh-2002/Cairn/blob/117dc8c/research_notes/Cairn%20single%20node%20performance%20plan/evidence-crosscheck.md)
supersedes this report's original arithmetic and attribution. The original text reported GM
1.057, median 1.133 and interval 0.766–1.459; correct binary-aware pairing gives **1.139036,
1.189077 and Student-t 95% interval 0.793619–1.634795**. The original per-successful-PUT sync
normalization (7.11→4.73, −33.5%) mixed capture and scored windows and is withdrawn, not refined.
The corrected tables below preserve the historical binaries and all four evidence JSON files
unchanged. No new load or fault reproduction was performed. See the
[final single-node plan](https://github.com/Harsh-2002/Cairn/blob/117dc8c/docs/performance-single-node-architecture-plan-2026-10.md) for the decision.

## Question

Does the unmerged `perf/put-cleanup-debt` + `perf/audit-co-commit` stack change captured sync-call
composition for plain 1-MiB PUT, and does that translate into throughput on this node?

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
This is a within-arm capacity policy, not an A/A equivalence gate. Across the twenty throughput
arms, retained MemTotal spans 8,833,784–9,982,912 KiB; swap capacity is 4,194,300 KiB. Neither
fixed cache capacity nor actual paging can be inferred from the host shorthand above.

## Mechanism: captured counters, not calls per PUT

Two alternating pairs with the glibc sync interposer, 12-second requested arms, in actual order
main/branch/branch/main. Counts are pooled by executable identity. Capture begins before Warp
creation and ends after Warp exit and analyzer reduction; scored rates cover a narrower interval.
Exact successful full-capture operation counts and post-probe timestamps are absent. Thus
`objects_per_second × analyzed_seconds` is not a matching denominator.

| Captured class | Main calls | Branch calls | Main / captured fdatasync | Branch / captured fdatasync |
| --- | ---: | ---: | ---: | ---: |
| `fdatasync`, descriptor role unknown | 1,427 | 1,772 | 1.000000 | 1.000000 |
| `fsync`, regular/unknown | 1,070 | 1,143 | 0.749825 | 0.645034 |
| `fsync`, other directory | 831 | 840 | 0.582341 | 0.474041 |
| `fsync`, exact data root | 1,427 | 1,772 | 1.000000 | 1.000000 |
| `fsync`, `.staging`-classified | **2,080** | **386** | **1.457603** | **0.217833** |
| **Total intercepted calls** | **6,835** | **5,913** | **4.789769** | **3.336907** |

The total-call/fdatasync ratio is **30.3326% lower**, and staging/fdatasync is **85.0554% lower**.
These are captured call-composition ratios, not per-object savings or physical flush counts.
The probe counts attempts, including failures; it cannot identify WAL/DB descriptors, cleanup
origin, successful objects or bypassed/raw syscalls. Summed syscall wall times overlap across
threads. See [probe semantics](../conformance/sync_probe.c) and
[runner boundaries](../conformance/rustfs_compare.py).

Source/tests separately support the intended never-created-spool, shared-absence and activity
mechanisms. These combined process-wide captures do not isolate their causal contributions,
prove less durable work per success, or establish higher throughput/equal physical persistence.

## Noise floor

Three A/A pairs (identical candidate binary on both sides), 12-second requested arms:
**CLI candidate/control** ratios 1.360, **1.013**, 0.756; sample log-ratio SD **0.293508**.
Chronological second/first ratios are instead 1.360, 0.987, 0.756. The arm range is
65.80–99.75 MiB/s; the label-consistent GM t95 interval is 0.488870–2.101324, not equivalence.
The originally reported interrupted 30-second A/A has no retained arm sequence in these JSON.

Conditional iid stationary normal-log planning (two-sided alpha .05, 80% power, test against zero
improvement) gives **35/75 pairs for 15%/10%** using A/A SD, or **103/221** using actual A/B SD.
Three A/A pairs estimate noise poorly; these are not prescribed counts or permission for more
traffic. Proving a gain exceeds 10% needs the true-effect-minus-margin signal and multiplicity/
session-aware planning, not this zero-effect calculation.

## Throughput

Ten alternating control/candidate pairs across two invocations (the second with control and
candidate swapped), 12-second arms:

| Statistic | Value |
| --- | ---: |
| Pairs | 10 |
| Candidate/main geomean | 1.139036 |
| Median paired ratio | 1.189077 |
| 95% Student-t interval for GM (df=9) | 0.793619 – 1.634795 |
| Paired wins | 5 / 10 |
| Sample paired log-ratio SD | 0.505111 |

Pair chronological arms and join CLI labels to each invocation's binary SHA: invocation B reverses
the assignments. The interval spans about **−20.6% to +63.5%**, conditional on independent normal
log-pairs; shared-node dependence/confounding is not corrected. The second arm wins **3/5 in each
invocation, 6/10 overall**; this does not establish strong arm-order causation. Neither a win nor
a regression is established. Formula, full pair table and stdlib reproduction are in the cross-check.

## Interpretation

- Captured sync composition changed; per-success and physical-flush savings are **unmeasured**.
- Throughput remains **INCONCLUSIVE**; log SD is not a percentage throughput SD.
- No merge decision should cite these throughput numbers. The next measurement needs a quiet host
  and a preregistered A/A/observer gate. Do not cherry-pick low-pressure winning arms or discard
  candidate-caused pressure after observing scores.

Read-only CI verification on 2026-10-27: [run 40521968061](https://github.com/Harsh-2002/Cairn/actions/runs/40521968061)
**succeeded for exact `12a5d0c381477070810f750737e2663d9c813772`**. This is historical branch CI,
not new performance evidence, production-readiness certification or authorization to merge drafts
#125/#126. This documentation correction has not itself been committed or run through remote CI.

## Evidence

Compact, non-secret JSON (heavy per-sample arrays removed):

- [noise floor](performance-evidence/2026-10/performance-flush-aa-2026-10-27.json)
- [mechanism pairs](performance-evidence/2026-10/performance-flush-mech-2026-10-27.json)
- [throughput, invocation A](performance-evidence/2026-10/performance-flush-thr1-2026-10-27.json)
- [throughput, invocation B, swapped](performance-evidence/2026-10/performance-flush-thr2-2026-10-27.json)
