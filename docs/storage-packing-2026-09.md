# Isolated small-record packing laboratory

Phase 4A is an isolated prototype under `conformance/storage_lab`. No production storage format,
routing, schema, feature flag, configuration or adoption threshold changes. The existing
raw/CRNB encoded record is the unit of publication; S3 authorization, request framing and encoding
are outside this experiment. Compilation, tiny correctness fixtures and CI are not campaign
measurements. Local prototype and complete workspace validation pass; final-head CI remains required.
No packing performance result is claimed.

## Publication and ownership

One actor owns the laboratory SQLite connection in WAL mode with `synchronous=FULL`. It admits
exact temporary/final artifact names before creation, publishes locations transactionally after
file sync, rename, parent sync and hash/length validation, and records exact cleanup debt. This
actor and its schema are a laboratory model, not the production Cairn Writer or evidence of
complete S3 metadata parity. Reads and descriptor pin acquisition serialize with retirement.
The actor, creation capabilities and pinned descriptors retain the exclusive laboratory root
lifetime. Process generation and immutable artifact generation are distinct identities.

Locations identify either a dedicated file and length, or a segment plus record offset and
length. Record bounds are checked before reads; positioned reads have independent cursors and
cannot cross into adjacent records. Row identity, encoded digest, logical length and trusted
encoding declarations remain authoritative. A segment header is 80 bytes and each record header
is 40 bytes. These bytes count toward the 4-MiB physical segment ceiling. This framing is
experimental and carries no production compatibility promise.

Completed, known-length encoded records up to 1 MiB can enter one builder. It seals on physical
capacity, 256 records or the first record's non-sliding 1-ms deadline. Large and unknown-length
records use dedicated streamed files. Bytes and record slots are admitted before payload
allocation, bounded at 32 MiB and 256 outstanding records, and retained through actual filesystem
work and SQLite acknowledgement. Filesystem scratch also consumes the byte allowance. The
coordinator uses one outstanding request per workload worker. It records both the byte peak and
record peak; neither is a whole-process RSS measurement.

## Required correctness and decision gates

4A covers publication barriers, exact receipts, SQL rollback/conditions, reader bounds, raw and
CRNB interpretation, corruption, cancellation and explicit cleanup ownership. Its fixed fixture
verifies every object and requires empty pending/debt state before success. A command deadline
or incomplete observation is INCONCLUSIVE, never a performance pass.

4B must add collection of segments at least 50% dead, replacement-space admission, exact-location
conditional relocation, reader-safe retirement, interrupted collection, ENOSPC, manifest-last
snapshots and fresh restore with survivor verification. Protected history and Object Lock cannot
be weakened by relocation. 4C will predeclare the paired matrix, primary metric, drift checks,
protected workloads and runtime admission before any comparative measurements. It must include
overwrite/delete/collection costs and retain files without a qualifying contiguous crossover.

The current single-arm coordinator reports counts and bounded latency count/sum/maximum only.
It does not report p99, cache-cold results, full PUT throughput, production recovery equivalence
or a packing adoption decision. Publication wall time and per-request latency include deterministic fixture generation,
admission, filesystem durability/hash validation and SQLite acknowledgement. Readback and
cleanup are outside that timer but remain inside campaign runtime. No encoding or S3 work is
performed. Every comparison must preserve this boundary in both arms.

## 4A validation record

Rust 1.97.1 passes all 37 packing tests and all-target Clippy. They cover exact publication
barriers and injected ENOSPC, SQL rollback and conditional batches, stale/duplicate ownership,
root identity and hostile paths, raw/CRNB encrypted interpretation after reopening, wrong keys,
truncation, ranges, protected history, cancelled replies and actual blocking jobs retaining
buffers/reader pins. The post-admission deadline fixture refuses physical creation after expiry.
Twelve coordinator tests pass, including real 16-object, 1-KiB fixtures at concurrency four in
files, packed and unknown-length modes. These fixed tests are not performance measurements or
power-loss evidence. The complete workspace gate passes (1,466 default / 1,492 all-feature tests, both Clippy
configurations and two doctests), as do all 47 standalone Rust tests and all 70 Python tests
with every live fixture enabled. Web checks, audits, installer and workflow policy pass.
Final-head CI remains pending.
