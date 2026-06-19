# Cairn documentation

This is the single source of truth for Cairn. The engineering specification is split here into focused, section-numbered reference documents; alongside them are the
operator guides. The **section numbers are stable identifiers** used throughout the codebase
(comments say e.g. "ARCH 28") and across these docs — find the number in the table below to jump
straight to the document that covers it.

## Reference (the engineering specification)

| Document | Sections | Covers |
|---|---|---|
| [overview.md](./overview.md) | 0–5 | How to read; executive summary; positioning, scope, and non-goals; why a Rust rewrite; baseline storage architecture; gap analysis and the delta to Cairn |
| [data-plane.md](./data-plane.md) | 6–7 | System overview (data plane, control plane, node model); concurrency, runtime, and the I/O model |
| [storage-durability.md](./storage-durability.md) | 8–10 | Durability and crash consistency; on-disk storage model and layout; transparent compression at rest |
| [metadata.md](./metadata.md) | 11–12 | Metadata store (topology, schema, the group-committing writer, WAL read pool, cache); the internal trait abstraction layer |
| [s3-api.md](./s3-api.md) | 13, 16–19, 21 | S3 protocol layer and operation catalogue; object versioning; tagging; CORS; lifecycle management; server-side request lifecycles |
| [auth.md](./auth.md) | 14–15 | Authentication (SigV4 header/presigned + Bearer); the authorization model (policy / ACL / public-access-block / ownership) |
| [replication.md](./replication.md) | 20 | Bucket replication (the outbox engine and SigV4-signing sink) |
| [control-plane.md](./control-plane.md) | 22–24 | Management API; the embedded web console; the command-line interface |
| [configuration.md](./configuration.md) | 28 | The full `CAIRN_*` configuration reference |
| [security-errors.md](./security-errors.md) | 25, 27 | The error model and S3 error mapping; the security and threat model |
| [observability.md](./observability.md) | 26 | Metrics, logging, and audit |
| [testing-performance.md](./testing-performance.md) | 29–30 | Testing and S3 conformance; performance engineering and targets |
| [delivery.md](./delivery.md) | 31–34 | Build, packaging, deployment, and operations; the phased roadmap; the architecture decision log; appendices |

## Operator guides

| Document | Covers |
|---|---|
| [operations.md](./operations.md) | Configuring, deploying, and running a node: the one-filesystem invariant, the configuration table, bootstrapping, deployment shapes, day-two signals, the durability guarantee, and the **master-key rotation runbook** |
| [backup-restore.md](./backup-restore.md) | The backup procedure (database-first snapshot + blob copy), its consistency argument, and restore |
| [s3-api-matrix.md](./s3-api-matrix.md) | The S3 API support matrix — which operations are supported, partial, or out of scope |
| [benchmarks.md](./benchmarks.md) | The benchmarking methodology and the harnesses under `conformance/` |

## Design & product

| Document | Covers |
|---|---|
| [design.md](./design.md) | The management console's visual design system (read for UI work) |
| [product.md](./product.md) | Product positioning, users, brand, and design principles |

The end-to-end verification harnesses themselves live in [`../conformance/`](../conformance).
