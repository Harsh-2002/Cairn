# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Cairn serves operators and developers who self-host S3-compatible object storage, most often on a
single host. They may understand applications and infrastructure without being storage or IAM
specialists. Their jobs include provisioning buckets, moving and inspecting object data, issuing
scoped application credentials, configuring data protection and replication, and diagnosing the
health and activity of a production storage node.

## Product Purpose

Cairn provides production-oriented S3-compatible object storage without requiring a distributed
storage cluster or a separate management stack. It combines the S3 data plane, management API,
browser console, and CLI in one Rust binary. Success means an operator can deploy a node, connect
unmodified S3 clients, manage access and data safely, and understand the node's state without first
becoming an expert in S3 IAM or Cairn's internals.

## Positioning

Cairn is the self-contained alternative for workloads that need a trustworthy single-node S3
endpoint rather than a distributed object-storage cluster. Object bytes remain ordinary files on a
POSIX filesystem, embedded SQLite is the metadata source of truth, and the optimized React console
ships inside the same binary. This gives operators a small deployment artifact, inspectable storage,
crash-consistent metadata semantics, and an approachable control surface while retaining standard S3
client compatibility.

## Operating Context

Cairn runs directly as a host service or as a container; Kubernetes deployments use one StatefulSet
replica with persistent storage. Server configuration is supplied exclusively through validated
`CAIRN_*` environment variables. The S3 data plane normally listens on port 7373, while the embedded
console and management API use a separate listener on port 7374 so browser credentials and stored
object content remain on distinct origins.

Operators work through standard S3 SDKs and tools, the browser console, or CLI subcommands. Typical
flows include bootstrapping the first administrator, creating and configuring buckets, uploading and
previewing objects, issuing scoped credentials, reviewing activity and metrics, configuring lifecycle
or replication, and running node-local backup, restore, reconciliation, and integrity operations.
TLS may terminate in Cairn or at a correctly configured trusted reverse proxy.

## Capabilities and Constraints

- The S3 surface includes multipart upload, versioning, Object Lock, tagging, policies and ACLs,
  public-access controls, CORS, lifecycle rules, checksums, range requests, presigned URLs, and
  persistent object shares. The exact implemented surface is tracked in `docs/s3-api-matrix.md`.
- The embedded console manages buckets and objects, users and credentials, tags, activity, metrics,
  replication, imports, and bucket settings. It is a presentation layer over the administrator-gated
  management API and contains no privileged business logic of its own.
- Optional transparent compression, SSE-S3, SSE-KMS-compatible request handling, mandatory or
  transparent encryption at rest, webhooks, imports, and asynchronous bucket replication are part of
  the production surface. KMS identifiers are labels and allow-list gates in v1, not cryptographic
  isolation domains.
- A Cairn node is deliberately single-node: it does not provide consensus, synchronous clustering,
  or erasure coding. Cross-host redundancy comes from asynchronous replication, while local media
  redundancy remains the operator's responsibility.
- The database, staging area, and blob directory must share one POSIX filesystem so atomic rename can
  preserve the durability protocol. Metadata writes are serialized through the configured SQLite
  writer; sharding is an explicit scale-up option rather than a distributed control plane.
- The default metadata backend is bundled SQLite. The libSQL and Turso-compatible backends are
  selectable alternatives with narrower maturity and platform constraints documented in the
  engineering specification.

## Brand Commitments

The product name is Cairn. Its voice is approachable, reassuring, and technically precise: a calm,
competent operator explaining the system in plain language. It lowers the intimidation of running
object storage without becoming playful, cute, or casual about production data. Claims must remain
specific and evidence-backed, especially around durability, security, compatibility, and performance.
The binding visual language and visual anti-references live in `docs/design.md`.

## Evidence on Hand

- The implemented behavior and architectural contract live in `crates/`, `web/`, `CONTRACT.md`, and
  the section-numbered engineering specification under `docs/`.
- `docs/s3-api-matrix.md` records compatibility by operation and condition; `conformance/` contains
  real-SDK, crash-consistency, replication, stress, and interoperability harnesses.
- `.github/workflows/ci.yml` is the hosted quality gate, while the root `Makefile` exposes the local
  formatting, lint, test, web-build, dependency-audit, and installer checks.
- `docs/benchmarks.md` records reproducible benchmark commands, observed results, and their caveats.
- No customer testimonials, adoption numbers, certifications, third-party audits, or independent
  performance claims are established in this repository; future product work must not invent them.

## Product Principles

- **Standard at the boundary, simple inside.** Work with ordinary S3 clients while keeping the node's
  deployment and storage model inspectable.
- **Trust is a behavior.** Durability ordering, fail-closed security, bounded resources, explicit
  warnings, and honest limitations matter more than feature theatre.
- **Make S3 concepts legible.** Present policies, credentials, versioning, lifecycle, encryption, and
  replication as understandable operator choices without hiding their consequences.
- **One capability, multiple honest interfaces.** The S3 API, management API, console, and CLI share
  the same underlying authority and semantics; the console must not gain a privileged shortcut.
- **Measure before claiming.** Compatibility and performance statements must point to a repeatable
  test, conformance harness, or benchmark with its environment and caveats.

## Accessibility & Inclusion

WCAG AA is the non-negotiable floor, with AAA contrast targeted where it does not obstruct the task.
The console supports full keyboard navigation, visible focus, semantic landmarks and controls,
generous mobile hit areas, reduced-motion preferences, and light/dark themes at contrast parity.
Muted and placeholder text must remain readable rather than functioning as decorative gray.
