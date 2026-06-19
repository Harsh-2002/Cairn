# Cairn: Architecture and Engineering Specification

**A production-grade, fully S3-compatible object storage server written in Rust, storing data natively on the local filesystem, with an embedded management UI, a command-line interface, optional transparent compression at rest, and asynchronous bucket replication.**

> **On the name.** This document uses the working codename **Cairn** for the new system. A cairn is a deliberate stack of stones that marks and endures; it suits a store whose job is to keep bytes safe on local disk. The name is a placeholder. Rename freely; nothing in the design depends on it.

> **Design philosophy.** Cairn is built from scratch in Rust from this specification. It is engineered for production rather than a single homelab node, carries a large feature set, and is designed around the properties Rust gives us for an I/O-bound, latency-sensitive, security-critical data plane. Where this document contrasts Cairn with a "naive" or "baseline" implementation, it does so only to motivate a design decision, not to describe any specific other system.

| | |
|---|---|
| Document status | Draft v2 |
| Target system | Cairn (Rust) |
| Audience | The Rust engineers who will build Cairn from scratch |
| Form | Technical specification in prose and tables. By request, this document contains no source-code listings; interfaces, schemas, and algorithms are specified in precise English and tabular form so that an engineer can implement them in whatever shape the language idioms of the day favour. |
| Prerequisite knowledge | The AWS S3 wire protocol and semantics, SigV4, HTTP/1.1 and HTTP/2, POSIX filesystem and durability semantics, SQLite/WAL internals, Rust async, and the general shape of the AWS access-control model (IAM-style policies, ACLs). |

---


> Part of the Cairn reference docs. The section numbers below are stable identifiers used throughout the code and docs; see the index in [`CLAUDE.md`](./CLAUDE.md) and [`../CLAUDE.md`](../CLAUDE.md).

## 0. How to read this document

The document is organised in seven parts.

Part I (Sections 1 to 5) sets context: what Cairn is, what it deliberately is not, the concrete engineering reasons it is written in Rust, the baseline storage architecture it builds on, and the catalogue of production gaps a naive implementation has together with the delta Cairn introduces.

Part II (Sections 6 to 10) is the core data-plane architecture: the node model, the concurrency and I/O model, the durability and crash-consistency model, the on-disk storage layout, and transparent compression at rest.

Part III (Sections 11 to 12) covers the metadata store and the internal abstraction layer that keeps the protocol code independent of any particular storage or metadata backend.

Part IV (Sections 13 to 21) is the S3 surface in full: the operation catalogue, authentication, the authorization model (bucket policy, ACL, Block Public Access, Object Ownership), versioning, tagging, CORS, lifecycle, bucket replication, and the end-to-end server-side request lifecycles that tie them together.

Part V (Sections 22 to 24) is the control plane: the management API, the embedded React web UI compiled into the binary, and the command-line interface.

Part VI (Sections 25 to 28) covers cross-cutting concerns: the error model, observability and audit, the security and threat model, and the full configuration surface.

Part VII (Sections 29 to 34) is delivery: the testing and S3-conformance strategy, performance engineering and targets, build and operations, the phased implementation roadmap, the architecture decision log, and the reference appendices (full schema as field tables, the complete configuration table, the full S3 API support matrix, and the catalogues of supported policy actions and condition keys).

If you intend to start building immediately, read Sections 5, 6, 7, 8, 12, and 32 first, in that order. Severity tags used in the gap analysis: **[BLOCKER]** must be solved before that subsystem ships; **[HIGH]** is a correctness, durability, or security issue; **[MED]** is performance or operability; **[LOW]** is polish.

---


## 1. Executive summary

Cairn is a single-binary object storage server that speaks the Amazon S3 API with enough fidelity that unmodified S3 clients, SDKs, and tools work against it without special-casing. It stores object data as plain files on a local POSIX filesystem and keeps all metadata in an embedded SQLite database. Its defining principle is that the metadata database is the single source of truth and object bytes live under opaque identifiers on disk, so that a metadata commit is the one and only linearization point of any mutation. Everything else in the system is built outward from that principle.

Cairn is intended for production. The design goal is that a team running a single-node MinIO, or any single-drive or single-host S3 endpoint, can replace it with Cairn and gain a simpler operational model, a smaller and safer binary, transparent compression, an embedded management UI, and asynchronous bucket replication for redundancy, without losing S3 compatibility. Cairn does not attempt to be a distributed, erasure-coded, multi-node cluster; that is an explicit non-goal (Section 2). Its redundancy and geo-distribution story is asynchronous bucket replication between independent Cairn deployments (or to any S3-compatible destination), in the spirit of S3 Cross-Region Replication, rather than synchronous clustering. Within a node, durability rests on correct fsync ordering plus the operator's choice of resilient storage underneath (RAID, ZFS, or a cloud block device with its own redundancy).

The feature surface is the full practical S3 control set: object operations (including multipart and ranged, conditional reads and writes, copy, and bulk delete), object versioning with delete markers, object and bucket tagging, per-bucket ACLs and bucket policies with a real policy-evaluation engine, Block Public Access and Object Ownership, per-bucket CORS configuration, per-bucket lifecycle rules with expiration and incomplete-multipart cleanup and optional transition to a remote cold tier, and per-bucket asynchronous replication. Control happens through a JSON management API that is consumed both by an embedded React single-page application compiled directly into the server binary and by a first-class command-line interface, so an operator manages Cairn from a browser or a terminal with the same capabilities.

The rewrite is in Rust for specific, defensible engineering reasons elaborated in Section 3: a storage data plane is dominated by disk and network I/O and by tail latency under concurrency, and Rust gives predictable, garbage-collector-free latency, precise control over buffers and copies, safe fearless concurrency for the read, replication, and lifecycle workers, and direct ergonomic access to the fastest kernel I/O primitives (io_uring, sendfile, splice, O_DIRECT, fallocate, fadvise). It is also a network-facing service that parses untrusted input (the SigV4 chunked-upload framing, S3 XML, policy JSON), where Rust's memory safety removes an entire category of remotely-triggerable vulnerabilities at no runtime cost.

The remainder of this document specifies, in depth and without code, how every part of that system is built.

---


## 2. Positioning, scope, and non-goals

### 2.1 What Cairn is

- A **single-node, locally-backed, fully S3-compatible** object storage server. One process, one binary, one data directory plus one database file, serving the S3 API and a management API.
- **Production-capable.** Stability, performance, security hardening, observability, and operational tooling are first-class requirements, not afterthoughts. The yardstick is "can replace a single-node MinIO in production."
- **Native to S3.** Cairn is, in the operator's words, a bridge: it presents the S3 contract on the front and keeps bytes on whatever local storage the operator chooses on the back. Compatibility is verified against real clients and against the de-facto S3 conformance test suite (Section 29), not asserted.
- **Self-contained.** The management web UI is built and embedded into the binary at compile time. There is no separate UI service to deploy. A CLI provides the same control from a terminal and a few node-local operations (bootstrap, integrity check, reconciliation, backup) that do not go through the API.
- **Redundant by replication, not by clustering.** Asynchronous per-bucket replication to peer Cairn deployments or to any S3-compatible target provides cross-host and cross-site copies.

### 2.2 In scope (this is the work)

The following are all in scope and specified in this document. Several were out of scope in the earlier homelab-oriented draft and are now required.

- Full object operations: PUT (including SigV4 streaming `aws-chunked` bodies), GET and HEAD with byte-range and conditional semantics, DELETE, bulk DeleteObjects, CopyObject, and the complete multipart lifecycle.
- **Object versioning**: per-bucket Enabled, Suspended, and Unversioned states; version identifiers; delete markers; listing of versions.
- **Object and bucket tagging**: key-value tags, usable as lifecycle filters and policy conditions.
- **Access control**: per-object and per-bucket **ACLs** (canned and grant-based), per-bucket **bucket policies** with a JSON policy-evaluation engine, **Block Public Access**, and **Object Ownership** (including the modern ACLs-disabled mode).
- **CORS**: per-bucket CORS configuration and correct preflight and actual-request handling.
- **Lifecycle**: per-bucket rules for object expiration, noncurrent-version expiration, aborting incomplete multipart uploads, and optional transition of cold objects to a remote S3 tier.
- **Bucket replication**: per-bucket asynchronous replication rules with prefix and tag filters, delete-marker replication, status tracking, and retries.
- **Transparent compression at rest**: optional, per-bucket, range-friendly block compression that preserves S3 ETag semantics.
- **Embedded React management UI** and a **management CLI**, both over a common management API.
- **Native TLS** (in addition to running behind a terminating proxy), production observability (metrics, tracing, audit log), and a specified backup and restore procedure.

### 2.3 Non-goals (explicitly out)

- **Distributed clustering, consensus, sharding, and erasure coding.** Cairn is one node per deployment. It does not pool drives across machines, does not run a consensus protocol, and does not erasure-code or stripe objects. This is the single biggest line between Cairn and distributed MinIO or Ceph, and it is deliberate: it is what keeps Cairn simple, predictable, and easy to operate. Cross-node redundancy is achieved by bucket replication (Section 20), not by clustering.
- **Within-node erasure coding or RAID.** Drive-level redundancy is the operator's responsibility, delegated to the filesystem or block layer (ZFS, mdraid, hardware RAID, cloud volumes). Cairn guarantees crash-consistent durability of what the kernel acknowledges; it does not reimplement RAID.
- **Synchronous cross-site replication.** Replication is asynchronous and eventually consistent, with observable lag and status. There is no synchronous quorum write across sites.
- **A storage-class hierarchy beyond what lifecycle transition needs.** Cairn exposes the `STANDARD` storage class and supports lifecycle transition to a single configurable remote cold tier; it does not model the full matrix of S3 storage classes.
- **IAM as a standalone product.** Cairn has its own user and credential model and a bucket-policy and ACL engine sufficient for S3 authorization. It is not a general identity provider and does not implement the full AWS IAM surface (roles, STS, federation). Identity-based policy is approximated by the user role model plus bucket policy; the boundaries are stated in Section 15.

### 2.4 Constraints

- Language: Rust, stable toolchain, edition 2021 or 2024. `unsafe` is permitted only in small, isolated, well-commented, test-covered modules where it buys a measured I/O performance gain (for example the zero-copy read path); everywhere else is safe Rust.
- Embedded metadata store: SQLite. The metadata abstraction already carries multiple backends, selectable at runtime via `CAIRN_META_BACKEND`: the bundled-C `rusqlite` engine is the default, with an async embedded `libsql` engine and a pure-Rust SQLite engine (Turso) also selectable (Section 11, Section 33). The operator's stated preference for a Rust-native SQLite is honoured by keeping the metadata backend behind a single interface, so the engine can be swapped without touching protocol code.
- Object data: plain files on a single local POSIX filesystem. The database file, the staging directory, and the blob directory must share one filesystem so that atomic rename works (a cross-device rename fails and would break the commit protocol).
- Platforms: Linux x86_64 and aarch64 for production, including a fully static build for minimal containers. macOS is supported for development.
- Operability: one binary that contains the server, the embedded UI, and the CLI subcommands. A single environment-variable configuration surface (`CAIRN_*` variables); the server has no configuration file and no command-line configuration flags (Section 28). The CLI subcommands take their own flags.

---


## 3. Why a Rust rewrite

This section is the engineering justification, stated concretely rather than as a preference. The claim is narrow and defensible: for the specific workload of an I/O-bound, concurrency-heavy, latency-sensitive, network-facing storage data plane that parses untrusted input, Rust is the better tool, and the advantages compound at production scale. Go is an excellent language; the point is not that Go is bad but that Cairn's goals lean directly into Rust's strengths.

### 3.1 Predictable tail latency without a garbage collector

A storage server is judged on its worst-case latency under load as much as its average, because clients time out and retry on the tail, and retries amplify load precisely when the system is already stressed. A garbage-collected runtime introduces latency that is not caused by the request being served: collector work, write barriers, assists charged to allocating goroutines, and scheduler interaction. Modern Go has a very good low-pause collector, but it is still a source of p99 and p999 jitter that scales with allocation rate and live heap, both of which are high in a service that is constantly allocating and releasing buffers for object bodies. Rust has no garbage collector and no runtime of that kind. Memory is reclaimed deterministically when values go out of scope. The latency a request experiences is the latency of the work that request does, which is exactly the property a storage data plane wants. This is the single most important reason and it is a structural property, not a tuning exercise.

### 3.2 Explicit control over buffers and copies

An object store is, mechanically, a machine for moving bytes between sockets and disk with as few copies as possible. Rust's ownership model and its reference-counted byte buffer types let the implementation express exactly when a buffer is shared, sliced, or handed off, with no hidden copies and no defensive duplication. Slicing a received buffer to feed it simultaneously to a hash function, a compressor, and the disk writer is a cheap reference-count operation rather than a copy. This control is the difference between a data plane that runs at memory and disk bandwidth and one that quietly spends cycles copying.

### 3.3 Memory safety on a hostile attack surface at no runtime cost

Cairn parses untrusted bytes in several places that are classic vulnerability sites: the SigV4 streaming chunk framing, S3 request XML, JSON policy and configuration documents, range headers, and key strings. In a language without memory safety these are where remote code execution and memory-disclosure bugs live. Rust eliminates buffer overflows, use-after-free, and data races at compile time, and it does so without a runtime cost, so the safety does not trade against the performance goal. For a service that will be exposed to the internet behind nothing more than a reverse proxy, removing this entire bug class is a security decision, not just an ergonomic one.

### 3.4 Fearless concurrency for the workers

Cairn runs several concurrent subsystems beyond request handling: a pool of read connections, asynchronous replication workers, a lifecycle scanner, a multipart-cleanup sweeper, and a WAL checkpointer. These share state (the metadata store, metrics, configuration) and must not race. Rust's type system makes data races a compile error rather than a production incident, which means the implementation can use aggressive parallelism with confidence instead of conservatism. This matters more as the feature set grows, because every new background subsystem is another opportunity for a concurrency bug that Rust simply will not compile.

### 3.5 Direct access to the fastest kernel I/O primitives

The performance ceiling of a local-disk object store is set by how efficiently it can talk to the kernel's storage and network stacks. Rust binds these primitives cleanly and they are central to Cairn's I/O model (Section 7): io_uring for batched, low-syscall asynchronous disk and socket I/O; sendfile and splice for zero-copy reads from page cache to socket; O_DIRECT for bypassing the page cache on large sequential transfers where the cache only adds copies; fallocate to reserve space and avoid fragmentation; and posix_fadvise to advise the kernel about access patterns. The data plane is designed so the hot paths can use the best primitive the kernel offers, with the slower portable path as the default and the fast path behind a feature where it requires platform-specific code. A garbage-collected runtime can use these too, but the combination of zero-cost abstraction and explicit lifetimes makes the zero-copy and registered-buffer patterns natural in Rust.

### 3.6 A small, dependency-light, predictable footprint

The result of compiling Cairn is a single static binary with no virtual machine, no interpreter, and a small, predictable memory footprint. It starts instantly, runs in a `scratch` or distroless container, and is dense to deploy. The mature async networking stack it builds on (the same stack used by high-throughput production systems and by AWS's own Rust SDK) is proven at scale. For an operator, the deployment artifact is one file that contains the server, its management UI, and its CLI.

### 3.7 The honest counterpoints

Rust costs more in compile time and in initial development effort, the async file-I/O story on stable Rust is less automatic than Go's transparently-blocking goroutine model and requires deliberate handling (Section 7), and the ecosystem, while strong, has fewer turn-key S3-server building blocks than one might wish, so Cairn implements more itself. These costs are real and are accepted because the resulting system's runtime properties match the goals. None of them affect the operator or the end user; they are paid once, by the implementers.

---


## 4. Baseline storage architecture

This section records the reference system at the level of detail needed to design Cairn, so that the implementer does not need the Go source. It is descriptive, not prescriptive; the prescriptive design is Parts II onward.

### 4.1 Shape

The baseline storage design is one process with one HTTP listener serving four route families: the S3 API at the root, a management JSON API under a management path prefix, a loopback-only first-start bootstrap endpoint, and unauthenticated signed public-read URLs under a public path prefix, plus liveness and readiness endpoints. It depends on a small set of libraries: a router, a CORS helper, a UUID generator, and an embedded SQLite. There is no external database or cache.

### 4.2 The storage model (inherited wholesale)

Object bytes are stored at paths derived from a freshly generated UUID, never from the object key. An object is a metadata row recording its bucket, key, size, ETag, content type, timestamps, and an opaque storage path of the form bucket-over-uuid. On disk there is a staging directory for in-progress writes and per-bucket directories holding committed blobs under their UUID names. Three properties follow, and Cairn keeps all three: the metadata insert is the commit point and a blob is live only when a row references it; two concurrent writes to the same key cannot corrupt each other because they write different UUID files and the unique constraint on bucket-and-key arbitrates the winner at commit; and because the key never becomes a filesystem path, key-based path traversal is structurally impossible at the storage layer.

### 4.3 Write, read, multipart, listing, auth (the baseline design)

A single-part write validates the bucket and key, computes content hashes while streaming the body through a tee into a staged temp file, fsyncs and renames the temp file into the bucket directory, validates the computed hashes against any client-supplied checksums, upserts the metadata row as the commit point, and then deletes any superseded blob. A read looks the row up, opens the blob, and serves it with a helper that provides range, conditional, and last-modified handling and, on Linux, a sendfile zero-copy path. Multipart staging writes each part to a per-upload staging area under a fresh UUID, records part metadata, and on completion validates the parts, assembles them into a new blob by sequential copy, computes the multipart ETag as the MD5 of the concatenated per-part MD5 digests with a part-count suffix, and atomically upserts the object and removes the upload session. Listing uses a key range with an optional prefix predicate and a result limit one larger than requested to detect truncation, and a delimiter variant groups keys into common prefixes using a SQL common-table expression. Authentication is an ordered chain trying a development bypass, then a Bearer scheme, then SigV4 in header form, then SigV4 in presigned-query form, yielding a principal carrying a user identity and a role.

### 4.4 What we inherit and keep

Cairn preserves: the UUID-blob storage model and metadata-as-truth; the temp-then-rename atomic commit; the startup reconciliation that reclaims orphaned blobs; the correct multipart ETag formula; the parameterized prefix predicate that avoids the classic wildcard-escaping bug; constant-time signature comparison; defense-in-depth key validation with a final within-root check; and hashing of Bearer secrets at rest. These are the parts the baseline design gets right and they need no redesign.

---


## 5. Gap and flaw analysis, and the delta to Cairn

This is the bridge from the baseline to the production system. Each finding states a baseline gap or a new production requirement, its impact, and the decision Cairn takes. Durability/correctness findings use the F-series identifiers; findings introduced by the expanded production scope use the N-series.

### 5.1 Durability and crash consistency

**F-1 [HIGH] No directory fsync after rename.** A naive temp-then-rename fsyncs the staged file but not the parent directory after renaming it in. On POSIX, the rename is only durable once the containing directory is fsynced, so a power loss can lose a blob whose metadata row already committed, producing a dangling reference that reconciliation does not repair. **Cairn:** fsync the destination directory after every rename into a final location, as a single mandatory step on every commit path.

**F-2 [HIGH] Blob durability is not ordered before metadata visibility.** The safe invariant is that a committed row never references a non-durable blob. **Cairn:** fix the commit sequence so blob data and its directory entry are both fsynced before the metadata transaction begins (Section 8).

**F-3 [MED] SQLite synchronous mode and checkpointing are implicit.** **Cairn:** make the synchronous mode a documented knob defaulting to the safe setting, and run an explicit, observable WAL checkpoint task (Section 8, Section 11).

**F-4 [MED] The crash window is asserted but untested.** **Cairn:** a fault-injection seam and tests that kill the process inside the dangerous window and assert post-restart consistency (Section 29).

**N-1 [HIGH] Single-node durability expectations must be stated and the operator guided.** A production user replacing MinIO needs to know exactly what Cairn guarantees on a single node and what it delegates to the storage layer. **Cairn:** document that Cairn guarantees crash-consistent durability of kernel-acknowledged writes and delegates drive redundancy to the filesystem or block layer, with explicit guidance (Section 8, Section 31), and that cross-host durability comes from bucket replication (Section 20).

### 5.2 S3 protocol correctness and completeness

**F-5 [BLOCKER] SigV4 streaming `aws-chunked` uploads are not handled and corrupt objects.** A body sent with the streaming-payload content-sha256 sentinel is a framed chunk stream, not raw bytes; storing it verbatim corrupts the object and the ETag. Several SDKs use this by default in common configurations. **Cairn:** a streaming chunk decoder in the ingest path that de-frames the body, optionally verifies the rolling per-chunk signature chain, and feeds only payload bytes onward (Section 21, Section 14). Treated as a blocker.

**F-6 [MED] No conditional writes.** **Cairn:** support If-Match and If-None-Match on writes, evaluated inside the commit transaction so check and mutation are atomic (Section 21).

**F-7 [LOW] Listing edge cases under delimiter are not exhaustively tested.** **Cairn:** property tests against a reference oracle (Section 29).

**N-2 [HIGH] The full S3 control surface is required.** A baseline implementation provides core object and bucket operations only. Cairn must additionally implement versioning, tagging, ACLs, bucket policies, Block Public Access, Object Ownership, CORS configuration, lifecycle, and bucket replication, each to a fidelity that passes conformance. **Cairn:** Sections 15 to 20 specify each subsystem; Section 13 catalogues the operations; Section 34 lists the full matrix.

**N-3 [HIGH] Authorization must be a real engine, not a role check.** With bucket policies and ACLs in scope, request authorization becomes a multi-source decision (identity and role, bucket policy, object and bucket ACL, Block Public Access, Object Ownership) with a defined precedence. **Cairn:** a single authorization pipeline with an explicit evaluation order modelled on AWS semantics (Section 15).

### 5.3 Scalability, memory, and write throughput

**F-8 [HIGH] Several paths load an entire bucket into memory.** Reconciliation, empty-bucket, and bulk operations enumerate without bound, risking OOM and long stalls at scale. **Cairn:** every internal enumeration is paged and streamed in fixed-size batches; no path holds more than a batch in memory (Section 11, Section 21).

**F-9 [MED] Reconciliation is an unconditional full filesystem walk on every start.** **Cairn:** keep the full walk as a bounded, parallel, observable backstop with a flag to defer it, and rely on a lazy per-read check as the always-on safety net (Section 8, Section 11).

**F-10 [MED] The metadata cache is a single global mutex with clone-on-read.** **Cairn:** a sharded concurrent cache holding reference-counted values so hits are refcount bumps, not deep copies (Section 11).

**F-11 [MED] The listing query cannot use the key index as a range seek.** A prefix applied as a per-row function predicate forces a scan. **Cairn:** rewrite listing as a half-open range on the indexed key column using a computed prefix upper bound, so the index seeks directly and stops early (Section 11).

**N-4 [HIGH] The single SQLite writer is a production write-throughput ceiling and must be engineered, not merely accepted.** A naive one-transaction-per-request writer caps small-object write rate at the fsync rate of the device. For production this is too low. **Cairn:** the writer task performs **group commit**, coalescing many queued mutations arriving within a small window into a single transaction and a single durability barrier, which raises sustained small-write throughput by roughly the batch factor while preserving per-request durability acknowledgement (Section 7, Section 11). This is the key write-path performance decision.

### 5.4 Concurrency, runtime, and I/O

**F-12 [HIGH, design] Go's goroutine-per-request blocking model does not translate directly.** A naive port that wraps every database and file call in the shared blocking pool suffers head-of-line blocking and ignores SQLite's one-writer-many-readers reality. **Cairn:** a runtime and concurrency model designed for the real constraints (Section 7): one serialized, group-committing writer; a pool of read-only WAL connections; and blob I/O isolated to a sized blocking pool with an io_uring fast path behind a feature.

**F-13 [MED] Async file I/O is not free in Rust.** Stable Tokio file I/O runs on a blocking pool. **Cairn:** isolate and bound that pool, stream with backpressure, and offer io_uring for the data plane (Section 7).

**F-14 [MED -> elevated] Zero-copy reads are not free in Rust, and for production they matter.** The earlier draft accepted a userspace copy because it assumed a proxy and a LAN. For a production MinIO replacement that may serve TLS itself and large objects at high rate, the read path should be able to avoid the copy. **Cairn:** a zero-copy read fast path using sendfile or splice, and awareness of kernel-TLS so that even TLS-terminated reads can stay zero-copy where the platform supports it, with a portable buffered streaming path as the default (Section 7, Section 21).

### 5.5 Security

**F-15 [HIGH] SigV4 secrets are stored in plaintext.** **Cairn:** envelope-encrypt SigV4 secrets at rest under a master key; keep Bearer secrets hashed; keep plaintext only transiently in memory in a zeroizing container (Section 27).

**F-16 [MED] No request-size limits, quotas, or graceful disk-full.** **Cairn:** configurable maximum object size with early rejection and a streaming ceiling, optional per-bucket and per-user byte quotas enforced in the commit transaction, mapping of out-of-space to the correct status, and concurrency and timeout limits (Section 27, Section 28).

**F-17 [LOW] The development auth bypass should be impossible to ship.** **Cairn:** compile the bypass out of release builds behind a feature, with a runtime loopback guard as a second layer (Section 14, Section 27).

**N-5 [HIGH] Public exposure and Block Public Access.** A production store must let an operator guarantee that nothing is inadvertently public. **Cairn:** implement Block Public Access at the account and bucket level, evaluated ahead of ACL and policy public grants, so a single setting can override any public access (Section 15).

**N-6 [MED] Native TLS.** **Cairn:** optional built-in TLS using a Rust TLS stack, in addition to running behind a terminating proxy, so a deployment needs no proxy to be secure on the wire (Section 7, Section 27, Section 31).

### 5.6 Operability and experience

**F-18 [HIGH] No metrics.** **Cairn:** a Prometheus metrics endpoint and structured tracing as a shipping requirement (Section 26).

**F-19 [MED] Backup is undefined.** **Cairn:** a specified, tested backup and restore procedure with its consistency argument and the one residual edge case and its mitigation (Section 31).

**F-20 [LOW] Shutdown does not drain or checkpoint deterministically.** **Cairn:** ordered graceful shutdown that drains in-flight work, flushes the writer, checkpoints, and stops background tasks (Section 11, Section 31).

**N-7 [MED] An embedded management UI and a CLI are required.** **Cairn:** a React single-page application built and embedded into the binary at compile time and served by the server, plus a CLI, both over one management API (Sections 22 to 24).

**N-8 [MED] Transparent compression at rest is required, without breaking S3 semantics or ranges.** **Cairn:** optional per-bucket block compression with a stored block index that keeps range reads efficient and preserves the plaintext-based ETag (Section 10).

### 5.7 Architecture and testability

**F-21 [MED] No abstraction boundary between engine and backends.** **Cairn:** the protocol and control layers depend only on a set of internal interfaces (blob store, metadata store, authenticator, policy evaluator, replication sink), so backends are swappable and the engine is unit-testable with in-memory doubles (Section 12).

**F-22 [LOW] String-based error handling.** **Cairn:** typed domain errors per module and one total translator to S3 and JSON error responses (Section 25).

### 5.8 Consolidated delta

The net of the above: Cairn keeps the baseline storage model and its correct instincts, fixes the durability and memory and concurrency weaknesses of a naive implementation, adds the entire production S3 control surface (versioning, tagging, ACL, policy, BPA, ownership, CORS, lifecycle, replication), engineers the write path for throughput via group commit, engineers the read path for zero-copy, adds transparent compression, adds native TLS and a full security posture, adds metrics and audit and a tested backup story, and ships an embedded UI and a CLI. The subsequent parts specify each of these.

---



