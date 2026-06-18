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

# Part II. Core data-plane architecture

## 6. System overview: data plane, control plane, node model

### 6.1 The node model

A Cairn deployment is one process on one host owning one data filesystem. That process is the entirety of the system for that deployment: there is no coordinator, no metadata service, no separate gateway. Multiple Cairn deployments relate to each other only through asynchronous bucket replication (Section 20), which is an S3 client relationship, not a cluster membership. This is the deliberate simplicity at the heart of the design (Section 2.3). The unit of scaling is the host: a bigger host, faster disks, and more network serve more load; cross-host capacity and redundancy come from running more independent deployments and replicating buckets between them.

### 6.2 Data plane and control plane

Within the process, two logical planes share the same address space and the same metadata store but are reasoned about separately.

The **data plane** is the S3 request path: accept a connection, parse and authenticate and authorize the request, and move object bytes between the socket and the disk, touching metadata at the commit point. It is latency- and throughput-critical and is where the I/O model (Section 7) and the durability model (Section 8) live.

The **control plane** is everything that configures and observes the system: the management API and its two clients (the embedded UI and the CLI), the background subsystems (lifecycle scanner, replication workers, multipart sweeper, WAL checkpointer, metrics refresher), and bootstrap. The control plane is not on the object hot path; it values correctness, observability, and clear operator semantics over raw throughput.

### 6.3 The request path, end to end

Every S3 request traverses the same outer pipeline before reaching an operation handler. A connection is accepted by the async HTTP server, optionally over built-in TLS (Section 7.7) or behind a terminating proxy. A middleware stack, ordered deliberately, applies: assignment of a request identifier and the opening of a tracing span; structured access logging; a global concurrency limit and a request timeout; a request-body size guard; and CORS handling, which for Cairn is per-bucket and therefore partly deferred into the handler (Section 18). The request is then routed by host and path and query string into one of the four families. For S3 requests, authentication (Section 14) establishes the principal, and authorization (Section 15) evaluates the combined decision of Block Public Access, bucket policy, ACL, and ownership for the specific action and resource. Only then does the operation handler run, depending solely on the internal interfaces (Section 12) rather than on concrete storage. The handler's result is rendered to the wire as S3 XML or, for the management API, JSON, with errors passed through the single error translator (Section 25).

### 6.4 The subsystems at a glance

The process hosts, besides request handling: the metadata writer task and the read-connection pool (Section 11); the blob I/O facility (Section 9); the replication engine and its worker pool (Section 20); the lifecycle scanner (Section 19); the multipart-upload sweeper; the WAL checkpointer; and the metrics and audit facilities (Section 26). Startup wires these, runs reconciliation (Section 8), and only then binds the listener. Shutdown reverses this in a defined order (Section 31).

---

## 7. Concurrency, runtime, and the I/O model

This section is the heart of the performance design and is specified with care, because the operator's premise is that Rust is chosen precisely for its command of hardware and I/O, and that premise is only realised if the I/O model is right.

### 7.1 ADR: a multi-threaded asynchronous runtime, with the data plane able to escalate to io_uring

Cairn runs on a multi-threaded asynchronous runtime (the mainstream Tokio model) as its baseline. This choice is for portability, for the maturity of the HTTP, TLS, and S3-client libraries that build on it, and for the practicality of sharing the metadata writer and the caches across tasks, which a strict thread-per-core shared-nothing runtime would complicate. The cost is that the very lowest-latency disk path and kernel-side batching are not the default. Cairn addresses this by keeping blob I/O behind an interface (Section 12) so that an io_uring-based data-plane implementation can be selected at build time without touching protocol code. The recommendation is to ship the portable runtime first, measure, and adopt io_uring for the blob path where the workload's syscall rate justifies it. The reason io_uring matters for this workload is that it lets many disk and socket operations be submitted and completed with very few syscalls and supports registered buffers and files, which removes per-operation setup cost; for a server doing a high rate of reads and writes this reduces CPU spent in the kernel boundary, which is exactly where a busy storage server spends it.

### 7.2 ADR: the metadata writer is a single, serialized, group-committing task

SQLite permits exactly one writer and many concurrent readers in WAL mode. Cairn models this directly rather than fighting it. All mutations are submitted to one writer task that owns the single write connection. This removes write-write lock contention entirely, because there is only one writer, so the database is never busy for a competing writer and the busy-timeout path effectively never triggers. Serialization is not a limitation to be worked around here; it is the physical reality of the storage engine, made explicit.

On its own, a single writer that does one transaction and one durability barrier per request would cap the small-object write rate at the device's synchronous-commit rate, which is far too low for production. The decisive optimisation is **group commit**. The writer drains its inbound queue opportunistically: it begins one transaction, applies every mutation currently waiting in the queue, commits that transaction once with a single durability barrier, and only then signals completion to every caller whose mutation was in that batch. Under load the queue refills during the previous commit's barrier, so batches form naturally without any artificial delay; under light load batches are size one and latency is minimal. An optional small linger window can be configured to deliberately wait a few hundred microseconds to enlarge batches under bursty load, trading a little latency for more throughput. The throughput ceiling rises from one commit per write to one commit per batch, so effective small-write throughput scales with batch size up to the point where the writer is CPU-bound rather than fsync-bound.

Two correctness details make group commit safe. First, each mutation in the batch is wrapped in its own savepoint within the shared transaction, so a mutation that must logically fail (a failed conditional-write precondition, a unique-constraint conflict surfaced as an S3 error) rolls back only its own effects and returns its own error, while the rest of the batch proceeds and commits. Second, the durability acknowledgement contract is preserved: a caller's await returns success only after the commit that included its mutation has been made durable, so no client is told its write succeeded before that write is on stable storage. Mutations are applied in submission order, so last-writer-wins for concurrent writes to the same key follows submission order deterministically.

### 7.3 Reads: a pool of read-only WAL connections

Reads do not go through the writer. A pool of read-only connections, opened in WAL mode, serves all metadata queries concurrently. WAL readers take a consistent snapshot and never block the writer or each other, so read throughput scales with the pool size and the available cores. The pool size defaults to roughly the core count and is configurable. Listing, get-object-metadata, head, policy and ACL lookups, and the management read endpoints all use this pool. Because reads never contend with the single writer, a read-heavy workload is unaffected by the write-rate ceiling, and a write-heavy workload does not starve reads.

### 7.4 Blob I/O: a bounded facility, streamed and backpressured

Object bytes are handled separately from metadata. On the baseline runtime, file operations execute on a dedicated, bounded blocking pool rather than the runtime's general-purpose blocking pool, so a flood of large transfers cannot exhaust threads needed elsewhere and cannot block the asynchronous reactor that drives request parsing and the network. The size of this pool is tuned to the useful I/O concurrency of the underlying device. Transfers are streamed in bounded chunks with end-to-end backpressure: the rate at which bytes are read from the network is coupled to the rate at which they are written to disk (and vice versa on reads), so a slow disk slows the network read rather than buffering unboundedly in memory, and a slow client slows the disk read rather than reading the whole object into memory. No operation buffers a whole object; memory use per transfer is a small constant.

### 7.5 The write data path

On a write, bytes arrive from the socket as a stream. They pass through a fan-out that simultaneously feeds the content hashers (always the MD5 that becomes the ETag, plus any client-requested checksum algorithms, computed once over the plaintext) and the disk writer, sharing the same buffers by reference rather than copying. When the object's length is known in advance from the content-length header, the staging file is preallocated to that length, which avoids fragmentation, lets the filesystem place the file well, and surfaces an out-of-space condition immediately and cleanly rather than partway through. For large transfers the kernel can be advised that access is sequential and, after the transfer, that the just-written pages are no longer needed, so a stream of large uploads does not evict the page cache that hot reads depend on. Where the deployment opts into it for objects above a size threshold, the staging write can bypass the page cache entirely, which gives more predictable write latency and avoids polluting the cache with write-once bulk data, at the cost of requiring aligned buffers and transfer sizes that the blob facility manages internally.

### 7.6 The read data path and zero-copy

A read that serves a whole object or a contiguous range from an uncompressed blob is, ideally, a transfer from the page cache to the socket with no copy through userspace. Cairn's portable default streams the file to the response body in bounded chunks, which incurs one userspace copy per chunk and is entirely adequate behind a proxy or for modest rates. For high-rate large-object serving, Cairn provides a zero-copy fast path that uses the kernel's file-to-socket transfer (sendfile, or splice through a pipe for finer control), moving bytes from page cache to the socket without entering userspace. This fast path applies only to uncompressed blobs served in plaintext, because compression and userspace TLS both require the bytes to pass through userspace. To keep TLS reads zero-copy as well, Cairn can use kernel-TLS where the platform supports it: the TLS handshake is performed in userspace by the Rust TLS stack, after which the symmetric encryption is offloaded to the kernel, so a file-to-socket transfer feeds the kernel-TLS socket and the kernel encrypts in place. Where kernel-TLS is unavailable, TLS reads fall back to userspace encryption with the attendant copy. The fast path is feature-gated because it involves platform-specific code and a small amount of carefully isolated unsafe; the portable path is always present. Range reads on uncompressed blobs seek to the offset and transfer the requested length; range reads on compressed blobs follow the decompression path in Section 10 and are necessarily served through userspace.

### 7.7 Network front end and TLS

Cairn serves HTTP/1.1 and HTTP/2. It can terminate TLS itself using a Rust TLS stack with modern defaults, reading certificate and key material from configured paths and supporting reload on change, which lets a deployment be secure on the wire with no external proxy. It also runs cleanly behind a terminating reverse proxy, in which case it serves plaintext on a trusted interface and trusts the proxy to pass through the authorization, range, conditional, and S3-specific headers unmodified and to stream rather than buffer large bodies. Both deployment shapes are first-class and documented (Section 31). When TLS is terminated upstream, the zero-copy-with-kernel-TLS consideration of Section 7.6 does not apply to Cairn, since Cairn sees plaintext.

### 7.8 Backpressure, limits, and fairness

A global concurrency limit caps the number of in-flight requests so that overload sheds cleanly rather than collapsing; excess requests wait briefly or are rejected with a retryable status. Per-request timeouts bound how long any single request can hold resources. The bounded blob pool and the streamed, backpressured transfers ensure that a small number of very large transfers cannot monopolise memory or threads. These mechanisms together give the server a defined behaviour at and beyond saturation, which is a production requirement a naive single-node server does not address.

---

## 8. Durability and crash consistency

### 8.1 The guarantee Cairn makes

Cairn guarantees that after any crash, on restart it converges to a state in which every metadata row that is visible references a present, complete, durable blob, and no orphaned blob remains, with no manual intervention. It guarantees that a write acknowledged to a client as successful is durable on the local storage as configured. It does not, by itself, guarantee survival of a drive failure; that is delegated to the storage layer the operator places underneath (Section 8.6), and survival of a host failure is provided by bucket replication (Section 20). Stating this boundary precisely is itself a requirement (N-1), because a production operator must know exactly where Cairn's guarantee ends and theirs begins.

### 8.2 The commit sequence

Every mutating operation follows one ordered sequence, and the order is the durability design.

First, the object's bytes are streamed to a staging file in the staging directory, with hashes computed inline and, where compression is enabled, the framed compressed form and its index trailer produced during the same pass. Second, the staging file is fsynced, which makes its data and inode durable. Third, the staging file is renamed into its final per-bucket directory under its UUID name; rename is atomic within the filesystem, which is why the staging directory must share the filesystem with the data directory. Fourth, and this is the step a naive temp-then-rename omits, the destination directory is fsynced, which makes the rename itself durable; without this the directory entry can be lost on power failure even though the file data is safe. Fifth, the computed hashes are validated against any client-supplied checksums, and on mismatch the blob is deleted and the operation fails with no metadata written. Sixth, the metadata transaction is submitted to the writer and committed; this is the single linearization point, and for conditional writes the precondition is evaluated inside this transaction so the check and the upsert are inseparable. Only after this commit is the operation acknowledged to the client. Seventh, after the commit, any superseded blob is deleted on a best-effort basis and the activity and metrics are recorded.

The invariant this produces is that a committed, visible row never references a blob that is not already durable, because blob durability (steps two through four) strictly precedes metadata commit (step six). A crash between step four and step six leaves a durable blob with no row, which is an orphan that reconciliation reclaims; a crash after step six leaves a consistent state. There is no ordering in which a visible row points at a blob that the filesystem has not promised to keep.

### 8.3 Durability and group commit

Group commit (Section 7.2) does not weaken this. The single durability barrier of a batch covers every mutation in that batch, and each caller is acknowledged only after that barrier completes, so the per-request durability contract holds for every member of the batch. The blob durability steps for each write happen before that write's mutation is submitted to the writer, so by the time a mutation is in a batch its blob is already durable irrespective of when the batch commits.

### 8.4 SQLite durability settings

The metadata database's own durability is governed by its synchronous setting. Cairn defaults to the fully-synchronous setting, under which a committed transaction is durable against power loss, and exposes a relaxed setting as a documented throughput option under which the last few committed transactions may be lost on power loss without database corruption. The write-ahead log is checkpointed by a background task on an interval and when it exceeds a size threshold, using a truncating checkpoint so the log does not grow without bound under sustained writes; checkpoint runs and the log size are observable as metrics so an operator can see whether long-lived readers are starving the checkpointer.

### 8.5 Reconciliation as the recovery and integrity mechanism

Reconciliation reconciles the on-disk blobs against the metadata. It exists to reclaim orphaned blobs left by crashes in the step-four-to-six window and to detect, as an integrity check, any divergence. It is engineered to be bounded in memory and time (F-8, F-9): it walks the per-bucket directories and, for each blob it encounters, checks membership against the metadata in fixed-size batches rather than loading the keyspace, deleting any blob and its index trailer that no row references and any staging artifact older than a safety margin and any multipart staging directory whose upload session no longer exists. It prunes emptied directories. It runs with bounded parallelism across buckets, logs progress, and reports counts as metrics. The full walk runs at startup before the listener binds by default, and can be deferred by configuration on very large stores in favour of running it out of band, in which case a lazy per-read integrity check remains the always-on safety net: a read whose blob is unexpectedly missing returns a clear error, emits a metric, and flags the row for repair. A repair mode of reconciliation can additionally drop rows whose blobs are missing, which is needed only for recovery from external damage or from a backup taken in the narrow window described in Section 31.4.

### 8.6 Single-node durability guidance for operators

Because Cairn does not implement drive redundancy, its single-node durability is exactly the durability of the storage beneath it. The guidance, stated in the operations section, is to place the data filesystem on redundant storage: a checksumming, redundant filesystem such as ZFS gives both redundancy and silent-corruption detection, software or hardware RAID gives redundancy, and a cloud block volume gives provider-level redundancy. Cairn's contribution is that it never lies about durability: it acknowledges a write only when the kernel has acknowledged the underlying writes per the configured synchronous settings, so whatever guarantee the storage layer provides is faithfully reflected to the client, and bit-rot in the stored bytes can be detected by the stored content hash (Section 26 notes an optional background scrub that re-verifies blobs against their recorded hashes).

---

## 9. On-disk storage model and layout

### 9.1 Principles

Object bytes live as files named by an opaque identifier, never by key; metadata is the source of truth; a blob is live only when referenced. Versioning, tagging, ACLs, and the rest are metadata concerns and do not change the fundamental on-disk shape: a version of an object is simply another row referencing another blob, and a delete marker is a row with no blob at all. This is why the storage model scales to the full feature set without a redesign: features accrete in metadata, not on disk.

### 9.2 Directory layout

The data filesystem holds a staging area for in-progress single-part writes, a multipart staging area organised per upload session, and one directory per bucket holding that bucket's committed blobs under their opaque identifiers. The database file lives on the same filesystem. The staging areas exist so that the only way a blob enters a bucket directory is by atomic rename after being fully written and fsynced, which is the basis of the commit protocol. Per-bucket directories keep the number of entries per directory bounded by the bucket's object count rather than by the whole store, and very large buckets can be sharded across subdirectories by a prefix of the identifier if a filesystem's per-directory scaling warrants it, which is an internal layout decision invisible to clients.

### 9.3 The blob file format

An uncompressed object is stored as exactly its bytes, with no header and no framing, so the simplicity and the byte-for-byte promise are preserved for the default case and an operator can read a blob directly if they ever need to. A compressed object is stored in a self-describing format: a sequence of independently-compressed logical blocks followed by an index that records, for each block, its size and offset, followed by a fixed trailer that records the index's location, the block size, the compression algorithm, the logical (plaintext) length, and a magic marker. This makes a compressed blob self-contained: a reader opens it, reads the trailer, reads the index, and can then seek to and decompress only the blocks it needs. Reconciliation and backup treat the whole file as one artifact; there is no sidecar to keep in sync. Whether a blob is compressed and which algorithm it uses is also recorded in the metadata row, so the reader knows the format before opening the file, and the trailer is the authoritative description for reading. Section 10 specifies the compression scheme in full.

### 9.4 Storage paths, versions, and delete markers

A storage path identifies a blob within the data directory and is recorded on the object-version row. Each distinct version of a key has its own row and its own blob under its own identifier; overwriting a key in a versioning-enabled bucket creates a new row and a new blob and leaves the previous version's row and blob untouched, which is what makes versioning cheap and safe under the UUID model. A delete marker is a version row carrying a flag and no storage path, representing a logical deletion that hides older versions from a plain GET while leaving them retrievable by version identifier. Permanent deletion of a specific version removes its row and reclaims its blob through the normal post-commit reclamation path.

### 9.5 Multipart staging

Each multipart upload session has a staging directory holding its parts, each part written under a fresh identifier so that re-uploading a part number does not clobber the previous attempt; the superseded part file is reclaimed at completion or abort. Assembly streams the ordered parts into a single committed blob through the same durable commit sequence as a single-part write, applying compression during the assembly pass if the bucket enables it. Part files and their staging directory are removed after the completion transaction commits, and the multipart sweeper removes the staging directories of sessions that exceed their lifetime or are aborted.

---

## 10. Transparent compression at rest

### 10.1 Goals and the tension with simplicity

Compression saves disk and, for compressible data, can save I/O time because fewer bytes are read and written. It is in scope by operator request. It is in tension with the byte-for-byte simplicity that is one of Cairn's selling points, and the design resolves the tension by making compression opt-in per bucket and off by default. A bucket with compression disabled stores blobs exactly as received, preserving the simple model; a bucket with compression enabled gains the space saving while Cairn hides the compression entirely behind the S3 contract, so clients neither know nor need to know that bytes are compressed on disk.

### 10.2 Preserving S3 semantics

Two S3 semantics must survive compression. The object's reported size is its logical, plaintext length, which is what a client wrote and what range arithmetic is computed against; the physical on-disk size is separate and is exposed only to the operator through metrics and the management API. The object's ETag must remain what S3 clients expect: for a single-part object it is the MD5 of the plaintext content, computed during ingest over the uncompressed bytes before or as they are compressed; for a multipart object it is the MD5 of the concatenated per-part plaintext MD5 digests with the part-count suffix, where each part's MD5 is computed over that part's plaintext as it is uploaded. Compression therefore never enters the ETag computation, and an object's ETag is identical whether or not the bucket compresses. Client-supplied checksums are likewise validated against the plaintext.

### 10.3 The block scheme and why it is block-based

Compression is applied per fixed-size logical block rather than as one stream over the whole object, and this is the central design choice that keeps ranged reads efficient. If an object were one compressed stream, serving a range that begins near the end of a large object would require decompressing everything before it, turning a cheap range read into a full-object decompression. By compressing independent blocks of a fixed logical size and recording each compressed block's location in the index trailer (Section 9.3), Cairn can serve a range by reading and decompressing only the blocks that overlap the requested range and then slicing to the exact bounds, so the cost of a ranged read is proportional to the range plus at most one block of overhead, not to the offset. The block scheme also makes decompression parallelisable across blocks for large reads and keeps per-block memory bounded.

### 10.4 Algorithm choice and the incompressibility heuristic

The default algorithm balances ratio and speed and is the modern general-purpose choice; a faster, lower-ratio algorithm is available for throughput-sensitive deployments, and compression can be off even within an otherwise compression-enabled policy. The algorithm and level are part of the per-bucket compression policy. Compressing already-compressed or incompressible data wastes CPU and can slightly enlarge the data, so Cairn applies a heuristic: object content types that are known to be already compressed, such as common image, video, audio, and archive formats, are stored uncompressed regardless of the policy, and for other content the first block is test-compressed and, if it fails to shrink beyond a threshold, the object is stored uncompressed. This keeps compression from ever hurting, at the cost of a small test on ingest. The decision per object is recorded so reads know the truth.

### 10.5 Interaction with the write, read, and multipart paths

On a single-part write to a compressing bucket, the ingest pass computes the plaintext MD5 and any requested checksums and simultaneously feeds the block compressor, producing the framed blob and its index in one streaming pass with bounded memory, after which the normal durable commit sequence applies to the framed file. On a read, an uncompressed blob takes the ordinary and possibly zero-copy path, while a compressed blob is read by consulting its trailer and index and decompressing the needed blocks through userspace, which is why compressed objects do not use the zero-copy fast path; for a full-object read this is a streaming decompression with bounded memory, and for a ranged read it is the block-selective path of Section 10.3. On multipart completion to a compressing bucket, compression is applied during the assembly pass that concatenates the parts, while the part MD5s used for the ETag were already computed over plaintext at upload time, so the multipart ETag is unaffected. Copy operations that change nothing about the bytes can copy the stored representation directly when source and destination compression policies match, and otherwise decompress and recompress as needed.

### 10.6 Operability of compression

The space saved is observable: the management API and metrics expose logical versus physical bytes per bucket and overall, so an operator can see the compression ratio being achieved and decide whether a bucket's policy is worthwhile. Changing a bucket's compression policy affects only objects written after the change; existing objects keep their stored form and remain readable because each blob is self-describing, and a deliberate rewrite, which a lifecycle action or an administrative tool can perform, is required to recompress existing data. This keeps policy changes cheap and safe.

---

# Part III. Metadata and abstractions

## 11. Metadata store: topology, schema, and behaviour

### 11.1 Role and engine

The metadata store is the source of truth for every fact about the system that is not the object bytes themselves: users and credentials, buckets and their configuration, object versions and their attributes, multipart sessions, tags, the replication queue, and the activity log. It is an embedded SQLite database in write-ahead-log mode. SQLite is chosen because it is a single file with no separate process to operate, because its WAL concurrency model maps exactly onto Cairn's one-writer-many-readers design, and because the relational query surface is genuinely useful for the access patterns here, particularly ordered prefix-range listing, which is awkward over a bare key-value store. The operator's interest in a Rust-native SQLite is honoured by keeping the entire store behind the metadata interface of Section 12; the engine underneath is already swappable, with `sqlite` (the default, bundled-C `rusqlite`), `libsql` (an async embedded engine), and `turso` (the pure-Rust SQLite rewrite) all backing the same `MetadataStore` interface and chosen by `CAIRN_META_BACKEND`, without any change to protocol or control code.

### 11.2 Connection topology

The store opens one write connection, owned by the single group-committing writer task (Section 7.2), and a pool of read-only connections for concurrent snapshot reads (Section 7.3). On open, each connection is configured for write-ahead logging, foreign-key enforcement, the chosen synchronous level, a busy timeout as defense in depth even though the single-writer design makes contention rare, and memory-mapping and cache sizing tuned to the working set so that reads touch the kernel as little as possible. Migrations run on the write connection at startup, before any request is served, and are recorded so they apply exactly once and in order. The WAL checkpointer (Section 8.4) runs against the write connection on its schedule.

### 11.3 Entities

The schema is specified field by field in Appendix 34.1; this section describes the entities and the design intent behind them.

**Users** hold an identity, a display name, a role that is administrator or member, an active flag, the Bearer access-key identifier and a hash of its secret, and optionally a SigV4 access-key identifier and its secret stored as ciphertext plus a nonce under envelope encryption (Section 27). The two credential schemes coexist per user.

**Buckets** hold a name as primary key, an owner, a creation time, the versioning state which is unversioned or enabled or suspended, the object-ownership mode which governs whether ACLs are in force, and a region label returned by the location operation. A bucket's richer configuration is held in associated records, one logical document each, for its policy, its access-control list, its CORS rules, its lifecycle rules, its replication rules, its stored replication targets (the remote destination descriptors, with credentials sealed under the master key), its default-encryption setting, its tag set, and its public-access-block settings. These documents arrive from the S3 API as XML or JSON, are validated and stored, and are read back both by the corresponding get operations and by the request pipeline during authorization and CORS and lifecycle and replication processing. Account-wide public-access-block settings exist as a singleton alongside the per-bucket ones.

**Object versions** are the core of the store. Each row represents one version of one key in one bucket and holds its version identifier, flags for whether it is the latest version and whether it is a delete marker, the logical size, the physical on-disk size, the ETag, the content type and the other system response headers it round-trips (content-encoding, cache-control, content-disposition, content-language, expires), the storage path, the compression descriptor recording algorithm and block size or indicating that the blob is uncompressed, the storage class, the owner, any client-supplied checksums, the set of user metadata entries that S3 carries as user-defined headers, the per-object access-control list where ownership mode permits object ACLs, the replication status for buckets with replication enabled, and timestamps. The unique identity of a row is the combination of bucket, key, and version identifier; a sentinel version identifier represents the single version of an object in an unversioned or suspended bucket. The latest-version flag and an index over bucket and key and version ordering let a plain GET find the current version efficiently and let version listing enumerate in the order S3 specifies.

**Object tags** are held as a small set of key-value pairs associated with an object version, stored so they can be both returned by the tagging operations and matched by lifecycle filters and policy conditions.

**Multipart uploads and their parts** hold the session's bucket and key and content type and status and owner and intended ACL and user metadata, and for each part its number, plaintext size, plaintext MD5 used for the eventual ETag, storage path, and any checksum, with the part identified by the session and the part number.

**The replication outbox** is the durable queue that drives asynchronous replication: each entry records the bucket, key, and version it concerns, the operation, the destination rule, the number of attempts, the next attempt time, the status, and the last error, so that replication survives restarts and retries with backoff (Section 20).

**The activity and audit log** records mutating actions with the actor, the resource, and the salient attributes, for the management activity view and for security audit (Section 26).

### 11.4 Query shapes that matter

Two query patterns are performance-critical and are designed explicitly. Listing the current objects of a bucket under a prefix is expressed as a half-open range over the indexed key column, seeking directly to the greater of the continuation point and the prefix and stopping at the computed upper bound of the prefix, with the latest-version and not-a-delete-marker conditions applied, and a limit one larger than the page size to detect truncation; this lets the index seek and stop early rather than scanning and filtering (F-11). Listing object versions is the same range seek without the latest-only condition, ordered by key and then by version recency as S3 requires. Reconciliation and empty-bucket enumerate storage paths in bounded pages over the same index, never materialising a whole bucket (F-8). The helpers that compute an exclusive successor of a continuation key and the upper bound of a prefix operate on the byte representation of keys and are unit-tested against empty, maximal, and multibyte inputs, because their correctness is the correctness of listing and of pagination.

### 11.5 The metadata cache

A concurrent, sharded, size-bounded cache fronts the store for hot reads of bucket metadata and of the small configuration documents consulted on every request, such as a bucket's policy and ACL and CORS and public-access-block settings. Object-version metadata is intentionally not cached; current-version, version, and listing reads go straight through to the read-connection pool. The cache holds reference-counted values, so a hit returns a cheap shared handle rather than copying the record, which removes both the global-lock contention and the per-hit allocation churn of a naive global cache (F-10). It is sized by an approximate byte budget and can be disabled. After any write transaction commits, the writer invalidates the specific entries the transaction affected, and bucket-configuration changes invalidate the relevant configuration entries so the next request sees the new policy or ACL immediately; because the cache holds only hot entries rather than the whole keyspace, even invalidating all object entries of a deleted bucket is bounded work. The cache is a decorator over the store interface, so it composes with any metadata backend and can be tested independently.

### 11.6 Transactions, group commit, and conditional logic

Every operation that constitutes a commit point, namely putting an object version, completing a multipart upload, deleting a version, and the configuration mutations, is a single transaction. Under group commit these single transactions are batched into one physical transaction with a savepoint per operation (Section 7.2), so a conditional write whose precondition fails, or a mutation that violates a uniqueness or foreign-key constraint, rolls back only its own savepoint and returns its own typed error while its batch-mates commit. The precondition for a conditional write is evaluated by reading the current state and deciding within the same savepoint that performs the upsert, so the decision and the change are atomic with respect to every other writer, which is the property conditional writes exist to provide.

---

## 12. The internal abstraction layer

Cairn's protocol and control layers are written against a small set of internal interfaces and never against concrete storage, database, or cryptographic implementations. This is the structural decision that makes the engine testable with fast in-memory doubles and makes backends swappable, and it is specified here interface by interface, in terms of the operations each exposes and the semantics and invariants each guarantees. No code is given; an implementer realises these as the language's interface mechanism of the day.

### 12.1 The blob store interface

The blob store owns object bytes on some medium and knows nothing of S3, identity, or metadata. It exposes the ability to stage a single object by consuming a byte stream, during which it computes the plaintext MD5 that becomes the ETag and any additional checksum algorithms the caller requests, applies the bucket's compression policy producing the self-describing blob format when compression is selected, enforces a hard size ceiling by aborting and cleaning up if exceeded, and performs the durable commit prefix of fsyncing the file and its directory before returning; on return it guarantees the blob is durable and reports the storage path, the logical and physical sizes, and the computed hashes, but it writes no metadata and does not itself verify against client-supplied checksums, which the caller does with the returned hashes. It exposes opening a committed blob for reading as a handle that serves the whole object or a byte range and transparently decompresses a compressed blob, so the protocol layer reads logical bytes without knowing the physical form. It exposes idempotent deletion of a committed blob, treating absence as success. It exposes staging a multipart part, reporting the part's plaintext size and MD5 and storage path, and assembling ordered parts into a single committed blob through the same durable sequence with compression applied during assembly, and idempotent deletion of all of a session's parts. Finally it exposes reconciliation, which must be bounded in memory and which is given batched membership oracles for live blobs and live upload sessions so it can stream the filesystem and reclaim orphans without materialising the keyspace. The default implementation is the local filesystem engine of Sections 7 through 10; an in-memory implementation backs unit tests; future implementations behind the same interface include an io_uring engine and a remote-S3-backed cold tier used by lifecycle transition.

### 12.2 The metadata store interface

The metadata store is the source-of-truth interface and exposes operations grouped by entity, all of whose enumerations are paged and bounded. For buckets it exposes creation, lookup, listing overall and by owner, deletion, and the get and set of each configuration aspect: versioning state, ownership mode, policy, ACL, CORS, lifecycle, replication rules, stored replication targets, default-encryption setting, tag set, and public-access-block settings, plus the account-wide public-access-block singleton. For object versions it exposes getting the current version of a key, getting a specific version, putting a new version as a commit point that returns any superseded blob's storage path for reclamation and that enforces a supplied precondition atomically, creating a delete marker, permanently deleting a specific version returning its blob's storage path, paged listing of current objects under a prefix with optional delimiter grouping, paged listing of all versions, and paged enumeration of storage paths for reconciliation and empty-bucket. For object tags it exposes get, set, and delete. For multipart it exposes creating a session, querying its status, recording a part, listing parts, atomically claiming a session for completion so a double completion is impossible, completing as a commit point that upserts the object version and removes the session in one transaction with a precondition, aborting, and paged enumeration of stale and of all sessions for the sweeper. For replication it exposes enqueuing an outbox entry as part of a write, claiming a batch of due entries for a worker, marking entries done or failed with backoff, and querying an object's replication status. For users it exposes lookup by Bearer key returning the stored hash, lookup by SigV4 key returning the decrypted secret in a zeroizing container, counting, and the create, list, update, and deactivate operations the management API needs. For audit it exposes recording an action and listing recent actions. For metrics it exposes the aggregate counts. The guarantees are that commit-point operations are single transactions, that preconditions are evaluated within them, and that no operation returns an unbounded result set. The default implementation is the SQLite store of Section 11; an in-memory implementation backs unit tests.

### 12.3 The authenticator interface

An authenticator examines a borrowed, library-neutral view of a request, comprising the method, path, query, headers, host, and a means to obtain the body hash where a signature requires it, and returns one of three outcomes: it does not apply, in which case the chain tries the next authenticator; it applies and succeeds, yielding a principal carrying the user identity, display name, access-key identifier, role, and the method by which authentication occurred; or it applies and fails, which denies the request and stops the chain. The implementations are a Bearer authenticator, a SigV4 header authenticator, a SigV4 presigned-query authenticator, and a development authenticator compiled only into development builds. They are composed into an ordered chain whose first applicable outcome decides. This three-valued contract is what lets several schemes coexist cleanly, and it isolates all wire-format and signing detail from the rest of the system.

### 12.4 The authorization engine interface

The authorization engine decides whether a given principal may perform a given action on a given resource in a given request context, returning an allow or a deny together with the reason, and it is a pure function over inputs that the request pipeline fetches for it: the account and bucket public-access-block settings, the bucket policy, the bucket and object ACLs, the bucket's ownership mode, and the request context comprising the source address, whether the transport was secure, the requested prefix or key, relevant object tags, and similar condition inputs. Keeping the engine pure and separate from fetching means its evaluation order and its policy-language semantics are tested in isolation with table-driven cases, which is essential because authorization correctness is both a security property and a conformance property. Section 15 specifies the evaluation order and the supported policy language; this interface is how the rest of the system consumes that logic.

### 12.5 The replication interfaces

Replication is expressed through two interfaces. A replication source is the outbox-consuming engine that the server runs as a worker pool, claiming due entries and driving them to completion. A replication sink abstracts a destination as an S3-compatible client capable of putting an object and its metadata, deleting an object or propagating a delete marker, and reporting success or a retryable or terminal failure. Separating the sink lets the destination be another Cairn, AWS S3, or any compatible endpoint, and lets tests substitute a fake sink that records what would have been replicated and can simulate failures. Section 20 specifies the engine's behaviour.

### 12.6 The cryptography, clock, and public-URL interfaces

A cryptography interface provides the envelope encryption and decryption of SigV4 secrets under the master key, the keyed hashing used for the public-read URL signatures, and the constant-time comparisons used throughout authentication, isolating key handling and algorithm choice. A clock interface provides the current time and is injected wherever time governs behaviour, namely signature skew validation, lifecycle expiry, multipart staleness, and replication backoff, so that those behaviours are tested deterministically with a controllable clock rather than by waiting. A public-URL interface provides the signing and verification of Cairn's signed public-read URLs, which are a Cairn extension rather than an S3 feature, computing a keyed signature over the method, the escaped path, and the expiry and verifying it in constant time with an expiry check. Each of these has a production implementation and a test implementation.

### 12.7 Why this layer is worth its cost

The abstraction layer adds indirection, and indirection has a cost in ceremony and sometimes in a virtual call. The cost is justified three times over. It makes the entire engine unit-testable without a disk or a database, so the protocol, authorization, versioning, lifecycle, and replication logic are tested in milliseconds against in-memory doubles, which is the difference between a test suite engineers run constantly and one they avoid. It makes backends swappable, so the io_uring blob engine, the remote cold tier, and a future pure-Rust metadata engine are drop-in rather than rewrites. And it forces clean seams that keep the protocol layer free of storage detail, which is the discipline that keeps the storage model's good instincts intact while everything around them is purpose-built for production. These are the same reasons the boundary is the single most important structural decision in the design (F-21).

---

# Part IV. The S3 surface

## 13. S3 protocol layer and operation catalogue

### 13.1 Dispatch

The S3 protocol layer receives a request that has been authenticated and authorized and routes it to an operation handler by examining the host and path to identify the bucket and key, the HTTP method, and the query string, which in S3 selects among same-path operations through subresource markers, and certain headers, which select variants such as a copy. Bucket-targeted requests with a subresource marker in the query select the get, put, or delete of that subresource, namely the access-control list, the policy, the CORS configuration, the lifecycle configuration, the replication configuration, the tag set, the versioning state, the ownership controls, or the public-access-block settings, as well as the listing variants for objects, object versions, and multipart uploads. Object-targeted requests select put, get, head, or delete, with a copy selected by the presence of a copy-source header, and the multipart operations selected by the upload-id and part-number parameters. Both path-style addressing, where the bucket is the first path segment, and virtual-host-style addressing, where the bucket is a hostname label, are supported, since clients use both and some default to virtual-host style. The handlers depend only on the internal interfaces of Section 12.

### 13.2 Service and bucket operations

At the service level Cairn supports listing the caller's buckets. At the bucket level it supports creating a bucket, deleting an empty bucket, heading a bucket to test existence and access, and returning a bucket's location. It supports getting and putting and deleting each bucket configuration aspect: the access-control list, the policy, the CORS configuration, the lifecycle configuration, the replication configuration, the tag set, the versioning state, the ownership controls, and the public-access-block settings. It supports the three bucket listings: current objects in both the version-two and the older version-one form with prefix and delimiter and pagination, all object versions with prefix and delimiter and pagination, and in-progress multipart uploads.

### 13.3 Object operations

At the object level Cairn supports putting an object, including bodies sent as plain content, as unsigned-payload content, and as SigV4 streaming chunked content, and including the user-defined metadata, content type, content encoding, cache control, content disposition, tag set, canned or explicit ACL, and conditional-write preconditions that S3 carries on a put. It supports getting an object with byte-range and conditional semantics and with response-header overrides, heading an object for its metadata, and deleting an object, which in a versioned bucket creates a delete marker and in an unversioned bucket removes the object. It supports bulk deletion of up to a thousand keys in one request. It supports copying an object from a source, with the option to replace or preserve metadata and with conditional copy preconditions, including the case where source and destination are the same key, which is how S3 clients change metadata in place. It supports the full multipart lifecycle: initiating an upload, uploading a part, uploading a part by copying from an existing object, listing the parts of an upload, completing an upload, and aborting it. It supports getting and putting an object's access-control list where ownership mode permits, getting and putting and deleting an object's tag set, and returning an object's attributes. The complete matrix, including which operations are supported in version one, is in Appendix 34.3.

### 13.4 Wire formats and headers

Request and response bodies for S3 operations are XML, parsed and generated by a streaming XML facility; the management API alone uses JSON. ETags are returned quoted as S3 requires, with the quoting handled in one place. Range responses carry the partial-content status and the content-range header; conditional responses carry the not-modified or precondition-failed status as appropriate; listings carry truncation flags and continuation tokens; and errors carry the S3 error document with a code, a message, the resource, and a request identifier that also appears as a response header and in the trace span for correlation (Section 25). User metadata round-trips through the user-defined header convention, and the standard system headers for content type, content encoding, cache control, content disposition, last modified, and the checksum set are honoured on write and reflected on read.

---

## 14. Authentication

Authentication establishes which user, if any, is making a request, and yields a principal that authorization then evaluates. Cairn supports the SigV4 schemes that S3 clients use and a simpler Bearer scheme for first-party use, composed as the ordered chain of Section 12.3.

### 14.1 SigV4, header form

The signature-version-four header scheme is the primary mechanism because it is what S3 SDKs and tools use. Cairn parses the authorization header into the credential scope, the list of signed headers, and the signature. It checks that the host header is among the signed headers, parses the credential scope into the access-key identifier, the date, the region, and the service, rejecting a service that is not the storage service, and validates the request timestamp against a fifteen-minute skew window and against the scope date. It looks up the user by the SigV4 access-key identifier and obtains the user's secret in decrypted form held in a zeroizing container, denying an inactive user. It determines the payload hash from the content-sha256 header, which is either a literal hash, an unsigned-payload sentinel, or one of the streaming sentinels handled in Section 21. It then reconstructs the canonical request by URI-encoding the path according to the storage service's specific rules, building the canonical query string in sorted form, assembling the canonical headers from the signed set, listing the signed headers, and appending the payload hash; it forms the string to sign from the algorithm, the timestamp, the scope, and the hash of the canonical request; it derives the signing key by the chain of keyed hashes over the secret, the date, the region, the service, and the terminator; and it computes the expected signature and compares it to the presented signature in constant time. A match yields the principal; a mismatch denies.

### 14.2 SigV4, presigned-query form

The presigned-query scheme carries the same elements as query parameters rather than headers and is used for time-limited URLs that embed the credential scope, the signed-headers list, the expiry, and the signature. Cairn validates it analogously, with the signature parameter excluded from the canonical query during reconstruction, the expiry honoured against an upper bound, and a guard against timestamps too far in the future. This is the mechanism behind client-generated presigned GET and PUT URLs, which Cairn supports so that applications can hand out temporary direct access.

### 14.3 SigV4 streaming bodies

When a put carries one of the streaming-payload content-sha256 sentinels, the body is a framed stream of signed or unsigned chunks rather than the raw object, and authentication of the envelope is only part of the story; the body must be de-framed and, for signed streaming, each chunk's signature verified against a rolling chain seeded by the request's seed signature. Cairn provides the verification primitives here in the authentication module, while the de-framing that transforms the body lives in the ingest path (Section 21.7), because the two concerns meet at the body stream. Supporting these sentinels is mandatory, because common client configurations use streaming signing by default and a server that ignores it corrupts objects (F-5).

### 14.4 Bearer form

The Bearer scheme is a simpler first-party mechanism in which the credential is an access-key identifier joined to a secret, presented in the authorization header. Cairn looks up the user by the identifier and compares the hash of the presented secret to the stored hash in constant time, yielding the principal on a match. The secret is stored only as a hash, which is safe because these are high-entropy machine-generated tokens rather than human passwords, so a slow password hash is unnecessary and a fast cryptographic hash suffices; this reasoning is recorded so a reviewer does not mistake it for an oversight.

### 14.5 Development form and the bootstrap

A development authenticator that yields a fixed administrator principal exists solely to ease local development and is compiled only into development builds, and even then refuses to operate unless the listener is bound to a loopback address, so a production binary cannot bypass authentication (F-17). The first-start bootstrap, by which the very first administrator credentials are created into an empty user table, is a node-local command run on the host against the data directory; it refuses to run once any user exists, and prints the generated Bearer and SigV4 secrets exactly once, after which only the hash and the encrypted SigV4 secret remain (Section 24.3).

---

## 15. Authorization model

Authorization decides whether the authenticated principal, or an anonymous requester, may perform the requested action on the requested resource. With bucket policies and ACLs and public-access controls in scope, this is a real decision engine with a defined precedence, specified here, and realised as the pure evaluation interface of Section 12.4 fed by inputs the request pipeline fetches.

### 15.1 The model and its relation to AWS

Cairn implements the S3 authorization concepts that govern access to buckets and objects: resource-based bucket policies, ACLs, Block Public Access, and Object Ownership. It has its own user and credential model rather than full AWS accounts and IAM, but it does support AWS-style identity-based policies attached to a user: where AWS would consult an identity-based IAM policy attached to a principal, Cairn evaluates the user's optional attached identity policy. An administrator is implicitly permitted, subject to an explicit deny in the bucket policy or the user's own identity policy; a member's access to a resource is the union of what the resource's policy and ACL grant and what the member's identity policy grants. This keeps Cairn a storage server rather than a full identity provider, and the boundary is stated so that conformance expectations are realistic: bucket-policy, identity-policy, ACL, and public-access semantics are implemented faithfully, while cross-account principals, IAM roles, temporary credentials, and federation are out of scope (Section 2.3). Principals in a bucket policy are expressed either as the wildcard meaning anyone including anonymous, or as a specific Cairn user identity.

### 15.2 The decision inputs

For each request the pipeline determines the requester class, which is the bucket owner or an administrator, an authenticated member, or an anonymous requester; the action, which is the S3 action the operation maps to; and the resource, which is a bucket or an object. It fetches the account-wide and the bucket public-access-block settings, the bucket policy if any, the bucket ACL and, for an object action, the object ACL, the bucket's ownership mode, and the request context comprising the source address, whether the transport was secure, the requested prefix or key and listing parameters, and the relevant object or request tags. These are the inputs to the evaluation.

### 15.3 The evaluation order

The decision follows a fixed order, and the order is the security model. First, an administrator and the bucket owner are permitted for the bucket's own resources, subject only to an explicit deny in the bucket policy or in the requester's own identity policy, since the owner cannot lock themselves out by omission but can by an explicit deny. Second, Block Public Access is applied as a gate: depending on its four independent settings, public ACL grants are ignored or rejected, public policy grants are rejected, and public buckets are restricted so that only authenticated and authorized principals reach them; if the only thing that would grant the request is a public grant and Block Public Access suppresses public grants, the request is denied here regardless of what the ACL or policy says. Third, an explicit deny in either an applicable bucket-policy statement or the requester's identity policy denies the request unconditionally, because an explicit deny always overrides any allow; this is the cornerstone of the policy model and is what lets an operator write a deny that nothing else can override. Fourth, the request is allowed if any applicable source grants it: a bucket-policy statement whose effect is allow and whose principal, action, resource, and conditions all match; or a matching allow in the requester's identity policy; or, where the ownership mode keeps ACLs in force, an ACL grant of the needed permission to the principal directly or to a group the principal belongs to, where an anonymous requester belongs to the all-users group and any authenticated requester belongs to the authenticated-users group; or the requester being the owner or an administrator as already noted. Identity-policy grants are never public grants, so an identity-policy allow survives Block Public Access. Fifth, if nothing has granted the request, it is denied by default. The net is the familiar rule that an explicit deny beats everything, that otherwise any allow suffices, and that absent any allow the default is denial, with Block Public Access sitting ahead of the public grants it governs.

### 15.4 Object Ownership and the ACLs-disabled mode

The ownership mode of a bucket governs whether ACLs participate at all. In the bucket-owner-enforced mode, which Cairn treats as the recommended default for new buckets and which mirrors the modern S3 default, ACLs are disabled entirely: object ACL operations are rejected, every object is owned by the bucket owner, and access is governed solely by the user model and the bucket policy, which is both simpler and safer because it removes the most common source of accidental exposure. In the other modes ACLs remain in force and object ACLs are honoured, with the modes differing in who owns objects written by a non-owner. Implementing the enforced mode well is important because steering operators toward it is a security benefit, and because a request to set an ACL under the enforced mode must be rejected with the specific error S3 uses so that clients behave correctly.

### 15.5 The bucket-policy language

A bucket policy is a JSON document with a version marker, an optional identifier, and a list of statements. Each statement carries an optional statement identifier, an effect that is allow or deny, a principal or an excluded principal, one or more actions or excluded actions, one or more resources or excluded resources, and an optional set of conditions. A principal is the wildcard for anyone including anonymous or a specific user identity. An action names an S3 action in the storage-service action namespace, with the wildcard and prefix-wildcard forms supported so that a policy can grant a family of actions; the catalogue of recognised actions and the operation each governs is in Appendix 34.4. A resource is expressed in the storage-service resource-name form identifying a bucket or, with a key or prefix and wildcards, a set of objects, so that a statement can scope itself to a prefix. The matching of a statement against a request requires the effect to apply, the principal to match the requester, the action to match the operation's action, the resource to match the target, and every condition to be satisfied; a statement that does not match contributes nothing, and a deny that matches denies. Policies are validated for structure and for resource-name well-formedness on put, and a policy that would lock out the owner is still accepted because the owner-deny capability is intentional, but the management surface warns about it.

### 15.6 Conditions

Conditions refine when a statement matches and are expressed as a set of operators each mapping a condition key to expected values. Cairn supports the string operators for equality and inequality and wildcard match, the boolean operator, the address operators for matching and not matching a source against address ranges, the numeric comparison operators, the date comparison operators, the existence operator, and the qualifier that makes a condition pass when its key is absent, as well as the set qualifiers that require all or any of a multi-valued key to match where feasible. The condition keys it supports include the source address of the request, whether the transport was secure, the referer and user-agent, the current time, the principal type, and the storage-service-specific keys for the listing prefix and delimiter and maximum-keys, the canned-ACL header on a write, the content-sha256 header, the version identifier, and the keys that match against an object's existing tags or a request's supplied tags; the supported keys are catalogued in Appendix 34.5. A condition key that Cairn does not recognise causes the statement not to match, which is the conservative behaviour, so an unknown condition never accidentally grants access. The condition inputs come from the request context the pipeline assembles, which is why that context includes the source address, the transport security, the listing parameters, and the tags.

### 15.7 Access-control lists

Where ACLs are in force, a bucket or object carries an owner and a set of grants, each grant giving a grantee a permission. A grantee is a specific user, or one of the predefined groups, namely the all-users group representing anonymous and any requester, the authenticated-users group representing any authenticated requester, and the log-delivery group. A permission is full control, read, write, read-of-ACL, or write-of-ACL, and these map to actions so that read on an object permits getting it, read on a bucket permits listing it, write on a bucket permits creating and overwriting and deleting its objects, read-of-ACL permits getting the ACL, and write-of-ACL permits putting the ACL, with full control implying all of these. ACLs may be set explicitly as a list of grants or through a canned ACL that names a common grant set, such as private giving the owner full control, public-read adding read for all users, public-read-write adding write for all users, authenticated-read adding read for authenticated users, and the bucket-owner variants and the log-delivery-write set; the canned names map to grant sets on input. Because public ACL grants are exactly what Block Public Access governs, the evaluation order ensures that enabling Block Public Access neutralises a public-read or public-read-write ACL regardless of what is stored.

### 15.8 The pipeline and anonymous access

In the request flow, after authentication establishes the principal or determines the request is anonymous, the pipeline maps the operation to its action, assembles the resource and the condition context, fetches the public-access-block settings and the policy and the ACLs and the ownership mode through the metadata interface with the help of the configuration cache, and calls the authorization engine, enforcing its decision before the operation handler runs. Anonymous access is simply the case where the principal is the anonymous one and only the all-users group and a wildcard-principal policy can grant it, gated by Block Public Access. Object sharing is a separate facility layered beside this, for handing out access to a single object without making the bucket public, in two complementary forms. A **persistent share** is a stored, revocable, optionally-forever capability: minting it (admin-only, audited) records an opaque token in the `object_shares` table, and `GET`/`HEAD` of `/p/{token}` resolves the token — rejecting an unknown one as not-found and a revoked or expired one as gone — then streams the named object under a least-privilege synthetic principal scoped to read of that one key. Because that is an identity grant rather than a public one it intentionally bypasses Block Public Access for the single named object, yet it can never reach another object or a write; the share may pin a specific version, force a download with a chosen filename, or expire never. A **presigned URL** is the standard S3 SigV4 query-signed form, minted server-side with the requester's own SigV4 credential for a `GET` (download) or `PUT` (upload), capped at seven days and interoperable with any S3 client; it carries no server-side state, so it is neither revocable nor listable, and on redemption it is an ordinary authenticated request that runs the full authorization model above. Revocation, listing, and the per-object share management are exposed through the management API and console, and the formerly described keyed-signature public-read URL is subsumed by the token form.

---

## 16. Object versioning

### 16.1 States

A bucket is in one of three versioning states. It begins unversioned, in which each key has a single version stored under a sentinel version identifier and a put overwrites it in place at the metadata level, reclaiming the prior blob, exactly as a non-versioned store behaves. Versioning can be enabled, after which every put creates a new version with a fresh identifier and never overwrites an existing version, and a delete creates a delete marker rather than removing data. Versioning can be suspended, after which existing versions are retained but new puts use the sentinel identifier again, overwriting only the sentinel version while leaving the previously created identified versions intact. These three states and their transitions match S3, and the state is part of the bucket configuration consulted on every put and delete to the bucket.

### 16.2 Versions, delete markers, and identifiers

A version is a metadata row with its own identifier and its own blob, so creating a version is cheap under the UUID-blob model and deleting one key's history does not touch another's. A version identifier is an opaque, sortable token generated at write time such that listing can present versions of a key from newest to oldest; the sentinel identifier represents the unversioned-or-suspended single version. A delete marker is a version row that carries the delete-marker flag and references no blob, and its purpose is to make a plain get of the key behave as if the object is gone while leaving the earlier versions retrievable by identifier; placing a delete marker is what a delete does in an enabled bucket. The latest-version flag tracks which version a plain get resolves to, and it moves as versions and delete markers are added.

### 16.3 Operation semantics under versioning

A put to an enabled bucket creates a new latest version and returns its identifier in the response header. A plain get returns the latest version, or, if the latest version is a delete marker, behaves as not found and signals the delete marker in the response, while a get that names a version identifier returns that specific version regardless of what is latest. A plain delete to an enabled bucket inserts a delete marker and returns the marker's identifier, whereas a delete that names a version identifier permanently removes that version and reclaims its blob, and permanently removing the current version promotes the next most recent version or delete marker to latest. Heading and the conditional and range semantics apply to whichever version is resolved. Copying can name a specific source version. The version-listing operation enumerates all versions and delete markers under a prefix with the same paged range-seek machinery as object listing, ordered as S3 specifies, and distinguishes versions from delete markers in its output.

### 16.4 Interaction with the rest of the system

Versioning is the substrate that several other features rely on. Lifecycle noncurrent-version expiration acts on versions that are no longer latest, and lifecycle expiration in an enabled bucket inserts delete markers rather than destroying data, with a separate provision for expiring the delete markers themselves once they are the only thing left. Bucket replication requires versioning to be enabled, because a stable per-version identity is what makes asynchronous replication well-defined and idempotent, and so enabling replication on a bucket requires the bucket to be versioning-enabled. Because versions are independent rows and blobs, all of this composes without special cases in the storage layer. The protective deletion control that S3 offers, requiring an extra factor to remove versions, is noted as a possible future addition rather than a v1 feature, and is called out here so its absence is a known decision rather than an omission.

---

## 17. Object and bucket tagging

### 17.1 Purpose and limits

Tags are key-value pairs attached to objects and to buckets, used to classify data for lifecycle and policy and for the operator's own organisation. The tagging body must be well-formed XML with each tag carrying a key and a value, and a malformed body is rejected. The S3 quantitative limits are enforced on write: at most ten tags on an object and fifty on a bucket, a key of one to a hundred and twenty-eight characters and a value of up to two hundred and fifty-six, the permitted character set of letters, digits, whitespace, and the punctuation S3 allows, no duplicate keys within a set, and no reserved-prefix keys; a violation is rejected with an invalid-tag error before the write is staged, and the inline tag set a put may carry is validated on the same path. Object tags are attached to a specific object version, and a put can carry an initial tag set inline while the dedicated tagging operations get, replace, and delete a resource's tags afterward.

### 17.2 Storage and use

Object tags are stored associated with the object version so they can be returned by the tagging get and matched efficiently by the features that consume them. Two features consume object tags. Lifecycle rules can filter on tags, so that a rule applies only to objects carrying a given tag, which lets an operator express policies such as expiring objects tagged as temporary; the lifecycle scanner consults an object's tags when evaluating a tag-filtered rule. Bucket policies can condition on tags, both an object's existing tags and the tags supplied on a request, so that access can depend on classification; the authorization context therefore includes the relevant tags. Bucket tags are simpler, serving organisation and policy conditioning at the bucket level. Tagging changes are ordinary metadata mutations and follow the commit path; in a versioned bucket, tag operations act on the relevant version. Because S3 itself offers no way to enumerate tags or to find the objects carrying one, Cairn adds a management-API tag browser — a reverse query over the tags of current objects, optionally scoped to a bucket, served by a covering index so it stays an index seek rather than a scan — which the console surfaces as a global Tags page (every `key=value` in use with its object count, drilling into the objects that carry it) and as a tag filter in the object browser.

---

## 18. CORS configuration

### 18.1 Per-bucket configuration

Cross-origin resource sharing is configured per bucket, because different buckets serve different web applications, and so Cairn's CORS handling is partly dynamic rather than a single static policy applied to the whole server. A bucket's CORS configuration is a list of rules, each naming the allowed origins, the allowed methods, the allowed request headers, the headers to expose to the browser, and the cache lifetime of a preflight result, with the wildcard forms S3 permits. The configuration is set, retrieved, and deleted through the corresponding bucket subresource operations and validated on write.

### 18.2 Preflight and actual-request handling

A browser issues a preflight options request before certain cross-origin requests, and Cairn answers it by finding the bucket's CORS configuration, matching the requested origin and method and headers against the rules, and, on a match, returning the allow-origin, allow-methods, allow-headers, expose-headers, and max-age response headers derived from the matching rule, or refusing the preflight when nothing matches. For an actual cross-origin request that carries an origin, Cairn evaluates the same rules and adds the corresponding allow-origin and expose-headers and, where the matched origin is specific rather than the wildcard, signals that the response varies by origin so that caches do not serve one origin's response to another. Because CORS is per bucket and depends on the bucket existing and having a configuration, the evaluation happens in the request pipeline with access to the bucket configuration through the cache, and the outer middleware defers to it rather than applying a fixed cross-origin policy. The management API, by contrast, uses a straightforward configured cross-origin policy of its own, since it is not the per-bucket S3 surface.

---

## 19. Lifecycle management

### 19.1 Rules

A bucket's lifecycle configuration is a list of rules, each with an identifier, an enabled or disabled status, an optional filter, and one or more actions. A filter selects the objects a rule applies to by key prefix, by one or more tags, by object size bounds, or a conjunction of these, and an empty filter applies to the whole bucket. The actions Cairn supports are expiration of current objects after a number of days from creation or on a specific date, expiration of noncurrent versions after a number of days from becoming noncurrent with an optional count of newest noncurrent versions to retain, removal of expired-object delete markers that have become the only remaining version of a key, aborting incomplete multipart uploads a number of days after they were initiated, and transition of objects to a remote cold tier after a number of days or on a date. The configuration is set, retrieved, and deleted through the lifecycle bucket subresource and validated on write for structural correctness and for sane combinations.

### 19.2 The scanner engine

Lifecycle actions are applied by a background scanner that runs on a configurable interval and processes the buckets that have a lifecycle configuration. For each such bucket it pages through the relevant objects, versions, and multipart sessions using the bounded enumeration interface so that memory stays flat regardless of bucket size, evaluates each item against the applicable rules using the item's age, size, and tags, and applies the due actions. The scanner is idempotent: applying it twice has the same effect as once, because each action is expressed as a state transition that is a no-op when already performed, which matters because a scan may be interrupted and rerun. Its work is surfaced as a structured per-scan report — objects expired, versions expired, delete markers removed, uploads aborted, and errors — logged at the end of each scan, so an operator can see lifecycle converging and can detect a bucket whose rules are not. The scanner uses the injected clock so its date and age logic is tested deterministically.

### 19.3 Expiration semantics under versioning

Expiration respects the bucket's versioning state, which is where lifecycle and versioning meet. In an unversioned bucket, expiring an object permanently deletes it and reclaims its blob. In a versioning-enabled bucket, expiring a current object does not destroy data; it inserts a delete marker so the object disappears from a plain listing while its versions remain retrievable, and a separate noncurrent-version-expiration action permanently deletes versions that have been noncurrent longer than the configured period, optionally preserving a configured number of the newest noncurrent versions, which is how an operator bounds version history. The removal of expired-object delete markers cleans up a delete marker once it is the only thing left for a key, so that fully-expired keys do not accumulate dangling markers. These behaviours match S3 and are the reason versioning is designed in as a substrate rather than bolted on.

### 19.4 Aborting incomplete multipart uploads

A lifecycle rule can specify that incomplete multipart uploads be aborted a number of days after they were initiated, which reclaims the staging space of uploads that clients began and never completed. This drives the same abort path as the multipart sweeper, with the threshold taken from the bucket's lifecycle rule rather than a global default, so an operator can tune retention of in-progress uploads per bucket. Aborting removes the session and its staged parts through the normal abort path.

### 19.5 Transition to a remote cold tier

> **Implementation status (deferred).** As of this revision the lifecycle engine parses and validates `Transition` actions but performs no data movement — it is a documented no-op, tracked as future work alongside the io_uring blob engine in Section 32, Phase 15. The design below is the intended shape; the remote-backed blob-store implementation it relies on is not yet built. All other lifecycle actions (current and noncurrent expiration, expired-delete-marker removal, incomplete-multipart abort) are implemented.

Transition moves an object's bytes off the local disk to a configured remote S3-compatible destination after the object has aged, trading read latency for lower local storage cost, which is how an operator keeps a long tail of rarely-accessed data without consuming local capacity. When an object transitions, Cairn streams its bytes to the remote destination through the same S3-client facility that replication uses, records on the object version that it now lives in the cold tier together with the remote locator, and reclaims the local blob; the object's metadata, size, ETag, and identity are unchanged, so to a client the object still exists with the same attributes. A read of a transitioned object is served by Cairn fetching the bytes from the remote destination and streaming them to the client transparently, so the client continues to get the object from Cairn without knowing it now originates remotely; this is why the blob-store interface admits a remote-backed implementation, and the cold tier is realised as such an implementation selected for transitioned objects. The cost of this transparency is that reads of cold objects carry the remote's latency and the operator's egress considerations, which the documentation makes explicit, and v1 treats transition as effectively one-way with reads served from the cold tier rather than implementing an explicit restore-to-local workflow, which is noted as a future refinement. Because compressed blobs are self-describing, a transitioned compressed object remains readable from the cold tier by the same decompression path.

---

## 20. Bucket replication

### 20.1 Purpose and the consistency model

Bucket replication is Cairn's answer to cross-host and cross-site redundancy, given that clustering is out of scope. It asynchronously copies the objects of a source bucket to a destination bucket on another Cairn deployment or any S3-compatible endpoint, so that a second site holds a current copy that can serve reads or take over on failure. It is eventually consistent with observable lag rather than synchronous, which is the deliberate trade that keeps the write path fast and the deployments independent: a write completes locally and is acknowledged as soon as it is durable on the source, and replication catches the destination up shortly after. This matches the spirit of S3 cross-region replication and is sufficient for redundancy and geo-distribution without the cost and fragility of synchronous cross-site writes.

### 20.2 Configuration

A bucket's replication configuration names an identity to act as and a list of rules, each with an identifier, a status, a priority that orders overlapping rules, an optional filter by prefix or tags, a destination giving the target endpoint and bucket and the credentials to use there, a setting for whether delete markers are replicated, and a setting for whether existing objects are backfilled when the rule is created in addition to replicating new writes. The destination credentials are stored encrypted at rest under the master key, like the SigV4 secrets, since they grant write access to another system. Following the established model for single-node S3 deployments, a destination is configured per source bucket as a named **remote target** — its endpoint, region, destination bucket, and credentials — identified by a generated resource name (an ARN); a rule references that target by its ARN, and an operator adds a target through the management API, the UI, or the CLI rather than through server configuration. The destination authenticates the inbound replicated writes with ordinary request signing, so the recommended practice is to create a **dedicated replication user** on the destination whose policy grants exactly the replication actions (`ReplicateObject`, `ReplicateDelete`) on the destination bucket, rather than sharing administrator credentials; this keeps the replication link mutually authenticated even on a plaintext interface. Replication requires the source bucket to have versioning enabled, because a stable per-version identity is what makes replication well-defined and idempotent, so the configuration is rejected unless versioning is enabled, and enabling replication is the operator's signal that this bucket's writes should propagate. A bucket may carry several enabled rules pointing at distinct remote targets: the rule that matches a written key by prefix and tags (highest priority wins) is selected at enqueue, and its target ARN is stamped on the durable outbox entry, so each object routes to exactly the target its own rule names and a single bucket can fan out to several destinations by prefix, tag, or priority. Because the ARN is fixed on the entry at enqueue time, a later rule edit never misroutes work already queued.

### 20.3 The durable outbox

Replication is driven by a durable outbox in the metadata store rather than by in-memory state, so that it survives restarts and gives at-least-once delivery with idempotent application. When a write to a replication-enabled bucket commits, and the written version matches a replication rule's filter, an outbox entry is enqueued recording the bucket, key, and version, the operation which is an object creation or a delete-marker propagation, the rule it belongs to, the attempt count, the next attempt time, the status, and the last error. The enqueue is part of the same logical commit as the write, so a committed write is guaranteed to have its replication intent recorded and no write is silently left unreplicated. The outbox is the single source of what remains to be replicated and what has been.

### 20.4 The worker pool and the sink

A pool of replication workers consumes the outbox. A worker claims a batch of entries that are due, respecting per-key ordering so that a key's versions replicate in the order they were written and respecting rule priority, and for each entry it drives the destination through the replication sink interface, which is an S3 client capable of putting an object with its content type, user metadata, and tags, and of propagating a deletion or delete marker. The sink does not currently ship the object's ACL; ACL replication is future work. On success the worker marks the entry completed and records on the object version that it has been replicated. On a retryable failure, such as a network error or a throttling response from the destination, the worker increments the attempt count and sets the next attempt time using exponential backoff with a cap, so transient failures are retried without overwhelming the destination, and after a configured maximum number of attempts the entry is marked failed and surfaced for operator attention rather than retried forever. Idempotency comes from the version identity: re-shipping a version that already reached the destination overwrites it with identical content, so a duplicate delivery is harmless, which is what makes at-least-once delivery safe. Objects that arrived at a bucket through replication are marked as replicas so they are not themselves re-replicated, which prevents loops when two deployments replicate to each other.

### 20.5 Status, observability, and operations

The replication status of each object version is queryable, taking the values pending, completed, failed, or replica, so an operator or the management UI can see whether a given object has reached its destination. The engine exposes metrics for the replication lag measured as the age of the oldest pending entry, the queue depth, the count of completed and of failed transfers, and the bytes replicated, so that replication health is visible and a growing lag or a rising failure count is alertable. Failed entries can be retried on demand through the management API after the underlying cause is fixed. Existing-object backfill (resync) is triggered explicitly through the management API for a bucket whose rule enables existing-object replication: a bounded background pass pages the bucket's current versions and idempotently enqueues an outbox entry for each one a backfill-enabled rule selects, so the existing objects converge to the destination through the same machinery and metrics as new writes, and re-running a resync is safe because the deterministic backfill entry id makes an already-queued or already-completed version a no-op. Because replication writes to another system using stored credentials, it is part of the security surface: the credentials are encrypted at rest, the destination should itself be access-controlled, and the documentation notes that replicating to an untrusted endpoint exports data there.

---

## 21. Server-side request lifecycles

This section traces the operations whose end-to-end behaviour ties together the pieces specified above. Each begins after the outer middleware, authentication, and authorization have run, except where the body itself is part of authentication.

### 21.1 Putting a single object

The handler confirms the target bucket exists and validates the key. It resolves the body source by inspecting the content-sha256 header: a plain or unsigned-payload body is consumed directly, while a streaming-payload body is wrapped in the chunk decoder of Section 21.7 so that only de-framed payload bytes flow onward. It determines which checksum algorithms to compute, always the MD5 that becomes the ETag and additionally any algorithm the client signalled, and it parses any client-supplied checksum or content-MD5 to verify against. It resolves the size ceiling, rejecting early when the content-length header exceeds the configured maximum and otherwise passing a hard ceiling into the stage operation to enforce while streaming. It then asks the blob store to stage the object from the body stream, which streams the bytes through the hashers and, if the bucket compresses, through the block compressor, writes the staging file, and performs the durable commit prefix of fsyncing the file and its directory, returning the storage path, the logical and physical sizes, and the computed hashes. The handler verifies the computed hashes against the client-supplied values and, on mismatch, deletes the staged blob and fails. It assembles the object-version metadata including the content type, user metadata, any inline tags, the ownership-determined owner, the ACL where applicable, the checksums, and the compression descriptor, and it evaluates any conditional-write precondition. It submits the put to the metadata writer, where, inside the commit transaction and under group commit with a per-operation savepoint, the precondition is checked and the version row is upserted, with versioning state determining whether this creates a new identified version or overwrites the sentinel version, and where, for a replication-enabled bucket whose rule matches, the replication outbox entry is enqueued as part of the same commit. The commit is the linearization point and the durability barrier; only after it does the handler acknowledge the client. After the commit it reclaims any superseded blob on a best-effort basis, then responds with the quoted ETag, the version identifier where versioning is enabled, and any checksum echoes. A crash before the commit leaves an orphan blob that reconciliation reclaims; a crash after leaves a consistent state with replication intent recorded.

### 21.2 Getting and heading an object

The handler validates the key and confirms the bucket, then resolves the version: a get or head naming a version identifier targets that version, while a plain request targets the current version, and if the current version is a delete marker the response is a not-found that signals the delete marker. It evaluates conditional headers against the resolved version's ETag and modification time, returning not-modified or precondition-failed as appropriate without transferring the body. For a head it returns the metadata headers and stops. For a get it opens the blob through the blob store, which transparently handles decompression, and serves the body: a whole-object read of an uncompressed blob can take the zero-copy fast path where enabled, a ranged read of an uncompressed blob seeks and transfers the requested length and returns partial content with a content-range, and any read of a compressed blob is served through the block-selective decompression path, which for a range decompresses only the overlapping blocks. Response-header overrides that the request carries are applied, and the cache-control is set as configured. Should the blob be unexpectedly absent although the row exists, which the durability ordering prevents for data written by Cairn but which a lazy integrity check still guards against, the handler returns an internal error and emits the integrity metric.

### 21.3 Multipart upload

Initiating an upload validates the bucket and key, records a session with its content type and intended metadata and ACL and owner, and returns the upload identifier. Uploading a part validates the part number, resolves the body source including the chunk decoder when streaming signing is used, enforces the size ceiling, and asks the blob store to stage the part, computing the part's plaintext MD5 which becomes its part ETag and recording the part metadata; re-uploading a part number stages a fresh blob and supersedes the previous attempt, which is reclaimed at completion or abort. A part may also be produced by copying a range of an existing object rather than from a request body, in which case the bytes come from a read of the source. Completing an upload parses the ordered list of parts and their ETags, atomically claims the session so that a concurrent completion cannot also proceed, validates that the named parts exist and are in ascending order and meet the minimum size for all but the last and match their ETags, asks the blob store to assemble the ordered parts into one durably-committed blob applying compression during the assembly pass, computes the multipart ETag as the MD5 of the concatenated per-part plaintext MD5 digests with the part-count suffix, evaluates any precondition, and commits the object-version upsert and the session removal in one transaction with the replication enqueue where applicable, after which it reclaims the part blobs and any superseded object blob and responds. Aborting an upload removes the session and reclaims its parts. The multipart sweeper and the lifecycle abort action remove sessions that linger.

### 21.4 Listing objects and versions

Listing current objects parses the prefix, the delimiter, the continuation token which encodes the last key returned, the start-after, and the maximum-keys which is clamped to the S3 ceiling, resolves the effective start point, and asks the metadata store for a page using the half-open range seek with the latest-and-not-a-delete-marker conditions, grouping into common prefixes when a delimiter is given. It renders the listing with the object entries and the common prefixes, the truncation flag, and the next continuation token derived from the page cursor, and the version-one form differs only in using the marker fields. Listing versions is the same with the latest-only condition dropped and the output distinguishing versions from delete markers and ordering by key and version recency. Listing multipart uploads enumerates the active sessions under a prefix. All three are bounded by paging and never materialise a whole bucket.

### 21.5 Deleting objects

A single delete validates the bucket and key and then behaves according to versioning: in an unversioned bucket it removes the object row and reclaims the blob; in a versioning-enabled bucket a plain delete inserts a delete marker and returns its identifier, while a delete naming a version identifier permanently removes that version, reclaims its blob, and promotes the next version to latest if the current one was removed, and for a replication-enabled bucket a delete-marker insertion enqueues replication of the marker where the rule permits. A bulk delete parses up to a thousand keys, applies the same per-key logic, collects per-key successes and errors, reclaims the freed blobs after their rows are removed, and returns the result document; it processes keys without materialising more than the request list, and the management force-empty of a bucket pages through the bucket rather than loading it.

### 21.6 Copying an object

A copy reads the source, which may name a specific source version, and writes a new object at the destination, either replacing the metadata with values supplied on the request or preserving the source's metadata as directed, and honouring any copy preconditions against the source. When the source and destination are the same key, which is how clients change metadata in place, the operation rewrites the version's metadata. When source and destination buckets have matching compression policies the stored representation can be copied directly without recompression, and otherwise the bytes are decompressed and recompressed as the destination policy requires; in all cases the destination ETag follows the normal rules. A copy to a replication-enabled destination enqueues replication like any other write.

### 21.7 The streaming chunked-upload decoder

The streaming-payload upload format is the piece of ingest most prone to subtle error and is specified in detail because getting it wrong corrupts objects silently (F-5). When a put or part upload carries a streaming-payload content-sha256 sentinel, the request body is not the object's bytes; it is a sequence of chunks, each introduced by a header line that states the chunk's payload size in hexadecimal and, for signed streaming, a per-chunk signature, followed by a separator, then exactly that many payload bytes, then a trailing separator, with the sequence ended by a zero-size chunk and, in the trailing-checksum variants, a set of trailer lines after it. The decoder is a streaming adapter over the raw body that emits only payload bytes to the downstream hasher and writer and never the framing. It is structured as a state machine that reads a chunk header, then reads exactly the declared number of payload bytes emitting them downstream, then consumes the trailing separator, and repeats until the zero-size chunk, after which it consumes any trailer. It maintains a small bounded parse buffer so that it works correctly when the underlying transport delivers reads that split a header or a payload across boundaries, which is the normal case over a real network, accumulating just enough to parse a header without ever buffering a whole chunk's payload. For signed streaming it maintains the rolling signature chain, in which each chunk's expected signature is computed from the previous signature and a hash of the chunk's payload and the request's scope, seeded by the seed signature established during request authentication, and it fails the stream immediately if a chunk's signature does not match, so a tampered or truncated body is rejected rather than stored. It enforces the same size ceiling as a plain body by bounding the total emitted payload. The decoder is the foremost fuzzing target (Section 29), exercised against malformed sizes, missing separators, split boundaries, oversized declarations, and non-terminating streams, and against captured real-client bodies, because its correctness is the difference between accepting the uploads that common SDK configurations actually send and corrupting them.

---

# Part V. Control plane

## 22. Management API

### 22.1 Role and shape

The management API is the control surface for operating Cairn, distinct from the S3 data surface. It is JSON over HTTP, versioned in its path, gated to administrators, and it is the single API that both the embedded web UI and the command-line interface consume, so that whatever can be done in a browser can be done from a terminal and the two never drift. It is not on the object hot path, so it favours clear, stable, well-documented request and response shapes over raw throughput, and it returns errors in a JSON envelope rather than the S3 XML error document. It is served by the same process and listener family as the S3 API, separated by path.

### 22.2 Endpoints

The API exposes an overview of the store, returning the bucket and object counts, the logical and physical storage totals and thus the achieved compression ratio, and summary replication health. It exposes listing and creating buckets, and a per-bucket detail that returns the bucket's size in logical and physical bytes, its object and version counts, its versioning and ownership state, and each of its configuration aspects, namely its policy, ACL, CORS, lifecycle, replication, tag set, and public-access-block settings, with operations to read and update each aspect so the UI can present and edit them; the same configuration is settable through the S3 subresource operations, and the management API is a convenience over the same stored state. It exposes a force-delete of a bucket that empties it by paging through its contents rather than loading it, for the common operator need to remove a populated bucket, and the same bounded-paging mechanism backs a recursive prefix (folder) delete that permanently removes every object and version beneath a key prefix in one operation, reporting the count removed and signalling with a continuation flag when a very large folder needs the call repeated. It exposes a paged object listing for the data browser, and the minting of Cairn signed public-read (share) URLs for sharing or testing. It exposes user management, namely creating, listing, updating, and deactivating users and rotating their credentials. It exposes the activity and audit log with a bounded result limit, and a stored time-series of API request metrics for the usage-analytics view, queryable over a selectable range and downsampled server-side so a chart stays light regardless of traffic (Section 26.5). It exposes replication operations, namely listing failed replication entries and retrying them and viewing per-bucket replication status. And it exposes the non-secret portions of the running configuration and the health and readiness state. Every mutating endpoint records an audit entry.

### 22.3 Authentication and authorization of the control plane

The management API authenticates with the same credential mechanisms as the rest of the system, with the Bearer scheme being the natural fit for the UI and the CLI, and it requires the administrator role for all operations, refusing members and anonymous requests. The UI authenticates once to obtain a first-party Bearer token, which it holds in browser storage and sends as an `Authorization: Bearer` header on every subsequent call; because it uses no ambient cookie credentials, cross-site request forgery does not apply. Because the control plane can read and change everything, it is held to the same wire-security expectations as the rest of the system: it is served over TLS, whether terminated by Cairn or by a proxy, and credentials never traverse an untrusted hop in clear.

---

## 23. Embedded web UI

### 23.1 What it is and how it ships

The management UI is a single-page application built with the React framework and its standard build toolchain, and it is compiled into the Cairn binary at build time so that a Cairn deployment is one binary that already contains its own management interface, with no separate UI service to deploy, host, or version-match. The built static assets, the markup, the script bundles, and the styles produced by the UI build are embedded into the binary through a compile-time asset-embedding mechanism that bakes the asset directory into the executable, and the server serves them from memory. This is the operator's stated requirement that the UI be installed and compiled into the binary itself and that management be possible through either the UI or the CLI, and it is satisfied by making the UI a build-time artifact of the same binary.

### 23.2 The build pipeline

Building Cairn with its UI is a two-stage process that the workspace orchestrates so that a normal release build produces a UI-containing binary. The first stage runs the Node toolchain to produce the optimised static asset bundle from the UI source. The second stage compiles the Rust binary with the asset-embedding step pulling that bundle into the executable. The orchestration caches the asset build so that Rust-only changes do not rebuild the UI and UI-only changes do not needlessly recompile unrelated Rust, which keeps the developer loop fast, and a build feature allows producing a binary without the embedded UI for cases that want a smaller artifact or a faster build, in which case the UI routes are simply absent. The result is reproducible: the same sources produce the same binary with the same embedded UI.

### 23.3 Serving and behaviour

The server serves the embedded single-page application under a management UI path, returning the application shell for client-side routes so that the framework's routing works on reload, and serving the script and style assets with appropriate caching headers since they are content-hashed by the build. The application is a client of the management API: it authenticates the operator, then renders the store overview with the storage and compression and replication figures, a bucket view for listing and creating buckets and for editing each bucket's versioning, quota, default encryption, compression, bucket policy, and replication settings while showing its CORS, lifecycle, tagging, and public-access-block configuration read-only, a data browser for paging through objects and for uploading, downloading, deleting, and generating share URLs, a user-management view, an activity and audit view, and a replication-status view that surfaces lag and failures and offers retry. The navigation sidebar lets the operator expand the buckets entry into an inline accordion that lists the buckets and deep-links straight into each one's browser, so a named bucket is one click away without first loading the list. The data browser previews an object by opening it in a new browser tab through a short-lived presigned URL, which delegates rendering of images, PDFs, and anything else to the browser's own native viewers rather than re-implementing them, and it deletes a whole folder by invoking the recursive prefix delete behind a confirmation that states the action is permanent. A dedicated metrics view charts API request volume over time with a one-day, one-week, two-week, or one-month range, alongside a breakdown by operation and the most active buckets, drawn from the request-metrics subsystem (Section 26.5). A Tags view lists every object tag in use across the buckets with its object count and drills into the objects carrying a chosen tag, and the object browser can filter its listing to a single tag (Section 17.2). The UI changes nothing that the API does not expose, so it carries no privileged logic of its own and remains a thin presentation over the control plane.

### 23.4 Security posture of the UI

Because the UI is served by the same process and talks to the same admin-gated API, its security reduces to the API's: it requires an administrator session, it is served over TLS, and it is subject to the same audit logging for the actions it triggers. Because the UI carries a first-party Bearer token in the `Authorization` header rather than an ambient cookie, it presents no cross-site-request-forgery surface; serving it from the same origin as the API simply lets the single-page application reach the management path without cross-origin configuration. The UI exposes no secret material beyond what an administrator is entitled to see, with credentials shown once at creation and only hashes and ciphertext retained thereafter.

---

## 24. Command-line interface

### 24.1 Role

The command-line interface is the terminal-first way to operate Cairn and is a first-class peer of the web UI, not an afterthought. It serves two distinct purposes. For remote administration it is a client of the management API, so that an operator can do from a terminal or a script everything the UI offers, which suits automation and remote management. For node-local operations that must run on the host and that operate directly on the data directory and the database, it provides commands that do not go through the API, because some operations are inherently local or must run when the server is not serving. Shipping both as subcommands of the same binary keeps the deployment a single artifact.

### 24.2 Remote administration commands

As an API client the CLI offers commands mirroring the management API: creating, listing, and removing buckets and force-emptying them; reading any bucket configuration aspect through `config get`, and writing the bucket policy through `config set` taking the document from a file so it can live in version control; managing users and rotating credentials; browsing and listing objects; uploading, downloading, and removing objects; and viewing and retrying replication. It is configured by flags or the corresponding `CAIRN_*` environment variables giving the endpoint, access key, and secret key, and it can emit either human-readable output for interactive use or structured output for scripting, so it composes into automation.

### 24.3 Node-local commands

The local commands run on the host against the data directory and database directly. They include the first-start bootstrap that creates the initial administrator into an empty store, which is inherently local and one-time; an integrity command that runs reconciliation on demand and, in its repair mode, resolves divergences such as rows whose blobs are missing, which is the recovery tool referenced by the durability and backup sections; a backup command that performs the consistent snapshot procedure of Section 31; configuration validation that checks a configuration without starting the server; and the database migration that the server also runs at startup, exposed for operators who prefer to migrate explicitly. These commands are how an operator bootstraps, verifies, backs up, and repairs a deployment from the host shell, complementing the remote administration that the API-client commands provide.

---

# Part VI. Cross-cutting concerns

## 25. Error model and S3 error mapping

### 25.1 Typed errors and one translator

Each module defines its own typed errors describing what went wrong in its own terms, rather than passing strings around, so that the cause of a failure is preserved with structure as it propagates (F-22). At the protocol boundary a single translator maps every internal error to the wire response: for the S3 surface to the S3 XML error document with a code, a human-readable message, the resource, and a request identifier, paired with the right HTTP status; for the management surface to the JSON error envelope. Keeping the mapping in one place makes it total and testable, and a test enumerates every internal error variant and asserts that each maps to a defined status and code, so no failure can reach a client as an unmapped internal error.

### 25.2 The mapping

The principal mappings are as follows.

| Condition | HTTP status | S3 error code |
|---|---|---|
| Bucket does not exist | 404 | NoSuchBucket |
| Object or version does not exist | 404 | NoSuchKey / NoSuchVersion |
| Bucket already exists or is owned | 409 | BucketAlreadyExists / BucketAlreadyOwnedByYou |
| Bucket not empty on delete | 409 | BucketNotEmpty |
| Multipart session not found or not active | 404 | NoSuchUpload |
| Conditional precondition failed | 412 | PreconditionFailed |
| Object exceeds configured maximum size | 400 | EntityTooLarge |
| Out of space on the data filesystem | 507 | InsufficientStorage |
| Supplied checksum or content-MD5 mismatch | 400 | BadDigest / InvalidDigest |
| Malformed request, XML, or policy document | 400 | MalformedXML / MalformedPolicy / InvalidArgument |
| Setting an ACL while ownership disables ACLs | 400 | AccessControlListNotSupported |
| Missing or unparseable credentials | 400 / 403 | AccessDenied / InvalidAccessKeyId |
| Signature mismatch or invalid signature | 403 | SignatureDoesNotMatch |
| Authorization denied by policy, ACL, or public-access block | 403 | AccessDenied |
| Range not satisfiable | 416 | InvalidRange |
| Operation not implemented | 501 | NotImplemented |
| Unexpected internal failure | 500 | InternalError |

The request identifier appears in the error body, as a response header, and in the request's trace span, so that a client report can be tied to the exact server-side trace.

---

## 26. Observability and audit

### 26.1 Tracing and logging

The system is instrumented with structured tracing throughout, with one span per request that carries the method, the route, the bucket and key, the principal, the request identifier, the resulting status, and the duration, and with structured logs that can be emitted as human-readable text or as machine-readable JSON depending on configuration, filtered by a configurable level. Errors are logged at the boundary with their request identifier so they correlate with the client-visible identifier and with the trace. This is the baseline of being able to see what the server is doing, which Cairn provides in full.

### 26.2 Metrics

A metrics endpoint in the widely-supported text exposition format publishes the series an operator needs to run Cairn in production (F-18). The set includes request counts and a latency histogram labelled by route and method and status; bytes received and sent; gauges for object, bucket, and version counts and the logical and physical stored-byte totals refreshed from the store and on mutation; counters for metadata cache hits and misses; the depth of the metadata writer's queue, which is the key early-warning signal for the single-writer write-rate ceiling so an operator can see write saturation before it becomes latency; the write-ahead-log checkpoint runs and the log size; the replication queue depth, lag, completed and failed counts, and bytes replicated; and the logical-versus-physical byte totals that express the compression ratio. These series make every subsystem specified in this document observable, which is what turns the design into something operable.

### 26.3 Audit log

Mutating actions across both the S3 and management surfaces are recorded in an audit log with the actor, the action, the resource, and the salient attributes, retained in the metadata store and surfaced through the management API and UI. This serves both the operational need to see recent activity and the security need to have a record of who changed or accessed what, and it is distinct from the operational metrics in that it is per-event and attributable rather than aggregate.

### 26.4 Health, readiness, and optional scrub

The system exposes a liveness endpoint that succeeds whenever the process is serving and a readiness endpoint that succeeds only once migrations have applied, reconciliation has completed or been deliberately deferred, and the writer and read pool are responsive, so that an orchestrator does not route traffic to a process that is not yet ready. As an optional integrity facility, a background scrub can re-read stored blobs and verify them against their recorded content hashes on a slow schedule, detecting silent corruption from failing storage and reporting it as a metric and an audit event, which complements the redundancy that the operator provides at the storage layer by giving early warning of bit rot.

### 26.5 Request metrics and usage analytics

Distinct from the scrape-only Prometheus series of Section 26.2, which are in-memory counters an external monitoring system samples, Cairn keeps a **stored, queryable time-series of API request activity** so the console can present a self-contained usage-analytics view without an external metrics stack. The two design constraints are that the request hot path must do no database work, and that a rolling month of history must stay bounded. Both are met by aggregating in memory and flushing in batches. Every completed request is classified into an operation name — `GetObject`, `PutObject`, `ListObjects`, the multipart operations, the coarse `Management` bucket for the control plane, and so on — and the request's target bucket and HTTP status class are noted; this tuple, floored to a configurable time window, indexes an in-process sharded counter that the request increments under a microsecond-held lock and nothing more. Alongside the count, each request also folds its **transferred bytes** (received and sent) and its **latency** — both as a running sum and into a small fixed-bucket histogram — into the same accumulated cell, so the hot path stays a single in-memory update. A background task periodically drains the accumulated cells and submits them as a single batched mutation through the same single writer that owns every other write, where they upsert-accumulate into a rollup table keyed by window, operation, bucket, and status class; the same flush prunes rows older than the retention horizon, so the table is self-bounding. Because storage is one row per active tuple per window rather than one row per request, a busy node costs a bounded number of rows per window, and the flush amortises all of a window's traffic into one transaction regardless of request rate.

The query side answers a range — one day, one week, two weeks, or one month — by reading the rollup since that range's lower bound and **downsampling the timeline into a window chosen for the range** (five minutes for a day, an hour for a week, three hours for a fortnight, six hours for a month), so the returned series has a bounded number of points whatever the underlying row count. Each timeline point carries its requests, errors, bytes, and average latency, and the query also returns the breakdown by operation (with bytes and latency), by bucket, and by status class, plus range-wide totals: request and error counts, bytes moved, the average and a histogram-estimated 95th-percentile latency, the busiest window, and the number of active buckets. From these the console renders a Grafana-style responsive dashboard — stat tiles for volume, throughput, error rate, and latency alongside system storage figures, and time-series and breakdown panels — that reflows from three columns on a desktop to one on a phone. The whole subsystem is gated by configuration and, when disabled, neither accumulates nor spawns its flush, and its window granularity, flush cadence, and retention are operator-tunable (Section 28.2). This is the per-tenant, per-operation visibility an operator needs to understand the shape and cost of their traffic, provided from Cairn's own store rather than requiring a separate observability deployment.

---

## 27. Security and threat model

### 27.1 Assets and trust boundaries

The assets Cairn protects are the object bytes, the user credentials, the destination credentials for replication, and the metadata database. Cairn trusts the host's filesystem and, when used, the terminating proxy; it does not trust request bodies, headers, keys, query strings, policy or configuration documents, or the contents of the metadata database against tampering by anyone who can read the file, which is why secrets in it are encrypted. The deployment is expected either to terminate TLS in Cairn or to sit behind a proxy that does, and never to expose the plaintext interface to an untrusted network.

### 27.2 Transport security

Cairn can terminate TLS itself using a modern Rust TLS stack with current defaults and reloadable certificate material, so a deployment is secure on the wire without an external proxy (N-6), and it also runs behind a terminating proxy on a trusted interface. Where Cairn terminates TLS and the platform supports kernel TLS, the read fast path can stay zero-copy by offloading symmetric encryption to the kernel after a userspace handshake (Section 7.6). The control plane, including the UI and the CLI, is held to the same transport expectations as the data plane.

### 27.3 Authentication and authorization controls

Authentication uses SigV4 and Bearer with constant-time comparison and signature-skew and credential-scope validation (Section 14). Authorization is the multi-source engine of Section 15 with its explicit precedence, in which an explicit policy deny overrides everything, Block Public Access gates public grants ahead of ACL and policy, and the bucket-owner-enforced ownership mode disables ACLs to remove the most common accidental-exposure vector. Block Public Access at the account and bucket level lets an operator guarantee that nothing is inadvertently public regardless of individual bucket settings (N-5).

### 27.4 Secrets at rest

SigV4 secrets and replication destination credentials are stored encrypted under a master key supplied to the process out of band, using authenticated encryption, so that reading the database file does not yield usable secrets (F-15); the plaintext exists only transiently in memory in a zeroizing container so it is scrubbed promptly and is less likely to appear in a core dump. Bearer secrets are stored as fast cryptographic hashes, which is appropriate because they are high-entropy machine-generated tokens rather than human passwords. The master key is never written to disk by Cairn and is kept out of the backup that contains the database, so that the backup alone does not disclose the secrets, and rotating the master key re-encrypts the stored secrets.

### 27.5 Input safety and resource limits

The key sanitiser and the final within-root path check make key-based path traversal structurally impossible and are property-tested (Section 29). A configurable maximum object size is enforced both by early rejection on the declared length and by a streaming ceiling, optional per-bucket and per-user byte quotas are enforced inside the commit transaction, and an out-of-space condition is mapped to the correct status rather than surfacing as an opaque failure (F-16). A global concurrency limit and per-request timeouts bound resource use under load, and the bounded blob pool and the streamed, backpressured transfers prevent a few large transfers from exhausting memory or threads. The development authentication bypass is compiled out of release builds and additionally refuses non-loopback binds (F-17).

### 27.6 Residual risks

Several risks are acknowledged and accepted for the initial scope, recorded so they are known decisions rather than oversights. Object bytes are stored without whole-blob encryption unless per-bucket or per-request SSE-S3 is enabled, in which case the object's data key is sealed under the master key; an attacker with filesystem access can therefore read the content of any object not written under SSE-S3, so the mitigations are enabling SSE-S3 and full-disk or filesystem-level encryption provided by the operator, with transparent whole-blob encryption as a blob-store decorator remaining future work. The authorization model supports per-user identity policies alongside the user role model, the bucket policy, and ACLs, but does not implement cross-account principals, temporary credentials, or federation (Section 15.1). Replicating to an endpoint exports data there, so the destination must be trusted and access-controlled. And single-node durability is the durability of the storage beneath Cairn, with cross-host durability provided only by asynchronous replication, which has lag (Section 8, Section 20).

---

## 28. Configuration reference

### 28.1 Surface and conventions

Server configuration comes entirely from **environment variables** in the `CAIRN_*` namespace, overlaid on the built-in defaults and validated on load so that an invalid configuration fails fast with a clear message rather than at first use. Cairn deliberately has **no configuration file and no server command-line flags**: a single, explicit environment surface keeps a deployment reproducible — identical whether run directly on a host or in a container — and avoids the precedence ambiguity of layering flags over a file over the environment. (The CLI subcommands accept their own flags, as any command-line tool does; this constraint concerns the server's own configuration, not the CLI.) The table below names settings logically; each maps to a `CAIRN_<SETTING>` variable. The settings continue the operational vocabulary an operator coming from a comparable server would expect. Note that **per-bucket replication targets and rules are primarily stored S3 resource state** — set through the management API, the UI, or the CLI (Section 20), with destination credentials sealed at rest under the master key — while a default or fallback target can still be configured through the `CAIRN_REPLICATION_*` environment variables and `CAIRN_REPLICATION_TARGETS`, used for any source bucket that has no stored target.

### 28.2 Settings

| Setting | Variable | Default | Meaning |
|---|---|---|---|
| S3 API listen address | `CAIRN_LISTEN_ADDR` | `0.0.0.0:7373` | Where the S3 data-plane listener binds: the S3 protocol, the signed public-read share URLs, and the liveness, readiness, and metrics endpoints. |
| Web-UI listen address | `CAIRN_UI_ADDR` | `0.0.0.0:7374` | Where the web console, the management API, and the S3 data plane the console drives are served at the root path; set it empty, or `off`/`none`/`disabled`, to run headless with no UI listener. |
| Metadata backend | `CAIRN_META_BACKEND` | `sqlite` | Which engine drives the metadata store: `sqlite` (the bundled-C rusqlite store), `libsql` (the async embedded driver), or `turso` (the pure-Rust SQLite rewrite). |
| TLS certificate and key paths | `CAIRN_TLS_CERT_PATH` / `CAIRN_TLS_KEY_PATH` | unset (plaintext) | Enable built-in TLS when both are set; otherwise serve plaintext behind a proxy. |
| Database path | `CAIRN_DB_PATH` | under the data root | Location of the SQLite metadata file. |
| Data directory | `CAIRN_DATA_DIR` | a data root | Root of the staging and per-bucket blob directories; must share a filesystem with the database. |
| Region | `CAIRN_REGION` | `us-east-1` | The region label returned by the location operation and used in SigV4 scope checks. |
| Public base URL | `CAIRN_PUBLIC_BASE_URL` | unset | External base URL used when generating URLs behind ingress. |
| Virtual-host base domain | `CAIRN_S3_DOMAIN` | unset | The base domain for virtual-host-style addressing (`<bucket>.<domain>`); unset serves path-style only. |
| Metadata cache budget | `CAIRN_META_CACHE_BYTES` | 64 MiB | Byte budget for the metadata and configuration cache; zero disables it. |
| Maximum object size | `CAIRN_MAX_OBJECT_SIZE` | a large ceiling | Hard per-object size limit. |
| Write-ahead-log checkpoint interval | `CAIRN_WAL_CHECKPOINT_INTERVAL_SECS` | on the order of a minute | Cadence of the truncating checkpoint. |
| Write-ahead-log checkpoint size threshold | `CAIRN_WAL_CHECKPOINT_SIZE_BYTES` | 64 MiB | Trigger a truncating checkpoint between interval ticks once the log grows past this; zero leaves only the interval. |
| Multipart session lifetime and sweep interval | `CAIRN_MULTIPART_UPLOAD_LIFETIME_SECS` / `CAIRN_MULTIPART_SWEEP_INTERVAL_SECS` | a day and an hour | When idle uploads become stale and how often the sweeper runs. |
| Lifecycle scan interval | `CAIRN_LIFECYCLE_INTERVAL_SECS` | on the order of hours | How often the lifecycle scanner runs. |
| Replication interval | `CAIRN_REPLICATION_INTERVAL_SECS` | tens of seconds | How often the replication worker drains the outbox. |
| Request metrics | `CAIRN_REQUEST_METRICS_ENABLED` / `_FLUSH_SECS` / `_BUCKET_SECS` / `_RETENTION_DAYS` | on, 15s flush, 60s window, 31-day retention | Whether the usage-analytics subsystem accumulates request counts, how often the in-memory aggregator flushes and prunes, the rollup window granularity, and how long history is kept (Section 26.5). |
| Metadata durability + throughput | `CAIRN_META_SYNCHRONOUS` / `_GROUP_COMMIT_LINGER_MICROS` / `_READ_POOL_SIZE` / `_CACHE_BYTES_PER_CONN` / `_CACHE_TOTAL_BUDGET_BYTES` / `_MMAP_BYTES` | `normal`, 0, `max(8,cores)`, 64 MiB, 2 GiB, 256 MiB | Tune the metadata store for throughput (Section 30): write durability (`normal`/`full`), the group-commit linger window (≤1 ms), the WAL read-pool size (≤64), the page cache per connection, the hard total-cache budget (startup refuses a pool×cache combination over this), and the mmap size. Defaults are throughput-tuned: WAL+NORMAL with the background checkpointer as the sole checkpointer. |
| Authentication cache TTL | `CAIRN_AUTH_CACHE_TTL_SECS` | 30 s | Time-to-live for the credential + parsed-policy cache (Section 30.4); a user-identity mutation invalidates it immediately via a shared epoch, so the TTL only bounds staleness for untouched entries. Capped at 1 hour; zero disables the cache. |
| Runtime thread sizing | `CAIRN_RUNTIME_WORKER_THREADS` / `_MAX_BLOCKING_THREADS` | auto (CPU count) / auto (`max(512, blob_io_pool + read_pool + 64)`) | Tokio compute and blocking-pool sizing (Section 30.4). `0` auto-derives; an explicit blocking cap is validated to stay at or above the floor the WAL read pool plus blob I/O pool require, so neither starves. |
| Default replication target | `CAIRN_REPLICATION_ENDPOINT` / `_DEST_BUCKET` / `_ACCESS_KEY` / `_SECRET` / `_REGION` | unset | A single default destination shipped to for any source bucket without a stored target (Section 20). |
| Named replication targets | `CAIRN_REPLICATION_TARGETS` | unset | A JSON array of named destinations; a source bucket is routed to the matching named target. |
| Root administrator credentials | `CAIRN_ROOT_ACCESS_KEY` / `CAIRN_ROOT_SECRET_KEY` | a well-known default | The access key and secret of an administrator ensured on every startup; override in production. |
| Master key | `CAIRN_MASTER_KEY` | required when any encrypted secret exists | The authenticated-encryption key for secrets at rest, supplied out of band. |
| Concurrency limit and request timeout | `CAIRN_CONCURRENCY_LIMIT` / `CAIRN_REQUEST_TIMEOUT_SECS` | bounded defaults | Maximum in-flight requests and per-request timeout. |
| Development authentication bypass | `CAIRN_DEV_AUTH` | off | Enables the loopback-only development administrator (debug builds only). |
| Log level and format | `CAIRN_LOG_LEVEL` / `CAIRN_LOG_FORMAT` | informational and text | Verbosity and whether logs are text or JSON. |

Validation rejects an empty listen address or paths, a public base URL that does not parse, TLS configuration that is incomplete, non-positive timeouts, a sweep or scan interval below a sane floor, a development bind that is not loopback, the presence of encrypted secrets without a master key, and any unrecognised `CAIRN_*` variable.

---

# Part VII. Delivery

## 29. Testing and S3 conformance

The testing strategy is built to prove three things: that the S3 contract holds for real clients across the whole expanded surface, that the storage invariant survives crashes, and that the subtle parsers and the authorization engine are correct on adversarial input. The layers below build up to that.

### 29.1 Unit tests

Each module is unit-tested against an in-memory double of its dependencies, so the tests are fast and deterministic. The pointed cases include the range-seek and prefix-upper-bound helpers against empty, maximal, and multibyte inputs, because they are the correctness of listing and pagination; the durable commit sequence, asserting through a seam that the destination directory is fsynced; the SigV4 canonicalisation and signing against the published test vectors; the totality of the error translator, enumerating every internal error and asserting a defined mapping; the authorization engine, table-driven across combinations of policy, ACL, ownership mode, and public-access-block settings with expected allow-or-deny outcomes; the ACL-to-action mapping and the canned-ACL expansion; and the chunked decoder against well-formed inputs.

### 29.2 Property-based tests

Property tests assert invariants over randomised inputs. The key sanitiser never panics and never accepts a key that resolves outside the data root. Listing is correct: the concatenation of pages equals a single unbounded listing, results are sorted and free of duplicates, common-prefix grouping matches a simple reference oracle, and pagination is gap-free and repeat-free across boundaries, tested over randomised key sets, prefixes, delimiters, and page sizes. SigV4 canonicalisation matches a reference for randomised paths and queries including reserved and multibyte characters. The authorization engine satisfies its precedence properties, for instance that an explicit deny always overrides any allow and that enabling public-access blocking never broadens access.

### 29.3 Fuzzing

The chunked decoder is the foremost fuzz target, fed arbitrary bytes and arbitrary read-boundary splits and asserted never to panic, never to buffer without bound, and to emit exactly the concatenated payloads for well-formed inputs, including adversarial oversized declarations, missing separators, headers split across reads, and non-terminating streams. The XML request parsers and the policy JSON parser are also fuzzed, since they are the untrusted-input surface that motivates the choice of a memory-safe language; the key sanitiser is covered by the property tests above rather than a dedicated fuzz target.

### 29.4 Crash-consistency tests

A test-only fault seam injects a failure in the window between blob durability and metadata commit, and a test arms it, performs a write, asserts the process stops with the blob present and no row, restarts the engine, runs reconciliation, and asserts the orphan is reclaimed and the store is consistent; a second variant arms the fault between multipart assembly and the completion commit and asserts the assembled blob is reclaimed. These tests make the durability claims real rather than asserted (F-4).

### 29.5 Conformance against real clients and the standard suite

The decisive tests run real S3 clients against a running Cairn. The boto3 AWS SDK drives a matrix covering the object operations including plain, unsigned-payload, and streaming-chunked puts so that the chunked path and real SigV4 are exercised by a real client, ranged and conditional gets, heads, deletes, bulk deletes, copies, the full multipart cycle including abort and out-of-order completion, and presigned URLs, together with versioning behaviour and version listing, tagging, and copy. Independently, the MinIO warp macro benchmark drives the server as a second real client across get, put, and mixed profiles in strict mode with a zero-error gate, so a genuinely different client validates the wire under load. The boto3 conformance script runs as a CI gate covering the core object lifecycle, versioning, tagging, multipart, copy, and bulk delete, while ACL, policy, public-access-block, CORS, lifecycle, and replication round-trips are covered by the unit and integration suites rather than by the live-client script. Replication is tested end to end between two Cairn instances and with a fake sink that can simulate failures to exercise retry and backoff; lifecycle is tested with a controllable clock so that expiry, transition, and abort timing are deterministic; and compression is tested for round-trip fidelity, for correct ranged reads against compressed blobs, for the incompressibility heuristic, and for ETag invariance between compressed and uncompressed storage of the same content.

### 29.6 Benchmarks and load

Micro-benchmarks confirm that hashing, compression, and chunked decoding are not the bottleneck on the ingest path. Macro load tests drive concurrent puts and gets for both large-object bandwidth-bound and small-object rate-bound profiles using a standard object-storage load tool, report throughput and latency percentiles, and characterise the single-writer ceiling by observing the write-queue-depth metric as concurrency rises, which is how the group-commit benefit and its limit are quantified rather than assumed.

---

## 30. Performance engineering and targets

### 30.1 The honest single-node model

Cairn's performance is the performance of one host's disks, network, and CPU, used efficiently. For large-object reads the limit is disk read bandwidth or the network link, whichever is lower, served from page cache at memory speed for hot objects, and the zero-copy path keeps the CPU out of the byte path so the limit is genuinely the hardware; the target is to saturate the device or the link. For large-object writes the limit is write bandwidth plus the two fsyncs of the durable commit, and the target is to be within a small constant of raw sequential write throughput. For small-object writes the limit is the single metadata writer and the fsync rate, and this is where group commit is decisive: by coalescing many writes into one transaction and one durability barrier, the effective small-object write rate rises with the batch factor up to the point where the writer becomes CPU-bound rather than fsync-bound, which moves the ceiling from the device's synchronous-commit rate to something far higher, while preserving per-write durability. Reads of metadata and listings scale with the read-connection pool and the cores, independent of the write rate, because WAL readers do not block the writer. Compression trades CPU for I/O and space: for compressible data on fast CPUs it can raise effective throughput by moving fewer bytes, and for incompressible data the heuristic avoids paying the CPU for no gain.

### 30.2 The write-rate ceiling and how to see it

The single writer is a real ceiling on small-object write throughput, the same ceiling the reference design has, now made explicit, raised by group commit, and instrumented. The write-queue-depth metric is the operator's window onto it: a depth that grows under load means small-object writes are the binding constraint, at which point the operator can accept the ceiling as the honest limit of a single node and scale by running more nodes with replication. The default metadata posture is already tuned for throughput rather than maximum durability: `PRAGMA synchronous=NORMAL` under WAL removes the per-commit fsync (group commit then commits at CPU speed, not fsync speed), and `PRAGMA wal_autocheckpoint=0` hands all checkpointing to the background truncating checkpointer so the writer never stalls mid-commit to checkpoint inline. Measured on a 2-vCPU host with the database on a real disk, those two changes raise the small-object metadata commit rate from roughly 13k to roughly 37k commits per second (≈2.8×) at 64 concurrent writers — the writer stops being the binding constraint for most workloads. An operator who needs zero-loss durability sets `CAIRN_META_SYNCHRONOUS=full`, which reinstates the per-commit fsync (and for which the group-commit linger again earns its keep, batching those fsyncs).

### 30.3 Tuning knobs and their effects

The metadata store is tuned entirely through the `CAIRN_META_*` environment surface (Section 28.2). The **synchronous level** (`CAIRN_META_SYNCHRONOUS`, default `normal`) trades durability for small-write rate: `normal` under WAL never corrupts and loses at most the last uncheckpointed transaction on power loss, which the blob-first write ordering downgrades to a reconcile-collected orphan blob rather than a torn store. The **group-commit linger** (`CAIRN_META_GROUP_COMMIT_LINGER_MICROS`, default `0`, capped at 1 ms) trades latency for batching, and helps only under `synchronous=full` — under `normal` there is no per-commit fsync to amortize, so a linger only adds latency (this was measured, not assumed). The **read-pool size** (`CAIRN_META_READ_POOL_SIZE`, default `max(8, cores)`, capped 64) raises read concurrency over independent WAL snapshots that never block the writer. The **per-connection cache** (`CAIRN_META_CACHE_BYTES_PER_CONN`) keeps hot pages resident; a total-budget clamp (`CAIRN_META_CACHE_TOTAL_BUDGET_BYTES`) refuses at startup any pool-size × cache combination that would risk OOM. `temp_store=MEMORY`, `journal_size_limit`, `analysis_limit`, and memory-mapping (`CAIRN_META_MMAP_BYTES`) reduce scratch I/O, bound the WAL footprint, and cut read syscalls. Each knob has a defined effect, so tuning is reasoning rather than guessing.

### 30.4 Per-request hot-path optimizations

Beyond the writer tuning of Section 30.2, a sequence of optimizations removes per-request work from the hot paths so the metadata commit and the WAL read pool stop being the binding constraint for read- and mixed-heavy workloads:

- **Authentication cache.** Every authenticated request otherwise paid two metadata reads (credential lookup by access-key-id, identity-policy load by user-id) plus a JSON policy parse *before* its own work. A short-lived cache (`CAIRN_AUTH_CACHE_TTL_SECS`, default 30 s, 0 disables) memoizes the verified credential (the sealed secret and the user fields a principal needs — never the plaintext secret) and the parsed policy. Coherency is a shared *auth epoch* the metadata layer bumps on every user-identity mutation (create/update/deactivate/set-policy), so a credential or policy change takes effect immediately by dropping every cached entry; the TTL is only a staleness backstop. The day-scoped SigV4 signing key is still re-derived per request from the sealed secret, so the signature-verification math is unchanged.
- **Prepared-statement caching.** The single writer re-runs a fixed set of hot statements (insert/demote/upsert/quota/enqueue/activity/metrics) on every mutation; these are compiled once and cached (rusqlite `prepare_cached`; the libSQL driver gains an equivalent per-connection statement cache).
- **Partial covering index.** A partial index over current rows only (`WHERE is_latest = 1`) carrying the full `ListObjects` projection makes latest-only listing index-only — no per-row table fetch and no stepping over historical versions.
- **Maintained roll-up counters.** Per-bucket and per-user counters (object/version counts and logical/physical bytes), maintained inside the writer transaction on every row insert/delete, turn the overview aggregates from a scan of every version into an O(buckets) read, and turn byte-quota enforcement from a per-write `SUM` scan into an O(1) counter read.
- **Lock-free account-wide reads.** The account public-access-block (four booleans) is packed into a single atomic, so the very-hot account-wide authorization read takes no lock. The cached config reads carry a generation-epoch re-check so an invalidation that races a reader's database round-trip can never install a stale value.
- **Blob path.** Object durability uses `fdatasync` (data + size, not timestamps); concurrent same-bucket commits coalesce their directory fsync into one barrier; the GET path opens and stats the file once (reusing the descriptor for the kernel zero-copy send) and defers the streamed reader until it is actually polled; the copy permit is released before the fsync barrier so reads do not queue behind writers' barriers; and the plaintext `sendfile` fast path serves single-`Range` GETs, not only full-object GETs.
- **Runtime sizing.** The Tokio worker and blocking-thread counts are explicit and validated (`CAIRN_RUNTIME_WORKER_THREADS` / `_MAX_BLOCKING_THREADS`); the blocking pool is floored at the combined concurrency the WAL read pool and blob I/O pool demand, so neither can starve the other.

Two roadmap items remain unrealized and are documented here for honesty. A second metadata database with its own writer for the high-churn best-effort tables (outbox/metrics/activity) was deprioritized: under the `synchronous=NORMAL` posture there is no per-commit fsync for a second stream to parallelize, and splitting would require a cross-file data migration for existing deployments, so its benefit does not justify the structural risk. Concurrent multi-writer execution via the Turso engine's `BEGIN CONCURRENT`/MVCC is blocked by the pinned Turso version, which parses the syntax but exposes no supported way to enable MVCC; it remains a future option gated on a Turso release that surfaces the feature.

---

## 31. Build, packaging, deployment, and operations

### 31.1 Build and packaging

Cairn builds with a stable Rust toolchain into a single binary, with a release profile favouring optimisation and a small stripped artifact, and it targets a fully static build so the container needs no system libraries, with the C SQLite compiled in so there is no external database dependency. The embedded UI is produced by the two-stage build of Section 23.2, so a release build yields one binary that contains the server, the management UI, and the CLI subcommands, and a feature switch can omit the UI for a smaller artifact. The container image is built in stages with dependency caching so that incremental builds are fast, and the final image is a minimal distroless or empty base holding just the static binary, which keeps the attack surface and the image size small.

### 31.2 The one-filesystem requirement

The database file, the staging directory, and the per-bucket blob directories must reside on the same filesystem, because the atomic-rename step of the commit protocol works only within a filesystem and a cross-device rename fails; the staging directory is therefore inside the data root by design. This is an operational invariant the documentation states plainly, because violating it by, for instance, placing the staging directory on a different mount would break the durability protocol.

### 31.3 Deployment shapes

Two deployment shapes are first-class. Cairn can terminate TLS itself, reading certificate and key material from configured paths, which lets it run as a standalone secure endpoint. Or it can run behind a terminating reverse proxy on a trusted interface, in which case the proxy must pass the authorization, range, conditional, and S3-specific headers through unchanged and must stream rather than buffer large request and response bodies, so that Cairn's streaming and backpressure are not defeated by a buffering proxy. For ingress with an external hostname the public base URL is configured so generated URLs are correct. Either shape requires the data filesystem to be on redundant storage as discussed below.

### 31.4 Backup and restore

Because the metadata database is the source of truth and blobs are immutable files named by identifier, a consistent backup follows a defined order with a clear consistency argument (F-19). First, the database is snapshotted consistently using the engine's online backup or copy-into facility, which yields a single transactionally-consistent file without stopping writes. Second, the blob directories are copied. Taking the database snapshot first and copying blobs second guarantees that the copied blob set is a superset of what the snapshot references, because every object in the snapshot existed at snapshot time and its immutable blob existed then and is never renamed, so the restore finds a blob for every row, with at most some extra blobs from objects written after the snapshot, which reconciliation harmlessly reclaims on restore. The one residual edge case is an object deleted in the window between the database snapshot and the blob copy, whose blob the copy may miss while the snapshot still references it, leaving a row without a blob on restore; the mitigations, in increasing strength, are to accept it as a single just-deleted object, or to copy blobs first and snapshot second and run reconciliation in its repair mode on restore so such rows are pruned rather than left dangling, or to quiesce writes briefly during the snapshot for a perfectly consistent backup. The default documented procedure is the database-first order with acceptance of the tiny window, with the repair-mode and quiesce options available for operators who need stronger guarantees. The master key is deliberately excluded from the backup that contains the database, so that the backup alone does not disclose the encrypted secrets. Restore places the snapshot and the blobs and starts with reconciliation enabled.

### 31.5 Day-two operations and storage guidance

In steady-state operation the signals to watch are the write-queue depth for write saturation, the write-ahead-log size for checkpoint health since a log that grows without bound indicates long-lived readers starving the checkpointer, the reconciliation counts for startup integrity, the rate of out-of-space responses against storage capacity, and the replication lag and failure counts for the health of redundancy. Because Cairn does not implement drive redundancy, the operational guidance is to place the data filesystem on redundant storage appropriate to the deployment, such as a checksumming redundant filesystem that also detects silent corruption, software or hardware RAID, or a cloud block volume with provider redundancy, and to rely on the optional background scrub for early warning of bit rot and on bucket replication for cross-host and cross-site copies. Capacity, write saturation, and replication failures are the natural alerting targets, and the metrics of Section 26 expose all three.

---

## 32. Phased implementation roadmap

Build in this order. Each phase lists its deliverable and the acceptance criteria that gate the next. The ordering front-loads the foundations, durability and the abstraction boundary, so that later feature work cannot quietly undermine them, and it sequences the large access-control and feature subsystems after the object core is solid.

**Phase 0, skeleton.** The workspace and crate layout, the shared types and the interface definitions, configuration parsing and validation across the full surface, the server standing up the HTTP stack with the middleware, health, readiness, metrics, structured tracing, and ordered graceful shutdown. Accepts when the binary starts, serves health and metrics, validates configuration including the rejection cases, and shuts down cleanly, and when continuous integration builds the workspace and the static target.

**Phase 1, storage and metadata foundations.** The SQLite metadata store with its writer-and-read-pool topology, group commit, migrations, the schema, the repositories, the range-seek helpers, and the cache; the local blob store with the durable commit sequence including directory fsync, ranged reads, multipart staging and assembly, bounded reconciliation, and path safety; and in-memory doubles of both interfaces. Accepts when the unit, property, and crash-consistency tests for these pass, when reconciliation is demonstrably bounded in memory at scale, and when the durability ordering is shown to hold under fault injection.

**Phase 2, authentication and secrets.** The Bearer and SigV4 header and presigned authenticators and the chain, the bootstrap, envelope encryption of secrets at rest, and the principal-establishing middleware, with the development bypass behind its feature. Accepts when SigV4 passes the published vectors, the bootstrap mints an administrator once on an empty store, an inspection of the database finds no plaintext secrets, and a release build cannot bypass authentication.

**Phase 3, S3 object and bucket core.** Putting objects including the streaming chunked decoder, getting with range and conditionals, heading, deleting, creating and deleting and heading buckets, returning location, the object and version-one and version-two listings with prefix and delimiter and pagination, the error mapping, and the size-limit and out-of-space handling. Accepts when the AWS SDK matrix for these passes including SDK-driven chunked puts, the chunked decoder fuzzing is clean, and listing conformance matches the reference across pagination.

**Phase 4, multipart.** Initiation, part upload including by copy, completion with the correct multipart ETag and the double-completion guard, abort, and the sweeper. Accepts when the SDK multipart cycle passes including abort and out-of-order completion, the multipart ETag matches the reference, and a fault during assembly reclaims the blob.

**Phase 5, copy, bulk delete, and public reads.** Copy including same-key metadata replace and conditional copy, bulk delete bounded in memory, and the signed public-read URLs. Accepts when the SDK copy and bulk-delete conformance passes, signed URLs verify and expire correctly, and bulk delete of a large set stays bounded.

**Phase 6, versioning.** The three versioning states, version identifiers, delete markers, version-aware get, delete, and listing, and the suspended-state semantics. Accepts when the SDK versioning and version-listing conformance passes and the state transitions behave as specified.

**Phase 7, authorization engine.** The policy-evaluation engine, ACLs and canned ACLs, Block Public Access, and Object Ownership including the ACLs-disabled mode, wired into the request pipeline with the specified precedence. Accepts when the table-driven authorization tests pass, the public-access-block and ownership behaviours match the reference, and the conformance suite's policy and ACL sections pass for the supported subset.

**Phase 8, tagging and CORS.** Object and bucket tagging with limits and their use in lifecycle filters and policy conditions, and per-bucket CORS with preflight and actual-request handling. Accepts when the SDK tagging and CORS conformance passes and tag-conditioned policy and tag-filtered lifecycle behave correctly.

**Phase 9, lifecycle.** The lifecycle configuration, the scanner engine, expiration under versioning, noncurrent-version expiration, incomplete-upload abort, and transition to a remote cold tier with transparent reads. Accepts when lifecycle actions apply correctly under a controllable clock, expiration respects versioning, and transitioned objects read back transparently.

**Phase 10, bucket replication.** The replication configuration, the durable outbox, the worker pool and the sink, retry and backoff, loop prevention, status tracking, and the metrics, requiring versioning on the source. Accepts when replication between two instances converges, retry and backoff behave under a failing sink, loops are prevented, and lag and failures are observable.

**Phase 11, compression.** The per-bucket block compression with the self-describing blob format and the block index, range-friendly reads, the incompressibility heuristic, ETag invariance, and the logical-versus-physical accounting. Accepts when compressed round-trips are faithful, ranged reads against compressed blobs are correct, the heuristic avoids enlarging incompressible data, and the ETag is invariant to compression.

**Phase 12, control plane.** The management API, the embedded React UI built and compiled into the binary, and the CLI with its remote and node-local commands. Accepts when each management endpoint behaves to its contract and is administrator-gated, the binary serves the embedded UI and the UI performs the documented operations, and the CLI performs both remote administration and the local bootstrap, integrity, and backup operations.

**Phase 13, hardening and observability completion.** The full metric set wired across the request path and the background subsystems, quotas, native TLS, the concurrency limit and timeouts, and the final security review against the threat model. Accepts when the metrics move correctly under load, quotas reject over-limit writes with the right error, native TLS serves correctly, and a soak test shows stable memory and no thread growth under sustained large-transfer concurrency.

**Phase 14, conformance, benchmarks, and documentation.** The cross-client conformance suite and the community conformance suite green for the supported surface in continuous integration, the benchmarks with published numbers and the characterised write ceiling, and the operator documentation derived from the configuration and operations sections. Accepts when the clients and the suite pass in continuous integration, the benchmark numbers are recorded, and backup and restore including the repair mode are exercised end to end. This phase is the production release gate.

**Phase 15, future work behind the interfaces.** The io_uring blob engine and the zero-copy read fast path with kernel TLS, an explicit restore-from-cold workflow, full-blob transparent encryption at rest as a blob-store decorator and SSE-KMS (object-level SSE-S3 already ships, with the per-object DEK sealed under the master key), and the protective version-deletion control. None of these require changes to the protocol or control layers, which is the payoff of the abstraction boundary.

---

## 33. Architecture decision log

| Decision | Summary | Drivers |
|---|---|---|
| Runtime | A multi-threaded asynchronous runtime in the initial scope, with the blob path able to escalate to io_uring behind the blob interface. | Portability and ecosystem now; the fastest disk path available later without protocol changes. |
| Metadata concurrency | One serialized, group-committing writer task plus a pool of read-only WAL connections. | Matches SQLite's real model, eliminates write contention, scales reads, and raises small-write throughput via batching. |
| Durability sequence | Stage, fsync the file, rename, fsync the directory, validate, commit metadata, then reclaim. | Crash consistency; a committed row never references a non-durable blob, fixing the reference design's missing directory fsync. |
| Read path | Buffered streaming by default with a zero-copy file-to-socket fast path, and kernel TLS where available, behind a feature. | Production read performance for large objects while keeping a portable default and isolating platform-specific unsafe code. |
| Storage model | Preserve the reference model: opaque-identifier blobs, metadata as source of truth, temp-then-rename, reconciliation, signed public reads. | The model is correct and is what makes versioning and the rest accrete in metadata rather than on disk. |
| Abstraction boundary | Blob store, metadata store, authenticator, authorization engine, replication sink, and supporting interfaces as the spine. | Unit-testability with in-memory doubles and swappable backends, including the cold tier and a future metadata engine. |
| Metadata engine | The mature C SQLite compiled in for the initial scope, behind the metadata interface so a pure-Rust SQLite can replace it later. | Maturity and full SQL today; honours the preference for a Rust-native engine without blocking on its readiness. |
| Compression | Optional per-bucket, off by default, block-based with a self-describing blob format and a block index. | Space saving without breaking the simple default, while keeping ranged reads efficient and the ETag plaintext-based. |
| Authorization | A real engine with explicit precedence: explicit deny wins, public-access blocking gates public grants, any allow otherwise suffices, default deny. | Faithful S3 access-control semantics and conformance, with a clear security model. |
| Object ownership | Recommend the ACLs-disabled enforced mode as the default for new buckets. | Removes the most common accidental-exposure vector. |
| Replication | Asynchronous, outbox-driven, at-least-once with idempotent application, requiring versioning on the source. | Cross-host redundancy without clustering, well-defined and idempotent through stable version identity. |
| Versioning | Designed in as a substrate that lifecycle and replication build on, with three states matching S3. | Cheap under the blob model and required by the features that depend on it. |
| Control plane | One JSON management API consumed by both an embedded React UI compiled into the binary and a CLI. | A single artifact to deploy and parity between browser and terminal administration. |
| Transport | Optional native TLS in addition to running behind a terminating proxy. | A standalone secure endpoint without mandating a proxy. |
| Secrets at rest | Envelope-encrypt SigV4 and replication secrets under an out-of-band master key; hash Bearer secrets. | Database disclosure must not yield usable secrets; high-entropy tokens need only a fast hash. |

---

## 34. Appendices

### 34.1 Metadata schema, field by field

The schema below is the reference for the SQLite store. Types are given in the engine's affinity terms; timestamps are stored in a sortable textual or integer form consistently; and the configuration aspects that arrive as documents are stored as validated text. Identifiers are opaque strings.

**Users.**

| Field | Type | Constraints and notes |
|---|---|---|
| id | text | Primary key, opaque identifier. |
| display_name | text | Not null. |
| access_key_id | text | Unique, the Bearer access-key identifier. |
| secret_hash | text | Not null, a fast cryptographic hash of the Bearer secret. |
| sigv4_access_key_id | text | Unique, nullable; a user may lack SigV4 credentials. |
| sigv4_secret_ciphertext | blob | Nullable; the SigV4 secret under authenticated encryption. |
| sigv4_secret_nonce | blob | Nullable; the nonce for the above. |
| role | text | Not null, constrained to administrator or member. |
| is_active | integer | Not null, default active. |
| quota_bytes | integer | Nullable; an optional per-user byte quota, null meaning unlimited. |
| policy | text | Nullable; an AWS-IAM-style principal-less identity policy attached to the user, evaluated in union with bucket policy and ACL; null meaning the user has no identity policy. |
| created_at, updated_at | timestamp | Not null. |

**Buckets.**

| Field | Type | Constraints and notes |
|---|---|---|
| name | text | Primary key. |
| owner_id | text | Not null, references a user. |
| created_at | timestamp | Not null. |
| versioning_state | text | Not null, one of unversioned, enabled, suspended. |
| ownership_mode | text | Not null, the object-ownership mode governing ACL participation. |
| region | text | Not null, the label returned by the location operation. |
| compression_policy | text | Nullable; the per-bucket compression policy document, absent meaning off. |
| quota_bytes | integer | Nullable; an optional per-bucket byte quota enforced inside the commit transaction, null meaning unlimited. |

**Bucket configuration aspects.** Each of the following is a validated document associated with a bucket, stored either as a column on a bucket-configuration table keyed by bucket name or as its own table; the choice is an implementation detail, but each is one logical document per bucket: the policy, the access-control list, the CORS configuration, the lifecycle configuration, the replication configuration, the replication remote-target descriptors (the ARN-identified targets whose secrets are sealed under the master key), the default-encryption setting (SSE-S3 applied to new uploads that carry no SSE header), and the tag set. The account-wide and per-bucket public-access-block settings are stored as their four boolean toggles.

**Object versions.**

| Field | Type | Constraints and notes |
|---|---|---|
| id | text | Primary key, opaque identifier, also the basis of the storage path. |
| bucket_name | text | Not null, references a bucket. |
| key | text | Not null. |
| version_id | text | Not null; a sentinel value for unversioned or suspended single versions. |
| is_latest | integer | Not null; whether a plain get resolves to this version. |
| is_delete_marker | integer | Not null; a delete marker carries no blob. |
| size_logical | integer | Not null; the plaintext length reported to clients. |
| size_physical | integer | Not null; the on-disk length, operator-visible only. |
| etag | text | Not null; plaintext MD5 or the multipart form, unquoted. |
| content_type | text | Not null, with a default. |
| content_encoding | text | Nullable; the standard S3 system-metadata header echoed back on get and head. |
| cache_control | text | Nullable; the standard S3 system-metadata header echoed back on get and head. |
| content_disposition | text | Nullable; the standard S3 system-metadata header echoed back on get and head. |
| content_language | text | Nullable; the standard S3 system-metadata header echoed back on get and head. |
| expires | text | Nullable; the standard S3 system-metadata header echoed back on get and head. |
| storage_path | text | Nullable for delete markers; the opaque blob path otherwise. |
| compression | text | Not null; the algorithm and block size, or a marker that the blob is uncompressed. |
| cold_locator | text | Nullable; the remote locator when the version has been transitioned to the cold tier. |
| sse_descriptor | text | Nullable; the SSE-S3 data-encryption key sealed under the master key, null meaning the object data is unencrypted. |
| storage_class | text | Not null; standard, or the cold tier after transition. |
| owner_id | text | Not null; the version's owner under the ownership mode. |
| user_metadata | text | The user-defined metadata entries. |
| acl | text | Nullable; the object ACL where ownership keeps ACLs in force. |
| checksums | text | Nullable; any client-supplied checksums. |
| replication_status | text | Nullable; pending, completed, failed, or replica, for replication-enabled buckets. |
| created_at, updated_at | timestamp | Not null. |
| Unique key | | The combination of bucket, key, and version identifier is unique. |
| Index | | Over bucket and key and version ordering, for current-version lookup and version listing. |

**Object tags.** Associated with an object version, a small set of key-value pairs, stored so they can be returned by the tagging operations and matched by lifecycle and policy.

**Multipart uploads.**

| Field | Type | Constraints and notes |
|---|---|---|
| id | text | Primary key, the upload identifier. |
| bucket_name | text | Not null, references a bucket. |
| key | text | Not null. |
| content_type | text | Not null, with a default. |
| status | text | Not null, one of active, completing, aborted. |
| owner_id | text | Not null. |
| intended_acl | text | Nullable; the ACL to apply on completion. |
| user_metadata | text | The metadata to apply on completion. |
| created_at, updated_at | timestamp | Not null. |
| Index | | Over status and update time, for the sweeper. |

**Multipart parts.**

| Field | Type | Constraints and notes |
|---|---|---|
| upload_id | text | Not null, references the upload, cascade on delete. |
| part_number | integer | Constrained to the valid part-number range. |
| size | integer | Not null; the plaintext part size. |
| etag | text | Not null; the plaintext MD5 of the part. |
| storage_path | text | Not null. |
| checksum | text | Nullable. |
| Primary key | | The combination of upload and part number. |

**Replication outbox.**

| Field | Type | Constraints and notes |
|---|---|---|
| id | text | Primary key. |
| bucket_name, key, version_id | text | Not null; the version this entry concerns. |
| operation | text | Not null; an object creation or a delete-marker propagation. |
| rule_id | text | Not null; the replication rule. |
| attempts | integer | Not null; the retry count. |
| next_attempt_at | timestamp | Not null; the backoff schedule. |
| priority | integer | Not null, default 0; higher dispatches first, carried from the matching rule. |
| status | text | Not null; pending, claimed, completed, or failed. |
| lease_until | integer | Nullable; the claim-lease expiry, set with the claimed status by the atomic claim so a stalled claim can be reclaimed. |
| last_error | text | Nullable. |
| Index | | Over status and next-attempt time, for due-entry claiming. |

**Activity and audit, and migrations.** The activity log records an identifier, an action, the bucket and key where applicable, the size and ETag where applicable, the actor, and a timestamp, indexed by time. The migrations table records each applied schema version and when it was applied.

### 34.2 Indexes summary

The indexes that carry the load are the unique index over bucket, key, and version that serves current-version lookup and version listing through range seeks; the unique indexes over the two access-key identifiers for authentication; the index over multipart status and time for the sweeper; and the index over outbox status and next-attempt time for replication claiming. Listing performance depends on the bucket-key-version index being used as a half-open range seek rather than a scan, which the query construction guarantees.

### 34.3 S3 API support matrix

| Operation | Supported | Notes |
|---|---|---|
| ListBuckets | Yes | |
| CreateBucket, DeleteBucket, HeadBucket | Yes | Delete requires empty; force-empty via the management surface. |
| GetBucketLocation | Yes | Returns the configured region. |
| GetBucketVersioning, PutBucketVersioning | Yes | Three states. |
| GetBucketPolicy, PutBucketPolicy, DeleteBucketPolicy | Yes | With the policy engine. |
| GetBucketAcl, PutBucketAcl | Yes | Where ownership keeps ACLs in force. |
| GetBucketCors, PutBucketCors, DeleteBucketCors | Yes | Per-bucket CORS. |
| GetBucketLifecycle, PutBucketLifecycle, DeleteBucketLifecycle | Yes | Expiration, noncurrent expiration, abort-incomplete, transition. |
| GetBucketReplication, PutBucketReplication, DeleteBucketReplication | Yes | Requires versioning enabled. |
| GetBucketTagging, PutBucketTagging, DeleteBucketTagging | Yes | |
| GetBucketOwnershipControls, PutBucketOwnershipControls | Yes | Including the ACLs-disabled mode. |
| GetPublicAccessBlock, PutPublicAccessBlock, DeletePublicAccessBlock | Yes | Account and bucket level. |
| ListObjectsV2, ListObjects (v1) | Yes | Prefix, delimiter, pagination, start-after or marker. |
| ListObjectVersions | Yes | |
| ListMultipartUploads | Yes | |
| PutObject | Yes | Plain, unsigned-payload, and streaming-chunked bodies; conditional writes; inline tags and ACL and metadata. |
| GetObject, HeadObject | Yes | Range, conditionals, response-header overrides, version selection. |
| DeleteObject | Yes | Delete marker in a versioned bucket; permanent with a version identifier. |
| DeleteObjects (bulk) | Yes | Bounded; up to the request cap. |
| CopyObject | Yes | Metadata replace or preserve; conditional copy; same-key metadata change. |
| CreateMultipartUpload, UploadPart, UploadPartCopy, ListParts, CompleteMultipartUpload, AbortMultipartUpload | Yes | Correct multipart ETag; double-completion guarded. |
| GetObjectAcl, PutObjectAcl | Yes | Where ownership keeps ACLs in force. |
| GetObjectTagging, PutObjectTagging, DeleteObjectTagging | Yes | |
| GetObjectAttributes | Yes | |
| Presigned GET and PUT | Yes | SigV4 query form. |
| Signed public-read URL | Yes | A Cairn extension, not an S3 operation. |
| Object-level SSE-S3 (`x-amz-server-side-encryption: AES256`) | Yes | Accepted on writes and echoed on reads, with the per-object data-encryption key sealed under the master key; a per-bucket default-encryption setting applies it to new uploads that carry no SSE header. `aws:kms` is rejected as unsupported. |
| GetBucketEncryption, PutBucketEncryption (the `?encryption` subresource) | No | The REST subresource returns not implemented; default encryption is set through the management plane. |
| Object lock and retention, requester pays, website and accelerate and analytics and inventory and metrics configurations | No | Out of scope; requests are answered as not implemented. |

### 34.4 Supported policy action catalogue

Each policy action names an operation or family that a statement can allow or deny. The catalogue maps the supported actions to the operations they govern: the object-read actions govern getting objects, their versions, their ACLs, their tags, and their attributes; the object-write actions govern putting objects, their ACLs, and their tags, and deleting objects and their versions and tags; the bucket-listing action governs listing a bucket's objects and versions and multipart uploads; the bucket-read and bucket-write configuration actions govern getting and putting each bucket configuration aspect, namely policy, ACL, CORS, lifecycle, replication, tagging, versioning, ownership, and public-access-block; the multipart actions govern initiating, uploading, listing, completing, and aborting uploads; and the bucket-existence actions govern creating, deleting, and heading buckets. The wildcard and prefix-wildcard forms expand over this catalogue so that a policy can grant a family of actions. The exact action names follow the storage-service action namespace, and the catalogue is the authoritative list of which names Cairn recognises; an unrecognised action in a statement simply never matches.

### 34.5 Supported condition-key catalogue

The condition keys Cairn evaluates, with their source, are: the request's source network address, taken from the connection or the trusted forwarded header behind a proxy; whether the transport was secure; the request's referer and user-agent headers; the current time; the principal type distinguishing anonymous from authenticated; and the storage-service-specific keys, namely the listing prefix, the listing delimiter, and the listing maximum-keys for list operations, the canned-ACL header supplied on a write, the content-sha256 header, the version identifier targeted by a request, and the keys that match against an object's existing tags and against the tags supplied on a request. Each key is usable with the operators its type supports, namely the string operators, the boolean operator, the address operators, the numeric operators, the date operators, the existence operator, and the absent-key qualifier, with the set qualifiers applied to multi-valued keys. A condition naming a key Cairn does not evaluate causes its statement not to match, which keeps an unknown condition from ever broadening access.

---

