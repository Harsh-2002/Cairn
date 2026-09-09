# Data plane: system model, concurrency, and I/O

> Part of the Cairn reference docs. The section numbers below are stable identifiers used throughout the code and docs; see the index in [`CLAUDE.md`](./CLAUDE.md) and [`../CLAUDE.md`](../CLAUDE.md).

## 6. System overview: data plane, control plane, node model

### 6.1 The node model

A Cairn deployment is one process on one host owning one data filesystem. That process is the entirety of the system for that deployment: there is no coordinator, no metadata service, no separate gateway. Multiple Cairn deployments relate to each other only through asynchronous bucket replication (Section 20), which is an S3 client relationship, not a cluster membership. This is the deliberate simplicity at the heart of the design (Section 2.3). The unit of scaling is the host: a bigger host, faster disks, and more network serve more load; cross-host capacity and redundancy come from running more independent deployments and replicating buckets between them.

### 6.2 Data plane and control plane

Within the process, two logical planes share the same address space and the same metadata store but are reasoned about separately.

The **data plane** is the S3 request path: accept a connection, parse and authenticate and authorize the request, and move object bytes between the socket and the disk, touching metadata at the commit point. It is latency- and throughput-critical and is where the I/O model (Section 7) and the durability model (Section 8) live.

The **control plane** is everything that configures and observes the system: the management API and its two clients (the embedded web console and the CLI), the background subsystems (lifecycle scanner, replication worker pool, multipart sweeper, WAL checkpointer, webhook delivery, integrity scrub, S3-import worker, outbox-prune plus the key-rewrap and counter-sync workers), and bootstrap. The control plane is not on the object hot path; it values correctness, observability, and clear operator semantics over raw throughput.

The two planes are separated at the socket boundary, not merely by a path-prefix convention. The data listener accepts S3, STS, persistent shares, and the infrastructure endpoints; it rejects the exact `/api/v1` namespace and never serves the embedded console. The optional control listener accepts only `/api/v1` and concrete embedded console assets; an unknown control path never falls through to S3 or a public share. Setting `CAIRN_WEB_ADDR=off` therefore removes the management socket rather than relocating management routes onto the data port. A reverse proxy must preserve this split as two distinct browser origins: object bytes and public shares belong to the data origin, while the httpOnly administrator cookie belongs only to the control origin.

### 6.3 The request path, end to end

Every S3 request traverses the same outer pipeline before reaching an operation handler. A connection is accepted by the async HTTP server, optionally over built-in TLS (Section 7.7) or behind a terminating proxy. A middleware stack, ordered deliberately, applies: assignment of a request identifier and the opening of a tracing span; structured access logging; a global concurrency limit and a request timeout; a request-body size guard; and CORS handling, which for Cairn is per-bucket and therefore partly deferred into the handler (Section 18). The request is then routed by host and path and query string into one of the four families. For S3 requests, authentication (Section 14) establishes the principal, and authorization (Section 15) evaluates the combined decision of Block Public Access, bucket policy, ACL, and ownership for the specific action and resource. Only then does the operation handler run, depending solely on the internal interfaces (Section 12) rather than on concrete storage. The handler's result is rendered to the wire as S3 XML or, for the management API, JSON, with errors passed through the single error translator (Section 25).

### 6.4 The subsystems at a glance

The process hosts, besides request handling: the metadata writer task and the read-connection pool (Section 11); the blob I/O facility (Section 9); the replication engine and its worker pool (Section 20); the webhook event-delivery engine; the lifecycle scanner (Section 19); the multipart-upload sweeper; the background S3-import worker; the WAL checkpointer; the opt-in integrity scrub (Section 26.4); the opt-in encrypted-suspect replication audit loop; and the metrics and audit facilities (Section 26). Startup wires these, runs reconciliation (Section 8), and only then binds the listener. Shutdown reverses this in a defined order (Section 31).

The shutdown order is externally observable and therefore contractual. The signal coordinator first withdraws readiness and only then publishes the stop signal. Accept loops stop accepting, and every periodic background worker checks the signal before beginning another scan, claim batch, or pass. Existing HTTP connections and already-started ordinary worker work drain concurrently under one 30-second grace; every retained task handle is joined, and an overrun is aborted and then joined rather than detached. The optional blocking `sendfile` path retains a duplicate socket on the async side: shutdown closes that duplicate to interrupt a stalled peer write and awaits the blocking closure before its HTTP connection can report stopped. A cancelled replication, webhook, or import operation leaves its durable lease/cursor intact; pre-bind startup recovery releases orphaned replication claims on every metadata shard and resumes durable import state. Storage ownership is durable (Section 8). Before admission, every physical write captures a fresh
plan and a bounded recovery slot. The Writer persists intent before permitting filesystem creation;
publication consumes that exact intent and atomically records cleanup debt. Cancellation or lost
acknowledgement transfers the plan and slot to the retained FIFO recovery consumer. It waits for
actual backend jobs and kernel file ownership to quiesce before resolving through the Writer.
The I/O lease retains node exclusivity even after its async request disappears. The consumer stays
alive until both HTTP connections and ordinary workers, including imports that invoke the S3
service, have returned or been aborted and joined. Only then is its stop sentinel sent and the
consumer joined under a separate bound. A late producer must never enqueue behind that sentinel.
Fresh pre-bind generations fence old attempts on every shard, resolve unfinished intent, recover
multipart/replication claims and run mandatory full reconciliation. Ambiguous ownership or failed
namespace synchronization preserves debt and prevents a clean recovery claim. Only after HTTP, ordinary workers, and the request-tail recovery consumer are gone does Cairn run a separately bounded final tail: drain request metrics, persist each SQLite shard's master-key seal counter, and checkpoint each SQLite WAL, in that order. Shutdown logs success only if every enabled drain and final operation completed; a timeout, error, or busy final checkpoint is reported as incomplete finalization.

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

Object bytes are handled separately from metadata. On the baseline runtime, file operations execute on a dedicated, bounded blocking pool rather than the runtime's general-purpose blocking pool, so a flood of large transfers cannot exhaust threads needed elsewhere and cannot block the asynchronous reactor that drives request parsing and the network. The size of this pool is tuned to the useful I/O concurrency of the underlying device. Live request-path transfers are streamed in bounded chunks with end-to-end backpressure: the rate at which bytes are read from the network is coupled to the rate at which they are written to disk (and vice versa on reads), so a slow disk slows the network read rather than buffering unboundedly in memory, and a slow client slows the disk read rather than reading the whole object into memory. Memory use per live transfer is a small constant. Asynchronous replication (Section 20.4) hashes and reopens immutable logical ranges for signed streaming, using multipart above 64 MiB. A process-wide weighted budget covers decoder/index/frame buffers and bounded completion XML across every worker and destination. Cancellation retains admission until any blocking reader exits; a single delivery deadline covers admission, both read passes and all destination requests.

### 7.5 The write data path

Before consuming bytes, the handler plans all temporary, final and optional spool names, and
awaits durable Writer admission under the current process generation (Section 8). Part quota
reservation and completion claim acquisition include this admission in their existing savepoint.
This additional ordinary PUT/Copy transaction is a durability cost; group commit can share its
barrier but cannot remove the dependency before I/O.

On a write, bytes arrive from the socket as a stream. They pass through a fan-out that simultaneously feeds the content hashers (always the MD5 that becomes the ETag, plus any client-requested checksum algorithms, computed once over the plaintext) and the disk writer, sharing the same buffers by reference rather than copying. When the object's length is known in advance from the content-length header, the default staging backend attempts `KEEP_SIZE` preallocation for objects at least 1 MiB, allowing contiguous placement without extending the visible file length. Placement hints are best-effort; actual writes remain responsible for reporting ENOSPC. After all bytes, framing and buffered writes are flushed, finalization releases reserved allocation beyond the descriptor's actual EOF before the existing file sync. It never truncates to the plaintext or declared input length; encrypted framing can exceed either. Failure to inspect or trim a hinted file aborts publication. The optional io_uring backend does not preallocate. For large transfers the kernel can be advised that access is sequential and, after the transfer, that the just-written pages are no longer needed, so a stream of large uploads does not evict the page cache that hot reads depend on. Where the deployment opts into it for objects above a size threshold, the staging write can bypass the page cache entirely, which gives more predictable write latency and avoids polluting the cache with write-once bulk data, at the cost of requiring aligned buffers and transfer sizes that the blob facility manages internally.

### 7.6 The read data path and zero-copy

The default GET path streams object data in bounded chunks. Small, uncompressed, unencrypted
objects can use a whole-object buffer; compressed or encrypted objects require userspace decoding.
Range reads on uncompressed objects seek to the requested offset; compressed ranges decode the
relevant blocks (Section 10).

The optional `fast-io` feature supplies two distinct optimizations on supported Linux builds:

- **Plaintext sendfile:** an HTTP/1.1 loop transfers eligible uncompressed, unencrypted object files
  directly to the socket, including a single resolved range. It keeps the connection for subsequent
  eligible GETs. An ineligible request is replayed to hyper, which owns that connection for the rest
  of its lifetime. Clients whose pooled connections begin with a PUT or bucket operation therefore
  bypass sendfile on later GETs. The recorded `warp` workload had 0% engagement even with the
  keep-alive loop; this is not a measurement of kTLS.
- **kTLS record encryption:** after a rustls handshake, `serve_tls` can install negotiated traffic
  keys on a kernel-TLS socket and serve hyper over it. The application still streams response bytes
  through userspace; this offloads TLS record encryption but does not provide file-to-socket HTTPS
  transfers. A failed startup capability probe keeps all connections on rustls. If an attempted
  per-connection handoff fails, that connection closes because the consumed stream cannot safely
  resume in rustls.

**Zero-copy HTTPS reads are not implemented.** The sendfile request loop is wired only to plaintext
connections, not to the kTLS stream. Kernel encryption support alone does not connect these paths.

Both optimizations are experimental, disabled by default and absent from standard release builds.
The optional dependency has a documented arm64-musl compatibility limitation; use glibc Linux builds
for `fast-io` evaluation. See [the benchmark record](benchmarks.md#sendfile-fast-path---features-fast-io-experimental-linux-only)
for workload-specific engagement and CPU measurements. A passing TLS fallback test does not prove
kernel offload engaged; inspect `cairn_ktls_offload_total{result="ok"}` on a supported host.

### 7.7 Network front end and TLS

Both listeners set `TCP_NODELAY` immediately after accepting a socket, before plaintext, TLS, or optional fast-I/O dispatch. This prevents small separately-written response headers and bodies from waiting on Nagle/delayed ACK interaction. Socket setup failure closes the connection and is logged; there is no configuration switch.

Cairn serves HTTP/1.1 and HTTP/2. It can terminate TLS itself using a Rust TLS stack with modern defaults, reading certificate and key material from configured paths and supporting reload on change, which lets a deployment be secure on the wire with no external proxy. It also runs cleanly behind a terminating reverse proxy, in which case it serves plaintext on a trusted interface and trusts the proxy to pass through the authorization, range, conditional, and S3-specific headers unmodified and to stream rather than buffer large bodies. The proxy's immediate address/network must be explicitly listed in `CAIRN_TRUSTED_PROXIES` before forwarding metadata has any authority; the proxy must strip client-supplied values and write a valid RFC `Forwarded` `for=` chain or `X-Forwarded-For` chain (or both with the same resolved client), plus either `Forwarded` `proto=` or a single `X-Forwarded-Proto` when the console must recover an external HTTPS scheme. Cairn walks client-address chains right-to-left through allow-listed hops. Untrusted peers' headers are ignored; missing, malformed, or conflicting client provenance from a trusted peer makes `aws:SourceIp` absent rather than substituting the proxy. The plaintext sendfile path uses the same resolver and hands such unavailable provenance to the general server rather than authorizing on a divergent value. Forwarded scheme affects only the console session cookie's `Secure` attribute; `aws:SecureTransport` remains the direct socket's TLS state. Both deployment shapes are first-class and documented (Section 31). When TLS is terminated upstream, the zero-copy-with-kernel-TLS consideration of Section 7.6 does not apply to Cairn, since Cairn sees plaintext.

### 7.8 Backpressure, limits, and fairness

Each listener continuously joins completed connection tasks, including while no clients arrive. Its task set retains active connections rather than the process-lifetime connection history. Shutdown takes priority, drains remaining tasks, and includes previously observed panics/cancellations in its final report. This bounds task retention; it does not imply RSS returns to startup levels after load.

A global concurrency limit caps the number of in-flight S3, management, and console requests so that overload sheds cleanly rather than collapsing; excess requests are rejected with a retryable status. The unauthenticated infrastructure endpoints do not consume that application budget, because an orchestrator must still observe a saturated node, but they are not an unbounded bypass: health, readiness, and metrics share a fixed four-request infrastructure budget and fail fast when it is full. Metrics rendering has a two-request sub-limit, permanently leaving infrastructure capacity for health and readiness during a scrape flood. Per-request timeouts bound how long any single request can hold resources. The bounded blob pool and the streamed, backpressured transfers ensure that a small number of very large transfers cannot monopolise memory or threads. These mechanisms together give the server a defined behaviour at and beyond saturation, which is a production requirement a naive single-node server does not address.

---
