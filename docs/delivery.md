# Delivery: build, deployment, roadmap, decisions, appendices

> Part of the Cairn reference docs (split from the former ARCH.md). The section numbers below are stable identifiers used throughout the code and docs; see the index in [`README.md`](./README.md) and [`../CLAUDE.md`](../CLAUDE.md).

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


