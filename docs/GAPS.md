# Cairn — Implementation Gap Report

Multi-agent audit of the implementation against ARCH.md (10 domain reviewers + synthesis).

**Original audit verdict:** partially-satisfies — **~68% of spec**.

## Remediation status (post-audit)

All three **critical** findings and the **high** findings have been remediated and verified:

| Finding | Status |
|---|---|
| C1 — signed-streaming chunk verification (F-5) | ✅ fixed — `Principal.chunk_signing` → verified per-chunk chain; tamper test rejects |
| C2 — subresource mis-routing (object-body corruption) | ✅ fixed — unknown subresources → 501; `?acl`/`?publicAccessBlock`/`?ownershipControls` routed |
| C3 — replication non-functional | ✅ fixed — enqueue-on-write + worker + real SigV4 sink; **verified node→node** |
| H — ACL/BPA/Ownership inert; corrupt config fails open | ✅ fixed — wired into authz; **fail closed** |
| H — WAL checkpointer absent | ✅ fixed — truncating checkpoint task + `cairn_wal_*` metrics |
| H — quotas unimplemented | ✅ fixed — settable + enforced in the commit transaction |
| H — client checksums never compared | ✅ fixed — mismatch → `BadDigest` |
| H — response-body buffering | ✅ fixed — streamed (`UnsyncBoxBody`, bounded memory) |
| H — conditionals incomplete; faked readiness | ✅ fixed — `If-*-Since` + HEAD short-circuit; real `/readyz` |
| H — mgmt API/CLI gaps | ✅ largely fixed — 8 config/user/replication endpoints; `backup`/`restore`/`migrate` CLI |
| H — crash-consistency harness inert | ✅ fixed — **live F-4 test passes** (crash → orphan → reconcile reclaims) |
| Medium — versioning fidelity, per-key errors, CORS preflight, tag context, one-fs check, storage_path index, data_root fsync | ✅ fixed |

Remaining (lower priority / documented): `UploadPartCopy` and `GetObjectAttributes`, ACL *body*
documents (canned `x-amz-acl` supported), and `warp` macro load profiles. The findings below are
the original audit text, kept for reference.

Recently completed:
- **HTTPS replication connector** — `HttpS3Sink` now uses a `hyper-rustls` `HttpsConnector`
  (`with_webpki_roots().https_or_http().enable_http1()`) so both `http://` and `https://`
  destination endpoints work; the former https-rejection is gone (a TLS-negotiation integration
  test asserts the connector sends a ClientHello for https).
- **Per-rule replication destinations** — the destination bucket is resolved *per source bucket*
  from that bucket's stored replication rule (`ConfigAspect::Replication` → `parse_replication` →
  `<Destination><Bucket>`, ARN prefix stripped). `S3SinkConfig` carries a `source -> dest` map plus
  a default; `cairn-server`'s `replication_loop` rebuilds the map before each drain and falls back
  to `CAIRN_REPLICATION_DEST_BUCKET`. The single-destination node→node path still works.
- **TLS cert hot-reload (§27.2)** — a `SIGHUP` handler reloads the cert/key from the same paths and
  atomically swaps the served `ServerConfig` via a `tokio::sync::watch` channel consulted per
  accept, without dropping the listener; a bad new cert is logged and the old config is kept. Every
  successful reload is logged.

---

**Original audit verdict:** partially-satisfies — **~68% of spec**.

## Executive summary

Cairn's core data-plane engine is genuinely strong and, in several places, exemplary: the durable commit ordering (fsync file -> rename -> fsync dir) is implemented exactly per F-1/F-2 and verified in code; the single group-committing metadata writer with per-mutation savepoints, one-COMMIT-per-barrier durability, and a separate WAL read pool matches the spec and is tested; the compression block format preserves plaintext-MD5 ETag semantics and supports range reads; the SigV4/Bearer auth primitives are correct against AWS vectors; the authorization ENGINE implements the §15.3 precedence exactly; the S3 error map is compiler-enforced total; and encryption-at-rest (F-15) is real (AES-256-GCM envelope encryption under a zeroizing master key, actually invoked to decrypt SigV4 secrets). However, the gap between "engine is correct" and "feature works end-to-end in production" is large and recurring. The single most serious finding, independently surfaced by three separate audits, is that signed-streaming chunk verification is NEVER wired into the request path: both PutObject and UploadPart unconditionally build ChunkDecoder::unsigned for any STREAMING body, so a tampered or truncated signed-streaming upload is accepted and stored silently -- the exact F-5 silent-corruption failure the spec calls mandatory to prevent. Compounding this, an entire family of security and operability subsystems is built-and-unit-tested but not connected: ACLs/ownership-modes/Block-Public-Access are inert (inputs hardcoded to None/BucketOwnerEnforced, no subresource routes, and unknown subresources silently MIS-ROUTE to data-plane handlers e.g. PUT object?acl overwrites the object body); replication never ships a byte (no real sink, no enqueue-on-write, no worker, no config encryption); quotas are unimplemented (only an unused error variant); the WAL checkpointer does not exist despite a comment claiming it does; client-supplied checksums are computed-and-stored but never compared; the management/CLI surface is missing most of §22/§24 (no config editing, no presign, no user rotate/deactivate, no replication ops, no backup/restore/migrate commands, no remote CLI); readiness is faked (constant true); and the HTTP adapter fully buffers request and response bodies, defeating the §7.8 bounded-memory streaming guarantee. Many of these are silently-dropped requirements that present a false sense of safety (replication returns 204 while doing nothing; corrupt BPA/policy configs fail OPEN). Production-readiness verdict: a solid durable single-node object store core wrapped in a protocol/security/operability shell with substantial holes; not yet production-ready for security-sensitive or replicated deployments.


## Critical gaps


### 1. Signed-streaming (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) per-chunk signature chain is never verified in the request path. PutObject and UploadPart unconditionally construct ChunkDecoder::unsigned for any body whose x-amz-content-sha256 starts with 'STREAMING'; ChunkVerifier/ChunkDecoder::signed and the seed-signature plumbing exist only in unit tests. Verified directly: service.rs:366 and :593 are the only constructions and both are ::unsigned.
- **Spec:** ARCH §14.3 / §21.7 / F-5 (lines 503-505, 675; supporting these sentinels is 'mandatory')
- **Impact:** Signed-streaming object bodies are unauthenticated past the request envelope. An attacker or corrupting proxy who modifies the chunked body after headers are signed has the tampered/truncated content accepted and stored silently. Common AWS SDKs default to streaming signing, so this is a live integrity hole, not an edge case. This is the highest-risk ingest component by the spec's own words.
- **Fix:** Plumb the request's seed signature from the auth layer (sigv4.rs:246) through the adapter/service into a ChunkVerifier; in PutObject/UploadPart select ChunkDecoder::signed when the sentinel is STREAMING-AWS4-HMAC-SHA256-PAYLOAD and the principal was established via SigV4; fail the stream on first chunk-signature mismatch. Add a crash/conformance test that tampers a signed-streaming body and asserts rejection.

### 2. ACL / Object-Ownership / Block-Public-Access subsystems are inert end-to-end, AND unrecognized subresources silently mis-route to data-plane handlers. AuthzInput is built with bucket_acl:None, object_acl:None unconditionally (service.rs:1263-1264); create_bucket hardcodes BucketOwnerEnforced (service.rs:237); PutObject writes acl:None ignoring x-amz-acl; there are zero routes for ?acl, ?ownershipControls, ?publicAccessBlock; account-level BPA has only a getter, no setter. The dispatch catch-all only fires on unknown HTTP methods, not unknown subresource query keys, so PUT object?acl falls through to put_object and OVERWRITES THE OBJECT BODY with the ACL XML.
- **Spec:** ARCH §12.4, §15.2, §15.4, §15.7, §13.2/§13.3, §34.3 (all marked 'Yes')
- **Impact:** The entire access-control configuration surface the spec foregrounds is non-functional: an object made 'public-read' via ACL is silently NOT public, ACL-based grants are silently never honored, operators cannot configure ownership mode or either BPA scope, and a control-plane request silently performs a destructive data-plane operation (object-body corruption). This is both a security gap and a data-safety hazard.
- **Fix:** Add ?acl, ?ownershipControls, ?publicAccessBlock subresource dispatch in bucket_op/object_op; load ConfigAspect::Acl and the object row acl into AuthzInput; honor x-amz-acl on PutObject; add the account-BPA setter and SetOwnership invocation; and change the dispatch default so an UNRECOGNIZED subresource query key returns NotImplemented instead of falling through to a data-plane handler.

### 3. Replication is non-functional end-to-end despite returning success and being advertised. No production ReplicationSink exists (only FakeReplicationSink in test code; no HTTP/S3 client dependency); no outbox enqueue on writes (every write path passes replication:None); no worker is spawned in the server; replication config (PUT ?replication) is stored as unvalidated plaintext with destination credentials NOT encrypted under the master key and no versioning-enabled check.
- **Spec:** ARCH §12.5, §20.2, §20.3, §20.4, §20.5
- **Impact:** An operator can enable replication, see 204 success and a stored config, and believe a second site is being kept current when nothing is replicated and no metric or log reveals the gap -- a silent false-durability guarantee. Storing destination credentials in plaintext also violates the §20.2/§27.4 encrypt-at-rest requirement (a credential-leak surface). The executive summary positions replication as a shipped feature.
- **Fix:** Implement a real S3-client ReplicationSink; enqueue outbox entries in the same commit transaction when a write matches a rule filter; spawn the ReplicationEngine in the server background; add parse_replication with versioning-required validation and seal destination credentials under the master key; surface lag/queue/failure metrics and a status/retry endpoint. If replication cannot ship in this release, make PUT ?replication return NotImplemented rather than silently accepting it.

## High gaps


### 1. WAL checkpointer does not exist, and a code comment falsely claims it does. No explicit/observable truncating checkpoint task anywhere; background.rs:2-3 states it is 'managed inside cairn-meta' but cairn-meta has no checkpoint method, PRAGMA, or hook. Relies on SQLite's implicit non-truncating PASSIVE autocheckpoint.
- **Spec:** ARCH §8.4, §11.2, §6.4, F-3
- **Impact:** Under sustained writes with a long-lived reader the -wal file can grow unbounded (the exact failure §8.4/§31.5 calls out), inflating disk use and read latency, with no checkpoint-runs or WAL-size metric to detect it. The misleading comment will cause a maintainer to believe a required durability/operability subsystem exists.
- **Fix:** Add a scheduled background task that issues PRAGMA wal_checkpoint(TRUNCATE) on the write connection at a configurable interval/size threshold; emit checkpoint-run and WAL-size metrics; remove the false comment.

### 2. Quotas are entirely unimplemented. The only artifact is the unused MetaError::QuotaExceeded variant; no quota config keys, no per-bucket/per-user accounting, no check in the commit transaction.
- **Spec:** ARCH §27.5, §28.2
- **Impact:** An operator cannot bound a tenant's storage; a single bucket or user can fill the volume. This is a silently dropped requirement, not deferred plumbing -- the spec states quotas are 'enforced inside the commit transaction'.
- **Fix:** Add per-bucket/per-user byte (and optionally object) quota config and accounting columns, and enforce them inside the writer's commit transaction returning QuotaExceeded -> 507.

### 3. Client-supplied x-amz-checksum-* values are computed and stored but never compared; trailing-checksum (STREAMING-...-TRAILER) trailer values are read and discarded. Only Content-MD5 is actually verified.
- **Spec:** ARCH §21.1, §21.7
- **Impact:** A corrupt upload that declares a checksum is silently accepted instead of failing with BadDigest, defeating the end-to-end integrity guarantee the checksum headers and trailer framing exist to provide.
- **Fix:** After staging, compare each computed checksum against the supplied header/trailer value and, on mismatch, delete the staged blob and return BadDigest, as §21.1 requires.

### 4. HTTP adapter fully buffers request and response bodies into a Vec (adapter.rs:69-70 collect request; :167-176 drains response stream into a Vec). Verified directly.
- **Spec:** ARCH §7.4, §7.6, §7.8
- **Impact:** Defeats the streamed, bounded-memory, backpressured transfer contract that §7.8 names a production hardening requirement: a few concurrent large GETs/PUTs buffer whole objects in memory -- the exact resource-exhaustion the limits section promises to prevent. Also forecloses the §7.6 zero-copy sendfile/kTLS read fast path entirely.
- **Fix:** Stream the response body straight from the blob store handle to the socket and stream the request body into the stager without full buffering; the S3Body::Stream variant already exists -- forward it instead of draining it.

### 5. Conditional-request handling is incomplete: If-Modified-Since/If-Unmodified-Since are never evaluated (GET or copy-source), and head_object never calls conditional_short_circuit so conditional HEAD never returns 304/412. Copy-source-if-* preconditions are also absent on CopyObject/UploadPartCopy.
- **Spec:** ARCH §21.2, §21.6, §13.3
- **Impact:** Clients relying on time-based conditional GET/HEAD (caching, optimistic concurrency) get incorrect 200s; conditional copies are performed unconditionally, a data-safety deviation.
- **Fix:** Parse and evaluate If-Modified-Since/If-Unmodified-Since against last-modified for GET and HEAD; route HEAD through conditional_short_circuit; read x-amz-copy-source-if-* on copy and short-circuit accordingly.

### 6. Several §34.3 'Yes' operations are entirely unrouted and mis-route: GetObjectAcl/PutObjectAcl, GetBucketAcl/PutBucketAcl, Get/PutBucketOwnershipControls, Get/Put/DeletePublicAccessBlock, GetObjectAttributes, and UploadPartCopy (a part produced by copying a range of an existing object).
- **Spec:** ARCH §21.3, §13.2/§13.3, §34.3
- **Impact:** UploadPartCopy with x-amz-copy-source is treated as a normal body part-upload, corrupting the part. The ACL/ownership/BPA/attributes subresources mis-route to unrelated handlers (worse than an honest 501).
- **Fix:** Add the missing subresource and x-amz-copy-source-on-part dispatch arms; implement UploadPartCopy as a ranged copy into a staged part.

### 7. Management API and CLI are missing most of §22/§24: no bucket-config read/edit endpoints (policy/ACL/CORS/lifecycle/replication/tags/BPA/compression), no presigned/public-read URL minting, no user update/deactivate/credential-rotation, no replication status/retry, no running-config exposure; readiness /health is a hardcoded constant true; and there is no remote-admin CLI, backup, restore, or explicit migrate command (cairn-cli is deferred in-code).
- **Spec:** ARCH §22.2, §23.3, §24.2/§24.3, §26.4, §31.4
- **Impact:** Operators cannot configure most bucket security/lifecycle settings, cannot disable a compromised user or rotate leaked credentials, cannot observe/retry replication, cannot back up or restore via tooling, and an orchestrator routing on the faked readiness can send traffic to a not-ready process. The single-binary 'whatever can be done in a browser can be done from a terminal' promise is currently false.
- **Fix:** Implement readiness that consults migrations-applied + writer/read-pool responsiveness; add the bucket-config CRUD, presign, user-management, and replication-ops endpoints; build the §31 consistent-snapshot backup/restore and a reconcile repair mode (prune/flag rows whose blobs are missing); ship the remote-admin CLI.

### 8. Crash-consistency harness (F-4) is inert: the fail-rs seams blob_after_durable/blob_after_assemble exist at the correct windows but the failpoints feature is enabled by no test target and no CI job, and no test runs the server as a child process, kills it in the window, and asserts reconcile reclaims the orphan. The community S3 conformance suite (s3-tests) is also absent as a CI gate.
- **Spec:** ARCH §29.3, §29.4, §29.5, F-4
- **Impact:** The durability ordering is 'asserted rather than made real' -- a regression moving the metadata commit before fsync_dir would not be caught by any automated test. Without s3-tests (the spec's 'practical definition of compatibility'), compatibility breadth is unverified.
- **Fix:** Add a child-process crash test that arms each failpoint, kills+restarts, and asserts orphan reclamation; wire the failpoints feature into CI; integrate the s3-tests supported subset as a gate with unsupported areas explicitly marked.

## Medium gaps


### 1. Reconcile_staging unconditionally deletes every file in .staging with no age/safety-margin check; ReconcileOpts.staging_safety_margin_secs is never read. Verified.
- **Fix:** Honor staging_safety_margin_secs (skip artifacts younger than the margin) so an out-of-band 'cairn reconcile' against a live data dir cannot delete in-flight single-part staging files (STAGING/{id}.tmp) and corrupt concurrent PUTs (§8.5).

### 2. Corrupt security configs fail OPEN: bucket BPA is parsed with unwrap_or_default() (all toggles off) and bucket policy with parse_policy(...).ok() (treated as no policy). A corrupt BPA doc silently opens public access; a corrupt policy silently drops an explicit Deny.
- **Fix:** Fail closed on unparseable BPA/policy documents (deny or surface an error) rather than defaulting to permissive (§15.3, §15.5).

### 3. CORS is configured but never enforced: no OPTIONS preflight handling and no Access-Control-* / Vary:Origin response headers on any path. Browsers cannot perform cross-origin reads/writes.
- **Fix:** Add OPTIONS routing and evaluate the stored CORS config against Origin/method/headers, emitting the preflight and actual-request CORS headers (§18.2).

### 4. Versioning fidelity gaps: delete responses never set x-amz-version-id / x-amz-delete-marker; plain GET of a delete-markered key returns 404 without signaling the marker; GET/HEAD naming a delete-marker's own versionId returns 404 instead of 405; Suspended-bucket DELETE permanently removes the null version instead of inserting a null delete marker (potential silent data loss).
- **Fix:** Surface delete-marker identity in delete responses, emit x-amz-delete-marker on reads, return 405 for delete-marker versionId GET/HEAD, and insert a null delete marker on Suspended-bucket delete (§16.1/§16.3).

### 5. Tag-based and tag-conditioned access control is non-functional: build_context hardcodes existing_tags/request_tags to empty, so any policy conditioned on s3:ExistingObjectTag/* or aws:RequestTag/* never matches; object tagging ignores ?versionId; and S3 tag-set limits (count/length/charset) are not validated on write.
- **Fix:** Load object and request tags into the condition context, honor ?versionId on tag ops, and validate tag-set limits on write (§15.6, §17.1/§17.2).

### 6. Reconciliation is not seek-efficient: no index on object_versions.storage_path, so each enumerate page and each live_blobs membership check is a full-table scan (O(N) per call), undermining the §8/F-8 'bounded in time' criterion at scale.
- **Fix:** Add an index on storage_path (or restructure live_blobs as a batched indexed seek).

### 7. Per-key error fidelity is lost: DeleteObjects reports every per-key failure as 'InternalError', and copy_object collapses all commit errors to Internal(500), masking AccessDenied/NoSuchVersion/PreconditionFailed/InsufficientStorage. DeleteObjects also performs no per-key authorization.
- **Fix:** Map each per-key/copy error to its true S3 code/status and authorize each key in bulk delete (§21.5, §21.6).

### 8. Audit attribution is absent: management record_activity hardcodes actor:None and the S3 data plane records no activity entries at all, so 'who changed or accessed what' is unrecorded.
- **Fix:** Thread the authenticated principal into management activity entries and record audit entries for mutating S3 operations (§26.3, §22.2).

### 9. Action granularity gaps in authz mapping: multipart init/complete, GetObjectVersion/DeleteObjectVersion, and copy variants are all collapsed to generic PutObject/GetObject/DeleteObject, so policies scoped to those distinct actions never grant or deny as written.
- **Fix:** Map versioned and multipart-lifecycle requests to their distinct catalogued actions in object_action (§34.4).

### 10. One-filesystem invariant unenforced and data_root not fsynced after lazily creating a new per-bucket directory; no startup st_dev/EXDEV check. The first committed blob in a brand-new bucket directory can lose its directory entry on power loss (F-1 one level up), and a cross-device mount makes every rename fail with a generic write-time Io error and no diagnostic.
- **Fix:** fsync data_root after create_dir_all of a new bucket dir; add a startup same-filesystem check for data_root/staging/bucket dirs (ARCH:93, §8.2, §9.2, F-1).

### 11. Property/oracle tests for listing/pagination and for SigV4 canonicalization are absent (only example-based tests), and three of four mandated fuzz targets (XML parser, policy/config JSON parser, key parser) plus macro/warp load tests characterizing the single-writer ceiling are missing.
- **Fix:** Add reference-oracle property tests for listing under random prefixes/delimiters/page sizes, fuzz the XML and policy/JSON parsers, and add a macro load profile that watches write-queue-depth as concurrency rises (§29.2/§29.3/§29.5/§30.2).

### 12. No cert hot-reload, no defer-reconcile-on-start flag, and background intervals (sweep/lifecycle/multipart lifetime) plus durability knobs (SQLite synchronous, blob fsync, group-commit linger, pool sizes, public-read secret/lifetimes) are hardcoded rather than config-backed -- roughly half of the §28.2 config surface is missing.
- **Fix:** Add file-watch/SIGHUP cert reload and expose the missing §28.2 config keys with validation (§27.2, §28.2).

## Low / nits

- Decrypted SigV4 secret is re-materialized as a plain String, not a Zeroizing container, at the auth-layer boundary (lib.rs:77,106) -- note: the at-rest encryption and master-key handling ARE correctly zeroizing, so this is only a transient-plaintext nit, not the F-15 gap it might first appear (§14.1).
- Object-level first-block incompressibility heuristic is absent; per-block raw fallback prevents size growth but a compressed-policy bucket still pays full CPU on incompressible non-allowlisted data (§10.4).
- Reconcile ignores opts.parallelism (serial) and does not prune emptied directories (§8.5).
- Compression format is detected by trailer-magic sniffing rather than the metadata CompressionDescriptor; structural validation makes silent corruption near-impossible but a benign object could be spuriously rejected (§9.3).
- UploadId path components bypass resolve() sanitization in the blob store (defense-in-depth hole; not exploitable via the normal request path due to metadata-layer checks) (§9.1/§12.1).
- Redundant idx_object_versions_bkv duplicates the auto-index from the UNIQUE constraint; the schema column is named 'compression' rather than the spec's 'compression_policy' (§34.1/§34.2).
- ListMultipartUploads and ListObjectVersions pagination are incomplete (no upper-bound seek / version-id-marker), loading whole sets in memory for large in-flight counts (§11.4, §16.3).
- x-amz-tagging inline initial tag set on PutObject is not read (§17.1).
- create_bucket via the control plane hardcodes owner_id='admin' and region='us-east-1' rather than the authenticated principal/configured region, corrupting ownership data.
- Concurrency limiter uses try_acquire (immediate 503) with no 'wait briefly' queue as §7.8 describes; rustls install_default result is ignored; coverage has no minimum-threshold gate.
- Out-of-scope subresources (SSE, object-lock, website, accelerate, etc.) mis-route to unrelated handlers instead of returning 501 NotImplemented (§34.3 final row).
- Zero-copy read fast path (§7.6 sendfile/splice/kTLS) is not implemented, but the spec marks it feature-gated/optional with the portable path always present, so this is a disclosed optional optimization rather than a gap -- though it is currently also foreclosed by the body-buffering adapter (see high gaps).

## Strengths (verified)

- Durable single-part and multipart commit ordering is implemented exactly per F-1/F-2 (fsync file -> create_dir_all -> rename -> fsync dir -> ack), verified directly in cairn-blob/src/lib.rs:257-281 and 460-470, with fail-rs crash seams at the correct windows.
- Metadata write path is spec-faithful and tested: a single group-committing writer owns the one write connection, each mutation is wrapped in its own SAVEPOINT (failed precondition isolates without dooming batch-mates), one COMMIT is the durability barrier, oneshot acks fire only post-barrier, and reads use a separate query_only WAL pool (§7.2/§7.3/§11.6).
- Compression preserves S3 semantics correctly: ETag is plaintext-MD5 computed in one ingest pass before compression, the self-describing CRNB block format with per-block index supports efficient range reads, and incompressible blocks fall back to raw so data never grows -- with passing fidelity/ETag/range/incompressibility tests.
- Authentication primitives are correct and test-backed: SigV4 header+presigned canonicalization and signing-key derivation pass the AWS get-vanilla vector, constant-time comparison is used throughout, the 15-minute skew window is enforced, and the dev-auth bypass is genuinely triple-gated (cargo feature + runtime flag + loopback-only) satisfying F-17.
- The authorization ENGINE (cairn-authz) is correct and well-tested in isolation: it implements the §15.3 precedence exactly (owner/admin short-circuit subject to explicit deny -> BPA gate -> explicit deny -> any-allow -> default deny), the BPA union/strip logic, ACLs-disabled enforced mode, anonymous handling, the condition engine, canned-ACL expansion, and the policy parser, with ~40 table-driven plus property tests.
- Encryption-at-rest (F-15) is real, not just an error variant: cairn-crypto provides AES-256-GCM envelope encryption under a 32-byte master key held in a Zeroizing buffer, supplied out-of-band, and cairn-auth actually calls crypto.open() to decrypt SigV4 secrets at use time; Bearer secrets are stored hashed.
- The S3 error map is provably total (no wildcard arm; the compiler enforces a mapping for every Error variant) and echoes the request id, satisfying §25.
- The chunked-decoder FRAMING is robust and rigorously tested: a bounded-buffer state machine correct across arbitrary read splits, a 2048-case proptest, a libfuzzer target wired into CI, and a criterion micro-benchmark -- the de-framing itself (as opposed to signature verification) is production-grade.
- The lifecycle scanner is production-wired and well-tested (current/noncurrent expiration under versioning, NewerNoncurrentVersions retention, expired-delete-marker removal, abort-incomplete-multipart), running on a real background interval with 19 gate tests.
- Strong engineering hygiene: figment-based layered config with fail-fast validation, ordered graceful shutdown with bounded drain, native rustls TLS, structured per-request tracing, a CI matrix (fmt/clippy -D warnings/nextest on gnu+musl/static-musl/doctests/coverage), and a boto3 conformance gate.

## Remediation order

1. Close the F-5 signed-streaming integrity hole FIRST: plumb the seed signature into a ChunkVerifier and verify the per-chunk chain in PutObject/UploadPart; add a crash/conformance test that tampers a signed-streaming body and asserts rejection. This is the single highest-risk silent-corruption gap.
2. Fix the silent dispatch mis-routing: make unrecognized subresource query keys return NotImplemented instead of falling through to data-plane handlers (stops PUT object?acl from overwriting object bodies), then add the ?acl/?ownershipControls/?publicAccessBlock and UploadPartCopy routes.
3. Wire the ACL/Ownership/BPA subsystem end-to-end (load ACL inputs, honor x-amz-acl, add account-BPA setter and SetOwnership) and make corrupt BPA/policy configs fail CLOSED -- the engine is already correct, this is pure wiring plus a fail-safe flip.
4. Either fully wire replication (real S3 sink, enqueue-on-write in the commit txn, worker, encrypted destination credentials, versioning-required validation, metrics, status/retry) OR make PUT ?replication return NotImplemented -- do not keep silently accepting it.
5. Add the WAL checkpointer (truncating, scheduled, with metrics) and remove the false comment; implement per-bucket/per-user quotas enforced in the commit transaction; verify client-supplied checksums and fail on mismatch.
6. Stream request/response bodies through the adapter instead of buffering (restores the §7.8 bounded-memory guarantee and unblocks the §7.6 zero-copy path); add the storage_path index for seek-efficient reconciliation; fsync data_root for new bucket dirs and add the one-filesystem startup check.
7. Build the operability surface: real readiness probe, bucket-config CRUD + presign + user-management + replication-ops management endpoints, backup/restore + reconcile repair-mode + remote CLI, and audit attribution across both planes.
8. Complete protocol fidelity: time-based and copy-source conditionals, HEAD conditionals, per-key error codes in DeleteObjects/copy, CORS preflight, versioning delete-marker signaling and Suspended-bucket null-marker behavior, tag-context loading + tag-set validation.
9. Make the durability/conformance verification real: a child-process crash test that arms the failpoints (and wire the failpoints feature into CI), integrate the s3-tests supported subset as a gate, add listing/SigV4 property tests and XML/policy fuzz targets, and add a macro load profile that characterizes the single-writer ceiling via write-queue-depth.
10. Round out config (§28.2 missing keys, cert hot-reload, defer-reconcile flag) and observability (route-labeled request metrics, writer-queue-depth, pool/checkpoint/WAL/replication/cache/bytes series).
