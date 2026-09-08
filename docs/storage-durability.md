# Storage and durability

> Part of the Cairn reference docs. The section numbers below are stable identifiers used throughout the code and docs; see the index in [`CLAUDE.md`](./CLAUDE.md) and [`../CLAUDE.md`](../CLAUDE.md).

## 8. Durability and crash consistency

### 8.1 The guarantee Cairn makes

Cairn guarantees that after any crash, on restart it converges to a state in which every metadata row that is visible references a present, complete, durable blob, and no orphaned blob remains, with no manual intervention. It guarantees that a write acknowledged to a client as successful is durable on the local storage as configured. It does not, by itself, guarantee survival of a drive failure; that is delegated to the storage layer the operator places underneath (Section 8.6), and survival of a host failure is provided by bucket replication (Section 20). Stating this boundary precisely is itself a requirement (N-1), because a production operator must know exactly where Cairn's guarantee ends and theirs begins.

### 8.2 The commit sequence

Every physical object or part write starts with a bounded, immutable storage plan: attempt and
server-generation tokens, the exact target identity, and every possible temporary, final and index
spool filename. Before any file can be created, the single metadata Writer durably admits that
plan; multipart reservation or completion-claim admission occurs in the same savepoint. A matching
typed `StorageCreationPermit` and `StorageIoLease` are mandatory at `stage`, `stage_part` and
`assemble`. A missing, rejected or unacknowledged admission cannot authorize file creation.

After admission, object publication follows one ordered sequence, and the order is the durability design.

First, the object's bytes are streamed to a staging file in the staging directory, with hashes computed inline and, where compression is enabled, the framed compressed form and its index trailer produced during the same pass. Second, the staging file is fsynced, which makes its data and inode durable. Third, the staging file is renamed into its final per-bucket directory under its UUID name; rename is atomic within the filesystem, which is why the staging directory must share the filesystem with the data directory. Fourth, and this is the step a naive temp-then-rename omits, the destination directory is fsynced, which makes the rename itself durable; without this the directory entry can be lost on power failure even though the file data is safe. Fifth, the computed hashes are validated against any client-supplied checksums, and on mismatch publication fails and the admitted paths enter exact recovery, with no visible object metadata written. Sixth, the metadata transaction is submitted to the writer with the exact admitted plan and committed; this is the single linearization point, and for conditional writes the precondition is evaluated inside this transaction so the check and the upsert are inseparable. Only after this commit is the operation acknowledged to the client. Seventh, exact cleanup debt recorded with publication reclaims superseded blobs and remaining provisional aliases, and the activity and metrics are recorded. Physical cleanup is retried independently; its failure cannot revoke an already committed object.

The invariant this produces is that a committed, visible row never references a blob that is not already durable, because blob durability (steps two through four) strictly precedes metadata commit (step six). A crash between step four and step six leaves a durable blob without a visible object row, whose admitted intent is resolved into exact cleanup during recovery; a crash after step six leaves a consistent state. There is no ordering in which a visible row points at a blob that the filesystem has not promised to keep.

Prepared creation paths hold shared locks on their actual directory descriptors. These locks
follow cloned descriptors into queued namespace work, so cleanup cannot remove an empty target
directory before a pending rename. Cleanup holds shared directory locks through unlink and sync,
then releases the child's shared lock before attempting nonblocking exclusive ownership for
pruning; the parent stays locked. A busy directory is left in place. Completed coalesced syncs
release all directory descriptors before acknowledgements, while their I/O leases remain owned
through actual job completion.
Acquisition rechecks the directory's name/inode after taking its lock and reopens if a pruner won
the race; an unlinked descriptor is never accepted as a creation target.

Cancellation does not synchronously unlink admitted paths. A bounded recovery slot and an exact
plan guard are acquired before admission can reach the Writer, covering a lost admission
acknowledgement as well as cancellation during staging, checksum validation and publication.
Dropping the request cancels further lease acquisition and enqueues its record to a retained
worker. Existing file owners, queued or executing blocking jobs, io_uring operations, returned
results and directory-sync waiters retain child leases through their actual completion. Each
lease retains the exclusive node lifetime and recovery slot; dropping an async future does not
prove that its I/O has stopped.

The worker submits exact cancellation through the serialized Writer, waits for all existing I/O
leases to drain, and probes exclusive locks on the plan's existing files. Only then
may typed quiescence resolve the intent. Resolution preserves an exact authoritative object or
part reference and converts unreferenced aliases into durable cleanup debt. Successful publication
atomically consumes the matching intent and records provisional/superseded cleanup; its guard is
disarmed before any post-commit await. An ambiguous Writer result preserves ownership for exclusive
startup recovery. A timeout or a lost acknowledgement is never evidence that bytes are unreferenced.

Cleanup uses exact Writer claims bound to a generation, claim token and expiry. Claims recheck
that no authoritative reference or active intent owns the path. The blob layer rejects a still
locked file, unlinks only the claimed filename, synchronizes its directory, and prunes only empty
supported parents with parent synchronization. Already-absent names still require a namespace
barrier. The Writer retires debt and associated quota only after durable absence and a matching,
unexpired acknowledgement. Unknown siblings survive; filesystem errors and stale acknowledgements
retain debt. Request abort and multipart termination do not authorize recursive session deletion.

When multipart termination or replacement attaches quota debt to an already-claimed scratch alias,
the Writer invalidates that obsolete claim and its lease in the same savepoint. A fresh claim may
retry immediately; the old acknowledgement remains rejected, and bytes stay charged until the new
claim proves durable absence. Reattaching the same quota owner preserves an existing valid claim.

Store construction also requires a maintenance `StorageIoLease` retaining the actual exclusive
node guard. Initialization executes in a leased blocking job, so cancelling construction cannot
release exclusion ahead of directory creation or synchronization. Serving and offline recovery
retain the same node lifetime through their dependent I/O and runtime teardown.

The Linux io_uring process-kill regression uses an isolated, frozen ext4 loop filesystem and
requires a real submitted write without a completion plus a kernel write-stack witness before
SIGKILL. It observes node exclusion and the staging-file lock remaining held while process teardown
is pending, rejects quiescence and cleanup, then permits them after thaw and process reap. This
qualifies exclusion through teardown; it does not demonstrate a detached kernel-reference interval
after process exit or power-loss durability.

### 8.3 Durability and group commit

Group commit (Section 7.2) does not weaken this. The single durability barrier of a batch covers every mutation in that batch, and each caller is acknowledged only after that barrier completes, so the per-request durability contract holds for every member of the batch. The blob durability steps for each write happen before that write's mutation is submitted to the writer, so by the time a mutation is in a batch its blob is already durable irrespective of when the batch commits.

### 8.4 SQLite durability settings

The metadata database's own durability is governed by its synchronous setting. Cairn defaults to the fully-synchronous setting, under which a committed transaction is durable against power loss, and exposes a relaxed setting as a documented throughput option under which the last few committed transactions may be lost on power loss without database corruption. The write-ahead log is checkpointed by a background task on an interval and when it exceeds a size threshold, using a truncating checkpoint so the log does not grow without bound under sustained writes. The truncating checkpoint is dispatched onto the single writer thread so it never races a mutation, and because a stalled checkpoint would freeze every queued write it runs with its busy handler dropped: a checkpoint contended by an active WAL reader — the normal case, since the read pool holds long-lived snapshots — returns immediately as busy and is retried on the next interval rather than blocking the writer. Checkpoint runs, the count that returned busy, and the log size are observable as metrics so an operator can see whether long-lived readers are starving the checkpointer.

### 8.5 Reconciliation as the recovery and integrity mechanism

Reconciliation compares on-disk artifacts with metadata using bounded pages and bucket workers
(F-8, F-9); total work remains proportional to stored artifacts. Membership protects authoritative
object and part references, every outstanding storage intent alias, and every cleanup-debt path,
including pending, claimed and expired claims. Root staging aliases are checked across metadata
shards. Session absence alone never authorizes recursive deletion. The walker reclaims only
recognized, unprotected files older than the safety margin and prunes empty directories; unknown
layouts and symlinks are preserved and reported. A missing directory-entry type (`DT_UNKNOWN`) is
resolved with descriptor-relative, no-follow metadata lookup; lookup failure fails the scan rather
than silently skipping a subtree.

The full walk remains a mandatory pre-bind startup gate. Under exclusive node ownership, startup
commits a new generation, invalidates old cleanup claims, probes prior intent files for outstanding
I/O, resolves those intents and recovers multipart completion ownership. It then performs the full
scan, releases only eligible legacy orphan accounting after that scan succeeds, and drains exact
cleanup claims before serving. Current intent and cleanup accounting cannot be retired merely
because a scan completed. An oracle error, filesystem walk/barrier failure or unresolved cleanup
aborts stack construction. Full scans remain active; the journal does not authorize scan-free
startup or nested placement.

Operators of very large stores can run the explicit integrity command for additional out-of-band checks, but there is no startup opt-out. A lazy per-read integrity check remains an always-on safety net: a read whose blob is unexpectedly missing returns a clear error, emits a metric, and flags the row for repair. A repair mode of reconciliation can additionally drop rows whose blobs are missing, which is needed only for recovery from external damage or from a backup taken in the narrow window described in Section 31.4. It cannot bypass Object Lock: the writer preserves still-retained or legally-held rows even when their blobs are missing, reports them as unresolved protected damage, and makes the repair command fail rather than manufacturing a clean report by deleting WORM metadata.

### 8.6 Single-node durability guidance for operators

Because Cairn does not implement drive redundancy, its single-node durability is exactly the durability of the storage beneath it. The guidance, stated in the operations section, is to place the data filesystem on redundant storage: a checksumming, redundant filesystem such as ZFS gives both redundancy and silent-corruption detection, software or hardware RAID gives redundancy, and a cloud block volume gives provider-level redundancy. Cairn's contribution is that it never lies about durability: it acknowledges a write only when the kernel has acknowledged the underlying writes per the configured synchronous settings, so whatever guarantee the storage layer provides is faithfully reflected to the client. Cairn also detects silent bit-rot of its own stored bytes. Two mechanisms, and they are not interchangeable. On the **read** path, encrypted and compressed blobs are integrity-checked on every read (per-block AES-GCM authentication and the self-describing CRNB format both fail closed on a tampered or rotted block) — but that only ever covers bytes somebody actually GETs. Cold data is exactly what a scrub is for, so the **opt-in background scrub** (`CAIRN_SCRUB_INTERVAL_SECS`, **off by default**) re-reads *every* stored blob on its schedule and verifies it against the internal plaintext SHA-256 recorded at ingest, or for legacy rows a stored full-object supplementary checksum or single-part ETag: plaintext, compressed, **and encrypted** — an encrypted version is re-read through its own unsealed DEK, so a node running `CAIRN_ENCRYPT_AT_REST` (where every version carries an SSE descriptor) is fully covered, and the ETag comparison holds because the ETag is the plaintext MD5 under encryption too (Section 27). A detected mismatch is logged and counted as `cairn_scrub_corruption_total` rather than served to a client. Six residues are counted rather than verified, and reported as `cairn_scrub_skipped_total{reason}` so coverage is never overstated: a version whose key is temporarily off the master ring (`key_unavailable` — a rotation window, retried, not alarmed); a legacy multipart object lacking both an internal digest and a full-object supplementary checksum, whose composite `{md5}-{n}` ETag cannot be hash-compared (`composite_etag`, Section 26.4); a transient filesystem read failure (`io_error` — the blob is left untouched and retried on the next scrub, never counted as corruption); a version referencing no blob (`no_blob`); a delete marker (`delete_marker`); and a version whose metadata could not be loaded (`metadata_unavailable`). A checksumming, redundant filesystem such as ZFS remains the recommended defense-in-depth (it also repairs, which Cairn does not).

---


## 9. On-disk storage model and layout

### 9.1 Principles

Object bytes live as files named by an opaque identifier, never by key; metadata is the source of truth; a blob is visible only when referenced, while admitted writes and cleanup debt also protect their exact physical paths during recovery. Versioning, tagging, ACLs, and the rest are metadata concerns and do not change the fundamental on-disk shape: a version of an object is simply another row referencing another blob, and a delete marker is a row with no blob at all. This is why the storage model scales to the full feature set without a redesign: features accrete in metadata, not on disk.

### 9.2 Directory layout

The data filesystem holds a staging area for in-progress single-part writes, a multipart staging area organised per upload session, and one directory per bucket holding that bucket's committed blobs under their opaque identifiers. The database file lives on the same filesystem. The staging areas exist so that the only way a blob enters a bucket directory is by atomic rename after being fully written and fsynced, which is the basis of the commit protocol. New writes use `bucket/<32-lowercase-hex-UUID>` (flat placement). On Linux, reconciliation also understands exactly `bucket/<first-two-UUID-hex-digits>/<32-lowercase-hex-UUID>`; this is a compatibility safeguard for the laboratory fanout evaluation, not adoption of nested writes. Existing metadata-named flat and nested reads remain valid. Unknown names, prefixes, depths and symlinks are preserved and reported as errors, never recursively reclaimed. Bucket enumeration streams directly into the bounded worker set; each worker holds bounded bucket/leaf entry pages and at most one nested leaf cursor. Directory-relative descriptors anchor creation, referenced input opens, traversal and exact cleanup.
Descendant opens require Linux `openat2` with `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS` and
`RESOLVE_NO_XDEV`; this rejects descendant mounts, including same-device bind mounts that device
comparisons cannot detect. The configured root itself may be a mount. Linux 5.6 or newer with those
resolution flags available is required; unavailable support fails closed. Root initialization
creates at most the final configured directory under an existing parent, then creates known staging
children through those descriptors; it rejects staging symlinks or mount crossings before writing
through them. Identity checks also precede pruning. Empty removal uses `rmdir` semantics, so a repopulated directory survives. Reclaimed files are followed by directory synchronization; a removed leaf or bucket is acknowledged only after its parent synchronizes. A sync failure propagates as a reconciliation failure. Full startup scans remain mandatory and proportional to stored artifacts; fanout reduces neither inode count nor the amount of scan work. Unsupported platforms do not fall back to a weaker namespace walk.

### 9.3 The blob file format

CRNB v1–v3 writers and readers share a 64-MiB index ceiling with nine bytes per block entry.
The effective encoded-object limit is `floor(64 MiB / 9) × logical_block_size`, also bounded by
`CAIRN_MAX_OBJECT_SIZE`: 488,671,805,440 bytes (about 455.11 GiB) for encryption-only 64-KiB blocks,
or 1,954,687,221,760 bytes (about 1.78 TiB) for default 256-KiB compression blocks. Smaller configured
blocks have proportionately smaller limits. Known encoded lengths are rejected before staging;
unknown-length streams fail before encoding an excess block. Multipart checks the recorded total
with overflow-safe addition before assembly and still checks streamed bytes. Rejection uses
`EntityTooLarge` and preserves retryable parts. Raw plaintext files have no CRNB index limit.

This prevents new writers from publishing a container the reader refuses for index size. It does
not repair an existing oversized container or promise 5-TiB encoded-object support.

Readers scan the index in 65,529-byte pages (7,281 entries), retaining a SHA-256 fingerprint and
starting physical offset per page. Summaries remain provisional until the complete index passes
structural validation and, for v3, the existing index/trailer HMAC. Later page loads use the same
file descriptor and verify the entire page fingerprint before interpreting any entry. One verified
page and its physical offsets are cached at a time; trailer geometry is never reloaded. This keeps
index working allocations below 192 KiB at the format ceiling, including page summaries, without
changing CRNB v1–v3 bytes. Plain v1 gains no initial cryptographic authentication.

The initial GET probe transfers its prepared reader to the response body, avoiding a second
complete parse and preventing a replaced pathname from selecting a different encoded file after
validation. Initial index I/O remains linear for every open, including each replication range pass.
Larger encoded objects or sublinear authenticated opening require a separately reviewed format.

The encoder drains serialized entries after each 64-KiB input batch and updates the v3 metadata
HMAC incrementally. An index of at most 64 KiB stays in a bounded request-owned buffer. Larger
indexes spill to a buffered temporary file on the data filesystem at the exact index-spool alias
already included in admission. Creation uses the anchored namespace and locks the file; the create
job unlinks only that newly created inode before returning its leased file owner. Queued writes,
reads and returned buffers retain that owner after request cancellation. A crash before unlink or
before its namespace barrier can leave or restore the alias, so publication/recovery records exact
cleanup even when the spool name appears absent. Full scans protect that debt until its claimed
cleanup proves durable absence. The spool is flushed and copied in 64-KiB batches into the final blob, followed by its unchanged MAC and trailer, **before** that
blob's existing file-sync/rename/directory-sync sequence. Scratch data has no separate fsync or
committed sidecar. Errors, including ENOSPC during spool flush/copy, abort publication.

Encoder payload buffers depend on trusted block geometry and a bounded feed batch, not object
length; the caller's incoming body chunk and kernel file cache are separate resource costs. The
spool can occupy up to the existing 64-MiB format cap on disk. This removes per-block heap state
from writes, but adds scratch I/O for larger indexes; no throughput gain is claimed without measurement.

An uncompressed object is stored as exactly its bytes, with no header and no framing, so the simplicity and the byte-for-byte promise are preserved for the default case and an operator can read a blob directly if they ever need to. A compressed object is stored in a self-describing format: a sequence of independently-compressed logical blocks followed by an index that records, for each block, its size and offset, followed by a fixed trailer that records the index's location, the block size, the compression algorithm, the logical (plaintext) length, a magic marker, and a format-version byte. The same self-describing container also holds objects that are encrypted at rest: the version byte then marks the blob encrypted, each block carries its own AES-GCM authentication tag, and current format v3 carries an additional authentication tag over the index and trailer semantics (Section 10.7). This makes such a blob self-contained for layout and range reads, but the file is not allowed to choose its own compatibility parser: the object row's SSE descriptor (or a multipart part's sealed-DEK marker) independently declares legacy v2 or authenticated v3, and the reader requires an exact match before returning bytes. Reconciliation and backup treat the whole file as one artifact; there is no sidecar to keep in sync. Whether a blob is compressed, which algorithm it uses, and its logical block size are also recorded in the metadata row and threaded into the reader. For legacy encrypted v2, whose trailer is not authenticated, the reader requires the trailer algorithm and block geometry to equal that trusted descriptor before it may drive decompression or range mapping. Section 10 specifies the compression scheme in full.

Legacy v2 additionally receives the trusted object row's `size_logical` or multipart
`PartRef.size`; its unauthenticated trailer and index logical total must equal that independent
value before any bytes are returned.

### 9.4 Storage paths, versions, and delete markers

A storage path identifies a blob within the data directory and is recorded on the object-version row. Each distinct version of a key has its own row and its own blob under its own identifier; overwriting a key in a versioning-enabled bucket creates a new row and a new blob and leaves the previous version's row and blob untouched, which is what makes versioning cheap and safe under the UUID model. A delete marker is a version row carrying a flag and no storage path, representing a logical deletion that hides older versions from a plain GET while leaving them retrievable by version identifier. Permanent deletion of a specific version removes its row and reclaims its blob through the normal post-commit reclamation path.

### 9.5 Multipart staging

Each multipart upload session has a staging directory holding its parts. An UploadPart request must declare its exact decoded length; before the blob layer opens a file, the metadata writer transaction reserves that many bytes against both the bucket and initiating principal, and reserves the `(upload, part-number)` cardinality slot. It refuses a request that would exceed the byte quota or configured session/part ceilings. The reservation carries a fresh attempt identifier, and the blob is written under the deterministic `{part-number}-{attempt-id}` name. The same Writer savepoint admits its complete storage plan, including the optional index-spool alias. Every failure therefore has exact recovery identities even if staging never returns a path; abandoned bytes remain charged until all associated physical cleanup debt is durably retired.

Recording a successful attempt atomically consumes its reservation. Re-uploading a part number never clobbers the previous file: the replacement becomes authoritative and the superseded path becomes explicit cleanup debt whose bytes remain charged until deletion succeeds. The same conservative accounting applies to completion and abort: terminal metadata removal converts remaining part/reservation bytes into accounting debt linked to exact physical cleanup paths. Admitted attempts retain their ownership until quiescence is established. Each filename is reclaimed under a Writer claim; empty session directories may then be pruned. Charges are released only after the last associated physical debt retires. Thus a crash or filesystem error can temporarily over-count staging use but can never make unremoved bytes disappear from quota accounting.

The directory chain is made durable from parent to child: store initialization creates `.staging/multipart` and fsyncs each parent whose directory entry changed, and the first part for a session creates its session directory and fsyncs `.staging/multipart` before opening the part file. A completed part is fsynced before its session directory is fsynced. This ordering means a successful part upload cannot depend on directory entries that a power loss is still permitted to forget. Assembly streams the ordered parts into a single committed blob through the same durable commit sequence as a single-part write, applying compression during the assembly pass if the bucket enables it. When the upload is server-side encrypted, each part is instead staged as an encrypted block container under its own fresh data key so that nothing plaintext reaches disk; assembly then decrypts each part on read — failing closed on a wrong key or tampered part rather than writing a partial or plaintext object — before re-encrypting the assembled object under its own data key. The decision to encrypt parts is fixed when the upload is initiated, an encrypted part is physically larger than its plaintext, and a re-uploaded part number is staged under a distinct data key.

Metadata status and the persisted per-attempt claim token protect those bytes during the terminal race: completion atomically owns `completing` under its token, abort can remove only `active`, and release or final completion must match that exact owner. A stale cancellation cannot release a newer completer. A failed or cancelled completion releases only its exact claim after outstanding storage I/O is quiescent; a successful completion removes the session in the same transaction that installs the already-durable assembled object, then reclaims the parts. A bounded background pass retries exact-path cleanup debt and aborts eligible stale sessions through the Writer. An admitted reservation is not reclaimed merely because it is old: cancellation recovery or exclusive startup recovery must first establish I/O quiescence. The mandatory full startup reconciliation protects exact part references, intent aliases and every cleanup-debt path even inside an absent session; only after a successful full walk may the writer release eligible legacy orphan accounting (Section 8.5). The stale-session pass is bounded to 10,000 items or 30 seconds. The same task independently checks exact cleanup every second while idle, processes at most eight paths concurrently from a 1,000-path batch, and limits each batch to 30 seconds. A full successful batch continues promptly after yielding and checking shutdown and the stale-session deadline; failures retain their claims and debt for retry.

---


## 10. Transparent compression at rest

### 10.1 Goals and the tension with simplicity

Compression saves disk and, for compressible data, can save I/O time because fewer bytes are read and written. It is in scope by operator request. It is in tension with the byte-for-byte simplicity that is one of Cairn's selling points, and the design resolves the tension by making compression opt-in per bucket and off by default. A bucket with compression disabled stores blobs exactly as received, preserving the simple model; a bucket with compression enabled gains the space saving while Cairn hides the compression entirely behind the S3 contract, so clients neither know nor need to know that bytes are compressed on disk.

### 10.2 Preserving S3 semantics

Two S3 semantics must survive compression. The object's reported size is its logical, plaintext length, which is what a client wrote and what range arithmetic is computed against; the physical on-disk size is separate and is exposed only to the operator through metrics and the management API. The object's ETag must remain what S3 clients expect: for a single-part object it is the MD5 of the plaintext content, computed during ingest over the uncompressed bytes before or as they are compressed; for a multipart object it is the MD5 of the concatenated per-part plaintext MD5 digests with the part-count suffix, where each part's MD5 is computed over that part's plaintext as it is uploaded. Compression therefore never enters the ETag computation, and an object's ETag is identical whether or not the bucket compresses. Client-supplied checksums are likewise validated against the plaintext.

### 10.3 The block scheme and why it is block-based

Compression is applied per fixed-size logical block rather than as one stream over the whole object, and this is the central design choice that keeps ranged reads efficient. If an object were one compressed stream, serving a range that begins near the end of a large object would require decompressing everything before it, turning a cheap range read into a full-object decompression. By compressing independent blocks of a fixed logical size and recording each compressed block's location in the index trailer (Section 9.3), Cairn can serve a range by reading and decompressing only the blocks that overlap the requested range and then slicing to the exact bounds, so the cost of a ranged read is proportional to the range plus at most one block of overhead, not to the offset. The block scheme also makes decompression parallelisable across blocks for large reads and keeps per-block memory bounded.

All CRNB versions bind trailer algorithm, block size and logical total to the authoritative
object/part metadata before serving bytes. At open, each raw physical payload must equal its
logical length and each compressed payload must be nonempty and strictly shorter; encrypted
entries additionally contain a 16-byte GCM tag. This bounds block-read allocations by trusted
logical geometry even when an unencrypted index or trailer is damaged. Physical-file length
alone is not a sufficient allocation bound.

Replication's guarded reader also carries a shared buffer reservation through the probe and
streaming blocking tasks, their prepared-reader results, and the response body. Cancelling the
HTTP delivery drops its own reference, but the reservation remains charged until the underlying
filesystem work and returned buffers release it. `BlobStore::read_memory_bound` supplies the
backend's decoder, page and queued-frame allowance before replication admission; the engine no
longer duplicates CRNB allocation assumptions. The allowance excludes kernel file cache, allocator
retention and caller-owned network/control buffers. Coalesced raw reads are bounded by the probed
length even if a local writer subsequently grows the file.

### 10.4 Algorithm choice and the incompressibility heuristic

The default algorithm balances ratio and speed and is the modern general-purpose choice; a faster, lower-ratio algorithm is available for throughput-sensitive deployments, and compression can be off even within an otherwise compression-enabled policy. The algorithm and level are part of the per-bucket compression policy. Compressing already-compressed or incompressible data wastes CPU and can slightly enlarge the data, so Cairn applies a heuristic: object content types that are known to be already compressed, such as common image, video, audio, and archive formats, are stored uncompressed regardless of the policy, and for other content the first block is test-compressed and, if it fails to shrink beyond a threshold, the object is stored uncompressed. This keeps compression from ever hurting, at the cost of a small test on ingest. The decision per object is recorded so reads know the truth.

### 10.5 Interaction with the write, read, and multipart paths

Multipart assembly reuses one lazily allocated 64-KiB plaintext read buffer across parts; an
all-encrypted assembly allocates no such buffer and retains the bounded decrypt-on-read path.
Hashes and transforms consume only bytes actually read, including the final partial chunk.
Durable part files are still copied into the final object: this reduces allocation churn, not the
approximately two payload writes inherent in staging parts followed by final-object assembly.
Assembly, permit-wait and durability timings are exposed separately (Section 26.2); metadata commit
occurs afterward and is measured by the metadata writer.

On a single-part write to a compressing bucket, the ingest pass computes the plaintext MD5 and any requested checksums and simultaneously feeds the block compressor, producing the framed blob and its index in one streaming pass with bounded memory, after which the normal durable commit sequence applies to the framed file. On a read, an uncompressed blob takes the ordinary and possibly zero-copy path, while a compressed blob is read by consulting its trailer and index and decompressing the needed blocks through userspace, which is why compressed objects do not use the zero-copy fast path; for a full-object read this is a streaming decompression with bounded memory, and for a ranged read it is the block-selective path of Section 10.3. On multipart completion to a compressing bucket, compression is applied during the assembly pass that concatenates the parts, while the part MD5s used for the ETag were already computed over plaintext at upload time, so the multipart ETag is unaffected. Copy operations that change nothing about the bytes can copy the stored representation directly when source and destination compression policies match, and otherwise decompress and recompress as needed.

### 10.6 Operability of compression

The space saved is observable: the management API and metrics expose logical versus physical bytes per bucket and overall, so an operator can see the compression ratio being achieved and decide whether a bucket's policy is worthwhile. Changing a bucket's compression policy affects only objects written after the change; existing objects keep their stored form and remain readable because each blob is self-describing, and a deliberate rewrite, which a lifecycle action or an administrative tool can perform, is required to recompress existing data. This keeps policy changes cheap and safe.

### 10.7 Encryption at rest reuses the block container

Server-side encryption at rest (Section 27) is layered onto the same self-describing block container as compression rather than a separate format. When a data-encryption key is supplied — for an SSE-S3, bucket-default, `aws:kms`-labelled, or at-rest-mode object — each logical block is compressed first and then encrypted with AES-256-GCM (compress-then-encrypt, because ciphertext does not compress), the 16-byte GCM tag is appended to the block so its recorded physical length covers ciphertext-plus-tag, and the trailer's version byte marks the blob encrypted so a read attempted without a key fails fast rather than returning ciphertext. The per-block 96-bit nonce is derived deterministically as the first twelve bytes of HMAC-SHA256 of the data key over the little-endian block index, so nonces are unique per block without any nonce being stored on disk and never repeat for a fixed key within a blob. Encrypted format v3 additionally appends a domain-separated HMAC-SHA256 tag over every byte of the plaintext index and fixed trailer. The reader checks bounded geometry against authoritative metadata and streams structural validation, the metadata HMAC, and page fingerprints. Its provisional summaries become usable only after complete index/trailer authentication succeeds. It does not trust the trailer's version byte to select those semantics: new object descriptors persist `blob_format_version: 3`, new encrypted multipart-part envelopes carry a `crnb3:` prefix, and the reader requires that declaration to match the file. Only an absent object marker or an unprefixed part envelope — representations written by the legacy v2 code — authorizes the v2 parser; unknown explicit versions and either mismatch direction fail closed. Legacy encrypted v2 blobs therefore remain readable without a schema migration, but their unauthenticated semantics are constrained by writer invariants before any block read: trailer algorithm/block size must equal trusted metadata; after subtracting the 16-byte GCM tag, a raw entry must have physical length exactly equal to logical length, while a compressed entry must have a non-empty physical payload strictly shorter than logical length. Those disjoint lengths prevent an unauthenticated compression-flag flip even when one byte string is a valid same-length compressed/plaintext polyglot. Any normal rewrite produces v3, providing an incremental migration path without an offline format conversion. An unencrypted blob is byte-for-byte identical to the pre-encryption format, so existing blobs read unchanged. Decryption fails closed: a wrong or missing key, a tampered or bit-rotted block, a metadata/container version mismatch, trusted-compression mismatch, or authenticated-metadata mismatch returns an error rather than plaintext, ciphertext, compressed representation, or zeros. Key management — the per-object data key sealed under the master-key ring — is specified in Section 27.

The trusted logical total is necessary in addition to those per-block invariants: without it, a v2
attacker could flip a compressed entry to raw and shrink both unauthenticated length fields to the
shorter authenticated payload. Object GET and multipart assembly both reject that construction at
open, before producing plaintext.

---

### 10.8 Internal content integrity baseline

Every newly staged or assembled object computes a plaintext SHA-256 in the existing streaming
hash pass, alongside MD5 and requested S3 checksums. The internal digest is committed on the
object-version row with the durable blob reference. PUT, copy, multipart completion, import and
replica ingest all use that path, including compressed and encrypted objects. It is independent
of the S3 ETag and checksum headers; requesting SHA-256 reuses the same hash state.

Existing rows retain a null internal digest after migration. Scrubbing must not populate it from
existing bytes: those bytes may already be damaged. A rewrite creates a new baseline but cannot
prove the old content was correct. Backup and restore preserve the digest through SQLite snapshots.
