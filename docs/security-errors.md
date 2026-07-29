# Security, threat model, and the error model

> Part of the Cairn reference docs. The section numbers below are stable identifiers used throughout the code and docs; see the index in [`CLAUDE.md`](./CLAUDE.md) and [`../CLAUDE.md`](../CLAUDE.md).

## 25. Error model and S3 error mapping

### 25.1 Typed errors and one translator

Each module defines its own typed errors describing what went wrong in its own terms, rather than passing strings around, so that the cause of a failure is preserved with structure as it propagates (F-22). At the protocol boundary a single translator maps every internal error to the wire response: for the S3 surface to the S3 XML error document with a code, a human-readable message, the resource, and a request identifier, paired with the right HTTP status; for the management surface to the JSON error envelope. Keeping the mapping in one place makes it total and testable, and a test enumerates every internal error variant and asserts that each maps to a defined status and code, so no failure can reach a client as an unmapped internal error.

### 25.1.1 Content negotiation: readable pages for browsers

An object store is addressed by both programs and people. The same URL that an SDK follows is also
pasted into a browser — a share link, a presigned link, a typo'd console path — and answering a
person with a raw `<Error>` XML document tells them nothing they can act on. Cairn therefore renders
a **human-readable HTML error page** when, and only when, the request is unambiguously a browser
performing a top-level navigation; every other client receives the byte-identical XML or JSON
described above. The predicate is deliberately narrow, and each clause is chosen against measured
client behaviour rather than assumption:

- the method is `GET` (never `HEAD`, which may carry no body, and never a mutating verb);
- `Accept` lists `text/html` as an exact media type — no AWS SDK, CLI, Go or Python client sends
  this, so machine traffic is unaffected by construction; and
- the request carries a browser navigation signal: `Sec-Fetch-Dest: document`, or, on a plain-HTTP
  origin where browsers suppress Fetch Metadata entirely, `Upgrade-Insecure-Requests`. This second
  clause excludes the one common non-browser that does send `text/html` (a bare
  `java.net.HttpURLConnection`), and excludes `<img>`/`<video>`/`fetch()` subresource loads, which
  must keep the machine-readable body.

Both branches carry `Vary: accept, sec-fetch-dest, upgrade-insecure-requests`, so a shared cache can
never serve one client class the other's body shape. The page is self-contained — inline CSS, no
scripts, images, or webfonts — because it must render on the S3 listener, where the console's assets
do not exist, and while the node is degraded enough to be returning a 5xx. It states the HTTP status,
a plain-language explanation of that specific failure, and the S3 code, resource, and **request id**,
so an operator can grep the logs for the exact failure. The share route deliberately omits the
resource: the token is a credential and is never echoed into the page. Because the page interpolates
an attacker-controlled path, it is HTML-escaped and additionally served with
`Content-Security-Policy: default-src 'none'` and `X-Content-Type-Options: nosniff`, making a
reflected script structurally impossible rather than merely escaped away. The generalization of 5xx
detail (§25.1) holds identically on this surface: the page renders fixed copy, never the cause.

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
| Invalid Object Lock enablement/versioning transition | 409 | InvalidBucketState |
| Object Lock blocks a permanent delete or retention weakening | 403 | AccessDenied |
| Setting an ACL while ownership disables ACLs | 400 | AccessControlListNotSupported |
| Missing or unparseable credentials | 400 / 403 | AccessDenied / InvalidAccessKeyId |
| Signature mismatch or invalid signature | 403 | SignatureDoesNotMatch |
| Authenticated identity policy unavailable or corrupt | 500 | InternalError |
| Authorization denied by policy, ACL, or public-access block | 403 | AccessDenied |
| Range not satisfiable | 416 | InvalidRange |
| Operation not implemented | 501 | NotImplemented |
| Unexpected internal failure | 500 | InternalError |

The request identifier appears in the error body, as a response header, and in the request's trace span, so that a client report can be tied to the exact server-side trace.

---


## 27. Security and threat model

### 27.1 Assets and trust boundaries

The assets Cairn protects are the object bytes, the user credentials, the destination credentials for replication, webhook HMAC signing keys, and the metadata database. Cairn trusts the host's filesystem and, when used, only a terminating proxy whose immediate peer address is explicitly allowed by `CAIRN_TRUSTED_PROXIES`; it does not trust forwarding metadata merely because a request contains it. It does not trust request bodies, headers, keys, query strings, policy or configuration documents, or the contents of the metadata database against tampering by anyone who can read the file, which is why secrets in it are encrypted. The deployment is expected either to terminate TLS in Cairn or to sit behind a proxy that does, and never to expose the plaintext interface to an untrusted network.

The browser-facing socket boundary is also a trust boundary. The control origin serves only the embedded console and `/api/v1`, and is the only listener that honours the administrator's httpOnly cookie. The data origin serves S3, STS, public shares, and infrastructure endpoints and never interprets that cookie as authentication. Object bytes therefore cannot become active content in the privileged console origin; console transfers use exact, short-lived SigV4 URLs backed by durable scoped sessions. A reverse proxy must expose the two listeners as distinct origins rather than recombine them under one browser origin.

### 27.2 Transport security

Cairn can terminate TLS itself using a modern Rust TLS stack with current defaults and reloadable certificate material, so a deployment is secure on the wire without an external proxy (N-6), and it also runs behind a terminating proxy on a trusted interface. Direct TLS cannot be downgraded by forwarded metadata. On plaintext control connections, Cairn derives external HTTPS only from syntactically valid, non-conflicting `Forwarded` / single-valued `X-Forwarded-Proto` assertions delivered by an allow-listed immediate peer; every other case fails closed to HTTP for cookie attributes. For `aws:SourceIp`, an untrusted immediate peer's forwarding headers are ignored and the canonical socket address is used. An allow-listed peer must provide a valid `Forwarded` `for=` or `X-Forwarded-For` chain; Cairn walks it right-to-left only while predecessors are themselves allow-listed, and requires both families to resolve to the same canonical client when both are present. Missing, malformed, or conflicting provenance makes the condition key absent rather than treating the trusted proxy as the client, preserving CIDR-deny policies. The plaintext sendfile path falls back to the general request path when provenance is unavailable. Where Cairn terminates TLS and the platform supports kernel TLS, the read fast path can stay zero-copy by offloading symmetric encryption to the kernel after a userspace handshake (Section 7.6). The control plane, including the web console and the CLI, is held to the same transport expectations as the data plane.

### 27.3 Authentication and authorization controls

Authentication uses SigV4 and Bearer with constant-time comparison and signature-skew and credential-scope validation (Section 14). Authorization is the multi-source engine of Section 15 with its explicit precedence, in which an explicit policy deny overrides everything, Block Public Access gates public grants ahead of ACL and policy, and the bucket-owner-enforced ownership mode disables ACLs to remove the most common accidental-exposure vector. Identity-policy state is fail-closed before that engine runs: only a successful `NULL` read means no policy; a metadata error or malformed stored document denies authentication as an opaque internal failure and is never cached as absence. Every administrator-derived session mint — management API, AWS STS, or console presigning — uses one composition rule: every parent identity Deny is retained, while a requested boundary supplies the only possible Allows or the unbounded STS form receives the explicit full-S3 baseline. Policy resolution or composition failure creates no credential and returns an opaque internal error. Block Public Access at the account and bucket level lets an operator guarantee that nothing is inadvertently public regardless of individual bucket settings (N-5).

### 27.4 Secrets at rest

SigV4 secrets, replication destination credentials, webhook HMAC signing keys, temporary
session-credential secrets (Section 14.6), S3-import source credentials, and per-object
data-encryption keys (Section 27.8) are all stored encrypted under a master key supplied to the
process out of band, using authenticated encryption, so that reading the database file does not
yield usable secrets (F-15). One exhaustive sealed-secret registry names those six durable streams
(object versions, users, replication targets, notifications, sessions, and import jobs) and is
shared by the SQLite re-wrap worker, retirement gate, and status endpoint; an old key is not
retire-eligible until every stream has completed a failure-free pass on every shard. Import history
remains in that population until retention deletes its row. In memory, configuration secrets,
opened remote credentials, master keys, DEKs, share bearer tokens, and derived streaming signing
keys use redacting, zeroize-on-drop owners; the 256-bit key owner is deliberately non-`Copy`, so
crossing a crate boundary cannot silently duplicate key bytes. `SystemCrypto` keeps master keys in
zeroizing owners and constructs only short-lived AES-GCM schedules for envelope operations. Enabled
upstream crypto features scrub AES schedules and supported GHASH/POLYVAL backends on drop; upstream
`polyval 0.6` still lacks drop scrubbing for its aarch64 PMULL backend, so host process and core-dump
controls remain defense in depth on that architecture. Bearer secrets are stored as fast
cryptographic hashes, which is appropriate because they are high-entropy machine-generated tokens
rather than human passwords. Persistent-share tokens use their own domain-separated SHA-256 lookup
hash and are returned only at mint; management reads expose only a stable non-secret id. The master
key is never written to disk by Cairn and is kept out of the backup that contains the database, so
that the backup alone does not disclose the secrets, and rotating the master key re-encrypts the
stored secrets.

For the SQLite rotation state, a master-key id is permanently bound to the full 256-bit SHA-256 hash
first stored for it; the API's eight-hex fingerprint is display-only. Startup preflights that
binding and every retirement-progress read across all physical shards before it writes any
key-state row or permits a path to seal a secret. A matching legacy eight-hex row is atomically
upgraded on the single Writer; once full, even different key material sharing that prefix is
rejected. Replacing key bytes under an existing id, any binding/progress read error, or removing an
id before all registry streams have moved is fatal; a mismatched upsert cannot overwrite the old
hash. This turns an operator configuration error or metadata fault into a refused start rather than
delayed credential/object unavailability.

### 27.5 Input safety and resource limits

The key sanitiser and the final within-root path check make key-based path traversal structurally impossible and are property-tested (Section 29). A configurable maximum object size is enforced both by early rejection on the declared length and by a streaming ceiling, optional per-bucket and per-user byte quotas are enforced inside the commit transaction, and an out-of-space condition is mapped to the correct status rather than surfacing as an opaque failure (F-16).

Multipart staging has its own fail-closed admission protocol because incomplete uploads are durable disk consumers rather than committed object rows. CreateMultipartUpload has positive configurable per-bucket and per-principal active-session ceilings, and UploadPart requires an exact decoded length. The single metadata writer reserves that many bytes and one distinct-part slot before filesystem staging; a replacement remains charged for both old cleanup debt and the new attempt until the old path is actually deleted. Abort, Complete, staging failure, process crash, and deletion failure therefore cannot erase accounting for bytes that may remain. Startup performs a path-exact reconciliation before releasing orphaned ledger rows, and a bounded recurring sweeper retries cleanup debt, abandoned reservations, and stale sessions. Admission failures return `507 InsufficientStorage`; malformed or missing part lengths return `400 InvalidRequest`.

A global concurrency limit and per-request timeouts bound resource use under load, and the bounded blob pool and the streamed, backpressured transfers prevent a few large transfers from exhausting memory or threads. The asynchronous replication sink's temporary whole-object SigV4 buffer is separately bounded by a fixed 2 GiB per-object cap and one process-wide weighted byte budget shared across every worker and target; one non-resetting delivery deadline covers upload, response head, and bounded response drain, and an untrusted destination can contribute at most 8 KiB to a retained error diagnostic (Section 20.4). The development authentication bypass is compiled out of release builds and additionally refuses non-loopback binds (F-17).

### 27.6 Residual risks

Several risks are acknowledged and accepted for the initial scope, recorded so they are known decisions rather than oversights. Object bytes are stored without whole-blob encryption unless server-side encryption is enabled for the object — SSE-S3, SSE-KMS, or transparent at-rest, each sealing a per-object data key under the master key (Section 27.8); an attacker with filesystem access can therefore read the content of any object not written under one of those modes, so the mitigations are enabling server-side encryption, which a bucket can additionally **mandate** via its encryption setting's `required` flag (Section 27.8, refusing any plaintext client PUT while transparently force-encrypting an inbound replica so replication is never broken), and full-disk or filesystem-level encryption provided by the operator, with transparent whole-blob encryption as a blob-store decorator remaining future work. SSE-KMS in this version is **label-only** — the key id is a validated label over the same master ring, not distinct key material — so it must not be relied on for cryptographic isolation between key ids (Section 27.8). A server-side-encrypted upload leaves nothing in plaintext on disk, including its in-flight multipart parts: each staged part is written as an encrypted block container under its own fresh per-part data key (itself sealed under the master key), and assembly decrypts the parts and re-encrypts the assembled object under its own data key — so a re-uploaded part gets a distinct key and an encrypted part is physically larger than its plaintext. The decision to encrypt parts is captured when the upload is initiated, so a bucket default added mid-upload applies only to the assembled object, not to that upload's already-staged parts; and if the master key sealing an in-flight part is retired before the upload completes, completion fails closed (the upload is retryable) rather than exposing plaintext. The authorization model supports per-user identity policies alongside the user role model, the bucket policy, and ACLs, but does not implement cross-account principals, temporary credentials, or federation (Section 15.1). Replicating to an endpoint exports data there, so the destination must be trusted and access-controlled; and because replication ships the **decrypted** body (the destination has a different master key and could not open the stored ciphertext), an object encrypted at the client's request is refused over a plaintext `http://` destination endpoint unless the operator opts in with `CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP` (Section 20.4, Section 28) — an earlier implementation shipped such objects' raw ciphertext instead, which either failed the destination's digest check outright or produced a right-sized, unreadable replica. And single-node durability is the durability of the storage beneath Cairn, with cross-host durability provided only by asynchronous replication, which has lag (Section 8, Section 20).

### 27.7 Outbound-endpoint safety (SSRF guard) and S3 import

Four subsystems dial an endpoint an **operator supplies**: bucket replication (Section 20), webhook event notifications (Section 20.6), **S3 import** — the migration of buckets and objects from a remote S3-compatible store (MinIO/Garage/R2/AWS/another Cairn) *into* this node — and the optional release-update feed. An unguarded operator-controlled dialer is a Server-Side Request Forgery primitive: pointing it at `http://169.254.169.254/…` (the cloud-metadata service), a loopback admin port, or an internal RFC1918 service would let an admin (or an attacker who reached the admin API) read the response back or use the request as a probe. Cairn's **SSRF guard** (`cairn-net`) centralises the defence with two checks on every dial. A URI-level connector rejects blocked IP-literal hosts before Hyper can bypass DNS resolution; hostname dials then pass through a guarded resolver that **rejects the whole resolved address set if any entry is internal** (loopback, private, link-local incl. `169.254.0.0/16`, ULA `fc00::/7`, unspecified, multicast, and the IPv4-mapped/NAT64 forms of those). The resolver runs for initial connections, redirects, and reconnects, so it checks the addresses actually used and defeats DNS rebinding. A fast validate-time check additionally rejects internal IP-literal update and environment replication endpoints during startup, and dynamic webhook/import/replication endpoints when they are created, for immediate operator feedback; connect-time enforcement remains the guarantee. The escape hatch is `CAIRN_ALLOW_INTERNAL_ENDPOINTS` (Section 28), for reaching storage deliberately deployed on a private network or an internal update mirror. Import **source credentials are sealed at rest** under the master key exactly like a user's SigV4 secret and are never returned by any endpoint (the create response and the job list/detail views are secret-free); the plaintext exists only transiently in a `Zeroizing` buffer inside the import worker. Imported objects land through the normal object-write path (so SSE, compression, quota, and versioning all apply). Source connect, response-head, body-idle, and whole-object deadlines prevent a malicious or broken source from pinning a worker indefinitely; timed-out attempts release their RAII permit before backoff, and bounded concurrent jobs share one fair global semaphore held below the blob-I/O pool, so one stalled job cannot starve live traffic or later imports. Operator runbook: `docs/migration.md`. v1 imports the **current version** of each object (metadata, tags, and standard headers preserved; historical versions, Object-Lock/retention state, and per-object ACLs are deferred).

### 27.8 Server-side encryption model (SSE-S3, SSE-KMS, at-rest)

Where an object version is encrypted, its blocks are sealed under a fresh random 256-bit data-encryption key (DEK) minted per version; the DEK is itself wrapped with authenticated encryption under the node master-key ring (the envelope of Section 27.4) and only that wrapped form is persisted, in the version's `sse_descriptor` — the raw DEK is never stored and lives only transiently in memory. An object becomes encrypted in one of three ways, recorded as the descriptor's mode: **SSE-S3**, requested by the client with `x-amz-server-side-encryption: AES256`; **SSE-KMS**, requested with `aws:kms` (plus an optional key id and bucket-key flag); and transparent **at-rest**, which the operator enables node-wide with `CAIRN_ENCRYPT_AT_REST` (Section 28) and which the client neither requests nor is told about — it is an operator storage property, not an SSE contract a client can rely on, so it is advertised to no one. The mode is only a labelling and advertising discriminator: all three seal the DEK under the same master ring, so the on-disk envelope is byte-identical across modes.

SSE-KMS is **label-only** in this version, an accepted and documented limitation rather than an oversight: the `aws:kms` key id is a validated label over the *same* master ring, not distinct key material and not cryptographic isolation. Because every DEK is wrapped by the one master ring, removing a key id from the allow-list does not lock existing objects (a read unwraps under the master key and ignores the key id), and two objects under different key ids are no more isolated than any two objects. A `KeyProvider` abstraction (v1 `LocalRingProvider`) resolves a key id to its sealing crypto and validates a requested id, shaped so a real external provider (AWS KMS, Vault) with genuine per-key material and revocation can slot in later without touching the S3 surface; until then the open path fails **closed** (a decrypt error, never plaintext) on any mismatch. The `CAIRN_KMS_KEY_IDS` allow-list gates **writes only**: unset accepts any id, and when set a write naming an id not on the list is refused fail-closed (`InvalidArgument`); a no-id `aws:kms` write names nothing to gate and is accepted. The id is validated where the write is planned — a single-part PUT at resolve time, a multipart upload at `CreateMultipartUpload` initiate — so an unknown id is rejected up front and never silently downgraded to SSE-S3 or plaintext at completion.

A bucket may **mandate** encryption through the `encryption` aspect's `required` flag: a client PUT whose resolved plan is not an advertised SSE mode — plaintext or transparent at-rest — is refused, while an inbound replica is force-encrypted (SSE-S3) rather than refused, so enabling the policy never breaks replication into the bucket. Every seam fails closed: a missing or wrong master key or a tampered DEK envelope yields a decrypt error on open, never plaintext, zeros, or partial data. The blob layer additionally refuses to read an encrypted blob when the caller supplies **no** key at all — framing is decided from the caller's stored descriptor, so such a read would otherwise stream the raw container bytes at exactly the plaintext length — and each refusal increments `cairn_blob_encrypted_without_key_total`. That guard has one benign, structural false positive: an object whose body *is* the verbatim bytes of an encrypted blob file, which is what a backup that copies the data directory into a bucket produces; those bytes remain intact on disk and recoverable out of band, and only the API read is refused. Part-level multipart encryption and the two fail-closed windows it entails (SSE intent pinned at initiate; master-key retirement mid-upload) are described in Section 27.6.

---
