# Security, threat model, and the error model

> Part of the Cairn reference docs (split from the former ARCH.md). The section numbers below are stable identifiers used throughout the code and docs; see the index in [`README.md`](./README.md) and [`../CLAUDE.md`](../CLAUDE.md).

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


