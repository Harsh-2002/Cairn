# S3 API Support Matrix

Cairn implements the practical S3 control set. Operations are addressed path-style
(`/{bucket}/{key}`) over HTTP/1.1 and HTTP/2; SigV4 (header + presigned) and a first-party
Bearer scheme authenticate; a real policy/ACL/Block-Public-Access/Object-Ownership engine
authorizes.

| Operation | Supported | Notes |
|---|---|---|
| ListBuckets | ✅ | |
| CreateBucket / DeleteBucket / HeadBucket | ✅ | Delete requires empty; force-empty via the management API. |
| GetBucketLocation | ✅ | Returns the configured region. |
| GetBucketVersioning / PutBucketVersioning | ✅ | Unversioned / Enabled / Suspended. |
| GetBucketPolicy / PutBucketPolicy / DeleteBucketPolicy | ✅ | Validated by the policy engine. |
| GetBucketCors / PutBucketCors / DeleteBucketCors | ✅ | Per-bucket config (validated). |
| GetBucketTagging / PutBucketTagging / DeleteBucketTagging | ✅ | |
| GetBucketLifecycleConfiguration / Put / Delete | ✅ | Expiration, noncurrent expiration, abort-incomplete, delete-marker removal. **Storage-class transition/tiering is not supported**: a Put containing a `Transition` rule is rejected (`NotImplemented`) rather than silently no-op'd. |
| GetBucketReplication / Put / Delete | ✅ | Enqueue-on-write + worker drains the outbox to a configured remote via a real SigV4-signing sink (verified node→node); one or more named targets (`CAIRN_REPLICATION_TARGETS`) with per-rule destinations. |
| ListObjectsV2 / ListObjects (v1) | ✅ | Prefix, delimiter, pagination (opaque tokens), start-after / marker. |
| ListObjectVersions | ✅ | Distinguishes versions from delete markers. |
| ListMultipartUploads | ✅ | |
| PutObject | ✅ | Plain, unsigned-payload, and **SigV4 streaming-chunked** bodies; conditional writes (If-Match / If-None-Match); inline metadata; Content-MD5 verification. **Flexible checksums** (CRC32, CRC32C, **CRC-64/NVME**, SHA-1, SHA-256): the default-on checksum every modern SDK sends is computed, verified (`BadDigest` on header mismatch), stored, and **echoed** on the response with `x-amz-checksum-type: FULL_OBJECT`. **Server-side encryption**: `x-amz-server-side-encryption: AES256` or `aws:kms` (`-aws-kms-key-id`, `-bucket-key-enabled`) mints a per-object DEK; a bucket default or `CAIRN_ENCRYPT_AT_REST` encrypts otherwise. |
| GetObject / HeadObject | ✅ | Byte ranges (206), conditionals (304/412), version selection. **Echoes the stored `x-amz-checksum-<algo>`** on a whole-object read when the request sends `x-amz-checksum-mode: ENABLED` (never on a Range read). |
| DeleteObject | ✅ | Delete marker in a versioned bucket; permanent with a version id. |
| DeleteObjects (bulk) | ✅ | Quiet mode; up to the request cap. |
| CopyObject | ✅ | COPY/REPLACE metadata directive; same-key metadata change; versioned source. |
| CreateMultipartUpload / UploadPart / UploadPartCopy / ListParts / CompleteMultipartUpload / AbortMultipartUpload | ✅ | Correct multipart ETag (`md5(concat(part-md5s))-N`); part validation; double-completion guarded; `UploadPartCopy` stages a ranged copy of a source object (`x-amz-copy-source-range`). SSE intent (`AES256`/`aws:kms`) is accepted at `CreateMultipartUpload`; each part is stored encrypted and re-encrypted under the object DEK at assembly (nothing plaintext on disk). |
| GetObjectAttributes | ✅ | Returns ETag, object size, storage class, checksum (`FULL_OBJECT`, or `COMPOSITE` for a multipart object) with its `ChecksumType`, and the parts list while retrievable. |
| GetObjectTagging / PutObjectTagging / DeleteObjectTagging | ✅ | Stored as queryable rows; usable by lifecycle/policy. |
| Presigned GET / PUT | ✅ | SigV4 query form. |
| GetObjectAcl / PutObjectAcl, GetBucketAcl / PutBucketAcl | ◑ | ACLs are off by default under the recommended BucketOwnerEnforced mode; the policy engine is primary. |
| Object Lock (PutObjectLockConfiguration / Get; PutObjectRetention / Get; PutObjectLegalHold / Get) | ✅ | WORM retention (`GOVERNANCE`/`COMPLIANCE`) + legal hold. Enable at bucket creation (`x-amz-bucket-object-lock-enabled`, which forces versioning Enabled); optional bucket **default retention** stamped on new versions. Enforced at every permanent-version-delete path (single/bulk delete, lifecycle): `COMPLIANCE` is immutable until expiry; `GOVERNANCE` is bypassable with `s3:BypassGovernanceRetention` + `x-amz-bypass-governance-retention: true`; legal hold blocks regardless. |
| Temporary security credentials (STS) | ◑ | **Two minting paths, SDK-compatible consumption.** (1) The **AWS-STS wire surface** — `AssumeRole` + `GetSessionToken` served on the S3 data-plane port as a form `POST /` returning AWS-STS XML, so the AWS SDK default credential-provider chain and Terraform's `assume_role{}` obtain creds with zero config (opt out with `CAIRN_STS_ENABLED=false`). `GetSessionToken` inherits the caller's **effective** access (identity policy + `Allow s3:*` on owned buckets; an admin gets full-S3); `AssumeRole` requires `RoleArn`/`RoleSessionName` (audit-only — no IAM roles) and honours an inline `Policy` **only** for an administrator (a non-admin supplying one is denied). (2) The **management API** (`POST /api/v1/credentials/temporary`, scoped inline policy). Both mint the same `CAIRNTMP…` credential (15m–12h) consumed with any S3 SDK that sends `X-Amz-Security-Token` (header or presigned query). Least-privilege: a session never inherits the parent's owner/admin bypass, and STS never mints broader than the caller. |
| Event notifications (webhooks) | ◑ | **Webhook-native**, not SNS/SQS/Lambda. Per-bucket endpoints (URL + event selectors + prefix/suffix filter + optional HMAC secret) are configured via the **management API** (`PUT /api/v1/buckets/{name}/notifications`); object events (`s3:ObjectCreated:*`, `s3:ObjectRemoved:*`) enqueue a durable delivery row that a background worker POSTs as S3-event-record JSON with retry/backoff and an optional `X-Cairn-Signature` (HMAC-SHA256). The S3 `?notification` subresource (SNS/SQS ARNs) stays `NotImplemented`. |
| Server-side encryption (GetBucketEncryption / PutBucketEncryption / DeleteBucketEncryption; SSE headers on write/read) | ✅ | `?encryption` bucket subresource (`ServerSideEncryptionConfiguration` XML, validated). `x-amz-server-side-encryption: AES256` (SSE-S3) or `aws:kms` (SSE-KMS: `-aws-kms-key-id`, `-bucket-key-enabled`) on PutObject / CreateMultipartUpload / CopyObject, echoed on PUT/GET/HEAD/Copy/Complete. Multipart parts are encrypted at rest (per-part DEK). `aws:kms` is a **validated label over the same master-key ring — not distinct key material / not tenant isolation** (see `s3-api.md` §13.5, `security-errors.md` §27.6/27.8). Optional transparent at-rest encryption via `CAIRN_ENCRYPT_AT_REST` (advertises nothing) and a mandatory-SSE `required` flag. |
| website / accelerate / analytics / inventory / requester-pays | ✖ | Out of scope; answered as NotImplemented. |

**Checksum scope.** Single-object checksums are full-object; multipart objects carry a **COMPOSITE**
checksum-of-part-checksums composed at CompleteMultipartUpload from the per-part digests, echoed with
`x-amz-checksum-type: COMPOSITE` on Complete and returned by `GetObjectAttributes` (CRC-64/NVME, which
has no composite form, falls back to a whole-object recompute). One related feature remains out of
scope for now: **server-side verification of the *trailing* checksum value** in `aws-chunked`
streaming uploads — the checksum is still computed and stored server-side from the selected algorithm,
and the non-streaming header-checksum path is verified and `BadDigest`-rejected on mismatch (see
`s3-api.md` §21.7).

**Management API** (`/api/v1`, admin-gated JSON) and the **embedded React console** (its own listener, port 7374) provide
control-plane operations (overview, bucket/user/activity management) consumed by both the web console
and the CLI.
