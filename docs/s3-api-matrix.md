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
| PutObject | ✅ | Plain, unsigned-payload, and **SigV4 streaming-chunked** bodies; conditional writes (If-Match / If-None-Match); inline metadata; requested checksums; Content-MD5 verification. |
| GetObject / HeadObject | ✅ | Byte ranges (206), conditionals (304/412), version selection. |
| DeleteObject | ✅ | Delete marker in a versioned bucket; permanent with a version id. |
| DeleteObjects (bulk) | ✅ | Quiet mode; up to the request cap. |
| CopyObject | ✅ | COPY/REPLACE metadata directive; same-key metadata change; versioned source. |
| CreateMultipartUpload / UploadPart / UploadPartCopy / ListParts / CompleteMultipartUpload / AbortMultipartUpload | ✅ | Correct multipart ETag (`md5(concat(part-md5s))-N`); part validation; double-completion guarded; `UploadPartCopy` stages a ranged copy of a source object (`x-amz-copy-source-range`). |
| GetObjectAttributes | ✅ | Returns ETag, object size, storage class, checksum, and the parts list. |
| GetObjectTagging / PutObjectTagging / DeleteObjectTagging | ✅ | Stored as queryable rows; usable by lifecycle/policy. |
| Presigned GET / PUT | ✅ | SigV4 query form. |
| GetObjectAcl / PutObjectAcl, GetBucketAcl / PutBucketAcl | ◑ | ACLs are off by default under the recommended BucketOwnerEnforced mode; the policy engine is primary. |
| Object Lock (PutObjectLockConfiguration / Get; PutObjectRetention / Get; PutObjectLegalHold / Get) | ✅ | WORM retention (`GOVERNANCE`/`COMPLIANCE`) + legal hold. Enable at bucket creation (`x-amz-bucket-object-lock-enabled`, which forces versioning Enabled); optional bucket **default retention** stamped on new versions. Enforced at every permanent-version-delete path (single/bulk delete, lifecycle): `COMPLIANCE` is immutable until expiry; `GOVERNANCE` is bypassable with `s3:BypassGovernanceRetention` + `x-amz-bypass-governance-retention: true`; legal hold blocks regardless. |
| SSE config, website / accelerate / analytics / inventory / requester-pays | ✖ | Out of scope; answered as NotImplemented. |

**Management API** (`/api/v1`, admin-gated JSON) and the **embedded React console** (its own listener, port 7374) provide
control-plane operations (overview, bucket/user/activity management) consumed by both the web UI
and the CLI.
