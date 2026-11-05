# Support and deployment scope

**Status: 1.0 release candidate (2026-11-05).** This page states what is supported today, what is
not, and the operational conditions the durability claims assume. It is the basis for the 1.0
support statement; it does not grant an SLA and is not a third-party certification. The measured
evidence and the remaining pre-1.0 gates are in
[`stability-2026-11-05.md`](stability-2026-11-05.md).

## Supported deployment

- **One node.** Cairn is a single-node object store: one process, one embedded SQLite database, one
  data directory. There is no clustering, consensus, leader election, or shared-nothing sharding.
  Cross-host redundancy is asynchronous bucket replication or backup/restore.
- **Linux 5.6 or newer**, x86_64 or aarch64. The data namespace requires `openat2` with
  `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS` and `RESOLVE_NO_XDEV`.
- **Local filesystem.** XFS is recommended; btrfs also supports reflink completion. ext4 and other
  filesystems are supported through the streaming copy fallback, which is slower for plaintext
  multipart completion. Do **not** put the data directory on a Docker overlayfs writable layer —
  mount it as a volume or bind mount on XFS/btrfs.
- **S3 surface** in [`s3-api-matrix.md`](s3-api-matrix.md): objects, versioning, multipart,
  checksums, range/conditional reads, tagging, CORS, lifecycle expiry and abort, Object Lock,
  presigned URLs, STS session credentials, and the documented management API. Unsupported S3
  features remain unsupported; the matrix is authoritative.
- **Durability:** `CAIRN_META_SYNCHRONOUS=full` (the default) commits the metadata transaction
  before the acknowledgement. The ACK also follows the data-file fsync, rename and destination
  directory barrier. A pre-upgrade snapshot and a tested restore are required operational
  practices, not optional extras.
- **Replication:** one-way asynchronous bucket replication, at-least-once with idempotent version
  application, requiring versioning on the source. The RPO is the replication lag at the moment the
  source is lost; there is no automatic promote-to-primary. A replica is a read-only copy for
  redundancy and geo-distribution.
- **Backup/restore** per [`backup-restore.md`](backup-restore.md), with the data directory and
  SQLite database kept together on the same filesystem.
- **Upgrade:** forward upgrade is supported from the last published release (verified
  v2026.09.13 → schema v42/sidecar v3). **Downgrade is unsupported** after schema v42; the old
  binary refuses the upgraded database. Restore the pre-upgrade snapshot to roll back.
- **TLS and access control:** native TLS on the listeners, SigV4/Bearer authentication, the
  policy/ACL/BPA/ownership engine, and the management console. Put a reverse proxy or network
  policy in front of a public deployment (see the limits below).

## Not supported or deliberately limited

- **High availability / automatic failover.** Replication is asynchronous and manual to promote; a
  single node is a single failure domain.
- **Rate limiting on the authentication path.** Cairn does not implement it. Put a reverse proxy,
  firewall, or upstream rate limiter in front of a public endpoint. Adding it is a product decision,
  not a hidden default.
- **OpenRC units.** The shipped service units and hardening target systemd.
- **Reflink on every filesystem.** `FICLONERANGE` is used only for known-plaintext, uncompressed,
  unencrypted multipart completions on the same XFS/btrfs filesystem. Every other shape and
  filesystem uses the streaming copy path. The copy path is correct but does not get the reflink
  completion speedup.
- **The unversioned DELETE ratio.** RustFS 1.0.0 strict does not fsync its unversioned delete path,
  so the unversioned comparison is as-configured throughput, not equal durability. Quote the
  versioned delete-marker arm instead.
- **Replication throughput on a remote target.** The controlled benchmark runs both nodes on one
  host, which is a worst-case contention measurement. A remote target changes the network and disk
  contention; measure it for the deployment. The worker count is tunable
  (`CAIRN_REPLICATION_WORKER_CONCURRENCY`): 4 is the balance, 2 favours source foreground, 8 favours
  backlog drain.
- **Long-duration evidence.** The 2026-11-05 record includes a 30-minute soak with a 10-minute idle
  plateau and a running 24-hour soak. A 48-hour soak and power-loss testing remain open gates for
  the final 1.0 declaration.
- **Third-party audit.** The security/architecture review is internal. It is not a certification.

## Operational checklist

1. Back up the data directory and database; verify a restore before upgrading.
2. Keep the master key/ring and the pre-upgrade snapshot; without them, sealed secrets and the old
   format are unrecoverable.
3. Use XFS or btrfs for the data directory if reflink completion matters; verify with
   `stat -f -c %T <data dir>`.
4. Keep time synchronized; SigV4 rejects skewed clocks.
5. Monitor replication lag and `cairn_replication_unreplicated`, scrub results, disk space, and the
   SQLite/WAL footprint.
6. For a public deployment, terminate TLS at a trusted proxy and apply rate limiting and network
   access control there.
7. Test the upgrade and rollback path against a copy of production data, not only a fresh store.

## Reporting and security

Security reports follow [`../SECURITY.md`](../SECURITY.md); do not open a public issue for an
unpatched vulnerability. Operational procedures are in [`operations.md`](operations.md) and
[`disaster-recovery.md`](disaster-recovery.md).
