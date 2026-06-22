// TypeScript mirrors of the management-API DTOs (crates/cairn-control/src/wire.rs).

import type { PolicyDoc } from "./policy";

export interface OverviewResp {
  buckets: number;
  objects: number;
  versions: number;
  logical_bytes: number;
  physical_bytes: number;
  compression_ratio: number;
}

export interface SystemResp {
  version: string;
  uptime_secs: number;
  s3_addr: string;
  ui_addr: string;
  tls: boolean;
  data_dir: string;
  disk_total_bytes: number | null;
  disk_free_bytes: number | null;
}

export interface BucketUsageEntry {
  name: string;
  objects: number;
  logical_bytes: number;
  physical_bytes: number;
}

export interface OverviewBucketsResp {
  buckets: BucketUsageEntry[];
}

export interface BucketListEntry {
  name: string;
  owner_id: string;
  created_at_ms: number;
  versioning: string;
}

export interface BucketListResp {
  buckets: BucketListEntry[];
}

export interface BucketDetailResp {
  name: string;
  versioning: string;
  ownership_mode: string;
  region: string;
  object_count: number;
  logical_bytes: number;
  compression: unknown;
}

export interface ObjectEntry {
  key: string;
  size: number;
  etag: string;
  last_modified_ms: number;
}

export interface ListObjectsResp {
  objects: ObjectEntry[];
  /** Key groups folded at the requested delimiter (the "folders"). */
  common_prefixes: string[];
  next: string | null;
}

export type ShareDisposition = "inline" | "attachment";
export type ShareStatus = "active" | "expired" | "revoked";

/** A persistent object-share (ARCH 15.8). */
export interface ShareRecord {
  token: string;
  bucket: string;
  key: string;
  version_id: string | null;
  expires_at_ms: number | null; // null = forever
  created_at_ms: number;
  created_by: string;
  disposition: ShareDisposition;
  filename: string | null;
  status: ShareStatus; // server-derived
}

export interface CreateShareReq {
  key: string;
  expires_in_secs?: number | null; // null/absent = forever
  disposition?: ShareDisposition;
  filename?: string | null;
  version_id?: string | null;
}

export interface CreateShareResp {
  token: string;
  url: string; // path "/p/{token}"
  expires_at_ms: number | null;
}

export interface ShareListResp {
  shares: ShareRecord[];
}

/** Presigned-URL minting request (interoperable S3 link). */
export interface PresignReq {
  key: string;
  method?: "GET" | "PUT";
  expires_in_secs: number; // 1..=604800
  version_id?: string | null;
  response_content_disposition?: string | null;
  response_content_type?: string | null;
  content_type?: string | null; // PUT: pin the content type
}

export interface PresignResp {
  url: string; // absolute
  expires_at_ms: number;
  absolute: true;
}

export interface BucketConfigResp {
  versioning: string;
  ownership_mode: string;
  quota_bytes: number | null;
  policy: unknown | null;
  cors: unknown | null;
  tagging: unknown | null;
  lifecycle: unknown | null;
  acl: unknown | null;
  public_access_block: unknown | null;
  /** Default SSE document ({"algorithm":"AES256"}) or null when off. */
  encryption: { algorithm?: string } | null;
}

// --- Webhook event notifications (ARCH 20.6) ---

/** One webhook endpoint as the management API returns it: the secret is reduced to a flag. */
export interface WebhookEndpointView {
  id: string;
  url: string;
  events: string[];
  prefix: string | null;
  suffix: string | null;
  has_secret: boolean;
}

export interface NotificationsResp {
  endpoints: WebhookEndpointView[];
}

/** One endpoint in the PUT body (the secret, when set, is written but never read back). */
export interface WebhookEndpointInput {
  id: string;
  url: string;
  events: string[];
  prefix?: string | null;
  suffix?: string | null;
  secret?: string | null;
}

export interface NotificationConfigInput {
  endpoints: WebhookEndpointInput[];
}

// --- STS temporary session credentials (ARCH 14.6) ---

export interface MintSessionReq {
  duration_secs: number;
  /** A standard identity-policy JSON object (the session's entire effective permission set). */
  policy: unknown;
}

export interface MintSessionResp {
  access_key_id: string;
  secret_access_key: string;
  session_token: string;
  expiration_epoch_secs: number;
}

export interface UserSummary {
  id: string;
  display_name: string;
  access_key_id: string;
  role: string;
  is_active: boolean;
}

export interface UserListResp {
  users: UserSummary[];
}

/** One-time credentials returned by user creation; never shown again. */
export interface CreateUserResp {
  id: string;
  bearer_access_key_id: string;
  bearer_secret: string;
  s3_access_key_id: string;
  s3_secret_key: string;
}

export interface UserDetailResp {
  id: string;
  display_name: string;
  access_key_id: string;
  sigv4_access_key_id: string | null;
  role: string;
  is_active: boolean;
  quota_bytes: number | null;
  policy: PolicyDoc | null;
}

export interface RotateCredentialsResp {
  bearer_access_key_id: string;
  bearer_secret: string;
}

export interface UserPolicyResp {
  policy: PolicyDoc | null;
}

export interface ActivityEntry {
  action: string;
  bucket: string | null;
  key: string | null;
  actor: string | null;
  at_ms: number;
}

export interface ActivityResp {
  entries: ActivityEntry[];
}

export interface FailedReplicationEntry {
  bucket: string;
  key: string;
  version_id: string;
  error: string;
  attempts: number;
  next_attempt_at_ms: number;
}

export interface FailedReplicationResp {
  entries: FailedReplicationEntry[];
}

export interface ReplicationRule {
  dest_bucket: string;
  prefix: string;
}

/** A per-bucket remote replication target (the secret is never returned). */
export interface ReplicationTarget {
  arn: string;
  endpoint: string;
  region: string;
  dest_bucket: string;
  access_key_id: string;
}

export interface ReplicationTargetListResp {
  targets: ReplicationTarget[];
}

/** Body for registering a remote target; the secret is sealed server-side. */
export interface CreateReplicationTargetReq {
  endpoint: string;
  region: string;
  dest_bucket: string;
  access_key: string;
  secret: string;
}

export interface ReplicationStatusError {
  key: string;
  version_id: string;
  error: string;
}

export interface ReplicationStatusResp {
  bucket: string;
  pending: number;
  failed: number;
  recent_errors: ReplicationStatusError[];
}

export interface ReplicationRetryResp {
  requeued: boolean;
  failed_observed: number;
}

export interface ReplicationResyncResp {
  started: boolean;
}

// Usage-analytics metrics (the Metrics view). Mirrors the management API's
// /metrics/requests aggregation and the bulk prefix-delete response.

/** One timeline bucket: a sample window of request activity. */
export interface MetricPoint {
  ts_ms: number;
  count: number;
  errors: number;
  bytes_in: number;
  bytes_out: number;
  latency_avg_ms: number;
}

/** Per-S3-operation roll-up over the whole window. */
export interface MetricOp {
  operation: string;
  count: number;
  bytes: number;
  latency_avg_ms: number;
}

/** Per-bucket roll-up over the whole window. */
export interface MetricBucket {
  bucket: string;
  count: number;
  bytes: number;
}

/** Response-status-class roll-up ("2xx", "3xx", "4xx", "5xx"). */
export interface MetricStatus {
  status_class: string;
  count: number;
}

export interface RequestMetricsResp {
  window_secs: number;
  total: number;
  total_errors: number;
  total_bytes_in: number;
  total_bytes_out: number;
  latency_avg_ms: number;
  latency_p95_ms: number;
  peak_window_count: number;
  active_buckets: number;
  timeline: MetricPoint[];
  by_operation: MetricOp[];
  top_buckets: MetricBucket[];
  /** Top buckets ranked by bytes transferred — distinct from top_buckets (by count). */
  top_buckets_by_bytes: MetricBucket[];
  by_status: MetricStatus[];
}

export interface DeletePrefixError {
  key: string;
  message: string;
}

export interface DeletePrefixResp {
  deleted: number;
  errors: DeletePrefixError[];
  more: boolean;
}

export type MetricsRange = "1d" | "1w" | "2w" | "1m";

// Object tagging, surfaced node-wide in the Tags view. Mirrors the management
// API's /tags summary + /tags/objects drill-down.

/** One distinct tag (key=value) and how many objects carry it. */
export interface TagSummaryItem {
  tag_key: string;
  tag_value: string;
  object_count: number;
}

export interface TagSummaryResp {
  tags: TagSummaryItem[];
}

/** An object carrying a selected tag. */
export interface TagObjectItem {
  bucket: string;
  key: string;
  version_id: string;
  size: number;
  last_modified_ms: number;
}

export interface TagObjectsResp {
  objects: TagObjectItem[];
}
