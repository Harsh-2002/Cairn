// Thin client of the Cairn management API (ARCH §22, §23).
//
// Every request is admin-gated via a Bearer token of the form
// `cairn_<id>.<secret>`. The token is held in memory and mirrored to
// localStorage so a reload keeps the session. The UI carries no privileged
// logic of its own: it is a pure presentation layer over the control plane.

import type {
  ActivityResp,
  BucketConfigResp,
  BucketDetailResp,
  BucketListResp,
  CreateReplicationTargetReq,
  CreateShareReq,
  CreateShareResp,
  CreateUserResp,
  DeletePrefixResp,
  FailedReplicationResp,
  ListObjectsResp,
  OverviewBucketsResp,
  OverviewResp,
  MetricsRange,
  PresignReq,
  PresignResp,
  ReplicationResyncResp,
  ReplicationRetryResp,
  ReplicationStatusResp,
  ReplicationTargetListResp,
  RequestMetricsResp,
  RotateCredentialsResp,
  ShareListResp,
  SystemResp,
  TagObjectsResp,
  TagSummaryResp,
  UserDetailResp,
  UserListResp,
  UserPolicyResp,
} from "./types";

const BASE = "/api/v1";
const TOKEN_KEY = "cairn.token";

let token: string | null = null;

export function loadToken(): string {
  if (token === null) {
    try {
      token = localStorage.getItem(TOKEN_KEY) ?? "";
    } catch {
      token = "";
    }
  }
  return token;
}

export function setToken(value: string): void {
  token = value || "";
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* storage may be unavailable; in-memory token still works */
  }
}

export function clearToken(): void {
  setToken("");
}

export function hasToken(): boolean {
  return !!loadToken();
}

// A session-expiry hook: the auth provider registers a callback that clears
// the session and bounces to /login whenever any request comes back 401.
let unauthorizedHandler: (() => void) | null = null;

export function onUnauthorized(handler: (() => void) | null): void {
  unauthorizedHandler = handler;
}

/** An error carrying the HTTP status so callers (e.g. login) can react to 401/403. */
export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export function errorMessage(e: unknown, fallback: string): string {
  if (e instanceof Error && e.message) return e.message;
  return fallback;
}

function authHeaders(extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = { Accept: "application/json", ...extra };
  const tok = loadToken();
  if (tok) headers.Authorization = `Bearer ${tok}`;
  return headers;
}

async function handleResponse<T>(res: Response): Promise<T> {
  if (res.status === 401) unauthorizedHandler?.();

  if (res.status === 204) return null as T;

  let payload: unknown = null;
  const text = await res.text();
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { raw: text };
    }
  }

  if (!res.ok) {
    const p = payload as { error?: string; message?: string } | null;
    const msg = p?.error || p?.message || `request failed (${res.status})`;
    throw new ApiError(msg, res.status);
  }
  return payload as T;
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const init: RequestInit = { method, headers: authHeaders() };
  if (body !== undefined) {
    (init.headers as Record<string, string>)["Content-Type"] =
      "application/json";
    init.body = JSON.stringify(body);
  }

  let res: Response;
  try {
    res = await fetch(BASE + path, init);
  } catch (e) {
    throw new ApiError(`network error: ${(e as Error).message ?? e}`, 0);
  }
  return handleResponse<T>(res);
}

// Like `request`, but the body is sent verbatim as a string rather than being
// JSON-encoded. Used by the policy endpoints, whose body is a raw policy JSON
// document the server validates and stores as-is.
async function requestRaw<T>(
  method: string,
  path: string,
  rawBody: string,
): Promise<T> {
  let res: Response;
  try {
    res = await fetch(BASE + path, {
      method,
      headers: authHeaders({ "Content-Type": "application/json" }),
      body: rawBody,
    });
  } catch (e) {
    throw new ApiError(`network error: ${(e as Error).message ?? e}`, 0);
  }
  return handleResponse<T>(res);
}

const enc = encodeURIComponent;

export const api = {
  health: () => request<{ status: string; ready: boolean }>("GET", "/health"),
  overview: () => request<OverviewResp>("GET", "/overview"),
  overviewBuckets: () =>
    request<OverviewBucketsResp>("GET", "/overview/buckets"),
  system: () => request<SystemResp>("GET", "/system"),

  listBuckets: () => request<BucketListResp>("GET", "/buckets"),
  createBucket: (name: string) =>
    request<{ name: string }>("POST", "/buckets", { name }),
  getBucket: (name: string) =>
    request<BucketDetailResp>("GET", `/buckets/${enc(name)}`),
  deleteBucket: (name: string) =>
    request<null>("DELETE", `/buckets/${enc(name)}`),
  listObjects: (
    name: string,
    {
      prefix = "",
      delimiter = "",
      limit = 100,
      cursor = "",
    }: {
      prefix?: string;
      delimiter?: string;
      limit?: number;
      cursor?: string;
    } = {},
  ) => {
    const q = new URLSearchParams();
    if (prefix) q.set("prefix", prefix);
    // A delimiter folds keys into common prefixes ("folders"), like S3 listing.
    if (delimiter) q.set("delimiter", delimiter);
    if (limit) q.set("limit", String(limit));
    // Continuation cursor returned as `next` by a prior page; omitted on the first page.
    if (cursor) q.set("cursor", cursor);
    const qs = q.toString();
    return request<ListObjectsResp>(
      "GET",
      `/buckets/${enc(name)}/objects${qs ? `?${qs}` : ""}`,
    );
  },

  // Persistent object shares (ARCH §15.8): revocable, optionally forever. `url` is a path
  // (/p/{token}) the caller turns into an absolute link.
  createShare: (name: string, body: CreateShareReq) =>
    request<CreateShareResp>(
      "POST",
      `/buckets/${enc(name)}/objects/share`,
      body,
    ),
  listShares: (name: string, key?: string) => {
    const qs = key ? `?key=${enc(key)}` : "";
    return request<ShareListResp>(
      "GET",
      `/buckets/${enc(name)}/objects/shares${qs}`,
    );
  },
  revokeShare: (name: string, token: string) =>
    request<null>(
      "DELETE",
      `/buckets/${enc(name)}/objects/shares/${enc(token)}`,
    ),
  // Mint an interoperable S3 presigned URL (GET download / PUT upload), returned absolute.
  presignShare: (name: string, body: PresignReq) =>
    request<PresignResp>(
      "POST",
      `/buckets/${enc(name)}/objects/presign`,
      body,
    ),

  // Bucket configuration (ARCH §22.2).
  getBucketConfig: (name: string) =>
    request<BucketConfigResp>("GET", `/buckets/${enc(name)}/config`),
  setVersioning: (name: string, status: string) =>
    request<null>("PUT", `/buckets/${enc(name)}/versioning`, { status }),
  setQuota: (name: string, quota_bytes: number | null) =>
    request<null>("PUT", `/buckets/${enc(name)}/quota`, { quota_bytes }),
  // Set/disable the bucket compression policy. algorithm: "zstd" | "lz4" | "none".
  setCompression: (name: string, algorithm: string, block_size = 65536) =>
    request<null>("PUT", `/buckets/${enc(name)}/compression`, {
      algorithm,
      block_size,
    }),
  // Default server-side encryption for new uploads. algorithm: "AES256" | "none".
  setEncryption: (name: string, algorithm: string) =>
    request<null>("PUT", `/buckets/${enc(name)}/encryption`, { algorithm }),
  // The policy body is a raw policy JSON document sent verbatim.
  setPolicy: (name: string, rawBody: string) =>
    requestRaw<null>("PUT", `/buckets/${enc(name)}/policy`, rawBody),
  deletePolicy: (name: string) =>
    request<null>("DELETE", `/buckets/${enc(name)}/policy`),

  listUsers: () => request<UserListResp>("GET", "/users"),
  // Created users are S3-API-only: the response carries their S3 (SigV4) access key + secret,
  // shown exactly once. `role` is always "member" from the console (the root admin is the sole
  // admin).
  createUser: (display_name: string, role = "member") =>
    request<CreateUserResp>("POST", "/users", { display_name, role }),
  getUser: (id: string) => request<UserDetailResp>("GET", `/users/${enc(id)}`),
  patchUser: (id: string, fields: { is_active?: boolean }) =>
    request<UserDetailResp>("PATCH", `/users/${enc(id)}`, fields),
  rotateCredentials: (id: string) =>
    request<RotateCredentialsResp>(
      "POST",
      `/users/${enc(id)}/rotate-credentials`,
    ),
  // Per-user byte quota (ARCH §27.5); null clears the limit.
  setUserQuota: (id: string, quota_bytes: number | null) =>
    request<null>("PUT", `/users/${enc(id)}/quota`, { quota_bytes }),
  // Identity (per-user) policy. The body is a raw policy JSON document sent verbatim.
  getUserPolicy: (id: string) =>
    request<UserPolicyResp>("GET", `/users/${enc(id)}/policy`),
  setUserPolicy: (id: string, rawBody: string) =>
    requestRaw<null>("PUT", `/users/${enc(id)}/policy`, rawBody),
  deleteUserPolicy: (id: string) =>
    request<null>("DELETE", `/users/${enc(id)}/policy`),

  // Per-bucket replication management (ARCH §20). Remote targets hold the
  // destination endpoint + credentials (secret sealed server-side) and mint the
  // ARN that replication rules reference.
  listReplicationTargets: (name: string) =>
    request<ReplicationTargetListResp>(
      "GET",
      `/buckets/${enc(name)}/replication/targets`,
    ),
  addReplicationTarget: (name: string, body: CreateReplicationTargetReq) =>
    request<{ arn: string }>(
      "POST",
      `/buckets/${enc(name)}/replication/targets`,
      body,
    ),
  deleteReplicationTarget: (name: string, arn: string) =>
    request<null>(
      "DELETE",
      `/buckets/${enc(name)}/replication/targets/${enc(arn)}`,
    ),
  // Requeue this bucket's terminally-failed replication entries.
  retryReplication: (name: string) =>
    request<ReplicationRetryResp>(
      "POST",
      `/buckets/${enc(name)}/replication/retry`,
    ),
  // Backfill: enqueue current versions for replication (needs an enabled rule
  // with existing-object replication).
  resyncReplication: (name: string) =>
    request<ReplicationResyncResp>(
      "POST",
      `/buckets/${enc(name)}/replication/resync`,
    ),
  replicationStatus: (name: string) =>
    request<ReplicationStatusResp>(
      "GET",
      `/buckets/${enc(name)}/replication/status`,
    ),

  failedReplication: (limit = 100) =>
    request<FailedReplicationResp>("GET", `/replication/failed?limit=${limit}`),

  activity: (limit = 50) =>
    request<ActivityResp>("GET", `/activity?limit=${limit}`),

  // Usage analytics: aggregated request volume over a rolling window (the
  // Metrics view).
  metrics: (range: MetricsRange) =>
    request<RequestMetricsResp>("GET", `/metrics/requests?range=${range}`),
  // Bulk-delete every object under a prefix. Consumed by the bucket browser.
  deletePrefix: (bucket: string, prefix: string) =>
    request<DeletePrefixResp>(
      "DELETE",
      `/buckets/${enc(bucket)}/objects?prefix=${encodeURIComponent(prefix)}`,
    ),

  // Object tagging (the Tags view). The summary lists every distinct key=value
  // tag in use; the drill-down lists the objects carrying a chosen tag. Both
  // optionally scope to a single bucket. `enc` is encodeURIComponent, so it is
  // the correct escaper for these query-string values too.
  listTags: (bucket?: string) =>
    request<TagSummaryResp>(
      "GET",
      `/tags${bucket ? `?bucket=${enc(bucket)}` : ""}`,
    ),
  listTagObjects: (key: string, value: string, bucket?: string) =>
    request<TagObjectsResp>(
      "GET",
      `/tags/objects?key=${enc(key)}&value=${enc(value)}${
        bucket ? `&bucket=${enc(bucket)}` : ""
      }`,
    ),
};
