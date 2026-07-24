// Thin client of the Cairn management API (ARCH 22, 23).
//
// Authentication is an httpOnly session cookie set by the server at sign-in
// (`POST /session`) and cleared at sign-out (`DELETE /session`). The browser
// attaches it automatically to these same-origin requests, so the credential is
// never held in JS-readable storage (no localStorage token to steal via XSS).
// The web console carries no privileged logic of its own: it is a pure presentation layer
// over the control plane.

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
  CreateImportReq,
  CreateImportResp,
  ImportJobDetail,
  ImportListResp,
  ProbeSourceReq,
  ProbeSourceResp,
  ListObjectsResp,
  ListSessionsResp,
  MintSessionReq,
  MintSessionResp,
  NotificationConfigInput,
  NotificationsResp,
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

/** The non-sensitive identity the server reports for the current session. */
export interface SessionInfo {
  access_key_id: string;
  display_name: string;
  role: "administrator" | "member";
}

// A session-expiry hook: the auth provider registers a callback that clears
// the session and bounces to /login whenever any request comes back 401.
let unauthorizedHandler: (() => void) | null = null;

export function onUnauthorized(handler: (() => void) | null): void {
  unauthorizedHandler = handler;
}

/** An error carrying the HTTP status (and an error code, when the server gives one) so callers can
 * react to 401/403 and the humanizer can map a precise cause. */
export class ApiError extends Error {
  status: number;
  /** A stable machine code when the server provides one (an S3 `<Code>` or a control error code). */
  code?: string;

  constructor(message: string, status: number, code?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

/**
 * Turn any thrown error into a sentence a non-expert operator can act on. `fallback` is the
 * caller's plain-language description of what was being attempted (e.g. "Couldn't save the rule.");
 * it's used as the lead when the server's own message is generic or missing.
 *
 * Order: a connection failure → a recognised cause (mapped from the server's code/message) → a
 * status-based hint built on the fallback → the server's own message if it already reads cleanly.
 */
export function errorMessage(e: unknown, fallback: string): string {
  if (e instanceof ApiError) return humanizeApiError(e, fallback);
  if (e instanceof Error && e.message) return sentence(e.message);
  return fallback;
}

function humanizeApiError(e: ApiError, fallback: string): string {
  // Couldn't reach the server at all (status 0 is set on a fetch/network failure).
  if (e.status === 0) {
    return "Couldn't reach the server. Check that it's running and that your connection is up, then try again.";
  }
  const raw = (e.message ?? "").trim();
  const known = knownCause(e.code, raw);
  if (known) return known;
  // A generic "X failed (404)" / "request failed (500)" / empty message carries no real cause, so
  // lead with the caller's description and add what the status implies.
  const generic = !raw || /failed \(\d+\)\s*$/i.test(raw) || /^request failed/i.test(raw);
  if (generic) return statusHint(e.status, fallback);
  // The server gave a real, human message — surface it as a clean sentence.
  return sentence(raw);
}

/** Map a known server cause (by S3/control code, else by message text) to plain, actionable copy. */
function knownCause(code: string | undefined, raw: string): string | null {
  const has = (re: RegExp) => re.test(raw);
  const is = (c: string) => code === c;

  // --- Replication / versioning (the configuration paths) ---
  if (has(/existing[- ]object replication/i) || is("InvalidExistingObjectReplication")) {
    return 'This bucket has no rule set to copy objects that already exist. Open the replication rule, turn on "Replicate existing objects", and save — then run "Resync existing".';
  }
  if (has(/versioning/i) && has(/enabl|requir|must|need/i)) {
    return "Replication needs versioning turned on for this bucket. Enable versioning first, then add the rule.";
  }
  // --- Bucket lifecycle conflicts ---
  if (is("BucketNotEmpty") || has(/bucket .*not empty|not empty/i)) {
    return "This bucket still has objects in it. Empty it first, then delete the bucket.";
  }
  if (is("BucketAlreadyExists") || has(/already exists/i)) {
    return "A bucket with that name already exists on this server. Bucket names are global — pick a different name.";
  }
  if (is("BucketAlreadyOwnedByYou") || has(/already owned by you/i)) {
    return "You already have a bucket with that name.";
  }
  if (is("InvalidBucketName") || has(/invalid bucket name|bucket name/i)) {
    return "That bucket name isn't allowed. Use 3–63 characters: lowercase letters, numbers, dots and hyphens, starting and ending with a letter or number.";
  }
  // --- Not found (often a stale view) ---
  if (is("NoSuchBucket")) {
    return "That bucket no longer exists — it may have been deleted. Refresh the page.";
  }
  if (is("NoSuchKey") || is("NoSuchVersion")) {
    return "That object no longer exists — it may have been deleted. Refresh the list.";
  }
  if (is("NoSuchUpload")) {
    return "This upload has expired or was cancelled. Start the upload again.";
  }
  // --- Quotas / size ---
  if (is("QuotaExceeded") || has(/quota/i)) {
    return "This would put the bucket over its storage quota. Raise the quota or free up space first.";
  }
  if (is("EntityTooLarge") || has(/entity too large|too large/i)) {
    return "That's larger than the server allows.";
  }
  if (is("InsufficientStorage") || has(/insufficient storage|no space/i)) {
    return "The server is out of disk space. Free up space and try again.";
  }
  // --- Validation: policy / JSON / XML ---
  if (
    is("MalformedPolicy") ||
    is("MalformedXML") ||
    has(/malformed|invalid (json|policy|xml|configuration)|parse|could not parse/i)
  ) {
    return "That couldn't be parsed. Check the syntax — a missing comma, quote or bracket is the usual cause — and try again.";
  }
  // --- Auth / permission / lock ---
  if (is("AccessDenied") || is("SignatureDoesNotMatch")) {
    return "That was refused. These credentials may not have permission, or a key/secret is wrong.";
  }
  if (is("InvalidAccessKeyId")) {
    return "That access key isn't recognised by the destination. Check the key and secret on the replication target.";
  }
  if (
    is("ObjectLockConfigurationNotFoundError") ||
    has(/object lock|retention|legal hold|worm/i)
  ) {
    return "Object Lock is protecting this object. It can't be deleted or changed until its retention period ends or its legal hold is removed.";
  }
  if (is("PreconditionFailed") || has(/precondition/i)) {
    return "Something changed since you loaded this. Refresh and try again.";
  }
  return null;
}

/** Build a message from the HTTP status, led by the caller's plain description of the action. */
function statusHint(status: number, fallback: string): string {
  const lead = fallback.replace(/\s*$/, "");
  const join = (hint: string) => `${lead.replace(/\.?$/, ".")} ${hint}`;
  if (status === 400 || status === 422)
    return join("The values weren't accepted — check them and try again.");
  if (status === 403)
    return join("These credentials may not have permission for this.");
  if (status === 404)
    return join("It may have already been removed — refresh and try again.");
  if (status === 409)
    return join("It conflicts with the current state — refresh and try again.");
  if (status === 413) return join("The value is too large.");
  if (status === 429)
    return join("The server is busy — wait a moment and try again.");
  if (status >= 500)
    return join("Something went wrong on the server — wait a moment and try again.");
  return lead;
}

/** Capitalise the first letter and ensure a single trailing period, so any message reads as a sentence. */
function sentence(s: string): string {
  const t = s.trim().replace(/\s+/g, " ");
  if (!t) return t;
  const cap = t.charAt(0).toUpperCase() + t.slice(1);
  return /[.!?)]$/.test(cap) ? cap : `${cap}.`;
}

function authHeaders(extra?: Record<string, string>): Record<string, string> {
  // No Authorization header: the httpOnly session cookie carries auth and the browser sends it
  // automatically on these same-origin requests (see `credentials: "same-origin"` below).
  return { Accept: "application/json", ...extra };
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
  const init: RequestInit = {
    method,
    headers: authHeaders(),
    credentials: "same-origin",
  };
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
      credentials: "same-origin",
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

  // --- Session (httpOnly cookie auth) ---
  // Sign in: the server validates the credential and sets the httpOnly session cookie. The secret
  // is sent once in this request body and never stored in JS afterwards.
  createSession: (accessKey: string, secretKey: string) =>
    request<SessionInfo>("POST", "/session", {
      access_key: accessKey,
      secret_key: secretKey,
    }),
  // Who am I: 200 with the current identity if the cookie authenticates, else 401. Lets the SPA
  // decide between the console and the login screen without ever reading the cookie.
  session: () => request<SessionInfo>("GET", "/session"),
  // Sign out: expire the cookie server-side.
  endSession: () => request<{ ok: boolean }>("DELETE", "/session"),

  overview: () => request<OverviewResp>("GET", "/overview"),
  overviewBuckets: () =>
    request<OverviewBucketsResp>("GET", "/overview/buckets"),
  system: () => request<SystemResp>("GET", "/system"),

  // Mint a short-lived, single-use ticket for the SSE live-update stream. EventSource cannot send
  // an Authorization header, so the browser POSTs here with its Bearer token, then opens the stream
  // with `?ticket=`. See lib/live.ts.
  eventsTicket: () => request<{ ticket: string }>("POST", "/events/ticket"),

  listBuckets: () => request<BucketListResp>("GET", "/buckets"),
  createBucket: (name: string, object_lock = false) =>
    request<{ name: string }>("POST", "/buckets", { name, object_lock }),
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

  // Persistent object shares (ARCH 15.8): revocable, optionally forever. `url` is a path
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

  // Bucket configuration (ARCH 22.2).
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
  // Default server-side encryption for new uploads. algorithm: "AES256" | "none". When `required`,
  // the bucket mandates encryption: a client PUT that would store a plaintext object is refused.
  setEncryption: (name: string, algorithm: string, required = false) =>
    request<null>("PUT", `/buckets/${enc(name)}/encryption`, { algorithm, required }),
  // The policy body is a raw policy JSON document sent verbatim.
  setPolicy: (name: string, rawBody: string) =>
    requestRaw<null>("PUT", `/buckets/${enc(name)}/policy`, rawBody),
  deletePolicy: (name: string) =>
    request<null>("DELETE", `/buckets/${enc(name)}/policy`),

  // --- webhook event notifications (the secret is write-only; GET returns has_secret) ---
  getNotifications: (name: string) =>
    request<NotificationsResp>("GET", `/buckets/${enc(name)}/notifications`),
  setNotifications: (name: string, config: NotificationConfigInput) =>
    request<null>("PUT", `/buckets/${enc(name)}/notifications`, config),
  clearNotifications: (name: string) =>
    request<null>("DELETE", `/buckets/${enc(name)}/notifications`),

  // --- STS temporary session credentials (the secret + token are shown exactly once) ---
  mintSessionCredential: (req: MintSessionReq) =>
    request<MintSessionResp>("POST", "/credentials/temporary", req),
  listSessions: () =>
    request<ListSessionsResp>("GET", "/credentials/temporary"),
  revokeSession: (accessKeyId: string) =>
    request<null>("DELETE", `/credentials/temporary/${enc(accessKeyId)}`),

  listUsers: () => request<UserListResp>("GET", "/users"),
  // Created users are S3-API-only: the response carries their S3 (SigV4) access key + secret,
  // shown exactly once. `role` is always "member" from the console (the root admin is the sole
  // admin).
  createUser: (display_name: string, role = "member") =>
    request<CreateUserResp>("POST", "/users", { display_name, role }),
  getUser: (id: string) => request<UserDetailResp>("GET", `/users/${enc(id)}`),
  patchUser: (id: string, fields: { is_active?: boolean }) =>
    request<UserDetailResp>("PATCH", `/users/${enc(id)}`, fields),
  // Permanently delete a user. Server-side this revokes the user's access immediately and cascades
  // its credentials, sessions, and identity policy. The server refuses (400) for the root admin, the
  // last administrator, the signed-in user, or a user that still owns buckets.
  deleteUser: (id: string) => request<null>("DELETE", `/users/${enc(id)}`),
  rotateCredentials: (id: string) =>
    request<RotateCredentialsResp>(
      "POST",
      `/users/${enc(id)}/rotate-credentials`,
    ),
  // Per-user byte quota (ARCH 27.5); null clears the limit.
  setUserQuota: (id: string, quota_bytes: number | null) =>
    request<null>("PUT", `/users/${enc(id)}/quota`, { quota_bytes }),
  // Identity (per-user) policy. The body is a raw policy JSON document sent verbatim.
  getUserPolicy: (id: string) =>
    request<UserPolicyResp>("GET", `/users/${enc(id)}/policy`),
  setUserPolicy: (id: string, rawBody: string) =>
    requestRaw<null>("PUT", `/users/${enc(id)}/policy`, rawBody),
  deleteUserPolicy: (id: string) =>
    request<null>("DELETE", `/users/${enc(id)}/policy`),

  // Per-bucket replication management (ARCH 20). Remote targets hold the
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

  // --- S3 import jobs (ARCH 27.7): import buckets + objects from another S3 store ---
  listImports: () => request<ImportListResp>("GET", "/imports"),
  getImport: (id: string) =>
    request<ImportJobDetail>("GET", `/imports/${enc(id)}`),
  createImport: (body: CreateImportReq) =>
    request<CreateImportResp>("POST", "/imports", body),
  probeSourceBuckets: (body: ProbeSourceReq) =>
    request<ProbeSourceResp>("POST", "/imports/source/buckets", body),
  cancelImport: (id: string) => request<null>("DELETE", `/imports/${enc(id)}`),
  resumeImport: (id: string) =>
    request<CreateImportResp>("POST", `/imports/${enc(id)}/resume`),

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
