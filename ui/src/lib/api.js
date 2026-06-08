// Thin client of the Cairn management API (ARCH §22, §23).
//
// Every request is admin-gated via a Bearer token of the form
// `cairn_<id>.<secret>`. The token is held in memory and mirrored to
// localStorage so a reload keeps the session. The UI carries no privileged
// logic of its own: it is a pure presentation layer over the control plane.

const BASE = "/api/v1";
const TOKEN_KEY = "cairn.token";

let token = null;

export function loadToken() {
  if (token === null) {
    try {
      token = localStorage.getItem(TOKEN_KEY) || "";
    } catch {
      token = "";
    }
  }
  return token;
}

export function setToken(value) {
  token = value || "";
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* storage may be unavailable; in-memory token still works */
  }
}

export function clearToken() {
  setToken("");
}

export function hasToken() {
  return !!loadToken();
}

// An error carrying the HTTP status so callers (e.g. login) can react to 401/403.
export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request(method, path, body) {
  const headers = { Accept: "application/json" };
  const tok = loadToken();
  if (tok) headers.Authorization = `Bearer ${tok}`;

  const init = { method, headers };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }

  let res;
  try {
    res = await fetch(BASE + path, init);
  } catch (e) {
    throw new ApiError(`network error: ${e.message || e}`, 0);
  }

  if (res.status === 204) return null;

  let payload = null;
  const text = await res.text();
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { raw: text };
    }
  }

  if (!res.ok) {
    const msg =
      (payload && (payload.error || payload.message)) ||
      `request failed (${res.status})`;
    throw new ApiError(msg, res.status);
  }
  return payload;
}

// Like `request`, but the body is sent verbatim as a string rather than being
// JSON-encoded. Used by the bucket-policy endpoint, whose body is a raw policy
// JSON document the server validates and stores as-is.
async function requestRaw(method, path, rawBody) {
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
  };
  const tok = loadToken();
  if (tok) headers.Authorization = `Bearer ${tok}`;

  let res;
  try {
    res = await fetch(BASE + path, { method, headers, body: rawBody });
  } catch (e) {
    throw new ApiError(`network error: ${e.message || e}`, 0);
  }

  if (res.status === 204) return null;

  let payload = null;
  const text = await res.text();
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { raw: text };
    }
  }

  if (!res.ok) {
    const msg =
      (payload && (payload.error || payload.message)) ||
      `request failed (${res.status})`;
    throw new ApiError(msg, res.status);
  }
  return payload;
}

export const api = {
  health: () => request("GET", "/health"),
  overview: () => request("GET", "/overview"),

  listBuckets: () => request("GET", "/buckets"),
  createBucket: (name) => request("POST", "/buckets", { name }),
  getBucket: (name) => request("GET", `/buckets/${encodeURIComponent(name)}`),
  deleteBucket: (name) =>
    request("DELETE", `/buckets/${encodeURIComponent(name)}`),
  listObjects: (name, { prefix = "", limit = 100, cursor = "" } = {}) => {
    const q = new URLSearchParams();
    if (prefix) q.set("prefix", prefix);
    if (limit) q.set("limit", String(limit));
    // Continuation cursor returned as `next` by a prior page; omitted on the first page.
    if (cursor) q.set("cursor", cursor);
    const qs = q.toString();
    return request(
      "GET",
      `/buckets/${encodeURIComponent(name)}/objects${qs ? `?${qs}` : ""}`,
    );
  },

  // Mint a signed, time-limited public-read ("share") URL for an object. Returns { url,
  // expires_at_ms }; `url` is a path (/p/...) the caller turns into an absolute link.
  shareObject: (name, key, expires_in_secs = 3600) =>
    request("POST", `/buckets/${encodeURIComponent(name)}/objects/share`, {
      key,
      expires_in_secs,
    }),

  // Bucket configuration (ARCH §22.2).
  getBucketConfig: (name) =>
    request("GET", `/buckets/${encodeURIComponent(name)}/config`),
  setVersioning: (name, status) =>
    request("PUT", `/buckets/${encodeURIComponent(name)}/versioning`, {
      status,
    }),
  setQuota: (name, quota_bytes) =>
    request("PUT", `/buckets/${encodeURIComponent(name)}/quota`, {
      quota_bytes,
    }),
  // Set/disable the bucket compression policy. algorithm: "zstd" | "lz4" | "none".
  setCompression: (name, algorithm, block_size = 65536) =>
    request("PUT", `/buckets/${encodeURIComponent(name)}/compression`, {
      algorithm,
      block_size,
    }),
  // The policy body is a raw policy JSON document, not the usual {error}/{message}
  // envelope; `rawBody` is sent verbatim as the request body.
  setPolicy: (name, rawBody) =>
    requestRaw("PUT", `/buckets/${encodeURIComponent(name)}/policy`, rawBody),
  deletePolicy: (name) =>
    request("DELETE", `/buckets/${encodeURIComponent(name)}/policy`),

  listUsers: () => request("GET", "/users"),
  // Created users are S3-API-only: the response carries their S3 (SigV4) access key + secret,
  // shown exactly once. `role` is always "member" from the console (the root admin is the sole admin).
  createUser: (display_name, role = "member") =>
    request("POST", "/users", { display_name, role }),
  getUser: (id) => request("GET", `/users/${encodeURIComponent(id)}`),
  patchUser: (id, fields) =>
    request("PATCH", `/users/${encodeURIComponent(id)}`, fields),
  rotateCredentials: (id) =>
    request("POST", `/users/${encodeURIComponent(id)}/rotate-credentials`),
  // Identity (per-user) policy. The body is a raw policy JSON document sent verbatim.
  setUserPolicy: (id, rawBody) =>
    requestRaw("PUT", `/users/${encodeURIComponent(id)}/policy`, rawBody),
  deleteUserPolicy: (id) =>
    request("DELETE", `/users/${encodeURIComponent(id)}/policy`),

  failedReplication: (limit = 100) =>
    request("GET", `/replication/failed?limit=${limit}`),

  activity: (limit = 50) => request("GET", `/activity?limit=${limit}`),
};

// Object data plane. The S3 API (served at the root, path-style) accepts the same
// Bearer credential as the management API, so the browser can upload, download,
// preview, and delete object bytes directly — no separate SDK or signing needed.
function s3headers() {
  const h = {};
  const tok = loadToken();
  if (tok) h.Authorization = `Bearer ${tok}`;
  return h;
}

function objectPath(bucket, key) {
  const k = String(key).split("/").map(encodeURIComponent).join("/");
  return `/${encodeURIComponent(bucket)}/${k}`;
}

export const s3 = {
  objectPath,
  async putObject(bucket, key, file, { encrypt = false } = {}) {
    const headers = {
      ...s3headers(),
      "Content-Type": file.type || "application/octet-stream",
    };
    // Server-side encryption (SSE-S3): the server generates and manages the key.
    if (encrypt) headers["x-amz-server-side-encryption"] = "AES256";
    const res = await fetch(objectPath(bucket, key), {
      method: "PUT",
      headers,
      body: file,
    });
    if (!res.ok) throw new ApiError(`upload failed (${res.status})`, res.status);
  },
  async getObjectBlob(bucket, key) {
    const res = await fetch(objectPath(bucket, key), { headers: s3headers() });
    if (!res.ok)
      throw new ApiError(`download failed (${res.status})`, res.status);
    return await res.blob();
  },
  async deleteObject(bucket, key) {
    const res = await fetch(objectPath(bucket, key), {
      method: "DELETE",
      headers: s3headers(),
    });
    if (!res.ok && res.status !== 204)
      throw new ApiError(`delete failed (${res.status})`, res.status);
  },

  // Per-bucket replication rule via the S3 subresource (?replication). Returns the rule's
  // destination bucket + prefix (or null when no rule is configured).
  async getReplication(bucket) {
    const res = await fetch(`/${encodeURIComponent(bucket)}?replication`, {
      headers: s3headers(),
    });
    if (res.status === 404) return null;
    if (!res.ok)
      throw new ApiError(`load replication failed (${res.status})`, res.status);
    const xml = await res.text();
    const dest = /<Bucket>(?:arn:aws:s3:::)?([^<]+)<\/Bucket>/.exec(xml);
    const prefix = /<Prefix>([^<]*)<\/Prefix>/.exec(xml);
    return { dest_bucket: dest ? dest[1] : "", prefix: prefix ? prefix[1] : "" };
  },
  async putReplication(bucket, destBucket, prefix = "") {
    const xml =
      `<ReplicationConfiguration><Role>cairn</Role><Rule><ID>cairn-ui</ID>` +
      `<Status>Enabled</Status><Filter><Prefix>${prefix}</Prefix></Filter>` +
      `<Destination><Bucket>arn:aws:s3:::${destBucket}</Bucket></Destination></Rule>` +
      `</ReplicationConfiguration>`;
    const res = await fetch(`/${encodeURIComponent(bucket)}?replication`, {
      method: "PUT",
      headers: { ...s3headers(), "Content-Type": "application/xml" },
      body: xml,
    });
    if (!res.ok && res.status !== 204)
      throw new ApiError(`set replication failed (${res.status})`, res.status);
  },
  async deleteReplication(bucket) {
    const res = await fetch(`/${encodeURIComponent(bucket)}?replication`, {
      method: "DELETE",
      headers: s3headers(),
    });
    if (!res.ok && res.status !== 204)
      throw new ApiError(`clear replication failed (${res.status})`, res.status);
  },
};
