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

export const api = {
  health: () => request("GET", "/health"),
  overview: () => request("GET", "/overview"),

  listBuckets: () => request("GET", "/buckets"),
  createBucket: (name) => request("POST", "/buckets", { name }),
  getBucket: (name) => request("GET", `/buckets/${encodeURIComponent(name)}`),
  deleteBucket: (name) =>
    request("DELETE", `/buckets/${encodeURIComponent(name)}`),
  listObjects: (name, { prefix = "", limit = 100 } = {}) => {
    const q = new URLSearchParams();
    if (prefix) q.set("prefix", prefix);
    if (limit) q.set("limit", String(limit));
    const qs = q.toString();
    return request(
      "GET",
      `/buckets/${encodeURIComponent(name)}/objects${qs ? `?${qs}` : ""}`,
    );
  },

  listUsers: () => request("GET", "/users"),
  createUser: (display_name, role) =>
    request("POST", "/users", { display_name, role }),

  activity: (limit = 50) => request("GET", `/activity?limit=${limit}`),
};
