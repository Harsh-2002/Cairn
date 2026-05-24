export interface PostSummary {
  slug: string;
  title: string;
  draft: boolean;
  date: string;
}

export interface Post extends PostSummary {
  body: string;
  source_path: string;
}

function adminSecret(): string {
  return (
    (window as unknown as { CAIRN_ADMIN_SECRET?: string }).CAIRN_ADMIN_SECRET ||
    localStorage.getItem("cairn:admin_secret") ||
    ""
  );
}

function headers(): HeadersInit {
  const h: Record<string, string> = { "Content-Type": "application/json" };
  const s = adminSecret();
  if (s) h["x-admin-secret"] = s;
  return h;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function jsonOrThrow<T>(label: string, r: Response): Promise<T> {
  if (!r.ok) {
    throw new ApiError(r.status, `${label}: ${r.status}`);
  }
  return (await r.json()) as T;
}

export async function listPosts(): Promise<PostSummary[]> {
  return jsonOrThrow("listPosts", await fetch("/api/posts", { headers: headers() }));
}

export async function getPost(slug: string): Promise<Post> {
  return jsonOrThrow(
    "getPost",
    await fetch(`/api/posts/${encodeURIComponent(slug)}`, { headers: headers() }),
  );
}

export async function createPost(title?: string): Promise<{ slug: string }> {
  return jsonOrThrow(
    "createPost",
    await fetch("/api/posts", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ title: title ?? "Untitled" }),
    }),
  );
}

export async function autosavePost(
  slug: string,
  body: string,
  session: string,
  title?: string,
): Promise<{ branch: string; commit: string }> {
  return jsonOrThrow(
    "autosave",
    await fetch(
      `/api/posts/${encodeURIComponent(slug)}/autosave?session=${encodeURIComponent(session)}`,
      {
        method: "PUT",
        headers: headers(),
        body: JSON.stringify({ body, title }),
      },
    ),
  );
}

export async function publishPost(
  slug: string,
  session: string,
): Promise<{ commit: string }> {
  return jsonOrThrow(
    "publish",
    await fetch(
      `/api/posts/${encodeURIComponent(slug)}/publish?session=${encodeURIComponent(session)}`,
      { method: "POST", headers: headers() },
    ),
  );
}

export async function deletePost(slug: string): Promise<{ commit: string }> {
  return jsonOrThrow(
    "deletePost",
    await fetch(`/api/posts/${encodeURIComponent(slug)}`, {
      method: "DELETE",
      headers: headers(),
    }),
  );
}

export async function presignAsset(
  sha256: string,
  ext: string,
  variant?: string,
): Promise<{ url: string; key: string }> {
  return jsonOrThrow(
    "presign",
    await fetch("/api/assets/presign", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ sha256, ext, variant }),
    }),
  );
}
