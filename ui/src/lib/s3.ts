// Object data plane. The S3 API (served at the root, path-style) accepts the same
// Bearer credential as the management API, so the browser can upload, download,
// preview, and delete object bytes directly — no separate SDK or signing needed.

import { ApiError, loadToken } from "./api";
import type { ReplicationRule } from "./types";

function s3headers(extra?: Record<string, string>): Record<string, string> {
  const h: Record<string, string> = { ...extra };
  const tok = loadToken();
  if (tok) h.Authorization = `Bearer ${tok}`;
  return h;
}

export function objectPath(bucket: string, key: string): string {
  const k = String(key).split("/").map(encodeURIComponent).join("/");
  return `/${encodeURIComponent(bucket)}/${k}`;
}

export async function putObject(
  bucket: string,
  key: string,
  file: File,
): Promise<void> {
  // Encryption is a bucket setting, not an upload option: the server applies
  // the bucket's default SSE-S3 configuration to every header-less upload.
  const headers = s3headers({
    "Content-Type": file.type || "application/octet-stream",
  });
  const res = await fetch(objectPath(bucket, key), {
    method: "PUT",
    headers,
    body: file,
  });
  if (!res.ok) throw new ApiError(`upload failed (${res.status})`, res.status);
}

export async function getObjectBlob(
  bucket: string,
  key: string,
): Promise<Blob> {
  const res = await fetch(objectPath(bucket, key), { headers: s3headers() });
  if (!res.ok)
    throw new ApiError(`download failed (${res.status})`, res.status);
  return await res.blob();
}

export async function deleteObject(bucket: string, key: string): Promise<void> {
  const res = await fetch(objectPath(bucket, key), {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`delete failed (${res.status})`, res.status);
}

// Per-bucket replication rule via the S3 subresource (?replication). Returns the rule's
// destination bucket + prefix (or null when no rule is configured).
export async function getReplication(
  bucket: string,
): Promise<ReplicationRule | null> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?replication`, {
    headers: s3headers(),
  });
  if (res.status === 404) return null;
  if (!res.ok)
    throw new ApiError(`load replication failed (${res.status})`, res.status);
  const xml = await res.text();
  const dest = /<Bucket>(?:arn:aws:s3:::)?([^<]+)<\/Bucket>/.exec(xml);
  const prefix = /<Prefix>([^<]*)<\/Prefix>/.exec(xml);
  return {
    dest_bucket: dest ? dest[1]! : "",
    prefix: prefix ? prefix[1]! : "",
  };
}

export async function putReplication(
  bucket: string,
  destBucket: string,
  prefix = "",
): Promise<void> {
  const xml =
    `<ReplicationConfiguration><Role>cairn</Role><Rule><ID>cairn-ui</ID>` +
    `<Status>Enabled</Status><Filter><Prefix>${prefix}</Prefix></Filter>` +
    `<Destination><Bucket>arn:aws:s3:::${destBucket}</Bucket></Destination></Rule>` +
    `</ReplicationConfiguration>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?replication`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body: xml,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`set replication failed (${res.status})`, res.status);
}

export async function deleteReplication(bucket: string): Promise<void> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?replication`, {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`clear replication failed (${res.status})`, res.status);
}
