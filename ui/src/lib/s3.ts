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

export async function deleteObject(
  bucket: string,
  key: string,
  versionId?: string,
): Promise<void> {
  const q = versionId ? `?versionId=${encodeURIComponent(versionId)}` : "";
  const res = await fetch(objectPath(bucket, key) + q, {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`delete failed (${res.status})`, res.status);
}

// Create an empty "folder" marker: a zero-byte object whose key ends in "/".
// S3 has no real folders; a prefix marker makes an empty folder browsable.
export async function createFolder(
  bucket: string,
  prefix: string,
): Promise<void> {
  const key = prefix.endsWith("/") ? prefix : `${prefix}/`;
  const res = await fetch(objectPath(bucket, key), {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/x-directory" }),
    body: new Blob([]),
  });
  if (!res.ok)
    throw new ApiError(`create folder failed (${res.status})`, res.status);
}

// Server-side copy (used for copy/move/rename): PUT the destination with an
// x-amz-copy-source header naming the source. A "move" is copy + delete.
export async function copyObject(
  bucket: string,
  srcKey: string,
  destKey: string,
): Promise<void> {
  const source = objectPath(bucket, srcKey); // "/bucket/encoded/key"
  const res = await fetch(objectPath(bucket, destKey), {
    method: "PUT",
    headers: s3headers({ "x-amz-copy-source": source }),
  });
  if (!res.ok)
    throw new ApiError(`copy failed (${res.status})`, res.status);
}

// --- XML helpers ----------------------------------------------------------------

function xmlEscape(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function parseXml(text: string): Document {
  return new DOMParser().parseFromString(text, "application/xml");
}

function childText(el: Element, tag: string): string {
  return el.getElementsByTagName(tag)[0]?.textContent ?? "";
}

// --- Object versions (?versions) ------------------------------------------------

export interface ObjectVersion {
  key: string;
  versionId: string;
  size: number;
  lastModifiedMs: number;
  isLatest: boolean;
  isDeleteMarker: boolean;
}

export interface VersionListing {
  versions: ObjectVersion[];
  commonPrefixes: string[];
}

export async function listObjectVersions(
  bucket: string,
  prefix = "",
  delimiter = "/",
): Promise<VersionListing> {
  let url = `/${encodeURIComponent(bucket)}?versions`;
  if (prefix) url += `&prefix=${encodeURIComponent(prefix)}`;
  if (delimiter) url += `&delimiter=${encodeURIComponent(delimiter)}`;
  const res = await fetch(url, { headers: s3headers() });
  if (!res.ok)
    throw new ApiError(`list versions failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());

  const read = (el: Element, isMarker: boolean): ObjectVersion => ({
    key: childText(el, "Key"),
    versionId: childText(el, "VersionId"),
    size: isMarker ? 0 : Number(childText(el, "Size") || 0),
    lastModifiedMs: Date.parse(childText(el, "LastModified")) || 0,
    isLatest: childText(el, "IsLatest") === "true",
    isDeleteMarker: isMarker,
  });

  const versions: ObjectVersion[] = [
    ...Array.from(doc.getElementsByTagName("Version")).map((el) =>
      read(el, false),
    ),
    ...Array.from(doc.getElementsByTagName("DeleteMarker")).map((el) =>
      read(el, true),
    ),
  ].sort(
    (a, b) =>
      a.key.localeCompare(b.key) || b.lastModifiedMs - a.lastModifiedMs,
  );

  const commonPrefixes = Array.from(
    doc.getElementsByTagName("CommonPrefixes"),
  ).map((el) => childText(el, "Prefix"));

  return { versions, commonPrefixes };
}

// --- Object tagging (?tagging) --------------------------------------------------

export interface ObjectTag {
  key: string;
  value: string;
}

export async function getObjectTagging(
  bucket: string,
  key: string,
): Promise<ObjectTag[]> {
  const res = await fetch(objectPath(bucket, key) + "?tagging", {
    headers: s3headers(),
  });
  if (res.status === 404) return [];
  if (!res.ok)
    throw new ApiError(`load tags failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  return Array.from(doc.getElementsByTagName("Tag")).map((el) => ({
    key: childText(el, "Key"),
    value: childText(el, "Value"),
  }));
}

export async function putObjectTagging(
  bucket: string,
  key: string,
  tags: ObjectTag[],
): Promise<void> {
  const body =
    `<Tagging><TagSet>` +
    tags
      .map(
        (t) =>
          `<Tag><Key>${xmlEscape(t.key)}</Key><Value>${xmlEscape(t.value)}</Value></Tag>`,
      )
      .join("") +
    `</TagSet></Tagging>`;
  const res = await fetch(objectPath(bucket, key) + "?tagging", {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`save tags failed (${res.status})`, res.status);
}

export async function deleteObjectTagging(
  bucket: string,
  key: string,
): Promise<void> {
  const res = await fetch(objectPath(bucket, key) + "?tagging", {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`clear tags failed (${res.status})`, res.status);
}

// --- Bulk delete (?delete) ------------------------------------------------------

export interface BulkDeleteResult {
  deleted: number;
  errors: { key: string; message: string }[];
}

export async function bulkDelete(
  bucket: string,
  keys: string[],
): Promise<BulkDeleteResult> {
  const body =
    `<Delete>` +
    keys.map((k) => `<Object><Key>${xmlEscape(k)}</Key></Object>`).join("") +
    `</Delete>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?delete`, {
    method: "POST",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok)
    throw new ApiError(`bulk delete failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  const deleted = doc.getElementsByTagName("Deleted").length;
  const errors = Array.from(doc.getElementsByTagName("Error")).map((el) => ({
    key: childText(el, "Key"),
    message: childText(el, "Message") || childText(el, "Code"),
  }));
  return { deleted, errors };
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
