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
  const k = String(key)
    .split("/")
    .map((seg) =>
      // encodeURIComponent leaves dots unescaped, so a "." or ".." key segment would let the
      // browser normalize the URL and escape the bucket prefix (audit #31). Percent-encode the
      // dots of a pure-dot segment so it round-trips as a literal key segment, not navigation.
      seg === "." || seg === ".." ? seg.replace(/\./g, "%2E") : encodeURIComponent(seg),
    )
    .join("/");
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

export interface UploadProgress {
  loaded: number;
  total: number;
  /** Smoothed transfer rate in BYTES per second. */
  bytesPerSec: number;
}

// Upload with live progress. fetch() gives no upload-progress events, so this
// uses XMLHttpRequest (whose upload.onprogress reports loaded/total). The rate
// is the byte delta over the wall-clock delta between events, lightly smoothed.
export function putObjectWithProgress(
  bucket: string,
  key: string,
  file: File,
  onProgress: (p: UploadProgress) => void,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", objectPath(bucket, key));
    const tok = loadToken();
    if (tok) xhr.setRequestHeader("Authorization", `Bearer ${tok}`);
    xhr.setRequestHeader(
      "Content-Type",
      file.type || "application/octet-stream",
    );

    let lastLoaded = 0;
    let lastTime = performance.now();
    let rate = 0;
    xhr.upload.onprogress = (e) => {
      if (!e.lengthComputable) return;
      const now = performance.now();
      const dt = (now - lastTime) / 1000;
      // Recompute the rate at most ~6×/s so it's stable, not jumpy.
      if (dt >= 0.15) {
        const inst = (e.loaded - lastLoaded) / dt;
        rate = rate === 0 ? inst : rate * 0.6 + inst * 0.4;
        lastLoaded = e.loaded;
        lastTime = now;
      }
      onProgress({ loaded: e.loaded, total: e.total, bytesPerSec: rate });
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve();
      else reject(new ApiError(`upload failed (${xhr.status})`, xhr.status));
    };
    xhr.onerror = () =>
      reject(new ApiError("network error during upload", 0));
    xhr.onabort = () => reject(new ApiError("upload cancelled", 0));
    if (signal) {
      if (signal.aborted) {
        xhr.abort();
        return;
      }
      signal.addEventListener("abort", () => xhr.abort(), { once: true });
    }
    xhr.send(file);
  });
}

export async function getObjectBlob(
  bucket: string,
  key: string,
  versionId?: string,
): Promise<Blob> {
  const q = versionId ? `?versionId=${encodeURIComponent(versionId)}` : "";
  const res = await fetch(objectPath(bucket, key) + q, { headers: s3headers() });
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

// --- Public Access Block (?publicAccessBlock) -----------------------------------

export interface PublicAccessBlock {
  blockPublicAcls: boolean;
  ignorePublicAcls: boolean;
  blockPublicPolicy: boolean;
  restrictPublicBuckets: boolean;
}

export async function getPublicAccessBlock(
  bucket: string,
): Promise<PublicAccessBlock | null> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?publicAccessBlock`, {
    headers: s3headers(),
  });
  if (res.status === 404) return null;
  if (!res.ok)
    throw new ApiError(`load access block failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  const flag = (tag: string) =>
    (doc.getElementsByTagName(tag)[0]?.textContent ?? "").trim() === "true";
  return {
    blockPublicAcls: flag("BlockPublicAcls"),
    ignorePublicAcls: flag("IgnorePublicAcls"),
    blockPublicPolicy: flag("BlockPublicPolicy"),
    restrictPublicBuckets: flag("RestrictPublicBuckets"),
  };
}

export async function putPublicAccessBlock(
  bucket: string,
  b: PublicAccessBlock,
): Promise<void> {
  const body =
    `<PublicAccessBlockConfiguration>` +
    `<BlockPublicAcls>${b.blockPublicAcls}</BlockPublicAcls>` +
    `<IgnorePublicAcls>${b.ignorePublicAcls}</IgnorePublicAcls>` +
    `<BlockPublicPolicy>${b.blockPublicPolicy}</BlockPublicPolicy>` +
    `<RestrictPublicBuckets>${b.restrictPublicBuckets}</RestrictPublicBuckets>` +
    `</PublicAccessBlockConfiguration>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?publicAccessBlock`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`save access block failed (${res.status})`, res.status);
}

// --- Object Ownership (?ownershipControls) --------------------------------------

// `mode` is the S3 ObjectOwnership value: BucketOwnerEnforced | BucketOwnerPreferred | ObjectWriter.
export async function putOwnershipControls(
  bucket: string,
  mode: string,
): Promise<void> {
  const body = `<OwnershipControls><Rule><ObjectOwnership>${mode}</ObjectOwnership></Rule></OwnershipControls>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?ownershipControls`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`save ownership failed (${res.status})`, res.status);
}

// --- Bucket tagging (?tagging on the bucket) ------------------------------------

export async function getBucketTagging(bucket: string): Promise<ObjectTag[]> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?tagging`, {
    headers: s3headers(),
  });
  if (res.status === 404) return [];
  if (!res.ok)
    throw new ApiError(`load bucket tags failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  return Array.from(doc.getElementsByTagName("Tag")).map((el) => ({
    key: childText(el, "Key"),
    value: childText(el, "Value"),
  }));
}

export async function putBucketTagging(
  bucket: string,
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
  const res = await fetch(`/${encodeURIComponent(bucket)}?tagging`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`save bucket tags failed (${res.status})`, res.status);
}

export async function deleteBucketTagging(bucket: string): Promise<void> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?tagging`, {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`clear bucket tags failed (${res.status})`, res.status);
}

// Per-bucket replication rule via the S3 subresource (?replication). `dest_bucket` carries the
// rule's raw `<Destination><Bucket>` — for a console-set rule this is the remote target ARN
// (`arn:cairn:replication:…`); a legacy `arn:aws:s3:::name` is returned with the prefix stripped.
// Match it against the bucket's registered targets to render the destination. Null when unset.
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

// `destination` is the remote **target ARN** (`arn:cairn:replication:…`) the rule ships to, not a
// bare bucket name: the engine matches each outbox entry to a registered target by that ARN (stamped
// at enqueue), so a rule naming only a bucket links to no target and every object lands in the failed
// queue. Register the target first (`addReplicationTarget`), then name its ARN here.
export async function putReplication(
  bucket: string,
  destination: string,
  prefix = "",
): Promise<void> {
  const xml =
    `<ReplicationConfiguration><Role>cairn</Role><Rule><ID>cairn-ui</ID>` +
    `<Status>Enabled</Status><Filter><Prefix>${xmlEscape(prefix)}</Prefix></Filter>` +
    `<Destination><Bucket>${xmlEscape(destination)}</Bucket></Destination></Rule>` +
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

// --- Object Lock / WORM (S3 ?object-lock, ?retention, ?legal-hold; ARCH 16.5) ---

export type LockMode = "GOVERNANCE" | "COMPLIANCE";

export interface BucketObjectLock {
  enabled: boolean;
  /** The bucket default retention, when configured. */
  defaultMode?: LockMode;
  defaultDays?: number;
  defaultYears?: number;
}

export interface ObjectRetention {
  mode: LockMode;
  /** ISO-8601 retain-until instant. */
  retainUntil: string;
}

/** Read a bucket's Object Lock configuration. Returns `{enabled:false}` when not configured. */
export async function getObjectLockConfig(
  bucket: string,
): Promise<BucketObjectLock> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?object-lock`, {
    headers: s3headers(),
  });
  if (res.status === 404 || res.status === 400) return { enabled: false };
  if (!res.ok)
    throw new ApiError(`load object lock failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  const text = (tag: string) =>
    (doc.getElementsByTagName(tag)[0]?.textContent ?? "").trim();
  const enabled = text("ObjectLockEnabled") === "Enabled";
  const mode = text("Mode");
  const days = text("Days");
  const years = text("Years");
  return {
    enabled,
    defaultMode: mode ? (mode as LockMode) : undefined,
    defaultDays: days ? Number(days) : undefined,
    defaultYears: years ? Number(years) : undefined,
  };
}

/** Set a bucket's Object Lock default retention (the bucket must already be lock-enabled). */
export async function putObjectLockConfig(
  bucket: string,
  cfg: BucketObjectLock,
): Promise<void> {
  let rule = "";
  if (cfg.defaultMode && (cfg.defaultDays || cfg.defaultYears)) {
    const period = cfg.defaultYears
      ? `<Years>${cfg.defaultYears}</Years>`
      : `<Days>${cfg.defaultDays}</Days>`;
    rule = `<Rule><DefaultRetention><Mode>${cfg.defaultMode}</Mode>${period}</DefaultRetention></Rule>`;
  }
  const xml =
    `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>` +
    `${rule}</ObjectLockConfiguration>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?object-lock`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body: xml,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`set object lock failed (${res.status})`, res.status);
}

function versionQuery(versionId?: string): string {
  return versionId ? `&versionId=${encodeURIComponent(versionId)}` : "";
}

export async function getObjectRetention(
  bucket: string,
  key: string,
  versionId?: string,
): Promise<ObjectRetention | null> {
  const res = await fetch(
    `/${encodeURIComponent(bucket)}/${encodeURI(key)}?retention${versionQuery(versionId)}`,
    { headers: s3headers() },
  );
  if (res.status === 404) return null;
  if (!res.ok)
    throw new ApiError(`load retention failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  const mode = (
    doc.getElementsByTagName("Mode")[0]?.textContent ?? ""
  ).trim();
  const until = (
    doc.getElementsByTagName("RetainUntilDate")[0]?.textContent ?? ""
  ).trim();
  if (!mode || !until) return null;
  return { mode: mode as LockMode, retainUntil: until };
}

export async function putObjectRetention(
  bucket: string,
  key: string,
  retention: ObjectRetention,
  opts: { bypassGovernance?: boolean; versionId?: string } = {},
): Promise<void> {
  const xml =
    `<Retention><Mode>${retention.mode}</Mode>` +
    `<RetainUntilDate>${retention.retainUntil}</RetainUntilDate></Retention>`;
  const headers: Record<string, string> = { "Content-Type": "application/xml" };
  if (opts.bypassGovernance)
    headers["x-amz-bypass-governance-retention"] = "true";
  const res = await fetch(
    `/${encodeURIComponent(bucket)}/${encodeURI(key)}?retention${versionQuery(opts.versionId)}`,
    { method: "PUT", headers: s3headers(headers), body: xml },
  );
  if (!res.ok && res.status !== 200)
    throw new ApiError(`set retention failed (${res.status})`, res.status);
}

export async function getObjectLegalHold(
  bucket: string,
  key: string,
  versionId?: string,
): Promise<boolean> {
  const res = await fetch(
    `/${encodeURIComponent(bucket)}/${encodeURI(key)}?legal-hold${versionQuery(versionId)}`,
    { headers: s3headers() },
  );
  if (res.status === 404) return false;
  if (!res.ok)
    throw new ApiError(`load legal hold failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  return (
    (doc.getElementsByTagName("Status")[0]?.textContent ?? "").trim() === "ON"
  );
}

export async function putObjectLegalHold(
  bucket: string,
  key: string,
  on: boolean,
  versionId?: string,
): Promise<void> {
  const xml = `<LegalHold><Status>${on ? "ON" : "OFF"}</Status></LegalHold>`;
  const res = await fetch(
    `/${encodeURIComponent(bucket)}/${encodeURI(key)}?legal-hold${versionQuery(versionId)}`,
    {
      method: "PUT",
      headers: s3headers({ "Content-Type": "application/xml" }),
      body: xml,
    },
  );
  if (!res.ok && res.status !== 200)
    throw new ApiError(`set legal hold failed (${res.status})`, res.status);
}

// --- CORS (S3 ?cors) ---

export interface CorsRule {
  allowedOrigins: string[];
  allowedMethods: string[];
  allowedHeaders: string[];
  exposeHeaders: string[];
  maxAgeSeconds?: number;
}

const textsOf = (parent: Element, tag: string): string[] =>
  Array.from(parent.getElementsByTagName(tag))
    .map((e) => (e.textContent ?? "").trim())
    .filter(Boolean);

export async function getCors(bucket: string): Promise<CorsRule[]> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?cors`, {
    headers: s3headers(),
  });
  if (res.status === 404) return [];
  if (!res.ok)
    throw new ApiError(`load CORS failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  return Array.from(doc.getElementsByTagName("CORSRule")).map((r) => {
    const max = (r.getElementsByTagName("MaxAgeSeconds")[0]?.textContent ?? "").trim();
    return {
      allowedOrigins: textsOf(r, "AllowedOrigin"),
      allowedMethods: textsOf(r, "AllowedMethod"),
      allowedHeaders: textsOf(r, "AllowedHeader"),
      exposeHeaders: textsOf(r, "ExposeHeader"),
      maxAgeSeconds: max ? Number(max) : undefined,
    };
  });
}

export async function putCors(bucket: string, rules: CorsRule[]): Promise<void> {
  const body =
    `<CORSConfiguration>` +
    rules
      .map(
        (r) =>
          `<CORSRule>` +
          r.allowedOrigins
            .map((o) => `<AllowedOrigin>${xmlEscape(o)}</AllowedOrigin>`)
            .join("") +
          r.allowedMethods
            .map((m) => `<AllowedMethod>${xmlEscape(m)}</AllowedMethod>`)
            .join("") +
          r.allowedHeaders
            .map((h) => `<AllowedHeader>${xmlEscape(h)}</AllowedHeader>`)
            .join("") +
          r.exposeHeaders
            .map((h) => `<ExposeHeader>${xmlEscape(h)}</ExposeHeader>`)
            .join("") +
          (r.maxAgeSeconds !== undefined
            ? `<MaxAgeSeconds>${r.maxAgeSeconds}</MaxAgeSeconds>`
            : "") +
          `</CORSRule>`,
      )
      .join("") +
    `</CORSConfiguration>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?cors`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`set CORS failed (${res.status})`, res.status);
}

export async function deleteCors(bucket: string): Promise<void> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?cors`, {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`delete CORS failed (${res.status})`, res.status);
}

// --- Lifecycle (S3 ?lifecycle) — expiration / noncurrent / abort-incomplete only;
//     storage-class transition is not implemented by Cairn (the server rejects it). ---

export interface LifecycleRule {
  id: string;
  enabled: boolean;
  prefix: string;
  expirationDays?: number;
  noncurrentDays?: number;
  abortDays?: number;
}

function intOf(parent: Element, path: string[]): number | undefined {
  let el: Element | undefined = parent;
  for (const tag of path) {
    el = el?.getElementsByTagName(tag)[0] ?? undefined;
    if (!el) return undefined;
  }
  const v = (el.textContent ?? "").trim();
  return v ? Number(v) : undefined;
}

export async function getLifecycle(bucket: string): Promise<LifecycleRule[]> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?lifecycle`, {
    headers: s3headers(),
  });
  if (res.status === 404) return [];
  if (!res.ok)
    throw new ApiError(`load lifecycle failed (${res.status})`, res.status);
  const doc = parseXml(await res.text());
  return Array.from(doc.getElementsByTagName("Rule")).map((r, i) => ({
    id: (r.getElementsByTagName("ID")[0]?.textContent ?? `rule-${i + 1}`).trim(),
    enabled:
      (r.getElementsByTagName("Status")[0]?.textContent ?? "").trim() ===
      "Enabled",
    prefix:
      (
        r.getElementsByTagName("Prefix")[0]?.textContent ?? ""
      ).trim(),
    expirationDays: intOf(r, ["Expiration", "Days"]),
    noncurrentDays: intOf(r, ["NoncurrentVersionExpiration", "NoncurrentDays"]),
    abortDays: intOf(r, [
      "AbortIncompleteMultipartUpload",
      "DaysAfterInitiation",
    ]),
  }));
}

export async function putLifecycle(
  bucket: string,
  rules: LifecycleRule[],
): Promise<void> {
  const body =
    `<LifecycleConfiguration>` +
    rules
      .map((r) => {
        let actions = "";
        if (r.expirationDays)
          actions += `<Expiration><Days>${r.expirationDays}</Days></Expiration>`;
        if (r.noncurrentDays)
          actions += `<NoncurrentVersionExpiration><NoncurrentDays>${r.noncurrentDays}</NoncurrentDays></NoncurrentVersionExpiration>`;
        if (r.abortDays)
          actions += `<AbortIncompleteMultipartUpload><DaysAfterInitiation>${r.abortDays}</DaysAfterInitiation></AbortIncompleteMultipartUpload>`;
        return (
          `<Rule><ID>${xmlEscape(r.id)}</ID>` +
          `<Status>${r.enabled ? "Enabled" : "Disabled"}</Status>` +
          `<Filter><Prefix>${xmlEscape(r.prefix)}</Prefix></Filter>` +
          actions +
          `</Rule>`
        );
      })
      .join("") +
    `</LifecycleConfiguration>`;
  const res = await fetch(`/${encodeURIComponent(bucket)}?lifecycle`, {
    method: "PUT",
    headers: s3headers({ "Content-Type": "application/xml" }),
    body,
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`set lifecycle failed (${res.status})`, res.status);
}

export async function deleteLifecycle(bucket: string): Promise<void> {
  const res = await fetch(`/${encodeURIComponent(bucket)}?lifecycle`, {
    method: "DELETE",
    headers: s3headers(),
  });
  if (!res.ok && res.status !== 204)
    throw new ApiError(`delete lifecycle failed (${res.status})`, res.status);
}
