// Bidirectional codec between the PermissionBuilder's visual model and an AWS-IAM-style,
// Principal-less identity-policy document (what the backend's `parse_user_policy` consumes).
//
// The visual model is a "preset": { scope, buckets, level }. `presetToPolicy` renders it to a doc;
// `policyToPreset` best-effort recovers a preset from a doc so the Split (side-by-side) view can keep
// the builder and the JSON in sync both ways. An unrecognized-but-valid doc is "custom" — the JSON
// stays authoritative and the builder shows a read-only notice.

// Permission levels → action patterns (the backend matches s3:* / s3:Get* prefixes and exact names).
export const LEVEL_ACTIONS = {
  read: ["s3:Get*", "s3:List*"],
  write: ["s3:Get*", "s3:List*", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
  full: ["s3:*"],
};

export const LEVELS = [
  { id: "read", label: "Read-only", hint: "List & download objects" },
  { id: "write", label: "Read & write", hint: "Read plus upload & delete" },
  { id: "full", label: "Full access", hint: "Every S3 action on the scope" },
];

// Grouped catalogue for the Advanced action picker.
export const ACTION_GROUPS = [
  { label: "Read", actions: ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"] },
  { label: "Write", actions: ["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"] },
  { label: "Multipart", actions: ["s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"] },
  { label: "Tagging", actions: ["s3:GetObjectTagging", "s3:PutObjectTagging", "s3:DeleteObjectTagging"] },
];

// Plain-language gloss for each raw s3: verb shown in the Advanced picker, so the user never has to
// know the AWS verb to understand what they are granting.
export const ACTION_GLOSS = {
  "s3:GetObject": "Download files",
  "s3:GetObjectVersion": "Download older versions of a file",
  "s3:ListBucket": "See the list of files in a bucket",
  "s3:GetBucketLocation": "Look up where a bucket lives",
  "s3:PutObject": "Upload and overwrite files",
  "s3:DeleteObject": "Delete files",
  "s3:DeleteObjectVersion": "Permanently delete older versions of a file",
  "s3:AbortMultipartUpload": "Cancel an in-progress large upload",
  "s3:ListMultipartUploadParts": "See the parts of an in-progress large upload",
  "s3:GetObjectTagging": "Read the tags on a file",
  "s3:PutObjectTagging": "Add or change the tags on a file",
  "s3:DeleteObjectTagging": "Remove the tags from a file",
};

// A running, plain-language list of what a chosen action set lets the user do. Returns an array of
// short phrases (deduped, in catalogue order) for the "This lets the user:" summary.
export function actionSummary(actions) {
  const set = new Set(asArray(actions));
  if (set.has("s3:*")) return ["Do everything S3 allows on the chosen buckets"];
  const out = [];
  const seen = new Set();
  for (const g of ACTION_GROUPS) {
    for (const a of g.actions) {
      if (!set.has(a)) continue;
      const phrase = ACTION_GLOSS[a] || a;
      if (seen.has(phrase)) continue;
      seen.add(phrase);
      out.push(phrase);
    }
  }
  // Any picked actions not in the catalogue (e.g. recovered from raw JSON): show them verbatim.
  for (const a of set) {
    if (a === "s3:*") continue;
    const known = ACTION_GROUPS.some((g) => g.actions.includes(a));
    if (!known) out.push(a);
  }
  return out;
}

const asArray = (v) => (Array.isArray(v) ? v : v == null ? [] : [v]);
const sameSet = (a, b) => {
  const sa = new Set(a);
  const sb = new Set(b);
  return sa.size === sb.size && [...sa].every((x) => sb.has(x));
};

const bucketResources = (buckets) =>
  buckets.flatMap((b) => [`arn:aws:s3:::${b}`, `arn:aws:s3:::${b}/*`]);

// Resolve the resource ARNs for a scope. Scope "all" grants every bucket; scope "specific" grants
// only the picked buckets. CRITICAL: "specific" with zero buckets resolves to an EMPTY list (no
// access) — it must never silently fall back to arn:aws:s3:::* (which would grant everything). The
// builder blocks creating such a policy and the empty list is what makes the intent unambiguous.
function scopeResources(scope, buckets) {
  if (scope === "all") return ["arn:aws:s3:::*"];
  return bucketResources(buckets);
}

// True when a built doc actually grants something the backend can act on (has at least one action
// and one resource). The UI uses this to block Create on an empty/no-op policy.
export function grantsAccess(doc) {
  if (!doc || !Array.isArray(doc.Statement)) return false;
  return doc.Statement.some(
    (s) =>
      s &&
      s.Effect === "Allow" &&
      asArray(s.Action).length > 0 &&
      asArray(s.Resource).length > 0,
  );
}

// Render a preset to a policy document.
export function presetToPolicy({ scope = "all", buckets = [], level = "read" } = {}) {
  return {
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Action: LEVEL_ACTIONS[level] || LEVEL_ACTIONS.read,
        Resource: scopeResources(scope, buckets),
      },
    ],
  };
}

// Build a doc from an explicit Advanced selection (a set of exact actions + a scope).
export function advancedToPolicy({ scope = "all", buckets = [], actions = [] } = {}) {
  return {
    Version: "2012-10-17",
    Statement: [
      { Effect: "Allow", Action: actions, Resource: scopeResources(scope, buckets) },
    ],
  };
}

// Best-effort recovery of a preset from a doc. Returns { recognized, scope, buckets, level }.
export function policyToPreset(doc) {
  const fail = { recognized: false };
  if (!doc || !Array.isArray(doc.Statement) || doc.Statement.length !== 1) return fail;
  const s = doc.Statement[0];
  if (s.Effect !== "Allow") return fail;
  const actions = asArray(s.Action);
  const resources = asArray(s.Resource);

  let level = null;
  if (sameSet(actions, LEVEL_ACTIONS.full)) level = "full";
  else if (sameSet(actions, LEVEL_ACTIONS.write)) level = "write";
  else if (sameSet(actions, LEVEL_ACTIONS.read)) level = "read";
  if (!level) return fail;

  // No resources = "specific" scope with nothing picked yet (grants nothing). Keep it recognized so
  // the builder reflects the empty selection instead of falling into the read-only custom mode.
  if (resources.length === 0)
    return { recognized: true, scope: "specific", buckets: [], level };

  if (sameSet(resources, ["arn:aws:s3:::*"]))
    return { recognized: true, scope: "all", buckets: [], level };

  const set = new Set(resources);
  const buckets = [];
  for (const r of resources) {
    const m = /^arn:aws:s3:::([^/]+)$/.exec(r);
    if (m && set.has(`arn:aws:s3:::${m[1]}/*`)) buckets.push(m[1]);
  }
  if (buckets.length && sameSet(resources, bucketResources(buckets)))
    return { recognized: true, scope: "specific", buckets, level };
  return fail;
}

// Parse + validate raw policy JSON. { ok, doc } or { ok:false, error }. This is intentionally strict:
// it only accepts the array-of-statement shape the backend's parse_user_policy consumes. The
// single-object Statement form (Statement: { ... }) is rejected here so the user finds out in the
// editor, not after a confusing server error.
export function validate(text) {
  let doc;
  try {
    doc = JSON.parse(text);
  } catch (e) {
    return { ok: false, error: `Invalid JSON: ${e.message}` };
  }
  if (typeof doc !== "object" || doc === null || Array.isArray(doc))
    return { ok: false, error: "Policy must be a JSON object." };

  const stmt = doc.Statement;
  if (!Array.isArray(stmt))
    return {
      ok: false,
      error:
        typeof stmt === "object" && stmt !== null
          ? "Statement must be an array of statements, not a single object. Wrap it in [ ]."
          : "Policy needs a Statement array.",
    };
  if (stmt.length === 0) return { ok: false, error: "Statement array cannot be empty." };

  for (let i = 0; i < stmt.length; i++) {
    const s = stmt[i];
    const at = `Statement[${i}]`;
    if (typeof s !== "object" || s === null || Array.isArray(s))
      return { ok: false, error: `${at} must be an object.` };
    if (s.Effect !== "Allow" && s.Effect !== "Deny")
      return { ok: false, error: `${at}.Effect must be "Allow" or "Deny".` };
    if (s.Action === undefined && s.NotAction === undefined)
      return { ok: false, error: `${at} needs an Action.` };
    if (s.Resource === undefined && s.NotResource === undefined)
      return { ok: false, error: `${at} needs a Resource.` };
    for (const key of ["Action", "NotAction", "Resource", "NotResource"]) {
      if (s[key] === undefined) continue;
      const vals = asArray(s[key]);
      if (vals.length === 0 || vals.some((v) => typeof v !== "string"))
        return { ok: false, error: `${at}.${key} must be a string or array of strings.` };
    }
  }
  return { ok: true, doc };
}

export const pretty = (doc) => JSON.stringify(doc, null, 2);
