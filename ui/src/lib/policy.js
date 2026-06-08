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

const asArray = (v) => (Array.isArray(v) ? v : v == null ? [] : [v]);
const sameSet = (a, b) => {
  const sa = new Set(a);
  const sb = new Set(b);
  return sa.size === sb.size && [...sa].every((x) => sb.has(x));
};

const bucketResources = (buckets) =>
  buckets.flatMap((b) => [`arn:aws:s3:::${b}`, `arn:aws:s3:::${b}/*`]);

// Render a preset to a policy document.
export function presetToPolicy({ scope = "all", buckets = [], level = "read" } = {}) {
  let resources =
    scope === "all" ? ["arn:aws:s3:::*"] : bucketResources(buckets);
  if (resources.length === 0) resources = ["arn:aws:s3:::*"]; // no buckets picked yet
  return {
    Version: "2012-10-17",
    Statement: [
      {
        Effect: "Allow",
        Action: LEVEL_ACTIONS[level] || LEVEL_ACTIONS.read,
        Resource: resources,
      },
    ],
  };
}

// Build a doc from an explicit Advanced selection (a set of exact actions + a scope).
export function advancedToPolicy({ scope = "all", buckets = [], actions = [] } = {}) {
  let resources =
    scope === "all" ? ["arn:aws:s3:::*"] : bucketResources(buckets);
  if (resources.length === 0) resources = ["arn:aws:s3:::*"];
  return {
    Version: "2012-10-17",
    Statement: [{ Effect: "Allow", Action: actions, Resource: resources }],
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

// Parse + lightly validate raw policy JSON. { ok, doc } or { ok:false, error }.
export function validate(text) {
  let doc;
  try {
    doc = JSON.parse(text);
  } catch (e) {
    return { ok: false, error: `Invalid JSON: ${e.message}` };
  }
  if (typeof doc !== "object" || doc === null || Array.isArray(doc))
    return { ok: false, error: "Policy must be a JSON object" };
  const stmt = doc.Statement;
  if (!Array.isArray(stmt) && typeof stmt !== "object")
    return { ok: false, error: "Policy needs a Statement array" };
  return { ok: true, doc };
}

export const pretty = (doc) => JSON.stringify(doc, null, 2);
