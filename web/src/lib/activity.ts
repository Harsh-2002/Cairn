// Plain-language labels for the machine action codes recorded in the activity log.
// The console's whole job is to make S3 concepts legible, so the audit trail should
// read in the operator's voice ("Deleted bucket"), not the wire's (`DeleteBucket`).
// The raw code stays available on hover for experts who grep by it.

const ACTION_LABELS: Record<string, string> = {
  CreateBucket: "Created bucket",
  DeleteBucket: "Deleted bucket",
  SetVersioning: "Changed versioning",
  SetBucketCompression: "Changed compression",
  SetBucketEncryption: "Changed encryption",
  SetBucketNotifications: "Changed notifications",
  SetBucketQuota: "Changed bucket quota",
  PutBucketPolicy: "Updated bucket policy",
  DeleteBucketPolicy: "Removed bucket policy",
  AddReplicationTarget: "Added replication target",
  DeleteReplicationTarget: "Removed replication target",
  CreateUser: "Created user",
  UpdateUser: "Updated user",
  SetUserPolicy: "Changed user policy",
  DeleteUserPolicy: "Removed user policy",
  SetUserQuota: "Changed user quota",
  RotateCredentials: "Rotated credentials",
  MintSessionCredential: "Minted session credential",
  RevokeSessionCredential: "Revoked session credential",
  CreateShare: "Created share",
  RevokeShare: "Revoked share",
  PutObject: "Uploaded object",
  DeleteObject: "Deleted object",
  DeleteObjects: "Deleted objects",
  DeletePrefix: "Deleted folder",
  CreateMultipartUpload: "Started multipart upload",
};

// Actions that destroy a bucket or object — the events an auditor scans for. These
// (and only these) carry the destructive cue; the verb "Deleted" already conveys it
// in text, so the colour is reinforcement, never the sole signal (WCAG, Sam).
const DESTRUCTIVE_ACTIONS = new Set([
  "DeleteBucket",
  "DeleteObject",
  "DeleteObjects",
  "DeletePrefix",
]);

/**
 * "Deleted bucket" for `DeleteBucket`. Falls back to de-camel-casing an unmapped or
 * future code into a sentence-cased phrase ("Set bucket lifecycle"), so the column
 * never regresses to a raw enum even when a new action ships before this map does.
 */
export function actionLabel(action: string): string {
  const known = ACTION_LABELS[action];
  if (known) return known;
  const words = action
    .replace(/([a-z\d])([A-Z])/g, "$1 $2") // fooBar -> foo Bar
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2") // ABCFoo -> ABC Foo
    .toLowerCase()
    .trim();
  return words ? words.charAt(0).toUpperCase() + words.slice(1) : action;
}

/** True for the bucket/object-destroying actions that earn the destructive cue. */
export function isDestructiveAction(action: string): boolean {
  return DESTRUCTIVE_ACTIONS.has(action);
}
