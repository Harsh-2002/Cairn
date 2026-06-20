// Per-bucket settings: versioning, quota, compression, bucket policy,
// replication, and the read-only configured-aspects list. Renders inside the
// BucketDetail layout (which owns the <Page> column), one bordered Card per
// concern with its action in the footer — the settings-page idiom.

import { useEffect, useId, useState, type ReactNode } from "react";
import { useParams } from "react-router";
import { Bell, CircleAlert, Plus, Trash2, X } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { ErrorAlert } from "@/components/error-alert";
import { FieldError } from "@/components/field-error";
import { JsonEditor } from "@/components/json-editor";
import { NotificationsCard } from "@/components/notifications-card";
import { StatusBadge } from "@/components/status-badge";
import { ApiError, api, errorMessage } from "@/lib/api";
import { bytes, count } from "@/lib/format";
import { pretty, validate } from "@/lib/policy";
import * as s3 from "@/lib/s3";
import { useResource } from "@/lib/use-resource";
import { cn } from "@/lib/utils";
import type {
  CreateReplicationTargetReq,
  ReplicationRule,
  ReplicationStatusResp,
  ReplicationTarget,
  WebhookEndpointView,
} from "@/lib/types";

const EXAMPLE_POLICY = pretty({
  Version: "2012-10-17",
  Statement: [
    {
      Sid: "AllowPublicRead",
      Effect: "Allow",
      Principal: "*",
      Action: ["s3:GetObject"],
      Resource: ["arn:aws:s3:::BUCKET/*"],
    },
  ],
});

// Map the server's lowercase versioning string to the capitalized form the
// PUT /versioning endpoint expects.
function statusFromState(s: string): string {
  if (s === "enabled") return "Enabled";
  if (s === "suspended") return "Suspended";
  return "Unversioned";
}

// CORS / Lifecycle / ACL remain read-only here (set via the S3 API); ownership,
// public access block, and bucket tags have dedicated editors below.
const ASPECTS: [keyof AspectsSource, string][] = [
  ["cors", "CORS"],
  ["lifecycle", "Lifecycle"],
  ["acl", "ACL"],
];

const PAB_TOGGLES: {
  key: keyof import("@/lib/s3").PublicAccessBlock;
  label: string;
  hint: string;
}[] = [
  {
    key: "blockPublicAcls",
    label: "Block public ACLs",
    hint: "Reject new public ACLs on the bucket and objects.",
  },
  {
    key: "ignorePublicAcls",
    label: "Ignore public ACLs",
    hint: "Ignore any existing public ACL grants.",
  },
  {
    key: "blockPublicPolicy",
    label: "Block public policy",
    hint: "Reject bucket policies that grant public access.",
  },
  {
    key: "restrictPublicBuckets",
    label: "Restrict public buckets",
    hint: "Limit access to authenticated principals when a public policy exists.",
  },
];

interface AspectsSource {
  cors: unknown | null;
  tagging: unknown | null;
  lifecycle: unknown | null;
  acl: unknown | null;
  public_access_block: unknown | null;
}

// Single-source the settings-card chrome: the `gap-4` Card, the `text-base`
// CardTitle, the optional description, and the `border-t pt-4` footer that the
// ~11 cards below all share. Each card supplies only its title, body, and
// footer actions; pass `footer={null}` for a card with no action row.
function SettingsCard({
  title,
  description,
  headerExtra,
  children,
  footer,
  footerClassName,
}: {
  title: ReactNode;
  description?: ReactNode;
  headerExtra?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  footerClassName?: string;
}) {
  return (
    <Card className="gap-4">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          {title}
        </CardTitle>
        {description ? <CardDescription>{description}</CardDescription> : null}
        {headerExtra}
      </CardHeader>
      {children}
      {footer !== undefined && footer !== null ? (
        <CardFooter
          className={cn("justify-end border-t pt-4!", footerClassName)}
        >
          {footer}
        </CardFooter>
      ) : null}
    </Card>
  );
}

export function BucketSettings() {
  const { name = "" } = useParams<{ name: string }>();
  const quotaId = useId();

  const res = useResource(async () => {
    const config = await api.getBucketConfig(name);
    let compression = "none";
    try {
      const detail = await api.getBucket(name);
      compression = (detail.compression as string | null) || "none";
    } catch {
      /* compression stays "none" */
    }
    let repl: ReplicationRule | null = null;
    try {
      repl = await s3.getReplication(name);
    } catch {
      /* treated as no rule */
    }
    let targets: ReplicationTarget[] = [];
    try {
      targets = (await api.listReplicationTargets(name)).targets;
    } catch {
      /* no targets / endpoint unavailable */
    }
    let replStatus: ReplicationStatusResp | null = null;
    try {
      replStatus = await api.replicationStatus(name);
    } catch {
      /* status unavailable */
    }
    let pab: s3.PublicAccessBlock | null = null;
    try {
      pab = await s3.getPublicAccessBlock(name);
    } catch {
      /* unset / unavailable */
    }
    let bucketTags: s3.ObjectTag[] = [];
    try {
      bucketTags = await s3.getBucketTagging(name);
    } catch {
      /* none */
    }
    let notifications: WebhookEndpointView[] = [];
    try {
      notifications = (await api.getNotifications(name)).endpoints;
    } catch {
      /* none / unavailable */
    }
    return {
      config,
      compression,
      repl,
      targets,
      replStatus,
      pab,
      bucketTags,
      notifications,
    };
  }, [name]);

  // Per-card editable state, seeded from the loaded snapshot.
  const [versioning, setVersioning] = useState("Unversioned");
  const [quotaInput, setQuotaInput] = useState("");
  const [quotaError, setQuotaError] = useState("");
  const [compression, setCompression] = useState("none");
  const [encryption, setEncryption] = useState("none");
  const [policyText, setPolicyText] = useState("");
  const [policyError, setPolicyError] = useState("");
  const [replDest, setReplDest] = useState("");
  const [replPrefix, setReplPrefix] = useState("");
  const [replError, setReplError] = useState("");
  const [busy, setBusy] = useState<string | null>(null); // which card is saving
  const [confirmDeletePolicy, setConfirmDeletePolicy] = useState(false);
  const [confirmClearRepl, setConfirmClearRepl] = useState(false);

  // Remote replication targets (endpoint + sealed credentials) and the add form.
  const blankTarget: CreateReplicationTargetReq = {
    endpoint: "",
    region: "us-east-1",
    dest_bucket: "",
    access_key: "",
    secret: "",
  };
  const [targetForm, setTargetForm] = useState(blankTarget);
  const [addingTarget, setAddingTarget] = useState(false);
  const [confirmDeleteTarget, setConfirmDeleteTarget] = useState<string | null>(
    null,
  );

  // Object Ownership (kebab-case from the API ↔ PascalCase S3 ObjectOwnership).
  const OWNERSHIP_TO_S3: Record<string, string> = {
    "bucket-owner-enforced": "BucketOwnerEnforced",
    "bucket-owner-preferred": "BucketOwnerPreferred",
    "object-writer": "ObjectWriter",
  };
  const [ownership, setOwnership] = useState("BucketOwnerEnforced");

  // Public Access Block toggles + bucket-level tags.
  const PAB_OFF: s3.PublicAccessBlock = {
    blockPublicAcls: false,
    ignorePublicAcls: false,
    blockPublicPolicy: false,
    restrictPublicBuckets: false,
  };
  const [pab, setPab] = useState<s3.PublicAccessBlock>(PAB_OFF);
  const [bucketTags, setBucketTags] = useState<s3.ObjectTag[]>([]);

  useEffect(() => {
    const d = res.data;
    if (!d) return;
    setVersioning(statusFromState(d.config.versioning));
    setQuotaInput(d.config.quota_bytes == null ? "" : String(d.config.quota_bytes));
    setPolicyText(d.config.policy ? JSON.stringify(d.config.policy, null, 2) : "");
    setCompression(d.compression);
    setEncryption(
      d.config.encryption?.algorithm?.toUpperCase() === "AES256" ? "AES256" : "none",
    );
    setReplDest(d.repl?.dest_bucket ?? "");
    setReplPrefix(d.repl?.prefix ?? "");
    setOwnership(OWNERSHIP_TO_S3[d.config.ownership_mode] ?? "BucketOwnerEnforced");
    setPab(d.pab ?? PAB_OFF);
    setBucketTags(d.bucketTags);
    setQuotaError("");
    setPolicyError("");
    setReplError("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [res.data]);

  // Live JSON validity for the policy editor (inline, before save).
  const policyValidation =
    policyText.trim() === "" ? null : validate(policyText);
  const policyEditorError = policyError
    ? policyError
    : policyValidation && !policyValidation.ok
      ? policyValidation.error
      : null;

  async function run(card: string, action: () => Promise<void>) {
    setBusy(card);
    try {
      await action();
      res.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "The change could not be saved."));
    } finally {
      setBusy(null);
    }
  }

  function saveVersioning() {
    void run("versioning", async () => {
      await api.setVersioning(name, versioning);
      toast.success(`Versioning set to ${versioning.toLowerCase()}.`);
    });
  }

  async function saveQuota(clear = false) {
    setQuotaError("");
    const raw = clear ? "" : quotaInput.trim();
    let quota: number | null = null;
    if (raw !== "") {
      if (!/^\d+$/.test(raw)) {
        setQuotaError("Enter a whole number of bytes, or leave empty for no limit.");
        return;
      }
      quota = Number(raw);
      if (!Number.isSafeInteger(quota)) {
        setQuotaError("That number is too large.");
        return;
      }
    }
    if (clear) setQuotaInput("");
    await run("quota", async () => {
      await api.setQuota(name, quota);
      toast.success(quota === null ? "Quota cleared." : `Quota set to ${bytes(quota)}.`);
    });
  }

  function saveCompression() {
    void run("compression", async () => {
      await api.setCompression(name, compression);
      toast.success(
        compression === "none"
          ? "Compression turned off."
          : `Compression set to ${compression}.`,
      );
    });
  }

  function saveEncryption() {
    void run("encryption", async () => {
      await api.setEncryption(name, encryption);
      toast.success(
        encryption === "none"
          ? "New uploads are stored unencrypted."
          : "New uploads will be encrypted (AES-256).",
      );
    });
  }

  async function savePolicy() {
    setPolicyError("");
    const raw = policyText.trim();
    if (raw === "") {
      setPolicyError("The policy is empty. Use Delete policy to remove it.");
      return;
    }
    const v = validate(raw);
    if (!v.ok) {
      setPolicyError(v.error);
      return;
    }
    setBusy("policy");
    try {
      await api.setPolicy(name, raw);
      toast.success("Policy saved.");
      res.refresh();
    } catch (e) {
      if (e instanceof ApiError && e.status === 400) {
        setPolicyError(e.message || "The server rejected this policy as invalid.");
      } else {
        toast.error(errorMessage(e, "Failed to save the policy."));
      }
    } finally {
      setBusy(null);
    }
  }

  function deletePolicy() {
    setConfirmDeletePolicy(false);
    void run("policy", async () => {
      await api.deletePolicy(name);
      toast.success("Policy deleted.");
      setPolicyText("");
    });
  }

  async function saveReplication() {
    setReplError("");
    if (!replDest.trim()) {
      setReplError("Enter a destination bucket to replicate into.");
      return;
    }
    setBusy("replication");
    try {
      await s3.putReplication(name, replDest.trim(), replPrefix.trim());
      toast.success(`Replicating to "${replDest.trim()}".`);
      res.refresh();
    } catch (e) {
      setReplError(
        `${errorMessage(e, "Failed to set replication.")} Replication needs versioning enabled and a matching destination configured on the server.`,
      );
    } finally {
      setBusy(null);
    }
  }

  function clearReplication() {
    setConfirmClearRepl(false);
    void run("replication", async () => {
      await s3.deleteReplication(name);
      toast.success("Replication rule removed.");
      setReplDest("");
      setReplPrefix("");
    });
  }

  async function addTarget() {
    const f = targetForm;
    if (
      !f.endpoint.trim() ||
      !f.region.trim() ||
      !f.dest_bucket.trim() ||
      !f.access_key.trim() ||
      !f.secret
    ) {
      toast.error(
        "Endpoint, region, destination bucket, access key, and secret are all required.",
      );
      return;
    }
    setAddingTarget(true);
    try {
      await api.addReplicationTarget(name, {
        endpoint: f.endpoint.trim(),
        region: f.region.trim(),
        dest_bucket: f.dest_bucket.trim(),
        access_key: f.access_key.trim(),
        secret: f.secret,
      });
      toast.success("Remote target added");
      setTargetForm(blankTarget);
      res.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to add the target."));
    } finally {
      setAddingTarget(false);
    }
  }

  function deleteTarget(arn: string) {
    setConfirmDeleteTarget(null);
    void run("target", async () => {
      await api.deleteReplicationTarget(name, arn);
      toast.success("Remote target removed");
    });
  }

  function retryReplication() {
    void run("retry", async () => {
      const r = await api.retryReplication(name);
      toast.success(
        r.failed_observed > 0
          ? `Requeued ${count(r.failed_observed)} failed object${r.failed_observed === 1 ? "" : "s"}.`
          : "No failed objects to retry.",
      );
    });
  }

  function resyncReplication() {
    void run("resync", async () => {
      await api.resyncReplication(name);
      toast.success("Backfill started for existing objects.");
    });
  }

  function saveOwnership() {
    void run("ownership", async () => {
      await s3.putOwnershipControls(name, ownership);
      toast.success("Object ownership updated.");
    });
  }

  function savePab() {
    void run("pab", async () => {
      await s3.putPublicAccessBlock(name, pab);
      toast.success("Public Access Block updated.");
    });
  }

  function saveBucketTags() {
    void run("buckettags", async () => {
      const cleaned = bucketTags
        .map((t) => ({ key: t.key.trim(), value: t.value }))
        .filter((t) => t.key !== "");
      if (cleaned.length === 0) await s3.deleteBucketTagging(name);
      else await s3.putBucketTagging(name, cleaned);
      toast.success("Bucket tags saved.");
    });
  }

  const data = res.data;
  const { config, repl, targets, replStatus } = data ?? {
    config: undefined,
    repl: undefined,
    targets: undefined,
    replStatus: undefined,
  };

  return (
    <div className="space-y-4">
      <h2 className="sr-only">Settings for {name}</h2>

      {/* Errored refresh shows the alert ABOVE the retained cards (overview
          idiom) rather than tearing the operator's view down. */}
      {res.error ? (
        <ErrorAlert
          title="Could not load bucket settings"
          message={res.error}
          onRetry={res.refresh}
        />
      ) : null}

      {res.loading && !data ? (
        <div aria-busy="true" className="space-y-4">
          <span className="sr-only" role="status">
            Loading bucket settings…
          </span>
          {[0, 1, 2].map((i) => (
            <Card key={i} className="p-5">
              <Skeleton className="mb-3 h-5 w-36" />
              <Skeleton className="mb-2 h-4 w-2/3" />
              <Skeleton className="h-9 w-64" />
            </Card>
          ))}
        </div>
      ) : data && config ? (
        <>
          {/* ---- Versioning ---- */}
          <SettingsCard
            title="Versioning"
            description="Keep previous versions of an object when it is overwritten or deleted, so you can recover them later."
            footer={
              <Button onClick={saveVersioning} disabled={busy === "versioning"}>
                {busy === "versioning" ? "Saving…" : "Save"}
              </Button>
            }
          >
            <CardContent>
              <Select value={versioning} onValueChange={setVersioning}>
                <SelectTrigger
                  className="w-full sm:w-56"
                  aria-label="Versioning state"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="Enabled">Enabled</SelectItem>
                  <SelectItem value="Suspended">Suspended</SelectItem>
                  <SelectItem value="Unversioned">Unversioned</SelectItem>
                </SelectContent>
              </Select>
            </CardContent>
          </SettingsCard>

          {/* ---- Quota ---- */}
          <SettingsCard
            title="Storage quota"
            description={
              <>
                Cap how much this bucket can store, in bytes. Leave empty for no
                limit.
                {config.quota_bytes != null
                  ? ` Current limit: ${bytes(config.quota_bytes)}.`
                  : ""}
              </>
            }
            footerClassName="gap-2"
            footer={
              <>
                {config.quota_bytes != null ? (
                  <Button
                    variant="outline"
                    onClick={() => void saveQuota(true)}
                    disabled={busy === "quota"}
                  >
                    Remove quota
                  </Button>
                ) : null}
                <Button
                  onClick={() => void saveQuota()}
                  disabled={busy === "quota"}
                >
                  {busy === "quota" ? "Saving…" : "Set quota"}
                </Button>
              </>
            }
          >
            <CardContent className="space-y-1.5">
              <Label htmlFor={quotaId} className="sr-only">
                Quota in bytes
              </Label>
              <Input
                id={quotaId}
                value={quotaInput}
                placeholder="No limit"
                inputMode="numeric"
                autoComplete="off"
                className="w-full font-mono sm:w-56"
                onChange={(e) => {
                  setQuotaInput(e.target.value);
                  setQuotaError("");
                }}
                aria-invalid={quotaError ? true : undefined}
                aria-describedby={
                  quotaError ? `${quotaId}-err` : `${quotaId}-hint`
                }
              />
              {quotaError ? (
                <FieldError>
                  <span id={`${quotaId}-err`}>{quotaError}</span>
                </FieldError>
              ) : (
                <p
                  id={`${quotaId}-hint`}
                  className="text-[13px] text-muted-foreground"
                >
                  {/^\d+$/.test(quotaInput.trim())
                    ? `= ${bytes(Number(quotaInput.trim()))}`
                    : "Whole bytes, e.g. 10737418240 for 10 GiB."}
                </p>
              )}
            </CardContent>
          </SettingsCard>

          {/* ---- Compression ---- */}
          <SettingsCard
            title="Compression"
            description="Compress new uploads at rest to save space. Existing objects are not changed."
            footer={
              <Button
                onClick={saveCompression}
                disabled={busy === "compression"}
              >
                {busy === "compression" ? "Saving…" : "Save"}
              </Button>
            }
          >
            <CardContent>
              <Select value={compression} onValueChange={setCompression}>
                <SelectTrigger
                  className="w-full sm:w-56"
                  aria-label="Compression algorithm"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="zstd">Zstandard (zstd)</SelectItem>
                  <SelectItem value="lz4">LZ4</SelectItem>
                  <SelectItem value="none">Off</SelectItem>
                </SelectContent>
              </Select>
            </CardContent>
          </SettingsCard>

          {/* ---- Replication ---- */}
          <SettingsCard
            title={
              <>
                Replication
                <StatusBadge tone={repl ? "positive" : "neutral"}>
                  {repl ? "Active" : "Off"}
                </StatusBadge>
              </>
            }
            description={
              <>
                Continuously copy new objects to another bucket. Needs
                versioning enabled and a remote target (below) for the
                destination.
                {repl
                  ? ` Currently replicating to "${repl.dest_bucket}"${repl.prefix ? ` (prefix "${repl.prefix}")` : ""}.`
                  : ""}
              </>
            }
            headerExtra={
              replStatus &&
              (replStatus.pending > 0 || replStatus.failed > 0) ? (
                <p className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px]">
                  <span className="text-muted-foreground">
                    Pending:{" "}
                    <span className="tabular-nums text-foreground">
                      {count(replStatus.pending)}
                    </span>
                  </span>
                  {replStatus.failed > 0 ? (
                    <span className="flex items-center gap-1 text-destructive">
                      <CircleAlert
                        aria-hidden="true"
                        className="size-3.5 shrink-0"
                      />
                      Failed:{" "}
                      <span className="tabular-nums">
                        {count(replStatus.failed)}
                      </span>
                    </span>
                  ) : null}
                </p>
              ) : null
            }
            footerClassName="flex-wrap gap-2"
            footer={
              <>
                {replStatus && replStatus.failed > 0 ? (
                  <Button
                    variant="outline"
                    onClick={retryReplication}
                    disabled={busy === "retry"}
                    aria-busy={busy === "retry" || undefined}
                  >
                    {busy === "retry" ? "Retrying…" : "Retry failed"}
                  </Button>
                ) : null}
                <Button
                  variant="outline"
                  onClick={resyncReplication}
                  disabled={busy === "resync"}
                  aria-busy={busy === "resync" || undefined}
                  title="Enqueue existing objects for replication (needs ExistingObjectReplication enabled)"
                >
                  {busy === "resync" ? "Starting…" : "Resync existing"}
                </Button>
                {repl ? (
                  <Button
                    variant="outline"
                    onClick={() => setConfirmClearRepl(true)}
                    disabled={busy === "replication"}
                  >
                    Remove rule
                  </Button>
                ) : null}
                <Button
                  onClick={() => void saveReplication()}
                  disabled={busy === "replication"}
                >
                  {busy === "replication" ? "Saving…" : "Save"}
                </Button>
              </>
            }
          >
            <CardContent className="space-y-1.5">
              <div className="flex flex-wrap gap-2">
                <Input
                  value={replDest}
                  placeholder="Destination bucket"
                  autoComplete="off"
                  aria-label="Replication destination bucket"
                  aria-invalid={replError ? true : undefined}
                  className="w-full font-mono sm:w-56"
                  onChange={(e) => {
                    setReplDest(e.target.value);
                    setReplError("");
                  }}
                />
                <Input
                  value={replPrefix}
                  placeholder="Prefix (optional)"
                  autoComplete="off"
                  aria-label="Replication prefix"
                  className="w-full font-mono sm:w-44"
                  onChange={(e) => setReplPrefix(e.target.value)}
                />
              </div>
              <FieldError>{replError || null}</FieldError>
            </CardContent>
          </SettingsCard>

          {/* ---- Replication targets (remote endpoints + sealed credentials) ---- */}
          <SettingsCard
            title="Replication targets"
            description="Remote destinations this bucket can replicate into. Each holds the endpoint and credentials of a bucket on another Cairn (or S3) node; the secret is sealed on the server and never shown again."
            footer={
              <Button onClick={addTarget} disabled={addingTarget}>
                {addingTarget ? "Adding…" : "Add target"}
              </Button>
            }
          >
            <CardContent className="space-y-4">
              {targets.length > 0 ? (
                <ul className="divide-y rounded-lg border">
                  {targets.map((t) => (
                    <li
                      key={t.arn}
                      className="flex flex-wrap items-center justify-between gap-2 px-3 py-2.5"
                    >
                      <div className="min-w-0">
                        <p
                          className="truncate font-mono text-[13px]"
                          title={t.arn}
                        >
                          {t.dest_bucket}{" "}
                          <span className="text-muted-foreground">
                            @ {t.endpoint}
                          </span>
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {t.region} · key {t.access_key_id}
                        </p>
                      </div>
                      <Button
                        variant="destructive-outline"
                        size="icon"
                        aria-label={`Remove target ${t.dest_bucket}`}
                        disabled={busy === "target"}
                        onClick={() => setConfirmDeleteTarget(t.arn)}
                      >
                        <Trash2 aria-hidden="true" />
                      </Button>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-[13px] text-muted-foreground">
                  No remote targets yet. Add one below, then set the replication
                  rule above to its destination bucket.
                </p>
              )}

              <div className="grid gap-3 rounded-lg border p-3 md:grid-cols-2">
                <div className="grid gap-1.5 md:col-span-2">
                  <Label htmlFor={`${quotaId}-ep`}>Endpoint</Label>
                  <Input
                    id={`${quotaId}-ep`}
                    value={targetForm.endpoint}
                    placeholder="https://s3.peer.example.com:7373"
                    autoComplete="off"
                    className="font-mono"
                    onChange={(e) =>
                      setTargetForm({ ...targetForm, endpoint: e.target.value })
                    }
                  />
                </div>
                <div className="grid gap-1.5">
                  <Label htmlFor={`${quotaId}-region`}>Region</Label>
                  <Input
                    id={`${quotaId}-region`}
                    value={targetForm.region}
                    autoComplete="off"
                    className="font-mono"
                    onChange={(e) =>
                      setTargetForm({ ...targetForm, region: e.target.value })
                    }
                  />
                </div>
                <div className="grid gap-1.5">
                  <Label htmlFor={`${quotaId}-db`}>Destination bucket</Label>
                  <Input
                    id={`${quotaId}-db`}
                    value={targetForm.dest_bucket}
                    autoComplete="off"
                    className="font-mono"
                    onChange={(e) =>
                      setTargetForm({
                        ...targetForm,
                        dest_bucket: e.target.value,
                      })
                    }
                  />
                </div>
                <div className="grid gap-1.5">
                  <Label htmlFor={`${quotaId}-ak`}>Access key</Label>
                  <Input
                    id={`${quotaId}-ak`}
                    value={targetForm.access_key}
                    autoComplete="off"
                    className="font-mono"
                    onChange={(e) =>
                      setTargetForm({
                        ...targetForm,
                        access_key: e.target.value,
                      })
                    }
                  />
                </div>
                <div className="grid gap-1.5">
                  <Label htmlFor={`${quotaId}-sk`}>Secret key</Label>
                  <Input
                    id={`${quotaId}-sk`}
                    type="password"
                    value={targetForm.secret}
                    autoComplete="off"
                    className="font-mono"
                    onChange={(e) =>
                      setTargetForm({ ...targetForm, secret: e.target.value })
                    }
                  />
                </div>
              </div>
            </CardContent>
          </SettingsCard>

          {/* ---- Encryption at rest ---- */}
          <SettingsCard
            title={
              <>
                Encryption at rest
                <StatusBadge tone={config.encryption ? "positive" : "neutral"}>
                  {config.encryption ? "AES-256" : "Off"}
                </StatusBadge>
              </>
            }
            description="Encrypt every new upload with a server-managed key (SSE-S3, AES-256). The key never leaves the server and downloads are transparent. Existing objects are not rewritten."
            footer={
              <Button onClick={saveEncryption} disabled={busy === "encryption"}>
                {busy === "encryption" ? "Saving…" : "Save"}
              </Button>
            }
          >
            <CardContent>
              <Select value={encryption} onValueChange={setEncryption}>
                <SelectTrigger
                  className="w-full sm:w-56"
                  aria-label="Default encryption"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="AES256">AES-256 (SSE-S3)</SelectItem>
                  <SelectItem value="none">Off</SelectItem>
                </SelectContent>
              </Select>
            </CardContent>
          </SettingsCard>

          {/* ---- Bucket policy ---- */}
          <SettingsCard
            title="Bucket policy"
            description={
              <>
                A JSON document that grants or denies access to this bucket and
                its objects. Bucket policies need a{" "}
                <code className="font-mono text-[12px]">Principal</code> per
                statement. If you would rather not write JSON, the Users page has
                a visual permission builder that writes per-user policies for
                you.
              </>
            }
            footerClassName="gap-2"
            footer={
              <>
                <Button
                  variant="destructive-outline"
                  onClick={() => setConfirmDeletePolicy(true)}
                  disabled={busy === "policy" || !config.policy}
                >
                  Delete policy
                </Button>
                <Button
                  onClick={() => void savePolicy()}
                  disabled={
                    busy === "policy" ||
                    policyText.trim() === "" ||
                    (policyValidation !== null && !policyValidation.ok)
                  }
                >
                  {busy === "policy" ? "Saving…" : "Save policy"}
                </Button>
              </>
            }
          >
            <CardContent className="space-y-2">
              <div className="flex items-center justify-between">
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setPolicyText(EXAMPLE_POLICY.replace(/BUCKET/g, name));
                    setPolicyError("");
                  }}
                >
                  Insert example
                </Button>
              </div>
              <JsonEditor
                value={policyText}
                onChange={(t) => {
                  setPolicyText(t);
                  setPolicyError("");
                }}
                error={policyText.trim() === "" ? null : policyEditorError}
                label="Bucket policy JSON"
                rows={12}
                validLabel="Valid policy document"
              />
              {policyText.trim() === "" ? (
                <p className="text-[13px] text-muted-foreground">
                  No policy set. Paste a policy document, or use “Insert example”
                  to start from a template.
                </p>
              ) : null}
            </CardContent>
          </SettingsCard>

          {/* ---- Object ownership ---- */}
          <SettingsCard
            title="Object ownership"
            description={
              <>
                Controls whether ACLs apply.{" "}
                <strong>Bucket owner enforced</strong> disables ACLs entirely
                (the safe default); the other modes let object writers set ACLs.
              </>
            }
            footer={
              <Button onClick={saveOwnership} disabled={busy === "ownership"}>
                {busy === "ownership" ? "Saving…" : "Save"}
              </Button>
            }
          >
            <CardContent>
              <Select value={ownership} onValueChange={setOwnership}>
                <SelectTrigger
                  className="w-full sm:w-56"
                  aria-label="Object ownership"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="BucketOwnerEnforced">
                    Bucket owner enforced
                  </SelectItem>
                  <SelectItem value="BucketOwnerPreferred">
                    Bucket owner preferred
                  </SelectItem>
                  <SelectItem value="ObjectWriter">Object writer</SelectItem>
                </SelectContent>
              </Select>
            </CardContent>
          </SettingsCard>

          {/* ---- Public Access Block ---- */}
          <SettingsCard
            title="Public Access Block"
            description="Guardrails that neutralise public access regardless of ACLs or policy. Enabling all four is the safe default."
            footer={
              <Button onClick={savePab} disabled={busy === "pab"}>
                {busy === "pab" ? "Saving…" : "Save"}
              </Button>
            }
          >
            <CardContent className="space-y-3">
              {PAB_TOGGLES.map((t) => (
                <label key={t.key} className="flex items-start gap-3">
                  <Checkbox
                    checked={pab[t.key]}
                    onCheckedChange={(v) =>
                      setPab({ ...pab, [t.key]: v === true })
                    }
                    aria-label={t.label}
                    className="mt-0.5"
                  />
                  <span>
                    <span className="block text-sm">{t.label}</span>
                    <span className="block text-[13px] text-muted-foreground">
                      {t.hint}
                    </span>
                  </span>
                </label>
              ))}
            </CardContent>
          </SettingsCard>

          {/* ---- Bucket tags ---- */}
          <SettingsCard
            title="Bucket tags"
            description="Key-value tags on the bucket, for organisation and policy conditioning."
            footer={
              <Button onClick={saveBucketTags} disabled={busy === "buckettags"}>
                {busy === "buckettags" ? "Saving…" : "Save tags"}
              </Button>
            }
          >
            <CardContent className="space-y-2">
              {bucketTags.length === 0 ? (
                <p className="text-[13px] text-muted-foreground">
                  No tags. Add one below.
                </p>
              ) : (
                bucketTags.map((t, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <Input
                      aria-label={`Tag ${i + 1} key`}
                      placeholder="Key"
                      value={t.key}
                      className="font-mono"
                      onChange={(e) =>
                        setBucketTags((cur) =>
                          cur.map((x, j) =>
                            j === i ? { ...x, key: e.target.value } : x,
                          ),
                        )
                      }
                    />
                    <Input
                      aria-label={`Tag ${i + 1} value`}
                      placeholder="Value"
                      value={t.value}
                      className="font-mono"
                      onChange={(e) =>
                        setBucketTags((cur) =>
                          cur.map((x, j) =>
                            j === i ? { ...x, value: e.target.value } : x,
                          ),
                        )
                      }
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      aria-label={`Remove tag ${i + 1}`}
                      onClick={() =>
                        setBucketTags((cur) => cur.filter((_, j) => j !== i))
                      }
                    >
                      <X aria-hidden="true" />
                    </Button>
                  </div>
                ))
              )}
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={bucketTags.length >= 50}
                onClick={() =>
                  setBucketTags((cur) => [...cur, { key: "", value: "" }])
                }
              >
                <Plus aria-hidden="true" />
                Add tag
              </Button>
            </CardContent>
          </SettingsCard>

          {/* ---- Event notifications (webhooks) ---- */}
          <SettingsCard
            title={
              <>
                <Bell aria-hidden="true" className="size-4" /> Event
                notifications
              </>
            }
            description="POST a JSON event record to a webhook when objects are created or removed."
            footer={null}
          >
            <NotificationsCard
              bucket={name}
              endpoints={data.notifications}
              onChanged={res.refresh}
            />
          </SettingsCard>

          {/* ---- Configured aspects (read-only: CORS / Lifecycle / ACL) ---- */}
          <SettingsCard
            title="Other S3 aspects"
            description="CORS, lifecycle, and ACL are configured through the S3 API and shown here for reference."
            footer={null}
          >
            <CardContent>
              <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
                {ASPECTS.map(([key, label]) => (
                  <div key={key}>
                    <dt className="text-[13px] text-muted-foreground">
                      {label}
                    </dt>
                    <dd className="mt-0.5">
                      {(config as unknown as AspectsSource)[key] ? (
                        <Badge variant="outline">Set</Badge>
                      ) : (
                        <span className="text-sm text-muted-foreground">—</span>
                      )}
                    </dd>
                  </div>
                ))}
              </dl>
            </CardContent>
          </SettingsCard>
        </>
      ) : null}

      <ConfirmDialog
        open={confirmDeletePolicy}
        onOpenChange={setConfirmDeletePolicy}
        destructive
        busy={busy === "policy"}
        title="Delete bucket policy"
        description={`This removes the access policy on "${name}". Access falls back to the default rules until you set a new policy.`}
        confirmLabel="Delete policy"
        cancelLabel="Keep policy"
        onConfirm={deletePolicy}
      />
      <ConfirmDialog
        open={confirmClearRepl}
        onOpenChange={setConfirmClearRepl}
        destructive
        busy={busy === "replication"}
        title="Remove the replication rule"
        description="New objects will stop copying to the destination bucket. Objects already replicated are not touched."
        confirmLabel="Remove rule"
        cancelLabel="Keep replicating"
        onConfirm={clearReplication}
      />
      <ConfirmDialog
        open={confirmDeleteTarget !== null}
        onOpenChange={(o) => !o && setConfirmDeleteTarget(null)}
        destructive
        busy={busy === "target"}
        title="Remove this replication target"
        description="Rules pointing at this destination will stop replicating. The destination bucket and its data are not touched."
        confirmLabel="Remove target"
        cancelLabel="Keep target"
        onConfirm={() =>
          confirmDeleteTarget && deleteTarget(confirmDeleteTarget)
        }
      />
    </div>
  );
}
