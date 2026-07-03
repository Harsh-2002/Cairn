// Per-bucket settings: versioning, quota, compression, bucket policy,
// replication, and the read-only configured-aspects list. Renders inside the
// BucketDetail layout (which owns the <Page> column), one bordered Card per
// concern with its action in the footer — the settings-page idiom.

import { useEffect, useId, useState, type ReactNode } from "react";
import { useParams } from "react-router";
import {
  Bell,
  CalendarClock,
  CircleAlert,
  Globe,
  Plus,
  ShieldCheck,
  Trash2,
  X,
} from "lucide-react";
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
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Tabs,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { CorsCard } from "@/components/cors-card";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { ErrorAlert } from "@/components/error-alert";
import { FieldError } from "@/components/field-error";
import { JsonEditor } from "@/components/json-editor";
import { LifecycleCard } from "@/components/lifecycle-card";
import { NotificationsCard } from "@/components/notifications-card";
import { ObjectLockCard } from "@/components/object-lock-card";
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
// CORS and lifecycle now have their own editable cards; ACL stays read-only (it is off by
// default under the recommended BucketOwnerEnforced ownership, where the policy engine governs).
const ASPECTS: [keyof AspectsSource, string][] = [["acl", "ACL"]];

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
        <CardTitle as="h3" className="flex items-center gap-2 text-base">
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
  const [sseRequired, setSseRequired] = useState(false);
  const [policyText, setPolicyText] = useState("");
  const [policyError, setPolicyError] = useState("");
  const [replTargetArn, setReplTargetArn] = useState("");
  const [replPrefix, setReplPrefix] = useState("");
  const [replExisting, setReplExisting] = useState(false);
  const [replDeleteMarkers, setReplDeleteMarkers] = useState(false);
  const [replError, setReplError] = useState("");
  const [busy, setBusy] = useState<string | null>(null); // which card is saving
  // Settings are grouped into tabs so the operator faces one concern at a time, not a long wall of
  // cards. Each card is gated by the active tab (the DOM order is preserved; only visibility shifts).
  const [tab, setTab] = useState("general");
  const [confirmDeletePolicy, setConfirmDeletePolicy] = useState(false);
  const [confirmClearRepl, setConfirmClearRepl] = useState(false);

  // Remote replication targets (endpoint + sealed credentials) and the add form.
  const blankTarget: CreateReplicationTargetReq = {
    endpoint: "",
    region: "us-east-1",
    dest_bucket: "",
    access_key: "",
    secret: "",
    ca_cert: "",
    insecure_skip_verify: false,
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
    setSseRequired(d.config.encryption?.required ?? false);
    setReplTargetArn(d.repl?.dest_bucket ?? "");
    setReplPrefix(d.repl?.prefix ?? "");
    setReplExisting(d.repl?.existing_objects ?? false);
    setReplDeleteMarkers(d.repl?.delete_markers ?? false);
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

  // One-click enable from the replication section, where versioning is a prerequisite.
  function enableVersioning() {
    void run("versioning", async () => {
      await api.setVersioning(name, "Enabled");
      setVersioning("Enabled");
      toast.success("Versioning enabled — you can configure replication now.");
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
      await api.setEncryption(name, encryption, sseRequired);
      toast.success(
        sseRequired
          ? encryption === "none"
            ? "Encryption required: uploads must request SSE-S3 or they're refused."
            : "Encryption required: every upload is encrypted (AES-256)."
          : encryption === "none"
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
    if (!replTargetArn) {
      setReplError("Choose a replication target to ship objects to.");
      return;
    }
    if (versioning !== "Enabled") {
      // Replication only works on versioned buckets — catch it before the request so the fix is
      // obvious, rather than letting the rule save and silently replicate nothing.
      setReplError(
        "Turn on versioning for this bucket first (in the General tab) — replication only copies versioned objects.",
      );
      return;
    }
    const dest = data?.targets.find((t) => t.arn === replTargetArn);
    setBusy("replication");
    try {
      await s3.putReplication(name, replTargetArn, replPrefix.trim(), {
        existingObjects: replExisting,
        deleteMarkers: replDeleteMarkers,
      });
      toast.success(
        dest
          ? `Replicating to "${dest.dest_bucket}" @ ${dest.endpoint}.`
          : "Replication rule saved.",
      );
      res.refresh();
    } catch (e) {
      setReplError(errorMessage(e, "Couldn't save the replication rule."));
    } finally {
      setBusy(null);
    }
  }

  function clearReplication() {
    setConfirmClearRepl(false);
    void run("replication", async () => {
      await s3.deleteReplication(name);
      toast.success("Replication rule removed.");
      setReplTargetArn("");
      setReplPrefix("");
      setReplExisting(false);
      setReplDeleteMarkers(false);
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
    const caCert = (f.ca_cert ?? "").trim();
    if (caCert && f.insecure_skip_verify) {
      toast.error("Trust a CA certificate or skip TLS verification — not both.");
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
        ca_cert: caCert || undefined,
        insecure_skip_verify: f.insecure_skip_verify,
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
  // The registered target the active rule ships to (its `<Destination><Bucket>` is the target ARN),
  // for a human-readable "replicating to bucket @ endpoint" instead of the raw ARN.
  const replActiveTarget = repl
    ? targets?.find((t) => t.arn === repl.dest_bucket)
    : undefined;

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
        <Tabs value={tab} onValueChange={setTab}>
          {/* Scroll the tab row within itself on narrow phones (<=360px) instead of leaking overflow
              to the document and forcing a horizontal page scroll (audit 2026-07). */}
          <TabsList className="w-full max-w-full justify-start overflow-x-auto">
            <TabsTrigger value="general">General</TabsTrigger>
            <TabsTrigger value="protection">Data protection</TabsTrigger>
            <TabsTrigger value="access">Access</TabsTrigger>
            <TabsTrigger value="integrations">Integrations</TabsTrigger>
          </TabsList>
          <div className="mt-4 space-y-4">
          {/* ---- Versioning ---- */}
          {tab === "general" && (
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
          )}

          {/* ---- Quota ---- */}
          {tab === "general" && (
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
          )}

          {/* ---- Compression ---- */}
          {tab === "general" && (
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
          )}

          {/* ---- Replication ---- */}
          {tab === "integrations" && (
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
                Continuously copy new objects to a remote target (configured
                below). Needs versioning enabled on this bucket.
                {repl
                  ? ` Currently replicating to ${
                      replActiveTarget
                        ? `"${replActiveTarget.dest_bucket}" @ ${replActiveTarget.endpoint}`
                        : repl.dest_bucket
                    }${repl.prefix ? ` (prefix "${repl.prefix}")` : ""}.`
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
                  disabled={busy === "resync" || !repl?.existing_objects}
                  aria-busy={busy === "resync" || undefined}
                  title={
                    repl?.existing_objects
                      ? "Enqueue the objects already in this bucket for replication"
                      : "Enable “Replicate existing objects” on the rule and save first"
                  }
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
                  disabled={
                    busy === "replication" ||
                    !targets?.length ||
                    versioning !== "Enabled"
                  }
                  title={
                    versioning !== "Enabled"
                      ? "Enable versioning first — replication only works on versioned buckets"
                      : undefined
                  }
                >
                  {busy === "replication" ? "Saving…" : "Save"}
                </Button>
              </>
            }
          >
            <CardContent className="space-y-3">
              {versioning !== "Enabled" ? (
                <Alert>
                  <CircleAlert aria-hidden="true" />
                  <AlertTitle>Versioning is required for replication</AlertTitle>
                  <AlertDescription>
                    <p>
                      Replication copies object <em>versions</em>, so it only
                      works on a versioned bucket — this applies to both new
                      writes and "Resync existing". Turn on versioning here,
                      then add a rule.
                    </p>
                    <Button
                      size="sm"
                      variant="outline"
                      className="mt-1"
                      onClick={enableVersioning}
                      disabled={busy === "versioning"}
                      aria-busy={busy === "versioning" || undefined}
                    >
                      {busy === "versioning" ? "Enabling…" : "Enable versioning"}
                    </Button>
                  </AlertDescription>
                </Alert>
              ) : null}
              {targets && targets.length > 0 ? (
                <>
                  <div className="flex flex-wrap gap-2">
                    <Select
                      value={replTargetArn}
                      onValueChange={(v) => {
                        setReplTargetArn(v);
                        setReplError("");
                      }}
                    >
                      <SelectTrigger
                        className="w-full sm:w-72"
                        aria-label="Replication target"
                        aria-invalid={replError ? true : undefined}
                      >
                        <SelectValue placeholder="Choose a target…" />
                      </SelectTrigger>
                      <SelectContent>
                        {targets.map((t) => (
                          <SelectItem key={t.arn} value={t.arn}>
                            {t.dest_bucket} @ {t.endpoint}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Input
                      value={replPrefix}
                      placeholder="Prefix (optional)"
                      autoComplete="off"
                      aria-label="Replication prefix"
                      className="w-full font-mono sm:w-44"
                      onChange={(e) => setReplPrefix(e.target.value)}
                    />
                  </div>
                  <div className="flex flex-col gap-2 pt-0.5">
                    <label className="flex items-start gap-3">
                      <Checkbox
                        checked={replExisting}
                        onCheckedChange={(v) => setReplExisting(v === true)}
                        aria-label="Replicate existing objects"
                        className="mt-0.5"
                      />
                      <span>
                        <span className="block text-sm">
                          Replicate existing objects
                        </span>
                        <span className="block text-[13px] text-muted-foreground">
                          Backfill objects already in this bucket — required
                          before "Resync existing" can run. New writes always
                          replicate regardless.
                        </span>
                      </span>
                    </label>
                    <label className="flex items-start gap-3">
                      <Checkbox
                        checked={replDeleteMarkers}
                        onCheckedChange={(v) => setReplDeleteMarkers(v === true)}
                        aria-label="Replicate delete markers"
                        className="mt-0.5"
                      />
                      <span>
                        <span className="block text-sm">
                          Replicate delete markers
                        </span>
                        <span className="block text-[13px] text-muted-foreground">
                          Propagate deletes to the destination. Off by default —
                          a delete on this bucket then leaves the target copy
                          intact.
                        </span>
                      </span>
                    </label>
                  </div>
                  <FieldError>{replError || null}</FieldError>
                </>
              ) : (
                <p className="text-[13px] text-muted-foreground">
                  Add a replication target below first — the rule ships objects
                  to a target you configure, not to a bare bucket name.
                </p>
              )}
            </CardContent>
          </SettingsCard>
          )}

          {/* ---- Replication targets (remote endpoints + sealed credentials) ---- */}
          {tab === "integrations" && (
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
                          {t.has_ca_cert ? " · custom CA" : ""}
                          {t.insecure_skip_verify ? " · TLS verify off" : ""}
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
                {/* Transport security for an https:// destination — grouped in its own
                    hairline-separated section (not lumped with the credentials), and always
                    visible so it's discoverable. Harmless/ignored for an http:// endpoint. */}
                <div className="mt-1 grid gap-3 border-t pt-4 md:col-span-2">
                  <p className="text-[13px] font-medium text-foreground">
                    Transport security
                    <span className="ml-1.5 font-normal text-muted-foreground">
                      — for an https:// endpoint
                    </span>
                  </p>
                  <div className="grid gap-1.5">
                    <Label htmlFor={`${quotaId}-ca`}>
                      CA certificate{" "}
                      <span className="font-normal text-muted-foreground">
                        — optional
                      </span>
                    </Label>
                    <Textarea
                      id={`${quotaId}-ca`}
                      value={targetForm.ca_cert ?? ""}
                      rows={6}
                      spellCheck={false}
                      disabled={targetForm.insecure_skip_verify}
                      // A truncated-but-real-shaped PEM sample — reads instantly as
                      // "a certificate goes here" and teaches the expected armored
                      // form (a common mistake is pasting the base64 without the
                      // BEGIN/END lines). The instruction proper lives in the helper
                      // text below, so the placeholder stays an example, not a sentence.
                      // Kept to three short lines so it never wraps and loses its shape.
                      placeholder={
                        "-----BEGIN CERTIFICATE-----\n" +
                        "MIIDkTCCAnmgAwIBAgIU…\n" +
                        "-----END CERTIFICATE-----"
                      }
                      // Cap the height so pasting a full PEM chain scrolls inside the box instead of
                      // ballooning the card and pushing the Add-target action off-screen (audit
                      // 2026-07). field-sizing-fixed keeps rows authoritative over auto-grow.
                      // tracking-wider: Geist Mono's hyphen sits mid-cap-height, so the "-----"
                      // PEM armor runs blur into the adjacent letters at 13–14px and read as a
                      // strikethrough (and throw the base64 lines out of left-alignment). A hair
                      // of letter-spacing separates the glyphs — armor renders crisp and aligned,
                      // for both the placeholder and a real pasted certificate.
                      className="field-sizing-fixed max-h-56 resize-y overflow-auto font-mono text-[13px] leading-relaxed tracking-wider disabled:opacity-50"
                      onChange={(e) =>
                        setTargetForm({ ...targetForm, ca_cert: e.target.value })
                      }
                    />
                    <p className="text-[13px] text-muted-foreground">
                      Paste the peer's certificate (PEM) when it's signed by a
                      private or self-signed CA. Leave empty to trust the public
                      certificate authorities.
                    </p>
                  </div>
                  <label className="flex items-start gap-3">
                    <Checkbox
                      checked={targetForm.insecure_skip_verify ?? false}
                      onCheckedChange={(v) =>
                        setTargetForm({
                          ...targetForm,
                          insecure_skip_verify: v === true,
                        })
                      }
                      aria-label="Skip TLS certificate verification"
                      className="mt-0.5"
                    />
                    <span>
                      <span className="block text-sm">
                        Skip certificate verification
                      </span>
                      <span className="block text-[13px] text-muted-foreground">
                        Accepts any certificate — for testing a self-signed
                        endpoint only. Prefer pasting the CA above.
                      </span>
                    </span>
                  </label>
                </div>
              </div>
            </CardContent>
          </SettingsCard>
          )}

          {/* ---- Encryption at rest ---- */}
          {tab === "protection" && (
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
            <CardContent className="space-y-4">
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
              <label className="flex items-start gap-3">
                <Checkbox
                  checked={sseRequired}
                  onCheckedChange={(v) => setSseRequired(v === true)}
                  aria-label="Require encryption"
                />
                <span className="text-sm">
                  <span className="font-medium">Require encryption</span>
                  <span className="block text-muted-foreground">
                    Refuse any upload that would store a plaintext object. With AES-256 selected,
                    header-less uploads are encrypted automatically; with encryption Off, clients
                    must send their own SSE header or the upload is rejected. Replicated objects are
                    always encrypted, never refused.
                  </span>
                </span>
              </label>
            </CardContent>
          </SettingsCard>
          )}

          {/* ---- Bucket policy ---- */}
          {tab === "access" && (
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
          )}

          {/* ---- Object ownership ---- */}
          {tab === "access" && (
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
          )}

          {/* ---- Public Access Block ---- */}
          {tab === "access" && (
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
          )}

          {/* ---- Bucket tags ---- */}
          {tab === "general" && (
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
              <div className="flex items-center gap-3">
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
                {bucketTags.length > 0 ? (
                  <span className="text-[13px] text-muted-foreground tabular-nums">
                    {bucketTags.length} / 50
                  </span>
                ) : null}
              </div>
            </CardContent>
          </SettingsCard>
          )}

          {/* ---- Event notifications (webhooks) ---- */}
          {tab === "integrations" && (
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
          )}

          {/* ---- Object Lock (WORM) ---- */}
          {tab === "protection" && (
            <SettingsCard
            title={
              <>
                <ShieldCheck aria-hidden="true" className="size-4" /> Object Lock
              </>
            }
            description="Write-once-read-many retention. Enabled at bucket creation; the default retention is stamped onto new objects."
            footer={null}
          >
            <ObjectLockCard bucket={name} />
          </SettingsCard>
          )}

          {/* ---- CORS ---- */}
          {tab === "access" && (
            <SettingsCard
            title={
              <>
                <Globe aria-hidden="true" className="size-4" /> CORS
              </>
            }
            description="Allow browsers from other origins to call this bucket directly."
            footer={null}
          >
            <CorsCard bucket={name} />
          </SettingsCard>
          )}

          {/* ---- Lifecycle ---- */}
          {tab === "protection" && (
            <SettingsCard
            title={
              <>
                <CalendarClock aria-hidden="true" className="size-4" /> Lifecycle
              </>
            }
            description="Expire objects and noncurrent versions, and abort stale multipart uploads."
            footer={null}
          >
            <LifecycleCard bucket={name} />
          </SettingsCard>
          )}

          {/* ---- Configured aspects (read-only: ACL) ---- */}
          {tab === "access" && (
            <SettingsCard
            title="Other S3 aspects"
            description="ACLs are configured through the S3 API and shown here for reference."
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
          )}
          </div>
        </Tabs>
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
