// Per-bucket settings: versioning, quota, compression, bucket policy,
// replication, and the read-only configured-aspects list. Renders inside the
// BucketDetail layout (which owns the <Page> column), one bordered Card per
// concern with its action in the footer — the settings-page idiom.

import { useEffect, useId, useState } from "react";
import { useParams } from "react-router";
import { CircleAlert } from "lucide-react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import { JsonEditor } from "@/components/json-editor";
import { ApiError, api, errorMessage } from "@/lib/api";
import { bytes } from "@/lib/format";
import { pretty, validate } from "@/lib/policy";
import * as s3 from "@/lib/s3";
import { useResource } from "@/lib/use-resource";
import type { ReplicationRule } from "@/lib/types";

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

const ASPECTS: [keyof AspectsSource, string][] = [
  ["cors", "CORS"],
  ["tagging", "Tagging"],
  ["lifecycle", "Lifecycle"],
  ["acl", "ACL"],
  ["public_access_block", "Public access block"],
];

interface AspectsSource {
  cors: unknown | null;
  tagging: unknown | null;
  lifecycle: unknown | null;
  acl: unknown | null;
  public_access_block: unknown | null;
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
    return { config, compression, repl };
  }, [name]);

  // Per-card editable state, seeded from the loaded snapshot.
  const [versioning, setVersioning] = useState("Unversioned");
  const [quotaInput, setQuotaInput] = useState("");
  const [quotaError, setQuotaError] = useState("");
  const [compression, setCompression] = useState("none");
  const [policyText, setPolicyText] = useState("");
  const [policyError, setPolicyError] = useState("");
  const [replDest, setReplDest] = useState("");
  const [replPrefix, setReplPrefix] = useState("");
  const [replError, setReplError] = useState("");
  const [busy, setBusy] = useState<string | null>(null); // which card is saving
  const [confirmDeletePolicy, setConfirmDeletePolicy] = useState(false);
  const [confirmClearRepl, setConfirmClearRepl] = useState(false);

  useEffect(() => {
    const d = res.data;
    if (!d) return;
    setVersioning(statusFromState(d.config.versioning));
    setQuotaInput(d.config.quota_bytes == null ? "" : String(d.config.quota_bytes));
    setPolicyText(d.config.policy ? JSON.stringify(d.config.policy, null, 2) : "");
    setCompression(d.compression);
    setReplDest(d.repl?.dest_bucket ?? "");
    setReplPrefix(d.repl?.prefix ?? "");
    setQuotaError("");
    setPolicyError("");
    setReplError("");
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

  if (res.loading) {
    return (
      <div className="space-y-4" aria-busy="true">
        <span className="visually-hidden" role="status">
          Loading bucket settings…
        </span>
        {[0, 1, 2].map((i) => (
          <Card key={i} className="rounded-lg p-5 shadow-none">
            <Skeleton className="mb-3 h-5 w-36" />
            <Skeleton className="mb-2 h-4 w-2/3" />
            <Skeleton className="h-9 w-64" />
          </Card>
        ))}
      </div>
    );
  }

  if (res.error || !res.data) {
    return (
      <Alert variant="destructive" role="alert">
        <CircleAlert aria-hidden="true" />
        <AlertTitle>Could not load bucket settings</AlertTitle>
        <AlertDescription>
          {res.error ?? "Unknown error."}
          <Button variant="outline" size="sm" onClick={res.refresh} className="mt-2">
            Try again
          </Button>
        </AlertDescription>
      </Alert>
    );
  }

  const { config, repl } = res.data;

  return (
    <div className="space-y-4">
      <h2 className="visually-hidden">Settings for {name}</h2>

      {/* ---- Versioning ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="text-base">Versioning</CardTitle>
          <CardDescription>
            Keep previous versions of an object when it is overwritten or
            deleted, so you can recover them later.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Select value={versioning} onValueChange={setVersioning}>
            <SelectTrigger className="w-56" aria-label="Versioning state">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="Enabled">Enabled</SelectItem>
              <SelectItem value="Suspended">Suspended</SelectItem>
              <SelectItem value="Unversioned">Unversioned</SelectItem>
            </SelectContent>
          </Select>
        </CardContent>
        <CardFooter className="justify-end border-t pt-4!">
          <Button onClick={saveVersioning} disabled={busy === "versioning"}>
            {busy === "versioning" ? "Saving…" : "Save"}
          </Button>
        </CardFooter>
      </Card>

      {/* ---- Quota ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="text-base">Storage quota</CardTitle>
          <CardDescription>
            Cap how much this bucket can store, in bytes. Leave empty for no
            limit.
            {config.quota_bytes != null
              ? ` Current limit: ${bytes(config.quota_bytes)}.`
              : ""}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-1.5">
          <Label htmlFor={quotaId} className="visually-hidden">
            Quota in bytes
          </Label>
          <Input
            id={quotaId}
            value={quotaInput}
            placeholder="No limit"
            inputMode="numeric"
            autoComplete="off"
            className="w-56 font-mono"
            onChange={(e) => {
              setQuotaInput(e.target.value);
              setQuotaError("");
            }}
            aria-invalid={quotaError ? true : undefined}
            aria-describedby={quotaError ? `${quotaId}-err` : `${quotaId}-hint`}
          />
          {quotaError ? (
            <p id={`${quotaId}-err`} role="alert" className="text-[13px] text-destructive">
              {quotaError}
            </p>
          ) : (
            <p id={`${quotaId}-hint`} className="text-[13px] text-muted-foreground">
              {/^\d+$/.test(quotaInput.trim())
                ? `= ${bytes(Number(quotaInput.trim()))}`
                : "Whole bytes, e.g. 10737418240 for 10 GiB."}
            </p>
          )}
        </CardContent>
        <CardFooter className="justify-end gap-2 border-t pt-4!">
          {config.quota_bytes != null ? (
            <Button
              variant="outline"
              onClick={() => void saveQuota(true)}
              disabled={busy === "quota"}
            >
              Remove quota
            </Button>
          ) : null}
          <Button onClick={() => void saveQuota()} disabled={busy === "quota"}>
            {busy === "quota" ? "Saving…" : "Set quota"}
          </Button>
        </CardFooter>
      </Card>

      {/* ---- Compression ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="text-base">Compression</CardTitle>
          <CardDescription>
            Compress new uploads at rest to save space. Existing objects are not
            changed.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Select value={compression} onValueChange={setCompression}>
            <SelectTrigger className="w-56" aria-label="Compression algorithm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="zstd">Zstandard (zstd)</SelectItem>
              <SelectItem value="lz4">LZ4</SelectItem>
              <SelectItem value="none">Off</SelectItem>
            </SelectContent>
          </Select>
        </CardContent>
        <CardFooter className="justify-end border-t pt-4!">
          <Button onClick={saveCompression} disabled={busy === "compression"}>
            {busy === "compression" ? "Saving…" : "Save"}
          </Button>
        </CardFooter>
      </Card>

      {/* ---- Replication ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            Replication
            {repl ? (
              <Badge variant="outline">Active</Badge>
            ) : (
              <Badge variant="outline" className="text-muted-foreground">
                Off
              </Badge>
            )}
          </CardTitle>
          <CardDescription>
            Continuously copy new objects to another bucket. Needs versioning
            enabled and a matching destination configured on the server.
            {repl
              ? ` Currently replicating to "${repl.dest_bucket}"${repl.prefix ? ` (prefix "${repl.prefix}")` : ""}.`
              : ""}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-1.5">
          <div className="flex flex-wrap gap-2">
            <Input
              value={replDest}
              placeholder="Destination bucket"
              autoComplete="off"
              aria-label="Replication destination bucket"
              aria-invalid={replError ? true : undefined}
              className="w-56 font-mono"
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
              className="w-44 font-mono"
              onChange={(e) => setReplPrefix(e.target.value)}
            />
          </div>
          {replError ? (
            <p role="alert" className="text-[13px] text-destructive">
              {replError}
            </p>
          ) : null}
        </CardContent>
        <CardFooter className="justify-end gap-2 border-t pt-4!">
          {repl ? (
            <Button
              variant="outline"
              onClick={() => setConfirmClearRepl(true)}
              disabled={busy === "replication"}
            >
              Remove rule
            </Button>
          ) : null}
          <Button onClick={() => void saveReplication()} disabled={busy === "replication"}>
            {busy === "replication" ? "Saving…" : "Save"}
          </Button>
        </CardFooter>
      </Card>

      {/* ---- Encryption (informational) ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            Encryption at rest
            <Badge variant="outline" className="text-muted-foreground">
              Per upload
            </Badge>
          </CardTitle>
          <CardDescription>
            New uploads are encrypted with a server-managed key (SSE-S3) when
            requested. Set it per file from the Browser tab with the “Encrypt at
            rest” option, or send the
            <code className="mx-1 font-mono text-[12px]">
              x-amz-server-side-encryption
            </code>
            header from any S3 client.
          </CardDescription>
        </CardHeader>
      </Card>

      {/* ---- Bucket policy ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="text-base">Bucket policy</CardTitle>
          <CardDescription>
            A JSON document that grants or denies access to this bucket and its
            objects. Bucket policies need a <code className="font-mono text-[12px]">Principal</code> per
            statement. If you would rather not write JSON, the Users page has a
            visual permission builder that writes per-user policies for you.
          </CardDescription>
        </CardHeader>
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
              No policy set. Paste a policy document, or use “Insert example” to
              start from a template.
            </p>
          ) : null}
        </CardContent>
        <CardFooter className="justify-end gap-2 border-t pt-4!">
          <Button
            variant="outline"
            className="text-destructive"
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
        </CardFooter>
      </Card>

      {/* ---- Configured aspects (read-only) ---- */}
      <Card className="gap-4 rounded-lg shadow-none">
        <CardHeader>
          <CardTitle className="text-base">Other S3 aspects</CardTitle>
          <CardDescription>
            Aspects configured through the S3 API are shown here for reference.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <dl className="grid grid-cols-2 gap-x-6 gap-y-3 sm:grid-cols-3">
            <div>
              <dt className="text-[13px] text-muted-foreground">Ownership mode</dt>
              <dd className="mt-0.5 text-sm">{config.ownership_mode}</dd>
            </div>
            {ASPECTS.map(([key, label]) => (
              <div key={key}>
                <dt className="text-[13px] text-muted-foreground">{label}</dt>
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
      </Card>

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
    </div>
  );
}
