// A user's detail page: identity + S3 access key, active toggle, credential
// rotation, and the attached identity policy edited via the PermissionBuilder.
// Created users are S3-API-only.

import { useEffect, useId, useRef, useState } from "react";
import { NavLink, useParams } from "react-router";
import { ShieldOff } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/primitives/badge";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/primitives/breadcrumb";
import { Button } from "@/components/primitives/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/primitives/card";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";
import { Skeleton } from "@/components/primitives/skeleton";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { CopyField } from "@/components/copy-field";
import { CredentialsPanel } from "@/components/credentials-panel";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page } from "@/components/page-header";
import { PermissionBuilder } from "@/components/permission-builder";
import { StatusBadge } from "@/components/status-badge";
import { api, errorMessage } from "@/lib/api";
import { bytes } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import type { RotateCredentialsResp } from "@/lib/types";
import type { PolicyDoc } from "@/lib/policy";

type Confirming = "deactivate" | "rotate" | "remove-policy" | null;

export function UserDetail() {
  const { id = "" } = useParams<{ id: string }>();
  const res = useResource(() => api.getUser(id), [id]);
  const user = res.data;

  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketsLoading, setBucketsLoading] = useState(true);
  useEffect(() => {
    let alive = true;
    api
      .listBuckets()
      .then((r) => {
        if (alive) setBuckets(r.buckets.map((b) => b.name));
      })
      .catch(() => {})
      .finally(() => {
        if (alive) setBucketsLoading(false);
      });
    return () => {
      alive = false;
    };
  }, []);

  // doc from the builder; null = invalid JSON or grants nothing.
  const [doc, setDoc] = useState<PolicyDoc | null>(null);
  const [saving, setSaving] = useState(false);
  const [confirming, setConfirming] = useState<Confirming>(null);
  const [busyConfirm, setBusyConfirm] = useState(false);
  const [rotated, setRotated] = useState<RotateCredentialsResp | null>(null);
  // Reveals the builder on a user that has no policy yet.
  const [attaching, setAttaching] = useState(false);

  // Storage quota, edited in GiB (operators don't think in bytes). Seeded from
  // the loaded value and re-seeded when the server value changes.
  const GIB = 1024 ** 3;
  const quotaId = useId();
  const [quotaGiB, setQuotaGiB] = useState("");
  const [savingQuota, setSavingQuota] = useState(false);
  useEffect(() => {
    if (!user) return;
    setQuotaGiB(user.quota_bytes != null ? String(user.quota_bytes / GIB) : "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.id, user?.quota_bytes]);

  async function saveQuota(clear: boolean) {
    let next: number | null = null;
    if (!clear) {
      const g = Number.parseFloat(quotaGiB);
      if (!Number.isFinite(g) || g <= 0) {
        toast.error("Enter a quota in GiB, or remove the limit.");
        return;
      }
      next = Math.round(g * GIB);
    }
    setSavingQuota(true);
    try {
      await api.setUserQuota(id, next);
      toast.success(clear ? "Quota removed" : "Quota updated");
      res.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to update the quota."));
    } finally {
      setSavingQuota(false);
    }
  }

  // Bring a fresh one-time secret into view so it is not scrolled off unnoticed.
  const rotatedRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!rotated || !rotatedRef.current) return;
    const reduce = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    rotatedRef.current.scrollIntoView({
      block: "center",
      behavior: reduce ? "auto" : "smooth",
    });
  }, [rotated]);

  async function setActive(next: boolean) {
    // Busy-guarded: keep the confirm dialog open (and disabled) until the PATCH
    // settles, so a fast double-click can't fire two requests and the action
    // shows an in-flight state.
    setBusyConfirm(true);
    try {
      await api.patchUser(id, { is_active: next });
      setConfirming(null);
      toast.success(next ? "User activated" : "User deactivated");
      res.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to update the user."));
    } finally {
      setBusyConfirm(false);
    }
  }

  async function rotate() {
    setBusyConfirm(true);
    try {
      const r = await api.rotateCredentials(id);
      setConfirming(null);
      setRotated(r);
      toast.success("New Bearer secret created");
    } catch (e) {
      toast.error(errorMessage(e, "Failed to rotate credentials."));
    } finally {
      setBusyConfirm(false);
    }
  }

  async function savePolicy() {
    if (doc === null) {
      toast.error("Fix the policy before saving.");
      return;
    }
    setSaving(true);
    try {
      await api.setUserPolicy(id, JSON.stringify(doc));
      toast.success("Policy saved");
      setAttaching(false);
      res.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to save the policy."));
    } finally {
      setSaving(false);
    }
  }

  async function removePolicy() {
    setBusyConfirm(true);
    try {
      await api.deleteUserPolicy(id);
      setConfirming(null);
      toast.success("Policy removed");
      setDoc(null);
      res.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to remove the policy."));
    } finally {
      setBusyConfirm(false);
    }
  }

  return (
    <Page>
      <Breadcrumb className="mb-3">
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink asChild>
              <NavLink to="/users">Users</NavLink>
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage>{user ? user.display_name : id}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>

      {res.error ? (
        <ErrorAlert
          title="Could not load this user"
          message={res.error}
          onRetry={res.refresh}
        />
      ) : null}

      {res.loading ? (
        <UserDetailSkeleton />
      ) : user ? (
        <div className="space-y-5">
          <header className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h1 className="text-xl font-semibold tracking-tight">
                {user.display_name}
              </h1>
              <p className="mt-1.5 flex flex-wrap items-center gap-1.5">
                <Badge variant="outline">{user.role}</Badge>
                <StatusBadge tone={user.is_active ? "positive" : "neutral"}>
                  {user.is_active ? "Active" : "Inactive"}
                </StatusBadge>
                <span className="text-[13px] text-muted-foreground">
                  Signs in to the S3 API only
                </span>
              </p>
            </div>
            <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:shrink-0">
              <Button
                variant={user.is_active ? "destructive-outline" : "outline"}
                className="w-full sm:w-auto"
                disabled={busyConfirm}
                onClick={() =>
                  user.is_active ? setConfirming("deactivate") : setActive(true)
                }
              >
                {user.is_active ? "Deactivate user" : "Activate user"}
              </Button>
              <Button
                variant="outline"
                className="w-full sm:w-auto"
                onClick={() => setConfirming("rotate")}
              >
                Rotate Bearer secret
              </Button>
            </div>
          </header>

          <Card className="gap-4">
            <CardHeader>
              <CardTitle className="text-base">Access keys</CardTitle>
              <CardDescription>
                The <strong>S3 access key</strong> signs requests from S3 tools
                and SDKs. The <strong>Bearer access key</strong> is for the
                management API and CLI. Both identify the same user.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              {user.sigv4_access_key_id ? (
                <CopyField
                  label="S3 access key ID"
                  value={user.sigv4_access_key_id}
                />
              ) : (
                <p className="text-[13px] text-muted-foreground">
                  No S3 access key — this user predates S3 key minting.
                </p>
              )}
              <CopyField label="Bearer access key ID" value={user.access_key_id} />

              {rotated ? (
                <div
                  ref={rotatedRef}
                  className="rounded-lg border-2 border-warning p-4"
                >
                  <CredentialsPanel
                    title="New Bearer secret created"
                    fields={[
                      {
                        label: "Bearer access key ID",
                        value: rotated.bearer_access_key_id,
                      },
                      {
                        label: "Bearer secret (shown once)",
                        value: rotated.bearer_secret,
                        secret: true,
                      },
                    ]}
                    doneLabel="Done — I saved it"
                    onDone={() => setRotated(null)}
                  />
                </div>
              ) : null}
            </CardContent>
          </Card>

          <Card className="gap-4">
            <CardHeader>
              <CardTitle className="text-base">Storage quota</CardTitle>
              <CardDescription>
                Cap the total bytes this user's uploads may consume across all
                buckets. Leave empty for no limit.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap items-end gap-2">
                <div className="grid gap-1.5">
                  <Label htmlFor={quotaId}>Quota (GiB)</Label>
                  <Input
                    id={quotaId}
                    type="number"
                    min="0"
                    step="any"
                    inputMode="decimal"
                    placeholder="No limit"
                    value={quotaGiB}
                    onChange={(e) => setQuotaGiB(e.target.value)}
                    className="w-full sm:w-44"
                  />
                </div>
                <Button
                  onClick={() => saveQuota(false)}
                  disabled={savingQuota}
                  aria-busy={savingQuota || undefined}
                >
                  {savingQuota ? "Saving…" : "Save quota"}
                </Button>
                {user.quota_bytes != null ? (
                  <Button
                    variant="outline"
                    onClick={() => saveQuota(true)}
                    disabled={savingQuota}
                  >
                    Remove limit
                  </Button>
                ) : null}
              </div>
              <p className="mt-2 text-[13px] text-muted-foreground">
                Current limit:{" "}
                {user.quota_bytes != null ? bytes(user.quota_bytes) : "No limit"}
              </p>
            </CardContent>
          </Card>

          <Card className="gap-4">
            <CardHeader>
              <CardTitle className="text-base">Access policy</CardTitle>
              <CardDescription>
                Choose which buckets this user can reach and what they can do in
                them.
              </CardDescription>
            </CardHeader>
            {user.policy || attaching ? (
              <>
                <CardContent>
                  <PermissionBuilder
                    key={user.id}
                    buckets={buckets}
                    bucketsLoading={bucketsLoading}
                    initial={user.policy}
                    onChange={setDoc}
                  />
                </CardContent>
                <CardFooter className="justify-end gap-2 border-t pt-4!">
                  {user.policy ? (
                    <Button
                      variant="destructive-outline"
                      disabled={saving}
                      onClick={() => setConfirming("remove-policy")}
                    >
                      Remove policy
                    </Button>
                  ) : null}
                  <Button
                    onClick={savePolicy}
                    disabled={saving || doc === null}
                    aria-busy={saving || undefined}
                  >
                    {saving ? "Saving…" : "Save policy"}
                  </Button>
                </CardFooter>
              </>
            ) : (
              <CardContent>
                <EmptyState
                  icon={ShieldOff}
                  title="No policy attached"
                  body="Without a policy this user cannot access any bucket."
                  action={
                    <Button onClick={() => setAttaching(true)}>
                      Attach policy
                    </Button>
                  }
                />
              </CardContent>
            )}
          </Card>
        </div>
      ) : null}

      <ConfirmDialog
        open={confirming === "deactivate"}
        onOpenChange={(o) => !o && setConfirming(null)}
        destructive
        busy={busyConfirm}
        title="Deactivate this user?"
        description="Deactivating blocks this user's S3 access immediately. You can reactivate them later."
        confirmLabel="Deactivate user"
        cancelLabel="Keep active"
        onConfirm={() => setActive(false)}
      />
      <ConfirmDialog
        open={confirming === "rotate"}
        onOpenChange={(o) => !o && setConfirming(null)}
        destructive
        busy={busyConfirm}
        title="Rotate the Bearer secret?"
        description="Rotating stops the current key working immediately. Anything using the old secret will need the new one. The new secret is shown only once."
        confirmLabel="Rotate secret"
        cancelLabel="Keep current secret"
        onConfirm={rotate}
      />
      <ConfirmDialog
        open={confirming === "remove-policy"}
        onOpenChange={(o) => !o && setConfirming(null)}
        destructive
        busy={busyConfirm}
        title="Remove this policy?"
        description="Removing the policy revokes this user's access to all buckets. They keep their keys but can do nothing until a new policy is attached."
        confirmLabel="Remove policy"
        cancelLabel="Keep policy"
        onConfirm={removePolicy}
      />
    </Page>
  );
}

/** First-paint skeletons mirroring the real header + three cards so the
    layout doesn't jump when the user arrives. */
function UserDetailSkeleton() {
  return (
    <div className="space-y-5" aria-hidden="true">
      <p className="sr-only" role="status">
        Loading user…
      </p>
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-2">
          <Skeleton className="h-7 w-48" />
          <Skeleton className="h-5 w-64" />
        </div>
        <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:shrink-0">
          <Skeleton className="h-9 w-full sm:w-36" />
          <Skeleton className="h-9 w-full sm:w-44" />
        </div>
      </header>
      {[0, 1, 2].map((i) => (
        <Card key={i} className="gap-4">
          <CardHeader>
            <Skeleton className="h-5 w-32" />
            <Skeleton className="h-4 w-full max-w-md" />
          </CardHeader>
          <CardContent className="space-y-3">
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-3/4" />
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
