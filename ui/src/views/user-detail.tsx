// A user's detail page: identity + S3 access key, active toggle, credential
// rotation, and the attached identity policy edited via the PermissionBuilder.
// Created users are S3-API-only.

import { useEffect, useRef, useState } from "react";
import { NavLink, useParams } from "react-router";
import { CircleAlert, ShieldOff } from "lucide-react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { CopyField } from "@/components/copy-field";
import { CredentialsPanel } from "@/components/credentials-panel";
import { EmptyState } from "@/components/empty-state";
import { Page } from "@/components/page-header";
import { PermissionBuilder } from "@/components/permission-builder";
import { api, errorMessage } from "@/lib/api";
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
        <Alert variant="destructive" role="alert" className="mb-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Could not load this user</AlertTitle>
          <AlertDescription>
            {res.error}
            <Button
              variant="outline"
              size="sm"
              onClick={res.refresh}
              className="mt-2"
            >
              Try again
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      {res.loading ? (
        <div className="space-y-4">
          <Skeleton className="h-8 w-56" />
          <Skeleton className="h-40 w-full" />
          <Skeleton className="h-64 w-full" />
        </div>
      ) : user ? (
        <div className="space-y-5">
          <header className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h1 className="text-xl font-semibold tracking-tight">
                {user.display_name}
              </h1>
              <p className="mt-1.5 flex flex-wrap items-center gap-1.5">
                <Badge variant="outline">{user.role}</Badge>
                {user.is_active ? (
                  <Badge variant="outline">Active</Badge>
                ) : (
                  <Badge variant="outline" className="text-muted-foreground">
                    Inactive
                  </Badge>
                )}
                <span className="text-[13px] text-muted-foreground">
                  Signs in to the S3 API only
                </span>
              </p>
            </div>
            <div className="flex shrink-0 gap-2">
              <Button
                variant="outline"
                disabled={busyConfirm}
                onClick={() =>
                  user.is_active ? setConfirming("deactivate") : setActive(true)
                }
              >
                {user.is_active ? "Deactivate user" : "Activate user"}
              </Button>
              <Button variant="outline" onClick={() => setConfirming("rotate")}>
                Rotate Bearer secret
              </Button>
            </div>
          </header>

          <Card className="gap-4 rounded-lg shadow-none">
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

          <Card className="gap-4 rounded-lg shadow-none">
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
                      variant="outline"
                      className="text-destructive"
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
