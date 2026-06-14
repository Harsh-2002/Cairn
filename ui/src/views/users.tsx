// Users are S3-API-only credentials scoped by an access policy. The root admin
// is the sole admin, so there is no role selector here. Create mints an S3
// (SigV4) key/secret shown exactly once and attaches the policy built with the
// PermissionBuilder.

import { useEffect, useId, useState } from "react";
import { NavLink } from "react-router";
import { Plus, Users as UsersIcon } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { TableCell, TableRow } from "@/components/ui/table";
import { CredentialsPanel } from "@/components/credentials-panel";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { PermissionBuilder } from "@/components/permission-builder";
import { StatusBadge } from "@/components/status-badge";
import { api, errorMessage } from "@/lib/api";
import { useResource } from "@/lib/use-resource";
import type { CreateUserResp } from "@/lib/types";
import type { PolicyDoc } from "@/lib/policy";

const COLUMNS: Column[] = [
  { key: "name", label: "Name" },
  { key: "key", label: "Bearer key" },
  { key: "role", label: "Role" },
  { key: "status", label: "Status" },
];

export function Users() {
  const nameId = useId();
  const users = useResource(() => api.listUsers(), []);

  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketsLoading, setBucketsLoading] = useState(true);
  useEffect(() => {
    let alive = true;
    api
      .listBuckets()
      .then((r) => {
        if (alive) setBuckets(r.buckets.map((b) => b.name));
      })
      .catch(() => {
        /* the builder shows its own empty state */
      })
      .finally(() => {
        if (alive) setBucketsLoading(false);
      });
    return () => {
      alive = false;
    };
  }, []);

  const [open, setOpen] = useState(false);
  const [displayName, setDisplayName] = useState("");
  const [nameTouched, setNameTouched] = useState(false);
  // doc from the builder; null = invalid JSON or grants nothing.
  const [doc, setDoc] = useState<PolicyDoc | null>(null);
  const [creating, setCreating] = useState(false);
  // One-time credentials; while set, the dialog refuses to dismiss.
  const [created, setCreated] = useState<CreateUserResp | null>(null);

  const nameError =
    nameTouched && !displayName.trim() ? "Enter a display name." : "";
  const canCreate = !!displayName.trim() && doc !== null && !creating;

  function resetForm() {
    setDisplayName("");
    setNameTouched(false);
    setDoc(null);
  }

  function onOpenChange(next: boolean) {
    // The credentials are shown exactly once: only the explicit Done button
    // (behind the saved acknowledgement) may close the dialog.
    if (!next && created) return;
    setOpen(next);
    if (!next) resetForm();
  }

  async function create(e: React.FormEvent) {
    e.preventDefault();
    setNameTouched(true);
    const dn = displayName.trim();
    if (!dn || doc === null || creating) return;
    setCreating(true);
    try {
      const res = await api.createUser(dn); // member; S3-only
      try {
        await api.setUserPolicy(res.id, JSON.stringify(doc));
      } catch (pe) {
        toast.error(
          `The user was created, but attaching the policy failed: ${errorMessage(pe, "unknown error")}. Attach it from the user's page.`,
        );
      }
      toast.success(`Created ${dn}`);
      setCreated(res);
      users.refresh();
    } catch (err) {
      toast.error(errorMessage(err, "Failed to create the user."));
    } finally {
      setCreating(false);
    }
  }

  function done() {
    setCreated(null);
    setOpen(false);
    resetForm();
  }

  const list = users.data?.users ?? [];

  return (
    <Page>
      <PageHeader
        title="Users"
        description="S3-API access keys, each scoped by an access policy."
        actions={
          <Button onClick={() => setOpen(true)}>
            <Plus aria-hidden="true" /> Create user
          </Button>
        }
      />

      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent
          className="max-h-[85vh] overflow-y-auto sm:max-w-3xl lg:max-w-5xl"
          showCloseButton={!created}
          onInteractOutside={(e) => {
            if (created) e.preventDefault();
          }}
          onEscapeKeyDown={(e) => {
            if (created) e.preventDefault();
          }}
        >
          {created ? (
            <>
              {/* Radix requires a title for the dialog's accessible name. */}
              <DialogHeader className="sr-only">
                <DialogTitle>One-time credentials</DialogTitle>
                <DialogDescription>
                  Save the new user's credentials before closing.
                </DialogDescription>
              </DialogHeader>
              <CredentialsPanel
                fields={[
                  { label: "S3 access key ID", value: created.s3_access_key_id },
                  {
                    label: "S3 secret key",
                    value: created.s3_secret_key,
                    secret: true,
                  },
                  {
                    label: "Bearer token (management API)",
                    value: `${created.bearer_access_key_id}.${created.bearer_secret}`,
                    secret: true,
                  },
                ]}
                explainer={
                  <>
                    Most clients use the <strong>S3 key and secret</strong> — any
                    S3 tool (boto3, aws-cli, rclone) signs with them against this
                    server. The <strong>Bearer token</strong> is an alternative
                    for Cairn's own API and tools; it cannot open this console,
                    which is admin-only.
                  </>
                }
                onDone={done}
              />
            </>
          ) : (
            <>
              <DialogHeader>
                <DialogTitle>Create user</DialogTitle>
                <DialogDescription>
                  New users can only call the S3 API with their keys. They cannot
                  sign in to this console.
                </DialogDescription>
              </DialogHeader>
              <form onSubmit={create} className="space-y-5">
                <div className="space-y-1.5">
                  <Label htmlFor={nameId}>Display name</Label>
                  <Input
                    id={nameId}
                    value={displayName}
                    placeholder="e.g. backup-bot"
                    autoComplete="off"
                    onChange={(e) => setDisplayName(e.target.value)}
                    onBlur={() => setNameTouched(true)}
                    aria-invalid={nameError ? true : undefined}
                    aria-describedby={nameError ? `${nameId}-err` : undefined}
                  />
                  {nameError ? (
                    <p
                      id={`${nameId}-err`}
                      role="alert"
                      className="text-[13px] text-destructive"
                    >
                      {nameError}
                    </p>
                  ) : null}
                </div>

                <div className="space-y-1.5">
                  <p className="text-sm font-medium">Access policy</p>
                  <p className="text-[13px] text-muted-foreground">
                    Choose what this user's S3 credentials can do. The Builder
                    uses plain choices; switch to Split or Code to edit the
                    policy JSON directly.
                  </p>
                  <PermissionBuilder
                    buckets={buckets}
                    bucketsLoading={bucketsLoading}
                    onChange={setDoc}
                  />
                </div>

                {doc === null ? (
                  <p className="text-[13px] text-destructive" role="alert">
                    This policy grants nothing or the JSON is invalid. Fix it
                    above before creating the user.
                  </p>
                ) : null}

                <div className="flex justify-end gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => onOpenChange(false)}
                  >
                    Cancel
                  </Button>
                  <Button type="submit" disabled={!canCreate}>
                    {creating ? "Creating…" : "Create user"}
                  </Button>
                </div>
              </form>
            </>
          )}
        </DialogContent>
      </Dialog>

      {users.error ? (
        <ErrorAlert
          title="Could not load users"
          message={users.error}
          onRetry={users.refresh}
        />
      ) : null}

      {users.loading ? (
        <DataTable columns={COLUMNS} minWidth={560}>
          <SkeletonRows rows={3} widths={["w-32", "w-44", "w-16", "w-14"]} />
        </DataTable>
      ) : list.length === 0 && !users.error ? (
        <EmptyState
          icon={UsersIcon}
          title="No users yet"
          body="Create one to mint an S3 access key scoped to the buckets and actions you choose."
          action={
            <Button onClick={() => setOpen(true)}>
              <Plus aria-hidden="true" /> Create user
            </Button>
          }
        />
      ) : list.length > 0 ? (
        <DataTable columns={COLUMNS} minWidth={560}>
          {list.map((u) => (
            <TableRow key={u.id}>
              <TableCell>
                <NavLink
                  to={`/users/${encodeURIComponent(u.id)}`}
                  className="text-link hover:underline underline-offset-4"
                >
                  {u.display_name}
                </NavLink>
              </TableCell>
              <TableCell className="font-mono text-[13px]">
                {u.access_key_id}
              </TableCell>
              <TableCell>
                <Badge variant="outline">{u.role}</Badge>
              </TableCell>
              <TableCell>
                <StatusBadge tone={u.is_active ? "positive" : "neutral"}>
                  {u.is_active ? "Active" : "Inactive"}
                </StatusBadge>
              </TableCell>
            </TableRow>
          ))}
        </DataTable>
      ) : null}
    </Page>
  );
}
