// Users are S3-API-only credentials scoped by an access policy. The root admin
// is the sole admin, so there is no role selector here. Create mints an S3
// (SigV4) key/secret shown exactly once and attaches the policy built with the
// PermissionBuilder.

import { useEffect, useId, useState } from "react";
import { Plus, Users as UsersIcon } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/primitives/badge";
import { Button } from "@/components/primitives/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/primitives/dialog";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";
import { Checkbox } from "@/components/primitives/checkbox";
import { TableCell, TableRow } from "@/components/primitives/table";
import { BulkBar } from "@/components/bulk-bar";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { CredentialsPanel } from "@/components/credentials-panel";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { useBulkSelection } from "@/hooks/use-bulk-selection";
import { ErrorAlert } from "@/components/error-alert";
import { FieldError } from "@/components/field-error";
import { Page, PageHeader } from "@/components/page-header";
import { PermissionBuilder } from "@/components/permission-builder";
import { StatusBadge } from "@/components/status-badge";
import { TextLink } from "@/components/text-link";
import { api, errorMessage } from "@/lib/api";
import { useResource } from "@/lib/use-resource";
import type { CreateUserResp } from "@/lib/types";
import type { PolicyDoc } from "@/lib/policy";

const COLUMNS: Column[] = [
  { key: "name", label: "Name" },
  { key: "key", label: "Access key ID" },
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
          `${dn} was created, but the policy didn't attach: ${errorMessage(pe, "the request was rejected.")} You can add it from the user's page.`,
        );
      }
      toast.success(`Created ${dn}.`);
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

  // ---- bulk selection ------------------------------------------------------
  const sel = useBulkSelection();
  const allIds = list.map((u) => u.id);
  const allSelected = allIds.length > 0 && allIds.every((id) => sel.has(id));
  const someSelected = sel.count > 0 && !allSelected;
  const [bulkBusy, setBulkBusy] = useState(false);
  const [confirmBulk, setConfirmBulk] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const columns: Column[] = [
    {
      key: "select",
      className: "w-10",
      label: (
        <Checkbox
          checked={allSelected ? true : someSelected ? "indeterminate" : false}
          onCheckedChange={(v) => sel.setAll(allIds, v === true)}
          aria-label="Select all users"
        />
      ),
    },
    ...COLUMNS,
  ];

  async function confirmBulkDeactivate() {
    if (sel.count === 0 || bulkBusy) return;
    setBulkBusy(true);
    const targets = [...sel.selected];
    let ok = 0;
    let failed = 0;
    for (const id of targets) {
      try {
        await api.patchUser(id, { is_active: false });
        ok++;
      } catch {
        failed++;
      }
    }
    if (failed === 0) {
      toast.success(`Deactivated ${ok} user${ok === 1 ? "" : "s"}.`);
    } else {
      toast.error(`Deactivated ${ok}, ${failed} failed.`);
    }
    sel.clear();
    setConfirmBulk(false);
    setBulkBusy(false);
    users.refresh();
  }

  async function confirmBulkDelete() {
    if (sel.count === 0 || bulkBusy) return;
    setBulkBusy(true);
    const targets = [...sel.selected];
    let ok = 0;
    const failures: string[] = [];
    for (const id of targets) {
      try {
        await api.deleteUser(id);
        ok++;
      } catch (e) {
        // Each guard (root / last admin / yourself / owns buckets) returns a plain-language reason.
        failures.push(errorMessage(e, "This user couldn't be deleted."));
      }
    }
    if (failures.length === 0) {
      toast.success(`Deleted ${ok} user${ok === 1 ? "" : "s"}.`);
    } else if (ok === 0) {
      // Nothing deleted — surface the reason so a blocked delete is actionable.
      toast.error(failures[0]);
    } else {
      toast.error(
        `Deleted ${ok}. ${failures.length} couldn't be deleted: ${failures[0]}`,
      );
    }
    sel.clear();
    setConfirmDelete(false);
    setBulkBusy(false);
    users.refresh();
  }

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
                  />
                  {/* role="alert" (inside FieldError) announces the message; it
                      renders nothing while nameError is empty. */}
                  <FieldError>{nameError}</FieldError>
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
                  <Button
                    type="submit"
                    disabled={!canCreate}
                    aria-busy={creating || undefined}
                  >
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
        <DataTable columns={columns} minWidth={600}>
          <SkeletonRows
            rows={3}
            widths={["w-4", "w-32", "w-44", "w-16", "w-14"]}
          />
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
        <>
          <BulkBar count={sel.count} onClear={sel.clear}>
            <Button
              variant="destructive-outline"
              size="sm"
              disabled={bulkBusy}
              onClick={() => setConfirmBulk(true)}
            >
              Deactivate selected
            </Button>
            <Button
              variant="destructive"
              size="sm"
              disabled={bulkBusy}
              onClick={() => setConfirmDelete(true)}
            >
              Delete selected
            </Button>
          </BulkBar>
          <DataTable columns={columns} minWidth={600}>
            {list.map((u) => (
              <TableRow
                key={u.id}
                data-state={sel.has(u.id) ? "selected" : undefined}
              >
                <TableCell>
                  <Checkbox
                    checked={sel.has(u.id)}
                    onCheckedChange={() => sel.toggle(u.id)}
                    aria-label={`Select ${u.display_name}`}
                  />
                </TableCell>
                <TableCell>
                  <TextLink to={`/users/${encodeURIComponent(u.id)}`}>
                    {u.display_name}
                  </TextLink>
                </TableCell>
              <TableCell
                data-label="Access key ID"
                className="font-mono text-[13px]"
              >
                {u.access_key_id}
              </TableCell>
              <TableCell data-label="Role">
                <Badge variant="outline">{u.role}</Badge>
              </TableCell>
              <TableCell data-label="Status">
                <StatusBadge tone={u.is_active ? "positive" : "neutral"}>
                  {u.is_active ? "Active" : "Inactive"}
                </StatusBadge>
              </TableCell>
            </TableRow>
          ))}
          </DataTable>
        </>
      ) : null}

      <ConfirmDialog
        open={confirmBulk}
        onOpenChange={(open) => {
          if (!open && !bulkBusy) setConfirmBulk(false);
        }}
        title={`Deactivate ${sel.count} user${sel.count === 1 ? "" : "s"}`}
        description={`This disables ${sel.count === 1 ? "this user's" : "these users'"} S3 access keys. You can reactivate ${sel.count === 1 ? "the user" : "them"} later from their page.`}
        confirmLabel={bulkBusy ? "Deactivating…" : "Deactivate selected"}
        cancelLabel="Keep active"
        busy={bulkBusy}
        onConfirm={() => void confirmBulkDeactivate()}
      />

      <ConfirmDialog
        open={confirmDelete}
        onOpenChange={(open) => {
          if (!open && !bulkBusy) setConfirmDelete(false);
        }}
        title={`Delete ${sel.count} user${sel.count === 1 ? "" : "s"}`}
        description={
          <>
            This permanently deletes {sel.count === 1 ? "this user" : "these users"} and{" "}
            <strong>immediately revokes</strong>{" "}
            {sel.count === 1 ? "its" : "their"} access keys, sessions, and policy. It can't be
            undone. The root administrator, the last administrator, the user you're signed in as, and
            anyone who still owns buckets can't be deleted — those are skipped with a reason.
          </>
        }
        confirmLabel={bulkBusy ? "Deleting…" : "Delete permanently"}
        cancelLabel="Cancel"
        destructive
        busy={bulkBusy}
        onConfirm={() => void confirmBulkDelete()}
      />
    </Page>
  );
}
