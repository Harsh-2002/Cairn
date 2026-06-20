import { useId, useState, type FormEvent } from "react";
import { useNavigate } from "react-router";
import { Database, MoreHorizontal, Plus } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { TableCell, TableRow } from "@/components/ui/table";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { FieldError } from "@/components/field-error";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { TextLink } from "@/components/text-link";
import { TypedConfirmDialog } from "@/components/typed-confirm-dialog";
import { api, ApiError, errorMessage } from "@/lib/api";
import { bytes, count, whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";

const NAME_RULE =
  "3–63 characters: lowercase letters, digits, hyphens, and dots; must start and end with a letter or digit.";

/** S3-style bucket-name check; returns a specific problem or null when valid. */
function nameIssue(name: string): string | null {
  if (name.length < 3 || name.length > 63)
    return "Bucket names must be 3–63 characters long.";
  if (!/^[a-z0-9.-]+$/.test(name))
    return "Only lowercase letters, digits, hyphens, and dots are allowed.";
  if (!/^[a-z0-9]/.test(name) || !/[a-z0-9]$/.test(name))
    return "Names must start and end with a letter or digit.";
  return null;
}

const COLUMNS: Column[] = [
  { key: "name", label: "Name" },
  { key: "objects", label: "Objects", className: "text-right" },
  { key: "size", label: "Size", className: "text-right" },
  { key: "versioning", label: "Versioning" },
  { key: "created", label: "Created" },
  { key: "actions", label: "Actions", srOnly: true },
];

/** Map a bucket's versioning state to a semantic badge tone. */
function versioningTone(state: string): StatusTone {
  if (state === "Enabled") return "positive";
  if (state === "Suspended") return "warning";
  return "neutral";
}

export function Buckets() {
  const navigate = useNavigate();
  // The list plus per-bucket usage (objects + bytes) in one refreshable unit;
  // usage failing alone degrades to em-dashes rather than failing the page.
  const list = useResource(async () => {
    const [l, usage] = await Promise.all([
      api.listBuckets(),
      api.overviewBuckets().catch(() => null),
    ]);
    return {
      buckets: l.buckets,
      usage: new Map((usage?.buckets ?? []).map((u) => [u.name, u])),
    };
  }, []);
  const buckets = list.data?.buckets ?? [];

  // ---- create dialog -------------------------------------------------------
  const nameId = useId();
  const helpId = useId();
  const errId = useId();
  const [createOpen, setCreateOpen] = useState(false);
  const [name, setName] = useState("");
  const [objectLock, setObjectLock] = useState(false);
  const [creating, setCreating] = useState(false);
  // Server-side failure (409 duplicate, etc.) shown on the name field.
  const [serverError, setServerError] = useState<string | null>(null);

  const clientIssue = name ? nameIssue(name) : null;
  const fieldError = serverError ?? clientIssue;
  const canCreate = !!name && !clientIssue && !creating;

  function openCreate(open: boolean) {
    setCreateOpen(open);
    if (open) {
      setName("");
      setObjectLock(false);
      setServerError(null);
    }
  }

  async function create(e: FormEvent) {
    e.preventDefault();
    if (!canCreate) return;
    setCreating(true);
    setServerError(null);
    try {
      await api.createBucket(name, objectLock);
      toast.success(`Bucket "${name}" created.`);
      setCreateOpen(false);
      list.refresh();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        setServerError(`A bucket named "${name}" already exists.`);
      } else {
        setServerError(errorMessage(err, "Failed to create bucket."));
      }
    } finally {
      setCreating(false);
    }
  }

  // ---- delete flow ---------------------------------------------------------
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  async function confirmDelete() {
    const target = pendingDelete;
    if (!target || deleting) return;
    setDeleting(true);
    try {
      await api.deleteBucket(target);
      toast.success(`Bucket "${target}" deleted.`);
      setPendingDelete(null);
      list.refresh();
    } catch (err) {
      toast.error(errorMessage(err, "Failed to delete bucket."));
    } finally {
      setDeleting(false);
    }
  }

  const createButton = (
    <Button onClick={() => openCreate(true)}>
      <Plus aria-hidden="true" /> Create bucket
    </Button>
  );

  return (
    <Page>
      <PageHeader
        title="Buckets"
        description="Top-level containers for your objects."
        actions={
          <>
            <RefreshButton
              loading={list.loading}
              refreshing={list.refreshing}
              onClick={list.refresh}
            />
            {createButton}
          </>
        }
      />

      {list.error ? (
        <ErrorAlert
          title="Couldn't load buckets"
          message={list.error}
          onRetry={list.refresh}
        />
      ) : null}

      {list.loading ? (
        <DataTable columns={COLUMNS} minWidth={720}>
          <SkeletonRows
            rows={3}
            widths={["w-32", "w-10", "w-16", "w-20", "w-36", "w-8"]}
          />
        </DataTable>
      ) : buckets.length === 0 && !list.error ? (
        <EmptyState
          icon={Database}
          title="No buckets yet"
          body="A bucket is a top-level container that holds your files as objects."
          action={createButton}
        />
      ) : (
        <DataTable columns={COLUMNS} minWidth={720}>
          {buckets.map((b) => (
            <TableRow key={b.name}>
              <TableCell>
                <TextLink
                  to={`/buckets/${encodeURIComponent(b.name)}/browser`}
                  className="font-mono text-[13px]"
                >
                  {b.name}
                </TextLink>
              </TableCell>
              <TableCell className="text-right text-[13px] tabular-nums">
                {count(list.data?.usage.get(b.name)?.objects ?? null)}
              </TableCell>
              <TableCell className="text-right text-[13px] tabular-nums">
                {bytes(list.data?.usage.get(b.name)?.logical_bytes ?? null)}
              </TableCell>
              <TableCell>
                <StatusBadge tone={versioningTone(b.versioning)}>
                  {b.versioning}
                </StatusBadge>
              </TableCell>
              <TableCell className="text-[13px] text-muted-foreground tabular-nums">
                {whenMs(b.created_at_ms)}
              </TableCell>
              <TableCell className="text-right">
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label={`Actions for ${b.name}`}
                    >
                      <MoreHorizontal aria-hidden="true" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem
                      onSelect={() =>
                        navigate(`/buckets/${encodeURIComponent(b.name)}/browser`)
                      }
                    >
                      Browse
                    </DropdownMenuItem>
                    <DropdownMenuItem
                      onSelect={() =>
                        navigate(`/buckets/${encodeURIComponent(b.name)}/settings`)
                      }
                    >
                      Settings
                    </DropdownMenuItem>
                    <DropdownMenuSeparator />
                    <DropdownMenuItem
                      variant="destructive"
                      onSelect={() => setPendingDelete(b.name)}
                    >
                      Delete
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </TableCell>
            </TableRow>
          ))}
        </DataTable>
      )}

      <Dialog open={createOpen} onOpenChange={creating ? undefined : openCreate}>
        <DialogContent className="sm:max-w-md">
          <form onSubmit={create} noValidate>
            <DialogHeader>
              <DialogTitle>Create bucket</DialogTitle>
              <DialogDescription>
                Bucket names are permanent — they can&apos;t be renamed later.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-1.5 py-4">
              <Label htmlFor={nameId}>Bucket name</Label>
              <Input
                id={nameId}
                value={name}
                placeholder="photos"
                autoComplete="off"
                spellCheck={false}
                className="font-mono"
                aria-invalid={fieldError ? true : undefined}
                aria-describedby={fieldError ? `${helpId} ${errId}` : helpId}
                onChange={(e) => {
                  setName(e.target.value);
                  setServerError(null);
                }}
              />
              <p id={helpId} className="text-[13px] text-muted-foreground">
                {NAME_RULE}
              </p>
              <div id={errId}>
                <FieldError>{fieldError}</FieldError>
              </div>

              <label className="mt-2 flex items-start gap-2 rounded-md border p-3 text-sm">
                <Checkbox
                  checked={objectLock}
                  disabled={creating}
                  onCheckedChange={(v) => setObjectLock(v === true)}
                  className="mt-0.5"
                />
                <span>
                  Enable Object Lock (WORM)
                  <span className="mt-0.5 block text-[13px] font-normal text-muted-foreground">
                    Forces versioning on and lets you set retention &amp; legal
                    holds. This can only be enabled now, not later.
                  </span>
                </span>
              </label>
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={creating}
                onClick={() => setCreateOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={!canCreate}>
                {creating ? "Creating…" : "Create bucket"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <TypedConfirmDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null);
        }}
        title="Delete bucket"
        description="This permanently deletes the bucket and every object and version in it. This cannot be undone."
        requireText={pendingDelete ?? ""}
        confirmLabel={deleting ? "Deleting…" : "Delete bucket"}
        cancelLabel="Keep bucket"
        busy={deleting}
        onConfirm={() => void confirmDelete()}
      />
    </Page>
  );
}
