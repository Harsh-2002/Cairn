// The bucket's "Uploads" tab: in-progress multipart uploads that started but
// haven't been completed or aborted. v1 is a flat list with a per-row abort and
// the shared refresh — no pagination web console, no parts drill-down. Renders inside the
// BucketDetail layout (which owns the <Page> column and the tab bar).

import { useState } from "react";
import { useParams } from "react-router";
import { MoreHorizontal, Trash2, UploadCloud } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/primitives/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/primitives/dropdown-menu";
import { TableCell, TableRow } from "@/components/primitives/table";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { RefreshButton } from "@/components/refresh-button";
import { errorMessage } from "@/lib/api";
import { relTime, whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import {
  abortMultipartUpload,
  listMultipartUploads,
  type MultipartUpload,
} from "@/lib/s3";

const UPLOAD_COLUMNS: Column[] = [
  { key: "key", label: "Key" },
  { key: "uploadId", label: "Upload ID" },
  { key: "initiated", label: "Initiated" },
  { key: "actions", label: "Actions", srOnly: true },
];

// One skeleton width per column, written as literal Tailwind classes for the JIT.
const UPLOAD_SKELETON_WIDTHS = ["w-56", "w-64", "w-20", "w-8"];

export function MultipartUploads() {
  // :name comes from the parent /buckets/:name layout route.
  const { name = "" } = useParams<{ name: string }>();

  const { data, error, loading, refreshing, refresh } = useResource(
    () => listMultipartUploads(name),
    [name],
  );
  const uploads = data?.uploads ?? [];

  // ---- abort flow (mirrors the object-delete flow) -----------------------------
  const [pending, setPending] = useState<MultipartUpload | null>(null);
  const [aborting, setAborting] = useState(false);

  async function confirmAbort() {
    const u = pending;
    if (!u || aborting) return;
    setAborting(true);
    try {
      await abortMultipartUpload(name, u.key, u.uploadId);
      toast.success("Upload aborted.");
      setPending(null);
      void refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Abort failed."));
    } finally {
      setAborting(false);
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-3">
        <div className="space-y-1">
          <h2 className="text-base font-semibold tracking-tight">
            In-progress uploads
          </h2>
          <p className="text-sm text-muted-foreground">
            Multipart uploads that have started but haven&apos;t been completed
            or aborted yet.
          </p>
        </div>
        <RefreshButton
          loading={loading}
          refreshing={refreshing}
          onClick={refresh}
        />
      </div>

      {error ? (
        <ErrorAlert
          title="Couldn't load uploads"
          message={error}
          onRetry={refresh}
        />
      ) : null}

      {loading ? (
        <DataTable columns={UPLOAD_COLUMNS} minWidth={680}>
          <SkeletonRows rows={4} widths={UPLOAD_SKELETON_WIDTHS} />
        </DataTable>
      ) : uploads.length === 0 && !error ? (
        <EmptyState
          icon={UploadCloud}
          positive
          title="No in-progress uploads"
          body="Incomplete multipart uploads show up here until they're completed or aborted. There's nothing in progress right now."
        />
      ) : uploads.length > 0 ? (
        <DataTable columns={UPLOAD_COLUMNS} minWidth={680}>
          {uploads.map((u) => (
            <TableRow key={`${u.key}:${u.uploadId}`}>
              <TableCell data-label="Key" className="max-w-[28rem]">
                <span
                  className="block truncate font-mono text-[13px]"
                  title={u.key}
                >
                  {u.key}
                </span>
              </TableCell>
              <TableCell data-label="Upload ID" className="max-w-[20rem]">
                <span
                  className="block truncate font-mono text-[13px] text-muted-foreground"
                  title={u.uploadId}
                >
                  {u.uploadId}
                </span>
              </TableCell>
              <TableCell
                data-label="Initiated"
                className="whitespace-nowrap text-[13px] text-muted-foreground tabular-nums"
                title={
                  Number.isNaN(u.initiatedMs) ? undefined : whenMs(u.initiatedMs)
                }
              >
                {Number.isNaN(u.initiatedMs) ? "—" : relTime(u.initiatedMs)}
              </TableCell>
              <TableCell className="text-right">
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="size-10 sm:size-9"
                      aria-label={`Actions for the upload of ${u.key}`}
                    >
                      <MoreHorizontal aria-hidden="true" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuItem
                      variant="destructive"
                      onSelect={() => setPending(u)}
                    >
                      <Trash2 aria-hidden="true" />
                      Abort upload
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </TableCell>
            </TableRow>
          ))}
        </DataTable>
      ) : null}

      <ConfirmDialog
        open={pending !== null}
        onOpenChange={(open) => {
          if (!open) setPending(null);
        }}
        title="Abort upload"
        description={
          <>
            This discards the in-progress multipart upload for{" "}
            <span className="break-all font-mono text-[13px] text-foreground">
              {pending?.key}
            </span>{" "}
            and deletes any parts already staged for it. This cannot be undone.
          </>
        }
        confirmLabel={aborting ? "Aborting…" : "Abort upload"}
        cancelLabel="Keep upload"
        destructive
        busy={aborting}
        onConfirm={() => void confirmAbort()}
      />
    </div>
  );
}
