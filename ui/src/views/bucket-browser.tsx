import {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  type ChangeEvent,
} from "react";
import { useParams } from "react-router";
import {
  Check,
  CircleAlert,
  FileBox,
  Loader2,
  MoreHorizontal,
  RotateCw,
  Search,
  Upload,
} from "lucide-react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { EmptyState } from "@/components/empty-state";
import { ObjectPreviewDialog } from "@/components/object-preview-dialog";
import { ShareDialog } from "@/components/share-dialog";
import { api, errorMessage } from "@/lib/api";
import { bytes, whenMs } from "@/lib/format";
import { deleteObject, getObjectBlob, putObject } from "@/lib/s3";
import type { ObjectEntry } from "@/lib/types";
import { cn } from "@/lib/utils";

interface UploadItem {
  name: string;
  status: "uploading" | "done" | "failed";
  message?: string;
}

const UPLOAD_STATUS_WORD: Record<UploadItem["status"], string> = {
  uploading: "uploading",
  done: "uploaded",
  failed: "failed",
};

export function BucketBrowser() {
  // :name comes from the parent /buckets/:name layout route.
  const { name = "" } = useParams<{ name: string }>();

  const prefixId = useId();
  const encryptId = useId();
  const encryptHelpId = useId();

  // ---- listing (manual state: pages accumulate, refresh keeps stale rows) ----
  const [prefixInput, setPrefixInput] = useState("");
  const [prefix, setPrefix] = useState("");
  const [objects, setObjects] = useState<ObjectEntry[] | null>(null);
  const [next, setNext] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Stale-response guard: any new load (or a bucket/prefix change) bumps the
  // ticket so an older in-flight response can't clobber newer state.
  const seqRef = useRef(0);

  // Switching buckets resets the whole browser.
  useEffect(() => {
    setPrefixInput("");
    setPrefix("");
    setObjects(null);
    setNext(null);
    setError(null);
    setUploads([]);
  }, [name]);

  // Debounce the prefix filter; changing it resets paging via load().
  useEffect(() => {
    const t = setTimeout(() => setPrefix(prefixInput), 300);
    return () => clearTimeout(t);
  }, [prefixInput]);

  const load = useCallback(async () => {
    const ticket = ++seqRef.current;
    setError(null);
    setRefreshing(true);
    try {
      const res = await api.listObjects(name, { prefix, limit: 100 });
      if (ticket !== seqRef.current) return;
      setObjects(res.objects ?? []);
      setNext(res.next ?? null);
    } catch (e) {
      if (ticket !== seqRef.current) return;
      setError(errorMessage(e, "Failed to load objects."));
    } finally {
      if (ticket === seqRef.current) setRefreshing(false);
    }
  }, [name, prefix]);

  useEffect(() => {
    void load();
  }, [load]);

  async function loadMore() {
    if (!next || loadingMore) return;
    const ticket = seqRef.current;
    setLoadingMore(true);
    setError(null);
    try {
      const res = await api.listObjects(name, { prefix, limit: 100, cursor: next });
      if (ticket !== seqRef.current) return;
      setObjects((cur) => [...(cur ?? []), ...(res.objects ?? [])]);
      setNext(res.next ?? null);
    } catch (e) {
      if (ticket === seqRef.current)
        setError(errorMessage(e, "Failed to load more objects."));
    } finally {
      setLoadingMore(false);
    }
  }

  // ---- uploads ---------------------------------------------------------------
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [encrypt, setEncrypt] = useState(false);
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [uploading, setUploading] = useState(false);

  async function onFilesPicked(e: ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files ?? []);
    e.target.value = ""; // allow re-picking the same file later
    if (files.length === 0) return;
    setUploading(true);
    setUploads(files.map((f) => ({ name: f.name, status: "uploading" as const })));
    let okCount = 0;
    for (const [i, f] of files.entries()) {
      try {
        await putObject(name, f.name, f, { encrypt });
        okCount++;
        setUploads((u) =>
          u.map((x, j) => (j === i ? { ...x, status: "done" as const } : x)),
        );
      } catch (err) {
        setUploads((u) =>
          u.map((x, j) =>
            j === i
              ? {
                  ...x,
                  status: "failed" as const,
                  message: errorMessage(err, "Upload failed."),
                }
              : x,
          ),
        );
      }
    }
    setUploading(false);
    if (okCount > 0) {
      toast.success(`${okCount} file(s) uploaded`);
      void load();
    }
  }

  // ---- per-object actions ------------------------------------------------------
  const [previewKey, setPreviewKey] = useState<string | null>(null);
  const [shareKey, setShareKey] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  async function download(key: string) {
    try {
      const blob = await getObjectBlob(name, key);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = key.split("/").pop() || key;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      toast.error(errorMessage(e, "Download failed."));
    }
  }

  async function confirmDelete() {
    const key = pendingDelete;
    if (!key || deleting) return;
    setDeleting(true);
    try {
      await deleteObject(name, key);
      toast.success("Object deleted.");
      setPendingDelete(null);
      void load();
    } catch (e) {
      toast.error(errorMessage(e, "Delete failed."));
    } finally {
      setDeleting(false);
    }
  }

  const showSkeleton = objects === null && !error;
  const showEmpty = objects !== null && objects.length === 0;

  return (
    <div className="space-y-4">
      {/* Toolbar: filter + refresh on the left, upload controls on the right. */}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-3">
        <div className="relative">
          <Search
            aria-hidden="true"
            className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
          />
          <label className="visually-hidden" htmlFor={prefixId}>
            Filter objects by key prefix
          </label>
          <Input
            id={prefixId}
            value={prefixInput}
            placeholder="Filter by prefix"
            autoComplete="off"
            spellCheck={false}
            className="w-56 pl-8 font-mono text-[13px]"
            onChange={(e) => setPrefixInput(e.target.value)}
          />
        </div>
        <Button
          type="button"
          variant="outline"
          aria-busy={refreshing}
          onClick={() => void load()}
        >
          <RotateCw aria-hidden="true" className={cn(refreshing && "animate-spin")} />
          Refresh
        </Button>

        <div className="ms-auto flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="flex items-start gap-2">
            <Checkbox
              id={encryptId}
              checked={encrypt}
              aria-describedby={encryptHelpId}
              className="mt-0.5"
              onCheckedChange={(v) => setEncrypt(v === true)}
            />
            <div className="leading-tight">
              <label htmlFor={encryptId} className="text-sm font-medium">
                Encrypt at rest (SSE-S3)
              </label>
              <p id={encryptHelpId} className="text-[13px] text-muted-foreground">
                AES-256, managed by the server
              </p>
            </div>
          </div>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            tabIndex={-1}
            aria-hidden="true"
            onChange={(e) => void onFilesPicked(e)}
          />
          <Button
            type="button"
            disabled={uploading}
            onClick={() => fileInputRef.current?.click()}
          >
            <Upload aria-hidden="true" />
            {uploading ? "Uploading…" : "Upload files"}
          </Button>
        </div>
      </div>

      {/* Per-file upload progress for the current batch. */}
      {uploads.length > 0 ? (
        <div className="rounded-lg border p-3">
          <div className="mb-2 flex items-center justify-between gap-2">
            <p className="text-xs font-medium text-muted-foreground">Uploads</p>
            <Button
              type="button"
              variant="ghost"
              size="xs"
              disabled={uploading}
              onClick={() => setUploads([])}
            >
              Clear
            </Button>
          </div>
          <ul aria-live="polite" className="space-y-1.5">
            {uploads.map((u, i) => (
              <li key={i} className="flex items-start gap-2 text-[13px]">
                {u.status === "uploading" ? (
                  <Loader2
                    aria-hidden="true"
                    className="mt-0.5 size-4 shrink-0 animate-spin text-muted-foreground"
                  />
                ) : u.status === "done" ? (
                  <Check aria-hidden="true" className="mt-0.5 size-4 shrink-0 text-success" />
                ) : (
                  <CircleAlert
                    aria-hidden="true"
                    className="mt-0.5 size-4 shrink-0 text-destructive"
                  />
                )}
                <span className="min-w-0 truncate font-mono" title={u.name}>
                  {u.name}
                </span>
                <span className="visually-hidden">{UPLOAD_STATUS_WORD[u.status]}</span>
                {u.status === "failed" && u.message ? (
                  <span className="text-destructive">{u.message}</span>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {error ? (
        <Alert variant="destructive" role="alert">
          <AlertTitle>Couldn&apos;t load objects</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {showSkeleton ? (
        <div className="overflow-x-auto rounded-lg border">
          <Table className="min-w-[640px]">
            <TableHeader>
              <TableRow>
                <TableHead className="text-xs text-muted-foreground">Key</TableHead>
                <TableHead className="text-right text-xs text-muted-foreground">Size</TableHead>
                <TableHead className="text-xs text-muted-foreground">Modified</TableHead>
                <TableHead>
                  <span className="visually-hidden">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {Array.from({ length: 4 }, (_, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <Skeleton className="h-4 w-64" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="ml-auto h-4 w-16" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-4 w-36" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="ml-auto size-8" />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      ) : showEmpty ? (
        prefix ? (
          <EmptyState
            icon={Search}
            title="No objects match this prefix"
            body="Clear the filter to see everything in this bucket."
          />
        ) : (
          <EmptyState
            icon={FileBox}
            title="No objects yet"
            body="Upload your first files to this bucket."
          />
        )
      ) : objects !== null ? (
        <>
          <div className="overflow-x-auto rounded-lg border">
            <Table className="min-w-[640px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="text-xs text-muted-foreground">Key</TableHead>
                  <TableHead className="text-right text-xs text-muted-foreground">Size</TableHead>
                  <TableHead className="text-xs text-muted-foreground">Modified</TableHead>
                  <TableHead>
                    <span className="visually-hidden">Actions</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {objects.map((o) => (
                  <TableRow key={o.key}>
                    <TableCell className="max-w-[28rem]">
                      <span
                        className="block truncate font-mono text-[13px]"
                        title={o.key}
                      >
                        {o.key}
                      </span>
                    </TableCell>
                    <TableCell className="text-right text-[13px] tabular-nums">
                      {bytes(o.size)}
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-[13px] text-muted-foreground tabular-nums">
                      {whenMs(o.last_modified_ms)}
                    </TableCell>
                    <TableCell className="text-right">
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            aria-label={`Actions for ${o.key}`}
                          >
                            <MoreHorizontal aria-hidden="true" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem onSelect={() => setPreviewKey(o.key)}>
                            Preview
                          </DropdownMenuItem>
                          <DropdownMenuItem onSelect={() => void download(o.key)}>
                            Download
                          </DropdownMenuItem>
                          <DropdownMenuItem onSelect={() => setShareKey(o.key)}>
                            Share
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                          <DropdownMenuItem
                            variant="destructive"
                            className="text-destructive"
                            onSelect={() => setPendingDelete(o.key)}
                          >
                            Delete
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          {next ? (
            <div className="flex justify-center">
              <Button
                type="button"
                variant="outline"
                disabled={loadingMore}
                aria-busy={loadingMore}
                onClick={() => void loadMore()}
              >
                {loadingMore ? "Loading…" : "Load more"}
              </Button>
            </div>
          ) : null}
        </>
      ) : null}

      <ObjectPreviewDialog
        bucket={name}
        objectKey={previewKey ?? ""}
        open={previewKey !== null}
        onOpenChange={(open) => {
          if (!open) setPreviewKey(null);
        }}
      />

      <ShareDialog
        bucket={name}
        objectKey={shareKey ?? ""}
        open={shareKey !== null}
        onOpenChange={(open) => {
          if (!open) setShareKey(null);
        }}
      />

      <ConfirmDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null);
        }}
        title="Delete object"
        description={
          <>
            This permanently deletes{" "}
            <span className="break-all font-mono text-[13px] text-foreground">
              {pendingDelete}
            </span>
            . This cannot be undone.
          </>
        }
        confirmLabel={deleting ? "Deleting…" : "Delete object"}
        cancelLabel="Keep object"
        destructive
        busy={deleting}
        onConfirm={() => void confirmDelete()}
      />
    </div>
  );
}
