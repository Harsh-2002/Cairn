import {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  type ChangeEvent,
  type DragEvent,
} from "react";
import { useParams } from "react-router";
import {
  Check,
  CircleAlert,
  FileBox,
  Folder,
  FolderPlus,
  Loader2,
  MoreHorizontal,
  Search,
  Tag,
  Upload,
} from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
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
import { ErrorAlert } from "@/components/error-alert";
import { ObjectPreviewDialog } from "@/components/object-preview-dialog";
import { ObjectTagsDialog } from "@/components/object-tags-dialog";
import { RefreshButton } from "@/components/refresh-button";
import { ShareDialog } from "@/components/share-dialog";
import { api, errorMessage } from "@/lib/api";
import { bytes, whenMs } from "@/lib/format";
import {
  createFolder,
  deleteObject,
  getObjectBlob,
  putObject,
} from "@/lib/s3";
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

  const filterId = useId();

  // ---- listing -----------------------------------------------------------------
  // `path` is the current folder ("" at the root, always "/"-terminated below);
  // `filter` narrows within it. The effective listing prefix is path + filter,
  // always folded at "/" so key groups render as folders.
  const [path, setPath] = useState("");
  const [filterInput, setFilterInput] = useState("");
  const [filter, setFilter] = useState("");
  const [objects, setObjects] = useState<ObjectEntry[] | null>(null);
  const [folders, setFolders] = useState<string[]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Stale-response guard: any new load (or a bucket/path change) bumps the
  // ticket so an older in-flight response can't clobber newer state.
  const seqRef = useRef(0);

  // Switching buckets resets the whole browser.
  useEffect(() => {
    setPath("");
    setFilterInput("");
    setFilter("");
    setObjects(null);
    setFolders([]);
    setNext(null);
    setError(null);
    setUploads([]);
  }, [name]);

  // Debounce the filter; changing it resets paging via load().
  useEffect(() => {
    const t = setTimeout(() => setFilter(filterInput), 300);
    return () => clearTimeout(t);
  }, [filterInput]);

  const listPrefix = path + filter;

  const load = useCallback(async () => {
    const ticket = ++seqRef.current;
    setError(null);
    setRefreshing(true);
    try {
      const res = await api.listObjects(name, {
        prefix: listPrefix,
        delimiter: "/",
        limit: 100,
      });
      if (ticket !== seqRef.current) return;
      setObjects(res.objects ?? []);
      setFolders(res.common_prefixes ?? []);
      setNext(res.next ?? null);
    } catch (e) {
      if (ticket !== seqRef.current) return;
      setError(errorMessage(e, "Failed to load objects."));
    } finally {
      if (ticket === seqRef.current) setRefreshing(false);
    }
  }, [name, listPrefix]);

  useEffect(() => {
    void load();
  }, [load]);

  async function loadMore() {
    if (!next || loadingMore) return;
    const ticket = seqRef.current;
    setLoadingMore(true);
    setError(null);
    try {
      const res = await api.listObjects(name, {
        prefix: listPrefix,
        delimiter: "/",
        limit: 100,
        cursor: next,
      });
      if (ticket !== seqRef.current) return;
      setObjects((cur) => [...(cur ?? []), ...(res.objects ?? [])]);
      setFolders((cur) => [...new Set([...cur, ...(res.common_prefixes ?? [])])]);
      setNext(res.next ?? null);
    } catch (e) {
      if (ticket === seqRef.current)
        setError(errorMessage(e, "Failed to load more objects."));
    } finally {
      setLoadingMore(false);
    }
  }

  function enterFolder(prefix: string) {
    setPath(prefix);
    setFilterInput("");
    setFilter("");
  }

  // Breadcrumb segments for the current path ("docs/sub/" → ["docs", "sub"]).
  const segments = path.split("/").filter(Boolean);

  // ---- uploads -------------------------------------------------------------------
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [uploading, setUploading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  // Counter-based drag tracking: child enter/leave events would flicker a boolean.
  const dragDepth = useRef(0);

  const uploadFiles = useCallback(
    async (files: File[]) => {
      if (files.length === 0 || uploading) return;
      setUploading(true);
      setUploads(
        files.map((f) => ({ name: path + f.name, status: "uploading" as const })),
      );
      let okCount = 0;
      for (const [i, f] of files.entries()) {
        try {
          // Uploads land in the folder being viewed. Encryption is the
          // bucket's default-SSE setting — no per-upload choice.
          await putObject(name, path + f.name, f);
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
        toast.success(`${okCount} file${okCount === 1 ? "" : "s"} uploaded`);
        void load();
      }
    },
    [name, path, uploading, load],
  );

  function onFilesPicked(e: ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files ?? []);
    e.target.value = ""; // allow re-picking the same file later
    void uploadFiles(files);
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    dragDepth.current = 0;
    setDragOver(false);
    void uploadFiles(Array.from(e.dataTransfer.files ?? []));
  }

  // ---- per-object actions ----------------------------------------------------------
  const [previewKey, setPreviewKey] = useState<string | null>(null);
  const [shareKey, setShareKey] = useState<string | null>(null);
  const [tagsKey, setTagsKey] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  // Create-folder dialog (a zero-byte "prefix/" marker in the current folder).
  const [createFolderOpen, setCreateFolderOpen] = useState(false);
  const [folderName, setFolderName] = useState("");
  const [creatingFolder, setCreatingFolder] = useState(false);

  async function submitCreateFolder() {
    const seg = folderName.trim().replace(/\/+$/, "");
    if (!seg) {
      toast.error("Enter a folder name.");
      return;
    }
    if (seg.includes("/")) {
      toast.error("Folder names can't contain “/”.");
      return;
    }
    setCreatingFolder(true);
    try {
      await createFolder(name, path + seg);
      toast.success(`Folder “${seg}” created`);
      setCreateFolderOpen(false);
      setFolderName("");
      void load();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to create the folder."));
    } finally {
      setCreatingFolder(false);
    }
  }

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
  const showEmpty =
    objects !== null && objects.length === 0 && folders.length === 0;

  return (
    <div
      className="space-y-4"
      onDragEnter={(e) => {
        if (!e.dataTransfer.types.includes("Files")) return;
        e.preventDefault();
        dragDepth.current++;
        setDragOver(true);
      }}
      onDragOver={(e) => {
        if (e.dataTransfer.types.includes("Files")) e.preventDefault();
      }}
      onDragLeave={() => {
        dragDepth.current = Math.max(0, dragDepth.current - 1);
        if (dragDepth.current === 0) setDragOver(false);
      }}
      onDrop={onDrop}
    >
      {/* Toolbar: filter + refresh on the left, upload on the right. */}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-3">
        <div className="relative">
          <Search
            aria-hidden="true"
            className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground"
          />
          <label className="visually-hidden" htmlFor={filterId}>
            Filter this folder by name prefix
          </label>
          <Input
            id={filterId}
            value={filterInput}
            placeholder="Filter this folder"
            autoComplete="off"
            spellCheck={false}
            className="w-full pl-8 font-mono text-[13px] sm:w-56"
            onChange={(e) => setFilterInput(e.target.value)}
          />
        </div>
        <RefreshButton
          loading={objects === null}
          refreshing={refreshing}
          onClick={() => void load()}
        />

        <div className="ms-auto flex items-center gap-3">
          <p className="hidden text-[13px] text-muted-foreground sm:block">
            or drag files anywhere here
          </p>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            tabIndex={-1}
            aria-hidden="true"
            onChange={onFilesPicked}
          />
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              setFolderName("");
              setCreateFolderOpen(true);
            }}
          >
            <FolderPlus aria-hidden="true" />
            New folder
          </Button>
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

      {/* Folder breadcrumb: the bucket root plus each path segment. */}
      <nav aria-label="Folder path" className="flex flex-wrap items-center gap-1 text-[13px]">
        <button
          type="button"
          onClick={() => enterFolder("")}
          className={cn(
            "rounded px-1.5 py-0.5 font-mono",
            path === ""
              ? "font-medium text-foreground"
              : "text-link hover:underline underline-offset-4",
          )}
          aria-current={path === "" ? "location" : undefined}
        >
          {name}
        </button>
        {segments.map((seg, i) => {
          const target = `${segments.slice(0, i + 1).join("/")}/`;
          const isLast = i === segments.length - 1;
          return (
            <span key={target} className="flex items-center gap-1">
              <span aria-hidden="true" className="text-muted-foreground">
                /
              </span>
              <button
                type="button"
                onClick={() => enterFolder(target)}
                className={cn(
                  "rounded px-1 py-0.5 font-mono",
                  isLast
                    ? "font-medium text-foreground"
                    : "text-link hover:underline underline-offset-4",
                )}
                aria-current={isLast ? "location" : undefined}
              >
                {seg}
              </button>
            </span>
          );
        })}
      </nav>

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
        <ErrorAlert
          title="Couldn't load objects"
          message={error}
          onRetry={() => void load()}
        />
      ) : null}

      {showSkeleton ? (
        <div className="overflow-x-auto rounded-lg border">
          <Table className="min-w-[640px]">
            <TableHeader>
              <TableRow>
                <TableHead className="text-xs text-muted-foreground">Name</TableHead>
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
        filter ? (
          <EmptyState
            icon={Search}
            title="Nothing matches this filter"
            body="Clear the filter to see everything in this folder."
          />
        ) : (
          <EmptyState
            icon={FileBox}
            title={path ? "This folder is empty" : "No objects yet"}
            body="Upload files with the button above, or drop them anywhere on this page."
          />
        )
      ) : objects !== null ? (
        <>
          <div
            className={cn(
              "overflow-x-auto rounded-lg border transition-colors",
              dragOver && "border-ring bg-muted/60",
            )}
          >
            <Table className="min-w-[640px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="text-xs text-muted-foreground">Name</TableHead>
                  <TableHead className="text-right text-xs text-muted-foreground">Size</TableHead>
                  <TableHead className="text-xs text-muted-foreground">Modified</TableHead>
                  <TableHead>
                    <span className="visually-hidden">Actions</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {folders.map((f) => (
                  <TableRow key={f}>
                    <TableCell colSpan={3}>
                      <button
                        type="button"
                        onClick={() => enterFolder(f)}
                        className="flex items-center gap-2 font-mono text-[13px] text-foreground hover:underline underline-offset-4"
                      >
                        <Folder
                          aria-hidden="true"
                          className="size-4 shrink-0 text-muted-foreground"
                        />
                        {f.slice(path.length)}
                      </button>
                    </TableCell>
                    <TableCell />
                  </TableRow>
                ))}
                {objects.map((o) => (
                  <TableRow key={o.key}>
                    <TableCell className="max-w-[28rem]">
                      <span
                        className="block truncate font-mono text-[13px]"
                        title={o.key}
                      >
                        {o.key.slice(path.length) || o.key}
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
                            size="icon"
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
                          <DropdownMenuItem onSelect={() => setTagsKey(o.key)}>
                            <Tag aria-hidden="true" />
                            Edit tags
                          </DropdownMenuItem>
                          <DropdownMenuItem onSelect={() => setShareKey(o.key)}>
                            Share
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                          <DropdownMenuItem
                            variant="destructive"
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

      <ObjectTagsDialog
        bucket={name}
        objectKey={tagsKey ?? ""}
        open={tagsKey !== null}
        onOpenChange={(open) => {
          if (!open) setTagsKey(null);
        }}
      />

      <Dialog
        open={createFolderOpen}
        onOpenChange={creatingFolder ? undefined : setCreateFolderOpen}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>New folder</DialogTitle>
            <DialogDescription>
              Creates an empty folder marker in{" "}
              <span className="font-mono">{path || `${name}/`}</span>. Folders are
              just key prefixes; uploading a file into one works without this.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-1.5">
            <Label htmlFor={`${filterId}-folder`}>Folder name</Label>
            <Input
              id={`${filterId}-folder`}
              value={folderName}
              autoFocus
              autoComplete="off"
              spellCheck={false}
              className="font-mono"
              onChange={(e) => setFolderName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void submitCreateFolder();
              }}
            />
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setCreateFolderOpen(false)}
              disabled={creatingFolder}
            >
              Cancel
            </Button>
            <Button onClick={() => void submitCreateFolder()} disabled={creatingFolder}>
              {creatingFolder ? "Creating…" : "Create folder"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
