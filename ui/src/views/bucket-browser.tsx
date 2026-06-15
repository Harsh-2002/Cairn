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
  FolderUp,
  Loader2,
  MoreHorizontal,
  Search,
  Tag,
  Upload,
} from "lucide-react";
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
import { Progress } from "@/components/ui/progress";
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
import { ManageSharesDialog } from "@/components/manage-shares-dialog";
import { ShareDialog } from "@/components/share-dialog";
import { StatusBadge } from "@/components/status-badge";
import { api, errorMessage } from "@/lib/api";
import { bytes, speed, whenMs } from "@/lib/format";
import {
  bulkDelete,
  copyObject,
  createFolder,
  deleteObject,
  getObjectBlob,
  listObjectVersions,
  putObjectWithProgress,
  type ObjectVersion,
} from "@/lib/s3";
import type { ObjectEntry } from "@/lib/types";
import { cn } from "@/lib/utils";

interface UploadItem {
  name: string;
  status: "uploading" | "done" | "failed";
  loaded: number;
  total: number;
  bytesPerSec: number;
  message?: string;
}

// A file plus its target key suffix under the current folder (preserves nested
// structure for folder uploads: "myfolder/sub/file.txt").
interface PendingUpload {
  file: File;
  rel: string;
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
  const [versions, setVersions] = useState<ObjectVersion[]>([]);
  const [showVersions, setShowVersions] = useState(false);
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
    setVersions([]);
    setShowVersions(false);
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
      if (showVersions) {
        // Version mode lists every version + delete marker (no server paging here).
        const res = await listObjectVersions(name, listPrefix, "/");
        if (ticket !== seqRef.current) return;
        setVersions(res.versions);
        setFolders(res.commonPrefixes);
        setObjects([]); // mark "loaded" so the skeleton clears
        setNext(null);
      } else {
        const res = await api.listObjects(name, {
          prefix: listPrefix,
          delimiter: "/",
          limit: 100,
        });
        if (ticket !== seqRef.current) return;
        setObjects(res.objects ?? []);
        setVersions([]);
        setFolders(res.common_prefixes ?? []);
        setNext(res.next ?? null);
      }
    } catch (e) {
      if (ticket !== seqRef.current) return;
      setError(errorMessage(e, "Failed to load objects."));
    } finally {
      if (ticket === seqRef.current) setRefreshing(false);
    }
  }, [name, listPrefix, showVersions]);

  useEffect(() => {
    void load();
  }, [load]);

  // Selection is scoped to the current folder/mode; clear it on any navigation.
  useEffect(() => {
    setSelected(new Set());
  }, [name, listPrefix, showVersions]);

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
  const folderInputRef = useRef<HTMLInputElement>(null);
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [uploading, setUploading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  // Counter-based drag tracking: child enter/leave events would flicker a boolean.
  const dragDepth = useRef(0);

  const uploadAll = useCallback(
    async (items: PendingUpload[]) => {
      if (items.length === 0 || uploading) return;
      setUploading(true);
      setUploads(
        items.map((it) => ({
          name: path + it.rel,
          status: "uploading" as const,
          loaded: 0,
          total: it.file.size,
          bytesPerSec: 0,
        })),
      );
      let okCount = 0;
      for (const [i, it] of items.entries()) {
        try {
          // Uploads land under the folder being viewed, preserving any nested
          // path from a folder pick. Encryption is the bucket's default-SSE
          // setting — no per-upload choice.
          await putObjectWithProgress(name, path + it.rel, it.file, (p) => {
            setUploads((u) =>
              u.map((x, j) =>
                j === i
                  ? { ...x, loaded: p.loaded, total: p.total, bytesPerSec: p.bytesPerSec }
                  : x,
              ),
            );
          });
          okCount++;
          setUploads((u) =>
            u.map((x, j) =>
              j === i
                ? { ...x, status: "done" as const, loaded: x.total, bytesPerSec: 0 }
                : x,
            ),
          );
        } catch (err) {
          setUploads((u) =>
            u.map((x, j) =>
              j === i
                ? {
                    ...x,
                    status: "failed" as const,
                    bytesPerSec: 0,
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
      } else {
        toast.error("Upload failed.");
      }
    },
    [name, path, uploading, load],
  );

  // Map picked Files to uploads, honoring webkitRelativePath (set by a folder pick).
  function toPending(files: File[]): PendingUpload[] {
    return files.map((f) => ({
      file: f,
      rel: (f as File & { webkitRelativePath?: string }).webkitRelativePath || f.name,
    }));
  }

  function onFilesPicked(e: ChangeEvent<HTMLInputElement>) {
    const files = Array.from(e.target.files ?? []);
    e.target.value = ""; // allow re-picking the same file later
    void uploadAll(toPending(files));
  }

  // Walk a dropped folder tree (webkitGetAsEntry) into a flat list with relative
  // paths, so dropping a folder uploads its whole contents. Entries must be read
  // synchronously during the drop event, before any await.
  async function gatherDropped(dt: DataTransfer): Promise<PendingUpload[]> {
    const entries = Array.from(dt.items ?? [])
      .map((it) => (it.webkitGetAsEntry ? it.webkitGetAsEntry() : null))
      .filter((e): e is FileSystemEntry => e !== null);
    if (entries.length === 0) return toPending(Array.from(dt.files ?? []));

    const out: PendingUpload[] = [];
    const walk = async (entry: FileSystemEntry, prefix: string): Promise<void> => {
      if (entry.isFile) {
        const file = await new Promise<File>((res, rej) =>
          (entry as FileSystemFileEntry).file(res, rej),
        );
        out.push({ file, rel: prefix + entry.name });
      } else if (entry.isDirectory) {
        const reader = (entry as FileSystemDirectoryEntry).createReader();
        // readEntries returns one batch at a time; loop until it returns none.
        for (;;) {
          const batch = await new Promise<FileSystemEntry[]>((res, rej) =>
            reader.readEntries(res, rej),
          );
          if (batch.length === 0) break;
          for (const child of batch) await walk(child, `${prefix}${entry.name}/`);
        }
      }
    };
    for (const e of entries) await walk(e, "");
    return out;
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    dragDepth.current = 0;
    setDragOver(false);
    void gatherDropped(e.dataTransfer).then((items) => uploadAll(items));
  }

  // ---- per-object actions ----------------------------------------------------------
  const [previewKey, setPreviewKey] = useState<string | null>(null);
  const [shareKey, setShareKey] = useState<string | null>(null);
  const [manageSharesKey, setManageSharesKey] = useState<string | null>(null);
  const [tagsKey, setTagsKey] = useState<string | null>(null);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  // Multi-select for bulk delete (object mode only).
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const [confirmBulk, setConfirmBulk] = useState(false);

  function toggleSelected(key: string) {
    setSelected((cur) => {
      const next = new Set(cur);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  async function confirmBulkDelete() {
    if (selected.size === 0) return;
    setBulkDeleting(true);
    try {
      const r = await bulkDelete(name, [...selected]);
      if (r.errors.length > 0) {
        toast.error(`Deleted ${r.deleted}, ${r.errors.length} failed.`);
      } else {
        toast.success(
          `Deleted ${r.deleted} object${r.deleted === 1 ? "" : "s"}.`,
        );
      }
      setSelected(new Set());
      setConfirmBulk(false);
      void load();
    } catch (e) {
      toast.error(errorMessage(e, "Bulk delete failed."));
    } finally {
      setBulkDeleting(false);
    }
  }

  // Copy / move (rename) one object via server-side CopyObject.
  const [copySource, setCopySource] = useState<string | null>(null);
  const [copyDest, setCopyDest] = useState("");
  const [copyAsMove, setCopyAsMove] = useState(false);
  const [copying, setCopying] = useState(false);

  async function submitCopy() {
    const src = copySource;
    const dest = copyDest.trim();
    if (!src || !dest) {
      toast.error("Enter a destination key.");
      return;
    }
    if (dest === src) {
      toast.error("Choose a different destination key.");
      return;
    }
    setCopying(true);
    try {
      await copyObject(name, src, dest);
      if (copyAsMove) await deleteObject(name, src);
      toast.success(copyAsMove ? "Object moved." : "Object copied.");
      setCopySource(null);
      void load();
    } catch (e) {
      toast.error(errorMessage(e, "Copy failed."));
    } finally {
      setCopying(false);
    }
  }

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

  async function download(key: string, versionId?: string) {
    try {
      const blob = await getObjectBlob(name, key, versionId);
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

  // Permanently deleting a specific version (versioned buckets only).
  const [pendingVersionDelete, setPendingVersionDelete] =
    useState<ObjectVersion | null>(null);
  const [deletingVersion, setDeletingVersion] = useState(false);

  async function confirmVersionDelete() {
    const v = pendingVersionDelete;
    if (!v || deletingVersion) return;
    setDeletingVersion(true);
    try {
      await deleteObject(name, v.key, v.versionId);
      toast.success("Version permanently deleted.");
      setPendingVersionDelete(null);
      void load();
    } catch (e) {
      toast.error(errorMessage(e, "Delete failed."));
    } finally {
      setDeletingVersion(false);
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
  const itemCount = showVersions ? versions.length : (objects?.length ?? 0);
  const showEmpty = objects !== null && itemCount === 0 && folders.length === 0;

  // Upload batch aggregates (overall % + combined live speed).
  const uploadDone = uploads.filter((u) => u.status === "done").length;
  const uploadTotalBytes = uploads.reduce((s, u) => s + u.total, 0);
  const uploadLoadedBytes = uploads.reduce(
    (s, u) => s + (u.status === "done" ? u.total : u.loaded),
    0,
  );
  const uploadOverallPct =
    uploadTotalBytes > 0
      ? Math.floor((uploadLoadedBytes / uploadTotalBytes) * 100)
      : 0;
  const uploadAggSpeed = uploads
    .filter((u) => u.status === "uploading")
    .reduce((s, u) => s + u.bytesPerSec, 0);

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
        <Button
          type="button"
          variant={showVersions ? "secondary" : "outline"}
          aria-pressed={showVersions}
          onClick={() => {
            setObjects(null);
            setShowVersions((v) => !v);
          }}
        >
          {showVersions ? "Hide versions" : "Show versions"}
        </Button>

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
          {/* webkitdirectory turns this into a folder picker; it isn't a typed
              React prop, so set it on the DOM node via the ref callback. */}
          <input
            ref={(el) => {
              folderInputRef.current = el;
              if (el) {
                el.setAttribute("webkitdirectory", "");
                el.setAttribute("directory", "");
              }
            }}
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
            variant="outline"
            disabled={uploading}
            onClick={() => folderInputRef.current?.click()}
          >
            <FolderUp aria-hidden="true" />
            Upload folder
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

      {/* Per-file upload progress (live %, speed) for the current batch. */}
      {uploads.length > 0 ? (
        <div className="rounded-lg border p-3">
          <div className="mb-2 flex items-center justify-between gap-2">
            <p className="text-xs font-medium text-muted-foreground">
              Uploads — {uploadDone}/{uploads.length}
              {uploading
                ? ` · ${uploadOverallPct}% · ${speed(uploadAggSpeed)}`
                : ""}
            </p>
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
          <ul aria-live="polite" className="space-y-2">
            {uploads.map((u, i) => {
              const pct =
                u.total > 0
                  ? Math.floor((u.loaded / u.total) * 100)
                  : u.status === "done"
                    ? 100
                    : 0;
              return (
                <li key={i} className="space-y-1 text-[13px]">
                  <div className="flex items-center gap-2">
                    {u.status === "uploading" ? (
                      <Loader2
                        aria-hidden="true"
                        className="size-4 shrink-0 animate-spin text-muted-foreground"
                      />
                    ) : u.status === "done" ? (
                      <Check
                        aria-hidden="true"
                        className="size-4 shrink-0 text-success"
                      />
                    ) : (
                      <CircleAlert
                        aria-hidden="true"
                        className="size-4 shrink-0 text-destructive"
                      />
                    )}
                    <span
                      className="min-w-0 flex-1 truncate font-mono"
                      title={u.name}
                    >
                      {u.name}
                    </span>
                    <span className="visually-hidden">
                      {UPLOAD_STATUS_WORD[u.status]}
                    </span>
                    {u.status === "uploading" ? (
                      <span className="shrink-0 tabular-nums text-muted-foreground">
                        {pct}% · {speed(u.bytesPerSec)}
                      </span>
                    ) : u.status === "done" ? (
                      <span className="shrink-0 tabular-nums text-muted-foreground">
                        {bytes(u.total)}
                      </span>
                    ) : null}
                  </div>
                  {u.status === "uploading" ? (
                    <Progress value={pct} className="h-1.5" />
                  ) : null}
                  {u.status === "failed" && u.message ? (
                    <p className="text-destructive">{u.message}</p>
                  ) : null}
                </li>
              );
            })}
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
          {selected.size > 0 ? (
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/40 px-3 py-2">
              <span className="text-[13px]">
                {selected.size} selected
              </span>
              <span className="flex gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setSelected(new Set())}
                >
                  Clear
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  className="text-destructive"
                  disabled={bulkDeleting}
                  onClick={() => setConfirmBulk(true)}
                >
                  Delete selected
                </Button>
              </span>
            </div>
          ) : null}
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
                {!showVersions &&
                  objects.map((o) => (
                    <TableRow
                      key={o.key}
                      data-state={selected.has(o.key) ? "selected" : undefined}
                    >
                      <TableCell className="max-w-[28rem]">
                        <span className="flex items-center gap-2">
                          <Checkbox
                            checked={selected.has(o.key)}
                            onCheckedChange={() => toggleSelected(o.key)}
                            aria-label={`Select ${o.key}`}
                          />
                          <span
                            className="block truncate font-mono text-[13px]"
                            title={o.key}
                          >
                            {o.key.slice(path.length) || o.key}
                          </span>
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
                            <DropdownMenuItem
                              onSelect={() => {
                                setCopySource(o.key);
                                setCopyDest(o.key);
                                setCopyAsMove(false);
                              }}
                            >
                              Copy or move…
                            </DropdownMenuItem>
                            <DropdownMenuItem onSelect={() => setShareKey(o.key)}>
                              Share
                            </DropdownMenuItem>
                            <DropdownMenuItem
                              onSelect={() => setManageSharesKey(o.key)}
                            >
                              Manage shares
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
                {showVersions &&
                  versions.map((v) => (
                    <TableRow key={`${v.key}:${v.versionId}`}>
                      <TableCell className="max-w-[28rem]">
                        <span
                          className="block truncate font-mono text-[13px]"
                          title={v.key}
                        >
                          {v.key.slice(path.length) || v.key}
                        </span>
                        <span className="flex items-center gap-1.5 pt-1">
                          {v.isLatest ? (
                            <StatusBadge tone="positive">Latest</StatusBadge>
                          ) : null}
                          {v.isDeleteMarker ? (
                            <StatusBadge tone="warning">Delete marker</StatusBadge>
                          ) : null}
                          <span
                            className="truncate font-mono text-[11px] text-muted-foreground"
                            title={v.versionId}
                          >
                            {v.versionId}
                          </span>
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-[13px] tabular-nums">
                        {v.isDeleteMarker ? "—" : bytes(v.size)}
                      </TableCell>
                      <TableCell className="whitespace-nowrap text-[13px] text-muted-foreground tabular-nums">
                        {whenMs(v.lastModifiedMs)}
                      </TableCell>
                      <TableCell className="text-right">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon"
                              aria-label={`Actions for version ${v.versionId} of ${v.key}`}
                            >
                              <MoreHorizontal aria-hidden="true" />
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            {!v.isDeleteMarker ? (
                              <DropdownMenuItem
                                onSelect={() => void download(v.key, v.versionId)}
                              >
                                Download this version
                              </DropdownMenuItem>
                            ) : null}
                            <DropdownMenuSeparator />
                            <DropdownMenuItem
                              variant="destructive"
                              onSelect={() => setPendingVersionDelete(v)}
                            >
                              Delete this version
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

      <ManageSharesDialog
        bucket={name}
        objectKey={manageSharesKey ?? ""}
        open={manageSharesKey !== null}
        onOpenChange={(open) => {
          if (!open) setManageSharesKey(null);
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

      <ConfirmDialog
        open={pendingVersionDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingVersionDelete(null);
        }}
        title="Delete this version"
        description={
          <>
            This permanently removes version{" "}
            <span className="break-all font-mono text-[13px] text-foreground">
              {pendingVersionDelete?.versionId}
            </span>{" "}
            of{" "}
            <span className="break-all font-mono text-[13px] text-foreground">
              {pendingVersionDelete?.key}
            </span>
            . This cannot be undone.
          </>
        }
        confirmLabel={deletingVersion ? "Deleting…" : "Delete version"}
        cancelLabel="Keep version"
        destructive
        busy={deletingVersion}
        onConfirm={() => void confirmVersionDelete()}
      />

      <ConfirmDialog
        open={confirmBulk}
        onOpenChange={(open) => {
          if (!open) setConfirmBulk(false);
        }}
        title="Delete selected objects"
        description={`This permanently deletes ${selected.size} object${selected.size === 1 ? "" : "s"}. This cannot be undone.`}
        confirmLabel={bulkDeleting ? "Deleting…" : "Delete selected"}
        cancelLabel="Keep objects"
        destructive
        busy={bulkDeleting}
        onConfirm={() => void confirmBulkDelete()}
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
        open={copySource !== null}
        onOpenChange={(open) => {
          if (!open && !copying) setCopySource(null);
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{copyAsMove ? "Move object" : "Copy object"}</DialogTitle>
            <DialogDescription className="break-all">
              {copyAsMove ? "Moving" : "Copying"} from{" "}
              <span className="font-mono">{copySource}</span>
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="grid gap-1.5">
              <Label htmlFor={`${filterId}-dest`}>Destination key</Label>
              <Input
                id={`${filterId}-dest`}
                value={copyDest}
                autoComplete="off"
                spellCheck={false}
                className="font-mono"
                onChange={(e) => setCopyDest(e.target.value)}
              />
            </div>
            <label className="flex items-center gap-2 text-[13px]">
              <Checkbox
                checked={copyAsMove}
                onCheckedChange={(v) => setCopyAsMove(v === true)}
              />
              Move (delete the original after copying)
            </label>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setCopySource(null)}
              disabled={copying}
            >
              Cancel
            </Button>
            <Button onClick={() => void submitCopy()} disabled={copying}>
              {copying
                ? copyAsMove
                  ? "Moving…"
                  : "Copying…"
                : copyAsMove
                  ? "Move"
                  : "Copy"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

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
