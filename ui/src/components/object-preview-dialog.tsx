import { useEffect, useRef, useState } from "react";
import { Download, Loader2 } from "lucide-react";
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
import { errorMessage } from "@/lib/api";
import { getObjectBlob } from "@/lib/s3";

const MAX_TEXT_BYTES = 256 * 1024;

const IMAGE_EXT = /\.(png|jpe?g|gif|webp|svg|avif)$/i;
const TEXT_EXT =
  /\.(txt|json|xml|ya?ml|csv|m?jsx?|tsx?|md|toml|ini|log|sh)$/i;

function isImage(type: string, key: string): boolean {
  return type.startsWith("image/") || IMAGE_EXT.test(key);
}

function isTexty(type: string, key: string): boolean {
  return (
    type.startsWith("text/") ||
    type.includes("json") ||
    type.includes("xml") ||
    type.includes("yaml") ||
    type.includes("csv") ||
    type.includes("javascript") ||
    type.includes("typescript") ||
    TEXT_EXT.test(key)
  );
}

type PreviewState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "image"; url: string }
  | { kind: "text"; text: string }
  | { kind: "none" }
  | { kind: "error"; message: string };

/**
 * Inline object preview: images render directly, small text-like files render
 * in a scrollable <pre>, everything else gets a quiet fallback with Download.
 */
export function ObjectPreviewDialog({
  bucket,
  objectKey,
  open,
  onOpenChange,
}: {
  bucket: string;
  objectKey: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [state, setState] = useState<PreviewState>({ kind: "idle" });
  const [downloading, setDownloading] = useState(false);

  // The live object URL (if any) plus a ticket counter so a stale fetch never
  // creates an orphaned URL or clobbers a newer preview (StrictMode-safe: the
  // URL is only created after the ticket check, so a discarded run leaks nothing).
  const urlRef = useRef<string | null>(null);
  const seqRef = useRef(0);

  function revokeUrl() {
    if (urlRef.current) {
      URL.revokeObjectURL(urlRef.current);
      urlRef.current = null;
    }
  }

  useEffect(() => {
    if (!open || !objectKey) {
      // Closed (or no target): cancel any in-flight fetch and free the URL.
      seqRef.current++;
      revokeUrl();
      setState({ kind: "idle" });
      return;
    }
    const ticket = ++seqRef.current;
    setState({ kind: "loading" });
    void (async () => {
      try {
        const blob = await getObjectBlob(bucket, objectKey);
        if (ticket !== seqRef.current) return;
        const type = blob.type || "";
        if (isImage(type, objectKey)) {
          revokeUrl();
          const url = URL.createObjectURL(blob);
          urlRef.current = url;
          setState({ kind: "image", url });
        } else if (isTexty(type, objectKey) && blob.size <= MAX_TEXT_BYTES) {
          const text = await blob.slice(0, MAX_TEXT_BYTES).text();
          if (ticket !== seqRef.current) return;
          setState({ kind: "text", text });
        } else {
          setState({ kind: "none" });
        }
      } catch (e) {
        if (ticket !== seqRef.current) return;
        setState({ kind: "error", message: errorMessage(e, "Preview failed.") });
      }
    })();
  }, [open, bucket, objectKey]);

  // Free the object URL if the dialog unmounts while open.
  useEffect(() => {
    return () => {
      seqRef.current++;
      revokeUrl();
    };
  }, []);

  async function download() {
    setDownloading(true);
    try {
      const blob = await getObjectBlob(bucket, objectKey);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = objectKey.split("/").pop() || objectKey;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      toast.error(errorMessage(e, "Download failed."));
    } finally {
      setDownloading(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle className="truncate pr-6 font-mono text-[15px]" title={objectKey}>
            {objectKey}
          </DialogTitle>
          <DialogDescription>
            Read-only preview of this object in{" "}
            <span className="font-mono text-[13px]">{bucket}</span>.
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-24">
          {state.kind === "loading" ? (
            <div className="flex h-24 items-center justify-center gap-2 text-sm text-muted-foreground">
              <Loader2 aria-hidden="true" className="size-4 animate-spin" />
              Loading preview…
            </div>
          ) : state.kind === "image" ? (
            <img
              src={state.url}
              alt={objectKey}
              className="max-h-[70vh] w-full object-contain"
            />
          ) : state.kind === "text" ? (
            <pre className="max-h-[60vh] overflow-auto rounded-md border bg-muted/50 p-3 font-mono text-xs">
              {state.text}
            </pre>
          ) : state.kind === "error" ? (
            <p className="text-[13px] text-destructive" role="alert">
              {state.message}
            </p>
          ) : state.kind === "none" ? (
            <p className="text-sm text-muted-foreground">
              Preview isn&apos;t available for this file type or size.
            </p>
          ) : null}
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={downloading || state.kind === "loading"}
            onClick={() => void download()}
          >
            <Download aria-hidden="true" />
            {downloading ? "Downloading…" : "Download"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
