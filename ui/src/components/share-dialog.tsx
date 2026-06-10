import { useEffect, useId, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { CopyField } from "@/components/copy-field";
import { api, errorMessage } from "@/lib/api";
import { whenMs } from "@/lib/format";

const EXPIRY_OPTIONS = [
  { value: "3600", label: "1 hour" },
  { value: "86400", label: "1 day" },
  { value: "604800", label: "7 days" },
] as const;

/**
 * Mint a signed, time-limited public-read link for one object. The result is
 * shown with its exact expiry so the operator knows what they are handing out.
 */
export function ShareDialog({
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
  const expiryId = useId();
  const [expiry, setExpiry] = useState("3600");
  const [creating, setCreating] = useState(false);
  const [link, setLink] = useState<{ url: string; expiresAtMs: number } | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);

  // Fresh state every time the dialog opens or points at a different object.
  useEffect(() => {
    setExpiry("3600");
    setLink(null);
    setError(null);
    setCreating(false);
  }, [open, bucket, objectKey]);

  async function create() {
    setCreating(true);
    setError(null);
    try {
      const res = await api.shareObject(bucket, objectKey, Number(expiry));
      setLink({
        url: window.location.origin + res.url,
        expiresAtMs: res.expires_at_ms,
      });
      toast.success("Share link created.");
    } catch (e) {
      setError(errorMessage(e, "Could not create share link."));
    } finally {
      setCreating(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Share object</DialogTitle>
          <DialogDescription>
            Create a time-limited public link for{" "}
            <span className="font-mono text-[13px] break-all">{objectKey}</span>.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="flex flex-wrap items-end gap-2">
            <div className="space-y-1.5">
              <Label htmlFor={expiryId} className="text-[13px] text-muted-foreground">
                Link expires after
              </Label>
              <Select value={expiry} onValueChange={setExpiry}>
                <SelectTrigger id={expiryId} className="w-36">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {EXPIRY_OPTIONS.map((o) => (
                    <SelectItem key={o.value} value={o.value}>
                      {o.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button type="button" disabled={creating} onClick={() => void create()}>
              {creating ? "Creating…" : "Create link"}
            </Button>
          </div>

          {error ? (
            <p className="text-[13px] text-destructive" role="alert">
              {error}
            </p>
          ) : null}

          {link ? (
            <div className="space-y-2">
              <CopyField label="Share link" value={link.url} />
              <p className="text-[13px] leading-relaxed text-muted-foreground">
                Anyone with this link can read the object until{" "}
                <span className="tabular-nums">{whenMs(link.expiresAtMs)}</span>.
              </p>
            </div>
          ) : null}
        </div>
      </DialogContent>
    </Dialog>
  );
}
