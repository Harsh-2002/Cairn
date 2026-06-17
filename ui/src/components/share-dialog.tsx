// Share one object two ways (ARCH §15.8):
//  • "Share link" (default): a persistent, revocable Cairn share — pick a duration
//    or "Never expires", view-in-browser vs force-download, get a /p/{token} link.
//  • "S3 link": a standard SigV4 presigned URL (download or upload), interoperable
//    with any S3 tool, capped at 7 days, stateless (not revocable).

import { useEffect, useId, useState } from "react";
import { TriangleAlert } from "lucide-react";
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
import { FieldError } from "@/components/field-error";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { CopyField } from "@/components/copy-field";
import { api, errorMessage } from "@/lib/api";
import { whenMs } from "@/lib/format";
import type { ShareDisposition } from "@/lib/types";

// Persistent durations include "forever"; presigned is capped at 7 days.
const SECS = { hour: 3600, day: 86400, week: 604800 } as const;

export function ShareDialog({
  bucket,
  objectKey,
  versionId,
  open,
  onOpenChange,
}: {
  bucket: string;
  objectKey: string;
  /** When set, shares are pinned to this version; otherwise they follow the latest. */
  versionId?: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const idp = useId();

  // --- persistent "share link" tab ---
  const [pExpiry, setPExpiry] = useState("86400"); // default 24h
  const [pDisposition, setPDisposition] = useState<ShareDisposition>("inline");
  const [pFilename, setPFilename] = useState("");
  const [pBusy, setPBusy] = useState(false);
  const [pLink, setPLink] = useState<{ url: string; expiresAtMs: number | null } | null>(null);

  // --- presigned "S3 link" tab ---
  const [sMethod, setSMethod] = useState<"GET" | "PUT">("GET");
  const [sExpiry, setSExpiry] = useState("86400");
  const [sContentType, setSContentType] = useState("");
  const [sBusy, setSBusy] = useState(false);
  const [sLink, setSLink] = useState<{ url: string; expiresAtMs: number } | null>(null);

  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setPExpiry("86400");
    setPDisposition("inline");
    setPFilename("");
    setPLink(null);
    setSMethod("GET");
    setSExpiry("86400");
    setSContentType("");
    setSLink(null);
    setError(null);
  }, [open, bucket, objectKey]);

  async function createShareLink() {
    setPBusy(true);
    setError(null);
    try {
      const res = await api.createShare(bucket, {
        key: objectKey,
        expires_in_secs: pExpiry === "forever" ? null : Number(pExpiry),
        disposition: pDisposition,
        filename:
          pDisposition === "attachment" && pFilename.trim()
            ? pFilename.trim()
            : null,
        version_id: versionId ?? null,
      });
      setPLink({
        url: window.location.origin + res.url,
        expiresAtMs: res.expires_at_ms,
      });
      toast.success("Share link created");
    } catch (e) {
      setError(errorMessage(e, "Could not create the share link."));
    } finally {
      setPBusy(false);
    }
  }

  async function createPresigned() {
    setSBusy(true);
    setError(null);
    try {
      const res = await api.presignShare(bucket, {
        key: objectKey,
        method: sMethod,
        expires_in_secs: Number(sExpiry),
        version_id: versionId ?? null,
        response_content_disposition:
          sMethod === "GET" && pDisposition === "attachment"
            ? "attachment"
            : null,
        content_type:
          sMethod === "PUT" && sContentType.trim() ? sContentType.trim() : null,
      });
      setSLink({ url: res.url, expiresAtMs: res.expires_at_ms });
      toast.success("Presigned URL created");
    } catch (e) {
      setError(errorMessage(e, "Could not create the presigned URL."));
    } finally {
      setSBusy(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Share object</DialogTitle>
          <DialogDescription className="break-all font-mono text-[13px]">
            {objectKey}
            {versionId ? (
              <span className="text-muted-foreground"> @ {versionId}</span>
            ) : null}
          </DialogDescription>
        </DialogHeader>

        <Tabs defaultValue="link">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="link">Share link</TabsTrigger>
            <TabsTrigger value="s3">S3 link</TabsTrigger>
          </TabsList>

          {/* Persistent, revocable share */}
          <TabsContent value="link" className="space-y-4 pt-3">
            <div className="grid gap-1.5">
              <Label htmlFor={`${idp}-pe`}>Expires</Label>
              <Select value={pExpiry} onValueChange={setPExpiry}>
                <SelectTrigger id={`${idp}-pe`} className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={String(SECS.hour)}>1 hour</SelectItem>
                  <SelectItem value={String(SECS.day)}>24 hours</SelectItem>
                  <SelectItem value={String(SECS.week)}>7 days</SelectItem>
                  <SelectItem value="forever">Never expires</SelectItem>
                </SelectContent>
              </Select>
              {pExpiry === "forever" ? (
                <p className="text-[13px] text-warning">
                  A forever link works until you revoke it.
                </p>
              ) : null}
            </div>

            <div className="grid gap-1.5">
              <Label htmlFor={`${idp}-pd`}>Delivery</Label>
              <Select
                value={pDisposition}
                onValueChange={(v) => setPDisposition(v as ShareDisposition)}
              >
                <SelectTrigger id={`${idp}-pd`} className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="inline">View in browser</SelectItem>
                  <SelectItem value="attachment">Force download</SelectItem>
                </SelectContent>
              </Select>
              {pDisposition === "attachment" ? (
                <Input
                  placeholder="Download filename (optional)"
                  value={pFilename}
                  onChange={(e) => setPFilename(e.target.value)}
                  className="font-mono"
                  aria-label="Download filename"
                />
              ) : null}
            </div>

            <Button onClick={() => void createShareLink()} disabled={pBusy}>
              {pBusy ? "Creating…" : "Create share link"}
            </Button>

            {pLink ? (
              <div className="space-y-2">
                <CopyField label="Share link" value={pLink.url} />
                <p className="text-[13px] text-muted-foreground">
                  {pLink.expiresAtMs === null ? (
                    "Anyone with this link can read the object until you revoke it."
                  ) : (
                    <>
                      Works until{" "}
                      <span className="tabular-nums">
                        {whenMs(pLink.expiresAtMs)}
                      </span>
                      , or until revoked.
                    </>
                  )}
                </p>
              </div>
            ) : null}
          </TabsContent>

          {/* Stateless, interoperable presigned URL */}
          <TabsContent value="s3" className="space-y-4 pt-3">
            <p className="text-[13px] text-muted-foreground">
              A standard S3 presigned URL — works with any S3 tool or a plain
              browser. Stateless, so it can’t be revoked or listed, and is capped
              at 7 days.
            </p>
            <div className="grid gap-1.5">
              <Label htmlFor={`${idp}-sm`}>Type</Label>
              <Select
                value={sMethod}
                onValueChange={(v) => setSMethod(v as "GET" | "PUT")}
              >
                <SelectTrigger id={`${idp}-sm`} className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="GET">Download (GET)</SelectItem>
                  <SelectItem value="PUT">Upload (PUT)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor={`${idp}-se`}>Expires</Label>
              <Select value={sExpiry} onValueChange={setSExpiry}>
                <SelectTrigger id={`${idp}-se`} className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={String(SECS.hour)}>1 hour</SelectItem>
                  <SelectItem value={String(SECS.day)}>24 hours</SelectItem>
                  <SelectItem value={String(SECS.week)}>7 days (max)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {sMethod === "PUT" ? (
              <>
                <div className="grid gap-1.5">
                  <Label htmlFor={`${idp}-ct`}>Pin content type (optional)</Label>
                  <Input
                    id={`${idp}-ct`}
                    placeholder="e.g. image/png"
                    value={sContentType}
                    onChange={(e) => setSContentType(e.target.value)}
                    className="font-mono"
                  />
                </div>
                <div className="flex items-start gap-2 text-[13px] text-warning">
                  <TriangleAlert
                    aria-hidden="true"
                    className="mt-0.5 size-4 shrink-0"
                  />
                  <span>
                    Anyone with this link can upload to{" "}
                    <span className="font-mono">{objectKey}</span> as you until
                    it expires. It can’t be revoked.
                  </span>
                </div>
              </>
            ) : null}

            <Button onClick={() => void createPresigned()} disabled={sBusy}>
              {sBusy ? "Creating…" : "Create presigned URL"}
            </Button>

            {sLink ? (
              <div className="space-y-2">
                <CopyField
                  label={sMethod === "PUT" ? "Upload URL" : "Download URL"}
                  value={sLink.url}
                />
                <p className="text-[13px] text-muted-foreground">
                  Valid until{" "}
                  <span className="tabular-nums">
                    {whenMs(sLink.expiresAtMs)}
                  </span>
                  .
                </p>
              </div>
            ) : null}
          </TabsContent>
        </Tabs>

        <FieldError>{error}</FieldError>

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  );
}
