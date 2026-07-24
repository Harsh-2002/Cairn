// List and revoke the persistent shares for one object (presigned URLs are
// stateless and never appear here). Opened from the object actions menu.

import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/primitives/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/primitives/dialog";
import { FieldError } from "@/components/field-error";
import { CopyField } from "@/components/copy-field";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { api, errorMessage } from "@/lib/api";
import { whenMs } from "@/lib/format";
import type { ShareRecord, ShareStatus } from "@/lib/types";

const TONE: Record<ShareStatus, StatusTone> = {
  active: "positive",
  expired: "neutral",
  revoked: "negative",
};

export function ManageSharesDialog({
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
  const [shares, setShares] = useState<ShareRecord[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [revoking, setRevoking] = useState<string | null>(null);

  function load() {
    setError(null);
    api
      .listShares(bucket, objectKey)
      .then((r) => setShares(r.shares))
      .catch((e) => setError(errorMessage(e, "Could not load shares.")));
  }

  useEffect(() => {
    if (!open || !objectKey) return;
    setShares(null);
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, bucket, objectKey]);

  async function revoke(token: string) {
    setRevoking(token);
    try {
      await api.revokeShare(bucket, token);
      toast.success("Share revoked");
      load();
    } catch (e) {
      toast.error(errorMessage(e, "Failed to revoke."));
    } finally {
      setRevoking(null);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Shares</DialogTitle>
          <DialogDescription className="break-all font-mono text-[13px]">
            {objectKey}
          </DialogDescription>
        </DialogHeader>

        {error ? (
          <FieldError>{error}</FieldError>
        ) : shares === null ? (
          <p className="py-4 text-sm text-muted-foreground">Loading…</p>
        ) : shares.length === 0 ? (
          <p className="py-4 text-[13px] text-muted-foreground">
            No share links for this object yet. Use “Share” to create one.
          </p>
        ) : (
          <ul className="max-h-[60vh] space-y-3 overflow-y-auto">
            {shares.map((s) => (
              <li key={s.token} className="space-y-2 rounded-lg border p-3">
                <div className="flex items-center justify-between gap-2">
                  <StatusBadge tone={TONE[s.status]}>{s.status}</StatusBadge>
                  {s.status === "active" ? (
                    <Button
                      variant="outline"
                      size="sm"
                      className="text-destructive"
                      disabled={revoking === s.token}
                      onClick={() => void revoke(s.token)}
                    >
                      {revoking === s.token ? "Revoking…" : "Revoke"}
                    </Button>
                  ) : null}
                </div>
                <CopyField
                  label="Link"
                  value={window.location.origin + "/share/" + s.token}
                />
                <p className="text-[13px] text-muted-foreground">
                  {s.disposition === "attachment" ? "Download" : "View"}
                  {s.version_id ? " · pinned version" : ""} ·{" "}
                  {s.expires_at_ms === null
                    ? "never expires"
                    : `expires ${whenMs(s.expires_at_ms)}`}
                </p>
              </li>
            ))}
          </ul>
        )}

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  );
}
