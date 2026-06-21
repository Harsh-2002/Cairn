// Per-object Object Lock controls (ARCH 16.5): view and change an object's retention (mode +
// retain-until) and legal hold. Loads the current state on open and applies only what changed.
// Reducing or removing an active GOVERNANCE retention needs the bypass toggle; COMPLIANCE is
// immutable until it expires (the server enforces this and surfaces the error).

import { useEffect, useState } from "react";
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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { FieldError } from "@/components/field-error";
import { InfoHint } from "@/components/info-hint";
import { errorMessage } from "@/lib/api";
import * as s3 from "@/lib/s3";

type Mode = "none" | "GOVERNANCE" | "COMPLIANCE";

/** ISO-8601 → the value a <input type="datetime-local"> expects (local, no seconds/zone). */
function toLocalInput(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

export function ObjectLockDialog({
  bucket,
  objectKey,
  versionId,
  open,
  onOpenChange,
}: {
  bucket: string;
  objectKey: string;
  versionId?: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [mode, setMode] = useState<Mode>("none");
  const [until, setUntil] = useState("");
  const [legalHold, setLegalHold] = useState(false);
  const [bypass, setBypass] = useState(false);
  // The loaded baseline, to detect what changed.
  const [base, setBase] = useState<{ mode: Mode; until: string; hold: boolean }>(
    { mode: "none", until: "", hold: false },
  );

  useEffect(() => {
    if (!open) return;
    let live = true;
    setLoading(true);
    setErr(null);
    setBypass(false);
    Promise.all([
      s3.getObjectRetention(bucket, objectKey, versionId).catch(() => null),
      s3.getObjectLegalHold(bucket, objectKey, versionId).catch(() => false),
    ])
      .then(([ret, hold]) => {
        if (!live) return;
        const m: Mode = ret ? ret.mode : "none";
        const u = ret ? toLocalInput(ret.retainUntil) : "";
        setMode(m);
        setUntil(u);
        setLegalHold(hold);
        setBase({ mode: m, until: u, hold });
      })
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [open, bucket, objectKey, versionId]);

  async function apply() {
    setErr(null);
    const retentionChanged = mode !== base.mode || until !== base.until;
    const holdChanged = legalHold !== base.hold;
    if (!retentionChanged && !holdChanged) {
      onOpenChange(false);
      return;
    }
    if (retentionChanged && mode !== "none" && !until) {
      return setErr("Choose a retain-until date");
    }
    setBusy(true);
    try {
      if (holdChanged) {
        await s3.putObjectLegalHold(bucket, objectKey, legalHold, versionId);
      }
      if (retentionChanged && mode !== "none") {
        const iso = new Date(until).toISOString();
        await s3.putObjectRetention(
          bucket,
          objectKey,
          { mode, retainUntil: iso },
          { bypassGovernance: bypass, versionId },
        );
      }
      toast.success("Object Lock updated");
      onOpenChange(false);
    } catch (e) {
      setErr(errorMessage(e, "Could not update Object Lock"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={busy ? undefined : onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Object Lock</DialogTitle>
          <DialogDescription className="truncate font-mono text-[13px]">
            {objectKey}
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <Skeleton className="h-40 rounded-md" />
        ) : (
          <div className="space-y-5 py-2">
            {err ? <FieldError>{err}</FieldError> : null}

            <label className="flex items-center gap-2 text-sm">
              <Checkbox
                checked={legalHold}
                onCheckedChange={(v) => setLegalHold(v === true)}
              />
              Legal hold
              <span className="text-[13px] text-muted-foreground">
                — blocks deletion until removed
              </span>
            </label>

            <div className="space-y-2">
              <div className="flex items-center gap-1.5">
                <Label>Retention</Label>
                <InfoHint label="About retention modes">
                  <p className="font-medium">Two retention modes</p>
                  <p className="mt-1 text-muted-foreground">
                    <span className="font-medium text-foreground">
                      Governance
                    </span>{" "}
                    blocks deletes and overwrites until the date, but a user
                    holding{" "}
                    <code className="font-mono text-[12px]">
                      s3:BypassGovernanceRetention
                    </code>{" "}
                    can lift it early.
                  </p>
                  <p className="mt-1.5 text-muted-foreground">
                    <span className="font-medium text-foreground">
                      Compliance
                    </span>{" "}
                    is absolute: no one — not even the root account — can
                    shorten or remove it until it expires.
                  </p>
                </InfoHint>
              </div>
              <div className="flex flex-wrap items-end gap-3">
                <Select value={mode} onValueChange={(v) => setMode(v as Mode)}>
                  <SelectTrigger className="w-40">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">None</SelectItem>
                    <SelectItem value="GOVERNANCE">Governance</SelectItem>
                    <SelectItem value="COMPLIANCE">Compliance</SelectItem>
                  </SelectContent>
                </Select>
                {mode !== "none" ? (
                  <Input
                    type="datetime-local"
                    value={until}
                    className="w-56"
                    onChange={(e) => setUntil(e.target.value)}
                  />
                ) : null}
              </div>
              {mode === "GOVERNANCE" ? (
                <p className="text-[13px] text-muted-foreground">
                  Blocks deletes and overwrites until this date. A user with the
                  bypass permission can still remove it early.
                </p>
              ) : null}
              {base.mode === "GOVERNANCE" &&
              (mode !== base.mode || until !== base.until) ? (
                <label className="flex items-start gap-2 text-[13px]">
                  <Checkbox
                    checked={bypass}
                    onCheckedChange={(v) => setBypass(v === true)}
                    className="mt-0.5"
                  />
                  <span>
                    Bypass governance retention
                    <span className="block text-muted-foreground">
                      Needed to shorten or remove an active Governance hold;
                      requires the{" "}
                      <code className="font-mono text-[12px]">
                        s3:BypassGovernanceRetention
                      </code>{" "}
                      permission.
                    </span>
                  </span>
                </label>
              ) : null}
              {mode === "COMPLIANCE" ? (
                <p className="text-[13px] text-amber-600 dark:text-amber-500">
                  Compliance retention can&apos;t be shortened or removed by
                  anyone until it expires.
                </p>
              ) : null}
            </div>
          </div>
        )}

        <DialogFooter>
          <Button
            variant="outline"
            disabled={busy}
            onClick={() => onOpenChange(false)}
          >
            Cancel
          </Button>
          <Button disabled={busy || loading} onClick={apply}>
            {busy ? "Saving…" : "Apply"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
