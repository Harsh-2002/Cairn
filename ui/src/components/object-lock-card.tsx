// Bucket Object Lock (WORM) settings (ARCH 16.5). Object Lock can only be turned on at bucket
// creation, so this card shows the current status and — when enabled — lets the operator edit the
// optional bucket DEFAULT RETENTION stamped onto every new object. Per-object retention and legal
// holds are managed from the object browser.

import { useEffect, useId, useState } from "react";
import { ShieldCheck } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { CardContent } from "@/components/ui/card";
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
import { StatusBadge } from "@/components/status-badge";
import { errorMessage } from "@/lib/api";
import * as s3 from "@/lib/s3";

type DefaultKind = "none" | "GOVERNANCE" | "COMPLIANCE";
type PeriodUnit = "days" | "years";

export function ObjectLockCard({ bucket }: { bucket: string }) {
  const [config, setConfig] = useState<s3.BucketObjectLock | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [mode, setMode] = useState<DefaultKind>("none");
  const [unit, setUnit] = useState<PeriodUnit>("days");
  const [amount, setAmount] = useState("30");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const amountId = useId();

  useEffect(() => {
    let live = true;
    s3.getObjectLockConfig(bucket)
      .then((c) => {
        if (!live) return;
        setConfig(c);
        if (c.defaultMode) {
          setMode(c.defaultMode);
          if (c.defaultYears) {
            setUnit("years");
            setAmount(String(c.defaultYears));
          } else if (c.defaultDays) {
            setUnit("days");
            setAmount(String(c.defaultDays));
          }
        }
      })
      .catch((e) => live && setLoadError(errorMessage(e, "Could not load")))
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [bucket]);

  async function save() {
    setErr(null);
    const next: s3.BucketObjectLock = { enabled: true };
    if (mode !== "none") {
      const n = Number(amount);
      if (!Number.isInteger(n) || n < 1)
        return setErr("Enter a whole number of days or years (≥ 1)");
      next.defaultMode = mode;
      if (unit === "years") next.defaultYears = n;
      else next.defaultDays = n;
    }
    setBusy(true);
    try {
      await s3.putObjectLockConfig(bucket, next);
      setConfig(next);
      toast.success("Object Lock default retention updated");
    } catch (e) {
      setErr(errorMessage(e, "Could not update Object Lock"));
    } finally {
      setBusy(false);
    }
  }

  if (loading) {
    return (
      <CardContent>
        <Skeleton className="h-20 rounded-md" />
      </CardContent>
    );
  }

  if (loadError) {
    return (
      <CardContent>
        <FieldError>{loadError}</FieldError>
      </CardContent>
    );
  }

  if (!config?.enabled) {
    return (
      <CardContent>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <StatusBadge tone="neutral">Not enabled</StatusBadge>
          Object Lock must be enabled when the bucket is created.
        </div>
      </CardContent>
    );
  }

  return (
    <CardContent className="space-y-4">
      {err ? <FieldError>{err}</FieldError> : null}
      <div className="flex items-center gap-2 text-sm">
        <StatusBadge tone="positive">
          <ShieldCheck className="size-3.5" aria-hidden="true" /> Enabled
        </StatusBadge>
        <span className="text-muted-foreground">
          New objects are write-once until their retention lapses.
        </span>
      </div>

      <div className="space-y-2">
        <Label>Default retention</Label>
        <p className="text-[13px] text-muted-foreground">
          Automatically applied to every new object. Per-object overrides and
          legal holds are set from the object browser.
        </p>
        <div className="flex flex-wrap items-end gap-3">
          <div className="space-y-1.5">
            <div className="flex items-center gap-1.5">
              <Label className="text-[13px] text-muted-foreground">Mode</Label>
              <InfoHint label="About retention modes">
                <p className="font-medium">Two retention modes</p>
                <p className="mt-1 text-muted-foreground">
                  <span className="font-medium text-foreground">
                    Governance
                  </span>{" "}
                  blocks deletes and overwrites until the date, but a user with{" "}
                  <code className="font-mono text-[12px]">
                    s3:BypassGovernanceRetention
                  </code>{" "}
                  can lift it early.
                </p>
                <p className="mt-1.5 text-muted-foreground">
                  <span className="font-medium text-foreground">
                    Compliance
                  </span>{" "}
                  is absolute: no one — not even the root account — can shorten
                  or remove it until it expires.
                </p>
              </InfoHint>
            </div>
            <Select
              value={mode}
              onValueChange={(v) => setMode(v as DefaultKind)}
            >
              <SelectTrigger className="w-44">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="none">No default</SelectItem>
                <SelectItem value="GOVERNANCE">Governance</SelectItem>
                <SelectItem value="COMPLIANCE">Compliance</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {mode !== "none" ? (
            <>
              <div className="space-y-1.5">
                <Label
                  htmlFor={amountId}
                  className="text-[13px] text-muted-foreground"
                >
                  Period
                </Label>
                <Input
                  id={amountId}
                  inputMode="numeric"
                  value={amount}
                  className="w-24"
                  onChange={(e) => setAmount(e.target.value)}
                />
              </div>
              <Select
                value={unit}
                onValueChange={(v) => setUnit(v as PeriodUnit)}
              >
                <SelectTrigger className="w-28">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="days">Days</SelectItem>
                  <SelectItem value="years">Years</SelectItem>
                </SelectContent>
              </Select>
            </>
          ) : null}
        </div>
        {mode === "COMPLIANCE" ? (
          <p className="text-[13px] text-amber-600 dark:text-amber-500">
            Compliance retention can&apos;t be shortened or bypassed by anyone
            until it expires.
          </p>
        ) : null}
      </div>

      <div className="flex justify-end border-t pt-3">
        <Button disabled={busy} onClick={save}>
          {busy ? "Saving…" : "Save default retention"}
        </Button>
      </div>
    </CardContent>
  );
}
