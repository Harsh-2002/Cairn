// Per-bucket opt-in for the object TTL extension (`X-Amz-Meta-Object-Expires`). The setting only
// enables interpretation of the header; an object uploaded without it never expires.

import { useEffect, useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/primitives/button";
import { CardContent } from "@/components/primitives/card";
import { Checkbox } from "@/components/primitives/checkbox";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";
import { Skeleton } from "@/components/primitives/skeleton";
import { FieldError } from "@/components/field-error";
import { api, errorMessage } from "@/lib/api";

const DEFAULT_MAX_DAYS = 365;
const MIN_DAYS = 1;
const MAX_DAYS = 3_650;

export function ObjectTtlCard({ bucket }: { bucket: string }) {
  const [enabled, setEnabled] = useState(false);
  const [maxDays, setMaxDays] = useState(String(DEFAULT_MAX_DAYS));
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    api
      .getBucketConfig(bucket)
      .then((config) => {
        if (!live) return;
        const ttl = config.object_ttl;
        setEnabled(ttl?.enabled === true);
        if (ttl?.max_ttl_secs) {
          setMaxDays(String(Math.max(1, Math.round(ttl.max_ttl_secs / 86_400))));
        }
      })
      .catch((e) =>
        live ? setErr(errorMessage(e, "Could not load object TTL")) : undefined,
      )
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [bucket]);

  async function save() {
    setErr(null);
    const days = Number(maxDays);
    if (
      enabled &&
      (!Number.isInteger(days) || days < MIN_DAYS || days > MAX_DAYS)
    ) {
      setErr(`Maximum TTL must be between ${MIN_DAYS} and ${MAX_DAYS} days`);
      return;
    }
    setBusy(true);
    try {
      await api.setObjectTtl(
        bucket,
        enabled,
        enabled ? days * 86_400 : undefined,
      );
      toast.success(enabled ? "Object TTL enabled" : "Object TTL disabled");
    } catch (e) {
      setErr(errorMessage(e, "Could not update object TTL"));
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

  return (
    <CardContent className="space-y-4">
      {err ? <FieldError>{err}</FieldError> : null}
      <p className="text-[13px] text-muted-foreground">
        Off by default. When enabled, uploads may set{" "}
        <code>X-Amz-Meta-Object-Expires</code> to a relative duration such as{" "}
        <code>+2h</code> or an RFC3339 timestamp. Objects uploaded without the
        header never expire. TTL cannot be combined with Object Lock.
      </p>
      <label className="flex items-center gap-2 text-sm">
        <Checkbox
          checked={enabled}
          onCheckedChange={(value) => setEnabled(value === true)}
        />
        Enable per-object TTL for this bucket
      </label>
      <div className="max-w-xs space-y-1.5">
        <Label htmlFor="object-ttl-max" className="text-[13px]">
          Maximum TTL (days)
        </Label>
        <Input
          id="object-ttl-max"
          inputMode="numeric"
          value={maxDays}
          disabled={!enabled}
          onChange={(e) => setMaxDays(e.target.value)}
        />
        <p className="text-[12px] text-muted-foreground">
          Defaults to 365 days; the server allows up to 3,650 days.
        </p>
      </div>
      <div className="flex justify-end">
        <Button disabled={busy} onClick={save}>
          {busy ? "Saving…" : "Save TTL setting"}
        </Button>
      </div>
    </CardContent>
  );
}
