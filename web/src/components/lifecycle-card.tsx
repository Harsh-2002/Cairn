// Lifecycle rules editor (S3 ?lifecycle). Cairn supports expiration, noncurrent-version expiration,
// and aborting incomplete multipart uploads — storage-class transition is intentionally not
// implemented (the server rejects it), so this editor does not offer it. Loads the current config
// and replaces it on save.

import { useEffect, useState } from "react";
import { CalendarClock, Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/primitives/button";
import { CardContent } from "@/components/primitives/card";
import { Checkbox } from "@/components/primitives/checkbox";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";
import { Skeleton } from "@/components/primitives/skeleton";
import { EmptyState } from "@/components/empty-state";
import { FieldError } from "@/components/field-error";
import { errorMessage } from "@/lib/api";
import * as s3 from "@/lib/s3";

interface Draft {
  id: string;
  enabled: boolean;
  prefix: string;
  expirationDays: string;
  noncurrentDays: string;
  abortDays: string;
}

function ruleToDraft(r: s3.LifecycleRule): Draft {
  return {
    id: r.id,
    enabled: r.enabled,
    prefix: r.prefix,
    expirationDays: r.expirationDays ? String(r.expirationDays) : "",
    noncurrentDays: r.noncurrentDays ? String(r.noncurrentDays) : "",
    abortDays: r.abortDays ? String(r.abortDays) : "",
  };
}

function emptyDraft(n: number): Draft {
  return {
    id: `rule-${n}`,
    enabled: true,
    prefix: "",
    expirationDays: "",
    noncurrentDays: "",
    abortDays: "",
  };
}

const numField = (s: string): number | undefined =>
  s.trim() ? Number(s) : undefined;

export function LifecycleCard({ bucket }: { bucket: string }) {
  const [rules, setRules] = useState<Draft[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    let live = true;
    s3.getLifecycle(bucket)
      .then((rs) => live && setRules(rs.map(ruleToDraft)))
      .catch((e) => live && setErr(errorMessage(e, "Could not load lifecycle")))
      .finally(() => live && setLoading(false));
    return () => {
      live = false;
    };
  }, [bucket]);

  function patch(i: number, change: Partial<Draft>) {
    setRules((rs) =>
      rs ? rs.map((r, j) => (j === i ? { ...r, ...change } : r)) : rs,
    );
  }

  async function save() {
    if (!rules) return;
    setErr(null);
    const out: s3.LifecycleRule[] = [];
    for (const d of rules) {
      if (!d.id.trim()) return setErr("Each rule needs an id");
      const exp = numField(d.expirationDays);
      const nc = numField(d.noncurrentDays);
      const ab = numField(d.abortDays);
      if (!exp && !nc && !ab)
        return setErr(`Rule "${d.id}" needs at least one action`);
      out.push({
        id: d.id.trim(),
        enabled: d.enabled,
        prefix: d.prefix.trim(),
        expirationDays: exp,
        noncurrentDays: nc,
        abortDays: ab,
      });
    }
    setBusy(true);
    try {
      if (out.length === 0) await s3.deleteLifecycle(bucket);
      else await s3.putLifecycle(bucket, out);
      toast.success("Lifecycle updated");
    } catch (e) {
      setErr(errorMessage(e, "Could not update lifecycle"));
    } finally {
      setBusy(false);
    }
  }

  if (loading)
    return (
      <CardContent>
        <Skeleton className="h-20 rounded-md" />
      </CardContent>
    );

  return (
    <CardContent className="space-y-4">
      {err ? <FieldError>{err}</FieldError> : null}
      <p className="text-[13px] text-muted-foreground">
        Expire objects, expire noncurrent versions, and abort stale multipart
        uploads. Storage-class transitions are not supported.
      </p>

      {rules && rules.length === 0 ? (
        <EmptyState
          icon={CalendarClock}
          title="No lifecycle rules"
          body="Add a rule to expire objects or clean up old versions automatically."
        />
      ) : null}

      {rules?.map((r, i) => (
        <div key={i} className="space-y-3 rounded-md border p-4">
          <div className="flex flex-wrap items-center gap-3">
            <Input
              value={r.id}
              className="h-8 w-40"
              placeholder="rule id"
              onChange={(e) => patch(i, { id: e.target.value })}
            />
            <label className="flex items-center gap-1.5 text-sm">
              <Checkbox
                checked={r.enabled}
                onCheckedChange={(v) => patch(i, { enabled: v === true })}
              />
              Enabled
            </label>
            <div className="ml-auto">
              <Button
                variant="ghost"
                size="icon"
                aria-label={`Remove rule ${r.id}`}
                disabled={busy}
                onClick={() =>
                  setRules((rs) => rs?.filter((_, j) => j !== i) ?? rs)
                }
              >
                <Trash2 className="size-4" />
              </Button>
            </div>
          </div>
          <div className="space-y-1.5">
            <Label className="text-[13px]">Key prefix (blank = whole bucket)</Label>
            <Input
              value={r.prefix}
              placeholder="logs/"
              onChange={(e) => patch(i, { prefix: e.target.value })}
            />
          </div>
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-1.5">
              <Label className="text-[13px]">Expire after (days)</Label>
              <Input
                inputMode="numeric"
                value={r.expirationDays}
                placeholder="30"
                onChange={(e) => patch(i, { expirationDays: e.target.value })}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-[13px]">Noncurrent expire (days)</Label>
              <Input
                inputMode="numeric"
                value={r.noncurrentDays}
                placeholder="—"
                onChange={(e) => patch(i, { noncurrentDays: e.target.value })}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-[13px]">Abort multipart (days)</Label>
              <Input
                inputMode="numeric"
                value={r.abortDays}
                placeholder="7"
                onChange={(e) => patch(i, { abortDays: e.target.value })}
              />
            </div>
          </div>
        </div>
      ))}

      <div className="flex items-center justify-between border-t pt-4">
        <Button
          variant="outline"
          disabled={busy}
          onClick={() =>
            setRules((rs) => [...(rs ?? []), emptyDraft((rs?.length ?? 0) + 1)])
          }
        >
          <Plus className="size-4" /> Add rule
        </Button>
        <Button disabled={busy} onClick={save}>
          {busy ? "Saving…" : "Save lifecycle"}
        </Button>
      </div>
    </CardContent>
  );
}
