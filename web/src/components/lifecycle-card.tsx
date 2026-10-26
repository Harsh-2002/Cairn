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
  tags: s3.LifecycleTag[];
  expirationDays: string;
  noncurrentDays: string;
  abortDays: string;
}

function ruleToDraft(r: s3.LifecycleRule): Draft {
  return {
    id: r.id,
    enabled: r.enabled,
    prefix: r.prefix,
    tags: r.tags,
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
    tags: [],
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

  function patchTag(i: number, j: number, change: Partial<s3.LifecycleTag>) {
    setRules((rs) =>
      rs
        ? rs.map((r, ri) =>
            ri === i
              ? {
                  ...r,
                  tags: r.tags.map((tag, tj) =>
                    tj === j ? { ...tag, ...change } : tag,
                  ),
                }
              : r,
          )
        : rs,
    );
  }

  function addTag(i: number) {
    setRules((rs) =>
      rs
        ? rs.map((r, ri) =>
            ri === i ? { ...r, tags: [...r.tags, { key: "", value: "" }] } : r,
          )
        : rs,
    );
  }

  function removeTag(i: number, j: number) {
    setRules((rs) =>
      rs
        ? rs.map((r, ri) =>
            ri === i
              ? { ...r, tags: r.tags.filter((_, tj) => tj !== j) }
              : r,
          )
        : rs,
    );
  }

  function addTtlDraft() {
    setRules((rs) => {
      const n = (rs?.length ?? 0) + 1;
      return [
        ...(rs ?? []),
        {
          ...emptyDraft(n),
          id: `ttl-7d-${n}`,
          tags: [{ key: "ttl", value: "7d" }],
          expirationDays: "7",
        },
      ];
    });
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
      const tags = d.tags
        .map((tag) => ({ key: tag.key.trim(), value: tag.value.trim() }))
        .filter((tag) => tag.key || tag.value);
      if (tags.some((tag) => !tag.key || !tag.value))
        return setErr(`Rule "${d.id}" has an incomplete tag filter`);
      out.push({
        id: d.id.trim(),
        enabled: d.enabled,
        prefix: d.prefix.trim(),
        tags,
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
        uploads. Add a tag filter such as <code>ttl=7d</code> with an expiration
        action to give matching objects a per-object TTL. Storage-class
        transitions are not supported.
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
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-3">
              <Label className="text-[13px]">Tag filters (all must match)</Label>
              <Button
                variant="ghost"
                size="xs"
                disabled={busy}
                onClick={() => addTag(i)}
              >
                <Plus className="size-3" /> Add tag
              </Button>
            </div>
            {r.tags.length === 0 ? (
              <p className="text-[12px] text-muted-foreground">
                Optional. A tag such as <code>ttl=7d</code> plus an expiration
                action scopes the rule to objects carrying that tag.
              </p>
            ) : null}
            {r.tags.map((tag, j) => (
              <div key={j} className="flex flex-wrap items-center gap-2">
                <Input
                  value={tag.key}
                  className="h-8 min-w-32 flex-1"
                  placeholder="ttl"
                  aria-label={`Tag ${j + 1} key`}
                  onChange={(e) => patchTag(i, j, { key: e.target.value })}
                />
                <Input
                  value={tag.value}
                  className="h-8 min-w-32 flex-1"
                  placeholder="7d"
                  aria-label={`Tag ${j + 1} value`}
                  onChange={(e) => patchTag(i, j, { value: e.target.value })}
                />
                <Button
                  variant="ghost"
                  size="icon-sm"
                  aria-label={`Remove tag ${tag.key || j + 1}`}
                  disabled={busy}
                  onClick={() => removeTag(i, j)}
                >
                  <Trash2 className="size-4" />
                </Button>
              </div>
            ))}
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

      <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            disabled={busy}
            onClick={() =>
              setRules((rs) => [...(rs ?? []), emptyDraft((rs?.length ?? 0) + 1)])
            }
          >
            <Plus className="size-4" /> Add rule
          </Button>
          <Button variant="outline" disabled={busy} onClick={addTtlDraft}>
            <CalendarClock className="size-4" /> Add 7-day TTL
          </Button>
        </div>
        <Button disabled={busy} onClick={save}>
          {busy ? "Saving…" : "Save lifecycle"}
        </Button>
      </div>
    </CardContent>
  );
}
