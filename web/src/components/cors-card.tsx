// CORS configuration editor (S3 ?cors). Each rule lists allowed origins, methods, request headers,
// exposed response headers, and a max-age. Loads the current config and replaces it on save.

import { useEffect, useState } from "react";
import { Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/primitives/button";
import { CardContent } from "@/components/primitives/card";
import { Checkbox } from "@/components/primitives/checkbox";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";
import { Skeleton } from "@/components/primitives/skeleton";
import { Textarea } from "@/components/primitives/textarea";
import { EmptyState } from "@/components/empty-state";
import { FieldError } from "@/components/field-error";
import { errorMessage } from "@/lib/api";
import * as s3 from "@/lib/s3";
import { Globe } from "lucide-react";

const METHODS = ["GET", "PUT", "POST", "DELETE", "HEAD"];

const linesToList = (s: string): string[] =>
  s
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);

interface Draft {
  origins: string;
  methods: string[];
  headers: string;
  expose: string;
  maxAge: string;
}

function ruleToDraft(r: s3.CorsRule): Draft {
  return {
    origins: r.allowedOrigins.join("\n"),
    methods: r.allowedMethods,
    headers: r.allowedHeaders.join("\n"),
    expose: r.exposeHeaders.join("\n"),
    maxAge: r.maxAgeSeconds !== undefined ? String(r.maxAgeSeconds) : "",
  };
}

function emptyDraft(): Draft {
  return { origins: "*", methods: ["GET"], headers: "*", expose: "", maxAge: "3000" };
}

export function CorsCard({ bucket }: { bucket: string }) {
  const [rules, setRules] = useState<Draft[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    let live = true;
    s3.getCors(bucket)
      .then((rs) => live && setRules(rs.map(ruleToDraft)))
      .catch((e) => live && setErr(errorMessage(e, "Could not load CORS")))
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
    const out: s3.CorsRule[] = [];
    for (const d of rules) {
      const origins = linesToList(d.origins);
      if (origins.length === 0) return setErr("Each rule needs an origin");
      if (d.methods.length === 0) return setErr("Each rule needs a method");
      out.push({
        allowedOrigins: origins,
        allowedMethods: d.methods,
        allowedHeaders: linesToList(d.headers),
        exposeHeaders: linesToList(d.expose),
        maxAgeSeconds: d.maxAge ? Number(d.maxAge) : undefined,
      });
    }
    setBusy(true);
    try {
      if (out.length === 0) await s3.deleteCors(bucket);
      else await s3.putCors(bucket, out);
      toast.success("CORS updated");
    } catch (e) {
      setErr(errorMessage(e, "Could not update CORS"));
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

      {rules && rules.length === 0 ? (
        <EmptyState
          icon={Globe}
          title="No CORS rules"
          body="Add a rule to let browsers from other origins call this bucket."
        />
      ) : null}

      {rules?.map((r, i) => (
        <div key={i} className="space-y-3 rounded-md border p-4">
          <div className="flex items-center justify-between">
            <span className="text-[13px] font-medium text-muted-foreground">
              Rule {i + 1}
            </span>
            <Button
              variant="ghost"
              size="icon"
              aria-label={`Remove rule ${i + 1}`}
              disabled={busy}
              onClick={() =>
                setRules((rs) => rs?.filter((_, j) => j !== i) ?? rs)
              }
            >
              <Trash2 className="size-4" />
            </Button>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label className="text-[13px]">Allowed origins (one per line)</Label>
              <Textarea
                rows={2}
                value={r.origins}
                placeholder="https://app.example.com"
                onChange={(e) => patch(i, { origins: e.target.value })}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-[13px]">Allowed methods</Label>
              <div className="flex flex-wrap gap-3 pt-1.5">
                {METHODS.map((m) => (
                  <label key={m} className="flex items-center gap-1.5 text-sm">
                    <Checkbox
                      checked={r.methods.includes(m)}
                      onCheckedChange={(v) =>
                        patch(i, {
                          methods:
                            v === true
                              ? [...r.methods, m]
                              : r.methods.filter((x) => x !== m),
                        })
                      }
                    />
                    {m}
                  </label>
                ))}
              </div>
            </div>
            <div className="space-y-1.5">
              <Label className="text-[13px]">
                Allowed request headers (one per line)
              </Label>
              <Textarea
                rows={2}
                value={r.headers}
                placeholder="*"
                onChange={(e) => patch(i, { headers: e.target.value })}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-[13px]">
                Exposed response headers (one per line)
              </Label>
              <Textarea
                rows={2}
                value={r.expose}
                placeholder="ETag"
                onChange={(e) => patch(i, { expose: e.target.value })}
              />
            </div>
            <div className="space-y-1.5">
              <Label className="text-[13px]">Max age (seconds)</Label>
              <Input
                inputMode="numeric"
                value={r.maxAge}
                className="w-32"
                onChange={(e) => patch(i, { maxAge: e.target.value })}
              />
            </div>
          </div>
        </div>
      ))}

      <div className="flex items-center justify-between border-t pt-4">
        <Button
          variant="outline"
          disabled={busy}
          onClick={() => setRules((rs) => [...(rs ?? []), emptyDraft()])}
        >
          <Plus className="size-4" /> Add rule
        </Button>
        <Button disabled={busy} onClick={save}>
          {busy ? "Saving…" : "Save CORS"}
        </Button>
      </div>
    </CardContent>
  );
}
