// Import view: bring buckets + objects in from another S3-compatible store
// (MinIO / Garage / R2 / AWS / another Cairn). A connection form starts a job;
// the table below shows every job with live progress and per-row cancel/resume.
// The source secret is sealed server-side and never returned. See ARCH 27.7.

import { useId, useState } from "react";
import { DownloadCloud } from "lucide-react";
import { toast } from "sonner";
import { api, errorMessage } from "@/lib/api";
import { bytes } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import type {
  CreateImportReq,
  ImportBucketMap,
  ImportJobEntry,
  ImportState,
} from "@/lib/types";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Textarea } from "@/components/ui/textarea";
import { TableCell, TableRow } from "@/components/ui/table";

const JOB_COLUMNS: Column[] = [
  { key: "source", label: "Source" },
  { key: "state", label: "Status" },
  { key: "progress", label: "Progress" },
  { key: "objects", label: "Objects", className: "text-right" },
  { key: "actions", label: "", className: "text-right" },
];

function stateBadge(s: ImportState): { tone: StatusTone; label: string } {
  switch (s) {
    case "running":
      return { tone: "warning", label: "Importing" };
    case "completed":
      return { tone: "positive", label: "Completed" };
    case "failed":
      return { tone: "negative", label: "Failed" };
    case "cancelled":
      return { tone: "neutral", label: "Cancelled" };
    default:
      return { tone: "neutral", label: "Pending" };
  }
}

function pct(j: ImportJobEntry): number {
  if (j.state === "completed") return 100;
  if (j.objects_total <= 0) return 0;
  return Math.min(100, Math.round((j.objects_done / j.objects_total) * 100));
}

/** Parse the buckets textarea: one `SRC` or `SRC:DEST` per line; blank = import all. */
function parseBuckets(text: string): ImportBucketMap[] {
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      const [src, dst] = l.split(":");
      const source = src.trim();
      const dest = (dst ?? "").trim() || source;
      return { source, dest };
    });
}

const BLANK = {
  source_endpoint: "",
  source_region: "us-east-1",
  access_key: "",
  secret: "",
  buckets: "",
  workers: "",
  ca_cert: "",
  insecure_skip_verify: false,
};

export function Imports() {
  const res = useResource(() => api.listImports(), []);
  // Live: the server registers an "imports" topic; re-fetch on each pulse (and it
  // degrades to the Refresh path when SSE is unavailable). Throttle to 3s.
  useLiveTopic("imports", res.refresh, 3_000);

  const [form, setForm] = useState(BLANK);
  const [busy, setBusy] = useState(false);
  const set = <K extends keyof typeof form>(k: K, v: (typeof form)[K]) =>
    setForm((f) => ({ ...f, [k]: v }));

  const endpointId = useId();
  const regionId = useId();
  const keyId = useId();
  const secretId = useId();
  const bucketsId = useId();
  const workersId = useId();
  const caId = useId();

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (
      !form.source_endpoint.trim() ||
      !form.source_region.trim() ||
      !form.access_key.trim() ||
      !form.secret
    ) {
      toast.error("Endpoint, region, access key, and secret are all required.");
      return;
    }
    const ca = form.ca_cert.trim();
    if (ca && form.insecure_skip_verify) {
      toast.error("Trust a CA certificate or skip TLS verification — not both.");
      return;
    }
    const body: CreateImportReq = {
      source_endpoint: form.source_endpoint.trim(),
      source_region: form.source_region.trim(),
      access_key: form.access_key.trim(),
      secret: form.secret,
      buckets: parseBuckets(form.buckets),
      insecure_skip_verify: form.insecure_skip_verify,
      ...(form.workers.trim() ? { workers: Number(form.workers) } : {}),
      ...(ca ? { ca_cert: ca } : {}),
    };
    setBusy(true);
    try {
      const { id } = await api.createImport(body);
      toast.success(`Started import ${id.slice(0, 8)}…`);
      // Clear the secret material; keep the connection fields for a follow-up job.
      setForm((f) => ({ ...f, secret: "", ca_cert: "" }));
      res.refresh();
    } catch (err) {
      toast.error(errorMessage(err, "Could not start the import."));
    } finally {
      setBusy(false);
    }
  }

  async function act(fn: () => Promise<unknown>, ok: string, bad: string) {
    try {
      await fn();
      toast.success(ok);
      res.refresh();
    } catch (err) {
      toast.error(errorMessage(err, bad));
    }
  }

  const jobs = res.data?.jobs ?? [];

  return (
    <Page>
      <PageHeader
        title="Import"
        description="Copy buckets and objects in from another S3-compatible store."
      />

      {res.error ? (
        <ErrorAlert
          title="Could not load import jobs"
          message={res.error}
          onRetry={res.refresh}
        />
      ) : null}

      {/* ---- New import ---- */}
      <Card>
        <CardHeader className="gap-1.5">
          <h2 className="text-base font-semibold tracking-tight">New import</h2>
          <p className="text-sm text-muted-foreground">
            Connect to the source with admin credentials. The secret is sealed on
            this server and never shown again.
          </p>
        </CardHeader>
        <CardContent>
          <form onSubmit={onSubmit} className="grid gap-4 md:grid-cols-2">
            <div className="grid gap-1.5">
              <Label htmlFor={endpointId}>Source endpoint</Label>
              <Input
                id={endpointId}
                value={form.source_endpoint}
                onChange={(e) => set("source_endpoint", e.target.value)}
                placeholder="https://s3.source.example:9000"
                autoComplete="off"
                className="font-mono"
              />
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor={regionId}>Region</Label>
              <Input
                id={regionId}
                value={form.source_region}
                onChange={(e) => set("source_region", e.target.value)}
                autoComplete="off"
                className="font-mono"
              />
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor={keyId}>Access key</Label>
              <Input
                id={keyId}
                value={form.access_key}
                onChange={(e) => set("access_key", e.target.value)}
                autoComplete="off"
                className="font-mono"
              />
            </div>
            <div className="grid gap-1.5">
              <Label htmlFor={secretId}>Secret key</Label>
              <Input
                id={secretId}
                type="password"
                value={form.secret}
                onChange={(e) => set("secret", e.target.value)}
                autoComplete="off"
                className="font-mono"
              />
            </div>

            <div className="grid gap-1.5 md:col-span-2">
              <Label htmlFor={bucketsId}>
                Buckets{" "}
                <span className="font-normal text-muted-foreground">
                  — one per line, as <code className="font-mono">source</code> or{" "}
                  <code className="font-mono">source:destination</code>; leave
                  empty to import every bucket
                </span>
              </Label>
              <Textarea
                id={bucketsId}
                value={form.buckets}
                onChange={(e) => set("buckets", e.target.value)}
                rows={3}
                spellCheck={false}
                className="resize-y font-mono text-[13px] leading-relaxed"
              />
            </div>

            <div className="grid gap-1.5">
              <Label htmlFor={workersId}>
                Workers{" "}
                <span className="font-normal text-muted-foreground">
                  — optional
                </span>
              </Label>
              <Input
                id={workersId}
                type="number"
                min={1}
                value={form.workers}
                onChange={(e) => set("workers", e.target.value)}
                placeholder="server default"
                className="w-40"
              />
            </div>

            {/* Transport security — for an https:// source. */}
            <div className="mt-1 grid gap-3 border-t pt-4 md:col-span-2">
              <p className="text-[13px] font-medium text-foreground">
                Transport security
                <span className="ml-1.5 font-normal text-muted-foreground">
                  — for an https:// source
                </span>
              </p>
              <div className="grid gap-1.5">
                <Label htmlFor={caId}>
                  CA certificate{" "}
                  <span className="font-normal text-muted-foreground">
                    — optional
                  </span>
                </Label>
                <Textarea
                  id={caId}
                  value={form.ca_cert}
                  onChange={(e) => set("ca_cert", e.target.value)}
                  rows={4}
                  spellCheck={false}
                  disabled={form.insecure_skip_verify}
                  // tracking-wider keeps a pasted PEM's "-----" armor from blurring
                  // into the adjacent letters at this size.
                  className="field-sizing-fixed max-h-56 resize-y overflow-auto font-mono text-[13px] leading-relaxed tracking-wider disabled:opacity-50"
                />
                <p className="text-[13px] text-muted-foreground">
                  Paste the source's certificate (PEM) when it's signed by a
                  private or self-signed CA. Leave empty to trust the public
                  certificate authorities.
                </p>
              </div>
              <label className="flex items-start gap-3">
                <Checkbox
                  checked={form.insecure_skip_verify}
                  onCheckedChange={(v) =>
                    set("insecure_skip_verify", v === true)
                  }
                  aria-label="Skip TLS certificate verification"
                  className="mt-0.5"
                />
                <span>
                  <span className="block text-sm">
                    Skip certificate verification
                  </span>
                  <span className="block text-[13px] text-muted-foreground">
                    Accepts any certificate — for testing a self-signed source
                    only. Prefer pasting the CA above.
                  </span>
                </span>
              </label>
            </div>

            <div className="md:col-span-2">
              <Button type="submit" disabled={busy} aria-busy={busy}>
                {busy ? "Starting…" : "Start import"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>

      {/* ---- Jobs ---- */}
      <section className="mt-8 space-y-3">
        <h2 className="text-base font-semibold tracking-tight">Import jobs</h2>
        {res.loading ? (
          <DataTable columns={JOB_COLUMNS} minWidth={760}>
            <SkeletonRows
              rows={2}
              widths={["w-48", "w-24", "w-32", "w-16", "w-24"]}
            />
          </DataTable>
        ) : jobs.length > 0 ? (
          <DataTable columns={JOB_COLUMNS} minWidth={760}>
            {jobs.map((j) => {
              const { tone, label } = stateBadge(j.state);
              const active = j.state === "pending" || j.state === "running";
              const resumable = j.state === "failed" || j.state === "cancelled";
              return (
                <TableRow key={j.id}>
                  <TableCell data-label="Source" className="font-mono text-[13px]">
                    <span
                      title={`${j.source_endpoint} (${j.access_key_id})`}
                      className="block max-w-[32ch] truncate"
                    >
                      {j.source_endpoint}
                    </span>
                    <span className="text-muted-foreground">{j.id.slice(0, 8)}</span>
                  </TableCell>
                  <TableCell data-label="Status">
                    <StatusBadge tone={tone}>{label}</StatusBadge>
                  </TableCell>
                  <TableCell data-label="Progress">
                    <div className="flex items-center gap-2">
                      <Progress value={pct(j)} className="h-1.5 w-28" />
                      <span className="tabular-nums text-[13px] text-muted-foreground">
                        {bytes(j.bytes_done)}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell
                    data-label="Objects"
                    className="text-right tabular-nums"
                  >
                    {j.objects_done}
                    {j.objects_total > 0 ? ` / ${j.objects_total}` : ""}
                  </TableCell>
                  <TableCell data-label="" className="text-right">
                    {active ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() =>
                          act(
                            () => api.cancelImport(j.id),
                            "Cancellation requested.",
                            "Could not cancel.",
                          )
                        }
                      >
                        Cancel
                      </Button>
                    ) : resumable ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() =>
                          act(
                            () => api.resumeImport(j.id),
                            "Import resumed.",
                            "Could not resume.",
                          )
                        }
                      >
                        Resume
                      </Button>
                    ) : null}
                  </TableCell>
                </TableRow>
              );
            })}
          </DataTable>
        ) : !res.error ? (
          <EmptyState
            icon={DownloadCloud}
            title="No imports yet"
            body="Start one above to copy buckets and objects in from another S3-compatible store."
          />
        ) : null}
      </section>
    </Page>
  );
}
