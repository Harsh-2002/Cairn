// Import view: bring buckets + objects in from another S3-compatible store
// (MinIO / Garage / R2 / AWS / another Cairn). A connection form starts a job;
// the table below shows every job with live progress, per-row cancel/resume, and
// the reason a failed job failed. The source secret is sealed server-side and
// never returned. See ARCH 27.7.

import { useId, useMemo, useState } from "react";
import {
  ArrowRight,
  Check,
  CircleAlert,
  Copy,
  DownloadCloud,
} from "lucide-react";
import { toast } from "sonner";
import { api, errorMessage } from "@/lib/api";
import { copyText } from "@/components/copy-field";
import { bytes } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import type {
  CreateImportReq,
  ImportBucketMap,
  ImportJobEntry,
  ImportState,
} from "@/lib/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { FieldError } from "@/components/field-error";
import { Page, PageHeader } from "@/components/page-header";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { TableCell, TableRow } from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const JOB_COLUMNS: Column[] = [
  { key: "source", label: "Source" },
  { key: "state", label: "Status" },
  { key: "progress", label: "Progress" },
  { key: "objects", label: "Objects", className: "text-right" },
  { key: "actions", label: "Actions", srOnly: true, className: "text-right" },
];

// A running import is not a warning: keep semantic color for genuine problems.
// Progress is conveyed by the bar and the object counts, not by an amber badge.
function stateBadge(s: ImportState): { tone: StatusTone; label: string } {
  switch (s) {
    case "running":
      return { tone: "neutral", label: "Importing" };
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

// Percent complete, or `null` when it can't be known yet (a running job still
// enumerating the source has no total) — the caller renders an indeterminate bar
// rather than a misleading static 0%.
function pct(j: ImportJobEntry): number | null {
  if (j.state === "completed") return 100;
  if (j.objects_total > 0) {
    return Math.min(100, Math.round((j.objects_done / j.objects_total) * 100));
  }
  if (j.bytes_total > 0) {
    return Math.min(100, Math.round((j.bytes_done / j.bytes_total) * 100));
  }
  return j.state === "running" ? null : 0;
}

/** Parse the buckets textarea: one `SRC` or `SRC:DEST` per line; blank = import all. */
function parseBuckets(text: string): ImportBucketMap[] {
  return text
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      // Split on the FIRST colon only, so a stray second colon isn't silently dropped.
      const i = l.indexOf(":");
      const source = (i === -1 ? l : l.slice(0, i)).trim();
      const dest = (i === -1 ? "" : l.slice(i + 1)).trim() || source;
      return { source, dest };
    });
}

/** Render bucket mappings back to the textarea's `SRC` / `SRC:DEST` line form. The textarea stays
 * the single source of truth; the picker is a live editor over it. */
function serialize(maps: ImportBucketMap[]): string {
  return maps
    .map((m) => (m.dest && m.dest !== m.source ? `${m.source}:${m.dest}` : m.source))
    .join("\n");
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
  const [formError, setFormError] = useState<string | null>(null);
  // Set once the form has been submitted, so required fields only turn `aria-invalid`
  // after an attempt — never on a pristine form.
  const [submitted, setSubmitted] = useState(false);
  const [confirmAll, setConfirmAll] = useState(false);
  // The "Fetch buckets" probe: the source's bucket names (null = not yet fetched).
  const [sourceBuckets, setSourceBuckets] = useState<string[] | null>(null);
  const [probing, setProbing] = useState(false);
  const [probeError, setProbeError] = useState<string | null>(null);
  // The job id most recently copied to the clipboard (drives the row's copy icon).
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const set = <K extends keyof typeof form>(k: K, v: (typeof form)[K]) =>
    setForm((f) => ({ ...f, [k]: v }));
  // A required field is invalid only once submitted and still empty; typing clears it live.
  const invalid = (v: string) => (submitted && !v.trim() ? true : undefined);

  const endpointId = useId();
  const regionId = useId();
  const keyId = useId();
  const secretId = useId();
  const bucketsId = useId();
  const bucketsHelpId = useId();
  const workersId = useId();
  const caId = useId();
  const formErrorId = useId();
  // Point the required fields at the form-level error message when it's showing.
  const errDesc = formError ? formErrorId : undefined;

  // The textarea is canonical; parse it once for the picker's checked/rename state.
  const maps = useMemo(() => parseBuckets(form.buckets), [form.buckets]);
  const selected = useMemo(
    () => new Map(maps.map((m) => [m.source, m.dest])),
    [maps],
  );
  const importsAll = maps.length === 0;
  const selectedCount = sourceBuckets
    ? sourceBuckets.filter((s) => selected.has(s)).length
    : 0;

  // Picker edits, all routed through the textarea so the two stay in lockstep.
  const toggleSource = (src: string, on: boolean) =>
    set(
      "buckets",
      serialize(
        on
          ? [...maps.filter((m) => m.source !== src), { source: src, dest: src }]
          : maps.filter((m) => m.source !== src),
      ),
    );
  const renameSource = (src: string, dest: string) =>
    set(
      "buckets",
      serialize(
        maps.map((m) =>
          m.source === src ? { source: src, dest: dest.trim() || src } : m,
        ),
      ),
    );
  const selectAllSources = (on: boolean) => {
    if (!sourceBuckets) return;
    if (on) {
      const have = new Set(maps.map((m) => m.source));
      const added = sourceBuckets
        .filter((s) => !have.has(s))
        .map((s) => ({ source: s, dest: s }));
      set("buckets", serialize([...maps, ...added]));
    } else {
      const drop = new Set(sourceBuckets);
      set("buckets", serialize(maps.filter((m) => !drop.has(m.source))));
    }
  };

  async function fetchBuckets() {
    // The probe needs the connection fields, not the bucket selection.
    if (
      !form.source_endpoint.trim() ||
      !form.source_region.trim() ||
      !form.access_key.trim() ||
      !form.secret
    ) {
      setProbeError("Fill in the endpoint, region, access key, and secret first.");
      return;
    }
    if (form.ca_cert.trim() && form.insecure_skip_verify) {
      setProbeError("Trust a CA certificate or skip TLS verification — not both.");
      return;
    }
    setProbeError(null);
    setProbing(true);
    try {
      const ca = form.ca_cert.trim();
      const { buckets } = await api.probeSourceBuckets({
        source_endpoint: form.source_endpoint.trim(),
        source_region: form.source_region.trim(),
        access_key: form.access_key.trim(),
        secret: form.secret,
        insecure_skip_verify: form.insecure_skip_verify,
        ...(ca ? { ca_cert: ca } : {}),
      });
      setSourceBuckets(buckets);
      if (buckets.length === 0) {
        setProbeError("These credentials can't see any buckets on the source.");
      }
    } catch (err) {
      setSourceBuckets(null);
      setProbeError(errorMessage(err, "Could not list the source's buckets."));
    } finally {
      setProbing(false);
    }
  }

  function validate(): string | null {
    if (
      !form.source_endpoint.trim() ||
      !form.source_region.trim() ||
      !form.access_key.trim() ||
      !form.secret
    ) {
      return "Endpoint, region, access key, and secret are all required.";
    }
    if (form.ca_cert.trim() && form.insecure_skip_verify) {
      return "Trust a CA certificate or skip TLS verification — not both.";
    }
    return null;
  }

  function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitted(true);
    const err = validate();
    setFormError(err);
    if (err) return;
    // Importing every bucket is a big, unconfirmed action — confirm it first.
    if (importsAll) {
      setConfirmAll(true);
      return;
    }
    void doCreate();
  }

  async function copyId(id: string) {
    if (!(await copyText(id))) return;
    setCopiedId(id);
    toast.success("Job id copied to the clipboard.");
    window.setTimeout(
      () => setCopiedId((c) => (c === id ? null : c)),
      1500,
    );
  }

  async function doCreate() {
    setConfirmAll(false);
    const ca = form.ca_cert.trim();
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
      // Clear the secret material and the selection; keep the connection host/key
      // for a follow-up job.
      setForm((f) => ({ ...f, secret: "", ca_cert: "", buckets: "" }));
      setSourceBuckets(null);
      setProbeError(null);
      setSubmitted(false);
      setFormError(null);
      res.refresh();
    } catch (err) {
      setFormError(errorMessage(err, "Could not start the import."));
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
  const running = jobs.filter((j) => j.state === "running").length;
  const failed = jobs.filter((j) => j.state === "failed").length;

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
          <form onSubmit={onSubmit} className="grid gap-4 md:grid-cols-2" noValidate>
            {formError ? (
              <div className="md:col-span-2">
                <FieldError id={formErrorId}>{formError}</FieldError>
              </div>
            ) : null}

            <div className="grid gap-1.5">
              <Label htmlFor={endpointId}>Source endpoint</Label>
              <Input
                id={endpointId}
                value={form.source_endpoint}
                onChange={(e) => set("source_endpoint", e.target.value)}
                placeholder="https://s3.source.example:9000"
                autoComplete="off"
                aria-invalid={invalid(form.source_endpoint)}
                aria-describedby={errDesc}
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
                aria-invalid={invalid(form.source_region)}
                aria-describedby={errDesc}
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
                aria-invalid={invalid(form.access_key)}
                aria-describedby={errDesc}
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
                aria-invalid={invalid(form.secret)}
                aria-describedby={errDesc}
                className="font-mono"
              />
            </div>

            <div className="grid gap-2 md:col-span-2">
              <div className="flex items-end justify-between gap-3">
                <Label htmlFor={bucketsId}>Buckets</Label>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={fetchBuckets}
                  disabled={probing}
                  aria-busy={probing}
                >
                  {probing
                    ? "Fetching…"
                    : sourceBuckets
                      ? "Refresh buckets"
                      : "Fetch buckets"}
                </Button>
              </div>

              {/* Picker: check the buckets to import and optionally rename each on
                  the way in. Every edit flows through the textarea below, which is
                  the canonical value and always shows exactly what will be sent. */}
              {sourceBuckets && sourceBuckets.length > 0 ? (
                <div className="overflow-hidden rounded-md border">
                  <div className="flex items-center justify-between gap-3 border-b bg-muted/40 px-3 py-2 text-[13px]">
                    <span className="text-muted-foreground">
                      {selectedCount} of {sourceBuckets.length} selected
                    </span>
                    <div className="flex items-center gap-1">
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="h-7 px-2"
                        onClick={() => selectAllSources(true)}
                        disabled={selectedCount === sourceBuckets.length}
                      >
                        Select all
                      </Button>
                      <Button
                        type="button"
                        variant="ghost"
                        size="sm"
                        className="h-7 px-2"
                        onClick={() => selectAllSources(false)}
                        disabled={selectedCount === 0}
                      >
                        Clear
                      </Button>
                    </div>
                  </div>
                  <ul className="max-h-64 divide-y overflow-auto">
                    {sourceBuckets.map((src) => {
                      const on = selected.has(src);
                      const dest = selected.get(src);
                      return (
                        <li
                          key={src}
                          className="flex items-center gap-3 px-3 py-2"
                        >
                          <Checkbox
                            checked={on}
                            onCheckedChange={(v) => toggleSource(src, v === true)}
                            aria-label={`Import ${src}`}
                          />
                          <span className="min-w-0 flex-1 truncate font-mono text-[13px]">
                            {src}
                          </span>
                          {on ? (
                            <>
                              <ArrowRight
                                aria-hidden="true"
                                className="size-3.5 shrink-0 text-muted-foreground"
                              />
                              <Input
                                value={dest === src ? "" : (dest ?? "")}
                                onChange={(e) =>
                                  renameSource(src, e.target.value)
                                }
                                placeholder={src}
                                aria-label={`Destination bucket for ${src}`}
                                className="h-8 w-44 font-mono text-[13px]"
                              />
                            </>
                          ) : null}
                        </li>
                      );
                    })}
                  </ul>
                </div>
              ) : null}

              {probeError ? <FieldError>{probeError}</FieldError> : null}

              <Textarea
                id={bucketsId}
                value={form.buckets}
                onChange={(e) => set("buckets", e.target.value)}
                rows={3}
                spellCheck={false}
                aria-describedby={bucketsHelpId}
                className="resize-y font-mono text-[13px] leading-relaxed"
              />
              <p id={bucketsHelpId} className="text-[13px] text-muted-foreground">
                One per line, as <code className="font-mono">source</code> or{" "}
                <code className="font-mono">source:destination</code>.{" "}
                <span className="font-medium text-foreground">Fetch buckets</span>{" "}
                lists what the credentials can see; or type names here. Leave empty
                to import every bucket.
              </p>
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
                {busy
                  ? "Starting…"
                  : importsAll
                    ? "Import all buckets"
                    : "Start import"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>

      {/* ---- Jobs ---- */}
      <section className="mt-8 space-y-3">
        <h2 className="text-base font-semibold tracking-tight">Import jobs</h2>
        {/* Announce state changes to assistive tech without reading the whole table. */}
        <p className="sr-only" role="status" aria-live="polite">
          {jobs.length} import {jobs.length === 1 ? "job" : "jobs"}; {running}{" "}
          running, {failed} failed.
        </p>
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
              const p = pct(j);
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
                    {/* The full id is long; show a short prefix, reveal the rest on
                        hover, and copy the whole thing on click. */}
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <button
                          type="button"
                          onClick={() => void copyId(j.id)}
                          aria-label={`Copy job id ${j.id}`}
                          className="inline-flex items-center gap-1 rounded text-muted-foreground transition-colors hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
                        >
                          {j.id.slice(0, 8)}
                          {copiedId === j.id ? (
                            <Check
                              aria-hidden="true"
                              className="size-3 text-success"
                            />
                          ) : (
                            <Copy
                              aria-hidden="true"
                              className="size-3 opacity-60"
                            />
                          )}
                        </button>
                      </TooltipTrigger>
                      <TooltipContent className="font-mono text-xs">
                        {j.id}
                      </TooltipContent>
                    </Tooltip>
                  </TableCell>
                  <TableCell data-label="Status">
                    <div className="flex flex-col items-start gap-1">
                      <StatusBadge tone={tone}>{label}</StatusBadge>
                      {j.state === "failed" && j.last_error ? (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="flex max-w-[30ch] items-center gap-1 text-[13px] text-destructive">
                              <CircleAlert
                                aria-hidden="true"
                                className="size-3.5 shrink-0"
                              />
                              <span className="truncate">{j.last_error}</span>
                            </span>
                          </TooltipTrigger>
                          <TooltipContent className="max-w-sm break-words">
                            {j.last_error}
                          </TooltipContent>
                        </Tooltip>
                      ) : null}
                    </div>
                  </TableCell>
                  <TableCell data-label="Progress">
                    <div className="flex items-center gap-2">
                      {p === null ? (
                        <div
                          role="progressbar"
                          aria-label="Importing; total not yet known"
                          className="h-1.5 w-28 overflow-hidden rounded-full bg-primary/20"
                        >
                          <div className="h-full w-3/5 rounded-full bg-primary/70 animate-pulse motion-reduce:animate-none" />
                        </div>
                      ) : (
                        <Progress value={p} className="h-1.5 w-28" />
                      )}
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

      <ConfirmDialog
        open={confirmAll}
        onOpenChange={setConfirmAll}
        title="Import every bucket?"
        description="You didn't name any buckets, so this will import every bucket the source credentials can see into this node. Destination buckets are created if they don't exist; objects with a colliding key are overwritten."
        confirmLabel="Import all buckets"
        busy={busy}
        onConfirm={() => void doCreate()}
      />
    </Page>
  );
}
