// Activity log: a read-only audit trail of recent administrative changes
// (bucket, user, and policy operations) recorded by this node.

import { useMemo } from "react";
import { useSearchParams } from "react-router";
import { History } from "lucide-react";
import { api } from "@/lib/api";
import { actionLabel, isDestructiveAction } from "@/lib/activity";
import { relTime, whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import { cn } from "@/lib/utils";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Button } from "@/components/primitives/button";
import { Input } from "@/components/primitives/input";
import { Page, PageHeader } from "@/components/page-header";
import { TextLink } from "@/components/text-link";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/primitives/select";
import { TableCell, TableRow } from "@/components/primitives/table";

const SKELETON_ROWS = 8;

// We load (and filter client-side over) the most recent slice; the page is honest
// about the ceiling so an operator knows older history exists beyond the window.
const LIMIT = 500;

const COLUMNS: Column[] = [
  { key: "when", label: "When" },
  { key: "action", label: "Action" },
  { key: "actor", label: "Actor" },
  { key: "bucket", label: "Bucket" },
  { key: "key", label: "Key" },
];

export function Activity() {
  const res = useResource(() => api.activity(LIMIT), []);
  // Live: new audit entries stream in — re-fetch when the server pushes an "activity" snapshot.
  useLiveTopic("activity", res.refresh);
  const entries = useMemo(() => res.data?.entries ?? [], [res.data]);

  // Filters live in the URL so a filtered view is linkable and survives a refresh.
  const [params, setParams] = useSearchParams();
  const action = params.get("action") ?? "all";
  const bucketFilter = params.get("bucket") ?? "";

  const setParam = (key: string, value: string) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value === "") next.delete(key);
        else next.set(key, value);
        return next;
      },
      { replace: true },
    );
  const setAction = (v: string) => setParam("action", v === "all" ? "" : v);
  const setBucketFilter = (v: string) => setParam("bucket", v.trim());
  const clearFilters = () =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete("action");
        next.delete("bucket");
        return next;
      },
      { replace: true },
    );

  const actions = useMemo(
    () => [...new Set(entries.map((e) => e.action))].sort(),
    [entries],
  );

  const filtered = useMemo(() => {
    const bf = bucketFilter.trim().toLowerCase();
    return entries.filter(
      (e) =>
        (action === "all" || e.action === action) &&
        (bf === "" || (e.bucket ?? "").toLowerCase().includes(bf)),
    );
  }, [entries, action, bucketFilter]);

  const filtering = action !== "all" || bucketFilter.trim() !== "";
  const atCap = entries.length >= LIMIT;
  // "500 most recent events" / "12 events" / "Showing 3 of 500 (500 most recent)".
  const summary = filtering
    ? `Showing ${filtered.length} of ${entries.length}${atCap ? " (500 most recent)" : ""}`
    : atCap
      ? "500 most recent events"
      : `${entries.length} event${entries.length === 1 ? "" : "s"}`;

  return (
    <Page>
      <PageHeader
        title="Activity"
        description="Recent administrative changes on this node."
      />

      {res.error ? (
        <ErrorAlert
          title="Could not load activity"
          message={res.error}
          onRetry={res.refresh}
        />
      ) : null}

      {!res.loading && entries.length > 0 ? (
        <div className="mb-4 flex flex-wrap items-center gap-2">
          <Select value={action} onValueChange={setAction}>
            <SelectTrigger className="w-full sm:w-56" aria-label="Filter by action">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All actions</SelectItem>
              {actions.map((a) => (
                <SelectItem key={a} value={a}>
                  {actionLabel(a)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            value={bucketFilter}
            placeholder="Filter by bucket"
            autoComplete="off"
            className="w-full font-mono sm:w-56"
            onChange={(e) => setBucketFilter(e.target.value)}
            aria-label="Filter by bucket"
          />
          {filtering ? (
            <Button variant="ghost" size="sm" onClick={clearFilters}>
              Clear filters
            </Button>
          ) : null}
          <p
            className="text-muted-foreground w-full text-sm tabular-nums sm:ms-auto sm:w-auto"
            aria-live="polite"
          >
            {summary}
          </p>
        </div>
      ) : null}

      {res.loading ? (
        <DataTable columns={COLUMNS} minWidth={760}>
          <SkeletonRows
            rows={SKELETON_ROWS}
            widths={["w-36", "w-28", "w-24", "w-28", "w-40"]}
          />
        </DataTable>
      ) : filtered.length > 0 ? (
        <DataTable columns={COLUMNS} minWidth={760}>
          {filtered.map((e, i) => (
            <TableRow
              key={`${e.at_ms}:${e.action}:${e.bucket ?? ""}:${e.key ?? ""}:${i}`}
            >
              <TableCell
                data-label="When"
                className="text-muted-foreground tabular-nums"
              >
                <span title={whenMs(e.at_ms)}>{relTime(e.at_ms)}</span>
              </TableCell>
              <TableCell
                data-label="Action"
                className={cn(
                  "text-sm font-medium",
                  isDestructiveAction(e.action) && "text-destructive",
                )}
              >
                <span title={e.action}>{actionLabel(e.action)}</span>
              </TableCell>
              <TableCell data-label="Actor" className="font-mono text-[13px]">
                {e.actor ? (
                  <span title={e.actor} className="block max-w-[20ch] truncate">
                    {e.actor}
                  </span>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </TableCell>
              <TableCell data-label="Bucket" className="font-mono text-[13px]">
                {e.bucket ? (
                  <TextLink to={`/buckets/${encodeURIComponent(e.bucket)}/browser`}>
                    {e.bucket}
                  </TextLink>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </TableCell>
              <TableCell data-label="Key" className="font-mono text-[13px]">
                {e.key ? (
                  <span title={e.key} className="block max-w-[28ch] truncate">
                    {e.key}
                  </span>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </TableCell>
            </TableRow>
          ))}
        </DataTable>
      ) : !res.error ? (
        <EmptyState
          icon={History}
          title={entries.length > 0 ? "Nothing matches" : "No activity yet"}
          body={
            entries.length > 0
              ? "No recorded activity matches these filters."
              : "Administrative actions (bucket, user, and policy changes) will appear here."
          }
          action={
            entries.length > 0 ? (
              <Button variant="outline" size="sm" onClick={clearFilters}>
                Clear filters
              </Button>
            ) : undefined
          }
        />
      ) : null}
    </Page>
  );
}
