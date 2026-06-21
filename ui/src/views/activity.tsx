// Activity log: a read-only audit trail of recent administrative changes
// (bucket, user, and policy operations) recorded by this node.

import { useMemo, useState } from "react";
import { History } from "lucide-react";
import { api } from "@/lib/api";
import { whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Input } from "@/components/ui/input";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import { TextLink } from "@/components/text-link";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { TableCell, TableRow } from "@/components/ui/table";

const SKELETON_ROWS = 8;

const COLUMNS: Column[] = [
  { key: "when", label: "When" },
  { key: "action", label: "Action" },
  { key: "actor", label: "Actor" },
  { key: "bucket", label: "Bucket" },
  { key: "key", label: "Key" },
];

export function Activity() {
  const res = useResource(() => api.activity(500), []);
  const entries = useMemo(() => res.data?.entries ?? [], [res.data]);

  // Client-side filters over the loaded page.
  const [action, setAction] = useState("all");
  const [bucketFilter, setBucketFilter] = useState("");

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

  return (
    <Page>
      <PageHeader
        title="Activity"
        description="Recent administrative changes on this node."
        actions={
          <RefreshButton
            loading={res.loading}
            refreshing={res.refreshing}
            onClick={res.refresh}
          />
        }
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
                  {a}
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
                {whenMs(e.at_ms)}
              </TableCell>
              <TableCell data-label="Action" className="text-sm font-medium">
                {e.action}
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
              ? "Clear the filters to see all recorded activity."
              : "Administrative actions (bucket, user, and policy changes) will appear here."
          }
        />
      ) : null}
    </Page>
  );
}
