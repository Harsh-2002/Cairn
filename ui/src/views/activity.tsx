// Activity log: a read-only audit trail of recent administrative changes
// (bucket, user, and policy operations) recorded by this node.

import { History } from "lucide-react";
import { NavLink } from "react-router";
import { api } from "@/lib/api";
import { whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import { TableCell, TableRow } from "@/components/ui/table";

const SKELETON_ROWS = 8;

const COLUMNS: Column[] = [
  { key: "when", label: "When" },
  { key: "action", label: "Action" },
  { key: "bucket", label: "Bucket" },
  { key: "key", label: "Key" },
];

export function Activity() {
  const res = useResource(() => api.activity(100), []);
  const entries = res.data?.entries ?? [];

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

      {res.loading ? (
        <DataTable columns={COLUMNS} minWidth={640}>
          <SkeletonRows
            rows={SKELETON_ROWS}
            widths={["w-36", "w-40", "w-28", "w-48"]}
          />
        </DataTable>
      ) : entries.length > 0 ? (
        <DataTable columns={COLUMNS} minWidth={640}>
          {entries.map((e, i) => (
            // Stable key per row: the timestamp plus what changed; the index
            // keeps keys unique if two entries ever share the same fields.
            <TableRow
              key={`${e.at_ms}:${e.action}:${e.bucket ?? ""}:${e.key ?? ""}:${i}`}
            >
              <TableCell className="text-muted-foreground tabular-nums">
                {whenMs(e.at_ms)}
              </TableCell>
              <TableCell className="text-sm font-medium">{e.action}</TableCell>
              <TableCell className="font-mono text-[13px]">
                {e.bucket ? (
                  <NavLink
                    to={`/buckets/${encodeURIComponent(e.bucket)}/browser`}
                    className="text-link underline-offset-4 hover:underline"
                  >
                    {e.bucket}
                  </NavLink>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </TableCell>
              <TableCell className="font-mono text-[13px]">
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
          title="No activity yet"
          body="Administrative actions (bucket, user, and policy changes) will appear here."
        />
      ) : null}
    </Page>
  );
}
