// Failed-replication tracker: objects that could not be copied to their
// replication destination after repeated tries. Empty is the good state.

import { CircleCheck } from "lucide-react";
import { api } from "@/lib/api";
import { whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import { TableCell, TableRow } from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const SKELETON_ROWS = 4;

const COLUMNS: Column[] = [
  { key: "bucket", label: "Bucket" },
  { key: "key", label: "Key" },
  { key: "version", label: "Version" },
  { key: "attempts", label: "Attempts", className: "text-right" },
  { key: "next", label: "Next attempt" },
  { key: "error", label: "Error" },
];

export function Replication() {
  const res = useResource(() => api.failedReplication(100), []);
  const entries = res.data?.entries ?? [];

  return (
    <Page>
      <PageHeader
        title="Replication"
        description="Objects that failed to replicate to their destination after repeated tries."
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
          title="Could not load replication status"
          message={res.error}
          onRetry={res.refresh}
        />
      ) : null}

      {res.loading ? (
        <DataTable columns={COLUMNS} minWidth={760}>
          <SkeletonRows
            rows={SKELETON_ROWS}
            widths={["w-24", "w-40", "w-20", "w-10", "w-32", "w-44"]}
          />
        </DataTable>
      ) : entries.length > 0 ? (
        <DataTable columns={COLUMNS} minWidth={760}>
          {entries.map((e, i) => (
            <TableRow key={`${e.bucket}:${e.key}:${e.version_id}:${e.attempts}:${i}`}>
              <TableCell className="font-mono text-[13px]">{e.bucket}</TableCell>
              <TableCell className="font-mono text-[13px]">
                <span title={e.key} className="block max-w-[28ch] truncate">
                  {e.key}
                </span>
              </TableCell>
              <TableCell className="font-mono text-[13px]">
                <span title={e.version_id} className="block max-w-[12ch] truncate">
                  {e.version_id || "—"}
                </span>
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {e.attempts}
              </TableCell>
              <TableCell className="text-muted-foreground tabular-nums">
                {whenMs(e.next_attempt_at_ms)}
              </TableCell>
              <TableCell className="text-[13px] text-destructive">
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span
                      tabIndex={0}
                      title={e.error}
                      className="block max-w-[32ch] truncate"
                    >
                      {e.error}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent className="max-w-sm break-words">
                    {e.error}
                  </TooltipContent>
                </Tooltip>
              </TableCell>
            </TableRow>
          ))}
        </DataTable>
      ) : !res.error ? (
        <EmptyState
          icon={CircleCheck}
          positive
          title="All caught up"
          body="No failed replication. Objects in buckets with a replication rule are copying normally."
        />
      ) : null}
    </Page>
  );
}
