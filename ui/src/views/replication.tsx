// Failed-replication tracker: objects that could not be copied to their
// replication destination after repeated tries. Empty is the good state.

import type { ReactNode } from "react";
import { CircleAlert, CircleCheck, RotateCw } from "lucide-react";
import { api } from "@/lib/api";
import { whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { EmptyState } from "@/components/empty-state";
import { Page, PageHeader } from "@/components/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const SKELETON_ROWS = 4;

export function Replication() {
  const res = useResource(() => api.failedReplication(100), []);
  const entries = res.data?.entries ?? [];

  return (
    <Page>
      <PageHeader
        title="Replication"
        description="Objects that failed to replicate to their destination after repeated tries."
        actions={
          <Button
            variant="outline"
            onClick={res.refresh}
            disabled={res.loading || res.refreshing}
            aria-busy={res.refreshing || undefined}
          >
            <RotateCw
              aria-hidden="true"
              className={res.refreshing ? "animate-spin" : undefined}
            />
            Refresh
          </Button>
        }
      />

      {res.error ? (
        <Alert variant="destructive" className="mb-4" role="alert">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Could not load replication status</AlertTitle>
          <AlertDescription>{res.error}</AlertDescription>
        </Alert>
      ) : null}

      {res.loading ? (
        <ReplicationTableShell>
          {Array.from({ length: SKELETON_ROWS }, (_, i) => (
            <TableRow key={i}>
              <TableCell>
                <Skeleton className="h-4 w-24" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-40" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-20" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-10" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-32" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-44" />
              </TableCell>
            </TableRow>
          ))}
        </ReplicationTableShell>
      ) : entries.length > 0 ? (
        <ReplicationTableShell>
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
        </ReplicationTableShell>
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

function ReplicationTableShell({ children }: { children: ReactNode }) {
  return (
    <div className="overflow-x-auto rounded-lg border">
      <Table className="min-w-[760px]">
        <TableHeader>
          <TableRow>
            <TableHead className="text-xs text-muted-foreground">Bucket</TableHead>
            <TableHead className="text-xs text-muted-foreground">Key</TableHead>
            <TableHead className="text-xs text-muted-foreground">Version</TableHead>
            <TableHead className="text-right text-xs text-muted-foreground">
              Attempts
            </TableHead>
            <TableHead className="text-xs text-muted-foreground">
              Next attempt
            </TableHead>
            <TableHead className="text-xs text-muted-foreground">Error</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>{children}</TableBody>
      </Table>
    </div>
  );
}
