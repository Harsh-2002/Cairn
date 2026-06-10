// Activity log: a read-only audit trail of recent administrative changes
// (bucket, user, and policy operations) recorded by this node.

import type { ReactNode } from "react";
import { CircleAlert, History, RotateCw } from "lucide-react";
import { NavLink } from "react-router";
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

const SKELETON_ROWS = 8;

export function Activity() {
  const res = useResource(() => api.activity(100), []);
  const entries = res.data?.entries ?? [];

  return (
    <Page>
      <PageHeader
        title="Activity"
        description="Recent administrative changes on this node."
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
        <Alert variant="destructive" className="mb-4">
          <CircleAlert aria-hidden="true" />
          <AlertTitle>Could not load activity</AlertTitle>
          <AlertDescription>{res.error}</AlertDescription>
        </Alert>
      ) : null}

      {res.loading ? (
        <ActivityTableShell>
          {Array.from({ length: SKELETON_ROWS }, (_, i) => (
            <TableRow key={i}>
              <TableCell>
                <Skeleton className="h-4 w-36" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-40" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-28" />
              </TableCell>
              <TableCell>
                <Skeleton className="h-4 w-48" />
              </TableCell>
            </TableRow>
          ))}
        </ActivityTableShell>
      ) : entries.length > 0 ? (
        <ActivityTableShell>
          {entries.map((e, i) => (
            // Stable key per row: the timestamp plus what changed; the index
            // keeps keys unique if two entries ever share the same fields.
            <TableRow key={`${e.at_ms}:${e.action}:${e.bucket ?? ""}:${e.key ?? ""}:${i}`}>
              <TableCell className="text-muted-foreground tabular-nums">
                {whenMs(e.at_ms)}
              </TableCell>
              <TableCell className="text-sm font-medium">{e.action}</TableCell>
              <TableCell className="font-mono text-[13px]">
                {e.bucket ? (
                  <NavLink
                    to={`/buckets/${encodeURIComponent(e.bucket)}/browser`}
                    className="text-link hover:underline underline-offset-4"
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
        </ActivityTableShell>
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

function ActivityTableShell({ children }: { children: ReactNode }) {
  return (
    <div className="overflow-x-auto rounded-lg border">
      <Table className="min-w-[640px]">
        <TableHeader>
          <TableRow>
            <TableHead className="text-xs text-muted-foreground">When</TableHead>
            <TableHead className="text-xs text-muted-foreground">Action</TableHead>
            <TableHead className="text-xs text-muted-foreground">Bucket</TableHead>
            <TableHead className="text-xs text-muted-foreground">Key</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>{children}</TableBody>
      </Table>
    </div>
  );
}
