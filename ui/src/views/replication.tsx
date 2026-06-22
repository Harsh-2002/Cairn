// Replication overview: where each bucket replicates, the live health of every
// rule (pending / failed), and — as a section beneath — the objects that could
// not be copied after repeated tries. The dead-letter queue is one part of the
// picture, not the whole page. Configuration lives per-bucket in Settings →
// Integrations; this page links there.

import { useMemo } from "react";
import { CircleAlert, CircleCheck, Repeat } from "lucide-react";
import { api } from "@/lib/api";
import * as s3 from "@/lib/s3";
import { whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import type { FailedReplicationEntry, ReplicationTarget } from "@/lib/types";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { TextLink } from "@/components/text-link";
import { TableCell, TableRow } from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const RULE_COLUMNS: Column[] = [
  { key: "bucket", label: "Bucket" },
  { key: "dest", label: "Replicates to" },
  { key: "status", label: "Status" },
  { key: "pending", label: "Pending", className: "text-right" },
  { key: "failed", label: "Failed", className: "text-right" },
];

const FAILED_COLUMNS: Column[] = [
  { key: "bucket", label: "Bucket" },
  { key: "key", label: "Key" },
  { key: "version", label: "Version" },
  { key: "attempts", label: "Attempts", className: "text-right" },
  { key: "next", label: "Next attempt" },
  { key: "error", label: "Error" },
];

interface RuleRow {
  bucket: string;
  /** The destination as the rule names it (target ARN, or a legacy bucket name). */
  destinationLabel: string;
  prefix: string;
  pending: number;
  failed: number;
}

function ruleTone(r: RuleRow): { tone: StatusTone; label: string } {
  if (r.failed > 0) return { tone: "negative", label: "Failing" };
  if (r.pending > 0) return { tone: "warning", label: "Replicating" };
  return { tone: "positive", label: "Healthy" };
}

function settingsHref(bucket: string): string {
  return `/buckets/${encodeURIComponent(bucket)}/settings`;
}

export function Replication() {
  const res = useResource(async () => {
    const { buckets } = await api.listBuckets();
    // Resolve each bucket's rule + targets + live status in parallel; keep only
    // the buckets that actually carry a replication rule.
    const perBucket = await Promise.all(
      buckets.map(async (b) => {
        const [rule, targets, status] = await Promise.all([
          s3.getReplication(b.name).catch(() => null),
          api
            .listReplicationTargets(b.name)
            .then((r) => r.targets)
            .catch(() => [] as ReplicationTarget[]),
          api.replicationStatus(b.name).catch(() => null),
        ]);
        if (!rule) return null;
        const match = targets.find((t) => t.arn === rule.dest_bucket);
        const row: RuleRow = {
          bucket: b.name,
          destinationLabel: match
            ? `${match.dest_bucket} @ ${match.endpoint}`
            : rule.dest_bucket,
          prefix: rule.prefix,
          pending: status?.pending ?? 0,
          failed: status?.failed ?? 0,
        };
        return row;
      }),
    );
    const rules = perBucket
      .filter((r): r is RuleRow => r !== null)
      .sort((a, b) => a.bucket.localeCompare(b.bucket));
    const failed = await api
      .failedReplication(100)
      .then((r) => r.entries)
      .catch(() => [] as FailedReplicationEntry[]);
    return { rules, failed };
  }, []);

  const rules = useMemo(() => res.data?.rules ?? [], [res.data]);
  const failed = useMemo(() => res.data?.failed ?? [], [res.data]);

  return (
    <Page>
      <PageHeader
        title="Replication"
        description="Where each bucket replicates, and the health of every copy."
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

      {/* ---- Rules + live health ---- */}
      <section className="space-y-3">
        <h2 className="text-base font-semibold tracking-tight">
          Replication rules
        </h2>
        {res.loading ? (
          <DataTable columns={RULE_COLUMNS} minWidth={720}>
            <SkeletonRows
              rows={3}
              widths={["w-28", "w-48", "w-20", "w-10", "w-10"]}
            />
          </DataTable>
        ) : rules.length > 0 ? (
          <DataTable columns={RULE_COLUMNS} minWidth={720}>
            {rules.map((r) => {
              const { tone, label } = ruleTone(r);
              return (
                <TableRow key={r.bucket}>
                  <TableCell data-label="Bucket" className="font-mono text-[13px]">
                    <TextLink to={settingsHref(r.bucket)}>{r.bucket}</TextLink>
                  </TableCell>
                  <TableCell
                    data-label="Replicates to"
                    className="font-mono text-[13px]"
                  >
                    <span
                      title={r.destinationLabel}
                      className="block max-w-[36ch] truncate"
                    >
                      {r.destinationLabel}
                    </span>
                    {r.prefix ? (
                      <span className="text-muted-foreground">
                        prefix “{r.prefix}”
                      </span>
                    ) : null}
                  </TableCell>
                  <TableCell data-label="Status">
                    <StatusBadge tone={tone}>{label}</StatusBadge>
                  </TableCell>
                  <TableCell
                    data-label="Pending"
                    className="text-right tabular-nums"
                  >
                    {r.pending}
                  </TableCell>
                  <TableCell
                    data-label="Failed"
                    className={
                      r.failed > 0
                        ? "text-right tabular-nums text-destructive"
                        : "text-right tabular-nums"
                    }
                  >
                    {r.failed}
                  </TableCell>
                </TableRow>
              );
            })}
          </DataTable>
        ) : !res.error ? (
          <EmptyState
            icon={Repeat}
            title="No replication configured"
            body="Replication copies a bucket's new objects to a remote target. Set it up in a bucket's Settings → Integrations: add a target, then a rule."
          />
        ) : null}
      </section>

      {/* ---- Failed objects (dead-letter) ---- */}
      <section className="mt-8 space-y-3">
        <h2 className="text-base font-semibold tracking-tight">
          Failed objects
        </h2>
        {res.loading ? (
          <DataTable columns={FAILED_COLUMNS} minWidth={760}>
            <SkeletonRows
              rows={3}
              widths={["w-24", "w-40", "w-20", "w-10", "w-32", "w-44"]}
            />
          </DataTable>
        ) : failed.length > 0 ? (
          <DataTable columns={FAILED_COLUMNS} minWidth={760}>
            {failed.map((e, i) => (
              <TableRow
                key={`${e.bucket}:${e.key}:${e.version_id}:${e.attempts}:${i}`}
              >
                <TableCell data-label="Bucket" className="font-mono text-[13px]">
                  <TextLink to={settingsHref(e.bucket)}>{e.bucket}</TextLink>
                </TableCell>
                <TableCell data-label="Key" className="font-mono text-[13px]">
                  <span title={e.key} className="block max-w-[28ch] truncate">
                    {e.key}
                  </span>
                </TableCell>
                <TableCell data-label="Version" className="font-mono text-[13px]">
                  <span title={e.version_id} className="block max-w-[12ch] truncate">
                    {e.version_id || "—"}
                  </span>
                </TableCell>
                <TableCell
                  data-label="Attempts"
                  className="text-right tabular-nums"
                >
                  {e.attempts}
                </TableCell>
                <TableCell
                  data-label="Next attempt"
                  className="text-muted-foreground tabular-nums"
                >
                  {whenMs(e.next_attempt_at_ms)}
                </TableCell>
                <TableCell
                  data-label="Error"
                  className="text-[13px] text-destructive"
                >
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span
                        title={e.error}
                        className="flex max-w-[32ch] items-center gap-1.5"
                      >
                        <CircleAlert
                          aria-hidden="true"
                          className="size-3.5 shrink-0"
                        />
                        <span className="truncate">{e.error}</span>
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
      </section>
    </Page>
  );
}
