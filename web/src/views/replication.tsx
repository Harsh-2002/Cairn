// Replication health is per destination. Unknown reads must never look like an empty queue.
import { useState } from "react";
import { Repeat, RefreshCw } from "lucide-react";
import { api, errorMessage } from "@/lib/api";
import { count, whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import {
  readReplicationBuckets,
  replicationRows,
  replicationHealth,
} from "@/lib/replication-status";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { TextLink } from "@/components/text-link";
import { Button } from "@/components/primitives/button";
import { TableCell, TableRow } from "@/components/primitives/table";

const COLUMNS: Column[] = [
  { key: "bucket", label: "Bucket" },
  { key: "target", label: "Replicates to" },
  { key: "status", label: "Status" },
  { key: "pending", label: "Pending", className: "text-right" },
  { key: "claimed", label: "In progress", className: "text-right" },
  { key: "failed", label: "Failed", className: "text-right" },
];
const FAILED_COLUMNS: Column[] = [
  { key: "bucket", label: "Bucket / destination" },
  { key: "key", label: "Object" },
  { key: "attempts", label: "Attempts", className: "text-right" },
  { key: "error", label: "Error / recovery" },
];
const PAGE_SIZE = 50;
const settingsHref = (bucket: string) =>
  `/buckets/${encodeURIComponent(bucket)}/settings`;

function failureSummary(raw: string | null): string {
  if (!raw) return "No error details were recorded.";
  const xml = raw.search(/<\?xml|<Error(?:\s|>)/);
  const detail =
    xml >= 0
      ? new DOMParser()
          .parseFromString(raw.slice(xml), "application/xml")
          .querySelector("Message")?.textContent
      : null;
  return errorMessage(
    new Error(detail || raw),
    "The destination refused this attempt.",
  );
}

export function Replication() {
  const res = useResource(async () => {
    const { buckets } = await api.listBuckets();
    return { buckets: await readReplicationBuckets(buckets), at: Date.now() };
  }, []);
  // Failure diagnostics remain usable even if one bucket is slow or unavailable.
  const failures = useResource(
    async () => ({ ...(await api.failedReplication(100)), at: Date.now() }),
    [],
  );
  const refresh = () => {
    res.refresh();
    failures.refresh();
  };
  useLiveTopic("replication", refresh, 12_000);
  const [page, setPage] = useState(0);
  const rows = (res.data?.buckets ?? [])
    .flatMap(replicationRows)
    .sort(
      (a, b) =>
        a.bucket.localeCompare(b.bucket) ||
        a.destination.localeCompare(b.destination),
    );
  const lastPage = Math.max(0, Math.ceil(rows.length / PAGE_SIZE) - 1);
  const currentPage = Math.min(page, lastPage);
  const stale = !!res.error;
  const errors = res.data?.buckets.filter((b) => b.errors.length) ?? [];
  const failed = failures.data?.entries ?? [];
  const busy = res.refreshing || failures.refreshing;
  return (
    <Page>
      <PageHeader
        title="Replication"
        description="Waiting work, active transfers, and failures for each destination."
        actions={
          <Button
            variant="outline"
            onClick={refresh}
            disabled={busy}
            aria-busy={busy || undefined}
            className="min-h-11"
          >
            <RefreshCw aria-hidden="true" />
            Refresh
          </Button>
        }
      />
      {res.data ? (
        <p className="mb-4 text-sm text-muted-foreground">
          {stale || errors.length ? "Last read" : "Updated"}{" "}
          {whenMs(res.data.at)}. Counts describe the replication queue, not a
          byte-for-byte destination audit.
        </p>
      ) : null}
      {res.error ? (
        <ErrorAlert
          title="Replication status is stale"
          message={res.error}
          onRetry={refresh}
        />
      ) : null}
      <section className="space-y-3" aria-labelledby="replication-destinations">
        <h2
          id="replication-destinations"
          className="text-base font-semibold tracking-tight"
        >
          Replication destinations
        </h2>
        {errors.length ? (
          <details className="rounded-lg border p-3 text-sm">
            <summary className="min-h-11 cursor-pointer py-3">
              Some replication information is unavailable (
              {count(errors.length)} buckets)
            </summary>
            <ul className="space-y-2">
              {errors.map((b) => (
                <li key={b.bucket}>
                  <TextLink to={settingsHref(b.bucket)}>{b.bucket}</TextLink>:{" "}
                  {b.errors.join(" ")}
                </li>
              ))}
            </ul>
          </details>
        ) : null}
        {res.loading ? (
          <DataTable columns={COLUMNS}>
            <SkeletonRows
              rows={3}
              widths={["w-28", "w-48", "w-20", "w-10", "w-10", "w-10"]}
            />
          </DataTable>
        ) : rows.length ? (
          <>
            <DataTable columns={COLUMNS} minWidth={850}>
              {rows
                .slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE)
                .map((r) => {
                  const health = stale
                    ? { tone: "neutral" as const, label: "Stale" }
                    : replicationHealth(r);
                  return (
                    <TableRow key={JSON.stringify([r.bucket, r.target])}>
                      <TableCell
                        data-label="Bucket"
                        className="font-mono text-[13px]"
                      >
                        <TextLink
                          className="inline-flex min-h-11 items-center break-all"
                          to={settingsHref(r.bucket)}
                        >
                          {r.bucket}
                        </TextLink>
                      </TableCell>
                      <TableCell
                        data-label="Replicates to"
                        className="max-w-sm whitespace-normal text-[13px]"
                      >
                        <span className="block break-all font-mono">
                          {r.destination}
                        </span>
                        {r.rules.length ? (
                          <details>
                            <summary className="min-h-11 cursor-pointer py-3">
                              {count(r.rules.length)} rule
                              {r.rules.length === 1 ? "" : "s"}
                            </summary>
                            <ul className="space-y-2 text-muted-foreground">
                              {r.rules.map((rule, index) => (
                                <li
                                  key={`${rule.id}:${index}`}
                                  className="break-all"
                                >
                                  {rule.id || "Unnamed rule"}:{" "}
                                  {rule.enabled ? "Enabled" : "Disabled"};{" "}
                                  {rule.prefix
                                    ? `prefix “${rule.prefix}”`
                                    : "all prefixes"}
                                  {rule.tags
                                    .map(
                                      (tag) => `; tag ${tag.key}=${tag.value}`,
                                    )
                                    .join("")}
                                </li>
                              ))}
                            </ul>
                          </details>
                        ) : (
                          <span className="text-muted-foreground">
                            No current rule details
                          </span>
                        )}
                      </TableCell>
                      <TableCell data-label="Status">
                        <StatusBadge tone={health.tone}>
                          {health.label}
                        </StatusBadge>
                      </TableCell>
                      {(["pending", "claimed", "failed"] as const).map(
                        (key, i) => (
                          <TableCell
                            key={key}
                            data-label={["Pending", "In progress", "Failed"][i]}
                            className="text-right tabular-nums"
                          >
                            {stale || r.errors.length || !r.counts
                              ? "—"
                              : count(r.counts[key])}
                          </TableCell>
                        ),
                      )}
                    </TableRow>
                  );
                })}
            </DataTable>
            {lastPage > 0 ? (
              <nav
                aria-label="Replication pages"
                className="flex flex-wrap items-center justify-end gap-3 text-sm"
              >
                <Button
                  variant="outline"
                  className="min-h-11"
                  disabled={currentPage === 0}
                  onClick={() => setPage(currentPage - 1)}
                >
                  Previous
                </Button>
                <span>
                  Page {currentPage + 1} of {lastPage + 1}
                </span>
                <Button
                  variant="outline"
                  className="min-h-11"
                  disabled={currentPage === lastPage}
                  onClick={() => setPage(currentPage + 1)}
                >
                  Next
                </Button>
              </nav>
            ) : null}
          </>
        ) : !res.error ? (
          <EmptyState
            icon={Repeat}
            title="No replication configured"
            body="Add a target and rule in a bucket’s Settings → Integrations."
          />
        ) : null}
      </section>
      <section
        className="mt-8 space-y-3"
        aria-labelledby="replication-failures"
      >
        <h2
          id="replication-failures"
          className="text-base font-semibold tracking-tight"
        >
          Failed attempts
        </h2>
        <p className="text-sm text-muted-foreground">
          Up to 100 retained failures. Older entries may have expired. Retry
          failed attempts in the bucket’s Settings → Integrations.
        </p>
        {failures.error ? (
          <ErrorAlert
            title="Failure list unavailable"
            message={`Previously loaded entries may be stale. ${failures.error}`}
            onRetry={failures.refresh}
          />
        ) : null}
        {failures.loading ? (
          <DataTable columns={FAILED_COLUMNS}>
            <SkeletonRows rows={3} widths={["w-28", "w-40", "w-10", "w-44"]} />
          </DataTable>
        ) : failed.length ? (
          <DataTable columns={FAILED_COLUMNS} minWidth={760}>
            {failed.map((e) => (
              <TableRow key={e.id}>
                <TableCell
                  data-label="Bucket / destination"
                  className="max-w-xs whitespace-normal text-[13px]"
                >
                  <TextLink
                    className="inline-flex min-h-11 items-center break-all font-mono"
                    to={settingsHref(e.bucket)}
                  >
                    {e.bucket}
                  </TextLink>
                  <span className="block break-all font-mono">
                    {rows.find(
                      (r) => r.bucket === e.bucket && r.target === e.target_arn,
                    )?.destination ??
                      e.target_arn ??
                      "Legacy / unassigned target"}
                  </span>
                </TableCell>
                <TableCell
                  data-label="Object"
                  className="max-w-xs whitespace-normal font-mono text-[13px]"
                >
                  <span className="break-all">{e.key}</span>
                  <details>
                    <summary className="min-h-11 cursor-pointer py-3">
                      Version
                    </summary>
                    <span className="break-all">
                      {e.version_id || "No version ID"}
                    </span>
                  </details>
                </TableCell>
                <TableCell
                  data-label="Attempts"
                  className="text-right tabular-nums"
                >
                  {count(e.attempts)}
                </TableCell>
                <TableCell
                  data-label="Error / recovery"
                  className="max-w-sm whitespace-normal text-sm"
                >
                  <p className="break-words text-destructive">
                    {failureSummary(e.error)}
                  </p>
                  <p className="mt-1 text-muted-foreground">
                    Manual retry required
                  </p>
                  {e.error ? (
                    <details>
                      <summary className="min-h-11 cursor-pointer py-3">
                        Full error
                      </summary>
                      <pre className="whitespace-pre-wrap break-all text-xs">
                        {e.error}
                      </pre>
                    </details>
                  ) : null}
                </TableCell>
              </TableRow>
            ))}
          </DataTable>
        ) : !failures.error ? (
          <p className="py-4 text-sm text-muted-foreground">
            No recent failed attempts.
          </p>
        ) : null}
      </section>
    </Page>
  );
}
