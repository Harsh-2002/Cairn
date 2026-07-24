// Overview: node-wide storage and compression at a glance, plus per-bucket
// usage. One combined load (overview + per-bucket usage + system facts) so
// the page arrives as a whole; Refresh re-fetches without tearing it down.

import { useMemo } from "react";
import { useNavigate } from "react-router";
import { Database } from "lucide-react";
import { api } from "@/lib/api";
import { actionLabel, isDestructiveAction } from "@/lib/activity";
import { bytes, count, duration, ratio, relTime, whenMs } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { StatusBadge } from "@/components/status-badge";
import { TextLink } from "@/components/text-link";
import { UsageBar } from "@/components/usage-bar";
import { Button } from "@/components/primitives/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/primitives/card";
import { Skeleton } from "@/components/primitives/skeleton";

export function Overview() {
  const navigate = useNavigate();

  const { data, error, loading, refresh } = useResource(
    () =>
      Promise.all([
        api.overview(),
        api.overviewBuckets(),
        api.system(),
        // The activity teaser is decoration — its failure never fails the page.
        api.activity(6).catch(() => null),
      ]).then(([overview, perBucket, system, activity]) => ({
        overview,
        perBucket: perBucket.buckets,
        system,
        activity: activity?.entries ?? [],
      })),
    [],
  );
  // Live: the server pushes an "overview" snapshot on a cadence; flow it through the normal fetch.
  useLiveTopic("overview", refresh);

  const buckets = useMemo(
    () =>
      [...(data?.perBucket ?? [])].sort(
        (a, b) => b.logical_bytes - a.logical_bytes,
      ),
    [data],
  );
  const totalLogical = useMemo(
    () => buckets.reduce((sum, b) => sum + b.logical_bytes, 0),
    [buckets],
  );

  const o = data?.overview;
  const sys = data?.system;

  // Compression figures. `saved` is clamped at zero: when data didn't
  // compress (physical > logical) we say so in plain words instead of
  // rendering a negative bar.
  const logical = o?.logical_bytes ?? 0;
  const physical = o?.physical_bytes ?? 0;
  const saved = Math.max(0, logical - physical);
  const savedPct = logical > 0 ? Math.round((saved / logical) * 100) : 0;
  const compressed = logical > 0 && physical <= logical;

  return (
    <Page>
      <PageHeader
        title="Overview"
        description="Storage, compression, and per-bucket usage across this node."
      />

      {error ? (
        <ErrorAlert
          title="Could not load the overview"
          message={error}
          onRetry={refresh}
        />
      ) : null}

      {loading ? (
        <OverviewSkeleton />
      ) : data && o && sys ? (
        <div className="space-y-4">
          {/* Stat grid: the four numbers the eye should land on first. */}
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            <StatCard label="Buckets" value={count(o.buckets)} />
            <StatCard label="Objects" value={count(o.objects)} />
            <StatCard label="Versions" value={count(o.versions)} />
            <StatCard
              label="Stored"
              value={bytes(o.physical_bytes)}
              sub={`of ${bytes(o.logical_bytes)} original`}
            />
          </div>

          {/* items-start so each card sizes to its own content — the Compression card is much
              shorter than Node, and stretching it to match left a large empty void. */}
          <div className="grid items-start gap-4 lg:grid-cols-2">
            {/* Node card: identity and health facts for this instance. */}
            <Card className="gap-4">
              <CardHeader className="gap-1">
                <CardTitle>Node</CardTitle>
                <CardDescription>This Cairn instance.</CardDescription>
              </CardHeader>
              <CardContent>
                <dl className="space-y-3 text-sm">
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">Version</dt>
                    {/* The version already carries its own form — `vYYYY.MM.DD` for a release,
                        `x.y.z-dev+gSHA` for a dev build — so render it verbatim (no prepended `v`). */}
                    <dd className="font-mono text-[13px]">{sys.version}</dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">Uptime</dt>
                    <dd className="text-[13px]">
                      {duration(sys.uptime_secs)}
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">S3 API</dt>
                    <dd className="min-w-0 truncate font-mono text-[13px]">
                      {sys.s3_addr}
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">Console</dt>
                    <dd className="min-w-0 text-right">
                      <span className="block truncate font-mono text-[13px]">
                        {sys.web_addr}
                      </span>
                      <span className="block text-xs text-muted-foreground">
                        configured address
                      </span>
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">TLS</dt>
                    <dd>
                      <StatusBadge tone={sys.tls ? "positive" : "neutral"}>
                        {sys.tls ? "TLS on" : "TLS off"}
                      </StatusBadge>
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">
                      Data directory
                    </dt>
                    <dd
                      className="min-w-0 truncate font-mono text-[13px]"
                      title={sys.data_dir}
                    >
                      {sys.data_dir}
                    </dd>
                  </div>
                </dl>

                {sys.disk_total_bytes != null &&
                sys.disk_free_bytes != null ? (
                  <div className="mt-5 space-y-1.5">
                    <p className="text-sm text-muted-foreground">Disk</p>
                    {(() => {
                      const usedPct =
                        ((sys.disk_total_bytes - sys.disk_free_bytes) /
                          sys.disk_total_bytes) *
                        100;
                      return (
                        <UsageBar
                          percent={usedPct}
                          // A near-full disk is exactly when an operator wants a signal.
                          fillClassName={cn(
                            usedPct >= 95
                              ? "bg-destructive"
                              : usedPct >= 85
                                ? "bg-warning"
                                : undefined,
                          )}
                          label={`Disk: ${bytes(
                            sys.disk_total_bytes - sys.disk_free_bytes,
                          )} used of ${bytes(sys.disk_total_bytes)}`}
                        />
                      );
                    })()}
                    <p className="text-xs text-muted-foreground">
                      {bytes(sys.disk_free_bytes)} free of{" "}
                      {bytes(sys.disk_total_bytes)}
                    </p>
                  </div>
                ) : (
                  <p className="mt-5 text-sm text-muted-foreground">
                    Disk usage unavailable.
                  </p>
                )}
              </CardContent>
            </Card>

            {/* Compression card: how much disk space compression saves. Leads with the ratio (the
                number that matters), and the bar fills to the SAVED proportion — so a high ratio
                reads as a full bar, not the near-empty 'stored' bar that contradicted the headline. */}
            <Card className="gap-4">
              <CardHeader className="gap-1">
                <CardTitle>Compression</CardTitle>
                <CardDescription>
                  How much disk space compression saves.
                </CardDescription>
              </CardHeader>
              <CardContent>
                {logical === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No data to compress yet — upload objects to a bucket to see
                    the space compression saves.
                  </p>
                ) : compressed ? (
                  <div className="space-y-4">
                    <div className="flex items-baseline gap-2">
                      <span className="text-3xl font-semibold tracking-tight tabular-nums">
                        {ratio(o.compression_ratio)}
                      </span>
                      <span className="text-sm text-muted-foreground">
                        smaller — {savedPct}% saved
                      </span>
                    </div>
                    <UsageBar
                      percent={savedPct}
                      label={`${savedPct}% saved: ${bytes(saved)} of ${bytes(
                        logical,
                      )} original`}
                    />
                    <dl className="space-y-2 text-sm">
                      <div className="flex items-baseline justify-between gap-4">
                        <dt className="text-muted-foreground">Stored</dt>
                        <dd className="font-mono text-[13px]">
                          {bytes(physical)}
                        </dd>
                      </div>
                      <div className="flex items-baseline justify-between gap-4">
                        <dt className="text-muted-foreground">Saved</dt>
                        <dd className="font-mono text-[13px]">
                          {bytes(saved)}
                        </dd>
                      </div>
                      <div className="flex items-baseline justify-between gap-4">
                        <dt className="text-muted-foreground">Original</dt>
                        <dd className="font-mono text-[13px]">
                          {bytes(logical)}
                        </dd>
                      </div>
                    </dl>
                  </div>
                ) : (
                  <p className="text-sm leading-relaxed text-muted-foreground">
                    This data didn&apos;t compress (stored {bytes(physical)} vs{" "}
                    {bytes(logical)} original).
                  </p>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Per-bucket usage: each bucket's share of total original bytes. */}
          <Card className="gap-4">
            <CardHeader className="gap-1">
              <CardTitle>Storage by bucket</CardTitle>
              <CardDescription>
                Each bucket&apos;s share of total original bytes.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {buckets.length === 0 ? (
                <EmptyState
                  icon={Database}
                  title="No buckets yet"
                  body="Create your first bucket to see usage here."
                  action={
                    <Button
                      variant="outline"
                      onClick={() => navigate("/buckets")}
                    >
                      Go to buckets
                    </Button>
                  }
                />
              ) : (
                <ul className="space-y-3">
                  {buckets.slice(0, 8).map((b) => {
                    const pct = Math.round(
                      (b.logical_bytes / Math.max(1, totalLogical)) * 100,
                    );
                    return (
                      <li
                        key={b.name}
                        className="grid grid-cols-[1fr_auto] items-center gap-x-4 gap-y-2 sm:grid-cols-[minmax(0,11rem)_1fr_auto]"
                      >
                        <TextLink
                          to={`/buckets/${encodeURIComponent(b.name)}/browser`}
                          className="min-w-0 truncate font-mono text-[13px]"
                          title={b.name}
                        >
                          {b.name}
                        </TextLink>
                        {/* On mobile the bar drops to its own full-width row
                            below the name + bytes; at sm: it sits inline as the
                            middle column of the 3-column grid. */}
                        <div className="order-last col-span-2 sm:order-none sm:col-span-1">
                          <UsageBar
                            percent={pct}
                            label={`${b.name}: ${bytes(
                              b.logical_bytes,
                            )}, ${pct}% of total storage`}
                          />
                        </div>
                        <div className="text-right">
                          <p className="font-mono text-[13px]">
                            {bytes(b.logical_bytes)}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            {count(b.objects)} {b.objects === 1 ? "object" : "objects"}
                          </p>
                        </div>
                      </li>
                    );
                  })}
                  {buckets.length > 8 ? (
                    <li className="pt-1">
                      <TextLink to="/buckets" className="text-[13px]">
                        and {count(buckets.length - 8)} more →
                      </TextLink>
                    </li>
                  ) : null}
                </ul>
              )}
            </CardContent>
          </Card>

          {/* Recent activity teaser: the latest administrative changes, with
              the full log one click away. */}
          {data && data.activity.length > 0 ? (
            <Card className="gap-3">
              <CardHeader className="gap-1">
                <CardTitle>Recent activity</CardTitle>
                <CardDescription>
                  The latest administrative changes on this node.
                </CardDescription>
                <CardAction>
                  <TextLink to="/activity" className="text-[13px]">
                    View all
                  </TextLink>
                </CardAction>
              </CardHeader>
              <CardContent>
                <ul className="divide-y">
                  {data.activity.map((e, i) => (
                    <li
                      key={`${e.at_ms}:${e.action}:${i}`}
                      className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 py-2 first:pt-0 last:pb-0"
                    >
                      <span
                        className={cn(
                          "text-sm font-medium",
                          isDestructiveAction(e.action) && "text-destructive",
                        )}
                        title={e.action}
                      >
                        {actionLabel(e.action)}
                      </span>
                      {e.bucket ? (
                        <TextLink
                          to={`/buckets/${encodeURIComponent(e.bucket)}/browser`}
                          className="font-mono text-[13px]"
                        >
                          {e.bucket}
                        </TextLink>
                      ) : null}
                      {e.key ? (
                        <span
                          className="max-w-[24ch] truncate font-mono text-[13px] text-muted-foreground"
                          title={e.key}
                        >
                          {e.key}
                        </span>
                      ) : null}
                      <span
                        className="ms-auto text-[13px] text-muted-foreground tabular-nums"
                        title={whenMs(e.at_ms)}
                      >
                        {relTime(e.at_ms)}
                      </span>
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>
          ) : null}
        </div>
      ) : null}
    </Page>
  );
}

/** First-paint skeletons mirroring the real layout so nothing jumps. */
function OverviewSkeleton() {
  return (
    <div className="space-y-4">
      <p className="sr-only" role="status">
        Loading overview…
      </p>
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4" aria-hidden="true">
        <StatCard label="Buckets" value="" loading />
        <StatCard label="Objects" value="" loading />
        <StatCard label="Versions" value="" loading />
        <StatCard label="Stored" value="" sub=" " loading />
      </div>
      <div className="grid gap-4 lg:grid-cols-2" aria-hidden="true">
        <Skeleton className="h-72 rounded-lg" />
        <Skeleton className="h-72 rounded-lg" />
      </div>
      <Skeleton className="h-44 rounded-lg" aria-hidden="true" />
    </div>
  );
}
