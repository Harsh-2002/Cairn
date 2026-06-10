// Overview: node-wide storage and compression at a glance, plus per-bucket
// usage. One combined load (overview + per-bucket usage + system facts) so
// the page arrives as a whole; Refresh re-fetches without tearing it down.

import { useMemo } from "react";
import { NavLink, useNavigate } from "react-router";
import { Database, RotateCw } from "lucide-react";
import { api } from "@/lib/api";
import { bytes, count, duration, ratio } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { EmptyState } from "@/components/empty-state";
import { Page, PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { UsageBar } from "@/components/usage-bar";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

export function Overview() {
  const navigate = useNavigate();

  const { data, error, loading, refreshing, refresh } = useResource(
    () =>
      Promise.all([api.overview(), api.overviewBuckets(), api.system()]).then(
        ([overview, perBucket, system]) => ({
          overview,
          perBucket: perBucket.buckets,
          system,
        }),
      ),
    [],
  );

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
  const storedPct =
    logical > 0 ? Math.round(Math.min(1, physical / logical) * 100) : 100;
  const compressed = logical > 0 && physical <= logical;

  return (
    <Page>
      <PageHeader
        title="Overview"
        description="Storage, compression, and per-bucket usage across this node."
        actions={
          <Button
            variant="outline"
            onClick={refresh}
            disabled={refreshing}
            aria-busy={refreshing}
          >
            <RotateCw
              aria-hidden="true"
              className={refreshing ? "animate-spin" : undefined}
            />
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
        }
      />

      {error ? (
        <Alert variant="destructive" role="alert" className="mb-4">
          <AlertTitle>Could not load the overview</AlertTitle>
          <AlertDescription>
            <p>{error}</p>
            <Button
              variant="outline"
              size="sm"
              className="mt-1 text-foreground"
              onClick={refresh}
            >
              Try again
            </Button>
          </AlertDescription>
        </Alert>
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

          <div className="grid gap-4 lg:grid-cols-2">
            {/* Node card: identity and health facts for this instance. */}
            <Card className="gap-4 rounded-lg shadow-none">
              <CardHeader className="gap-1">
                <CardTitle>Node</CardTitle>
                <CardDescription>This Cairn instance.</CardDescription>
              </CardHeader>
              <CardContent>
                <dl className="space-y-3 text-sm">
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">Version</dt>
                    <dd className="font-mono text-[13px] tabular-nums">
                      v{sys.version}
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">Uptime</dt>
                    <dd className="text-[13px] tabular-nums">
                      {duration(sys.uptime_secs)}
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">S3 API</dt>
                    <dd className="min-w-0 truncate font-mono text-[13px] tabular-nums">
                      {sys.s3_addr}
                    </dd>
                  </div>
                  <div className="flex items-baseline justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">Console</dt>
                    <dd className="min-w-0 text-right">
                      <span className="block truncate font-mono text-[13px] tabular-nums">
                        {sys.ui_addr}
                      </span>
                      <span className="block text-xs text-muted-foreground">
                        configured address
                      </span>
                    </dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="shrink-0 text-muted-foreground">TLS</dt>
                    <dd>
                      <Badge variant="outline">
                        {sys.tls ? "TLS on" : "TLS off"}
                      </Badge>
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
                    <UsageBar
                      percent={
                        ((sys.disk_total_bytes - sys.disk_free_bytes) /
                          sys.disk_total_bytes) *
                        100
                      }
                      label={`Disk: ${bytes(
                        sys.disk_total_bytes - sys.disk_free_bytes,
                      )} used of ${bytes(sys.disk_total_bytes)}`}
                    />
                    <p className="text-xs text-muted-foreground tabular-nums">
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

            {/* Compression card: how much disk space compression saves. */}
            <Card className="gap-4 rounded-lg shadow-none">
              <CardHeader className="gap-1">
                <CardTitle>Compression</CardTitle>
                <CardDescription className="tabular-nums">
                  {compressed
                    ? `${savedPct}% smaller · ${ratio(o.compression_ratio)}`
                    : "How much disk space compression saves."}
                </CardDescription>
              </CardHeader>
              <CardContent>
                {logical === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    Nothing stored yet.
                  </p>
                ) : compressed ? (
                  <div className="space-y-4">
                    <UsageBar
                      percent={storedPct}
                      label={`Stored ${bytes(physical)} of ${bytes(
                        logical,
                      )} original (${savedPct}% saved)`}
                    />
                    <dl className="space-y-2 text-sm">
                      <div className="flex items-baseline justify-between gap-4">
                        <dt className="text-muted-foreground">Stored</dt>
                        <dd className="font-mono text-[13px] tabular-nums">
                          {bytes(physical)}
                        </dd>
                      </div>
                      <div className="flex items-baseline justify-between gap-4">
                        <dt className="text-muted-foreground">Saved</dt>
                        <dd className="font-mono text-[13px] tabular-nums">
                          {bytes(saved)}
                        </dd>
                      </div>
                      <div className="flex items-baseline justify-between gap-4">
                        <dt className="text-muted-foreground">Original</dt>
                        <dd className="font-mono text-[13px] tabular-nums">
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
          <Card className="gap-4 rounded-lg shadow-none">
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
                  {buckets.map((b) => {
                    const pct = Math.round(
                      (b.logical_bytes / Math.max(1, totalLogical)) * 100,
                    );
                    return (
                      <li
                        key={b.name}
                        className="grid grid-cols-[minmax(0,11rem)_1fr_auto] items-center gap-4"
                      >
                        <NavLink
                          to={`/buckets/${encodeURIComponent(b.name)}/browser`}
                          className="truncate font-mono text-[13px] text-link hover:underline underline-offset-4"
                          title={b.name}
                        >
                          {b.name}
                        </NavLink>
                        <UsageBar
                          percent={pct}
                          label={`${b.name}: ${bytes(
                            b.logical_bytes,
                          )}, ${pct}% of total storage`}
                        />
                        <div className="text-right">
                          <p className="font-mono text-[13px] tabular-nums">
                            {bytes(b.logical_bytes)}
                          </p>
                          <p className="text-xs text-muted-foreground tabular-nums">
                            {count(b.objects)} {b.objects === 1 ? "object" : "objects"}
                          </p>
                        </div>
                      </li>
                    );
                  })}
                </ul>
              )}
            </CardContent>
          </Card>
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
