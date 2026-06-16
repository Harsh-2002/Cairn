// Metrics: API request volume and usage analytics. A rolling-window view of
// how the S3 data plane is being exercised — request volume over time, the
// operation mix, and which buckets are busiest. One load per range; the range
// tabs swap the window without tearing the page down to a skeleton.

import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Activity as ActivityIcon } from "lucide-react";
import { api } from "@/lib/api";
import { count } from "@/lib/format";
import type { MetricsRange } from "@/lib/types";
import { useResource } from "@/lib/use-resource";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import { UsageBar } from "@/components/usage-bar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";

// Seconds in each range window — drives the "avg N req/s" derived stat. The
// 1-month figure is the canonical 31-day month the backend windows on.
const RANGE_SECS: Record<MetricsRange, number> = {
  "1d": 86400,
  "1w": 604800,
  "2w": 1209600,
  "1m": 2678400,
};

const RANGES: { value: MetricsRange; label: string }[] = [
  { value: "1d", label: "1 day" },
  { value: "1w", label: "1 week" },
  { value: "2w", label: "2 weeks" },
  { value: "1m", label: "1 month" },
];

/** A request-rate string with sensible precision: "12.3 req/s", "0.04 req/s". */
function reqPerSec(total: number, secs: number): string {
  if (secs <= 0) return "—";
  const r = total / secs;
  if (r === 0) return "0 req/s";
  if (r >= 100) return `${Math.round(r).toLocaleString()} req/s`;
  if (r >= 1) return `${r.toFixed(1)} req/s`;
  return `${r.toFixed(2)} req/s`;
}

// On a 1-day window the x-axis reads as wall-clock time; over longer windows a
// month/day label keeps ticks legible. The tooltip always shows the full
// timestamp so a hovered point is never ambiguous.
function tickTime(range: MetricsRange) {
  return (ms: number): string => {
    const d = new Date(ms);
    if (Number.isNaN(d.getTime())) return "";
    if (range === "1d") {
      return d.toLocaleTimeString(undefined, {
        hour: "2-digit",
        minute: "2-digit",
      });
    }
    return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  };
}

function fullTime(ms: number): string {
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

export function Metrics() {
  const [range, setRange] = useState<MetricsRange>("1d");

  const { data, error, loading, refreshing, refresh } = useResource(
    () => api.metrics(range),
    [range],
  );

  const total = data?.total ?? 0;
  const avgRate = useMemo(
    () => reqPerSec(total, RANGE_SECS[range]),
    [total, range],
  );

  const tickFmt = useMemo(() => tickTime(range), [range]);

  const maxBucket = useMemo(
    () =>
      (data?.top_buckets ?? []).reduce(
        (m, b) => Math.max(m, b.count),
        0,
      ),
    [data],
  );

  return (
    <Page>
      <PageHeader
        title="Metrics"
        description="API request volume and usage analytics."
        actions={
          <RefreshButton
            loading={loading}
            refreshing={refreshing}
            onClick={refresh}
          />
        }
      />

      {/* The range filter: an underline tab row swapping the rolling window. */}
      <Tabs
        value={range}
        onValueChange={(v) => setRange(v as MetricsRange)}
        className="mb-4"
      >
        <TabsList
          variant="line"
          className="h-auto! w-full justify-start border-b p-0 pb-1"
        >
          {RANGES.map((r) => (
            <TabsTrigger
              key={r.value}
              value={r.value}
              className="flex-none px-2.5 py-1.5"
            >
              {r.label}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>

      {error ? (
        <ErrorAlert
          title="Could not load metrics"
          message={error}
          onRetry={refresh}
        />
      ) : null}

      {loading ? (
        <MetricsSkeleton />
      ) : data && total === 0 ? (
        <EmptyState
          icon={ActivityIcon}
          title="No request activity yet"
          body="Once objects are read and written over the S3 API, request volume will appear here."
        />
      ) : data ? (
        <div className="space-y-4">
          {/* Card 1 — request volume over the window, as an area chart. */}
          <Card className="gap-4">
            <CardHeader className="flex-row items-start justify-between gap-4">
              <div className="min-w-0 space-y-1">
                <CardTitle>Requests over time</CardTitle>
                <CardDescription>
                  {count(total)} requests in this window.
                </CardDescription>
              </div>
              <div className="shrink-0 text-right">
                <p className="text-lg font-semibold tabular-nums">{avgRate}</p>
                <p className="text-xs text-muted-foreground">average rate</p>
              </div>
            </CardHeader>
            <CardContent>
              <div className="h-72 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart
                    data={data.timeline}
                    margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
                  >
                    <defs>
                      <linearGradient
                        id="metrics-area"
                        x1="0"
                        y1="0"
                        x2="0"
                        y2="1"
                      >
                        <stop
                          offset="0%"
                          stopColor="var(--color-link)"
                          stopOpacity={0.25}
                        />
                        <stop
                          offset="100%"
                          stopColor="var(--color-link)"
                          stopOpacity={0}
                        />
                      </linearGradient>
                    </defs>
                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="var(--color-border)"
                      vertical={false}
                    />
                    <XAxis
                      dataKey="ts_ms"
                      type="number"
                      domain={["dataMin", "dataMax"]}
                      scale="time"
                      tickFormatter={tickFmt}
                      tickLine={false}
                      axisLine={{ stroke: "var(--color-border)" }}
                      tick={{
                        fill: "var(--color-muted-foreground)",
                        fontSize: 12,
                      }}
                      minTickGap={40}
                    />
                    <YAxis
                      allowDecimals={false}
                      width={44}
                      tickLine={false}
                      axisLine={false}
                      tick={{
                        fill: "var(--color-muted-foreground)",
                        fontSize: 12,
                      }}
                      tickFormatter={(v: number) => count(v)}
                    />
                    <Tooltip
                      cursor={{ stroke: "var(--color-border)" }}
                      contentStyle={tooltipStyle}
                      labelStyle={tooltipLabelStyle}
                      itemStyle={tooltipItemStyle}
                      labelFormatter={(label) => fullTime(Number(label))}
                      formatter={(value) => [count(Number(value)), "Requests"]}
                    />
                    <Area
                      type="monotone"
                      dataKey="count"
                      stroke="var(--color-link)"
                      strokeWidth={2}
                      fill="url(#metrics-area)"
                      isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          {/* Card 2 — the operation mix, as a horizontal bar chart. */}
          <Card className="gap-4">
            <CardHeader className="gap-1">
              <CardTitle>By operation</CardTitle>
              <CardDescription>
                Requests broken down by S3 operation.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {data.by_operation.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No operations recorded in this window.
                </p>
              ) : (
                <div
                  className="w-full"
                  style={{
                    height: Math.max(
                      160,
                      data.by_operation.length * 34 + 16,
                    ),
                  }}
                >
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart
                      layout="vertical"
                      data={data.by_operation}
                      margin={{ top: 0, right: 16, bottom: 0, left: 0 }}
                    >
                      <CartesianGrid
                        strokeDasharray="3 3"
                        stroke="var(--color-border)"
                        horizontal={false}
                      />
                      <XAxis
                        type="number"
                        allowDecimals={false}
                        tickLine={false}
                        axisLine={{ stroke: "var(--color-border)" }}
                        tick={{
                          fill: "var(--color-muted-foreground)",
                          fontSize: 12,
                        }}
                        tickFormatter={(v: number) => count(v)}
                      />
                      <YAxis
                        type="category"
                        dataKey="operation"
                        width={120}
                        tickLine={false}
                        axisLine={false}
                        tick={{
                          fill: "var(--color-muted-foreground)",
                          fontSize: 12,
                        }}
                      />
                      <Tooltip
                        cursor={{ fill: "var(--color-muted)" }}
                        contentStyle={tooltipStyle}
                        labelStyle={tooltipLabelStyle}
                        itemStyle={tooltipItemStyle}
                        formatter={(value) => [
                          count(Number(value)),
                          "Requests",
                        ]}
                      />
                      <Bar
                        dataKey="count"
                        fill="var(--color-chart-4)"
                        radius={[0, 4, 4, 0]}
                        isAnimationActive={false}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Card 3 — busiest buckets, each as a share of the busiest. */}
          <Card className="gap-4">
            <CardHeader className="gap-1">
              <CardTitle>Most active buckets</CardTitle>
              <CardDescription>
                Buckets ranked by request count in this window.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {data.top_buckets.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No bucket activity in this window.
                </p>
              ) : (
                <ul className="space-y-3">
                  {data.top_buckets.map((b) => {
                    const pct = (b.count / Math.max(1, maxBucket)) * 100;
                    return (
                      <li
                        key={b.bucket}
                        className="grid grid-cols-[1fr_auto] items-center gap-x-4 gap-y-2 sm:grid-cols-[minmax(0,11rem)_1fr_auto]"
                      >
                        <span
                          className="min-w-0 truncate font-mono text-[13px]"
                          title={b.bucket}
                        >
                          {b.bucket}
                        </span>
                        <div className="order-last col-span-2 sm:order-none sm:col-span-1">
                          <UsageBar
                            percent={pct}
                            label={`${b.bucket}: ${count(b.count)} requests`}
                          />
                        </div>
                        <span className="text-right font-mono text-[13px] tabular-nums">
                          {count(b.count)}
                        </span>
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

// A floating-layer tooltip styled to match the popover surface (border + soft
// shadow), the one place this view leans on the theme's surface tokens.
const tooltipStyle: React.CSSProperties = {
  background: "var(--color-popover)",
  border: "1px solid var(--color-border)",
  borderRadius: "var(--radius-md)",
  boxShadow: "0 4px 12px rgb(0 0 0 / 0.08)",
  fontSize: 12,
  padding: "8px 10px",
};

const tooltipLabelStyle: React.CSSProperties = {
  color: "var(--color-muted-foreground)",
  marginBottom: 2,
};

const tooltipItemStyle: React.CSSProperties = {
  color: "var(--color-popover-foreground)",
};

/** First-paint skeletons mirroring the three-card layout so nothing jumps. */
function MetricsSkeleton() {
  return (
    <div className="space-y-4" aria-hidden="true">
      <p className="sr-only" role="status">
        Loading metrics…
      </p>
      <Skeleton className="h-96 rounded-lg" />
      <Skeleton className="h-72 rounded-lg" />
      <Skeleton className="h-56 rounded-lg" />
    </div>
  );
}
