// Metrics: a Grafana-style dashboard over the S3 data plane. A responsive
// auto-grid of stat tiles + chart panels — request volume and errors over
// time, throughput and latency, the operation/status mix, and the busiest
// buckets. Two loads: metrics(range) keyed on the range tabs, and a one-shot
// overview() for the system stat tiles. The grid collapses 3 → 2 → 1 column
// from desktop to mobile; the hero "Requests over time" chart spans the row.

import { useEffect, useMemo, useRef, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Activity as ActivityIcon } from "lucide-react";
import { api } from "@/lib/api";
import { bytes, compactNum, count } from "@/lib/format";
import type {
  MetricOp,
  MetricStatus,
  MetricsRange,
  RequestMetricsResp,
} from "@/lib/types";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { StatCard } from "@/components/stat-card";
import { UsageBar } from "@/components/usage-bar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

// Seconds in each range window — drives the "avg req/s" derived stat. The
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

// S3 operations that read vs. write — the "reads vs writes" split. Anything
// not listed (e.g. control-plane probes) lands in neither bucket.
const WRITE_OPS = new Set([
  "PutObject",
  "DeleteObject",
  "DeleteObjects",
  "UploadPart",
  "CreateMultipartUpload",
  "CompleteMultipartUpload",
  "CreateBucket",
  "DeleteBucket",
]);
const READ_OPS = new Set(["GetObject", "HeadObject", "ListObjects"]);

/** A request-rate string with sensible precision: "12.3/s", "0.04/s". */
function reqPerSec(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0/s";
  if (value >= 100) return `${Math.round(value).toLocaleString()}/s`;
  if (value >= 1) return `${value.toFixed(1)}/s`;
  return `${value.toFixed(2)}/s`;
}

/** A latency string in milliseconds: "12 ms", "1.4 ms", "0 ms". */
function ms(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  if (n === 0) return "0 ms";
  if (n >= 100) return `${Math.round(n).toLocaleString()} ms`;
  if (n >= 10) return `${n.toFixed(1)} ms`;
  return `${n.toFixed(2)} ms`;
}

/** A percentage with one decimal below 10%, none above: "0.4%", "23%". */
function pct(part: number, whole: number): string {
  if (whole <= 0) return "0%";
  const p = (part / whole) * 100;
  if (p === 0) return "0%";
  if (p < 10) return `${p.toFixed(1)}%`;
  return `${Math.round(p)}%`;
}

/**
 * Percentage labels for the parts of a whole that always sum to exactly 100%. Rounding each share
 * on its own (e.g. "98%" + "1.8%" = 99.8%) leaves a donut's labels short of the full ring they draw;
 * this distributes the rounding remainder by the largest-fractional-part rule, at one-decimal
 * precision, so the legend agrees with the chart. Returns "0%" for every slice when the whole is 0.
 */
function shareLabels(values: number[]): string[] {
  const whole = values.reduce((s, v) => s + v, 0);
  if (whole <= 0) return values.map(() => "0%");
  // Work in tenths of a percent so labels carry one decimal and sum to exactly 1000 (= 100.0%).
  const tenths = values.map((v) => (v / whole) * 1000);
  const floor = tenths.map(Math.floor);
  let leftover = 1000 - floor.reduce((s, v) => s + v, 0);
  const byFrac = tenths
    .map((v, i) => ({ i, frac: v - Math.floor(v) }))
    .sort((a, b) => b.frac - a.frac);
  for (let k = 0; leftover > 0 && k < byFrac.length; k++, leftover--) {
    floor[byFrac[k].i] += 1;
  }
  return floor.map((t) => {
    const p = t / 10;
    return Number.isInteger(p) ? `${p}%` : `${p.toFixed(1)}%`;
  });
}

// On a 1-day window the x-axis reads as wall-clock time; over longer windows a
// month/day label keeps ticks legible. The tooltip always shows the full
// timestamp so a hovered point is never ambiguous.
function tickTime(range: MetricsRange) {
  return (value: number): string => {
    const d = new Date(value);
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

function fullTime(value: number): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString();
}

// Map a status class to a semantic chart tone: success / neutral / amber / red.
// 3xx (and anything non-2/4/5) reads as neutral, but a mid-gray (chart-2) so the
// slice stays visible on a white card; the faint chart-3 nearly vanishes.
function statusColor(cls: string): string {
  if (cls.startsWith("2")) return "var(--color-success)";
  if (cls.startsWith("4")) return "var(--color-warning)";
  if (cls.startsWith("5")) return "var(--color-destructive)";
  return "var(--color-chart-2)";
}

export function Metrics() {
  const [range, setRange] = useState<MetricsRange>("1d");

  // `range` stays OUT of the resource deps on purpose: switching it re-fetches through the
  // stale-while-revalidate `refresh` path (loadRef always closes over the latest range), so the
  // dashboard stays mounted and never tears down to a skeleton — only the first mount does.
  const metrics = useResource(() => api.metrics(range), []);
  const overview = useResource(() => api.overview(), []);
  // Live: refresh both resources when the server pushes a "metrics" snapshot.
  useLiveTopic("metrics", () => {
    metrics.refresh();
    overview.refresh();
  });

  const { data, error, loading } = metrics;

  const firstRange = useRef(true);
  useEffect(() => {
    if (firstRange.current) {
      firstRange.current = false;
      return;
    }
    metrics.refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [range]);

  // A single Refresh button re-fetches both resources; the busy/disabled state
  // tracks the metrics load (the primary content) but kicks both.
  const refresh = () => {
    metrics.refresh();
    overview.refresh();
  };

  const tickFmt = useMemo(() => tickTime(range), [range]);

  return (
    <Page>
      <PageHeader
        title="Metrics"
        description={`API request volume and usage analytics over the last ${
          RANGES.find((r) => r.value === range)?.label ?? "period"
        }.`}
      />

      {/* The range filter swaps the entire view, so it is a single-choice control (radiogroup),
          not a tablist — keeps the underline-tab look with a real accessible name + arrow-key nav. */}
      <RangePicker value={range} onChange={setRange} />

      {error ? (
        <ErrorAlert
          title="Could not load metrics"
          message={error}
          onRetry={refresh}
        />
      ) : null}

      {loading ? (
        <MetricsSkeleton />
      ) : data ? (
        <Dashboard
          data={data}
          range={range}
          tickFmt={tickFmt}
          overview={overview.data}
        />
      ) : null}
    </Page>
  );
}

// The range filter as a labelled segmented control. It swaps the whole dashboard, so semantically
// it's a single-choice radiogroup (the old Radix tablist left a dangling aria-controls and no
// accessible name). Roving tabindex + arrow keys keep it keyboard-friendly; the underline-active
// look matches the rest of the console.
function RangePicker({
  value,
  onChange,
}: {
  value: MetricsRange;
  onChange: (v: MetricsRange) => void;
}) {
  const idx = RANGES.findIndex((r) => r.value === value);
  function onKeyDown(e: React.KeyboardEvent) {
    let next = idx;
    if (e.key === "ArrowRight" || e.key === "ArrowDown")
      next = (idx + 1) % RANGES.length;
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp")
      next = (idx - 1 + RANGES.length) % RANGES.length;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = RANGES.length - 1;
    else return;
    e.preventDefault();
    onChange(RANGES[next].value);
  }
  return (
    <div
      role="radiogroup"
      aria-label="Time range"
      onKeyDown={onKeyDown}
      className="mb-4 flex w-full gap-1 border-b"
    >
      {RANGES.map((r) => {
        const active = r.value === value;
        return (
          <button
            key={r.value}
            type="button"
            role="radio"
            aria-checked={active}
            tabIndex={active ? 0 : -1}
            onClick={() => onChange(r.value)}
            className={cn(
              "-mb-px border-b-2 px-2.5 py-1.5 text-sm font-medium transition-colors duration-150 ease-out",
              active
                ? "border-foreground text-foreground"
                : "border-transparent text-muted-foreground hover:text-foreground",
            )}
          >
            {r.label}
          </button>
        );
      })}
    </div>
  );
}

// The dashboard grid. `total === 0` still renders the system stat tiles (which
// describe stored data, not request traffic) with an empty-state in between.
function Dashboard({
  data,
  range,
  tickFmt,
  overview,
}: {
  data: RequestMetricsResp;
  range: MetricsRange;
  tickFmt: (v: number) => string;
  overview:
    | { objects: number; physical_bytes: number; compression_ratio: number }
    | undefined;
}) {
  const total = data.total;
  const empty = total === 0;

  const avgRate = total / RANGE_SECS[range];
  const peakRate =
    data.window_secs > 0 ? data.peak_window_count / data.window_secs : 0;

  const throughputData = useMemo(
    () =>
      data.timeline.map((p) => ({
        ts_ms: p.ts_ms,
        bytes: p.bytes_in + p.bytes_out,
      })),
    [data.timeline],
  );

  const reads = useMemo(() => splitOps(data.by_operation), [data.by_operation]);

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {/* Request-traffic stat tiles (suppressed when the window is empty). The section label makes
          the windowed scope explicit, so these read as distinct from the point-in-time storage tiles. */}
      {!empty ? (
        <>
          <h2 className="col-span-full text-sm font-medium text-muted-foreground">
            Activity · last{" "}
            {RANGES.find((r) => r.value === range)?.label ?? "period"}
          </h2>
          <StatCard label="Total requests" value={count(total)} />
          <StatCard label="Avg req/s" value={reqPerSec(avgRate)} />
          <StatCard label="Peak req/s" value={reqPerSec(peakRate)} />
          <StatCard
            label="Error rate"
            value={pct(data.total_errors, total)}
            sub={`${count(data.total_errors)} errors`}
          />
          <StatCard label="Active buckets" value={count(data.active_buckets)} />
          <StatCard label="Data received" value={bytes(data.total_bytes_in)} />
          <StatCard label="Data sent" value={bytes(data.total_bytes_out)} />
          <StatCard label="Avg latency" value={ms(data.latency_avg_ms)} />
          <StatCard label="p95 latency" value={ms(data.latency_p95_ms)} />
        </>
      ) : null}

      {/* System stat tiles from the overview snapshot — current state, NOT windowed; the label
          keeps that scope clear when the range above changes. */}
      {overview ? (
        <>
          <h2 className="col-span-full text-sm font-medium text-muted-foreground">
            Stored data
          </h2>
          <StatCard label="Objects" value={count(overview.objects)} />
          <StatCard label="Storage used" value={bytes(overview.physical_bytes)} />
          <StatCard
            label="Compression"
            value={`${overview.compression_ratio.toFixed(2)}×`}
          />
        </>
      ) : null}

      {/* When there's no request traffic, say so — but keep the system tiles
          above and skip every traffic chart below. */}
      {empty ? (
        <div className="sm:col-span-2 lg:col-span-3">
          <EmptyState
            icon={ActivityIcon}
            title="No request activity yet"
            body="Once objects are read and written over the S3 API, request volume will appear here."
          />
        </div>
      ) : (
        <>
          {/* Panel A — the hero: request volume over the window. Spans wide. */}
          <ChartCard
            className="lg:col-span-3"
            title="Requests over time"
            description={`${count(total)} requests in this window.`}
          >
            <div
              className="h-72 w-full"
              role="img"
              aria-label={`Requests over time: ${count(total)} requests in this window.`}
            >
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  accessibilityLayer={false}
                  data={data.timeline}
                  margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
                >
                  <defs>
                    <linearGradient
                      id="m-requests"
                      x1="0"
                      y1="0"
                      x2="0"
                      y2="1"
                    >
                      <stop
                        offset="0%"
                        stopColor="var(--color-chart-4)"
                        stopOpacity={0.25}
                      />
                      <stop
                        offset="100%"
                        stopColor="var(--color-chart-4)"
                        stopOpacity={0}
                      />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="var(--color-border)"
                    vertical={false}
                  />
                  <TimeXAxis tickFmt={tickFmt} />
                  <YAxis
                    allowDecimals={false}
                    width={44}
                    tickLine={false}
                    axisLine={false}
                    tick={axisTick}
                    tickFormatter={(v: number) => compactNum(v)}
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
                    stroke="var(--color-chart-4)"
                    strokeWidth={2}
                    fill="url(#m-requests)"
                    dot={data.timeline.length < 2}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>

          {/* Panel B — errors over time. */}
          <ChartCard
            title="Errors over time"
            description={`${count(data.total_errors)} errors (${pct(
              data.total_errors,
              total,
            )}).`}
          >
            <div
              className="h-56 w-full"
              role="img"
              aria-label={`Errors over time: ${count(data.total_errors)} errors (${pct(
                data.total_errors,
                total,
              )}).`}
            >
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  accessibilityLayer={false}
                  data={data.timeline}
                  margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
                >
                  <defs>
                    <linearGradient id="m-errors" x1="0" y1="0" x2="0" y2="1">
                      <stop
                        offset="0%"
                        stopColor="var(--color-destructive)"
                        stopOpacity={0.25}
                      />
                      <stop
                        offset="100%"
                        stopColor="var(--color-destructive)"
                        stopOpacity={0}
                      />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="var(--color-border)"
                    vertical={false}
                  />
                  <TimeXAxis tickFmt={tickFmt} />
                  <YAxis
                    allowDecimals={false}
                    width={44}
                    tickLine={false}
                    axisLine={false}
                    tick={axisTick}
                    tickFormatter={(v: number) => compactNum(v)}
                  />
                  <Tooltip
                    cursor={{ stroke: "var(--color-border)" }}
                    contentStyle={tooltipStyle}
                    labelStyle={tooltipLabelStyle}
                    itemStyle={tooltipItemStyle}
                    labelFormatter={(label) => fullTime(Number(label))}
                    formatter={(value) => [count(Number(value)), "Errors"]}
                  />
                  <Area
                    type="monotone"
                    dataKey="errors"
                    stroke="var(--color-destructive)"
                    strokeWidth={2}
                    fill="url(#m-errors)"
                    dot={data.timeline.length < 2}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>

          {/* Panel C — throughput (bytes in + out) over time. */}
          <ChartCard
            title="Throughput over time"
            description={`${bytes(
              data.total_bytes_in + data.total_bytes_out,
            )} transferred.`}
          >
            <div
              className="h-56 w-full"
              role="img"
              aria-label={`Throughput over time: ${bytes(
                data.total_bytes_in + data.total_bytes_out,
              )} transferred.`}
            >
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  accessibilityLayer={false}
                  data={throughputData}
                  margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
                >
                  <defs>
                    <linearGradient
                      id="m-throughput"
                      x1="0"
                      y1="0"
                      x2="0"
                      y2="1"
                    >
                      <stop
                        offset="0%"
                        stopColor="var(--color-chart-5)"
                        stopOpacity={0.3}
                      />
                      <stop
                        offset="100%"
                        stopColor="var(--color-chart-5)"
                        stopOpacity={0}
                      />
                    </linearGradient>
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="var(--color-border)"
                    vertical={false}
                  />
                  <TimeXAxis tickFmt={tickFmt} />
                  <YAxis
                    width={64}
                    tickLine={false}
                    axisLine={false}
                    tick={axisTick}
                    tickFormatter={(v: number) => bytes(v)}
                  />
                  <Tooltip
                    cursor={{ stroke: "var(--color-border)" }}
                    contentStyle={tooltipStyle}
                    labelStyle={tooltipLabelStyle}
                    itemStyle={tooltipItemStyle}
                    labelFormatter={(label) => fullTime(Number(label))}
                    formatter={(value) => [bytes(Number(value)), "Transferred"]}
                  />
                  <Area
                    type="monotone"
                    dataKey="bytes"
                    stroke="var(--color-chart-5)"
                    strokeWidth={2}
                    fill="url(#m-throughput)"
                    dot={data.timeline.length < 2}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>

          {/* Panel D — average latency over time. The line plots the per-window AVERAGE only; p95 is
              a single range-wide figure (in the caption + its own stat tile), not a plotted series. */}
          <ChartCard
            title="Average latency over time"
            description={`${ms(data.latency_avg_ms)} average over the window (p95 ${ms(
              data.latency_p95_ms,
            )}).`}
          >
            <div
              className="h-56 w-full"
              role="img"
              aria-label={`Average latency over time, ${ms(data.latency_avg_ms)} average over the window.`}
            >
              <ResponsiveContainer width="100%" height="100%">
                <LineChart
                  accessibilityLayer={false}
                  data={data.timeline}
                  margin={{ top: 8, right: 8, bottom: 0, left: 0 }}
                >
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="var(--color-border)"
                    vertical={false}
                  />
                  <TimeXAxis tickFmt={tickFmt} />
                  <YAxis
                    width={52}
                    tickLine={false}
                    axisLine={false}
                    tick={axisTick}
                    tickFormatter={(v: number) =>
                      v >= 1000
                        ? `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}s`
                        : `${Math.round(v)}ms`
                    }
                  />
                  <Tooltip
                    cursor={{ stroke: "var(--color-border)" }}
                    contentStyle={tooltipStyle}
                    labelStyle={tooltipLabelStyle}
                    itemStyle={tooltipItemStyle}
                    labelFormatter={(label) => fullTime(Number(label))}
                    formatter={(value) => [ms(Number(value)), "Avg latency"]}
                  />
                  <Line
                    type="monotone"
                    dataKey="latency_avg_ms"
                    stroke="var(--color-chart-4)"
                    strokeWidth={2}
                    dot={data.timeline.length < 2}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </ChartCard>

          {/* Panel E — request mix by operation, ranked. Rendered as full-width ranking bars
              (the same affordance as the bucket panels below) rather than a recharts horizontal
              bar chart, whose category axis ate most of the narrow grid cell. */}
          <ChartCard
            title="By operation"
            description="Requests broken down by S3 operation."
          >
            {data.by_operation.length === 0 ? (
              <PanelEmpty>No operations recorded in this window.</PanelEmpty>
            ) : (
              <BucketBars
                rows={[...data.by_operation]
                  .sort((a, b) => b.count - a.count)
                  .slice(0, 10)
                  .map((o) => ({
                    bucket: o.operation,
                    value: o.count,
                    display: count(o.count),
                    aria: `${o.operation}: ${count(o.count)} requests`,
                  }))}
              />
            )}
          </ChartCard>

          {/* Panel F — response status mix, as a donut. */}
          <ChartCard
            title="Status mix"
            description="Responses by HTTP status class."
          >
            {data.by_status.length === 0 ? (
              <PanelEmpty>No responses recorded in this window.</PanelEmpty>
            ) : (
              <StatusDonut by_status={data.by_status} />
            )}
          </ChartCard>

          {/* Panel G — busiest buckets by request count. */}
          <ChartCard
            title="Most active buckets"
            description="Buckets ranked by request count."
          >
            <BucketBars
              rows={data.top_buckets.map((b) => ({
                bucket: b.bucket,
                value: b.count,
                display: `${count(b.count)}`,
                aria: `${b.bucket}: ${count(b.count)} requests`,
              }))}
            />
          </ChartCard>

          {/* Panel H — top buckets by data transferred. Uses the server's by-bytes ranking, not the
              by-count cohort re-sorted (which would silently omit a low-traffic bucket that moved the
              most data — e.g. a backup target). */}
          <ChartCard
            title="Top buckets by data"
            description="Buckets ranked by bytes transferred."
          >
            <BucketBars
              rows={data.top_buckets_by_bytes.map((b) => ({
                bucket: b.bucket,
                value: b.bytes,
                display: bytes(b.bytes),
                aria: `${b.bucket}: ${bytes(b.bytes)} transferred`,
              }))}
            />
          </ChartCard>

          {/* Panel I — reads vs. writes split, as a small donut. */}
          <ChartCard
            title="Reads vs writes"
            description="Read versus mutating operations."
          >
            {reads.reads + reads.writes === 0 ? (
              <PanelEmpty>No classifiable operations in this window.</PanelEmpty>
            ) : (
              <ReadsWritesDonut reads={reads.reads} writes={reads.writes} />
            )}
          </ChartCard>
        </>
      )}
    </div>
  );
}

// Sum read vs. write request counts from the per-operation roll-up.
function splitOps(ops: MetricOp[]): { reads: number; writes: number } {
  let reads = 0;
  let writes = 0;
  for (const op of ops) {
    if (READ_OPS.has(op.operation)) reads += op.count;
    else if (WRITE_OPS.has(op.operation)) writes += op.count;
  }
  return { reads, writes };
}

// A status-class donut: 2xx green, 3xx neutral, 4xx amber, 5xx red. A small
// legend below names each slice with its share.
function StatusDonut({ by_status }: { by_status: MetricStatus[] }) {
  const total = by_status.reduce((s, x) => s + x.count, 0);
  const sorted = [...by_status].sort((a, b) =>
    a.status_class.localeCompare(b.status_class),
  );
  const shares = shareLabels(sorted.map((s) => s.count));
  return (
    <div className="flex flex-col items-center gap-4">
      <div
        className="relative h-48 w-full"
        role="img"
        aria-label={`Response status mix across ${count(total)} responses by HTTP status class.`}
      >
        <ResponsiveContainer width="100%" height="100%">
          <PieChart accessibilityLayer={false}>
            <Pie
              data={sorted}
              dataKey="count"
              nameKey="status_class"
              innerRadius={48}
              outerRadius={72}
              paddingAngle={2}
              stroke="var(--color-background)"
              strokeWidth={2}
              isAnimationActive={false}
            >
              {sorted.map((s) => (
                <Cell
                  key={s.status_class}
                  fill={statusColor(s.status_class)}
                />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        {/* The whole the slices divide, anchored in the hole — the focal value the empty centre wasted. */}
        <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center gap-0.5">
          <span className="text-xl font-semibold leading-none tabular-nums">
            {compactNum(total)}
          </span>
          <span className="text-[11px] text-muted-foreground">responses</span>
        </div>
      </div>
      <ul className="flex w-full flex-col items-center gap-y-1.5 text-[13px] sm:flex-row sm:flex-wrap sm:justify-center sm:gap-x-4 sm:gap-y-2">
        {sorted.map((s, i) => (
          <li
            key={s.status_class}
            className="flex items-center gap-1.5 whitespace-nowrap"
          >
            <span
              aria-hidden="true"
              className="inline-block size-2.5 rounded-sm"
              style={{ background: statusColor(s.status_class) }}
            />
            <span className="font-medium">{s.status_class}</span>
            <span className="text-muted-foreground">
              {count(s.count)} · {shares[i]}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// Reads vs. writes as a two-slice donut with a legend.
function ReadsWritesDonut({
  reads,
  writes,
}: {
  reads: number;
  writes: number;
}) {
  const total = reads + writes;
  // Two clearly-distinct monochrome tones (not the old two near-identical blues), matching the
  // ranking-bar treatment and keeping blue reserved for links/focus per the design system.
  const slices = [
    { name: "Reads", value: reads, fill: "var(--color-chart-1)" },
    { name: "Writes", value: writes, fill: "var(--color-chart-2)" },
  ];
  const shares = shareLabels(slices.map((s) => s.value));
  return (
    <div className="flex flex-col items-center gap-4">
      <div
        className="relative h-48 w-full"
        role="img"
        aria-label={`Reads versus writes across ${count(total)} operations: ${count(reads)} reads, ${count(writes)} writes.`}
      >
        <ResponsiveContainer width="100%" height="100%">
          <PieChart accessibilityLayer={false}>
            <Pie
              data={slices}
              dataKey="value"
              nameKey="name"
              innerRadius={48}
              outerRadius={72}
              paddingAngle={2}
              stroke="var(--color-background)"
              strokeWidth={2}
              isAnimationActive={false}
            >
              {slices.map((s) => (
                <Cell key={s.name} fill={s.fill} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
        {/* The whole the slices divide, anchored in the hole — the focal value the empty centre wasted. */}
        <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center gap-0.5">
          <span className="text-xl font-semibold leading-none tabular-nums">
            {compactNum(total)}
          </span>
          <span className="text-[11px] text-muted-foreground">operations</span>
        </div>
      </div>
      <ul className="flex w-full flex-col items-center gap-y-1.5 text-[13px] sm:flex-row sm:flex-wrap sm:justify-center sm:gap-x-4 sm:gap-y-2">
        {slices.map((s, i) => (
          <li key={s.name} className="flex items-center gap-1.5 whitespace-nowrap">
            <span
              aria-hidden="true"
              className="inline-block size-2.5 rounded-sm"
              style={{ background: s.fill }}
            />
            <span className="font-medium">{s.name}</span>
            <span className="text-muted-foreground">
              {count(s.value)} · {shares[i]}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// A bar-list of buckets, each row a UsageBar scaled to the busiest. Shared by
// the "most active" (by count) and "top by data" (by bytes) panels.
function BucketBars({
  rows,
}: {
  rows: { bucket: string; value: number; display: string; aria: string }[];
}) {
  if (rows.length === 0) {
    return <PanelEmpty>No bucket activity in this window.</PanelEmpty>;
  }
  const max = rows.reduce((m, r) => Math.max(m, r.value), 0);
  return (
    <ul className="space-y-3">
      {rows.map((r) => (
        <li key={r.bucket} className="space-y-1.5">
          <div className="flex items-baseline justify-between gap-3">
            <span
              className="min-w-0 truncate font-mono text-[13px]"
              title={r.bucket}
            >
              {r.bucket}
            </span>
            <span className="shrink-0 font-mono text-[13px] tabular-nums">
              {r.display}
            </span>
          </div>
          <UsageBar
            percent={(r.value / Math.max(1, max)) * 100}
            label={r.aria}
          />
        </li>
      ))}
    </ul>
  );
}

// A chart panel: a Card with a title + description and the chart as children.
function ChartCard({
  title,
  description,
  className,
  children,
}: {
  title: string;
  description: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <Card className={`gap-4 ${className ?? ""}`}>
      <CardHeader className="gap-1">
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}

// A small, consistent in-panel empty state — one convention across every chart
// panel (the page-level EmptyState is reserved for the whole-view empty case).
function PanelEmpty({ children }: { children: React.ReactNode }) {
  return (
    <p className="flex min-h-32 items-center justify-center text-center text-sm text-muted-foreground">
      {children}
    </p>
  );
}

// The shared time x-axis used by every timeline chart.
function TimeXAxis({ tickFmt }: { tickFmt: (v: number) => string }) {
  return (
    <XAxis
      dataKey="ts_ms"
      type="number"
      domain={["dataMin", "dataMax"]}
      scale="time"
      tickFormatter={tickFmt}
      tickLine={false}
      axisLine={{ stroke: "var(--color-border)" }}
      tick={axisTick}
      minTickGap={40}
    />
  );
}

const axisTick = {
  fill: "var(--color-muted-foreground)",
  fontSize: 12,
} as const;

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
  color: "var(--color-foreground)",
  fontWeight: 500,
  marginBottom: 2,
};

const tooltipItemStyle: React.CSSProperties = {
  color: "var(--color-popover-foreground)",
};

/** First-paint skeletons mirroring the dashboard grid so nothing jumps. */
function MetricsSkeleton() {
  return (
    <>
      <p className="sr-only" role="status">
        Loading metrics…
      </p>
      <div
        className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3"
        aria-hidden="true"
      >
        {Array.from({ length: 12 }).map((_, i) => (
          <StatCard key={i} label="" value="" loading />
        ))}
        <Skeleton className="h-80 rounded-lg lg:col-span-3" />
        <Skeleton className="h-64 rounded-lg" />
        <Skeleton className="h-64 rounded-lg" />
        <Skeleton className="h-64 rounded-lg" />
      </div>
    </>
  );
}
