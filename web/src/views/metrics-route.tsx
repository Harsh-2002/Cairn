// The Metrics route, code-split. Kept out of `routes.tsx` so that file exports only the router:
// a module that mixes a component with a non-component export breaks Fast Refresh, and the
// charting weight belongs next to the thing that loads it either way.
import { Suspense, lazy } from "react";
import { Page } from "@/components/page-header";
import { Skeleton } from "@/components/primitives/skeleton";

// Pulls in the charting library (recharts); lazy-load it so that
// weight is code-split into its own chunk and never ships in the initial bundle.
const Metrics = lazy(() =>
  import("@/views/metrics").then((m) => ({ default: m.Metrics })),
);

// While the metrics chunk loads, show a skeleton with the SAME page chrome the real view renders
// (Page padding, header, range tabs, chart grid) so the swap is seamless — no bare gray block
// flashing at a different size/position before the dashboard appears.
const metricsFallback = (
  <Page>
    <header className="mb-6 flex items-start justify-between gap-3 border-b pb-5">
      <div className="space-y-2">
        <Skeleton className="h-6 w-28" />
        <Skeleton className="h-4 w-72" />
      </div>
      <Skeleton className="h-9 w-24" />
    </header>
    <Skeleton className="mb-4 h-9 w-full max-w-md" />
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
      <Skeleton className="h-72 rounded-lg lg:col-span-3" />
      <Skeleton className="h-56 rounded-lg" />
      <Skeleton className="h-56 rounded-lg" />
      <Skeleton className="h-56 rounded-lg" />
    </div>
  </Page>
);

export function MetricsRoute() {
  return (
    <Suspense fallback={metricsFallback}>
      <Metrics />
    </Suspense>
  );
}
