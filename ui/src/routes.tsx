import { Suspense, lazy } from "react";
import { Navigate, createHashRouter } from "react-router";
import { AppShell } from "@/components/app-shell";
import { Page } from "@/components/page-header";
import { Skeleton } from "@/components/ui/skeleton";
import { RequireAuth } from "@/providers/auth-provider";
import { Activity } from "@/views/activity";
import { BucketBrowser } from "@/views/bucket-browser";
import { BucketDetail } from "@/views/bucket-detail";
import { BucketSettings } from "@/views/bucket-settings";
import { Buckets } from "@/views/buckets";
import { Credentials } from "@/views/credentials";
import { Login } from "@/views/login";
import { Overview } from "@/views/overview";
import { Replication } from "@/views/replication";
import { Tags } from "@/views/tags";
import { UserDetail } from "@/views/user-detail";
import { Users } from "@/views/users";

// The Metrics view pulls in the charting library (recharts); lazy-load it so that
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

// Hash routing on purpose: the server serves the SPA shell only at `/` on the
// UI port; every other path is the S3 data plane the console itself uses for
// object bytes, so history-mode routes would collide with /{bucket}/{key}.
export const router = createHashRouter([
  { path: "/login", element: <Login /> },
  {
    element: (
      <RequireAuth>
        <AppShell />
      </RequireAuth>
    ),
    children: [
      { index: true, element: <Navigate to="/overview" replace /> },
      { path: "overview", element: <Overview /> },
      {
        path: "metrics",
        element: (
          <Suspense fallback={metricsFallback}>
            <Metrics />
          </Suspense>
        ),
      },
      { path: "buckets", element: <Buckets /> },
      {
        path: "buckets/:name",
        element: <BucketDetail />,
        children: [
          { index: true, element: <Navigate to="browser" replace /> },
          { path: "browser", element: <BucketBrowser /> },
          { path: "settings", element: <BucketSettings /> },
        ],
      },
      { path: "users", element: <Users /> },
      { path: "users/:id", element: <UserDetail /> },
      { path: "credentials", element: <Credentials /> },
      { path: "tags", element: <Tags /> },
      { path: "activity", element: <Activity /> },
      { path: "replication", element: <Replication /> },
      // Parity with the old router: anything unknown lands on the overview.
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
