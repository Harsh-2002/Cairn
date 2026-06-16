import { Suspense, lazy } from "react";
import { Navigate, createHashRouter } from "react-router";
import { AppShell } from "@/components/app-shell";
import { Skeleton } from "@/components/ui/skeleton";
import { RequireAuth } from "@/providers/auth-provider";
import { Activity } from "@/views/activity";
import { BucketBrowser } from "@/views/bucket-browser";
import { BucketDetail } from "@/views/bucket-detail";
import { BucketSettings } from "@/views/bucket-settings";
import { Buckets } from "@/views/buckets";
import { Login } from "@/views/login";
import { Overview } from "@/views/overview";
import { Replication } from "@/views/replication";
import { UserDetail } from "@/views/user-detail";
import { Users } from "@/views/users";

// The Metrics view pulls in the charting library (recharts); lazy-load it so that
// weight is code-split into its own chunk and never ships in the initial bundle.
const Metrics = lazy(() =>
  import("@/views/metrics").then((m) => ({ default: m.Metrics })),
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
          <Suspense fallback={<Skeleton className="h-72 rounded-lg" />}>
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
      { path: "activity", element: <Activity /> },
      { path: "replication", element: <Replication /> },
      // Parity with the old router: anything unknown lands on the overview.
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
