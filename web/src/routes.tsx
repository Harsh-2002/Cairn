import { Navigate, createHashRouter } from "react-router";
import { AppShell } from "@/components/app-shell";
import { MetricsRoute } from "@/views/metrics-route";
import { RequireAuth } from "@/providers/auth-provider";
import { Activity } from "@/views/activity";
import { BucketBrowser } from "@/views/bucket-browser";
import { BucketDetail } from "@/views/bucket-detail";
import { BucketSettings } from "@/views/bucket-settings";
import { Buckets } from "@/views/buckets";
import { Credentials } from "@/views/credentials";
import { Login } from "@/views/login";
import { MultipartUploads } from "@/views/multipart-uploads";
import { Overview } from "@/views/overview";
import { Replication } from "@/views/replication";
import { Imports } from "@/views/imports";
import { Tags } from "@/views/tags";
import { UserDetail } from "@/views/user-detail";
import { Users } from "@/views/users";

// Hash routing on purpose: the control listener serves the SPA shell only at `/` plus concrete
// embedded assets, and fail-closes every other non-API path. A history router would require a new,
// explicit server route family; it must never fall through into the separate S3 data origin.
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
        element: <MetricsRoute />,
      },
      { path: "buckets", element: <Buckets /> },
      {
        path: "buckets/:name",
        element: <BucketDetail />,
        children: [
          { index: true, element: <Navigate to="browser" replace /> },
          { path: "browser", element: <BucketBrowser /> },
          { path: "uploads", element: <MultipartUploads /> },
          { path: "settings", element: <BucketSettings /> },
        ],
      },
      { path: "users", element: <Users /> },
      { path: "users/:id", element: <UserDetail /> },
      { path: "credentials", element: <Credentials /> },
      { path: "tags", element: <Tags /> },
      { path: "activity", element: <Activity /> },
      { path: "replication", element: <Replication /> },
      { path: "imports", element: <Imports /> },
      // Parity with the old router: anything unknown lands on the overview.
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
