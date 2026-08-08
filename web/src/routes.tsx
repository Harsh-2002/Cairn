import { Navigate, createHashRouter } from "react-router";
import { AppShell } from "@/components/app-shell";
import { RouteError } from "@/components/route-error";
import { RequireAuth } from "@/providers/auth-provider";

// Hash routing on purpose: the control listener serves the SPA shell only at `/` plus concrete
// embedded assets, and fail-closes every other non-API path. A history router would require a new,
// explicit server route family; it must never fall through into the separate S3 data origin.
export const router = createHashRouter([
  {
    path: "/login",
    errorElement: <RouteError />,
    lazy: async () => {
      const { Login } = await import("@/views/login");
      return { Component: Login };
    },
  },
  {
    errorElement: <RouteError />,
    element: (
      <RequireAuth>
        <AppShell />
      </RequireAuth>
    ),
    children: [
      { index: true, element: <Navigate to="/overview" replace /> },
      {
        path: "overview",
        lazy: async () => {
          const { Overview } = await import("@/views/overview");
          return { Component: Overview };
        },
      },
      {
        path: "metrics",
        lazy: async () => {
          const { MetricsRoute } = await import("@/views/metrics-route");
          return { Component: MetricsRoute };
        },
      },
      {
        path: "buckets",
        lazy: async () => {
          const { Buckets } = await import("@/views/buckets");
          return { Component: Buckets };
        },
      },
      {
        path: "buckets/:name",
        lazy: async () => {
          const { BucketDetail } = await import("@/views/bucket-detail");
          return { Component: BucketDetail };
        },
        children: [
          { index: true, element: <Navigate to="browser" replace /> },
          {
            path: "browser",
            lazy: async () => {
              const { BucketBrowser } = await import("@/views/bucket-browser");
              return { Component: BucketBrowser };
            },
          },
          {
            path: "uploads",
            lazy: async () => {
              const { MultipartUploads } = await import("@/views/multipart-uploads");
              return { Component: MultipartUploads };
            },
          },
          {
            path: "settings",
            lazy: async () => {
              const { BucketSettings } = await import("@/views/bucket-settings");
              return { Component: BucketSettings };
            },
          },
        ],
      },
      {
        path: "users",
        lazy: async () => {
          const { Users } = await import("@/views/users");
          return { Component: Users };
        },
      },
      {
        path: "users/:id",
        lazy: async () => {
          const { UserDetail } = await import("@/views/user-detail");
          return { Component: UserDetail };
        },
      },
      {
        path: "credentials",
        lazy: async () => {
          const { Credentials } = await import("@/views/credentials");
          return { Component: Credentials };
        },
      },
      {
        path: "tags",
        lazy: async () => {
          const { Tags } = await import("@/views/tags");
          return { Component: Tags };
        },
      },
      {
        path: "activity",
        lazy: async () => {
          const { Activity } = await import("@/views/activity");
          return { Component: Activity };
        },
      },
      {
        path: "replication",
        lazy: async () => {
          const { Replication } = await import("@/views/replication");
          return { Component: Replication };
        },
      },
      {
        path: "imports",
        lazy: async () => {
          const { Imports } = await import("@/views/imports");
          return { Component: Imports };
        },
      },
      // Parity with the old router: anything unknown lands on the overview.
      { path: "*", element: <Navigate to="/overview" replace /> },
    ],
  },
]);
