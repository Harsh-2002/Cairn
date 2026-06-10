import { NavLink, Outlet, useLocation, useNavigate, useParams } from "react-router";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Page } from "@/components/page-header";

/**
 * The /buckets/:name layout: breadcrumb, bucket title, and the Browser /
 * Settings tab bar. The active tab mirrors the child route so it deep-links;
 * the tab body itself is rendered by the child route via <Outlet/>.
 */
export function BucketDetail() {
  const { name = "" } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const location = useLocation();

  const tab = location.pathname.endsWith("/settings") ? "settings" : "browser";

  return (
    <Page>
      <Breadcrumb className="mb-3">
        <BreadcrumbList>
          <BreadcrumbItem>
            <BreadcrumbLink asChild>
              <NavLink to="/buckets">Buckets</NavLink>
            </BreadcrumbLink>
          </BreadcrumbItem>
          <BreadcrumbSeparator />
          <BreadcrumbItem>
            <BreadcrumbPage className="font-mono text-[13px]">{name}</BreadcrumbPage>
          </BreadcrumbItem>
        </BreadcrumbList>
      </Breadcrumb>

      <h1 className="mb-5 font-mono text-xl font-semibold tracking-tight">{name}</h1>

      {/* The component's "line" variant IS the underline style — it carries the
          active-tab indicator and the dark-mode handling, so no per-trigger
          border overrides (which fought the pill variant and left stray boxes). */}
      <Tabs
        value={tab}
        onValueChange={(v) => navigate(`/buckets/${encodeURIComponent(name)}/${v}`)}
      >
        <TabsList
          variant="line"
          className="h-auto! w-full justify-start border-b p-0 pb-1"
        >
          <TabsTrigger value="browser" className="flex-none px-2.5 py-1.5">
            Browser
          </TabsTrigger>
          <TabsTrigger value="settings" className="flex-none px-2.5 py-1.5">
            Settings
          </TabsTrigger>
        </TabsList>
      </Tabs>

      <div className="pt-6">
        <Outlet />
      </div>
    </Page>
  );
}
