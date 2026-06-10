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

      <Tabs
        value={tab}
        onValueChange={(v) => navigate(`/buckets/${encodeURIComponent(name)}/${v}`)}
      >
        <TabsList className="h-auto! w-full justify-start gap-4 rounded-none border-b bg-transparent p-0">
          <TabsTrigger
            value="browser"
            className="flex-none rounded-none border-b-2 border-transparent px-1 pb-2.5 pt-1 text-sm data-[state=active]:border-foreground data-[state=active]:bg-transparent data-[state=active]:shadow-none"
          >
            Browser
          </TabsTrigger>
          <TabsTrigger
            value="settings"
            className="flex-none rounded-none border-b-2 border-transparent px-1 pb-2.5 pt-1 text-sm data-[state=active]:border-foreground data-[state=active]:bg-transparent data-[state=active]:shadow-none"
          >
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
