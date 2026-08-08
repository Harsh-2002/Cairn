import { NavLink, Outlet, useParams } from "react-router";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/primitives/breadcrumb";
import { Page } from "@/components/page-header";
import { cn } from "@/lib/utils";

/**
 * The /buckets/:name layout: breadcrumb, bucket title, and the Browser /
 * Settings tab bar. The active tab mirrors the child route so it deep-links;
 * the tab body itself is rendered by the child route via <Outlet/>.
 */
export function BucketDetail() {
  const { name = "" } = useParams<{ name: string }>();
  const sections = [
    { path: "browser", label: "Browser" },
    { path: "uploads", label: "Uploads" },
    { path: "settings", label: "Settings" },
  ];

  return (
    <Page>
      <Breadcrumb className="mb-3" aria-label="Bucket location">
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

      {/* These change the URL and page content, so they are links rather than ARIA tabs (which
          require an in-DOM tabpanel for every trigger). The active underline keeps the same visual
          language while browser link behavior and assistive-technology semantics stay intact. */}
      <nav
        aria-label="Bucket sections"
        className="flex w-full items-center gap-1 border-b pb-1"
      >
        {sections.map((section) => (
          <NavLink
            key={section.path}
            to={`/buckets/${encodeURIComponent(name)}/${section.path}`}
            className={({ isActive }) =>
              cn(
                "relative rounded-md px-2.5 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:text-foreground",
                "after:absolute after:inset-x-0 after:-bottom-1.5 after:h-0.5 after:bg-foreground after:opacity-0 after:transition-opacity",
                isActive && "text-foreground after:opacity-100",
              )
            }
          >
            {section.label}
          </NavLink>
        ))}
      </nav>

      <div className="pt-6">
        <Outlet />
      </div>
    </Page>
  );
}
