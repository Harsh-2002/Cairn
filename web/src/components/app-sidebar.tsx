import { useEffect, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import {
  Activity,
  BarChart3,
  ChevronRight,
  Database,
  DownloadCloud,
  Home,
  KeyRound,
  LogOut,
  PanelLeft,
  RefreshCw,
  Search,
  Tags,
  Users,
  type LucideIcon,
} from "lucide-react";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/primitives/collapsible";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  useSidebar,
} from "@/components/primitives/sidebar";
import { Button } from "@/components/primitives/button";
import { ThemeToggle } from "@/components/theme-toggle";
import { api } from "@/lib/api";
import { useAuth } from "@/providers/auth-provider";
import { cn } from "@/lib/utils";

interface NavItem {
  label: string;
  path: string;
  icon: LucideIcon;
}

// Platform-correct hint for the command-palette shortcut.
const SHORTCUT_HINT =
  typeof navigator !== "undefined" &&
  /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent)
    ? "⌘K"
    : "Ctrl K";

// The rail is grouped so nine destinations read as three short, labelled scans
// instead of one long flat list. Buckets carries an inline accordion of the
// bucket names (below), so Tags/Replication/Import sit with it under Storage.
const NAV_GROUPS: { label: string; items: NavItem[] }[] = [
  {
    label: "Insights",
    items: [
      { label: "Overview", path: "/overview", icon: Home },
      { label: "Metrics", path: "/metrics", icon: BarChart3 },
      { label: "Activity", path: "/activity", icon: Activity },
    ],
  },
  {
    label: "Storage",
    items: [
      { label: "Buckets", path: "/buckets", icon: Database },
      { label: "Tags", path: "/tags", icon: Tags },
      { label: "Replication", path: "/replication", icon: RefreshCw },
      { label: "Import", path: "/imports", icon: DownloadCloud },
    ],
  },
  {
    label: "Access",
    items: [
      { label: "Users", path: "/users", icon: Users },
      { label: "Credentials", path: "/credentials", icon: KeyRound },
    ],
  },
];

/** Which nav section a path belongs to (bucket subroutes light up Buckets, etc.).
 * Segment-aware: a prefix only matches on a `/` boundary, so `/tags` never lights
 * up for a hypothetical `/tagsomething` and each section owns exactly its subtree. */
function isActive(navPath: string, pathname: string): boolean {
  if (navPath === "/overview") {
    return (
      pathname === "/" ||
      pathname === "/overview" ||
      pathname.startsWith("/overview/")
    );
  }
  return pathname === navPath || pathname.startsWith(navPath + "/");
}

export function AppSidebar() {
  const location = useLocation();
  const navigate = useNavigate();
  const { logout } = useAuth();
  const { isMobile, setOpenMobile, state, toggleSidebar } = useSidebar();

  function signOut() {
    logout();
    navigate("/login");
  }
  const [bucketsOpen, setBucketsOpen] = useState(false);
  const [buckets, setBuckets] = useState<string[] | null>(null);

  // Which bucket (if any) the current route is scoped to, so the matching
  // sub-link can light up alongside the parent "Buckets" section.
  const bucketMatch = location.pathname.match(/^\/buckets\/([^/]+)/);
  const activeBucket = bucketMatch ? decodeURIComponent(bucketMatch[1]) : null;

  // Bucket names load lazily the first time the section is expanded, so the
  // sidebar costs nothing until someone reaches for it.
  useEffect(() => {
    if (!bucketsOpen || buckets !== null) return;
    api
      .listBuckets()
      .then((r) => setBuckets(r.buckets.map((b) => b.name)))
      .catch(() => setBuckets([]));
  }, [bucketsOpen, buckets]);

  function closeOnMobile() {
    if (isMobile) setOpenMobile(false);
  }

  // One item's markup. Buckets is the accordion special case: the label still
  // navigates to the list view, but a chevron expands an inline, lazily-loaded
  // list of buckets without leaving the current page.
  function renderNavItem(item: NavItem) {
    const active = isActive(item.path, location.pathname);

    if (item.path === "/buckets") {
      return (
        <Collapsible
          key={item.path}
          open={bucketsOpen}
          onOpenChange={setBucketsOpen}
          asChild
        >
          <SidebarMenuItem>
            <SidebarMenuButton asChild isActive={active} tooltip={item.label}>
              <Link
                to={item.path}
                aria-current={active ? "page" : undefined}
                onClick={closeOnMobile}
              >
                <item.icon aria-hidden="true" />
                <span>{item.label}</span>
              </Link>
            </SidebarMenuButton>
            <CollapsibleTrigger asChild>
              <button
                type="button"
                aria-label={bucketsOpen ? "Collapse buckets" : "Expand buckets"}
                className="absolute top-1.5 right-1 flex aspect-square w-5 items-center justify-center rounded-md text-sidebar-foreground/70 ring-sidebar-ring outline-hidden transition-transform hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2 group-data-[collapsible=icon]:hidden"
              >
                <ChevronRight
                  aria-hidden="true"
                  className={cn(
                    "transition-transform duration-150 ease-out",
                    bucketsOpen && "rotate-90"
                  )}
                />
              </button>
            </CollapsibleTrigger>
            <CollapsibleContent>
              <SidebarMenuSub>
                {buckets === null ? (
                  <SidebarMenuSubItem>
                    <span className="flex h-7 items-center px-2 text-xs text-muted-foreground">
                      Loading…
                    </span>
                  </SidebarMenuSubItem>
                ) : buckets.length === 0 ? (
                  <SidebarMenuSubItem>
                    <span className="flex h-7 items-center px-2 text-xs text-muted-foreground">
                      No buckets
                    </span>
                  </SidebarMenuSubItem>
                ) : (
                  buckets.map((b) => {
                    const bucketActive = activeBucket === b;
                    return (
                      <SidebarMenuSubItem key={b}>
                        <SidebarMenuSubButton asChild isActive={bucketActive}>
                          <Link
                            to={`/buckets/${encodeURIComponent(b)}/browser`}
                            aria-current={bucketActive ? "page" : undefined}
                            onClick={closeOnMobile}
                          >
                            <span className="font-mono text-[13px]">{b}</span>
                          </Link>
                        </SidebarMenuSubButton>
                      </SidebarMenuSubItem>
                    );
                  })
                )}
              </SidebarMenuSub>
            </CollapsibleContent>
          </SidebarMenuItem>
        </Collapsible>
      );
    }

    return (
      <SidebarMenuItem key={item.path}>
        <SidebarMenuButton asChild isActive={active} tooltip={item.label}>
          <Link
            to={item.path}
            aria-current={active ? "page" : undefined}
            onClick={closeOnMobile}
          >
            <item.icon aria-hidden="true" />
            <span>{item.label}</span>
          </Link>
        </SidebarMenuButton>
      </SidebarMenuItem>
    );
  }

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="px-3 py-4 group-data-[collapsible=icon]:px-2">
        <div className="flex items-center justify-between gap-2">
          {/* The wordmark: a quiet filled square + name (Geist 600). The name hides in the
              collapsed icon rail, leaving the square; the collapse toggle is always reachable. */}
          <Link
            to="/overview"
            className="flex items-center gap-2 group-data-[collapsible=icon]:hidden"
          >
            <span
              aria-hidden="true"
              className="inline-block size-4 rounded-[4px] bg-foreground"
            />
            <span className="text-[15px] font-semibold tracking-tight text-foreground">
              Cairn
            </span>
          </Link>
          {/* Desktop collapse toggle (mobile uses the SidebarTrigger in the top bar). Drives the
              framework's own open/close + cookie persistence — no parallel state. */}
          <Button
            variant="ghost"
            size="icon-sm"
            onClick={toggleSidebar}
            aria-label={state === "collapsed" ? "Expand sidebar" : "Collapse sidebar"}
            aria-expanded={state === "expanded"}
            className="hidden shrink-0 md:flex"
          >
            <PanelLeft aria-hidden="true" />
          </Button>
        </div>
      </SidebarHeader>
      <SidebarContent>
        {/* Visible entry to the Cmd/Ctrl+K palette, so the shortcut isn't the only way to find it.
            Collapses to just the search icon in the icon rail. */}
        <SidebarGroup className="pb-0">
          <SidebarGroupContent>
            <button
              type="button"
              onClick={() =>
                window.dispatchEvent(new Event("cairn:command-palette"))
              }
              aria-label="Open command palette (search)"
              className="flex h-8 w-full items-center gap-2 rounded-md border border-sidebar-border bg-background px-2 text-sm text-muted-foreground outline-none transition-colors hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2 focus-visible:ring-sidebar-ring group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-0"
            >
              <Search aria-hidden="true" className="size-4 shrink-0" />
              <span className="group-data-[collapsible=icon]:hidden">
                Search…
              </span>
              <kbd className="ms-auto hidden rounded border bg-muted px-1.5 font-mono text-[11px] text-muted-foreground md:inline group-data-[collapsible=icon]:!hidden">
                {SHORTCUT_HINT}
              </kbd>
            </button>
          </SidebarGroupContent>
        </SidebarGroup>
        {NAV_GROUPS.map((group) => (
          <SidebarGroup key={group.label}>
            {/* The group label is hidden in the collapsed icon rail (there's no room), where the
                per-item tooltips carry the names instead. */}
            <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
            <SidebarGroupContent role="navigation" aria-label={group.label}>
              <SidebarMenu className="stagger-children">
                {group.items.map((item) => renderNavItem(item))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>
      <SidebarFooter className="px-3 py-3 group-data-[collapsible=icon]:px-2">
        {/* Account + appearance controls live at the foot of the rail (webapp shell), not a header.
            The version lives on Overview, so it isn't repeated here. In the collapsed icon rail the
            controls stack as icons and the "Sign out" label is dropped. */}
        <div className="flex items-center gap-1 group-data-[collapsible=icon]:flex-col group-data-[collapsible=icon]:gap-1.5">
          <Button
            variant="ghost"
            onClick={signOut}
            aria-label="Sign out"
            className="h-8 flex-1 justify-start gap-2 px-2 text-sm font-normal text-muted-foreground hover:text-foreground group-data-[collapsible=icon]:size-8 group-data-[collapsible=icon]:flex-none group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-0"
          >
            <LogOut aria-hidden="true" className="size-4 shrink-0" />
            <span className="group-data-[collapsible=icon]:hidden">Sign out</span>
          </Button>
          <ThemeToggle />
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
