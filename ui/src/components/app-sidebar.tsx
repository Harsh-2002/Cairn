import { useEffect, useState } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import {
  Activity,
  BarChart3,
  ChevronRight,
  Database,
  Home,
  KeyRound,
  LogOut,
  RefreshCw,
  Tags,
  Users,
} from "lucide-react";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { Button } from "@/components/ui/button";
import { CommandMenu } from "@/components/command-menu";
import { ThemeToggle } from "@/components/theme-toggle";
import { api } from "@/lib/api";
import { useAuth } from "@/providers/auth-provider";
import { cn } from "@/lib/utils";

const NAV = [
  { label: "Overview", path: "/overview", icon: Home },
  { label: "Metrics", path: "/metrics", icon: BarChart3 },
  { label: "Buckets", path: "/buckets", icon: Database },
  { label: "Tags", path: "/tags", icon: Tags },
  { label: "Users", path: "/users", icon: Users },
  { label: "Credentials", path: "/credentials", icon: KeyRound },
  { label: "Activity", path: "/activity", icon: Activity },
  { label: "Replication", path: "/replication", icon: RefreshCw },
];

/** Which nav section a path belongs to (bucket subroutes light up Buckets, etc.). */
function isActive(navPath: string, pathname: string): boolean {
  if (navPath === "/overview") return pathname === "/" || pathname.startsWith("/overview");
  return pathname.startsWith(navPath) || (navPath === "/users" && pathname.startsWith("/users"));
}

export function AppSidebar() {
  const location = useLocation();
  const navigate = useNavigate();
  const { logout } = useAuth();
  const { isMobile, setOpenMobile } = useSidebar();

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

  // Bucket names load lazily the first time the section is expanded (mirrors
  // the ⌘K palette), so the sidebar costs nothing until someone reaches for it.
  useEffect(() => {
    if (!bucketsOpen || buckets !== null) return;
    api
      .listBuckets()
      .then((r) => setBuckets(r.buckets.map((b) => b.name)))
      .catch(() => setBuckets([]));
  }, [bucketsOpen, buckets]);

  return (
    <Sidebar>
      <SidebarHeader className="gap-3 px-4 py-4">
        <Link to="/overview" className="flex items-center gap-2">
          {/* The wordmark: a quiet filled square + name, Geist 600. */}
          <span
            aria-hidden="true"
            className="inline-block size-4 rounded-[4px] bg-foreground"
          />
          <span className="text-[15px] font-semibold tracking-tight text-foreground">
            Cairn
          </span>
        </Link>
        {/* Search lives in the app rail, not a top chrome bar — the ⌘K palette opens from here. */}
        <CommandMenu />
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent role="navigation" aria-label="Main">
            <SidebarMenu className="stagger-children">
              {NAV.map((item) => {
                // One source of truth for "current section": both the visual
                // highlight and the announced aria-current come from the same
                // helper, so they can never disagree (plain Link, not NavLink,
                // whose own exact-match aria-current would override ours).
                const active = isActive(item.path, location.pathname);

                // Buckets is an accordion: the label still navigates to the
                // list view, but a chevron expands an inline, lazily-loaded
                // list of buckets without leaving the current page.
                if (item.path === "/buckets") {
                  return (
                    <Collapsible
                      key={item.path}
                      open={bucketsOpen}
                      onOpenChange={setBucketsOpen}
                      asChild
                    >
                      <SidebarMenuItem>
                        <SidebarMenuButton asChild isActive={active}>
                          <Link
                            to={item.path}
                            aria-current={active ? "page" : undefined}
                            onClick={() => {
                              if (isMobile) setOpenMobile(false);
                            }}
                          >
                            <item.icon aria-hidden="true" />
                            <span>{item.label}</span>
                          </Link>
                        </SidebarMenuButton>
                        <CollapsibleTrigger asChild>
                          <button
                            type="button"
                            aria-label={
                              bucketsOpen ? "Collapse buckets" : "Expand buckets"
                            }
                            className="absolute top-1.5 right-1 flex aspect-square w-5 items-center justify-center rounded-md text-sidebar-foreground/70 ring-sidebar-ring outline-hidden transition-transform hover:bg-sidebar-accent hover:text-sidebar-accent-foreground focus-visible:ring-2"
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
                                    <SidebarMenuSubButton
                                      asChild
                                      isActive={bucketActive}
                                    >
                                      <Link
                                        to={`/buckets/${encodeURIComponent(b)}/browser`}
                                        aria-current={
                                          bucketActive ? "page" : undefined
                                        }
                                        onClick={() => {
                                          if (isMobile) setOpenMobile(false);
                                        }}
                                      >
                                        <span className="font-mono text-[13px]">
                                          {b}
                                        </span>
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
                    <SidebarMenuButton asChild isActive={active}>
                      <Link
                        to={item.path}
                        aria-current={active ? "page" : undefined}
                        onClick={() => {
                          if (isMobile) setOpenMobile(false);
                        }}
                      >
                        <item.icon aria-hidden="true" />
                        <span>{item.label}</span>
                      </Link>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                );
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="px-3 py-3">
        {/* Account + appearance controls live at the foot of the rail (webapp shell), not a header.
            The version lives on Overview, so it isn't repeated here. */}
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            onClick={signOut}
            className="h-8 flex-1 justify-start gap-2 px-2 text-sm font-normal text-muted-foreground hover:text-foreground"
          >
            <LogOut aria-hidden="true" className="size-4" />
            Sign out
          </Button>
          <ThemeToggle />
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
