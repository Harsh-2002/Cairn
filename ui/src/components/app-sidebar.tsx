import { useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router";
import { Activity, Database, Home, RefreshCw, Users } from "lucide-react";
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
  useSidebar,
} from "@/components/ui/sidebar";
import { api } from "@/lib/api";

const NAV = [
  { label: "Overview", path: "/overview", icon: Home },
  { label: "Buckets", path: "/buckets", icon: Database },
  { label: "Users", path: "/users", icon: Users },
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
  const { isMobile, setOpenMobile } = useSidebar();
  const [version, setVersion] = useState<string | null>(null);

  useEffect(() => {
    api
      .system()
      .then((s) => setVersion(s.version))
      .catch(() => setVersion(null));
  }, []);

  return (
    <Sidebar>
      <SidebarHeader className="px-4 py-4">
        <NavLink to="/overview" className="flex items-center gap-2">
          {/* The wordmark: a quiet filled square + name, Geist 600. */}
          <span
            aria-hidden="true"
            className="inline-block size-4 rounded-[4px] bg-foreground"
          />
          <span className="text-[15px] font-semibold tracking-tight text-foreground">
            Cairn
          </span>
        </NavLink>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV.map((item) => (
                <SidebarMenuItem key={item.path}>
                  <SidebarMenuButton
                    asChild
                    isActive={isActive(item.path, location.pathname)}
                  >
                    <NavLink
                      to={item.path}
                      onClick={() => {
                        if (isMobile) setOpenMobile(false);
                      }}
                    >
                      <item.icon aria-hidden="true" />
                      <span>{item.label}</span>
                    </NavLink>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="px-4 py-3">
        <p className="text-xs text-muted-foreground">
          {version ? `Cairn v${version}` : "Cairn"}
        </p>
      </SidebarFooter>
    </Sidebar>
  );
}
