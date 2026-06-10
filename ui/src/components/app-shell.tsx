import { useEffect, useRef } from "react";
import { Outlet, useLocation, useNavigate } from "react-router";
import { LogOut } from "lucide-react";
import { AppSidebar } from "@/components/app-sidebar";
import { CommandMenu } from "@/components/command-menu";
import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { Toaster } from "@/components/ui/sonner";
import { useAuth } from "@/providers/auth-provider";

/** Per-route document titles so history and tabs read meaningfully. */
function titleFor(pathname: string): string {
  const seg = pathname.split("/").filter(Boolean);
  const name = seg[0] ?? "overview";
  const map: Record<string, string> = {
    overview: "Overview",
    buckets: seg[1] ? decodeURIComponent(seg[1]) : "Buckets",
    users: seg[1] ? "User" : "Users",
    activity: "Activity",
    replication: "Replication",
  };
  return `${map[name] ?? "Overview"} — Cairn`;
}

export function AppShell() {
  const { logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const mainRef = useRef<HTMLElement>(null);
  const firstRender = useRef(true);

  // Route-change accessibility: retitle the document and move focus to the
  // main region so screen readers announce the new page.
  useEffect(() => {
    document.title = titleFor(location.pathname);
    if (firstRender.current) {
      firstRender.current = false;
      return;
    }
    mainRef.current?.focus();
  }, [location.pathname]);

  function signOut() {
    logout();
    navigate("/login");
  }

  return (
    <SidebarProvider>
      <a
        href="#main-content"
        className="fixed top-2 left-2 z-50 -translate-y-16 rounded-md border bg-background px-3 py-2 text-sm shadow-md transition-transform focus:translate-y-0"
      >
        Skip to content
      </a>
      <AppSidebar />
      <SidebarInset>
        <header className="sticky top-0 z-10 flex h-14 shrink-0 items-center gap-2 border-b bg-background/95 px-4 backdrop-blur supports-[backdrop-filter]:bg-background/75">
          <SidebarTrigger aria-label="Toggle sidebar" />
          <Separator orientation="vertical" className="mr-1 !h-4" />
          <div className="ml-auto flex items-center gap-1.5">
            <CommandMenu />
            <ThemeToggle />
            <Button
              variant="ghost"
              size="sm"
              onClick={signOut}
              className="gap-1.5 text-muted-foreground"
            >
              <LogOut aria-hidden="true" className="size-3.5" />
              <span className="hidden sm:inline">Sign out</span>
            </Button>
          </div>
        </header>
        <main
          id="main-content"
          ref={mainRef}
          tabIndex={-1}
          className="flex-1 outline-none"
        >
          <Outlet />
        </main>
      </SidebarInset>
      <Toaster position="bottom-right" />
    </SidebarProvider>
  );
}
