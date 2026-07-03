import { useEffect, useRef, useState } from "react";
import { Link, Outlet, useLocation } from "react-router";
import { AppSidebar } from "@/components/app-sidebar";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { Toaster } from "@/components/ui/sonner";

/** The sidebar's persisted open/closed state (the framework stores it in the `sidebar_state`
 * cookie). Seeding `defaultOpen` from it on first paint avoids an expanded→collapsed flash for a
 * user who left the rail collapsed. Defaults to open when the cookie is absent. */
function sidebarDefaultOpen(): boolean {
  if (typeof document === "undefined") return true;
  const m = document.cookie.match(/(?:^|;\s*)sidebar_state=([^;]+)/);
  return m ? m[1] === "true" : true;
}

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
    imports: "Import",
  };
  return `${map[name] ?? "Overview"} — Cairn`;
}

export function AppShell() {
  const location = useLocation();
  const mainRef = useRef<HTMLElement>(null);
  const firstRender = useRef(true);
  const [announce, setAnnounce] = useState("");

  // Route-change accessibility: retitle the document, move focus to the main
  // region, and announce the new page through a polite live region so screen
  // readers always hear where they landed.
  useEffect(() => {
    const title = titleFor(location.pathname);
    document.title = title;
    if (firstRender.current) {
      firstRender.current = false;
      return;
    }
    mainRef.current?.focus();
    setAnnounce(title.replace(/ — Cairn$/, ""));
  }, [location.pathname]);

  return (
    <SidebarProvider defaultOpen={sidebarDefaultOpen()}>
      <a
        href="#main-content"
        className="fixed top-2 left-2 z-50 -translate-y-16 rounded-md border bg-background px-3 py-2 text-sm shadow-md transition-transform focus:translate-y-0"
      >
        Skip to content
      </a>
      <AppSidebar />
      <SidebarInset>
        {/* Mobile-only top bar: opens the off-canvas rail and shows the wordmark. On desktop the
            rail is always present, so there is no top chrome — content fills the height (app shell). */}
        <header className="sticky top-0 z-10 flex h-14 shrink-0 items-center gap-2 border-b bg-background/95 px-4 backdrop-blur md:hidden supports-[backdrop-filter]:bg-background/75">
          <SidebarTrigger aria-label="Open navigation" />
          <Link to="/overview" className="flex items-center gap-2">
            <span
              aria-hidden="true"
              className="inline-block size-4 rounded-[4px] bg-foreground"
            />
            <span className="text-[15px] font-semibold tracking-tight">
              Cairn
            </span>
          </Link>
        </header>
        <main
          id="main-content"
          ref={mainRef}
          tabIndex={-1}
          className="flex-1 outline-none"
        >
          {/* A calm fade+rise when moving between top-level sections. Keyed by the first path
              segment so in-page tab switches (bucket Browser/Settings) don't re-animate. */}
          <div key={location.pathname.split("/")[1] || "overview"} className="animate-enter">
            <Outlet />
          </div>
        </main>
      </SidebarInset>
      <Toaster position="bottom-right" />
      {/* Polite live region: announces the destination on every client-side navigation. */}
      <div role="status" aria-live="polite" className="sr-only">
        {announce}
      </div>
    </SidebarProvider>
  );
}
