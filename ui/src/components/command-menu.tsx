import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import {
  Activity,
  BarChart3,
  Database,
  Home,
  Moon,
  RefreshCw,
  Search,
  Sun,
  SunMoon,
  Tags,
  Users,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import { api } from "@/lib/api";
import { useTheme } from "@/providers/theme-provider";

/**
 * The ⌘K palette: jump to any view or straight into a bucket, switch themes.
 * Bucket names load lazily the first time the palette opens.
 */
export function CommandMenu() {
  const navigate = useNavigate();
  const { setTheme } = useTheme();
  const [open, setOpen] = useState(false);
  const [buckets, setBuckets] = useState<string[] | null>(null);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((o) => !o);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (!open || buckets !== null) return;
    api
      .listBuckets()
      .then((r) => setBuckets(r.buckets.map((b) => b.name)))
      .catch(() => setBuckets([]));
  }, [open, buckets]);

  function go(path: string) {
    setOpen(false);
    navigate(path);
  }

  return (
    <>
      <Button
        variant="outline"
        onClick={() => setOpen(true)}
        aria-label="Search"
        className="h-9 justify-start gap-2 px-3 font-normal text-muted-foreground hover:text-foreground sm:w-60"
      >
        <Search aria-hidden="true" className="size-4 shrink-0" />
        <span className="hidden sm:inline">Search…</span>
        <kbd className="pointer-events-none ml-auto hidden h-5 items-center rounded border bg-muted px-1.5 font-mono text-[11px] font-medium sm:inline-flex">
          ⌘K
        </kbd>
      </Button>
      <CommandDialog
        open={open}
        onOpenChange={setOpen}
        title="Command menu"
        description="Jump to a view or bucket"
      >
        <CommandInput placeholder="Where to?" />
        <CommandList>
          <CommandEmpty>Nothing matches.</CommandEmpty>
          <CommandGroup heading="Views">
            <CommandItem onSelect={() => go("/overview")}>
              <Home aria-hidden="true" /> Overview
            </CommandItem>
            <CommandItem onSelect={() => go("/metrics")}>
              <BarChart3 aria-hidden="true" /> Metrics
            </CommandItem>
            <CommandItem onSelect={() => go("/buckets")}>
              <Database aria-hidden="true" /> Buckets
            </CommandItem>
            <CommandItem onSelect={() => go("/tags")}>
              <Tags aria-hidden="true" /> Tags
            </CommandItem>
            <CommandItem onSelect={() => go("/users")}>
              <Users aria-hidden="true" /> Users
            </CommandItem>
            <CommandItem onSelect={() => go("/activity")}>
              <Activity aria-hidden="true" /> Activity
            </CommandItem>
            <CommandItem onSelect={() => go("/replication")}>
              <RefreshCw aria-hidden="true" /> Replication
            </CommandItem>
          </CommandGroup>
          {buckets && buckets.length > 0 ? (
            <>
              <CommandSeparator />
              <CommandGroup heading="Buckets">
                {buckets.map((b) => (
                  <CommandItem
                    key={b}
                    value={`bucket ${b}`}
                    onSelect={() => go(`/buckets/${encodeURIComponent(b)}/browser`)}
                  >
                    <Database aria-hidden="true" />
                    <span className="font-mono text-[13px]">{b}</span>
                  </CommandItem>
                ))}
              </CommandGroup>
            </>
          ) : null}
          <CommandSeparator />
          <CommandGroup heading="Theme">
            <CommandItem onSelect={() => { setTheme("light"); setOpen(false); }}>
              <Sun aria-hidden="true" /> Light
            </CommandItem>
            <CommandItem onSelect={() => { setTheme("dark"); setOpen(false); }}>
              <Moon aria-hidden="true" /> Dark
            </CommandItem>
            <CommandItem onSelect={() => { setTheme("system"); setOpen(false); }}>
              <SunMoon aria-hidden="true" /> System
            </CommandItem>
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </>
  );
}
