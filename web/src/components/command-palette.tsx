// A lightweight command palette: Cmd/Ctrl+K opens a fuzzy launcher that jumps to any page or bucket
// and fires the common actions, so an operator who lives in the console never has to reach for the
// mouse to navigate. Built on the raw Radix Dialog primitive (focus trap, Escape, focus-return for
// free) rather than a new dependency, matching the console's hand-written component convention.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router";
import {
  Activity,
  BarChart3,
  CornerDownLeft,
  Database,
  DownloadCloud,
  Home,
  KeyRound,
  LogOut,
  Plus,
  RefreshCw,
  Search,
  Tags,
  Users,
  type LucideIcon,
} from "lucide-react";
import { Dialog as DialogPrimitive } from "radix-ui";
import { api } from "@/lib/api";
import { useAuth } from "@/providers/auth-provider";
import { cn } from "@/lib/utils";

type Group = "Pages" | "Buckets" | "Actions";

interface Command {
  id: string;
  label: string;
  group: Group;
  icon: LucideIcon;
  /** Extra text folded into the search haystack (never shown). */
  keywords?: string;
  run: () => void;
}

const PAGES: { label: string; path: string; icon: LucideIcon; keywords?: string }[] = [
  { label: "Overview", path: "/overview", icon: Home, keywords: "home dashboard storage" },
  { label: "Metrics", path: "/metrics", icon: BarChart3, keywords: "charts requests" },
  { label: "Activity", path: "/activity", icon: Activity, keywords: "audit log history" },
  { label: "Buckets", path: "/buckets", icon: Database, keywords: "storage objects" },
  { label: "Tags", path: "/tags", icon: Tags },
  { label: "Replication", path: "/replication", icon: RefreshCw, keywords: "mirror" },
  { label: "Import", path: "/imports", icon: DownloadCloud, keywords: "migrate" },
  { label: "Users", path: "/users", icon: Users, keywords: "access keys iam" },
  { label: "Credentials", path: "/credentials", icon: KeyRound, keywords: "secret token" },
];

/** Order groups render in; also the flat order the keyboard cursor walks. */
const GROUP_ORDER: Group[] = ["Pages", "Buckets", "Actions"];

export function CommandPalette() {
  const navigate = useNavigate();
  const { logout } = useAuth();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const [buckets, setBuckets] = useState<string[] | null>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Global toggle. Cmd+K (mac) / Ctrl+K (win/linux); preventDefault so it never falls through to the
  // browser's own address-bar shortcut.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && (e.key === "k" || e.key === "K")) {
        e.preventDefault();
        setOpen((v) => !v);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Any visible affordance (the sidebar "Search…" button) opens the palette by dispatching this
  // event, so the shortcut isn't the only way in.
  useEffect(() => {
    function openIt() {
      setOpen(true);
    }
    window.addEventListener("cairn:command-palette", openIt);
    return () => window.removeEventListener("cairn:command-palette", openIt);
  }, []);

  // Bucket names load lazily the first time the palette opens, then are reused. Reset the query and
  // cursor on every open so it always starts clean.
  useEffect(() => {
    if (!open) return;
    setQuery("");
    setActive(0);
    if (buckets === null) {
      api
        .listBuckets()
        .then((r) => setBuckets(r.buckets.map((b) => b.name)))
        .catch(() => setBuckets([]));
    }
  }, [open, buckets]);

  const go = useCallback(
    (fn: () => void) => {
      setOpen(false);
      fn();
    },
    [],
  );

  const commands = useMemo<Command[]>(() => {
    const pages: Command[] = PAGES.map((p) => ({
      id: `page:${p.path}`,
      label: p.label,
      group: "Pages",
      icon: p.icon,
      keywords: p.keywords,
      run: () => navigate(p.path),
    }));
    const bucketCmds: Command[] = (buckets ?? []).map((b) => ({
      id: `bucket:${b}`,
      label: b,
      group: "Buckets",
      icon: Database,
      keywords: "bucket browse objects",
      run: () => navigate(`/buckets/${encodeURIComponent(b)}/browser`),
    }));
    const actions: Command[] = [
      {
        id: "action:new-bucket",
        label: "Create bucket",
        group: "Actions",
        icon: Plus,
        keywords: "new add bucket",
        run: () => navigate("/buckets?new=1"),
      },
      {
        id: "action:sign-out",
        label: "Sign out",
        group: "Actions",
        icon: LogOut,
        keywords: "logout exit",
        run: () => {
          logout();
          navigate("/login");
        },
      },
    ];
    return [...pages, ...bucketCmds, ...actions];
  }, [buckets, navigate, logout]);

  // Filter: every whitespace-separated token must appear in the item's (label + keywords + group)
  // haystack. Simple, predictable substring matching — no ranking surprises.
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return commands;
    const tokens = q.split(/\s+/);
    return commands.filter((c) => {
      const hay = `${c.label} ${c.keywords ?? ""} ${c.group}`.toLowerCase();
      return tokens.every((t) => hay.includes(t));
    });
  }, [query, commands]);

  // Keep the cursor in range as the result set shrinks.
  useEffect(() => {
    setActive((a) => (filtered.length === 0 ? 0 : Math.min(a, filtered.length - 1)));
  }, [filtered.length]);

  // Scroll the active row into view as the cursor moves.
  useEffect(() => {
    const el = listRef.current?.querySelector<HTMLElement>('[data-active="true"]');
    el?.scrollIntoView({ block: "nearest" });
  }, [active]);

  function onInputKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((a) => (filtered.length ? (a + 1) % filtered.length : 0));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((a) => (filtered.length ? (a - 1 + filtered.length) % filtered.length : 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      const cmd = filtered[active];
      if (cmd) go(cmd.run);
    }
  }

  const listboxId = "command-palette-listbox";
  const activeId = filtered[active]?.id;

  return (
    <DialogPrimitive.Root open={open} onOpenChange={setOpen}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="fixed inset-0 z-50 bg-black/50 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:animate-in data-[state=open]:fade-in-0" />
        <DialogPrimitive.Content
          className="fixed top-[12vh] left-[50%] z-50 w-full max-w-[calc(100%-2rem)] translate-x-[-50%] overflow-hidden rounded-xl border bg-popover text-popover-foreground shadow-lg outline-none duration-150 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95 data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95 sm:max-w-lg"
          aria-label="Command palette"
        >
          <DialogPrimitive.Title className="sr-only">
            Command palette
          </DialogPrimitive.Title>
          <DialogPrimitive.Description className="sr-only">
            Search to jump to a page or bucket, or run an action. Use the arrow keys and Enter.
          </DialogPrimitive.Description>

          <div className="flex items-center gap-2 border-b px-3">
            <Search
              aria-hidden="true"
              className="size-4 shrink-0 text-muted-foreground"
            />
            {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
            <input
              autoFocus
              type="text"
              role="combobox"
              aria-expanded="true"
              aria-controls={listboxId}
              aria-activedescendant={activeId}
              aria-autocomplete="list"
              placeholder="Jump to a page or bucket, or run an action…"
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setActive(0);
              }}
              onKeyDown={onInputKeyDown}
              className="h-11 w-full bg-transparent text-sm outline-none placeholder:text-muted-foreground"
              autoComplete="off"
              autoCorrect="off"
              spellCheck={false}
            />
          </div>

          <div
            id={listboxId}
            ref={listRef}
            role="listbox"
            aria-label="Results"
            className="max-h-[60vh] overflow-y-auto p-1.5"
          >
            {filtered.length === 0 ? (
              <p className="px-3 py-6 text-center text-sm text-muted-foreground">
                No matches for “{query.trim()}”.
              </p>
            ) : (
              GROUP_ORDER.map((group) => {
                const items = filtered.filter((c) => c.group === group);
                if (items.length === 0) return null;
                return (
                  <div key={group} className="mb-1 last:mb-0">
                    <p className="px-2 py-1.5 text-xs font-medium text-muted-foreground">
                      {group}
                    </p>
                    {items.map((c) => {
                      const idx = filtered.indexOf(c);
                      const isActive = idx === active;
                      return (
                        <button
                          key={c.id}
                          id={c.id}
                          type="button"
                          role="option"
                          aria-selected={isActive}
                          data-active={isActive}
                          // Pointer hover moves the cursor so mouse and keyboard never disagree.
                          onMouseMove={() => setActive(idx)}
                          onClick={() => go(c.run)}
                          className={cn(
                            "flex w-full items-center gap-2.5 rounded-md px-2 py-2 text-left text-sm outline-none",
                            isActive
                              ? "bg-accent text-accent-foreground"
                              : "text-foreground",
                          )}
                        >
                          <c.icon
                            aria-hidden="true"
                            className="size-4 shrink-0 text-muted-foreground"
                          />
                          <span
                            className={cn(
                              "min-w-0 flex-1 truncate",
                              c.group === "Buckets" && "font-mono text-[13px]",
                            )}
                          >
                            {c.label}
                          </span>
                          {isActive ? (
                            <CornerDownLeft
                              aria-hidden="true"
                              className="size-3.5 shrink-0 text-muted-foreground"
                            />
                          ) : null}
                        </button>
                      );
                    })}
                  </div>
                );
              })
            )}
          </div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}
