// Tags: a node-wide, master-detail view of every object tag in use. The left rail lists each
// distinct key=value tag with how many objects carry it; picking one fills the right pane with the
// matching objects, each linking into its bucket's browser. The two regions share one bordered
// frame split by a hairline (no nested cards) and fill the column height like a file navigator.

import { useMemo, useState } from "react";
import { Search, Tags as TagsIcon } from "lucide-react";
import { api } from "@/lib/api";
import { bytes, count, whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { TextLink } from "@/components/text-link";
import { Input } from "@/components/primitives/input";
import { Skeleton } from "@/components/primitives/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/primitives/table";
import type { TagSummaryItem } from "@/lib/types";
import { cn } from "@/lib/utils";

/** A monospace key=value chip — the single visual identity for a tag, used in headers/empties. */
function TagChip({ tagKey, value }: { tagKey: string; value: string }) {
  return (
    <span className="inline-flex max-w-full items-baseline gap-1 rounded-md border bg-muted px-1.5 py-0.5 font-mono text-[13px]">
      <span className="truncate">{tagKey}</span>
      <span aria-hidden="true" className="text-muted-foreground">
        =
      </span>
      <span className="truncate text-muted-foreground">{value}</span>
    </span>
  );
}

export function Tags() {
  const tags = useResource(() => api.listTags(), []);
  // Live: the server pulses "tags" on its cadence; re-fetch through the normal authenticated path.
  useLiveTopic("tags", tags.refresh);
  const [sel, setSel] = useState<TagSummaryItem | null>(null);
  const [filter, setFilter] = useState("");

  const list = tags.data?.tags ?? [];
  const shown = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return list;
    return list.filter((t) =>
      `${t.tag_key}=${t.tag_value}`.toLowerCase().includes(q),
    );
  }, [list, filter]);

  const total = list.length;

  return (
    <Page>
      <PageHeader
        title="Tags"
        description={
          tags.data
            ? total === 0
              ? "No object tags in use yet."
              : `${count(total)} distinct ${total === 1 ? "tag" : "tags"} in use across your buckets.`
            : "Every object tag in use across your buckets."
        }
      />

      {tags.error ? (
        <ErrorAlert
          title="Could not load tags"
          message={tags.error}
          onRetry={tags.refresh}
        />
      ) : tags.data && total === 0 ? (
        <EmptyState
          icon={TagsIcon}
          title="No tags yet"
          body="Tag an object from its actions menu in the bucket browser to see it grouped here."
        />
      ) : (
        // One bordered frame, split by a single hairline: tag rail | objects detail. Fills the
        // column height on desktop so each region scrolls on its own; stacks on mobile.
        <div className="overflow-hidden rounded-lg border md:grid md:h-[calc(100dvh-9.5rem)] md:min-h-[26rem] md:grid-cols-[clamp(15rem,24vw,20rem)_1fr]">
          {/* ---- Master: the tag rail ----------------------------------------- */}
          <aside className="flex min-h-0 flex-col border-b md:border-r md:border-b-0">
            <div className="border-b p-2">
              <div className="relative">
                <Search
                  aria-hidden="true"
                  className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground"
                />
                <label className="sr-only" htmlFor="tag-filter">
                  Filter tags
                </label>
                <Input
                  id="tag-filter"
                  value={filter}
                  placeholder="Filter tags…"
                  autoComplete="off"
                  spellCheck={false}
                  className="h-9 pl-8 font-mono text-[13px]"
                  onChange={(e) => setFilter(e.target.value)}
                />
              </div>
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto p-1.5 max-md:max-h-[50vh]">
              {tags.loading ? (
                <div className="space-y-1.5 p-1">
                  {Array.from({ length: 6 }, (_, i) => (
                    <Skeleton key={i} className="h-9 rounded-md" />
                  ))}
                </div>
              ) : shown.length === 0 ? (
                <p className="px-2 py-8 text-center text-[13px] text-muted-foreground">
                  No tags match “{filter}”.
                </p>
              ) : (
                <ul>
                  {shown.map((t) => {
                    const active =
                      sel?.tag_key === t.tag_key &&
                      sel?.tag_value === t.tag_value;
                    return (
                      <li key={`${t.tag_key}=${t.tag_value}`}>
                        <button
                          type="button"
                          onClick={() => setSel(t)}
                          aria-current={active || undefined}
                          className={cn(
                            "flex w-full items-center gap-2 rounded-md px-2.5 py-2 text-left transition-colors",
                            active
                              ? "bg-accent text-accent-foreground"
                              : "hover:bg-accent/60",
                          )}
                        >
                          <span className="min-w-0 flex-1 truncate font-mono text-[13px]">
                            <span className="font-medium text-foreground">
                              {t.tag_key}
                            </span>
                            <span className="text-muted-foreground">=</span>
                            <span className="text-muted-foreground">
                              {t.tag_value}
                            </span>
                          </span>
                          <span
                            className={cn(
                              "shrink-0 rounded-full px-2 py-0.5 text-[12px] tabular-nums",
                              active
                                ? "bg-background/70 text-foreground"
                                : "bg-muted text-muted-foreground",
                            )}
                          >
                            {count(t.object_count)}
                          </span>
                        </button>
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          </aside>

          {/* ---- Detail: objects carrying the selected tag -------------------- */}
          <section className="flex min-h-0 min-w-0 flex-col">
            {sel ? (
              <TagObjects key={`${sel.tag_key}=${sel.tag_value}`} sel={sel} />
            ) : (
              <div className="flex flex-1 flex-col items-center justify-center gap-2 p-10 text-center">
                <TagsIcon
                  aria-hidden="true"
                  className="size-7 text-muted-foreground/60"
                />
                <p className="text-sm font-medium">Select a tag</p>
                <p className="max-w-xs text-[13px] leading-relaxed text-muted-foreground">
                  Pick a tag on the left to see every object that carries it.
                </p>
              </div>
            )}
          </section>
        </div>
      )}
    </Page>
  );
}

/** The detail pane: the objects carrying the selected tag. Borderless table inside the shared frame. */
function TagObjects({ sel }: { sel: TagSummaryItem }) {
  const objects = useResource(
    () => api.listTagObjects(sel.tag_key, sel.tag_value),
    [sel.tag_key, sel.tag_value],
  );

  const rows = objects.data?.objects ?? [];

  return (
    <>
      <header className="flex flex-wrap items-center gap-x-2 gap-y-1 border-b p-3">
        <span className="text-sm font-medium">Objects tagged</span>
        <TagChip tagKey={sel.tag_key} value={sel.tag_value} />
        {objects.data ? (
          <span className="ms-auto text-[13px] text-muted-foreground tabular-nums">
            {count(rows.length)} {rows.length === 1 ? "object" : "objects"}
          </span>
        ) : null}
      </header>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {objects.error ? (
          <div className="p-4">
            <ErrorAlert
              title="Could not load objects"
              message={objects.error}
              onRetry={objects.refresh}
            />
          </div>
        ) : objects.loading ? (
          <div className="space-y-2 p-4">
            {Array.from({ length: 5 }, (_, i) => (
              <Skeleton key={i} className="h-8 rounded-md" />
            ))}
          </div>
        ) : rows.length === 0 ? (
          <p className="p-10 text-center text-[13px] text-muted-foreground">
            No current objects carry this tag.
          </p>
        ) : (
          // table-stack reuses the responsive card stacking from the list views on mobile.
          <div className="table-stack max-md:p-3">
            <Table>
              <TableHeader className="sticky top-0 z-10 bg-background">
                <TableRow>
                  <TableHead>Object</TableHead>
                  <TableHead>Bucket</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                  <TableHead>Modified</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((o) => (
                  <TableRow key={`${o.bucket}/${o.key}@${o.version_id}`}>
                    <TableCell className="font-mono text-[13px]">
                      <TextLink
                        to={`/buckets/${encodeURIComponent(o.bucket)}/browser`}
                        className="block max-w-[40ch] truncate"
                        title={o.key}
                      >
                        {o.key}
                      </TextLink>
                    </TableCell>
                    <TableCell
                      data-label="Bucket"
                      className="font-mono text-[13px]"
                    >
                      <TextLink
                        to={`/buckets/${encodeURIComponent(o.bucket)}/browser`}
                      >
                        {o.bucket}
                      </TextLink>
                    </TableCell>
                    <TableCell
                      data-label="Size"
                      className="text-right text-[13px] tabular-nums"
                    >
                      {bytes(o.size)}
                    </TableCell>
                    <TableCell
                      data-label="Modified"
                      className="text-[13px] text-muted-foreground tabular-nums"
                    >
                      {whenMs(o.last_modified_ms)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </div>
    </>
  );
}
