// Tags: a node-wide, master-detail view of every object tag in use. The left
// pane lists each distinct key=value tag and how many objects carry it; picking
// one loads the matching objects into the right pane, each linking into its
// bucket's browser.

import { useState } from "react";
import { Link } from "react-router";
import { Tags as TagsIcon } from "lucide-react";
import { api } from "@/lib/api";
import { bytes, count, whenMs } from "@/lib/format";
import { useResource } from "@/lib/use-resource";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { RefreshButton } from "@/components/refresh-button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { TableCell, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import type { TagSummaryItem } from "@/lib/types";

const TAG_COLUMNS: Column[] = [
  { key: "tag", label: "Tag" },
  { key: "objects", label: "Objects", className: "text-right" },
];

const OBJECT_COLUMNS: Column[] = [
  { key: "bucket", label: "Bucket" },
  { key: "object", label: "Object" },
  { key: "size", label: "Size", className: "text-right" },
  { key: "modified", label: "Modified" },
];

/** A monospace key=value chip, the one visual identity for a tag. */
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
  const [sel, setSel] = useState<TagSummaryItem | null>(null);

  const list = tags.data?.tags ?? [];

  return (
    <Page>
      <PageHeader
        title="Tags"
        description="Every object tag in use across your buckets."
        actions={
          <RefreshButton
            loading={tags.loading}
            refreshing={tags.refreshing}
            onClick={tags.refresh}
          />
        }
      />

      {tags.error ? (
        <ErrorAlert
          title="Could not load tags"
          message={tags.error}
          onRetry={tags.refresh}
        />
      ) : null}

      {/* Two-column split on desktop (tag list left, objects right); stacks on
          mobile. */}
      <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
        {/* ---- Master: the tag list ---------------------------------------- */}
        <Card className="gap-4">
          <CardHeader className="gap-1">
            <CardTitle>Tags</CardTitle>
            <CardDescription>
              {list.length > 0
                ? `${count(list.length)} distinct ${
                    list.length === 1 ? "tag" : "tags"
                  } in use.`
                : "Distinct key=value tags in use."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {tags.loading ? (
              <DataTable columns={TAG_COLUMNS}>
                <SkeletonRows rows={6} widths={["w-40", "w-10"]} />
              </DataTable>
            ) : list.length === 0 && !tags.error ? (
              <EmptyState
                icon={TagsIcon}
                title="No tags yet"
                body="Tag an object from its actions menu to see it here."
              />
            ) : (
              <DataTable columns={TAG_COLUMNS}>
                {list.map((t) => {
                  const active =
                    sel?.tag_key === t.tag_key && sel?.tag_value === t.tag_value;
                  return (
                    <TableRow
                      key={`${t.tag_key}=${t.tag_value}`}
                      onClick={() => setSel(t)}
                      data-state={active ? "selected" : undefined}
                      className={cn(
                        "cursor-pointer",
                        active && "bg-muted/60",
                      )}
                    >
                      <TableCell>
                        <TagChip tagKey={t.tag_key} value={t.tag_value} />
                      </TableCell>
                      <TableCell className="text-right text-[13px] tabular-nums">
                        {count(t.object_count)}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </DataTable>
            )}
          </CardContent>
        </Card>

        {/* ---- Detail: objects carrying the selected tag ------------------- */}
        <TagObjects sel={sel} />
      </div>
    </Page>
  );
}

const OBJECT_SKELETON_WIDTHS = ["w-24", "w-48", "w-16", "w-32"];

/** The detail pane: the objects carrying the selected tag, or a placeholder. */
function TagObjects({ sel }: { sel: TagSummaryItem | null }) {
  const objects = useResource(
    () =>
      sel
        ? api.listTagObjects(sel.tag_key, sel.tag_value)
        : Promise.resolve({ objects: [] }),
    [sel],
  );

  const rows = objects.data?.objects ?? [];

  return (
    <Card className="gap-4">
      <CardHeader className="gap-1">
        <CardTitle>Objects</CardTitle>
        <CardDescription>
          {sel ? (
            <span className="inline-flex items-baseline gap-1.5">
              Tagged
              <span className="font-mono text-[13px]">
                {sel.tag_key}={sel.tag_value}
              </span>
            </span>
          ) : (
            "Objects carrying the selected tag."
          )}
        </CardDescription>
      </CardHeader>
      <CardContent>
        {!sel ? (
          <EmptyState
            icon={TagsIcon}
            title="Select a tag"
            body="Pick a tag on the left to see the objects that carry it."
          />
        ) : (
          <>
            {objects.error ? (
              <ErrorAlert
                title="Could not load objects"
                message={objects.error}
                onRetry={objects.refresh}
              />
            ) : null}

            {objects.loading ? (
              <DataTable columns={OBJECT_COLUMNS} minWidth={560}>
                <SkeletonRows rows={6} widths={OBJECT_SKELETON_WIDTHS} />
              </DataTable>
            ) : rows.length === 0 && !objects.error ? (
              <EmptyState
                icon={TagsIcon}
                title="No objects"
                body="No objects currently carry this tag."
              />
            ) : (
              <DataTable columns={OBJECT_COLUMNS} minWidth={560}>
                {rows.map((o) => (
                  <TableRow key={`${o.bucket}/${o.key}@${o.version_id}`}>
                    <TableCell className="font-mono text-[13px]">
                      <Link
                        to={`/buckets/${encodeURIComponent(o.bucket)}/browser`}
                        className="text-link underline-offset-4 hover:underline"
                      >
                        {o.bucket}
                      </Link>
                    </TableCell>
                    <TableCell className="font-mono text-[13px]">
                      <Link
                        to={`/buckets/${encodeURIComponent(o.bucket)}/browser`}
                        className="block max-w-[36ch] truncate text-link underline-offset-4 hover:underline"
                        title={o.key}
                      >
                        {o.key}
                      </Link>
                    </TableCell>
                    <TableCell className="text-right text-[13px] tabular-nums">
                      {bytes(o.size)}
                    </TableCell>
                    <TableCell className="text-[13px] text-muted-foreground tabular-nums">
                      {whenMs(o.last_modified_ms)}
                    </TableCell>
                  </TableRow>
                ))}
              </DataTable>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
