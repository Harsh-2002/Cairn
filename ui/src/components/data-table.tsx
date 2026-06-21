// Shared table scaffolding: the bordered, horizontally-scrollable shell every list
// view wraps its table in, plus a skeleton-rows helper. Collapses what used to be a
// per-view "…TableShell" component duplicated five times (and the header markup
// duplicated again inside each skeleton branch).

import type { CSSProperties, ReactNode } from "react";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

export interface Column {
  /** Stable key for React. */
  key: string;
  /** Header label. */
  label: ReactNode;
  /** Extra header-cell classes (e.g. `text-right` for a numeric column). */
  className?: string;
  /** Render the label visually hidden (e.g. an actions column). */
  srOnly?: boolean;
}

export function DataTable({
  columns,
  minWidth = 560,
  stacked = true,
  children,
}: {
  columns: Column[];
  /**
   * Min table width in px (default 560), applied as an inline style so it survives
   * the JIT. On desktop it guarantees the columns don't crush; on mobile the stacked
   * layout ignores it (cells go full-width), so it never forces a sideways scroll.
   */
  minWidth?: number;
  /**
   * Stack rows into cards below `md` (default on). Cells carry a `data-label` so each
   * value is named in the card; the title/actions cells stay unlabelled. Pass false to
   * keep a horizontally-scrolling grid on every viewport.
   */
  stacked?: boolean;
  children: ReactNode;
}) {
  // The inline min-width pins the desktop grid so columns don't crush. In stacked
  // mode the `.table-stack` CSS neutralises it below md (min-width: 0 !important
  // beats the inline style) so the cards go full-width instead of overflowing.
  const style: CSSProperties = { minWidth };
  return (
    <div
      className={cn(
        "overflow-x-auto rounded-lg border",
        stacked &&
          "table-stack max-md:overflow-x-visible max-md:rounded-none max-md:border-0",
      )}
    >
      <Table style={style}>
        <TableHeader>
          <TableRow>
            {columns.map((c) => (
              <TableHead key={c.key} className={c.className}>
                {c.srOnly ? <span className="sr-only">{c.label}</span> : c.label}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody className="stagger-children">{children}</TableBody>
      </Table>
    </div>
  );
}

/**
 * Placeholder rows for the initial load. `widths` is one literal Tailwind width
 * class per column (written at the call site so the JIT sees them).
 */
export function SkeletonRows({
  rows,
  widths,
}: {
  rows: number;
  widths: string[];
}) {
  return (
    <>
      {Array.from({ length: rows }, (_, i) => (
        <TableRow key={i}>
          {widths.map((w, j) => (
            <TableCell key={j}>
              <Skeleton className={cn("h-4", w)} />
            </TableCell>
          ))}
        </TableRow>
      ))}
    </>
  );
}
