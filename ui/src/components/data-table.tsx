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
  minWidth,
  children,
}: {
  columns: Column[];
  /** A min table width in px; applied as an inline style so it survives the JIT. */
  minWidth?: number;
  children: ReactNode;
}) {
  const style: CSSProperties | undefined = minWidth ? { minWidth } : undefined;
  return (
    <div className="overflow-x-auto rounded-lg border">
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
        <TableBody>{children}</TableBody>
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
