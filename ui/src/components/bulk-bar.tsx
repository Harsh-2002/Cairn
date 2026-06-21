// The "N selected" action bar that sits above a list table once rows are checked. Mirrors the bar
// the object browser already uses, so bulk selection looks identical everywhere.

import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";

export function BulkBar({
  count,
  onClear,
  children,
}: {
  count: number;
  onClear: () => void;
  /** The action button(s) for the selection (e.g. Delete, Deactivate). */
  children: ReactNode;
}) {
  if (count === 0) return null;
  return (
    <div className="mb-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border bg-muted/40 px-3 py-2 animate-enter">
      <span className="text-[13px] tabular-nums">{count} selected</span>
      <span className="flex gap-2">
        <Button variant="ghost" size="sm" onClick={onClear}>
          Clear
        </Button>
        {children}
      </span>
    </div>
  );
}
