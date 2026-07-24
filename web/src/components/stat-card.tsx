import type { ReactNode } from "react";
import { Card } from "@/components/primitives/card";
import { Skeleton } from "@/components/primitives/skeleton";
import { cn } from "@/lib/utils";

/** A Vercel-style stat: quiet label, big tabular number, optional sub-line. */
export function StatCard({
  label,
  value,
  sub,
  mono = false,
  loading = false,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  mono?: boolean;
  loading?: boolean;
}) {
  return (
    <Card className="gap-1 rounded-lg border p-4 shadow-none">
      <p className="text-[13px] text-muted-foreground">{label}</p>
      {loading ? (
        <Skeleton className="h-8 w-24" />
      ) : (
        <p
          className={cn(
            "text-2xl font-semibold tracking-tight tabular-nums",
            mono && "font-mono text-xl",
          )}
        >
          {value}
        </p>
      )}
      {sub ? (
        <p className="text-xs text-muted-foreground">{loading ? " " : sub}</p>
      ) : null}
    </Card>
  );
}
