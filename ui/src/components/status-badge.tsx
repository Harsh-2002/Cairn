// A status badge with a semantic tone. Keeps the flat Geist outline look but adds
// a small colored dot so binary health/config states (Active/Inactive, On/Off,
// healthy/failed) are glanceable instead of all rendering as identical muted
// outlines. The text label remains, so the signal is never colour-only.

import type { ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export type StatusTone = "positive" | "negative" | "warning" | "neutral";

const TONE_DOT: Record<StatusTone, string> = {
  positive: "bg-success",
  negative: "bg-destructive",
  warning: "bg-warning",
  neutral: "bg-muted-foreground/40",
};

export function StatusBadge({
  tone = "neutral",
  children,
  className,
}: {
  tone?: StatusTone;
  children: ReactNode;
  className?: string;
}) {
  return (
    <Badge
      variant="outline"
      className={cn(
        "gap-1.5",
        tone === "neutral" && "text-muted-foreground",
        className,
      )}
    >
      <span
        aria-hidden="true"
        className={cn("size-1.5 rounded-full", TONE_DOT[tone])}
      />
      {children}
    </Badge>
  );
}
