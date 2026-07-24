import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * A reassuring empty state: dashed border, plain-language explanation, and an
 * optional next step. `positive` renders the icon in the success colour for
 * "empty is good" cases (e.g. no failed replication).
 */
export function EmptyState({
  icon: Icon,
  title,
  body,
  action,
  positive = false,
  className,
}: {
  icon?: LucideIcon;
  title: string;
  body?: ReactNode;
  action?: ReactNode;
  positive?: boolean;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed px-6 py-12 text-center",
        className,
      )}
    >
      {Icon ? (
        <Icon
          aria-hidden="true"
          className={cn(
            "mb-1 size-7",
            positive ? "text-success" : "text-muted-foreground",
          )}
        />
      ) : null}
      <p className="text-sm font-medium">{title}</p>
      {body ? (
        <p className="max-w-sm text-sm leading-relaxed text-muted-foreground">
          {body}
        </p>
      ) : null}
      {action ? <div className="mt-3">{action}</div> : null}
    </div>
  );
}
