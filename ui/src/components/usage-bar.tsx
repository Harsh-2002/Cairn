import { cn } from "@/lib/utils";

/**
 * A quiet horizontal usage bar: hairline-bordered track, neutral fill.
 * `percent` is clamped to 0..100. Always labelled for assistive tech.
 */
export function UsageBar({
  percent,
  label,
  className,
  fillClassName,
}: {
  percent: number;
  /** Human-readable description, e.g. "photos: 1.2 GiB, 34% of total". */
  label: string;
  className?: string;
  fillClassName?: string;
}) {
  const clamped = Math.max(0, Math.min(100, Math.round(percent)));
  return (
    <div
      role="progressbar"
      aria-valuenow={clamped}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={label}
      className={cn(
        "h-2 w-full overflow-hidden rounded-full border bg-muted",
        className,
      )}
    >
      <div
        className={cn("h-full rounded-full bg-foreground/80", fillClassName)}
        style={{ width: `${clamped}%` }}
      />
    </div>
  );
}
