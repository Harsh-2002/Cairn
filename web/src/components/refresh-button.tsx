// The one canonical refresh control. Every list/detail view that re-fetches uses
// this so the disabled condition, busy affordance, and spin animation are uniform
// (previously three slightly different hand-rolled buttons).

import { RotateCw } from "lucide-react";
import { Button } from "@/components/primitives/button";

export function RefreshButton({
  loading,
  refreshing,
  onClick,
  label = "Refresh",
}: {
  /** True before first data — the button is disabled while the initial load runs. */
  loading: boolean;
  /** True during a non-destructive refresh — drives the spinner + aria-busy. */
  refreshing: boolean;
  onClick: () => void;
  label?: string;
}) {
  return (
    <Button
      variant="outline"
      onClick={onClick}
      disabled={loading || refreshing}
      aria-busy={refreshing || undefined}
    >
      <RotateCw
        aria-hidden="true"
        className={refreshing ? "animate-spin" : undefined}
      />
      {label}
    </Button>
  );
}
