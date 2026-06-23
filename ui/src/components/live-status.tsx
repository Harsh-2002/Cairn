// The live-data status control. On a view wired to a live topic it replaces the old manual Refresh
// button: while the SSE stream is healthy it is a quiet, non-interactive "Live" indicator (data flows
// in on the server's cadence, so there is nothing to click); if the stream drops it degrades to an
// actionable Refresh button that forces an immediate refetch and reconnect. It takes the same props
// as RefreshButton so swapping one for the other is a one-line change in each view.

import { useEffect, useState } from "react";
import { RotateCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { reconnectNow, useLiveStatus } from "@/lib/live";

export function LiveStatus({
  loading,
  refreshing,
  onClick,
}: {
  /** True before first data — the indicator reads "Connecting…" until the first load resolves. */
  loading: boolean;
  /** True during a manual refetch from the dropped-stream fallback — drives the spinner. */
  refreshing: boolean;
  /** The view's refetch; invoked (alongside an immediate reconnect) from the fallback button. */
  onClick: () => void;
}) {
  const { connected, failures } = useLiveStatus();

  // Only surface "reconnecting" once the trouble has persisted ~1.5s. The server cycles the stream
  // roughly every 5 minutes (a clean close→reopen); without this debounce the indicator would blink
  // to the fallback on every healthy reconnect. A genuine outage keeps failing and crosses 1.5s.
  const [troubled, setTroubled] = useState(false);
  useEffect(() => {
    if (failures === 0) {
      setTroubled(false);
      return;
    }
    const t = setTimeout(() => setTroubled(true), 1500);
    return () => clearTimeout(t);
  }, [failures]);

  // Stream is down for real: fall back to a manual control. A warning dot signals that live updates
  // are paused; clicking refetches now and skips the backoff. Mirrors the old RefreshButton so the
  // recovery path stays familiar.
  if (troubled) {
    return (
      <Button
        variant="outline"
        onClick={() => {
          onClick();
          reconnectNow();
        }}
        disabled={refreshing}
        aria-busy={refreshing || undefined}
        aria-label="Live updates interrupted — refresh now and reconnect"
        title="Live updates interrupted. Refreshes now and reconnects."
      >
        {refreshing ? (
          <RotateCw aria-hidden="true" className="animate-spin" />
        ) : (
          <span
            aria-hidden="true"
            className="size-1.5 rounded-full bg-warning"
          />
        )}
        Refresh
      </Button>
    );
  }

  // Healthy (or still connecting): a calm status, not a button — the data refreshes itself.
  const live = connected && !loading;
  return (
    <span
      role="status"
      aria-live="polite"
      className="inline-flex h-9 items-center gap-1.5 px-1 text-sm text-muted-foreground select-none"
    >
      <span aria-hidden="true" className="relative inline-flex size-1.5">
        {live && (
          <span className="absolute inline-flex size-full animate-ping rounded-full bg-success opacity-60 motion-reduce:hidden" />
        )}
        <span
          className={cn(
            "relative inline-flex size-1.5 rounded-full",
            live ? "bg-success" : "bg-muted-foreground/40",
          )}
        />
      </span>
      {live ? "Live" : "Connecting…"}
    </span>
  );
}
