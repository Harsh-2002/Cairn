// App-level live-update layer over the server's Server-Sent-Events channel (one multiplexed
// EventSource per tab). Views subscribe to a topic with `useLiveTopic`; the manager keeps a single
// connection open carrying the union of subscribed topics, mints a fresh single-use ticket for each
// (re)connection, and dispatches each `event: <topic>` frame to that topic's subscribers. On error
// or the server's periodic stream close it reconnects with backoff. Falls back silently to each
// view's existing Refresh button when the stream can't be established.

import { useEffect, useRef } from "react";
import { api } from "@/lib/api";

type Listener = (data: unknown) => void;

const listeners = new Map<string, Set<Listener>>();
let source: EventSource | null = null;
let openTopics = "";
let reopenTimer: ReturnType<typeof setTimeout> | null = null;
let coalesceTimer: ReturnType<typeof setTimeout> | null = null;
// Consecutive connection failures, for exponential reconnect backoff. Reset to 0 once a stream
// opens successfully so a healthy reconnect (e.g. the server's periodic stream close) is prompt.
let failures = 0;
// Set true while the tab is hidden so a backgrounded console stops driving the refresh cadence.
let paused = false;

/** Backoff for the Nth consecutive failure: 1s, 2s, 4s, … capped at 30s, with ±20% jitter. */
function backoffMs(n: number): number {
  const base = Math.min(30_000, 1000 * 2 ** Math.min(n, 5));
  return Math.round(base * (0.8 + Math.random() * 0.4));
}

/** The topics with at least one live subscriber, as a stable comma-joined key. */
function activeTopics(): string {
  return [...listeners.keys()]
    .filter((t) => (listeners.get(t)?.size ?? 0) > 0)
    .sort()
    .join(",");
}

function closeStream() {
  if (source) {
    source.close();
    source = null;
  }
  openTopics = "";
}

function scheduleReopen(delayMs: number) {
  if (reopenTimer != null) return;
  reopenTimer = setTimeout(() => {
    reopenTimer = null;
    void openStream();
  }, delayMs);
}

async function openStream() {
  if (paused) return; // hidden tab: don't hold a stream open
  const topics = activeTopics();
  if (topics === "") {
    closeStream();
    return;
  }
  // Already streaming exactly these topics — nothing to do.
  if (source && topics === openTopics) return;
  closeStream();

  let ticket: string;
  try {
    ticket = (await api.eventsTicket()).ticket;
  } catch {
    // Not authed yet, or the server has no SSE — retry with backoff; views keep working via their
    // Refresh button in the meantime.
    failures += 1;
    scheduleReopen(backoffMs(failures));
    return;
  }
  // Topics may have changed while awaiting the ticket; re-check.
  const want = activeTopics();
  if (want === "") return;

  const url = `/api/v1/events/stream?ticket=${encodeURIComponent(ticket)}&topics=${encodeURIComponent(want)}`;
  const src = new EventSource(url);
  source = src;
  openTopics = want;
  // A clean open clears the failure count so a normal reconnect doesn't inherit a long backoff.
  src.onopen = () => {
    if (source === src) failures = 0;
  };
  for (const topic of want.split(",")) {
    src.addEventListener(topic, (e: MessageEvent) => {
      let data: unknown;
      try {
        data = JSON.parse(e.data);
      } catch {
        data = undefined;
      }
      listeners.get(topic)?.forEach((fn) => fn(data));
    });
  }
  // EventSource auto-reconnect would reuse the now-consumed single-use ticket, so drive reconnection
  // ourselves: close and reopen with a fresh ticket after an exponential, jittered backoff (so a
  // persistently failing endpoint is not hammered every few seconds).
  src.onerror = () => {
    if (source === src) {
      closeStream();
      failures += 1;
      scheduleReopen(backoffMs(failures));
    }
  };
}

/**
 * Tear the live layer down completely — called on logout. Closes the stream, cancels pending
 * reconnects, and forgets the backoff state so a later login starts clean. Subscriptions are left
 * intact (mounted views re-open the stream); a logout unmounts them anyway.
 */
export function stopLive() {
  closeStream();
  if (reopenTimer != null) {
    clearTimeout(reopenTimer);
    reopenTimer = null;
  }
  if (coalesceTimer != null) {
    clearTimeout(coalesceTimer);
    coalesceTimer = null;
  }
  failures = 0;
}

// Pause the stream while the tab is hidden (no user is watching, so don't drive the refresh
// cadence), and resume on return. Registered once at module load.
if (typeof document !== "undefined") {
  document.addEventListener("visibilitychange", () => {
    paused = document.hidden;
    if (paused) {
      closeStream();
    } else {
      reconcileSoon();
    }
  });
}

/** Coalesce a burst of subscribe/unsubscribe (e.g. a route change) into one reopen. */
function reconcileSoon() {
  if (coalesceTimer != null) return;
  coalesceTimer = setTimeout(() => {
    coalesceTimer = null;
    if (activeTopics() !== openTopics) void openStream();
  }, 50);
}

function subscribe(topic: string, fn: Listener): () => void {
  let set = listeners.get(topic);
  if (!set) {
    set = new Set();
    listeners.set(topic, set);
  }
  set.add(fn);
  reconcileSoon();
  return () => {
    listeners.get(topic)?.delete(fn);
    reconcileSoon();
  };
}

/**
 * Subscribe a view to a live topic. The server emits an empty *pulse* per topic on its cadence;
 * `onMessage` fires for each pulse, and views pass a `refresh` so live data flows through their
 * existing (authenticated) fetch path. The subscription is torn down on unmount.
 *
 * `minIntervalMs` throttles how often `onMessage` actually fires (leading + trailing), independent
 * of the pulse cadence — use it for views whose refresh is expensive (e.g. a per-bucket fan-out) so
 * a 3 s pulse cadence does not trigger a request storm. A trailing call guarantees the view still
 * converges to the latest state after a burst.
 */
export function useLiveTopic(
  topic: string,
  onMessage: (data: unknown) => void,
  minIntervalMs = 0,
) {
  const cb = useRef(onMessage);
  cb.current = onMessage;
  useEffect(() => {
    let last = 0;
    let trailing: ReturnType<typeof setTimeout> | null = null;
    const fire = (data: unknown) => {
      if (minIntervalMs <= 0) {
        cb.current(data);
        return;
      }
      const now = Date.now();
      const wait = last + minIntervalMs - now;
      if (wait <= 0) {
        last = now;
        cb.current(data);
      } else if (trailing == null) {
        trailing = setTimeout(() => {
          trailing = null;
          last = Date.now();
          cb.current(data);
        }, wait);
      }
    };
    const unsub = subscribe(topic, fire);
    return () => {
      unsub();
      if (trailing != null) clearTimeout(trailing);
    };
  }, [topic, minIntervalMs]);
}
