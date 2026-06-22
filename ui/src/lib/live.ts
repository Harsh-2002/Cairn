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
    // Not authed yet, or the server has no SSE — retry; views keep working via their Refresh button.
    scheduleReopen(5000);
    return;
  }
  // Topics may have changed while awaiting the ticket; re-check.
  const want = activeTopics();
  if (want === "") return;

  const url = `/api/v1/events/stream?ticket=${encodeURIComponent(ticket)}&topics=${encodeURIComponent(want)}`;
  const src = new EventSource(url);
  source = src;
  openTopics = want;
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
  // ourselves: close and reopen with a fresh ticket after a short backoff.
  src.onerror = () => {
    if (source === src) {
      closeStream();
      scheduleReopen(3000);
    }
  };
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
 * Subscribe a view to a live topic. `onMessage` receives each pushed snapshot (the same JSON the
 * topic's normal GET returns); most views pass a `refresh` so live data flows through their existing
 * fetch path with no merge logic. The subscription is torn down on unmount.
 */
export function useLiveTopic(topic: string, onMessage: (data: unknown) => void) {
  const cb = useRef(onMessage);
  cb.current = onMessage;
  useEffect(() => subscribe(topic, (data) => cb.current(data)), [topic]);
}
