// A tiny toast store. `toast(msg, kind)` queues a transient notice; the <Toasts/> component renders
// the queue. Kinds: "ok" | "error" | "info".
import { writable } from "svelte/store";

export const toasts = writable([]);
let nextId = 1;

export function toast(message, kind = "info", ms = 3500) {
  const id = nextId++;
  toasts.update((t) => [...t, { id, message, kind }]);
  if (ms > 0) setTimeout(() => dismiss(id), ms);
  return id;
}

export const ok = (m) => toast(m, "ok");
export const err = (m) => toast(m, "error", 6000);

export function dismiss(id) {
  toasts.update((t) => t.filter((x) => x.id !== id));
}
