// Minimal hash-based router. Hash routing keeps reloads working without any
// server-side rewrite (ARCH §23.3 — the SPA shell is returned for client-side
// routes; with hash routing the shell is always index.html).
//
// Routes:
//   #/overview
//   #/buckets
//   #/buckets/<name>           (object browser + config panel for a bucket)
//   #/users
//   #/replication
//   #/activity

import { readable } from "svelte/store";

function parse() {
  const raw = (window.location.hash || "#/overview").replace(/^#/, "");
  const parts = raw.split("/").filter(Boolean);
  // parts[0] = view, parts[1..] = params
  const view = parts[0] || "overview";
  return { view, params: parts.slice(1).map(decodeURIComponent), raw };
}

export const route = readable(parse(), (set) => {
  const handler = () => set(parse());
  window.addEventListener("hashchange", handler);
  return () => window.removeEventListener("hashchange", handler);
});

export function navigate(path) {
  const target = path.startsWith("#") ? path : `#${path}`;
  if (window.location.hash === target) return;
  window.location.hash = target;
}
