// Hash-based router with nested routes + named params. Hash routing keeps reloads and deep links
// working without any server rewrite — the SPA shell is served only for `/`, so every in-app route
// lives in the fragment. Add a section by adding one line to ROUTES.
//
//   #/overview
//   #/buckets                       #/buckets/<name>/browser   #/buckets/<name>/settings
//   #/users                         #/users/<id>
//   #/activity                      #/replication

import { readable } from "svelte/store";

// Patterns are length-discriminated, so `/users` and `/users/:id` never collide.
const ROUTES = [
  { name: "overview", pattern: "/overview" },
  { name: "buckets", pattern: "/buckets" },
  { name: "bucket.browser", pattern: "/buckets/:name/browser" },
  { name: "bucket.settings", pattern: "/buckets/:name/settings" },
  { name: "users", pattern: "/users" },
  { name: "user", pattern: "/users/:id" },
  { name: "activity", pattern: "/activity" },
  { name: "replication", pattern: "/replication" },
];

// Match a raw hash path (no leading '#') against the table → { name, params, raw }. Unknown paths
// fall back to the overview.
export function match(raw) {
  const path = raw.split("?")[0];
  const segs = path.split("/").filter(Boolean);
  for (const r of ROUTES) {
    const pat = r.pattern.split("/").filter(Boolean);
    if (pat.length !== segs.length) continue;
    const params = {};
    let ok = true;
    for (let i = 0; i < pat.length; i++) {
      if (pat[i].startsWith(":")) params[pat[i].slice(1)] = decodeURIComponent(segs[i]);
      else if (pat[i] !== segs[i]) {
        ok = false;
        break;
      }
    }
    if (ok) return { name: r.name, params, raw };
  }
  return { name: "overview", params: {}, raw };
}

function current() {
  const raw = (window.location.hash || "#/overview").replace(/^#/, "");
  return match(raw);
}

export const route = readable(current(), (set) => {
  const handler = () => set(current());
  window.addEventListener("hashchange", handler);
  return () => window.removeEventListener("hashchange", handler);
});

export function navigate(path) {
  const target = path.startsWith("#") ? path : `#${path}`;
  if (window.location.hash === target) return;
  window.location.hash = target;
}
