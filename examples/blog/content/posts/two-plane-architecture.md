---
title: "The two-plane architecture"
date: 2026-05-20T09:30:00+02:00
tags: [architecture]
summary: Why Cairn separates authoring from build.
---

Cairn has exactly two planes, and they meet only at git.

**The authoring plane** is interactive. Its job is to land conforming markdown — plus its assets — into a git commit. The built-in editor, an external editor pointed at the same folder, and the Notion adapter are all clients of this plane. It ends the moment a commit lands.

**The build plane** is a pure deterministic function. Its job is to read a commit and produce a static site. It runs in CI, takes inputs from the repo, and is byte-identical run-to-run.

The two planes never share state. The only thing between them is a git commit. That's what makes the system stateless: each plane is independently stateless and the seam between them is git, which is durable by being git.

```rust
fn build(repo: &Repo) -> Site {
    // deterministic, no clock, no env, no mtime
    render_all(repo.read_at_head())
}
```

The build above will produce the same bytes today, tomorrow, on any machine, in any timezone. That's not a slogan; it's the invariant the rest of the design depends on.
