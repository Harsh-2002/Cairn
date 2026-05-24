# Cairn Plugin Contract

> Plugins are external processes. Run by Cairn at fixed hook points; communicate over stdin/stdout JSON.

This is the authoritative wire contract. The Go plugin runner in `internal/plugins` is the reference implementation; everything else (validation rules, ordering, error handling) lives here.

## Hooks

Seven fixed lifecycle points. Each is a directory under `plugins/<hook-name>/`. Every executable file in that directory is a plugin for that point. Plugins run in **filename order** (lexicographic; prefix with `00-`, `10-`, `20-` to order). Plugins are language-agnostic — anything that reads JSON on stdin and writes JSON on stdout works.

| Hook | When it fires | Input | Output |
|---|---|---|---|
| `pre-ingest` | Before frontmatter is parsed; sees raw source. | `{ "path": "...", "source": "<raw markdown>" }` | `{ "source": "<possibly modified>", "abort": false }` |
| `post-ingest` | After normalization; sees canonical post. | `{ "path": "...", "frontmatter": {...}, "body": "<canonical markdown>" }` | `{ "body": "<possibly modified>", "frontmatter_updates": {...}, "abort": false }` |
| `pre-asset` | Before content-addressing of an asset. | `{ "path": "...", "ext": "png", "size": 12345 }` | `{ "skip": false, "abort": false }` |
| `post-asset` | After upload, with the public URL. | `{ "sha256": "...", "ext": "png", "key": "...", "url": "..." }` | `{ "abort": false }` (side-effect hook) |
| `pre-render` | Sees the document model about to render. | `{ "slug": "...", "frontmatter": {...}, "html": "<HTML so far>" }` | `{ "html": "<possibly modified>" }` |
| `post-render` | Sees the rendered HTML and aggregate pages. | `{ "slug": "...", "html": "<final HTML>", "kind": "post"/"index"/"sitemap"/"feed" }` | `{ "html": "<possibly modified>" }` |
| `post-deploy` | After deploy succeeds. | `{ "site_url": "...", "commit": "<sha>" }` | `{}` (side-effect hook) |

## I/O semantics

- **Input** is a single JSON object on stdin, terminated by EOF.
- **Output** is a single JSON object on stdout, terminated by EOF.
- **Stderr** is forwarded to Cairn's logs verbatim.
- **Exit code** 0 = success. Non-zero = error (Cairn aborts the run with the exit code surfaced; future runs of the same operation will retry from scratch).
- **Timeout** is 30 seconds per plugin. Configurable per-hook in `cairn.toml`.

## Validation

- Plugin output is validated against the hook's JSON Schema (generated from the Go types in `internal/plugins`). Output that fails validation aborts the run with the structured error.
- A plugin may set `"abort": true` to deliberately abort the pipeline (e.g. a `pre-ingest` link checker that found broken links). The error surfaces with the plugin's filename and the `"reason"` field if provided.

## Determinism

Plugins are part of the build plane when invoked at `pre-render`, `post-render`. To honor Invariant 6, those plugins **must** be deterministic — same input → same output. Authoring-plane hooks (`pre-ingest`, `post-ingest`, asset hooks) are not subject to the determinism rule because they run interactively and side-effects are accepted there.

## Plugin set is in git

`plugins/` is a directory in the source repository. The active plugin set is therefore part of the single source of truth (Invariant 1) and travels with the blog. Adding a plugin is `chmod +x` plus a commit; removing one is `rm` plus a commit.

## First-party plugins (Phase 6)

Three reference implementations ship as examples:

- `plugins/post-ingest/00-mermaid-prerender.sh` — Mermaid is now rendered natively, but this plugin shows the shape (it would have replaced fenced ```mermaid blocks with inline SVG via `mermaid-cli`).
- `plugins/post-ingest/10-katex-prerender.sh` — same shape, math rendering. Native server-side MathML supersedes it in Cairn ≥ v180526.
- `plugins/post-render/50-link-check.sh` — visits every `<a href="…">` in the rendered HTML and aborts the run if any 404.

These exist as documentation. The native Mermaid and KaTeX paths in `cairn-render` are the supported default; the plugins demonstrate that the seam is real.

## Errors

| Class | Behaviour |
|---|---|
| Plugin not executable | Skipped with a warning. |
| Plugin times out | Run aborts with a clear timeout error pointing at the filename. |
| Non-zero exit | Run aborts; stderr forwarded. |
| Invalid JSON output | Run aborts; plugin filename + the parsing error surfaced. |
| `abort: true` in result | Run aborts cleanly; reason field surfaced if present. |

There is no retry. Plugin failures are visible and the operator decides what to do.

## Schema versioning

This contract is **v1**. Future breaking changes require a `"cairn_plugin_schema_version": 2` field in plugin output; v1 output continues to be accepted on a v1+v2 runner. Additive changes (new optional response fields) do not require a version bump.
