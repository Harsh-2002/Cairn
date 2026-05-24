# Cairn — Onboarding

> A single Go binary that publishes a fully static blog from markdown in git, with no server and no state.

This file is the 5-minute map. Read it, then read the spec for the why.

---

## 1. Authoritative docs

| Doc | Path | Treat as |
|---|---|---|
| Architecture spec | `docs/ARCH.md` | **ground truth** — settles disputes |
| Frontmatter contract | `docs/frontmatter.md` | the schema every editor must satisfy |
| Plugin contract | `docs/PLUGIN_CONTRACT.md` | the wire contract for plugin processes |
| Release policy | `docs/release-policy.md` | how versions get cut |
| Contributing | `CONTRIBUTING.md` | dev workflow, repo conventions, recipes |

If a doc, comment, or PR description contradicts the spec, the spec wins. File an issue and fix the conflict.

## 2. Workspace map

```
.
├── go.mod / go.sum               module github.com/Harsh-2002/Cairn (Go 1.26+)
├── cmd/
│   └── cairn/                    main package — thin cobra wiring
├── internal/                     package-private; no external Go imports
│   ├── core/                     shared types (Post, RepoPath, AssetRef, …)
│   ├── frontmatter/              YAML schema, validation, slug derivation
│   ├── markdown/                 goldmark + custom canonical serializer
│   ├── repo/                     RepositoryProvider interface
│   │   ├── local/                  go-git implementation
│   │   └── github/                 REST API implementation
│   ├── asset/                    content-addressed pipeline (gocloud.dev/blob)
│   ├── render/                   build plane (pongo2 + chroma + 3 themes)
│   │   ├── highlight/              chroma wrapper
│   │   └── themes/                 stones / drift / press (embedded)
│   ├── notion/                   Notion adapter (markdown + blocks API)
│   ├── plugins/                  external-process plugin runner (7 hooks)
│   ├── server/                   chi router + admin SPA host
│   │   ├── signer/                 S3 / mock URL signer
│   │   └── admin/admin-dist/       built Svelte SPA, embedded via //go:embed
│   └── cli/                      subcommand handlers
│       └── deploy/                 embedded GH-pages / Cloudflare workflows
├── admin/                        Svelte 5 + Vite SPA (frontend source)
├── docs/                         authoritative contracts (frontmatter, plugin, arch)
├── examples/blog/                reference blog the determinism CI builds
└── .github/workflows/            ci / determinism / release / e2e-publish
```

**Discipline:**
- Ingestion logic in `internal/markdown` / `internal/frontmatter` / `internal/notion`.
- Build plane in `internal/render`.
- All git access through `internal/repo` (never reach for `go-git` or `net/http` against `api.github.com` outside that subtree).
- All object storage through `internal/asset` and `internal/server/signer` (never reach for `gocloud.dev/blob` outside).

## 3. The eight invariants

These are non-negotiable. A simpler implementation that breaks one is wrong by definition.

1. Git is the single source of truth for source.
2. Markdown is the canonical content format.
3. The pipeline is stateless.
4. The served blog has zero compute.
5. Editor-agnostic by contract.
6. Determinism.
7. One binary.
8. Open source from the first commit.

See `docs/ARCH.md` §3 for the precise wording.

## 4. The two-plane mental model

```
AUTHORING PLANE (interactive, stateless)              BUILD PLANE (pure, deterministic, stateless)
   built-in editor / external editor / Notion            same binary, run by CI on commit
            │                                                       │
            ▼                                                       ▼
   asset pipeline (content-addressed → object storage)       render to _site/, deploy via strategy
            │
            ▼
   RepositoryProvider.Commit(markdown+frontmatter+originals)
            │
            ▼
        ┌──── GIT ────┐  ← the only seam between the planes
```

Authoring writes source to git. Build reads source from git. They never share memory; their interface is a commit.

## 5. Build / test commands

| Command | What it does |
|---|---|
| `go build ./...` | type-check all packages |
| `go test ./...` | run every test (unit + integration) |
| `go test ./internal/markdown -run TestAllFixturesAreStable` | the 35-fixture markdown roundtrip suite |
| `go test ./internal/cli -run TestBuildIsDeterministic` | e2e two-build byte-identical check |
| `go build -o cairn ./cmd/cairn` | build the binary |
| `go run ./cmd/cairn build examples/blog -o _site` | render the reference blog |
| `go run ./cmd/cairn new "Post title"` | scaffold a draft post under `content/posts/` |
| `make` (if `make` available) | shorthand for `go build ./... && go test ./...` |
| `cd admin && npm ci && npm run build` | build the Svelte admin SPA into `admin/dist/` |
| `cd admin && npm run dev` | Vite dev server for admin (use with `cairn serve --vite-proxy`) |

## 6. Local dev environment

| Tool | Why | Version |
|---|---|---|
| `go` | core build | 1.26+ |
| `node` | admin SPA build | 24+ |
| `npm` | admin package manager (lockfile committed) | any recent |
| `docker` | MinIO for asset pipeline integration tests | optional |
| `katex` (npm) | first-party KaTeX plugin (post-render) | optional |
| `mermaid-cli` | first-party Mermaid plugin (post-render) | optional |

**Admin SPA embed:** `internal/server/admin/admin.go` declares `//go:embed admin/admin-dist`. The `admin-dist/` directory is `.gitignore`d; CI and a fresh checkout regenerate it by running `cd admin && npm ci && npm run build` and copying the result to `internal/server/admin/admin-dist/`. A placeholder `index.html` lives in the directory so the embed compiles before the SPA is built.

### Svelte MCP server

The Svelte MCP server exposes Svelte 5 and SvelteKit documentation as four tools. Prefer it over external search for any Svelte/SvelteKit question, including topics that look "already known" — the docs evolve.

| Tool | What it does |
|---|---|
| `list-sections` | Returns the documentation table of contents (titles, use cases, paths). First call when starting any Svelte/SvelteKit work — use the `use_cases` field to decide which sections to fetch. |
| `get-documentation` | Fetches full content for one or more sections. After `list-sections`, fetch every section whose `use_cases` field matches the task. |
| `svelte-autofixer` | Analyzes a Svelte snippet and reports issues + suggestions. Run on any Svelte code before treating it as final; re-run until it returns clean. |
| `playground-link` | Generates a Svelte Playground URL from a snippet. Useful for sharing a self-contained sample — not for code already written to files in the project. |

## 7. Working conventions

- **Comment policy.** None unless the *why* is non-obvious. No "// added for X" or "// used by Y" comments — those rot.
- **Commit style.** Conventional Commits, scope = package name. Example: `feat(repo): GitHubApiProvider supports force_commit_to_branch`.
- **Branch model.** Trunk-based on `main`. Never push to `cairn/drafts/*` from a human session — those are managed by the editor server.
- **Determinism rule.** Any code in the build plane (`internal/render`, `internal/markdown`, `internal/frontmatter`, `internal/asset`, `internal/core`) that wants `time.Now()`, `os.Getenv`, `rand.*`, or filesystem mtimes needs an explicit justification. Map iteration must be over sorted keys. Slice ordering must be stable with explicit tiebreakers.
- **Test first or alongside, not after.** A new public function without a test fails review.
- **Errors.** Sentinel errors (`var ErrFoo = errors.New(…)`) + typed errors (`type FooError struct{ … }`) usable with `errors.Is` / `errors.As`. Wrap with `fmt.Errorf("…: %w", err)`. Don't `panic` except in package init or `must*`-style helpers behind a clearly-stated invariant.

## 8. Cairn-specific discipline

- The architecture spec is authoritative. If a comment, a PR description, or a doc contradicts it, the spec wins.
- The eight invariants override convenience. A simpler implementation that breaks an invariant is wrong by definition.
- Don't reintroduce patterns the architecture fixes: asset cache outside git, autosaves on `main`, GitHub token in the browser, Notion mapping table separate from frontmatter, output committed to source branch.
- Core vs plugin: the core does what *every* blog needs. Everything else is a plugin. Mermaid, KaTeX, comments, analytics, link-checking all sit at the plugin seam, never in core.

## 9. Engineering principles

Tradeoff: these guidelines bias toward caution over speed. For trivial tasks, use judgment.

### 9.1 Think before coding

Don't assume. Don't hide confusion. Surface tradeoffs.

Before implementing:
- State assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 9.2 Simplicity first

Minimum code that solves the problem. Nothing speculative.

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

The senior-engineer test: would someone say this is overcomplicated? If yes, simplify.

### 9.3 Surgical changes

Touch only what you must. Clean up only your own mess.

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that your changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: every changed line should trace directly to the user's request.

### 9.4 Goal-driven execution

Define success criteria. Loop until verified.

- "Add validation" → "Write tests for invalid inputs, then make them pass."
- "Fix the bug" → "Write a test that reproduces it, then make it pass."
- "Refactor X" → "Ensure tests pass before and after."

For multi-step tasks, state a brief plan with verification per step. Strong success criteria let you loop independently; weak criteria ("make it work") require constant clarification.

These guidelines are working if: fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

---

## 10. You are here

Check `git log --oneline` for the most recent state. The set of working subcommands is whatever `cairn --help` prints today, and the set of packages is whatever `go list ./...` lists today — both are more current than any narrative checkpoint kept in this file.

If you're picking the project up cold, the right order is:

1. Read sections 1–9 of this file.
2. Skim `docs/ARCH.md`.
3. (If touching the server or admin) build the SPA: `cd admin && npm ci && npm run build && mkdir -p ../internal/server/admin/admin-dist && cp -r dist/. ../internal/server/admin/admin-dist/`.
4. Confirm the tree is green: `go test ./...`.
5. Look at `git log -10` to see what landed last.
