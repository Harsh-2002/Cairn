# Contributing to Cairn

Thanks for thinking about contributing. Cairn is a small, opinionated project — please read this file before opening a PR.

## Where to start

- **Architecture spec:** [`docs/ARCH.md`](docs/ARCH.md). It is authoritative; the eight invariants are non-negotiable.
- **Onboarding map:** [`CLAUDE.md`](CLAUDE.md). 5-minute tour of the workspace, conventions, and dev commands.
- **Contracts:** [`docs/frontmatter.md`](docs/frontmatter.md), [`docs/PLUGIN_CONTRACT.md`](docs/PLUGIN_CONTRACT.md), [`docs/release-policy.md`](docs/release-policy.md).

If a doc, comment, or PR description contradicts the spec, the spec wins. File an issue and fix the conflict.

## Prerequisites

| Tool | Version | Why |
|---|---|---|
| Go | 1.26+ | Core build (uses `slices.SortStableFunc`, generics, modern `embed`). |
| Node | 24+ | Admin SPA build (Svelte 5 + Vite). |
| npm | any recent | SPA package manager (lockfile is committed). |
| git | any recent | Required by the local repository provider. |
| Docker | optional | For real-S3 / MinIO integration testing of the asset pipeline. |

## Dev loop

```sh
# Type-check + tests
go build ./...
go test ./...

# Format + lint
go vet ./...
gofmt -w .

# Build + run against the reference blog
go build -o cairn ./cmd/cairn
./cairn build examples/blog -o /tmp/site

# Two-run determinism check
./cairn build examples/blog -o /tmp/site1
./cairn build examples/blog -o /tmp/site2
diff -r /tmp/site1 /tmp/site2          # must be empty

# Cross-compile (no CGO required)
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -o /tmp/cairn-linux-arm64        ./cmd/cairn
CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -o /tmp/cairn-darwin-arm64       ./cmd/cairn
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o /tmp/cairn-windows-amd64.exe  ./cmd/cairn
```

If you'd rather `make`:

```sh
make            # build + test
make build      # just build the binary
make test       # just test
make fmt        # gofmt -w .
make cross      # cross-compile all 5 release targets into ./dist/
make admin      # build the Svelte SPA + stage it for the embed
make clean      # remove build artifacts
```

## Repo conventions

- **Comments.** None unless the *why* is non-obvious. No "added for X", "used by Y", or change-log style comments — they rot.
- **Commits.** Conventional Commits with the package name as scope:
  - `feat(repo): force_commit_to_branch on GitHubApiProvider`
  - `fix(render): trim trailing newline on math blocks`
  - `docs(arch): clarify Decision 5 wording`
- **Branches.** Trunk-based on `main`. Never push to `cairn/drafts/*` — those are managed by the editor server (`cairn serve`).
- **Errors.** Sentinel errors (`var ErrFoo = errors.New(…)`) and typed errors (`type FooError struct{ … }`) usable with `errors.Is` / `errors.As`. Wrap with `fmt.Errorf("…: %w", err)`. Don't `panic` except in package init or `must*` helpers behind a clearly-stated invariant.
- **Tests.** Every public function or exported type should have a test. Use stdlib `testing` for ergonomic table-driven tests; `httptest.Server` for HTTP-facing tests; `t.TempDir()` for filesystem-backed tests (e.g., LocalGitProvider). The 35-fixture markdown roundtrip and the e2e determinism check (`internal/cli/build_determinism_test.go`) are the two load-bearing tests — never let them go red.
- **Determinism.** Build-plane packages — `internal/render`, `internal/markdown`, `internal/frontmatter`, `internal/asset`, `internal/core` — must not call `time.Now()`, `os.Getenv`, or `rand.*`, and must not read filesystem mtimes. If you absolutely must, add a comment explaining why and reviewers will scrutinise.

## Architectural discipline

- **Where things go.**
  - Ingestion logic → `internal/markdown` / `internal/frontmatter` / `internal/notion`.
  - Build plane → `internal/render`.
  - All git access → `internal/repo` (never `go-git` or GitHub REST elsewhere).
  - All object storage → `internal/asset` and `internal/server/signer` (never `gocloud.dev/blob` elsewhere).
  - HTTP routing → `internal/server`.
  - CLI subcommand handlers → `internal/cli`.

- **Core vs plugin.** The core does what *every* blog needs. Everything else is a plugin. Mermaid, KaTeX, comments, analytics, and link-checking all sit at the plugin seam (`internal/plugins`), never in core. See [`docs/PLUGIN_CONTRACT.md`](docs/PLUGIN_CONTRACT.md).

- **Anti-patterns explicitly forbidden:**
  - Asset cache outside git.
  - Autosaves on `main` (drafts live on `cairn/drafts/<slug>/<session>` branches).
  - GitHub token in the browser (the server holds it; the browser sees only the admin-secret it typed in).
  - Notion mapping table separate from frontmatter (`notion_page_id` field is the mapping).
  - Build output committed to source branch (`_site/` is gitignored; deploy strategies push elsewhere).

## Adding things

### A new theme

1. `internal/render/themes/<name>/` with `theme.toml`, `templates/{index,post,sitemap,feed}.html`, `templates/partials/`, and `static/theme.css`.
2. Add the theme name to `KnownThemes` in `internal/render/loader.go`.
3. The embed picks it up automatically (the `//go:embed themes` directive globs).
4. Add a fixture or extend tests to keep the determinism check honest.

### A new plugin hook

Plugins are external processes; they don't extend the binary. See `docs/PLUGIN_CONTRACT.md`. Don't add a new hook unless you can also explain why the existing seven aren't enough.

### A new subcommand

1. New file in `internal/cli/<name>.go` with `func newXxxCmd() *cobra.Command`.
2. Register it in `NewRootCommand()` in `internal/cli/cli.go`.
3. Tests in `internal/cli/<name>_test.go`.

### A new dependency

Justify it in the PR description. The whole point of "one binary, no CGO" is that we control the surface; every dep is a surface to audit.

## CI

Four workflows live in `.github/workflows/`:

- `ci.yml` — build + test + vet + gofmt on linux / macos / windows; cross-compile all 5 release targets.
- `determinism.yml` — two-run + cross-platform site-tree hash matching across linux x86_64, linux arm64, macos arm64.
- `e2e-publish.yml` — manual; exercises every (theme × deploy) combination via `cairn init` + `cairn build`.
- `release.yml` — manual; cuts a `vDDMMYY` release with the 5 platform archives and `SHA256SUMS`.

Run them locally before pushing if your change touches the build plane or release flow.

## Reviewing a PR

- The diff should only touch what the title says it touches.
- New public function → new test.
- Build-plane change → determinism check still passes.
- Editor / SPA change → no token leakage in the diff or any new endpoint that doesn't go through `requireAdminSecret`.
- Doc change → reflects the *current* code, not the planned code.

## Licence

By contributing you agree your work is licensed under the MIT licence in [LICENSE](LICENSE).
