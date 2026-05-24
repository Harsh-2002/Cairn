# Cairn

> A stack of stones a person assembles by hand to mark a path and to last.

A single Go binary that publishes a fully static, zero-server blog. Markdown in git is the only source of truth. It ships its own browser-based block editor with a Notion-grade typing experience, and also accepts content from any external editor or Notion.

**Setup is one thing:** a GitHub repository and a scoped token.

```
authoring plane  ────►  git (the single seam)  ────►  build plane
 (interactive,                                          (pure, deterministic,
  stateless)                                            stateless)
```

---

## What Cairn is for

A personal technical blog should not require operating infrastructure. Cairn replaces a Ghost stack — container + database + reverse proxy + backup job — with **a directory of markdown files** that renders to a CDN with no compute behind it.

- **Git is the single source of truth.** Markdown + frontmatter + templates + originals of every image, all in one repository.
- **The pipeline is stateless.** Same repository state → same output bytes. Wipe the cache, the bucket, the binary — they all rebuild from the repo.
- **Editor-agnostic by contract.** The built-in browser editor, Obsidian, VSCode, Notion, and vim are all conforming clients of the same markdown contract. None is privileged.
- **Zero compute at serve time.** HTML sits on a CDN, assets sit on object storage. There is nothing to operate.
- **Open source from the first commit.**

The full architecture (eight invariants, two-plane split, repository-provider abstraction, content-addressed assets, editor model, plugin seam) is in [`docs/ARCH.md`](docs/ARCH.md). It is the authoritative spec.

---

## Quick start

### Install

```sh
curl -fsSL https://github.com/Harsh-2002/Cairn/raw/main/install.sh | sh
```

The script detects your OS + architecture and installs the latest `cairn` binary to `~/.local/bin/cairn`. Override the prefix with `CAIRN_INSTALL_DIR=/usr/local/bin`.

**Binaries are pure Go (no CGO).** Cross-compiles cleanly to linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, and windows/amd64. No runtime dependencies — they run on any libc, any distribution.

Manual install: grab a binary from [Releases](https://github.com/Harsh-2002/Cairn/releases) and put it on your `PATH`.

From source (Go 1.26+):

```sh
go install github.com/Harsh-2002/Cairn/cmd/cairn@latest
```

### Initialize a blog

```sh
cairn init my-blog \
  --title "My Blog" \
  --base-url "https://me.github.io/my-blog" \
  --author "Your Name" \
  --theme stones \
  --deploy github-pages

cd my-blog
cairn new "Hello, world"      # writes content/posts/hello-world.md as a draft
cairn build . -o _site         # render to _site/
```

Push to a GitHub repo, enable Pages with source = GitHub Actions in repo
settings (one click), and the included workflow takes over: every push to
`main` builds and deploys the site.

### Themes

```sh
cairn init my-blog --theme <name>     # stones | drift | press
```

- **stones** — quiet editorial layout. Single column, serif body, generous
  whitespace, dark-mode aware. The default.
- **drift** — dark-mode-first technical blog. Geometric sans, indigo accent,
  card-based index, prominent code blocks with accent-bordered left edge.
- **press** — magazine layout. Display-serif headlines, grid index, drop-cap
  on the first paragraph, claret accent for category labels.

To customise a single template without forking a theme, drop a file at
`<source>/templates/<name>` and Cairn will use it instead of the bundled one.

### Deploy targets

```sh
cairn init my-blog --deploy <strategy>  # github-pages | cloudflare-pages | none
```

- `--deploy github-pages` (default) — uses `actions/deploy-pages`.
  One-time: in your repo settings → Pages, set source to "GitHub Actions".
- `--deploy cloudflare-pages` — uses Cloudflare's official Wrangler action.
  One-time: create a Pages project in Cloudflare, then add repo secrets
  `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`.
- `--deploy none` — no workflow scaffolded. Wire up your own under
  `.github/workflows/`; the build step is always `cairn build . -o _site`.

### Browser editing against a remote repo

`cairn serve` runs the embedded Svelte admin SPA. Two modes:

```sh
# Local clone (LocalGitProvider — needs a working copy)
cairn serve . --admin-secret <secret> \
              --bucket-endpoint http://127.0.0.1:9000 \
              --bucket-name my-blog \
              --bucket-access-key <ak> --bucket-secret-key <sk>

# Remote (GitHubApiProvider — no local clone needed)
cairn serve --repo me/my-blog \
            --token-env GITHUB_TOKEN \
            --admin-secret <secret> \
            --bucket-endpoint <s3-endpoint> --bucket-name <bucket> \
            --bucket-access-key <ak> --bucket-secret-key <sk>
```

In remote mode the server writes commits directly through the GitHub REST
API — autosaves land on a `cairn/drafts/<slug>/<session>` branch and publish
squashes them onto `main`. The PAT needs Contents: read+write (plus the
`workflow` scope if you intend cairn to push the CI workflow file).

### Update Cairn

```sh
cairn upgrade
```

The binary checks GitHub Releases for a newer version and replaces itself atomically when one is available.

---

## Workspace layout

```
.
├── go.mod / go.sum             # Go module (module github.com/Harsh-2002/Cairn)
├── cmd/
│   └── cairn/                  # main package — thin cobra wiring
├── internal/                   # package-private; no external Go imports
│   ├── core/                   # shared types (Post, RepoPath, AssetRef, …)
│   ├── frontmatter/            # YAML schema, validation, slug derivation
│   ├── markdown/               # goldmark + custom canonical serializer
│   ├── repo/                   # RepositoryProvider interface
│   │   ├── local/              #   go-git implementation
│   │   └── github/             #   REST API implementation
│   ├── asset/                  # content-addressed pipeline (gocloud.dev/blob)
│   ├── render/                 # build plane (pongo2 templates + chroma)
│   │   ├── highlight/          #   chroma wrapper
│   │   └── themes/             #   stones / drift / press (embedded)
│   ├── notion/                 # Notion Markdown + Blocks API adapters
│   ├── plugins/                # external-process plugin runner (7 hooks)
│   ├── server/                 # chi router + admin SPA host
│   │   ├── signer/             #   S3 / mock URL signer
│   │   └── admin/admin-dist/   #   built Svelte SPA, embedded via //go:embed
│   └── cli/                    # subcommand handlers
│       └── deploy/             #   embedded GH-pages / Cloudflare workflows
├── admin/                      # Svelte 5 + Vite admin SPA (frontend source)
├── docs/                       # ARCH.md, frontmatter.md, PLUGIN_CONTRACT.md, …
├── examples/blog/              # reference blog the determinism CI builds
├── .github/workflows/          # ci / determinism / release / e2e-publish
├── install.sh                  # one-line install script (Unix)
└── CONTRIBUTING.md             # dev workflow + repo conventions
```

**Discipline:** all git access flows through `internal/repo` (no `go-git` outside `internal/repo/local`, no GitHub REST outside `internal/repo/github`). All object storage flows through `internal/asset` (no `gocloud.dev/blob` outside that package or the server signer).

---

## How it works

### The two planes

The architecture has exactly two planes that meet **only at git**:

- **Authoring plane** is interactive. It gets conforming markdown + assets committed into git. The built-in editor, external editors, and the Notion adapter all live here. It ends the moment a commit lands.
- **Build plane** is a pure deterministic function from `(commit) → site`. It runs in CI when a commit lands on `main`, renders deterministically, and hands `_site/` to the deploy strategy.

Neither plane shares memory or state with the other. Their entire interface is a git commit.

### The frontmatter contract

Every editor produces conforming markdown:

```markdown
---
title: "Leaving Ghost"
date: 2026-05-22T18:00:00+02:00
tags: [migration]
summary: Why the operational tax finally outweighed the value.
---

Body in markdown.
```

Full schema: [`docs/frontmatter.md`](docs/frontmatter.md).

### Content-addressed assets

Image bytes hash to a SHA-256. The storage key is `<sha>/original.<ext>`. Variants for `<picture srcset>` are at `<sha>/<width>w.<ext>`. The original is also committed to git under `content/assets/<sha>.<ext>`, so the bucket is reconstructable from any past state of the repository.

### Determinism

Same repo state, same machine architecture, same date → byte-identical output. Verified continuously by [`.github/workflows/determinism.yml`](.github/workflows/determinism.yml) across linux-x86_64, linux-aarch64, and macos-aarch64.

---

## Develop locally

Prerequisites: Go 1.26+, Node 24+ (for the admin SPA), GNU `make` (optional).

```sh
# Run tests + vet + fmt
go test ./...
go vet ./...
gofmt -l .

# Build the binary
go build -o cairn ./cmd/cairn

# Build + run against the reference blog
./cairn build examples/blog -o _site

# Cross-compile (no CGO required)
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -o dist/cairn-linux-arm64        ./cmd/cairn
CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -o dist/cairn-darwin-arm64       ./cmd/cairn
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o dist/cairn-windows-amd64.exe  ./cmd/cairn
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full dev workflow, repo conventions, and how to add a new theme or plugin.

---

## Releases

Cairn follows a **one-active-release policy**: only one release exists at any time. Each new release deletes the previous one. The version is the build date in `DDMMYY` form (e.g., `v180526` for 18 May 2026).

Details in [`docs/release-policy.md`](docs/release-policy.md).

---

## Status

Active. The core build plane (`build`, `new`, `init`, the three themes, the 35-fixture markdown roundtrip, cross-platform determinism) is green; the admin editor (`serve`) ships with a working API but the Svelte SPA is iterating.

---

## License

MIT — see [LICENSE](LICENSE). You can build, distribute, modify, and use this software freely, including commercially; you must keep the copyright notice and the license text alongside the work. Contributions follow the same licensing.
