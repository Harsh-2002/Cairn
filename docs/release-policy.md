# Cairn Release Policy

## One active release at a time

Only **one** release exists in [Releases](https://github.com/Harsh-2002/Cairn/releases) at any given moment. When a new release is published, the previous one is deleted by the release workflow. This is deliberate — Cairn ships a single binary that auto-upgrades, and keeping a long release history complicates the upgrade path and the "where do I get it" answer.

If you need a specific historic build, you have the git tag and the source — `go install github.com/Harsh-2002/Cairn/cmd/cairn@v<ddmmyy>` reproduces any past release byte-identically (Invariant 6).

## Version scheme: DDMMYY

The tag and release name are the UTC build date in `DDMMYY` form, prefixed with `v`. Examples:

| Date          | Tag       |
| ------------- | --------- |
| 18 May 2026   | `v180526` |
| 1 June 2026   | `v010626` |
| 31 Dec 2026   | `v311226` |

A given calendar day can produce **at most one** release. Re-running the release workflow on the same day overwrites the binary artifacts on that day's release (the tag is force-pushed; the release is recreated).

## What's in a release

Each release ships:

- `cairn-linux-x86_64.tar.gz` — **statically linked against musl libc**, no glibc dependency, runs on any Linux distribution (Alpine, Debian, Ubuntu, RHEL, …) of the same architecture.
- `cairn-linux-aarch64.tar.gz` — same, ARM64.
- `cairn-macos-x86_64.tar.gz` — links libSystem dynamically (Apple ABI requirement), every other dependency statically linked. Runs on any macOS of the same major version.
- `cairn-macos-aarch64.tar.gz` — same, Apple Silicon.
- `cairn-windows-x86_64.zip` — built with `target-feature=+crt-static`, no MSVC runtime DLL dependency.
- `SHA256SUMS` covering all of the above.
- A Docker image at `ghcr.io/harsh-2002/cairn:latest` and `ghcr.io/harsh-2002/cairn:v<DDMMYY>` (multi-arch: `linux/amd64` and `linux/arm64`), built `FROM scratch` plus the musl binary and ca-certificates only.

The Linux musl binaries are verified during the build with `ldd` to confirm "not a dynamic executable" or "statically linked"; the workflow fails the matrix if a dynamic dependency creeps in.

## How releases are produced

`.github/workflows/release.yml` handles the full flow:

1. **Trigger:** `workflow_dispatch` (manual) — the maintainer runs it from the Actions tab. No tag push trigger because the tag itself is computed from the current UTC date.
2. **Compute tag:** the workflow's first step calculates `v$(date -u +%d%m%y)`.
3. **Delete the previous release and tag** via `gh release delete --yes` and `git push --delete origin v…`. The workflow keeps a record of which tag it deleted in the run logs.
4. **Build** the binary on the matrix of platforms (Ubuntu x86_64, Ubuntu ARM64, macOS ARM64, macOS x86_64, Windows x86_64). Each job uploads its tarball as an artifact.
5. **Build the Docker image** (linux/amd64 + linux/arm64), tag with both `latest` and the version, push to `ghcr.io/harsh-2002/cairn`.
6. **Create the new release** with the freshly built artifacts and SHA256SUMS. Mark it as latest.

## Auto-upgrade

`cairn upgrade` checks `https://api.github.com/repos/Harsh-2002/Cairn/releases/latest` and compares the published `DDMMYY` against the binary's compiled-in version. If newer, it downloads the appropriate platform tarball, verifies the SHA256, and replaces the running binary in place atomically.

No telemetry, no automatic check — the upgrade only happens when the user runs the subcommand.
