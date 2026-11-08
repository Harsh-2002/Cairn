#!/usr/bin/env bash
# Fetch one release asset, preferring the authenticated GitHub release API when a token is
# available. GitHub's unauthenticated asset CDN has returned intermittent 503/504s; the API path
# is independent of it. Callers still verify their own pinned checksum before executing anything.
#
# Usage: fetch_github_release <owner/repo> <tag> <asset-name> <destination>
fetch_github_release() {
  local repo="$1" tag="$2" asset="$3" dest="$4" dir
  if [ -n "${GH_TOKEN:-}" ] && command -v gh >/dev/null 2>&1; then
    dir="$(mktemp -d)"
    if gh release download "$tag" --repo "$repo" --pattern "$asset" --dir "$dir" --clobber; then
      mv "$dir/$asset" "$dest" && rmdir "$dir" && return 0
    fi
    rm -rf "$dir"
  fi
  curl -fsSL -o "$dest" "https://github.com/$repo/releases/download/$tag/$asset"
}
