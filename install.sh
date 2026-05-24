#!/bin/sh
# Cairn installer. Detects OS + arch, fetches the latest binary from
# GitHub Releases, verifies SHA256, installs to $CAIRN_INSTALL_DIR
# (defaults to ~/.local/bin).
#
#   curl -fsSL https://github.com/Harsh-2002/Cairn/raw/main/install.sh | sh
#
# Environment overrides:
#   CAIRN_INSTALL_DIR   target install directory (default ~/.local/bin)
#   CAIRN_VERSION       specific tag to install (default: latest)
#   CAIRN_REPO          repo slug (default Harsh-2002/Cairn)
#
# Following the one-active-release policy in docs/release-policy.md,
# "latest" is the only released version at any time.

set -eu

REPO="${CAIRN_REPO:-Harsh-2002/Cairn}"
INSTALL_DIR="${CAIRN_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${CAIRN_VERSION:-latest}"

say() { printf '%s\n' "$*"; }
err() { printf 'error: %s\n' "$*" >&2; exit 1; }

require() { command -v "$1" >/dev/null 2>&1 || err "$1 is required"; }
require curl
require tar
require uname
require shasum 2>/dev/null || require sha256sum

uname_s=$(uname -s)
uname_m=$(uname -m)

case "$uname_s" in
  Linux)  os=linux ;;
  Darwin) os=darwin ;;
  *) err "unsupported OS: $uname_s (Cairn supports Linux, macOS)" ;;
esac

# Go's GOARCH names: amd64 (x86_64) and arm64 (aarch64).
case "$uname_m" in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) err "unsupported arch: $uname_m" ;;
esac

asset="cairn-${os}-${arch}.tar.gz"

if [ "$VERSION" = "latest" ]; then
  url="https://github.com/${REPO}/releases/latest/download/${asset}"
  sums_url="https://github.com/${REPO}/releases/latest/download/SHA256SUMS"
else
  url="https://github.com/${REPO}/releases/download/${VERSION}/${asset}"
  sums_url="https://github.com/${REPO}/releases/download/${VERSION}/SHA256SUMS"
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

say "Downloading $asset from $url"
curl -fsSL -o "$tmp/$asset" "$url"
curl -fsSL -o "$tmp/SHA256SUMS" "$sums_url"

say "Verifying SHA256"
expected=$(grep "  ${asset}\$" "$tmp/SHA256SUMS" | awk '{print $1}')
[ -n "$expected" ] || err "could not find checksum for $asset"

if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$asset" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$tmp/$asset" | awk '{print $1}')
fi

[ "$expected" = "$actual" ] || err "SHA256 mismatch (expected $expected, got $actual)"

say "Extracting"
tar -xzf "$tmp/$asset" -C "$tmp"

mkdir -p "$INSTALL_DIR"
mv "$tmp/cairn" "$INSTALL_DIR/cairn"
chmod +x "$INSTALL_DIR/cairn"

say ""
say "Cairn installed to $INSTALL_DIR/cairn"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) say "Add $INSTALL_DIR to PATH to use the command from any shell." ;;
esac
say "Try: cairn --help"
