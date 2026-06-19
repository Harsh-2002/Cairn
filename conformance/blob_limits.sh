#!/usr/bin/env bash
# Blob-store limit regression (ARCH 9): out-of-space (507 on a size-constrained tmpfs, then the
# store stays healthy and a fitting write still works), a huge object round-tripping byte-identical,
# and many tiny objects listed across pages. Part A mounts a small tmpfs (needs passwordless sudo,
# which GitHub-hosted runners provide). Stdlib-only Python.
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/blob_limits.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
DATA="$(mktemp -d)"
cleanup() { sudo umount "$DATA/tmpfs" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT

[ -x "$BIN" ] || { echo "FAIL: binary not found: $BIN (build it: cargo build --bin cairn)"; exit 1; }
sudo -n true 2>/dev/null || { echo "FAIL: this harness needs passwordless sudo (to mount a tmpfs for the out-of-space test)"; exit 1; }

BIN="$BIN" DATA="$DATA" PORT="${PORT:-9090}" TMPFS_MB="${TMPFS_MB:-48}" HUGE_MB="${HUGE_MB:-128}" MANY="${MANY:-1100}" \
  "$PY" "$(dirname "$0")/blob_limits.py"
