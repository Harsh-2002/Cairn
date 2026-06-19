#!/usr/bin/env bash
# Multi-point durability crash regression (ARCH 8, F-4): crash at every blob-commit seam the build
# exposes (a plain PutObject and a multipart CompleteMultipartUpload) and assert reconcile reclaims
# the orphan and the object is absent — for each write path. Complements crash_consistency.sh (one
# seam). Requires a --features failpoints build (this script builds it unless SKIP_BUILD=1).
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/crash_multipoint.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
DATA="$(mktemp -d)"
cleanup() { rm -rf "$DATA"; }
trap cleanup EXIT

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  echo "  building cairn with --features failpoints"
  ( cd "$ROOT" && cargo build --bin cairn --features failpoints ) >/dev/null 2>&1 \
    || { echo "FAIL: cargo build --features failpoints failed"; exit 1; }
fi
[ -x "$BIN" ] || { echo "FAIL: binary not found: $BIN"; exit 1; }

BIN="$BIN" DATA="$DATA" PORT="${PORT:-9089}" "$PY" "$(dirname "$0")/crash_multipoint.py"
