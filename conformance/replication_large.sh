#!/usr/bin/env bash
# Real >2 GiB and >5 GiB replication to Cairn and MinIO, with bounded fixture memory.
# The pinned MinIO executable is authenticated before execution, including cached copies.
# Usage: BIN=target/debug/cairn LARGE_WORK_ROOT=/path/with/space bash conformance/replication_large.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
MINIO_VERSION=RELEASE.2025-09-07T16-13-09Z
case "$(uname -m)" in
  x86_64) MINIO_ARCH=amd64; MINIO_SHA=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f ;;
  aarch64|arm64) MINIO_ARCH=arm64; MINIO_SHA=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d ;;
  *) echo 'FAIL: no pinned MinIO binary for this architecture' >&2; exit 1 ;;
esac
MINIO_CACHE="${MINIO_CACHE:-/tmp/cairn-minio-verified}"
MINIO="$MINIO_CACHE/minio-$MINIO_ARCH-$MINIO_VERSION"
mkdir -p "$MINIO_CACHE"
if [ ! -f "$MINIO" ]; then
  download="$(mktemp "$MINIO_CACHE/.download.XXXXXX")"
  trap 'rm -f "$download"' EXIT
  curl -fsSL -o "$download" "https://github.com/minio/minio/releases/download/$MINIO_VERSION/minio.linux-$MINIO_ARCH.$MINIO_VERSION"
  printf '%s  %s\n' "$MINIO_SHA" "$download" | sha256sum -c - >/dev/null
  chmod 0700 "$download"
  mv "$download" "$MINIO"
  trap - EXIT
fi
printf '%s  %s\n' "$MINIO_SHA" "$MINIO" | sha256sum -c - >/dev/null
[ -x "$BIN" ] || { echo "FAIL: Cairn binary missing: $BIN" >&2; exit 1; }
export LARGE_WORK_ROOT="${LARGE_WORK_ROOT:-$ROOT/target}"
mkdir -p "$LARGE_WORK_ROOT"
BIN="$BIN" MINIO="$MINIO" "${PY:-python3}" "$ROOT/conformance/replication_large.py" "$@"
