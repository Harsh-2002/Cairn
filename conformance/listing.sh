#!/usr/bin/env bash
# Listing / pagination / versioning conformance (package B+E): delimiter + CommonPrefixes (incl. a
# non-"/" and a multi-character delimiter, and the empty-but-present `delimiter=`), real token-fed
# pagination loops for ListObjectsV2, ListObjects v1 (Marker/NextMarker) and ListObjectVersions
# (KeyMarker/VersionIdMarker), StartAfter, EncodingType, UTF-8 ordering — and the versioning
# semantics: suspended null-version overwrite-in-place, delete markers, the canonical undelete,
# version-scoped GET/HEAD/DELETE/COPY and NoSuchVersion. See conformance/listing.py.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/listing.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9081}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off  # the harness only drives the S3 API; no console listener
# `export VAR="$(cmd)"` masks the command's exit status under `set -e` — split the assignment.
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true
  [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY (needs boto3)"
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by $PY"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
# Poll readiness — never a bare sleep (a timing-based wait is how these harnesses go flaky).
READY=""
for _ in $(seq 1 200); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null; then READY=1; break; fi
  kill -0 "$SRV" 2>/dev/null || { cat "$DATA/server.log" >&2; fail "server exited during startup"; }
  sleep 0.1
done
[ -n "$READY" ] || { cat "$DATA/server.log" >&2; fail "server never became healthy on :$PORT"; }

"$PY" "$(dirname "$0")/listing.py" "$AK" "$SK" "http://127.0.0.1:$PORT"
