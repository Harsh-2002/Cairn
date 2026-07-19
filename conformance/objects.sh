#!/usr/bin/env bash
# Object payload / header / range conformance (package F): the six stored system response headers
# (ARCH 13.4) and their `aws-chunked` normalization, `x-amz-meta-*` round-trip, the CopyObject
# metadata/tagging directives, `response-*` overrides on GET AND HEAD, single-part ETag shape,
# zero-byte objects, Range edge cases with exact 206/416 statuses, keys that need URL encoding
# (space / unicode / emoji / + / % / & / 1024-byte), Content-MD5, and DeleteObjects
# quiet/partial-failure/duplicate-key behavior. Every assertion pins an exact S3 code AND status.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/objects.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9083}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off  # pure S3 data plane; no console needed
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

# `export VAR="$(cmd)"` masks the command's exit status under `set -e` — keep them split.
BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
# Poll for readiness — never sleep-and-hope.
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done
curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null \
  || fail "server never became healthy; log: $(cat "$DATA/server.log")"

if "$PY" "$ROOT/conformance/objects.py" "$AK" "$SK" "http://127.0.0.1:$PORT"; then
  echo "PASS: object payload / header / range conformance"
else
  fail "object payload/header/range assertions failed; server log: $(tail -20 "$DATA/server.log")"
fi
