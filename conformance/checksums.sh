#!/usr/bin/env bash
# Modern-SDK flexible-checksum round-trip (ARCH 21.1): an SDK with default data-integrity protection
# (boto3 >=1.36, aws-cli v2, JS/Go/Java v2) sends a checksum on every PUT and validates the download
# by reading `x-amz-checksum-{algo}` off the response. This asserts Cairn computes, stores, and
# ECHOES the checksum on PUT and on GET/HEAD (mode=ENABLED), across CRC32/SHA256 (+ CRC32C/CRC64NVME
# when botocore[crt] is present), and never leaks it unprompted or on a Range read.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/checksums.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
PORT="${PORT:-9092}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
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
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

if "$PY" "$ROOT/conformance/checksums.py" "$AK" "$SK" "http://127.0.0.1:$PORT"; then
  echo "PASS: checksum round-trip conformance"
else
  fail "checksum round-trip assertions failed; server log: $(tail -5 "$DATA/server.log")"
fi
