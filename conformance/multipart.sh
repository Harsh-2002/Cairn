#!/usr/bin/env bash
# Multipart-upload lifecycle harness: drive the LOW-LEVEL create/upload-part/complete triple against
# a freshly-bootstrapped Cairn server and pin every session-state error code + the paging loops
# (issues #2/#3). See conformance/multipart.py.
# Usage: BIN=target/debug/cairn PY=python3 conformance/multipart.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9082}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off  # the harness tests the S3 API only; no web console listener
CAIRN_MASTER_KEY="$(openssl rand -hex 32)"  # split from the export: `export X="$(cmd)"` masks $?
export CAIRN_MASTER_KEY
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

BOOT="$("$BIN" bootstrap)"
AKID="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SECRET="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"

"$BIN" serve &
SRV=$!
cleanup() { kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT

for _ in $(seq 1 100); do
  curl -s -o /dev/null "http://127.0.0.1:$PORT/healthz" && break
  sleep 0.1
done

"$PY" "$(dirname "$0")/multipart.py" "$AKID" "$SECRET" "http://127.0.0.1:$PORT"
