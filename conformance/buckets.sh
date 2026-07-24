#!/usr/bin/env bash
# Bucket-level operation surface conformance: CreateBucket idempotency, bucket-name validation,
# DeleteBucket / HeadBucket / GetBucketLocation status codes, the PUT->GET->DELETE->GET round-trip
# for every bucket config subresource, and a real unauthenticated CORS preflight.
#
# Standing guard: after EVERY config-subresource DELETE the harness re-asserts the BUCKET ITSELF
# still exists (HEAD -> 200) — the cheap regression net for the PR #1 subresource fall-through class
# where a DELETE on a `?subresource` reached the bare DELETE verb and destroyed the bucket.
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/buckets.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9084}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off  # the harness only drives the S3 port; no console listener
CAIRN_MASTER_KEY="$(openssl rand -hex 32)"   # split from `export`: `export X=$(cmd)` masks failure
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

"$PY" "$(dirname "$0")/buckets.py" "$AKID" "$SECRET" "http://127.0.0.1:$PORT"
