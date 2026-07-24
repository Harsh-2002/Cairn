#!/usr/bin/env bash
# Auth, tenancy, and credential regression harness (ARCH 14–15): boot a real cairn binary, mint a
# SECOND non-admin identity through the management API, and prove the authentication and
# authorization boundaries hold with the EXACT S3 error codes AWS specifies:
#
#   * tenant isolation — a member's ListBuckets never enumerates another tenant's buckets, and
#     GET/PUT/DELETE/LIST against a bucket it does not own are AccessDenied 403;
#   * AccessDenied takes precedence over NoSuchKey — a 404 for a key in someone else's bucket
#     would leak whether that key exists;
#   * SigV4 failure modes — bad signature, unknown access key, skewed clock — each pinned to its
#     own code, crafted with hand-signed raw requests so the harness controls the signature;
#   * presigned URLs — redeemable without credentials, and REJECTED after expiry (the only
#     revocation mechanism a presigned bearer credential has);
#   * anonymous access — denied by default, allowed exactly as far as a public bucket policy
#     grants, and shut off again by Block Public Access.
#
# The web console/management listener must be ON: minting the second identity is `POST /api/v1/users`.
#
# Usage: BIN=target/debug/cairn PY=/path/to/python-with-boto3 conformance/authz.sh
#        STRICT_GAPS=1 ... conformance/authz.sh   # treat known S3-spec deviations as failures
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9085}"
UIPORT="${UIPORT:-$((PORT + 1))}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
# The console listener stays ON: the harness mints its second tenant via POST /api/v1/users.
export CAIRN_WEB_ADDR="127.0.0.1:$UIPORT"
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

# `export VAR="$(cmd)"` masks the command's exit status under `set -e`; keep them split.
BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
# The management API is Bearer-authenticated with a dedicated `cairn_<id>.<secret>` token, printed
# on the bootstrap "Authorization: Bearer …" line (distinct from the SigV4 pair used for S3).
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $3}')"
[ -n "$AK" ] && [ -n "$SK" ] && [ -n "$BEARER" ] || fail "could not parse bootstrap credentials"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done
# The management listener is a separate socket; poll it too before the driver posts a user.
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null -H "authorization: Bearer $BEARER" \
    "http://127.0.0.1:$UIPORT/api/v1/users" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

"$PY" "$(dirname "$0")/authz.py" "$AK" "$SK" \
  "http://127.0.0.1:$PORT" "http://127.0.0.1:$UIPORT" "$BEARER"

echo "PASS: tenant isolation, SigV4 failure modes, presigned expiry, and anonymous access enforced"
