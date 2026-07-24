#!/usr/bin/env bash
# Conformance gate for object sharing (ARCH 15.8): persistent share tokens and
# interoperable SigV4 presigned URLs, exercised end to end against a freshly
# bootstrapped Cairn server with plain curl.
#
# Usage: BIN=target/debug/cairn conformance/share.sh
set -euo pipefail

BIN="${BIN:-target/debug/cairn}"
PORT="${PORT:-9078}"
DATA="$(mktemp -d)"
BASE="http://127.0.0.1:$PORT"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off
export CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

BOOT="$("$BIN" bootstrap)"
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $NF}')"
AUTH=(-H "Authorization: Bearer $BEARER")

"$BIN" serve &
SRV=$!
cleanup() { kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT

for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "$BASE/healthz" 2>/dev/null && break
  sleep 0.1
done

PASS=0
fail() { echo "FAIL: $1"; exit 1; }
ok() { PASS=$((PASS + 1)); echo "ok: $1"; }
# assert_status <expected> <actual> <label>
as() { [ "$2" = "$1" ] || fail "$3 (expected $1, got $2)"; ok "$3"; }

B=conf-share
API="$BASE/api/v1/buckets/$B/objects"
curl -s "${AUTH[@]}" -X POST "$BASE/api/v1/buckets" -H 'content-type: application/json' -d "{\"name\":\"$B\"}" -o /dev/null
printf 'v1-contents' | curl -s "${AUTH[@]}" -X PUT --data-binary @- "$BASE/$B/doc.txt" -o /dev/null

# --- persistent share: create -> fetch 200 + forced-download disposition ---
SH=$(curl -s "${AUTH[@]}" -X POST "$API/share" -H 'content-type: application/json' \
  -d '{"key":"doc.txt","expires_in_secs":3600,"disposition":"attachment","filename":"r.txt"}')
TOK=$(echo "$SH" | grep -oE '"token":"[^"]+"' | cut -d'"' -f4)
[ -n "$TOK" ] || fail "share create returned no token"
BODY=$(curl -s "$BASE/p/$TOK")
[ "$BODY" = "v1-contents" ] || fail "share fetch body mismatch ($BODY)"
DISP=$(curl -sI "$BASE/p/$TOK" | tr -d '\r' | awk -F': ' 'tolower($1)=="content-disposition"{print $2}')
[ "$DISP" = 'attachment; filename="r.txt"' ] || fail "disposition mismatch ($DISP)"
ok "persistent share fetch + forced-download disposition"

# --- forever share ---
FSH=$(curl -s "${AUTH[@]}" -X POST "$API/share" -H 'content-type: application/json' -d '{"key":"doc.txt"}')
echo "$FSH" | grep -q '"expires_at_ms":null' || fail "forever share should have null expiry"
FTOK=$(echo "$FSH" | grep -oE '"token":"[^"]+"' | cut -d'"' -f4)
as 200 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/p/$FTOK")" "forever share fetches 200"

# --- revoke -> 410; unknown -> 404 ---
curl -s "${AUTH[@]}" -X DELETE "$API/shares/$TOK" -o /dev/null
as 410 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/p/$TOK")" "revoked share -> 410"
as 404 "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/p/deadbeefdeadbeef")" "unknown token -> 404"

# --- anonymous mint -> 403 ---
as 403 "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/share" -H 'content-type: application/json' -d '{"key":"doc.txt"}')" "anonymous mint -> 403"

# --- presigned GET: mint -> fetch unauth 200 ---
PG=$(curl -s "${AUTH[@]}" -X POST "$API/presign" -H 'content-type: application/json' -d '{"key":"doc.txt","method":"GET","expires_in_secs":3600}')
PGURL=$(echo "$PG" | grep -oE '"url":"[^"]+"' | cut -d'"' -f4)
[ "$(curl -s "$PGURL")" = "v1-contents" ] || fail "presigned GET body mismatch"
ok "presigned GET fetch (unauthenticated)"

# --- presigned PUT: mint -> upload unauth 200 -> verify ---
PP=$(curl -s "${AUTH[@]}" -X POST "$API/presign" -H 'content-type: application/json' -d '{"key":"up.bin","method":"PUT","expires_in_secs":3600}')
PPURL=$(echo "$PP" | grep -oE '"url":"[^"]+"' | cut -d'"' -f4)
printf 'via-presigned-put' | curl -s -X PUT --data-binary @- -o /dev/null "$PPURL"
[ "$(curl -s "${AUTH[@]}" "$BASE/$B/up.bin")" = "via-presigned-put" ] || fail "presigned PUT object missing"
ok "presigned PUT upload (unauthenticated)"

# --- presigned >7d rejected at mint ---
as 400 "$(curl -s -o /dev/null -w '%{http_code}' "${AUTH[@]}" -X POST "$API/presign" -H 'content-type: application/json' -d '{"key":"doc.txt","method":"GET","expires_in_secs":700000}')" "presigned >7d -> 400 at mint"

# --- version pinning: pin v1, overwrite v2, pinned share still serves v1 ---
curl -s "${AUTH[@]}" -X PUT "$BASE/api/v1/buckets/$B/versioning" -H 'content-type: application/json' -d '{"status":"Enabled"}' -o /dev/null
printf 'pinned-v1' | curl -s "${AUTH[@]}" -X PUT --data-binary @- "$BASE/$B/ver.txt" -o /dev/null
VID=$(curl -sI "${AUTH[@]}" "$BASE/$B/ver.txt" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-amz-version-id"{print $2}')
[ -n "$VID" ] || fail "no version id returned for ver.txt"
PSH=$(curl -s "${AUTH[@]}" -X POST "$API/share" -H 'content-type: application/json' -d "{\"key\":\"ver.txt\",\"version_id\":\"$VID\"}")
PTOK=$(echo "$PSH" | grep -oE '"token":"[^"]+"' | cut -d'"' -f4)
printf 'NEW-v2' | curl -s "${AUTH[@]}" -X PUT --data-binary @- "$BASE/$B/ver.txt" -o /dev/null
[ "$(curl -s "$BASE/p/$PTOK")" = "pinned-v1" ] || fail "version-pinned share did not serve the pinned version"
ok "version-pinned share serves the pinned version after overwrite"

echo "ALL SHARE CONFORMANCE CHECKS PASSED ($PASS)"
