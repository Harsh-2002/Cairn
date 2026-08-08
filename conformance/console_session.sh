#!/usr/bin/env bash
# Console session-cookie regression (audit: clear-text token storage). Boot a real cairn binary with
# the web-console listener on and prove the httpOnly session-cookie + disjoint-origin transfer flow
# end to end with plain curl (no SDK):
#   * POST /api/v1/session requires the exact control Origin, then returns 200 + a
#     `cairn_session` Set-Cookie for valid admin credentials.
#   * the cookie alone (no Authorization header) authenticates only the management API.
#   * every cookie-authenticated mutation proves its exact control Origin; same-site data-origin
#     content and a missing Origin are denied, while explicit Bearer clients remain compatible.
#   * control-listener object paths and data-listener `/api/v1` paths are fail-closed 404s.
#   * the management API mints an exact data-origin SigV4 URL backed by a scoped temporary session;
#     a browser-shaped CORS preflight and PUT/GET round-trip succeed only for the signed origin.
#   * object/share responses cannot install a directory-scoped service worker on the data origin.
#   * GET /api/v1/session reports the identity; with no cookie it is 401.
#   * a wrong secret is refused 401; DELETE /api/v1/session clears the cookie so the API is locked
#     out again.
#
# Usage: BIN=target/debug/cairn bash conformance/console_session.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9096}"
WEBPORT="${WEBPORT:-9097}"
DATA="$(mktemp -d)"
JAR="$DATA/cookies.txt"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR="127.0.0.1:$WEBPORT"
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() {
  if [ -n "$SRV" ]; then
    kill "$SRV" 2>/dev/null || true
    wait "$SRV" 2>/dev/null || true
  fi
  rm -rf "$DATA"
}
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }

WEB="http://127.0.0.1:$WEBPORT"
S3="http://127.0.0.1:$PORT"

# GET/POST/etc. helper: prints the HTTP status code; body (if any) goes to $BODY.
BODY="$DATA/body"
HEADERS="$DATA/headers"
code() { # code <curl args...>
  curl -s -o "$BODY" -w '%{http_code}' "$@"
}

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
if [ -z "$AK" ] || [ -z "$SK" ]; then
  fail "could not parse bootstrap credentials"
fi

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "$S3/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

# 1) No cookie yet: whoami is 401, and the management API is locked.
st="$(code "$WEB/api/v1/session")"
[ "$st" = "401" ] || fail "whoami without a cookie should be 401, got $st"
ok "whoami without a cookie is 401"

# 2) Sign in itself is origin-bound: neither an origin-less form nor active data-origin content can
# replace the victim's ambient console identity with attacker-chosen credentials.
st="$(code -D "$HEADERS" -X POST -H 'Content-Type: application/json' \
  -d "{\"access_key\":\"$AK\",\"secret_key\":\"$SK\"}" "$WEB/api/v1/session")"
[ "$st" = "403" ] || fail "origin-less login should be 403, got $st (body: $(cat "$BODY"))"
grep -qi '^set-cookie:.*cairn_session' "$HEADERS" &&
  fail "origin-less login unexpectedly set a session cookie"
st="$(code -D "$HEADERS" -X POST -H 'Content-Type: application/json' -H "Origin: $S3" \
  -d "{\"access_key\":\"$AK\",\"secret_key\":\"$SK\"}" "$WEB/api/v1/session")"
[ "$st" = "403" ] || fail "data-origin login should be 403, got $st (body: $(cat "$BODY"))"
grep -qi '^set-cookie:.*cairn_session' "$HEADERS" &&
  fail "data-origin login unexpectedly set a session cookie"
ok "origin-less and same-site data-origin login CSRF is denied"

# The same-origin console login succeeds and saves the cookie to the jar.
st="$(code -c "$JAR" -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
  -d "{\"access_key\":\"$AK\",\"secret_key\":\"$SK\"}" "$WEB/api/v1/session")"
[ "$st" = "200" ] || fail "login should be 200, got $st (body: $(cat "$BODY"))"
grep -q 'cairn_session' "$JAR" || fail "login did not set a cairn_session cookie"
grep -q "$AK" "$BODY" || fail "login response should echo the access_key_id"
# The secret must never be echoed in the response body.
grep -q "$SK" "$BODY" && fail "login response leaked the secret in its body"
ok "login returns 200, sets cairn_session, echoes identity (not the secret)"

# 3) The cookie alone authenticates the management API (no Authorization header).
st="$(code -b "$JAR" "$WEB/api/v1/overview")"
[ "$st" = "200" ] || fail "cookie should authenticate /overview, got $st"
ok "cookie authenticates the management API"

st="$(code -b "$JAR" "$WEB/api/v1/session")"
[ "$st" = "200" ] || fail "whoami with the cookie should be 200, got $st"
ok "whoami with the cookie is 200"

# 4) The route matrices are disjoint in both directions.
st="$(code -b "$JAR" -X PUT "$WEB/conf-session/greeting.txt")"
[ "$st" = "404" ] || fail "control-listener object path should be 404, got $st"
ok "control listener does not fall through to S3"

st="$(code -b "$JAR" "$S3/api/v1/overview")"
[ "$st" = "404" ] || fail "data-listener management path should be 404, got $st"
ok "data listener rejects the management namespace and ignores the cookie"

# 5) SameSite is not an origin boundary: the two localhost ports are same-site. A simple
# cross-origin POST from active object content must not be able to spend the ambient control cookie.
st="$(code -b "$JAR" -X POST -H 'Content-Type: text/plain' -H "Origin: $S3" \
  -d '{"name":"csrf-data-origin"}' "$WEB/api/v1/buckets")"
[ "$st" = "403" ] ||
  fail "same-site data-origin cookie mutation should be 403, got $st (body: $(cat "$BODY"))"
st="$(code -b "$JAR" -X POST -H 'Content-Type: text/plain' \
  -d '{"name":"csrf-missing-origin"}' "$WEB/api/v1/buckets")"
[ "$st" = "403" ] ||
  fail "cookie mutation without Origin should be 403, got $st (body: $(cat "$BODY"))"
st="$(code -b "$JAR" "$WEB/api/v1/buckets")"
[ "$st" = "200" ] || fail "bucket list after CSRF probes should be 200, got $st"
grep -Eq 'csrf-data-origin|csrf-missing-origin' "$BODY" &&
  fail "a rejected CSRF probe created a bucket"
ok "same-site cross-origin and origin-less cookie mutations are denied without state change"

# An explicit Authorization client is not using ambient browser authority and remains compatible
# without an Origin header.
st="$(code -b "$JAR" -X POST -H "Authorization: Bearer $AK.$SK" \
  -H 'Content-Type: application/json' \
  -d '{"name":"explicit-client"}' "$WEB/api/v1/buckets")"
[ "$st" = "201" ] ||
  fail "explicit Bearer mutation without Origin should remain 201, got $st (body: $(cat "$BODY"))"
ok "explicit Authorization mutation remains origin-independent"

# 6) Create a bucket through management, then drive the console's real transfer shape:
# management presign -> browser preflight -> cross-origin S3 PUT -> cross-origin S3 GET.
st="$(code -b "$JAR" -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
  -d '{"name":"conf-session"}' "$WEB/api/v1/buckets")"
[ "$st" = "201" ] || fail "management CreateBucket should be 201, got $st (body: $(cat "$BODY"))"

# A browser removes literal and encoded dot segments before sending. Presigning any such key must
# fail rather than silently target a different object; direct SDK/CLI access remains available.
for unsafe_key in "." ".." "a/%2E/b" "a/%2e%2e/b" "a/%252E/b"; do
  st="$(code -b "$JAR" -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
    -d "{\"key\":\"$unsafe_key\",\"method\":\"GET\",\"expires_in_secs\":300,\"origin\":\"$WEB\"}" \
    "$WEB/api/v1/buckets/conf-session/objects/presign")"
  [ "$st" = "400" ] ||
    fail "dot-segment presign for '$unsafe_key' should be 400, got $st"
  grep -q 'direct S3 client or the Cairn CLI' "$BODY" ||
    fail "dot-segment error for '$unsafe_key' did not explain the SDK/CLI fallback"
done
ok "browser-normalized dot-segment keys fail closed at presign"

PUT_JSON="$(curl -sS -b "$JAR" -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
  -d "{\"key\":\"greeting.txt\",\"method\":\"PUT\",\"expires_in_secs\":300,\"origin\":\"$WEB\",\"headers\":[[\"content-type\",\"text/plain\"]]}" \
  "$WEB/api/v1/buckets/conf-session/objects/presign")"
PUT_URL="$(printf '%s' "$PUT_JSON" | grep -oE '"url":"[^"]+"' | cut -d'"' -f4)"
[ -n "$PUT_URL" ] || fail "console PUT presign returned no URL ($PUT_JSON)"
case "$PUT_URL" in "$S3"/conf-session/*) ;; *) fail "console presign was not on data origin: $PUT_URL";; esac
printf '%s' "$PUT_JSON" | grep -q '"session":' || fail "console presign returned no reusable session handle"
printf '%s' "$PUT_JSON" | grep -q "$SK" && fail "console presign leaked the administrator secret"

st="$(curl -sS -D "$HEADERS" -o "$BODY" -w '%{http_code}' -X OPTIONS \
  -H "Origin: $WEB" -H 'Access-Control-Request-Method: PUT' \
  -H 'Access-Control-Request-Headers: content-type' "$PUT_URL")"
[ "$st" = "200" ] || fail "matching console preflight should be 200, got $st"
grep -qi "^access-control-allow-origin: $WEB" "$HEADERS" ||
  fail "matching preflight did not allow the signed console origin"
ok "browser preflight is granted for the signed console origin"

st="$(curl -sS -D "$HEADERS" -o "$BODY" -w '%{http_code}' -X OPTIONS \
  -H 'Origin: http://attacker.invalid' -H 'Access-Control-Request-Method: PUT' \
  -H 'Access-Control-Request-Headers: content-type' "$PUT_URL")"
grep -qi '^access-control-allow-origin:' "$HEADERS" &&
  fail "wrong-origin preflight must not receive Access-Control-Allow-Origin"
ok "wrong-origin preflight is not granted (status $st)"

st="$(printf 'hello-from-presign' | curl -sS -D "$HEADERS" -o "$BODY" -w '%{http_code}' \
  -X PUT -H "Origin: $WEB" -H 'Content-Type: text/plain' --data-binary @- "$PUT_URL")"
[ "$st" = "200" ] || fail "console presigned PUT should be 200, got $st (body: $(cat "$BODY"))"
grep -qi "^access-control-allow-origin: $WEB" "$HEADERS" ||
  fail "actual PUT did not expose its response to the signed console origin"

GET_JSON="$(curl -sS -b "$JAR" -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
  -d "{\"key\":\"greeting.txt\",\"method\":\"GET\",\"expires_in_secs\":300,\"origin\":\"$WEB\"}" \
  "$WEB/api/v1/buckets/conf-session/objects/presign")"
GET_URL="$(printf '%s' "$GET_JSON" | grep -oE '"url":"[^"]+"' | cut -d'"' -f4)"
[ -n "$GET_URL" ] || fail "console GET presign returned no URL ($GET_JSON)"
st="$(curl -sS -D "$HEADERS" -o "$BODY" -w '%{http_code}' -H "Origin: $WEB" "$GET_URL")"
[ "$st" = "200" ] || fail "console presigned GET should be 200, got $st"
[ "$(cat "$BODY")" = "hello-from-presign" ] || fail "console presigned GET body mismatch"
grep -qi "^access-control-allow-origin: $WEB" "$HEADERS" ||
  fail "actual GET did not expose its response to the signed console origin"
grep -qi '^service-worker-allowed: /conf-session/greeting.txt/.cairn-service-worker-disabled/' "$HEADERS" ||
  fail "ordinary object response did not narrow its service-worker scope"
ok "cross-origin presigned PUT/GET round-trip succeeds without ambient credentials"

# A conforming browser marks a service-worker script request. Object data is never allowed to become
# a persistent network interceptor for the data origin.
st="$(curl -sS -o "$BODY" -w '%{http_code}' -H 'Service-Worker: script' "$GET_URL")"
[ "$st" = "403" ] ||
  fail "ordinary object service-worker fetch should be 403, got $st (body: $(cat "$BODY"))"

SHARE_JSON="$(curl -sS -b "$JAR" -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
  -d '{"key":"greeting.txt","expires_in_secs":3600,"disposition":"inline"}' \
  "$WEB/api/v1/buckets/conf-session/objects/shares")"
SHARE_URL="$(printf '%s' "$SHARE_JSON" | grep -oE '"url":"[^"]+"' | cut -d'"' -f4)"
[ -n "$SHARE_URL" ] || fail "persistent share mint returned no URL ($SHARE_JSON)"
SHARE_TOKEN="${SHARE_URL##*/}"
st="$(curl -sS -D "$HEADERS" -o "$BODY" -w '%{http_code}' "$SHARE_URL")"
[ "$st" = "200" ] || fail "persistent share GET should be 200, got $st"
[ "$(cat "$BODY")" = "hello-from-presign" ] || fail "persistent share body mismatch"
grep -qi "^service-worker-allowed: /share/$SHARE_TOKEN/.cairn-service-worker-disabled/" "$HEADERS" ||
  fail "public share response did not isolate its service-worker scope"
st="$(curl -sS -o "$BODY" -w '%{http_code}' -H 'Service-Worker: script' "$SHARE_URL")"
[ "$st" = "403" ] ||
  fail "public share service-worker fetch should be 403, got $st (body: $(cat "$BODY"))"
ok "object and share bytes cannot install a data-origin service worker"

# 7) A wrong secret is refused (and sets no cookie).
st="$(code -X POST -H 'Content-Type: application/json' -H "Origin: $WEB" \
  -d "{\"access_key\":\"$AK\",\"secret_key\":\"definitely-wrong\"}" "$WEB/api/v1/session")"
[ "$st" = "401" ] || fail "login with a wrong secret should be 401, got $st"
ok "login with a wrong secret is 401"

# 8) Logout clears the cookie; the management API is locked out again.
st="$(code -b "$JAR" -c "$JAR" -X DELETE -H "Origin: $WEB" "$WEB/api/v1/session")"
[ "$st" = "200" ] || fail "logout should be 200, got $st"
st="$(code -b "$JAR" "$WEB/api/v1/overview")"
[ "$st" != "200" ] || fail "after logout the cookie must no longer authenticate, got $st"
ok "logout clears the cookie; the API is locked out again ($st)"

echo "CONSOLE SESSION OK — origin-bound cookie mutations, disjoint listeners, scoped presign CORS transfer, service-worker isolation, sign-out"
echo "PASS: console listener/origin boundary holds end-to-end"
