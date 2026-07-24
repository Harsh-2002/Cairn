#!/usr/bin/env bash
# Console session-cookie regression (audit: clear-text token storage). Boot a real cairn binary with
# the web-console listener on and prove the httpOnly session-cookie auth flow end to end with plain curl
# (no SDK — the cookie flow is pure HTTP):
#   * POST /api/v1/session with the admin credential -> 200 + a `cairn_session` Set-Cookie.
#   * the cookie alone (no Authorization header) authenticates the management API AND the S3 data
#     plane on the web-console port (CreateBucket / PutObject / GetObject round-trip).
#   * GET /api/v1/session reports the identity; with no cookie it is 401.
#   * the cookie is REJECTED on the S3 data-plane port (:PORT) — cookies are not port-isolated, so
#     this proves the data plane never honors a console cookie (the key security property).
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
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }

WEB="http://127.0.0.1:$WEBPORT"
S3="http://127.0.0.1:$PORT"

# GET/POST/etc. helper: prints the HTTP status code; body (if any) goes to $BODY.
BODY="$DATA/body"
code() { # code <curl args...>
  curl -s -o "$BODY" -w '%{http_code}' "$@"
}

[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials"

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

# 2) Sign in: 200 + a cairn_session cookie saved to the jar.
st="$(code -c "$JAR" -X POST -H 'Content-Type: application/json' \
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

# 4) The cookie authenticates the S3 data plane on the web-console port: create bucket, put + get an object.
st="$(code -b "$JAR" -X PUT "$WEB/conf-session")"
[ "$st" = "200" ] || fail "CreateBucket via cookie should be 200, got $st (body: $(cat "$BODY"))"
st="$(code -b "$JAR" -X PUT --data-binary 'hello-from-cookie' "$WEB/conf-session/greeting.txt")"
[ "$st" = "200" ] || fail "PutObject via cookie should be 200, got $st (body: $(cat "$BODY"))"
st="$(code -b "$JAR" "$WEB/conf-session/greeting.txt")"
[ "$st" = "200" ] || fail "GetObject via cookie should be 200, got $st"
[ "$(cat "$BODY")" = "hello-from-cookie" ] || fail "GetObject body mismatch via cookie"
ok "cookie authenticates the S3 data plane (PUT/GET round-trip) on the web-console port"

# 5) SECURITY: the cookie is NOT honored on the S3 data-plane port. Cookies are not port-isolated,
#    so the same jar is sent to :PORT — the data plane (serve_web=false) must still refuse it.
st="$(code -b "$JAR" "$S3/api/v1/overview")"
[ "$st" != "200" ] || fail "the session cookie must NOT authenticate the S3 data-plane port"
ok "cookie is rejected on the S3 data-plane port ($st) — not port-isolated, correctly ignored"

# 6) A wrong secret is refused (and sets no cookie).
st="$(code -X POST -H 'Content-Type: application/json' \
  -d "{\"access_key\":\"$AK\",\"secret_key\":\"definitely-wrong\"}" "$WEB/api/v1/session")"
[ "$st" = "401" ] || fail "login with a wrong secret should be 401, got $st"
ok "login with a wrong secret is 401"

# 7) Logout clears the cookie; the management API is locked out again.
st="$(code -b "$JAR" -c "$JAR" -X DELETE "$WEB/api/v1/session")"
[ "$st" = "200" ] || fail "logout should be 200, got $st"
st="$(code -b "$JAR" "$WEB/api/v1/overview")"
[ "$st" != "200" ] || fail "after logout the cookie must no longer authenticate, got $st"
ok "logout clears the cookie; the API is locked out again ($st)"

echo "CONSOLE SESSION OK — httpOnly cookie auth: sign-in, cookie-authenticated API + S3, port isolation, sign-out"
echo "PASS: console session-cookie auth holds end-to-end"
