#!/usr/bin/env bash
# Browser error-page content negotiation (ARCH 25.1.1). Boot a real cairn binary and prove, over the
# wire, that the readable HTML page is served to browsers and ONLY to browsers:
#
#   * an SDK-shaped request (no Accept, or `*/*`) keeps the byte-identical `application/xml`
#     `<Error><Code>…` document — this is the invariant every `<Code>`-parsing assertion in the rest
#     of conformance/ depends on, and the one a loosened predicate would silently break;
#   * a browser navigation (Accept: text/html + Sec-Fetch-Dest: document, or the plain-HTTP
#     Upgrade-Insecure-Requests form) gets `text/html` with the failure explained;
#   * a browser SUBRESOURCE load (`<img>`: Sec-Fetch-Dest: image) keeps XML — a broken image must
#     never receive an HTML body;
#   * `Accept: text/htmlx` is not `text/html` (token match, not prefix);
#   * a non-browser that happens to send text/html (java.net.HttpURLConnection's hardcoded Accept,
#     which carries no Fetch Metadata and no Upgrade-Insecure-Requests) keeps XML;
#   * HEAD never grows a body;
#   * both branches advertise `Vary`, so a shared cache cannot serve one client class the other's
#     body shape;
#   * the HTML branch carries the hardening headers (`nosniff`, `default-src 'none'` CSP) and does
#     NOT echo a share token.
#
# Usage: BIN=target/debug/cairn bash conformance/error_pages.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9114}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR="off"
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }

S3="http://127.0.0.1:$PORT"
HDR="$DATA/h"; BODY="$DATA/b"

# Issue a request, capturing headers and body. Extra curl args are passed through.
req() {
  local method="$1"; shift
  local url="$1"; shift
  curl -sS -o "$BODY" -D "$HDR" -X "$method" "$@" "$url" -w '%{http_code}'
}
ctype() { grep -i '^content-type:' "$HDR" | tail -1 | tr -d '\r' | sed 's/^[Cc]ontent-[Tt]ype: *//'; }
hdr()   { grep -i "^$1:" "$HDR" | tail -1 | tr -d '\r' | sed "s/^[^:]*: *//"; }

"$BIN" serve &
SRV=$!
for _ in $(seq 1 60); do curl -fsS "$S3/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
curl -fsS "$S3/healthz" >/dev/null 2>&1 || fail "server did not become healthy"

MISSING="$S3/no-such-bucket-xyz/some/key.txt"

# ---- 1. machine clients keep XML, byte-identical ------------------------------------------------
code="$(req GET "$MISSING")"
[ "$code" = "404" ] || fail "no-Accept request: expected 404, got $code"
case "$(ctype)" in application/xml*) ;; *) fail "no-Accept request must stay XML, got '$(ctype)'";; esac
grep -q '<Code>NoSuchBucket</Code>' "$BODY" || fail "no-Accept request lost its <Code> element"
ok "no Accept header -> application/xml with <Code>"

code="$(req GET "$MISSING" -H 'Accept: */*')"
case "$(ctype)" in application/xml*) ;; *) fail "Accept:*/* must stay XML, got '$(ctype)'";; esac
grep -q '<Code>NoSuchBucket</Code>' "$BODY" || fail "Accept:*/* lost its <Code> element"
ok "Accept: */* -> application/xml with <Code>"

# The exact byte-for-byte shape SDKs parse must not drift between the two machine forms.
req GET "$MISSING" >/dev/null; a="$(sed 's/<RequestId>[^<]*<\/RequestId>//' "$BODY")"
req GET "$MISSING" -H 'Accept: */*' >/dev/null; b="$(sed 's/<RequestId>[^<]*<\/RequestId>//' "$BODY")"
[ "$a" = "$b" ] || fail "XML body differs between no-Accept and Accept:*/*"
ok "machine XML body identical across SDK-shaped requests"

# ---- 2. a real browser navigation gets the page --------------------------------------------------
code="$(req GET "$MISSING" -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9' \
  -H 'Sec-Fetch-Dest: document' -H 'Upgrade-Insecure-Requests: 1')"
[ "$code" = "404" ] || fail "browser navigation: expected 404, got $code"
case "$(ctype)" in text/html*) ;; *) fail "browser navigation must get text/html, got '$(ctype)'";; esac
grep -qi 'Bucket not found' "$BODY" || fail "page does not explain the failure"
grep -q '<Code>' "$BODY" && fail "page must not be the raw XML document"
ok "browser navigation (https shape) -> text/html page"

# Plain-HTTP browsers send no Sec-Fetch-* at all; Upgrade-Insecure-Requests is the only signal left.
code="$(req GET "$MISSING" -H 'Accept: text/html,application/xhtml+xml' -H 'Upgrade-Insecure-Requests: 1')"
case "$(ctype)" in text/html*) ;; *) fail "plain-http browser navigation must get text/html, got '$(ctype)'";; esac
ok "browser navigation (plain-http shape, no Sec-Fetch) -> text/html page"

# ---- 3. everything browser-adjacent that is NOT a navigation keeps XML ----------------------------
req GET "$MISSING" -H 'Accept: text/html' -H 'Sec-Fetch-Dest: image' >/dev/null
case "$(ctype)" in application/xml*) ;; *) fail "<img> subresource must stay XML, got '$(ctype)'";; esac
ok "Sec-Fetch-Dest: image (a broken <img>) -> stays XML"

req GET "$MISSING" -H 'Accept: text/html' -H 'Sec-Fetch-Dest: empty' >/dev/null
case "$(ctype)" in application/xml*) ;; *) fail "fetch()/XHR must stay XML, got '$(ctype)'";; esac
ok "Sec-Fetch-Dest: empty (fetch/XHR) -> stays XML"

req GET "$MISSING" -H 'Accept: text/htmlx' -H 'Upgrade-Insecure-Requests: 1' >/dev/null
case "$(ctype)" in application/xml*) ;; *) fail "text/htmlx is not text/html, got '$(ctype)'";; esac
ok "Accept: text/htmlx -> stays XML (token match, not prefix)"

# java.net.HttpURLConnection's hardcoded Accept: text/html but no Fetch Metadata, no UIR.
req GET "$MISSING" -H 'Accept: text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2' >/dev/null
case "$(ctype)" in application/xml*) ;; *) fail "bare java URLConnection must stay XML, got '$(ctype)'";; esac
ok "java.net.HttpURLConnection Accept -> stays XML"

# ---- 4. HEAD never grows a body ------------------------------------------------------------------
# `--head` (not `-X HEAD`) so curl expects the body-less response HEAD is defined to return, and
# `size_download` rather than the output file, because with --head curl writes the headers there.
read -r code size <<<"$(curl -sS --head -o /dev/null -D "$HDR" -w '%{http_code} %{size_download}' \
  -H 'Accept: text/html' -H 'Sec-Fetch-Dest: document' -H 'Upgrade-Insecure-Requests: 1' "$MISSING")"
[ "$code" = "404" ] || fail "HEAD: expected 404, got $code"
[ "$size" = "0" ] || fail "HEAD returned a body ($size bytes)"
case "$(ctype)" in text/html*) fail "HEAD must not switch to the HTML shape";; esac
ok "HEAD stays body-less and machine-shaped even with a browser Accept"

# ---- 5. caching + hardening ----------------------------------------------------------------------
req GET "$MISSING" -H 'Accept: text/html' -H 'Sec-Fetch-Dest: document' >/dev/null
v="$(hdr vary)"; case "$v" in *accept*) ;; *) fail "HTML branch missing Vary: accept (got '$v')";; esac
[ "$(hdr x-content-type-options)" = "nosniff" ] || fail "HTML branch missing nosniff"
case "$(hdr content-security-policy)" in *"default-src 'none'"*) ;; *) fail "HTML branch missing CSP";; esac
ok "HTML branch: Vary + nosniff + default-src 'none' CSP"

req GET "$MISSING" >/dev/null
v="$(hdr vary)"; case "$v" in *accept*) ;; *) fail "XML branch missing Vary: accept (got '$v')";; esac
ok "XML branch advertises Vary too (a cache cannot mix the two shapes)"

# ---- 6. a share token is never echoed into the page -----------------------------------------------
TOKEN="deadbeefcafebabe0123456789abcdef"
req GET "$S3/share/$TOKEN" -H 'Accept: text/html' -H 'Sec-Fetch-Dest: document' >/dev/null
case "$(ctype)" in text/html*) ;; *) fail "share error must render a page for a browser, got '$(ctype)'";; esac
grep -q "$TOKEN" "$BODY" && fail "the share token leaked into the error page"
ok "share error renders a page and never echoes the token"

printf 'PASS: browser error-page negotiation (ARCH 25.1.1)\n'
