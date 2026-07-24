#!/usr/bin/env bash
# Sendfile keep-alive engagement (ARCH 7.6, `fast-io`). The whole point of the keep-alive rewrite is
# that a POOLED client is served on the zero-copy `sendfile` path for EVERY request on a connection,
# not just the first. This is a deterministic regression (no warp needed): PUT one large object, then
# issue N GETs of it over a SINGLE keep-alive TCP connection (curl reuses the connection across the
# URLs in one invocation) and assert that `cairn_sendfile_get_total{result=ok}` rose by N — under the
# old peek-then-handoff it would rise by 1. Every body is verified byte-identical.
#
# Needs a `--features fast-io` binary on Linux; on any other build the fast path is compiled out and
# the engagement counter stays flat, so this is skipped there.
#
# Usage: BIN=target/debug/cairn bash conformance/sendfile_keepalive.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PORT="${PORT:-9100}"
N="${N:-20}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off
export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-warn}"
# 64 KiB floor; the test object is larger so it is above the floor and takes the fast path.
export CAIRN_FASTIO_MIN_BYTES="${CAIRN_FASTIO_MIN_BYTES:-65536}"

SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true; rm -rf "$DATA"; }
trap cleanup EXIT
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf '  ok: %s\n' "$*"; }

E="http://127.0.0.1:$PORT"
[ -x "$BIN" ] || fail "binary not found or not executable: $BIN"

BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $3}')"
[ -n "$BEARER" ] || fail "could not parse bootstrap bearer token"
H="Authorization: Bearer $BEARER"

"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "$E/healthz" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || fail "server exited at startup; log: $(cat "$DATA/server.log")"
  sleep 0.1
done

# If the fast path is compiled out (no `fast-io`), the engagement counter never appears — skip.
if ! curl -s "$E/metrics" | grep -q 'cairn_sendfile_'; then
  echo "SKIP: binary has no sendfile fast path (build with --features fast-io to exercise this)"
  exit 0
fi

# A 512 KiB incompressible object: eligible for the zero-copy path (uncompressed + unencrypted).
head -c 524288 /dev/urandom > "$DATA/big.bin"
curl -fsS -X PUT -H "$H" "$E/keepalive" >/dev/null || fail "create bucket failed"
curl -fsS -X PUT -H "$H" --data-binary @"$DATA/big.bin" "$E/keepalive/big.bin" >/dev/null || fail "put failed"
ok "populated a 512 KiB object"

scrape() { curl -s "$E/metrics" | awk '/^cairn_sendfile_get_total\{.*result="ok"/ {s+=$NF} END{print s+0}'; }
before="$(scrape)"

# N GETs of the object in ONE curl invocation → ONE keep-alive TCP connection.
urls=""
for _ in $(seq 1 "$N"); do urls="$urls $E/keepalive/big.bin"; done
# shellcheck disable=SC2086
curl -fsS -H "$H" -o "$DATA/last.bin" $urls >/dev/null || fail "keep-alive GET burst failed"
cmp -s "$DATA/last.bin" "$DATA/big.bin" || fail "the GET body was not byte-identical"
ok "$N GETs over one keep-alive connection all returned byte-identical bodies"

after="$(scrape)"
delta=$((after - before))
[ "$delta" -eq "$N" ] || fail "expected $N sendfile engagements on one connection, got $delta (a regression: the connection is not being kept alive across GETs)"
ok "every one of the $N GETs engaged the sendfile path on the single connection (delta=$delta)"

echo "SENDFILE KEEP-ALIVE OK — pooled GETs engage the zero-copy path on every request, not just the first"
echo "PASS: sendfile keep-alive engagement holds ($N/$N on one connection)"
