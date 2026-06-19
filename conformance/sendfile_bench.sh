#!/usr/bin/env bash
# Plaintext sendfile(2) fast-path A/B benchmark for the `fast-io` GET takeover (ARCH 7.x / 30.2).
#
# The plaintext HTTP/1.1 object-GET fast path (crates/cairn-server/src/fast_get.rs) serves an
# uncompressed, unencrypted object file->socket with a single sendfile(2), bypassing the userspace
# body copy. This harness quantifies the win the only way that is meaningful — SERVER CPU PER GiB
# SENT at equal throughput — by driving a GET-heavy `warp` load and sampling the server process's
# CPU time (utime+stime from /proc/<pid>/stat) and the new sendfile counters from /metrics across
# the run.
#
# It is an A/B: the "fast" arm is a binary built WITH `--features fast-io`; the optional "base" arm
# is a binary built WITHOUT it (the unchanged userspace streamed path). The sendfile win is
# base_cpu_per_gib / fast_cpu_per_gib. Without a baseline binary it reports the fast arm alone plus
# the sendfile engage rate (how many GETs actually took the zero-copy path vs fell back, and why).
#
# IMPORTANT: a trustworthy number needs real hardware. On a small/shared box the absolute figures
# are noisy; the A/B RATIO and the engage rate are still informative. The objects must be stored
# UNCOMPRESSED at rest for the fast path to engage (it transfers on-disk bytes verbatim) — this
# harness uses incompressible random object bodies; if your bucket has compression enabled the
# engage rate will be ~0 and the report says so.
#
# Usage:
#   BIN=target/release/cairn-fastio conformance/sendfile_bench.sh
#   BIN=...-fastio BASELINE_BIN=target/release/cairn conformance/sendfile_bench.sh   # full A/B
#   OBJ_SIZE=64MiB OBJECTS=16 DURATION=30s CONCURRENT=8 BIN=... conformance/sendfile_bench.sh
#
# Build the two binaries:
#   cargo build --release --features fast-io --bin cairn && cp target/release/cairn /tmp/cairn-fastio
#   cargo build --release --bin cairn                    && cp target/release/cairn /tmp/cairn-base
#
# Exit status: 0 if every measured arm completed with zero warp operation errors; non-zero on any
# infrastructure failure or warp errors.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/cairn}"
BASELINE_BIN="${BASELINE_BIN:-}"
PORT="${PORT:-9085}"
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-sendfile-bench}"

# Large, incompressible objects, served from the page cache, are where zero-copy pays off. Defaults
# land a run in a few minutes; bump OBJ_SIZE/DURATION on real hardware for a stable number.
OBJ_SIZE="${OBJ_SIZE:-32MiB}"
OBJECTS="${OBJECTS:-16}"
CONCURRENT="${CONCURRENT:-8}"
DURATION="${DURATION:-20s}"

WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"
WARP="${WARP:-}"

note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# --- obtain warp (same pin/path as warp.sh) ---------------------------------------------------
if [ -z "$WARP" ]; then
  if command -v warp >/dev/null 2>&1; then
    WARP="$(command -v warp)"
  else
    WARP="$WARP_CACHE/warp"
    if [ ! -x "$WARP" ]; then
      note "downloading warp v$WARP_VERSION"
      mkdir -p "$WARP_CACHE"
      tgz="$WARP_CACHE/warp.tar.gz"
      curl -fsSL -o "$tgz" "$WARP_URL" || fail "could not download warp from $WARP_URL"
      tar -xzf "$tgz" -C "$WARP_CACHE" warp || fail "warp tarball had no 'warp' binary"
      chmod +x "$WARP"
      rm -f "$tgz"
    fi
  fi
fi
[ -x "$WARP" ] || fail "warp not found/executable: $WARP"

CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# Sum a /metrics counter family (all label sets) into a single float.
scrape_sum() { # <metric-name>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 ~ "^"m"(\\{|$| )" {s += $2} END {printf "%.0f", s+0}'
}

# Process CPU seconds = (utime + stime) / CLK_TCK, summed over all threads (field 14, 15 of stat).
proc_cpu_secs() { # <pid>
  awk -v hz="$CLK_TCK" '{print ($14 + $15) / hz}' "/proc/$1/stat" 2>/dev/null || echo 0
}

# --- one measured arm -------------------------------------------------------------------------
run_arm() { # <label> <bin>
  local label="$1" bin="$2"
  [ -x "$bin" ] || fail "$label binary not found/executable: $bin"

  local data; data="$(mktemp -d)"
  export CAIRN_DATA_DIR="$data/data"
  export CAIRN_DB_PATH="$data/data/cairn.db"
  export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
  export CAIRN_UI_ADDR=off
  export CAIRN_MASTER_KEY; CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
  export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"

  local boot akid secret srv
  boot="$("$bin" bootstrap)" || fail "$label bootstrap failed"
  akid="$(echo "$boot" | awk '/Access Key Id/ {print $NF}')"
  secret="$(echo "$boot" | awk '/Secret Access Key/ {print $NF}')"
  [ -n "$akid" ] && [ -n "$secret" ] || fail "$label could not parse bootstrap credentials"

  "$bin" serve >"$data/server.log" 2>&1 &
  srv=$!

  local started=0 _
  for _ in $(seq 1 100); do
    if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null; then started=1; break; fi
    kill -0 "$srv" 2>/dev/null || { cat "$data/server.log"; fail "$label server exited at startup"; }
    sleep 0.1
  done
  [ "$started" -eq 1 ] || { cat "$data/server.log"; fail "$label server never became healthy"; }

  local common=(--host "127.0.0.1:$PORT" --access-key "$akid" --secret-key "$secret"
                --region "$REGION" --bucket "$BUCKET" --obj.size "$OBJ_SIZE"
                --objects "$OBJECTS" --concurrent "$CONCURRENT" --noclear)

  # Prepare the object set first (PUTs), OUTSIDE the CPU window, so the sample isolates GET cost.
  note "$label: preparing $OBJECTS x $OBJ_SIZE objects"
  ( cd "$data" && "$WARP" get "${common[@]}" --duration 1s ) >/dev/null 2>&1 || true

  # Sample CPU + counters strictly around the measured GET phase.
  local cpu0 sent0 ok0 fb0
  cpu0="$(proc_cpu_secs "$srv")"
  sent0="$(scrape_sum cairn_bytes_sent_total)"
  ok0="$(scrape_sum cairn_sendfile_get_total)"
  fb0="$(scrape_sum cairn_sendfile_fallback_total)"

  local report
  report="$(cd "$data" && "$WARP" get "${common[@]}" --duration "$DURATION" 2>&1)" || true

  local cpu1 sent1 ok1 fb1
  cpu1="$(proc_cpu_secs "$srv")"
  sent1="$(scrape_sum cairn_bytes_sent_total)"
  ok1="$(scrape_sum cairn_sendfile_get_total)"
  fb1="$(scrape_sum cairn_sendfile_fallback_total)"

  kill "$srv" 2>/dev/null || true; wait "$srv" 2>/dev/null || true
  rm -rf "$data"

  local errs
  errs="$(printf '%s\n' "$report" | sed -n 's/^Errors: \([0-9]*\).*/\1/p' | tail -1)"; [ -z "$errs" ] && errs=0

  # Report. CPU-per-GiB is the headline; engage rate shows how many GETs took the zero-copy path.
  awk -v label="$label" -v cpu0="$cpu0" -v cpu1="$cpu1" -v s0="$sent0" -v s1="$sent1" \
      -v ok0="$ok0" -v ok1="$ok1" -v fb0="$fb0" -v fb1="$fb1" -v errs="$errs" 'BEGIN {
    cpu = cpu1 - cpu0; gib = (s1 - s0) / (1024*1024*1024);
    ok = ok1 - ok0; fb = fb1 - fb0; tot = ok + fb;
    printf "\n=== %s ===\n", label;
    printf "  bytes sent:        %.2f GiB\n", gib;
    printf "  server CPU:        %.2f s\n", cpu;
    if (gib > 0) printf "  CPU per GiB:       %.3f s/GiB\n", cpu / gib;
    printf "  sendfile GETs:     %d ok, %d fell back", ok, fb;
    if (tot > 0) printf "  (engage %.1f%%)", 100.0 * ok / tot;
    printf "\n  warp op errors:    %d\n", errs;
    # Stash the headline for the A/B summary.
    printf "%s %.6f %.6f\n", label, (gib>0 ? cpu/gib : 0), (tot>0 ? 100.0*ok/tot : 0) > "/tmp/sendfile_bench.$label";
  }'
  [ "$errs" -eq 0 ] || fail "$label: warp reported $errs operation errors"
}

run_arm fast "$BIN"
if [ -n "$BASELINE_BIN" ]; then
  run_arm base "$BASELINE_BIN"
  # A/B ratio: how many times fewer CPU-seconds per GiB the sendfile arm spends.
  fast_cpg="$(awk '{print $2}' /tmp/sendfile_bench.fast 2>/dev/null || echo 0)"
  base_cpg="$(awk '{print $2}' /tmp/sendfile_bench.base 2>/dev/null || echo 0)"
  awk -v f="$fast_cpg" -v b="$base_cpg" 'BEGIN {
    printf "\n=== A/B ===\n";
    if (f > 0 && b > 0) printf "  CPU/GiB: base %.3f vs fast %.3f -> %.2fx less CPU on the sendfile path\n", b, f, b / f;
    else print "  (insufficient data for a ratio; check engage rate and object compression)";
  }'
  rm -f /tmp/sendfile_bench.fast /tmp/sendfile_bench.base
fi

printf '\nDone. Interpretation + caveats: docs/benchmarks.md.\n'
