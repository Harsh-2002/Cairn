#!/usr/bin/env bash
# Cairn stress + throughput harness (ARCH 30) — the single "is it still fast AND stable?" check to
# run after a change. It drives `warp` (the MinIO S3 macro-benchmark) against a throwaway Cairn and
# reports, in one table: peak write/read/mixed throughput (obj/s + MiB/s), a concurrency-escalation
# ramp that proves the server bends-not-breaks past the single-writer ceiling (throughput plateaus,
# zero errors, stays alive), and server-side stability — RSS over the whole run (leak check) and the
# peak writer-queue depth. Emits a PASS/FAIL verdict.
#
# Unlike warp.sh (which only gates on the error count) this parses warp's throughput numbers and
# samples the server process, so the output is a usable benchmark + a stability assertion.
#
# Usage:
#   conformance/stress.sh                                  # release binary, default profile (~4 min)
#   BIN=target/release/cairn conformance/stress.sh
#   CONCURRENT=16 OBJ_SIZE=4MiB DURATION=15s conformance/stress.sh
#   LEVELS="4 8 16 32 64" conformance/stress.sh            # escalation ramp
#   STRESS_OUT=/tmp/cairn-stress.json conformance/stress.sh # write machine-readable results
#   BASELINE=/tmp/cairn-stress.json conformance/stress.sh   # compare vs a prior run, warn on regression
#
# Exit status: 0 only if every phase ran with ZERO operation errors, the server stayed alive, and
# RSS did not grow past LEAK_PCT. Non-zero otherwise. Absolute numbers are hardware-bound (on a small
# or shared box warp and cairn share CPU); the durable signals are the error count, the throughput
# plateau shape, and RSS stability — see docs/benchmarks.md.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/cairn}"
PORT="${PORT:-9090}"
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-stress}"

# Throughput-phase profile.
OBJ_SIZE="${OBJ_SIZE:-1MiB}"
CONCURRENT="${CONCURRENT:-8}"
DURATION="${DURATION:-10s}"
OBJECTS="${OBJECTS:-200}"        # pool size for get/mixed prepare
# Escalation profile: small objects maximise writer-commit pressure; ramp concurrency to saturation.
LEVELS="${LEVELS:-4 8 16 32 64}"
ESC_OBJ_SIZE="${ESC_OBJ_SIZE:-64KiB}"
ESC_DURATION="${ESC_DURATION:-6s}"
LEAK_PCT="${LEAK_PCT:-25}"       # fail if STEADY-STATE RSS (last third vs middle third) grows >this %
RSS_CEILING_KIB="${RSS_CEILING_KIB:-1048576}"  # absolute runaway backstop (default 1 GiB)
REGRESS_PCT="${REGRESS_PCT:-20}" # warn if a headline obj/s is more than this % below BASELINE

WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"
WARP="${WARP:-}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_UI_ADDR=off
export CAIRN_MASTER_KEY="$(openssl rand -hex 32)"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"

SRV=""
SAMPLER=""
cleanup() {
  [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null || true
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true
  [ -n "$SRV" ] && wait "$SRV" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT
note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# --- 0. warp binary (shared cache with warp.sh) ----------------------------------------------
if [ -z "$WARP" ]; then
  if command -v warp >/dev/null 2>&1; then WARP="$(command -v warp)"; else
    WARP="$WARP_CACHE/warp"
    if [ ! -x "$WARP" ]; then
      note "downloading warp v$WARP_VERSION"
      mkdir -p "$WARP_CACHE"; tgz="$WARP_CACHE/warp.tar.gz"
      curl -fsSL -o "$tgz" "$WARP_URL" || fail "could not download warp from $WARP_URL"
      tar -xzf "$tgz" -C "$WARP_CACHE" warp || fail "warp tarball had no 'warp' binary"
      chmod +x "$WARP"; rm -f "$tgz"
    fi
  fi
fi
[ -x "$WARP" ] || fail "warp not found or not executable: $WARP"

# --- 1. server -------------------------------------------------------------------------------
[ -x "$BIN" ] || fail "binary not found: $BIN (build it: cargo build --release --bin cairn)"
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
note "cairn (pid $SRV) healthy on 127.0.0.1:$PORT — $(uname -m), $(nproc) cores"

# --- 2. background sampler: RSS (KiB) + writer queue depth every 1s ---------------------------
SAMPLES="$DATA/samples.tsv"
(
  while kill -0 "$SRV" 2>/dev/null; do
    rss="$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' ')"
    wq="$(curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null | awk '/^cairn_writer_queue_depth/ {print $2; exit}')"
    printf '%s\t%s\n' "${rss:-0}" "${wq:-0}" >>"$SAMPLES"
    sleep 1
  done
) &
SAMPLER=$!

common=(--host "127.0.0.1:$PORT" --access-key "$AK" --secret-key "$SK"
        --region "$REGION" --bucket "$BUCKET" --concurrent "$CONCURRENT" --noclear)
total_errors=0
declare -A OBJS MIBS

# Run one warp op, print its throughput, accumulate errors, capture obj/s + MiB/s into OBJS/MIBS.
run_phase() {
  local label="$1" op="$2"; shift 2
  local out rc
  out="$(cd "$DATA" && "$WARP" "$op" "${common[@]}" --obj.size "$OBJ_SIZE" "$@" 2>&1)"; rc=$?
  local objs mibs errs raw
  # put/get report `* Average: X MiB/s, Y obj/s`; mixed reports `Cluster Total: X MiB/s, Y obj/s`.
  objs="$(printf '%s\n' "$out" | sed -n 's/.*Average:[^,]*, \([0-9.]*\) obj\/s.*/\1/p' | head -1)"
  [ -z "$objs" ] && objs="$(printf '%s\n' "$out" | sed -n 's/.*Cluster Total:[^,]*, \([0-9.]*\) obj\/s.*/\1/p' | head -1)"
  mibs="$(printf '%s\n' "$out" | sed -n 's/.*Average: \([0-9.]*\) MiB\/s.*/\1/p' | head -1)"
  [ -z "$mibs" ] && mibs="$(printf '%s\n' "$out" | sed -n 's/.*Cluster Total: \([0-9.]*\) MiB\/s.*/\1/p' | head -1)"
  errs="$(printf '%s\n' "$out" | sed -n 's/^Errors: \([0-9]*\).*/\1/p' | tail -1)"; [ -z "$errs" ] && errs=0
  raw="$(printf '%s\n' "$out" | grep -cE '<ERROR>.*(signature does not match|error)' 2>/dev/null || true)"
  total_errors=$((total_errors + errs + (rc != 0 ? (raw > 0 ? raw : 1) : 0)))
  OBJS[$label]="${objs:-0}"; MIBS[$label]="${mibs:-0}"
  printf '  %-7s obj.size=%-7s conc=%-3s  ->  %8s obj/s  %8s MiB/s  (errors: %s)\n' \
    "$label" "$OBJ_SIZE" "$CONCURRENT" "${objs:-?}" "${mibs:-?}" "$((errs + (rc!=0?raw:0)))"
}

printf '\n=== throughput (concurrency %s, %s) ===\n' "$CONCURRENT" "$OBJ_SIZE"
run_phase WRITE put   --duration "$DURATION"
run_phase READ  get   --objects "$OBJECTS" --duration "$DURATION"
run_phase MIXED mixed --objects "$OBJECTS" --duration "$DURATION"

# --- 3. escalation: ramp concurrency, prove plateau + liveness + zero errors ------------------
printf '\n=== escalation (write-pressure, obj.size %s) ===\n' "$ESC_OBJ_SIZE"
esc_report=""
for lvl in $LEVELS; do
  out="$(cd "$DATA" && "$WARP" put --host "127.0.0.1:$PORT" --access-key "$AK" --secret-key "$SK" \
        --region "$REGION" --bucket "stress-esc" --obj.size "$ESC_OBJ_SIZE" \
        --concurrent "$lvl" --duration "$ESC_DURATION" --noclear 2>&1)"; rc=$?
  objs="$(printf '%s\n' "$out" | sed -n 's/.*Average:[^,]*, \([0-9.]*\) obj\/s.*/\1/p' | head -1)"
  errs="$(printf '%s\n' "$out" | grep -cE '<ERROR>.*(signature does not match|error)' 2>/dev/null || true)"
  errs="${errs:-0}"
  alive="no"; kill -0 "$SRV" 2>/dev/null && curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && alive="yes"
  # count per-op errors always (warp put exits 0 even with them); +1 if it aborted with none parsed
  total_errors=$(( total_errors + errs + ( rc != 0 && errs == 0 ? 1 : 0 ) ))
  printf '  concurrency %-4s  ->  %8s obj/s   alive=%s  errors=%s\n' "$lvl" "${objs:-?}" "$alive" "$errs"
  esc_report="${esc_report}${lvl}:${objs:-0} "
  [ "$alive" = "yes" ] || { total_errors=$((total_errors+1)); note "server NOT alive at concurrency $lvl"; }
done

# --- 4. stability: RSS leak check + peak writer queue depth -----------------------------------
sleep 1
rss_start="$(head -1 "$SAMPLES" 2>/dev/null | cut -f1)"; rss_start="${rss_start:-0}"
rss_end="$(tail -1 "$SAMPLES" 2>/dev/null | cut -f1)"; rss_end="${rss_end:-0}"
rss_peak="$(awk -F'\t' 'BEGIN{m=0} {if($1>m)m=$1} END{print m+0}' "$SAMPLES" 2>/dev/null)"
wq_peak="$(awk -F'\t' 'BEGIN{m=0} {if($2>m)m=$2} END{print m+0}' "$SAMPLES" 2>/dev/null)"
# Leak signal: compare the mean RSS of the LAST third of the run to the MIDDLE third (both under
# sustained load). This excludes the cold-start ramp in the first third — comparing idle-start RSS
# to under-load RSS would flag normal cache warm-up as a leak. A real leak keeps climbing; a healthy
# server plateaus, so last-third ≈ middle-third.
read -r rss_mid rss_late rss_delta_pct <<EOF
$(awk -F'\t' '{r[NR]=$1; n=NR} END {
  if (n < 6) { print "0 0 0.0"; exit }
  a=int(n/3); b=int(2*n/3);
  for(i=a;i<b;i++){m1+=r[i+1];c1++}
  for(i=b;i<n;i++){m2+=r[i+1];c2++}
  mid=(c1?m1/c1:0); late=(c2?m2/c2:0);
  leak=(mid>0?(late-mid)*100.0/mid:0);
  printf "%.0f %.0f %.1f", mid, late, leak
}' "$SAMPLES" 2>/dev/null)
EOF
rss_mid="${rss_mid:-0}"; rss_late="${rss_late:-0}"; rss_delta_pct="${rss_delta_pct:-0.0}"
# total requests served, from the server's own counters
reqs="$(curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null | awk '/^cairn_requests_total/ {s+=$2} END {printf "%d", s+0}')"

# --- 5. report + verdict ---------------------------------------------------------------------
printf '\n=== stability ===\n'
printf '  RSS  cold-start=%s KiB  peak=%s KiB  end=%s KiB\n' "$rss_start" "$rss_peak" "$rss_end"
printf '  RSS steady-state  middle-third=%s KiB  last-third=%s KiB  (growth %s%%, leak threshold %s%%, ceiling %s KiB)\n' \
  "$rss_mid" "$rss_late" "$rss_delta_pct" "$LEAK_PCT" "$RSS_CEILING_KIB"
printf '  peak writer queue depth: %s    server requests served: %s\n' "$wq_peak" "${reqs:-?}"

leaked="no"
awk "BEGIN{exit !($rss_delta_pct > $LEAK_PCT)}" && leaked="yes"          # steady-state climb
[ "${rss_peak:-0}" -gt "$RSS_CEILING_KIB" ] 2>/dev/null && leaked="yes"  # absolute runaway
server_alive="no"; kill -0 "$SRV" 2>/dev/null && server_alive="yes"

if [ -n "${STRESS_OUT:-}" ]; then
  printf '{"write_obj_s":%s,"read_obj_s":%s,"mixed_obj_s":%s,"write_mib_s":%s,"read_mib_s":%s,"escalation":"%s","rss_start_kib":%s,"rss_peak_kib":%s,"rss_end_kib":%s,"rss_delta_pct":%s,"wq_peak":%s,"errors":%s}\n' \
    "${OBJS[WRITE]:-0}" "${OBJS[READ]:-0}" "${OBJS[MIXED]:-0}" "${MIBS[WRITE]:-0}" "${MIBS[READ]:-0}" \
    "$esc_report" "$rss_start" "$rss_peak" "$rss_end" "$rss_delta_pct" "$wq_peak" "$total_errors" >"$STRESS_OUT"
  note "results written to $STRESS_OUT"
fi

if [ -n "${BASELINE:-}" ] && [ -f "${BASELINE:-/nonexistent}" ]; then
  printf '\n=== regression check vs %s (threshold %s%%) ===\n' "$BASELINE" "$REGRESS_PCT"
  for pair in "write_obj_s:WRITE" "read_obj_s:READ" "mixed_obj_s:MIXED"; do
    k="${pair%%:*}"; label="${pair##*:}"
    base="$(sed -n "s/.*\"$k\":\([0-9.]*\).*/\1/p" "$BASELINE")"
    cur="${OBJS[$label]:-0}"
    [ -z "$base" ] && continue
    if awk "BEGIN{exit !($base>0 && $cur < $base*(1-$REGRESS_PCT/100.0))}"; then
      printf '  REGRESSION: %s %s obj/s vs baseline %s (>%s%% slower)\n' "$label" "$cur" "$base" "$REGRESS_PCT"
    else
      printf '  ok: %s %s obj/s vs baseline %s\n' "$label" "$cur" "$base"
    fi
  done
fi

printf '\n=== verdict ===\n'
if [ "$total_errors" -eq 0 ] && [ "$server_alive" = "yes" ] && [ "$leaked" = "no" ]; then
  printf 'PASS: %s WRITE / %s READ / %s MIXED obj/s; 0 op-errors; server alive; RSS stable (%s%%).\n' \
    "${OBJS[WRITE]:-?}" "${OBJS[READ]:-?}" "${OBJS[MIXED]:-?}" "$rss_delta_pct"
  exit 0
fi
printf 'FAIL: errors=%s alive=%s leaked=%s (rss delta %s%%, threshold %s%%)\n' \
  "$total_errors" "$server_alive" "$leaked" "$rss_delta_pct" "$LEAK_PCT" >&2
[ "$server_alive" != "yes" ] && printf 'server log tail:\n%s\n' "$(tail -8 "$DATA/server.log")" >&2
exit 1
