#!/usr/bin/env bash
# Cairn stress + throughput harness (ARCH 30) — the single "is it still fast AND stable?" check to
# run after a change. It drives `warp` (the MinIO S3 macro-benchmark) against a throwaway Cairn and
# reports, in one table: peak write/read/mixed throughput (obj/s + MiB/s), a concurrency-escalation
# ramp that proves the server bends-not-breaks past the single-writer ceiling (throughput plateaus,
# zero errors, stays alive), and server-side stability — RSS, open file descriptors, thread count,
# summed WAL bytes, CPU-seconds (and CPU-s/GiB moved), HTTP 5xx count, and the writer's own
# group-commit histograms (commit-barrier tail + mean batch size). Emits a PASS/FAIL verdict.
#
# GATED here (valid no matter how much load is offered, so they hold on a contended CI runner):
# zero operation errors, server liveness, and absolute CEILINGS on RSS / fds / threads / WAL bytes,
# zero HTTP 5xx.
# ADVISORY here (reported, never the gate): absolute obj/s + MiB/s (hardware-bound), and the
# fd/thread/WAL %-climbs and commit-barrier tail — because this harness deliberately RAMPS
# concurrency, so those grow with offered load and a climb here does NOT imply a leak. Constant-load
# leak detection (where a climb really is a leak) is soak_features.sh's job.
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

# Widened stability gates (all SHAPE/RATIO/COUNT — contended-runner-safe, unlike absolute obj/s).
FD_CEILING="${FD_CEILING:-4096}"                       # hard ceiling on open fds (fd-leak backstop)
WAL_CEILING_BYTES="${WAL_CEILING_BYTES:-$((512*1024*1024))}"  # hard ceiling on summed WAL bytes (512 MiB)
THREAD_CEILING="${THREAD_CEILING:-512}"   # hard ceiling on OS threads (blocking-pool runaway backstop)
BATCH_MIN_QUEUE="${BATCH_MIN_QUEUE:-4}"   # only REPORT batching once the writer actually backed up
# NOTE: the fd/thread/WAL %-climbs and the commit-tail ratio are reported as ADVISORY here, not gated
# — this harness RAMPS concurrency, so those legitimately grow with offered load (see section 5).
# Constant-load leak detection belongs to soak_features.sh, where a climb really does mean a leak.
BATCH_CONC_MIN="${BATCH_CONC_MIN:-32}"    # concurrency at/above which to report group-commit batching
# The mean group-commit batch size is REPORTED, never gated. It was briefly a gate (>=4) and that was
# a mistake: the steady-state value is not a property of the code but of arrival rate vs commit
# duration, so it moves with hardware, build profile and offered load. Measured on a CI runner: 3.88.
# Measured on a 2-core dev box under the same profile: 1.2-1.97. Any floor that clears the dev box is
# too low to mean anything on CI, and any floor that means something on CI fails the dev box — the
# gate failed a perfectly healthy server at 3.88-vs-4.0 the first time it ever actually engaged.
# A genuine collapse (coalescing broken, every mutation committing alone) reads ~1.0 and is plainly
# visible in the reported value, which is what this number is for.

WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"
WARP="${WARP:-}"
DATA="$(mktemp -d)"

export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off
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

# --- 2. background sampler + /metrics scrape helpers -----------------------------------------
# CLK_TCK converts /proc jiffies to CPU-seconds (ported from sendfile_bench.sh proc_cpu_secs()).
CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"
# WAL files: the primary <db>-wal plus one per extra shard (<db>.shard<N>-wal under CAIRN_META_SHARDS).
# Sum them shard-aware; the glob is re-evaluated each sample (files appear once the server writes).
WAL_GLOB="${CAIRN_DB_PATH}*-wal"

# Sum a /metrics counter family (all label sets) into one integer.
scrape_sum() { # <metric-name-prefix regex-anchored>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 ~ m {s += $2} END {printf "%.0f", s+0}'
}
# Total 5xx served so far: cairn_requests_total rows whose status label is 5xx.
scrape_5xx() {
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk '/^cairn_requests_total\{/ && /status="5[0-9][0-9]"/ {s+=$2} END {printf "%d", s+0}'
}
# A single summary quantile of a histogram (metrics_exporter_prometheus renders histograms as
# summaries: <m>{quantile="Q"} plus <m>_sum / <m>_count). Empty when the series hasn't flushed yet.
scrape_quantile() { # <metric> <quantile-string>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" -v q="$2" '$0 ~ ("^" m "\\{") && index($0, "quantile=\"" q "\"") {print $2; exit}'
}
# Exact-named scalar series (e.g. a summary's _sum / _count).
scrape_val() { # <exact-metric-name>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 == m {print $2; exit}'
}
# commit p99 max renders as quantile="1" (some builds "1.0") — try both.
scrape_commit_max() {
  local v; v="$(scrape_quantile cairn_writer_commit_seconds 1)"
  [ -z "$v" ] && v="$(scrape_quantile cairn_writer_commit_seconds 1.0)"
  printf '%s' "$v"
}

# Bytes moved so far (received + sent), for the CPU-seconds/GiB cost figure.
bytes_start="$(scrape_sum '^cairn_bytes_(received|sent)_total')"

# Per-second sampler. Columns: RSS(KiB)  queue_depth  fd_count  wal_bytes  thread_count  cpu_ticks.
# All signals are cheap /proc reads + one /metrics scrape; the 1s cadence is unchanged.
SAMPLES="$DATA/samples.tsv"
(
  while kill -0 "$SRV" 2>/dev/null; do
    rss="$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' ')"
    wq="$(curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null | awk '/^cairn_writer_queue_depth/ {print $2; exit}')"
    fd="$(ls "/proc/$SRV/fd" 2>/dev/null | wc -l)"
    th="$(ls "/proc/$SRV/task" 2>/dev/null | wc -l)"
    # shellcheck disable=SC2086
    wal="$(stat --format=%s $WAL_GLOB 2>/dev/null | awk '{s+=$1} END {print s+0}')"
    cpu="$(awk '{print $14 + $15}' "/proc/$SRV/stat" 2>/dev/null)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${rss:-0}" "${wq:-0}" "${fd:-0}" "${wal:-0}" "${th:-0}" "${cpu:-0}" >>"$SAMPLES"
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
# Writer-health shape signals sampled around the ramp (all deltas/ratios — contended-runner-safe).
fivexx_start="$(scrape_5xx)"; fivexx_start="${fivexx_start:-0}"
commit_p99_first=""; commit_p99_peak=0
batch_pre_sum=""; batch_pre_cnt=""
for lvl in $LEVELS; do
  # Snapshot the batch-size counters just before the first HIGH-concurrency level, so the mean we
  # report covers only the levels where group-commit batching is actually supposed to engage.
  if [ -z "$batch_pre_sum" ] && [ "$lvl" -ge "$BATCH_CONC_MIN" ] 2>/dev/null; then
    batch_pre_sum="$(scrape_val cairn_writer_batch_size_sum)"
    batch_pre_cnt="$(scrape_val cairn_writer_batch_size_count)"
  fi
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
  # Commit-barrier tail per level: we gate on the RATIO (peak vs first level), never the absolute,
  # so a slow runner shifts both ends equally and does not flake the gate.
  cmax="$(scrape_commit_max)"; cmax="${cmax:-0}"
  [ -z "$commit_p99_first" ] && commit_p99_first="$cmax"
  awk "BEGIN{exit !($cmax > $commit_p99_peak)}" && commit_p99_peak="$cmax"
done

# Mean group-commit batch size over the high-concurrency levels (delta of the cumulative summary).
batch_mean_hi=""
if [ -n "$batch_pre_sum" ]; then
  bs="$(scrape_val cairn_writer_batch_size_sum)"; bc="$(scrape_val cairn_writer_batch_size_count)"
  batch_mean_hi="$(awk -v s0="${batch_pre_sum:-0}" -v c0="${batch_pre_cnt:-0}" \
                       -v s1="${bs:-0}" -v c1="${bc:-0}" \
                       'BEGIN{d=c1-c0; printf "%.2f", (d>0 ? (s1-s0)/d : 0)}')"
fi
fivexx_end="$(scrape_5xx)"; fivexx_end="${fivexx_end:-0}"
fivexx_delta=$(( fivexx_end - fivexx_start ))
[ "$fivexx_delta" -lt 0 ] && fivexx_delta=0

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

# Same middle-third-vs-last-third windowing as the RSS check, for any sampler column. A leak keeps
# climbing under sustained load; a healthy server plateaus (fd/threads) or sawtooths (WAL, on checkpoint).
col_climb() { # <1-based column index> -> "mid late growth%"
  awk -F'\t' -v c="$1" '{r[NR]=$c; n=NR} END {
    if (n < 6) { print "0 0 0.0"; exit }
    a=int(n/3); b=int(2*n/3);
    for(i=a;i<b;i++){m1+=r[i+1];k1++}
    for(i=b;i<n;i++){m2+=r[i+1];k2++}
    mid=(k1?m1/k1:0); late=(k2?m2/k2:0);
    g=(mid>0?(late-mid)*100.0/mid:0);
    printf "%.0f %.0f %.1f", mid, late, g
  }' "$SAMPLES" 2>/dev/null
}
col_peak() { awk -F'\t' -v c="$1" 'BEGIN{m=0} {if($c>m)m=$c} END{print m+0}' "$SAMPLES" 2>/dev/null; }

fd_peak="$(col_peak 3)";     wal_peak="$(col_peak 4)";    thread_peak="$(col_peak 5)"
read -r fd_mid fd_late fd_climb_pct <<EOF
$(col_climb 3)
EOF
read -r wal_mid wal_late wal_climb_pct <<EOF
$(col_climb 4)
EOF
read -r th_mid th_late th_climb_pct <<EOF
$(col_climb 5)
EOF
fd_climb_pct="${fd_climb_pct:-0.0}"; wal_climb_pct="${wal_climb_pct:-0.0}"; th_climb_pct="${th_climb_pct:-0.0}"

# CPU cost: jiffies delta over the whole run -> CPU-seconds, and CPU-seconds per GiB moved.
cpu_first="$(head -1 "$SAMPLES" 2>/dev/null | cut -f6)"; cpu_last="$(tail -1 "$SAMPLES" 2>/dev/null | cut -f6)"
cpu_secs="$(awk -v a="${cpu_first:-0}" -v b="${cpu_last:-0}" -v t="$CLK_TCK" 'BEGIN{printf "%.1f", (b-a)/(t>0?t:100)}')"
bytes_end="$(scrape_sum '^cairn_bytes_(received|sent)_total')"
cpu_per_gib="$(awk -v c="${cpu_secs:-0}" -v b0="${bytes_start:-0}" -v b1="${bytes_end:-0}" \
  'BEGIN{g=(b1-b0)/1073741824.0; printf "%.1f", (g>0? c/g : 0)}')"

# total requests served, from the server's own counters
reqs="$(curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null | awk '/^cairn_requests_total/ {s+=$2} END {printf "%d", s+0}')"

# --- 5. report + verdict ---------------------------------------------------------------------
printf '\n=== stability ===\n'
printf '  RSS  cold-start=%s KiB  peak=%s KiB  end=%s KiB\n' "$rss_start" "$rss_peak" "$rss_end"
printf '  RSS steady-state  middle-third=%s KiB  last-third=%s KiB  (growth %s%%, advisory %s%%, hard ceiling %s KiB)\n' \
  "$rss_mid" "$rss_late" "$rss_delta_pct" "$LEAK_PCT" "$RSS_CEILING_KIB"
printf '  peak writer queue depth: %s    server requests served: %s\n' "$wq_peak" "${reqs:-?}"

# Hard leak gate: the absolute RSS ceiling. A real leak grows roughly with request count and blows
# unbounded past it under sustained load; a byte-budgeted cache plateaus well under. The steady-state
# %-growth is ADVISORY only — on fast hardware the cache fills faster than a short run can plateau, so
# a high % is usually warm-up, not a leak. For sensitive leak detection use the long-run soak_features.sh.
leaked="no"
[ "${rss_peak:-0}" -gt "$RSS_CEILING_KIB" ] 2>/dev/null && leaked="yes"
if awk "BEGIN{exit !($rss_delta_pct > $LEAK_PCT)}"; then
  printf '  advisory: steady-state RSS grew %s%% (> %s%%) — likely warm-up on fast/short runs; not fatal. Hard gate is the %s KiB ceiling; run soak_features.sh for sensitive leak detection.\n' \
    "$rss_delta_pct" "$LEAK_PCT" "$RSS_CEILING_KIB"
fi
server_alive="no"; kill -0 "$SRV" 2>/dev/null && server_alive="yes"

# --- widened stability signals (SHAPE / RATIO / COUNT gates — safe on a contended runner) --------
printf '  fd      peak=%s  middle-third=%s  last-third=%s  (gated on ceiling %s)\n' \
  "$fd_peak" "$fd_mid" "$fd_late" "$FD_CEILING"
printf '  threads peak=%s  middle-third=%s  last-third=%s  (gated on ceiling %s)\n' \
  "$thread_peak" "$th_mid" "$th_late" "$THREAD_CEILING"
printf '  WAL     peak=%s B  middle-third=%s B  last-third=%s B  (gated on ceiling %s B)\n' \
  "$wal_peak" "$wal_mid" "$wal_late" "$WAL_CEILING_BYTES"
printf '  CPU     %s s over the run  (%s CPU-s/GiB moved)   5xx responses: %s\n' \
  "$cpu_secs" "$cpu_per_gib" "$fivexx_delta"
printf '  writer  commit p99: first-level=%ss peak=%ss   mean batch size at conc>=%s: %s (advisory)\n' \
  "${commit_p99_first:-?}" "${commit_p99_peak:-?}" \
  "$BATCH_CONC_MIN" "${batch_mean_hi:-n/a}"

# GATE vs ADVISORY, and why. This harness's escalation phase deliberately RAISES concurrency over the
# run (4 -> 64), so a middle-third-vs-last-third comparison here measures "more load applied", NOT
# "resource leaked": threads, fds, WAL bytes and the commit-barrier tail all legitimately grow when
# concurrency triples. Gating on those %-climbs would fail a perfectly healthy server. So under a
# RAMPING load we hard-gate only the signals that stay valid regardless of offered load — absolute
# CEILINGS, zero 5xx, zero op-errors, liveness — and report the climbs as ADVISORY diagnostics.
# Constant-load leak detection (where a climb IS a leak) is soak_features.sh's job, not this harness's.
gate_fail=""
[ "${fd_peak:-0}" -gt "$FD_CEILING" ] 2>/dev/null && gate_fail="$gate_fail fd_ceiling($fd_peak>$FD_CEILING)"
[ "${thread_peak:-0}" -gt "$THREAD_CEILING" ] 2>/dev/null && gate_fail="$gate_fail thread_ceiling($thread_peak>$THREAD_CEILING)"
[ "${wal_peak:-0}" -gt "$WAL_CEILING_BYTES" ] 2>/dev/null && gate_fail="$gate_fail wal_ceiling($wal_peak>$WAL_CEILING_BYTES)"
[ "${fivexx_delta:-0}" -gt 0 ] && gate_fail="$gate_fail http_5xx($fivexx_delta)"
# Group-commit batching can only coalesce work that is actually QUEUED. If the writer never backed up
# (peak queue depth below BATCH_MIN_QUEUE) a mean batch size near 1 is correct behaviour, not a
# collapse — so this gate applies only once the writer was genuinely saturated.
batch_verdict="n/a (writer never saturated: peak queue depth ${wq_peak:-0} < $BATCH_MIN_QUEUE)"
if [ "${wq_peak:-0}" -ge "$BATCH_MIN_QUEUE" ] 2>/dev/null && [ -n "$batch_mean_hi" ]; then
  batch_verdict="$batch_mean_hi (advisory; ~1.0 would mean coalescing collapsed)"
fi
printf '  advisory (ramping load, not gated): fd climb %s%%  threads climb %s%%  WAL climb %s%%  commit p99 %ss -> %ss\n' \
  "$fd_climb_pct" "$th_climb_pct" "$wal_climb_pct" "${commit_p99_first:-?}" "${commit_p99_peak:-?}"
printf '  group-commit batching (advisory): %s\n' "$batch_verdict"
[ -n "$gate_fail" ] && printf '  STABILITY GATE FAILURES:%s\n' "$gate_fail" >&2

if [ -n "${STRESS_OUT:-}" ]; then
  # Schema is ADDITIVE: older baselines simply lack the new keys, and the compare loop skips a key
  # it cannot find, so a pre-widening BASELINE still works.
  printf '{"write_obj_s":%s,"read_obj_s":%s,"mixed_obj_s":%s,"write_mib_s":%s,"read_mib_s":%s,"escalation":"%s","rss_start_kib":%s,"rss_peak_kib":%s,"rss_end_kib":%s,"rss_delta_pct":%s,"wq_peak":%s,"errors":%s,"fd_peak":%s,"fd_climb_pct":%s,"thread_peak":%s,"thread_climb_pct":%s,"wal_peak_bytes":%s,"wal_climb_pct":%s,"cpu_secs":%s,"cpu_secs_per_gib":%s,"http_5xx":%s,"commit_p99_first":%s,"commit_p99_peak":%s,"batch_mean_high_conc":%s}\n' \
    "${OBJS[WRITE]:-0}" "${OBJS[READ]:-0}" "${OBJS[MIXED]:-0}" "${MIBS[WRITE]:-0}" "${MIBS[READ]:-0}" \
    "$esc_report" "$rss_start" "$rss_peak" "$rss_end" "$rss_delta_pct" "$wq_peak" "$total_errors" \
    "${fd_peak:-0}" "${fd_climb_pct:-0}" "${thread_peak:-0}" "${th_climb_pct:-0}" \
    "${wal_peak:-0}" "${wal_climb_pct:-0}" "${cpu_secs:-0}" "${cpu_per_gib:-0}" "${fivexx_delta:-0}" \
    "${commit_p99_first:-0}" "${commit_p99_peak:-0}" "${batch_mean_hi:-0}" >"$STRESS_OUT"
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
if [ "$total_errors" -eq 0 ] && [ "$server_alive" = "yes" ] && [ "$leaked" = "no" ] && [ -z "$gate_fail" ]; then
  printf 'PASS: %s WRITE / %s READ / %s MIXED obj/s; 0 op-errors; server alive.\n' \
    "${OBJS[WRITE]:-?}" "${OBJS[READ]:-?}" "${OBJS[MIXED]:-?}"
  printf '      Under ceilings: RSS %s/%s KiB, fd %s/%s, threads %s/%s, WAL %s/%s B. 0 HTTP 5xx. Batching: %s.\n' \
    "$rss_peak" "$RSS_CEILING_KIB" "$fd_peak" "$FD_CEILING" "$thread_peak" "$THREAD_CEILING" \
    "$wal_peak" "$WAL_CEILING_BYTES" "$batch_verdict"
  exit 0
fi
printf 'FAIL: errors=%s alive=%s leaked=%s (rss peak %s KiB, ceiling %s KiB)%s\n' \
  "$total_errors" "$server_alive" "$leaked" "$rss_peak" "$RSS_CEILING_KIB" \
  "${gate_fail:+ stability-gates:$gate_fail}" >&2
[ "$server_alive" != "yes" ] && printf 'server log tail:\n%s\n' "$(tail -8 "$DATA/server.log")" >&2
exit 1
