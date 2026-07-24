#!/usr/bin/env bash
# Cairn MIXED-FEATURE SOAK (ARCH 29-30) — the harness where CONSTANT-LOAD LEAK DETECTION lives.
#
# THE GAP. Every feature has a functional harness, and the stress harnesses push one path hard, but
# nothing ran the FEATURES TOGETHER under sustained load for long enough to expose a slow leak.
# `soak.sh` is the only long-running harness and it is two-node replication only: plaintext, one
# bucket, a fixed 64 KiB body, one bucket's worth of keys, and it samples nothing but the source's
# RSS. So fd growth, WAL bloat, staging accumulation, thread-pool creep and session-credential row
# growth under a realistic mixed workload have had nowhere to show up.
#
# WHY THIS HARNESS MAY GATE ON A CLIMB AND `stress.sh` DELIBERATELY MAY NOT. `stress.sh` RAMPS
# concurrency (4 -> 64), so a middle-third-vs-last-third climb there measures "more load was
# offered", not "a resource leaked" — which is exactly why its fd/thread/WAL climbs are ADVISORY and
# only absolute ceilings are gated. THIS harness holds the offered load CONSTANT for the entire run:
# a fixed-size worker pool loops without sleeping from the first second to the last, so concurrency
# never drifts. Under constant load a monotonic climb in RSS / fds / threads / WAL bytes / staging
# bytes IS a leak signal, and the steady-state SHAPE gates below are sound. That is the division of
# labour the `stress.sh` header states; this is the other half of it.
#
# THE MIX (all continuous and concurrent against ONE node — see soak_features.py for detail):
#   SSE-S3 + SSE-KMS single-part churn | a VERSIONED bucket accumulating per-version DEKs, delete
#   markers and version deletes | composite-checksum multipart uploads completed AND aborted, plus
#   abandoned sessions for the background sweeper | an object-lock bucket whose locked versions are
#   attacked with deletes all run | STS session credentials minted on a cadence and driving a slice
#   of the traffic | a lifecycle bucket with an immediately-due rule so the scanner runs every second.
#
# SAMPLED every second for the whole run: RSS, open fds, thread count, summed WAL bytes
# (shard-aware), CPU ticks, plus (every SOAK_HEAVY_EVERY ticks, carried forward in between, so the
# sampler stays cheap enough not to perturb what it measures) total `.staging` bytes and the
# `session_credentials` row count.
#
# GATED — CORRECTNESS (all valid regardless of offered load):
#   * zero operation errors; every sampled GET byte-exact; every locked version still undeletable;
#     the lifecycle control prefix never touched; a tampered session token refused on every mint;
#     multipart assemblies byte-exact with a COMPOSITE checksum; aborted uploads leave no staging.
#   * `/healthz` never STOPS answering (a 60 s per-probe WEDGE timeout — probe latency under a
#     saturating debug-build workload is offered load, not signal), and the server is alive at the end.
#   * HTTP 5xx EQUAL to the driver's declared budget, which for this mix is exactly ZERO: every
#     deliberate rejection here (WORM, tampered token, expired session) is a 4xx.
# GATED — STEADY-STATE LEAK SHAPE (last-third mean vs middle-third mean, the stress.sh windowing;
#   the first third is excluded because it is cold-start/cache warm-up, not steady state). Every one
#   requires BOTH a % growth AND a meaningful ABSOLUTE delta, so a +4 fd wobble can never fail a run:
#   * RSS, fd count, thread count must not climb.
#   * staging bytes must PLATEAU, not grow monotonically (reclaim + the sweeper must keep up).
#   * WAL must sawtooth under its ceiling rather than climb monotonically.
# GATED — ABSOLUTE CEILINGS as backstops: RSS / fd / threads / WAL, same knobs and defaults as
#   `stress.sh`, so all three stress/soak harnesses agree on what "runaway" means.
# ADVISORY (printed, written to SOAK_OUT, never gating): throughput and per-feature op counts,
#   Complete wall times, CPU-seconds and CPU-s/GiB, lifecycle expirations observed, session-credential
#   row count, staging peak, writer commit p99 and mean batch size. CI drives the DEBUG artifact,
#   whose AES-GCM is unoptimized software crypto — a rate here says nothing about the code.
#
# TWO THINGS ONLY A LONG RUN CAN ASSERT, and which report as explicitly SKIPPED otherwise (never
# silently "pass"): the multipart SWEEPER reclaiming abandoned sessions (needs the run to outlast
# CAIRN_MULTIPART_UPLOAD_LIFETIME_SECS), and an EXPIRED session credential being refused (the
# server's floor for a session lifetime is 900 s, ARCH 14, so it needs SOAK_SECS > ~930).
#
# NOTE on the session-credential row count: it is SAMPLED AND REPORTED, never gated. A minted
# credential cannot live less than 900 s, and the reaper only prunes rows past their expiry, so on a
# CI-length soak the row count growing monotonically is CORRECT behaviour, not a leak. On a deep
# manual run (SOAK_SECS well past 900) the plateau becomes visible in the sampled column.
#
# Usage:
#   conformance/soak_features.sh                                  # ~3 min, the CI profile
#   SOAK_SECS=1800 conformance/soak_features.sh                   # deep run (gates sweeper + STS expiry)
#   BIN=target/release/cairn SOAK_SECS=600 conformance/soak_features.sh
#   SOAK_OUT=/tmp/cairn-soak-features.json conformance/soak_features.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9105}"
KEY_ID="${KEY_ID:-alias/cairn-soak}"

SOAK_SECS="${SOAK_SECS:-180}"
SAMPLE_SECS="${SAMPLE_SECS:-1}"
HEAVY_EVERY="${SOAK_HEAVY_EVERY:-5}"   # ticks between the two expensive samples (du + row count)

# Background-loop cadences the mix depends on. All are ordinary CAIRN_* knobs (ARCH 28).
LC_INTERVAL="${SOAK_LC_INTERVAL:-1}"     # lifecycle scanner: run every second so it churns all soak
MP_SWEEP="${SOAK_MP_SWEEP:-5}"           # multipart sweeper cadence
MP_LIFETIME="${SOAK_MP_LIFETIME:-90}"    # a session idle beyond this is swept
WAL_CKPT="${SOAK_WAL_CKPT:-15}"          # see the WAL note below

# Absolute ceilings — identical knobs/defaults to stress.sh and stress_multipart.sh.
RSS_CEILING_KIB="${RSS_CEILING_KIB:-1048576}"
FD_CEILING="${FD_CEILING:-4096}"
THREAD_CEILING="${THREAD_CEILING:-512}"
WAL_CEILING_BYTES="${WAL_CEILING_BYTES:-$((512*1024*1024))}"

# ---- LEAK thresholds. Each gate needs BOTH the % growth AND the absolute delta, because a pure
# percentage is a trap on small counts (fds going 40 -> 50 is +25% and is nothing) while a pure
# absolute is a trap on big ones. Every threshold is justified where it is defined.
#
# RSS: the metadata cache is byte-budgeted and the blob path is streaming, so under CONSTANT load RSS
# reaches its working set inside the first third and then plateaus. The middle third is already warm,
# so a further 10% AND 64 MiB in the last third is not warm-up — it is unreclaimed allocation.
RSS_LEAK_PCT="${RSS_LEAK_PCT:-10}"
RSS_LEAK_MIN_KIB="${RSS_LEAK_MIN_KIB:-65536}"
# fds: the client pool is fixed-size and every server-side file handle is scoped to one request, so
# the steady-state fd count is flat by construction. 25% is generous for keep-alive churn; +32 fds is
# far outside that wobble and is what a per-request handle leak looks like after a few thousand ops.
FD_LEAK_PCT="${FD_LEAK_PCT:-25}"
FD_LEAK_MIN="${FD_LEAK_MIN:-32}"
# threads: tokio's worker count is fixed at startup and the blocking pool is bounded and idles out,
# so the thread count is flat under constant load. +16 threads (and 10%) is a pool that is growing
# rather than reusing.
THREAD_LEAK_PCT="${THREAD_LEAK_PCT:-10}"
THREAD_LEAK_MIN="${THREAD_LEAK_MIN:-16}"
# staging: multipart parts are staged then either assembled or reclaimed, so the directory sawtooths
# around a working set proportional to the IN-FLIGHT sessions (a constant here), never to the total
# number of sessions. The gate is therefore a PLATEAU test rather than a mean-growth test — see
# col_monotonic below — with a 64 MiB floor so ordinary sawtooth peaks (one 5 MiB part per in-flight
# session) cannot trip it.
STAGING_LEAK_MIN_BYTES="${STAGING_LEAK_MIN_BYTES:-$((64*1024*1024))}"
# WAL: inline auto-checkpointing is disabled (PRAGMA wal_autocheckpoint=0), so the WAL is bounded by
# the background checkpointer alone. It must SAWTOOTH: grow, checkpoint, shrink. A WAL whose
# last-third MINIMUM is above its middle-third MAXIMUM never came back down at all, which means
# checkpointing stopped keeping up. 32 MiB of floor keeps a normal inter-checkpoint peak from firing.
WAL_LEAK_MIN_BYTES="${WAL_LEAK_MIN_BYTES:-$((32*1024*1024))}"

DATA="$(mktemp -d)"
export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off              # STS is minted off the S3 port, so no console listener is needed
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"
export CAIRN_MASTER_KEY="${CAIRN_MASTER_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"
export CAIRN_KMS_KEY_IDS="$KEY_ID"    # label-only KMS: the key id must be on the write-time allow-list
export CAIRN_STS_ENABLED=true         # the AWS-STS form surface on the S3 port (ARCH 14)
export CAIRN_LIFECYCLE_INTERVAL_SECS="$LC_INTERVAL"
export CAIRN_MULTIPART_SWEEP_INTERVAL_SECS="$MP_SWEEP"
export CAIRN_MULTIPART_UPLOAD_LIFETIME_SECS="$MP_LIFETIME"
# The WAL leak gate is only MEANINGFUL if the checkpointer can actually run inside the window. The
# default interval is 300 s — longer than a CI-length soak — so the WAL would grow monotonically by
# CONFIGURATION and the gate would fire on a healthy server. Drive it to 15 s so the sawtooth is
# real, and keep the default 64 MiB size trigger as the second bound.
export CAIRN_WAL_CHECKPOINT_INTERVAL_SECS="$WAL_CKPT"
# Pin the server-side request timeout well above any plausible op on a contended debug runner: if it
# were left ambient a slow box could answer 503 RequestTimeout, breaking the zero-errors gate and the
# exact-5xx-budget gate at once — a load-dependent flake, which is what this harness must never have.
export CAIRN_REQUEST_TIMEOUT_SECS="${CAIRN_REQUEST_TIMEOUT_SECS:-600}"

# Values the driver needs to reason about the sweeper window must MATCH the server's config.
export SOAK_SECS SOAK_MP_SWEEP="$MP_SWEEP" SOAK_MP_LIFETIME="$MP_LIFETIME"

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
log_tail() { [ -f "$DATA/server.log" ] && printf '\n--- server.log (tail) ---\n%s\n' \
  "$(tail -30 "$DATA/server.log")" >&2; }

[ -x "$BIN" ] || fail "binary not found: $BIN (build it: cargo build --bin cairn)"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY"
# boto3 is NOT optional: it IS the harness. A box that quietly lost boto3 must go red rather than
# green-with-no-coverage (the provenance lesson from stress_encrypted.sh).
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by '$PY' — this harness IS the boto3 driver"
[ "$SOAK_SECS" -ge 60 ] 2>/dev/null || fail "SOAK_SECS must be >= 60 (got $SOAK_SECS): the sampler's \
third-vs-third leak windows need enough samples to mean anything"
[ "$HEAVY_EVERY" -ge 1 ] 2>/dev/null || fail "SOAK_HEAVY_EVERY must be >= 1 (got $HEAVY_EVERY)"

CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# --- /metrics scrape helpers (same shapes as stress.sh) ---------------------------------------
scrape_sum() { # <metric-name regex>
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 ~ m {s += $2} END {printf "%.0f", s+0}'
}
scrape_5xx() {
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk '/^cairn_requests_total\{/ && /status="5[0-9][0-9]"/ {s+=$2} END {printf "%d", s+0}'
}
scrape_quantile() { # <metric> <quantile-string>
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" -v q="$2" '$0 ~ ("^" m "\\{") && index($0, "quantile=\"" q "\"") {print $2; exit}'
}
scrape_val() { # <exact-metric-name>
  curl -fsS --max-time 10 "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 == m {print $2; exit}'
}
scrape_commit_max() {
  local v; v="$(scrape_quantile cairn_writer_commit_seconds 1)"
  [ -z "$v" ] && v="$(scrape_quantile cairn_writer_commit_seconds 1.0)"
  printf '%s' "$v"
}
jget() { # <key> — a JSON scalar out of the driver's advisory file, no jq dependency
  "$PY" - "$1" "$SOAK_JSON" <<'PYEOF' 2>/dev/null
import json, sys
try:
    with open(sys.argv[2], encoding="utf-8") as fh:
        print(json.load(fh).get(sys.argv[1], 0))
except Exception:
    print(0)
PYEOF
}

# --- 1. server (deterministic startup: bootstrap, serve, poll /healthz) -----------------------
BOOT="$("$BIN" bootstrap)" || fail "bootstrap failed"
AK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
SK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials"
"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
up=""
for _ in $(seq 1 100); do
  curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && { up=yes; break; }
  kill -0 "$SRV" 2>/dev/null || { printf 'FAIL: server exited at startup\n' >&2; log_tail; exit 1; }
  sleep 0.1
done
[ -n "$up" ] || { printf 'FAIL: server never became healthy\n' >&2; log_tail; exit 1; }
printf '\n=== cairn (pid %s) on 127.0.0.1:%s — %s, %s cores; soak %ss ===\n' \
  "$SRV" "$PORT" "$(uname -m)" "$(nproc)" "$SOAK_SECS"

# --- 2. per-second sampler ---------------------------------------------------------------------
# Columns: RSS(KiB)  fd  threads  wal_bytes  cpu_ticks  staging_bytes  session_rows  writer_queue
# The first five are the stress.sh signals; the last three are what a MIXED-feature soak adds.
SAMPLES="$DATA/samples.tsv"
WAL_GLOB="${CAIRN_DB_PATH}*-wal"
STAGING="$CAIRN_DATA_DIR/.staging"

# session_credentials row count. Read-only, out-of-band, and best-effort: SQLite can open a live WAL
# database read-only, and if it cannot (no client available, locked, schema moved) the column simply
# stays 0 and the report says the sample was unavailable. It is ADVISORY either way — see the header
# note on why a growing row count is CORRECT on a sub-900 s run.
SESS_MODE=none
if command -v sqlite3 >/dev/null 2>&1; then SESS_MODE=cli
elif "$PY" -c "import sqlite3" 2>/dev/null; then SESS_MODE=py
fi
sess_rows() {
  case "$SESS_MODE" in
    cli) sqlite3 "file:$CAIRN_DB_PATH?mode=ro" \
           "SELECT COUNT(*) FROM session_credentials;" 2>/dev/null || echo 0 ;;
    py)  "$PY" - "$CAIRN_DB_PATH" <<'PYEOF' 2>/dev/null || echo 0
import sqlite3, sys
try:
    con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=2)
    print(con.execute("SELECT COUNT(*) FROM session_credentials").fetchone()[0])
    con.close()
except Exception:
    print(0)
PYEOF
         ;;
    *)   echo 0 ;;
  esac
}

(
  tick=0; staging=0; sess=0
  while kill -0 "$SRV" 2>/dev/null; do
    tick=$((tick + 1))
    rss="$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' ')"
    fd="$(ls "/proc/$SRV/fd" 2>/dev/null | wc -l)"
    th="$(ls "/proc/$SRV/task" 2>/dev/null | wc -l)"
    # shellcheck disable=SC2086
    wal="$(stat --format=%s $WAL_GLOB 2>/dev/null | awk '{s+=$1} END {print s+0}')"
    cpu="$(awk '{print $14 + $15}' "/proc/$SRV/stat" 2>/dev/null)"
    wq="$(curl -fsS --max-time 5 "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
          | awk '/^cairn_writer_queue_depth/ {print $2; exit}')"
    # The two expensive samples run every HEAVY_EVERY ticks and are carried forward in between, so
    # the sampler never becomes a load generator of its own. Carrying forward only repeats a value —
    # it cannot invent a climb or hide one.
    if [ $(( (tick - 1) % HEAVY_EVERY )) -eq 0 ]; then
      staging="$(du -sb "$STAGING" 2>/dev/null | awk '{print $1+0}')"
      sess="$(sess_rows | tr -dc '0-9')"
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${rss:-0}" "${fd:-0}" "${th:-0}" "${wal:-0}" "${cpu:-0}" \
      "${staging:-0}" "${sess:-0}" "${wq:-0}" >>"$SAMPLES"
    sleep "$SAMPLE_SECS"
  done
) &
SAMPLER=$!

fivexx_start="$(scrape_5xx)"; fivexx_start="${fivexx_start:-0}"
bytes_start="$(scrape_sum '^cairn_bytes_(received|sent)_total')"

# --- 3. the driver -----------------------------------------------------------------------------
SOAK_JSON="$DATA/driver.json"
DRIVER_RC=0
"$PY" "$ROOT/conformance/soak_features.py" "$AK" "$SK" "http://127.0.0.1:$PORT" \
  "$CAIRN_DATA_DIR" "$KEY_ID" "$SOAK_JSON" || DRIVER_RC=$?

# Stop sampling THE INSTANT the workload stops. Everything below (metric scrapes, liveness probe)
# runs against an idle server; samples taken then would land in the last third and bias the plateau
# gates toward PASS — staging drains and the WAL checkpoints down once nothing is writing.
kill "$SAMPLER" 2>/dev/null; SAMPLER=""

# --- 4. collect ---------------------------------------------------------------------------------
fivexx_end="$(scrape_5xx)"; fivexx_end="${fivexx_end:-0}"
fivexx_delta=$(( fivexx_end - fivexx_start ))
[ "$fivexx_delta" -lt 0 ] && fivexx_delta=0
bytes_end="$(scrape_sum '^cairn_bytes_(received|sent)_total')"
commit_p99="$(scrape_commit_max)"; commit_p99="${commit_p99:-0}"
bsum="$(scrape_val cairn_writer_batch_size_sum)"; bcnt="$(scrape_val cairn_writer_batch_size_count)"
batch_mean="$(awk -v s="${bsum:-0}" -v c="${bcnt:-0}" 'BEGIN{printf "%.2f", (c>0? s/c : 0)}')"
requests="$(scrape_sum '^cairn_requests_total')"

alive="no"
kill -0 "$SRV" 2>/dev/null && \
  curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && alive="yes"

samples="$(wc -l <"$SAMPLES" 2>/dev/null | tr -d ' ')"; samples="${samples:-0}"

# --- 5. sampler analysis -------------------------------------------------------------------------
col_peak() { awk -F'\t' -v c="$1" 'BEGIN{m=0} {if($c>m)m=$c} END{print m+0}' "$SAMPLES" 2>/dev/null; }
# Middle-third-vs-last-third windowing, straight from stress.sh: the FIRST third is excluded because
# it is cold-start and cache warm-up, and comparing an idle-start value to an under-load one would
# flag normal warm-up as a leak. Under CONSTANT load a healthy server has last ~= middle.
col_climb() { # <col> -> "mid late growth%"
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
# PLATEAU test for a signal that is SUPPOSED to sawtooth (staging bytes, WAL bytes): compare the
# last third's MINIMUM to the middle third's MAXIMUM. A healthy sawtooth keeps coming back down, so
# its last-third minimum sits below the middle-third maximum and the delta is <= 0. A signal that
# never returns to its earlier floor is climbing monotonically, whatever its mean does.
col_monotonic() { # <col> -> "min_late max_mid delta"
  awk -F'\t' -v c="$1" '{r[NR]=$c; n=NR} END {
    if (n < 6) { print "0 0 0"; exit }
    a=int(n/3); b=int(2*n/3);
    mx=0; for(i=a;i<b;i++){ if(r[i+1]>mx) mx=r[i+1] }
    mn=-1; for(i=b;i<n;i++){ if(mn<0 || r[i+1]<mn) mn=r[i+1] }
    if (mn < 0) mn = 0;
    printf "%.0f %.0f %.0f", mn, mx, mn-mx
  }' "$SAMPLES" 2>/dev/null
}

rss_peak="$(col_peak 1)"; fd_peak="$(col_peak 2)"; thread_peak="$(col_peak 3)"
wal_peak="$(col_peak 4)"; staging_peak="$(col_peak 6)"; sess_peak="$(col_peak 7)"
wq_peak="$(col_peak 8)"
read -r rss_mid rss_late rss_pct <<EOF
$(col_climb 1)
EOF
read -r fd_mid fd_late fd_pct <<EOF
$(col_climb 2)
EOF
read -r th_mid th_late th_pct <<EOF
$(col_climb 3)
EOF
read -r wal_mid wal_late wal_pct <<EOF
$(col_climb 4)
EOF
read -r stg_mid stg_late stg_pct <<EOF
$(col_climb 6)
EOF
read -r sess_mid sess_late sess_pct <<EOF
$(col_climb 7)
EOF
read -r stg_minlate stg_maxmid stg_delta <<EOF
$(col_monotonic 6)
EOF
read -r wal_minlate wal_maxmid wal_delta <<EOF
$(col_monotonic 4)
EOF

cpu_first="$(head -1 "$SAMPLES" 2>/dev/null | cut -f5)"; cpu_last="$(tail -1 "$SAMPLES" 2>/dev/null | cut -f5)"
cpu_secs="$(awk -v a="${cpu_first:-0}" -v b="${cpu_last:-0}" -v t="$CLK_TCK" \
  'BEGIN{printf "%.1f", (b-a)/(t>0?t:100)}')"
gib="$(awk -v b0="${bytes_start:-0}" -v b1="${bytes_end:-0}" 'BEGIN{printf "%.2f", (b1-b0)/1073741824.0}')"
cpu_per_gib="$(awk -v c="${cpu_secs:-0}" -v g="${gib:-0}" 'BEGIN{printf "%.1f", (g>0? c/g : 0)}')"

declared_5xx="$(jget declared_5xx)"; declared_5xx="${declared_5xx:-0}"
case "$declared_5xx" in ''|*[!0-9]*) declared_5xx=0 ;; esac

# --- 6. gates -------------------------------------------------------------------------------------
GATE_FAIL=""
[ "$DRIVER_RC" -eq 0 ] || GATE_FAIL="$GATE_FAIL driver_assertions(rc=$DRIVER_RC)"
[ "$alive" = "yes" ] || GATE_FAIL="$GATE_FAIL server_dead"
[ "$fivexx_delta" -ne "$declared_5xx" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL unexpected_http_5xx($fivexx_delta!=$declared_5xx)"
# A run with too few samples cannot support ANY shape gate — say so rather than pass vacuously.
[ "$samples" -lt 12 ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL too_few_samples($samples<12)"
# A column that reads ZERO for the whole run means the SAMPLER is broken, not that the server is
# clean: a zero peak passes its ceiling and a zero delta passes its plateau test, so a stale glob
# (sharded WAL naming, staging relocated) would silently disable two gates at once.
[ "${rss_peak:-0}" -gt 0 ] 2>/dev/null ||
  GATE_FAIL="$GATE_FAIL rss_never_sampled"
[ "${wal_peak:-0}" -gt 0 ] 2>/dev/null ||
  GATE_FAIL="$GATE_FAIL wal_never_sampled(glob=$WAL_GLOB)"
[ "${staging_peak:-0}" -gt 0 ] 2>/dev/null ||
  GATE_FAIL="$GATE_FAIL staging_never_sampled(dir=$STAGING)"

# Absolute ceilings (backstops, identical to stress.sh).
[ "${rss_peak:-0}" -gt "$RSS_CEILING_KIB" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL rss_ceiling(${rss_peak}>$RSS_CEILING_KIB)"
[ "${fd_peak:-0}" -gt "$FD_CEILING" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL fd_ceiling(${fd_peak}>$FD_CEILING)"
[ "${thread_peak:-0}" -gt "$THREAD_CEILING" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL thread_ceiling(${thread_peak}>$THREAD_CEILING)"
[ "${wal_peak:-0}" -gt "$WAL_CEILING_BYTES" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL wal_ceiling(${wal_peak}>$WAL_CEILING_BYTES)"

# Steady-state leak gates. BOTH conditions must hold — the % growth AND the absolute delta.
leak_gate() { # <name> <mid> <late> <pct> <pct-threshold> <abs-threshold>
  awk -v n="$1" -v mid="$2" -v late="$3" -v p="$4" -v pt="$5" -v at="$6" \
    'BEGIN{ if (p > pt && (late-mid) > at) printf "%s(+%.1f%%,+%d)", n, p, late-mid }'
}
GATE_FAIL="$GATE_FAIL $(leak_gate rss_leak   "$rss_mid" "$rss_late" "$rss_pct" "$RSS_LEAK_PCT"    "$RSS_LEAK_MIN_KIB")"
GATE_FAIL="$GATE_FAIL $(leak_gate fd_leak    "$fd_mid"  "$fd_late"  "$fd_pct"  "$FD_LEAK_PCT"     "$FD_LEAK_MIN")"
GATE_FAIL="$GATE_FAIL $(leak_gate thread_leak "$th_mid" "$th_late"  "$th_pct"  "$THREAD_LEAK_PCT" "$THREAD_LEAK_MIN")"
# Plateau gates: staging bytes and WAL bytes must come back DOWN, not just grow slowly.
if [ "${stg_delta:-0}" -gt "$STAGING_LEAK_MIN_BYTES" ] 2>/dev/null; then
  GATE_FAIL="$GATE_FAIL staging_monotonic(min_late=${stg_minlate}>max_mid=${stg_maxmid})"
fi
if [ "${wal_delta:-0}" -gt "$WAL_LEAK_MIN_BYTES" ] 2>/dev/null; then
  GATE_FAIL="$GATE_FAIL wal_monotonic(min_late=${wal_minlate}>max_mid=${wal_maxmid})"
fi
GATE_FAIL="$(printf '%s' "$GATE_FAIL" | tr -s ' ')"
[ "$GATE_FAIL" = " " ] && GATE_FAIL=""

# --- 7. report -----------------------------------------------------------------------------------
printf '\n=== steady-state shape (GATED: constant load, so a climb IS a leak) ===\n'
printf '  %-16s %12s %12s %10s %14s\n' signal middle-third last-third growth gate
printf '  %-16s %12s %12s %9s%% %14s\n' "RSS (KiB)" "$rss_mid" "$rss_late" "$rss_pct" \
  ">${RSS_LEAK_PCT}% & +${RSS_LEAK_MIN_KIB}"
printf '  %-16s %12s %12s %9s%% %14s\n' "open fds" "$fd_mid" "$fd_late" "$fd_pct" \
  ">${FD_LEAK_PCT}% & +${FD_LEAK_MIN}"
printf '  %-16s %12s %12s %9s%% %14s\n' "threads" "$th_mid" "$th_late" "$th_pct" \
  ">${THREAD_LEAK_PCT}% & +${THREAD_LEAK_MIN}"
printf '  %-16s %12s %12s %9s%% %14s\n' "WAL bytes" "$wal_mid" "$wal_late" "$wal_pct" "plateau"
printf '  %-16s %12s %12s %9s%% %14s\n' "staging bytes" "$stg_mid" "$stg_late" "$stg_pct" "plateau"
printf '  plateau check — staging: last-third min %s vs middle-third max %s (delta %s, gate >%s)\n' \
  "$stg_minlate" "$stg_maxmid" "$stg_delta" "$STAGING_LEAK_MIN_BYTES"
printf '  plateau check — WAL:     last-third min %s vs middle-third max %s (delta %s, gate >%s)\n' \
  "$wal_minlate" "$wal_maxmid" "$wal_delta" "$WAL_LEAK_MIN_BYTES"
printf '  ceilings (backstops): RSS %s/%s KiB  fd %s/%s  threads %s/%s  WAL %s/%s B   5xx %s/%s\n' \
  "$rss_peak" "$RSS_CEILING_KIB" "$fd_peak" "$FD_CEILING" "$thread_peak" "$THREAD_CEILING" \
  "$wal_peak" "$WAL_CEILING_BYTES" "$fivexx_delta" "$declared_5xx"
printf '  samples: %s at %ss (heavy samples every %s ticks)\n' "$samples" "$SAMPLE_SECS" "$HEAVY_EVERY"

printf '\n=== ADVISORY (never gating — contention- and build-profile-bound) ===\n'
printf '  ops: %s total, %s ops/s over %ss with %s constant workers\n' \
  "$(jget total_ops)" "$(jget ops_per_sec)" "$(jget elapsed_secs)" "$(jget workers)"
printf '  SSE:       %s PUT / %s verified / %s DELETE   (%s of them on STS session creds)\n' \
  "$(jget sse_puts)" "$(jget sse_verified)" "$(jget sse_deletes)" "$(jget sts_driven_ops)"
printf '  versions:  %s PUT / %s version-GETs verified / %s delete markers / %s version deletes\n' \
  "$(jget ver_puts)" "$(jget ver_verified)" "$(jget ver_delete_markers)" "$(jget ver_version_deletes)"
printf '  multipart: %s completed (%s composite-checksummed, %s verified) / %s aborted   wall min %ss med %ss max %ss\n' \
  "$(jget mp_completes)" "$(jget mp_composite_ok)" "$(jget mp_verified)" "$(jget mp_aborts)" \
  "$(jget complete_wall_min)" "$(jget complete_wall_med)" "$(jget complete_wall_max)"
printf '  WORM:      %s versions locked, %s delete attempts, %s refused\n' \
  "$(jget lock_versions_locked)" "$(jget worm_delete_attempts)" "$(jget worm_delete_refused)"
printf '  lifecycle: %s expiring PUTs, %s expirations OBSERVED, %s control-prefix reads verified\n' \
  "$(jget lc_expiring_puts)" "$(jget lc_expirations_observed)" "$(jget lc_keep_verified)"
printf '  STS:       %s mints, %s usable, %s tampered-token refusals   sweeper: %s   expiry: %s\n' \
  "$(jget sts_mints)" "$(jget sts_sessions_usable)" "$(jget sts_tampered_refused)" \
  "$(jget sweeper)" "$(jget sts_expiry)"
printf '  session_credentials rows: middle-third %s -> last-third %s (%s%%, peak %s, sampler=%s) — NOT gated:\n' \
  "$sess_mid" "$sess_late" "$sess_pct" "$sess_peak" "$SESS_MODE"
printf '             a session cannot live under 900 s (ARCH 14), so on a sub-900 s soak a rising row\n'
printf '             count is CORRECT; the plateau only becomes observable on a deep run.\n'
printf '  staging peak: %s B   writer commit p99 max: %ss   mean batch: %s   peak queue depth: %s\n' \
  "$staging_peak" "$commit_p99" "$batch_mean" "${wq_peak:-0}"
printf '  CPU: %s s (%s CPU-s/GiB over %s GiB moved)   requests served: %s   /healthz: %s probes (worst %s ms)\n' \
  "$cpu_secs" "$cpu_per_gib" "$gib" "${requests:-0}" "$(jget healthz_probes)" "$(jget healthz_worst_ms)"

if [ -n "${SOAK_OUT:-}" ]; then
  # Values are passed as ARGV STRINGS and coerced in Python, never interpolated into source: a
  # Prometheus summary can legitimately render NaN / +Inf, which are not Python literals, so
  # interpolating them would NameError and — with `set -uo pipefail` (no -e) — still print
  # "results written" while exiting 0 with no file. A silent missing artifact is the worst outcome.
  "$PY" - "$SOAK_JSON" "$SOAK_OUT" \
      "${rss_mid:-0}" "${rss_late:-0}" "${rss_pct:-0}" "${rss_peak:-0}" \
      "${fd_mid:-0}" "${fd_late:-0}" "${fd_pct:-0}" "${fd_peak:-0}" \
      "${th_mid:-0}" "${th_late:-0}" "${th_pct:-0}" "${thread_peak:-0}" \
      "${wal_mid:-0}" "${wal_late:-0}" "${wal_pct:-0}" "${wal_peak:-0}" \
      "${wal_minlate:-0}" "${wal_maxmid:-0}" \
      "${stg_mid:-0}" "${stg_late:-0}" "${stg_pct:-0}" "${staging_peak:-0}" \
      "${stg_minlate:-0}" "${stg_maxmid:-0}" \
      "${sess_mid:-0}" "${sess_late:-0}" "${sess_peak:-0}" \
      "${cpu_secs:-0}" "${cpu_per_gib:-0}" "${gib:-0}" "${fivexx_delta:-0}" "${requests:-0}" \
      "${commit_p99:-0}" "${batch_mean:-0}" "${wq_peak:-0}" "${samples:-0}" \
      "${DRIVER_RC}" "${GATE_FAIL:-none}" <<'PYEOF'
import json, sys, math

def num(v):
    """Coerce a scraped value to a JSON-safe number (NaN/Inf/garbage -> 0)."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0
    if math.isnan(f) or math.isinf(f):
        return 0
    return int(f) if f.is_integer() else f

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        adv = json.load(fh)
except Exception:
    adv = {}
a = sys.argv
keys = ["rss_mid_kib", "rss_late_kib", "rss_climb_pct", "rss_peak_kib",
        "fd_mid", "fd_late", "fd_climb_pct", "fd_peak",
        "thread_mid", "thread_late", "thread_climb_pct", "thread_peak",
        "wal_mid_bytes", "wal_late_bytes", "wal_climb_pct", "wal_peak_bytes",
        "wal_min_late", "wal_max_mid",
        "staging_mid_bytes", "staging_late_bytes", "staging_climb_pct", "staging_peak_bytes",
        "staging_min_late", "staging_max_mid",
        "session_rows_mid", "session_rows_late", "session_rows_peak",
        "cpu_secs", "cpu_secs_per_gib", "gib_moved", "http_5xx", "requests",
        "commit_p99", "batch_mean", "wq_peak", "samples", "driver_rc"]
adv.update({k: num(v) for k, v in zip(keys, a[3:3 + len(keys)])})
adv["gates"] = a[3 + len(keys)]
with open(sys.argv[2], "w", encoding="utf-8") as fh:
    json.dump(adv, fh, indent=1)
PYEOF
  note "results written to $SOAK_OUT"
fi

printf '\n=== verdict ===\n'
if [ -z "$GATE_FAIL" ]; then
  printf 'PASS: %ss of CONSTANT mixed-feature load (%s ops, %s workers) — SSE + versioning +\n' \
    "$(jget elapsed_secs)" "$(jget total_ops)" "$(jget workers)"
  printf '      composite multipart + object-lock + STS + lifecycle churn, all concurrent.\n'
  printf '      Correctness held throughout (byte-exact reads, WORM unbroken, 0 errors, %s/%s 5xx);\n' \
    "$fivexx_delta" "$declared_5xx"
  printf '      steady state did not climb: RSS %s%%, fd %s%%, threads %s%%; staging and WAL plateaued;\n' \
    "$rss_pct" "$fd_pct" "$th_pct"
  printf '      under every ceiling (RSS %s/%s KiB, fd %s/%s, threads %s/%s, WAL %s/%s B).\n' \
    "$rss_peak" "$RSS_CEILING_KIB" "$fd_peak" "$FD_CEILING" "$thread_peak" "$THREAD_CEILING" \
    "$wal_peak" "$WAL_CEILING_BYTES"
  exit 0
fi
printf 'FAIL: gates:%s\n' "$GATE_FAIL" >&2
log_tail
exit 1
