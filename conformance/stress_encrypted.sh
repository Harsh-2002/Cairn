#!/usr/bin/env bash
# Cairn ENCRYPTED-path stress + A/B harness (ARCH 27 + ARCH 30) — the missing load coverage for the
# AES-GCM read/write/assemble paths. Every other stress harness runs with encryption OFF, so the
# newest and most CPU-heavy code in the data plane has had zero throughput / CPU / stability
# coverage. This one runs the SAME warp profile TWICE on the same box, back to back:
#
#   LEG A (plaintext)  CAIRN_ENCRYPT_AT_REST unset  -> committed blobs are plain CRNB containers
#   LEG B (encrypted)  CAIRN_ENCRYPT_AT_REST=true   -> EVERY committed blob is a VERSION_ENCRYPTED
#                                                      CRNB container (sealed DEK + AES-256-GCM)
#
# Transparent at-rest needs no SSE request headers, which is exactly why it is the A/B vehicle: warp
# drives both legs completely unmodified, so the only variable between them is the crypto path.
#
# Why the read sizes matter: an encrypted object is structurally DISQUALIFIED from both GET fast
# paths — the `fast-io` sendfile zero-copy path (the bytes on disk are not the bytes on the wire) and
# the small-object inline read. So an encrypted GET always falls back to the streamed read, which
# runs on a spawn_blocking task holding an owned read permit for the whole client-paced transfer.
# The read phases therefore probe BOTH sides of the small-object threshold (64 KiB and 1 MiB): if
# losing the fast paths ever turns into permit starvation or a stall, it shows up here.
#
# On top of the two warp legs, leg B runs a small explicit-SSE arm (warp cannot send SSE headers):
# a concurrent PUT/GET loop with `x-amz-server-side-encryption: AES256` and with `aws:kms`
# (+ CAIRN_KMS_KEY_IDS allow-listing the id), asserting byte-exact round-trips under concurrency.
# Its job is correctness-under-concurrency for the seal/open path, not throughput.
#
# GATED (hard, load-INDEPENDENT — these hold no matter how much load a contended runner actually
# absorbed, which is the lesson from stress.sh):
#   * CORRECTNESS UNDER LOAD — after the encrypted leg, objects written under concurrency read back
#     BYTE-EXACT, and a committed blob on disk is a VERSION_ENCRYPTED CRNB container with a known
#     plaintext marker ABSENT. A crypto regression under concurrency fails here first.
#   * zero warp operation errors in BOTH legs; zero HTTP 5xx in BOTH legs; server alive in both legs.
#   * absolute CEILINGS in both legs: RSS, open fds, threads, summed WAL bytes.
#   * every warp phase must produce a PARSED non-zero throughput in both legs (a phase that measured
#     nothing is a failure, not a free pass — it would otherwise silently disable the ratio check).
#   * on a RELEASE build only: a DELIBERATELY LOOSE catastrophic-collapse floor — encrypted
#     throughput at least ENC_MIN_RATIO_PCT percent of the plaintext leg's, per phase (default 10).
#     This exists ONLY to catch "encryption made it 100x slower / deadlocked / serialised the whole
#     server"; a healthy release ratio (measured here: 65-135%) clears 10% by a mile. On a DEBUG
#     build the ratio is ADVISORY, never gated — see the build-profile note below.
# ADVISORY (printed, never gating): the enc/plaintext throughput ratio per phase, CPU-seconds and
# CPU-seconds/GiB per leg and their ratio, RSS/fd/thread deltas between legs, and the writer's
# group-commit tail + mean batch size per leg. Those are all absolute or hardware-bound numbers —
# they are the interesting output, but gating on them would produce flaky red CI.
#
# BUILD PROFILE MATTERS for the ratio (and only for the ratio): a debug binary's AES-GCM is
# unoptimized software crypto. Measured on this repo with an identical profile: release enc/plain =
# 135% / 126% / 65% (write / 64KiB read / 1MiB read), the same run on the debug binary = 34% / 5.7% /
# 0.7%. A debug ratio therefore says nothing about the code, so on debug the ratio is REPORTED and
# NOT gated. (It is also not contention-invariant: leg B is CPU-bound while leg A is syscall-bound,
# so a busy 2-core runner squeezes leg B harder.) CI drives the shared DEBUG artifact, like every
# other conformance job — so in CI the load-independent gates below are what protect the encrypted
# path, and the ratio is diagnostic output. Set ENC_MIN_RATIO_PCT to force a floor on any profile.
#
# NOTE on the advisory per-leg numbers: leg B's CPU/RSS/request counters include the explicit-SSE
# correctness arm, which leg A has no equivalent of — so the CPU-s/GiB ratio is an over-estimate of
# pure warp-path cost. It is advisory precisely because of caveats like this.
#
# Usage:
#   conformance/stress_encrypted.sh                          # default profile (~2-3 min)
#   BIN=target/release/cairn conformance/stress_encrypted.sh
#   DURATION=30s CONCURRENT=32 conformance/stress_encrypted.sh
#   STRESS_ENC_OUT=/tmp/cairn-stress-enc.json conformance/stress_encrypted.sh
#   SKIP_SSE_ARM=1 conformance/stress_encrypted.sh           # warp legs only (operator-requested;
#                                                            # an AUTOMATIC skip, i.e. boto3 missing
#                                                            # when not requested, FAILS the run)
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9102}"
REGION="${REGION:-us-east-1}"
KEY_ID="${KEY_ID:-alias/cairn-stress}"

# --- profile (CI-tractable by default; every knob is env-overridable for a longer manual run) ----
CONCURRENT="${CONCURRENT:-8}"
DURATION="${DURATION:-8s}"        # per warp phase, per leg (3 phases x 2 legs)
WRITE_SIZE="${WRITE_SIZE:-64KiB}" # small objects maximise writer/commit (seal) pressure
READ_SMALL="${READ_SMALL:-64KiB}" # at/below the small-object inline read threshold (256 KiB)
READ_LARGE="${READ_LARGE:-1MiB}"  # above it — the streamed read path
OBJECTS="${OBJECTS:-120}"         # pool size warp prepares for each get phase

# Collapse floor. Deliberately loose — see the header. Only "encryption broke the server" trips it.
# The default is BUILD-PROFILE-AWARE (resolved below once $BIN is known), because a DEBUG binary's
# AES-GCM is unoptimized software crypto: measured on this repo, release runs at 65-135% of the
# plaintext leg while the very same debug binary runs at 0.7-34%. Gating a debug run at 10% would be
# a guaranteed red CI that says nothing about the code. Set ENC_MIN_RATIO_PCT explicitly to override.
ENC_MIN_RATIO_PCT="${ENC_MIN_RATIO_PCT:-}"

# Absolute ceilings — same knobs/defaults as stress.sh so the two harnesses agree on "runaway".
RSS_CEILING_KIB="${RSS_CEILING_KIB:-1048576}"
FD_CEILING="${FD_CEILING:-4096}"
THREAD_CEILING="${THREAD_CEILING:-512}"
WAL_CEILING_BYTES="${WAL_CEILING_BYTES:-$((512*1024*1024))}"

# Explicit-SSE correctness arm (leg B only). Small on purpose: correctness, not throughput.
SSE_OBJECTS="${SSE_OBJECTS:-48}"
SSE_WORKERS="${SSE_WORKERS:-8}"
SKIP_SSE_ARM="${SKIP_SSE_ARM:-}"
# Provenance matters: an OPERATOR who asked to skip the arm gets a warp-only run that can still
# PASS, but an AUTOMATIC skip (boto3 missing where it was expected) must FAIL the run — otherwise
# a CI box that quietly lost boto3 would go green without ever proving encryption correctness.
SSE_ARM_SKIP_REQUESTED="${SKIP_SSE_ARM:-}"

WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"
WARP="${WARP:-}"
DATA="$(mktemp -d)"

export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"
export CAIRN_MASTER_KEY="${CAIRN_MASTER_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"

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

# --- 0. warp binary (shared cache with warp.sh / stress.sh) ----------------------------------
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
[ -x "$BIN" ] || fail "binary not found: $BIN (build it: cargo build --bin cairn)"

# Resolve the build profile: an unstripped debug `cairn` is ~290 MB and lives under target/debug.
# Either signal is enough; both are cheap and neither needs the binary to run.
# NOTE: CI uploads a STRIPPED debug binary (~25 MB), so the size signal never fires there — the
# path match is what classifies it. BUILD_PROFILE=... overrides both if you drive an odd path.
BUILD_PROFILE="${BUILD_PROFILE:-}"
if [ -z "$BUILD_PROFILE" ]; then
  BUILD_PROFILE=release
  case "$BIN" in */debug/*) BUILD_PROFILE=debug ;; esac
  bin_bytes="$(stat --format=%s "$BIN" 2>/dev/null || echo 0)"
  [ "${bin_bytes:-0}" -gt $((100*1024*1024)) ] 2>/dev/null && BUILD_PROFILE=debug
fi
# The enc/plaintext ratio is only GATED on a release build. A debug binary's AES-GCM is unoptimized
# software crypto (measured here: release 65-135% of plaintext, the same debug binary 0.7-34%), so a
# debug ratio carries no signal about the code — gating it would be pure flake. Worse, the two legs
# are not equally contention-sensitive (leg B is CPU-bound, leg A syscall-bound), so a busy 2-core
# runner compresses leg B harder and the ratio is NOT contention-invariant. On debug we therefore
# REPORT the ratio and gate only on the load-independent signals (correctness, errors, 5xx,
# liveness, ceilings). Set ENC_MIN_RATIO_PCT explicitly to force a floor on any profile.
ENC_RATIO_GATED=yes
if [ -z "$ENC_MIN_RATIO_PCT" ]; then
  if [ "$BUILD_PROFILE" = debug ]; then ENC_RATIO_GATED=no; ENC_MIN_RATIO_PCT=0
  else ENC_MIN_RATIO_PCT=10; fi
fi
if [ "$ENC_RATIO_GATED" = yes ]; then
  note "build profile: $BUILD_PROFILE — collapse floor ENC_MIN_RATIO_PCT=${ENC_MIN_RATIO_PCT}% (GATED)"
else
  note "build profile: $BUILD_PROFILE — enc/plaintext ratio is ADVISORY (debug crypto is unoptimized)"
fi

# The explicit-SSE arm needs boto3; without it, keep the warp A/B and skip only that arm.
if [ -z "$SKIP_SSE_ARM" ]; then
  if ! command -v "$PY" >/dev/null 2>&1 || ! "$PY" -c "import boto3" 2>/dev/null; then
    note "boto3 not importable by '$PY' — SKIPPING the explicit-SSE arm (warp A/B still runs)"
    SKIP_SSE_ARM=1
  fi
fi

CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# --- /metrics scrape helpers (ported verbatim from stress.sh) ---------------------------------
scrape_sum() { # <metric-name regex>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 ~ m {s += $2} END {printf "%.0f", s+0}'
}
scrape_5xx() {
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk '/^cairn_requests_total\{/ && /status="5[0-9][0-9]"/ {s+=$2} END {printf "%d", s+0}'
}
scrape_quantile() { # <metric> <quantile-string>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" -v q="$2" '$0 ~ ("^" m "\\{") && index($0, "quantile=\"" q "\"") {print $2; exit}'
}
scrape_val() { # <exact-metric-name>
  curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null \
    | awk -v m="$1" '$1 == m {print $2; exit}'
}
scrape_commit_max() {
  local v; v="$(scrape_quantile cairn_writer_commit_seconds 1)"
  [ -z "$v" ] && v="$(scrape_quantile cairn_writer_commit_seconds 1.0)"
  printf '%s' "$v"
}
col_peak() { awk -F'\t' -v c="$1" 'BEGIN{m=0} {if($c>m)m=$c} END{print m+0}' "$2" 2>/dev/null; }

# Results, keyed "<leg>,<field>".
declare -A R
TOTAL_ERRORS=0
GATE_FAIL=""

# Run one warp op against the currently-booted leg; record obj/s, MiB/s, errors.
# args: <leg> <label> <op> <obj-size> [extra warp args...]
run_phase() {
  local leg="$1" label="$2" op="$3" size="$4"; shift 4
  local out rc objs mibs errs raw
  out="$(cd "$DATA" && "$WARP" "$op" --host "127.0.0.1:$PORT" --access-key "$AK" --secret-key "$SK" \
        --region "$REGION" --bucket "encstress" --concurrent "$CONCURRENT" --noclear \
        --obj.size "$size" "$@" 2>&1)"; rc=$?
  objs="$(printf '%s\n' "$out" | sed -n 's/.*Average:[^,]*, \([0-9.]*\) obj\/s.*/\1/p' | head -1)"
  [ -z "$objs" ] && objs="$(printf '%s\n' "$out" | sed -n 's/.*Cluster Total:[^,]*, \([0-9.]*\) obj\/s.*/\1/p' | head -1)"
  mibs="$(printf '%s\n' "$out" | sed -n 's/.*Average: \([0-9.]*\) MiB\/s.*/\1/p' | head -1)"
  [ -z "$mibs" ] && mibs="$(printf '%s\n' "$out" | sed -n 's/.*Cluster Total: \([0-9.]*\) MiB\/s.*/\1/p' | head -1)"
  errs="$(printf '%s\n' "$out" | sed -n 's/^Errors: \([0-9]*\).*/\1/p' | tail -1)"; [ -z "$errs" ] && errs=0
  raw="$(printf '%s\n' "$out" | grep -cE '<ERROR>.*(signature does not match|error)' 2>/dev/null || true)"
  local phase_errs=$(( errs + (rc != 0 ? (raw > 0 ? raw : 1) : 0) ))
  TOTAL_ERRORS=$(( TOTAL_ERRORS + phase_errs ))
  R["$leg,${label}_objs"]="${objs:-0}"
  R["$leg,${label}_mibs"]="${mibs:-0}"
  R["$leg,${label}_errors"]="$phase_errs"
  printf '  %-11s obj.size=%-7s conc=%-3s  ->  %8s obj/s  %8s MiB/s  (errors: %s)\n' \
    "$label" "$size" "$CONCURRENT" "${objs:-?}" "${mibs:-?}" "$phase_errs"
}

# Boot one leg, sample it for its whole life, drive the three warp phases, tear it down.
# args: <leg-name A|B>
run_leg() {
  local leg="$1"
  local dir="$DATA/leg$leg" samples="$DATA/leg$leg.tsv" boot
  mkdir -p "$dir"
  export CAIRN_DATA_DIR="$dir/data"
  export CAIRN_DB_PATH="$dir/data/cairn.db"
  export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
  export CAIRN_WEB_ADDR=off
  if [ "$leg" = "B" ]; then
    # Figment is strict: the boolean must be the literal `true`, NOT `1`.
    export CAIRN_ENCRYPT_AT_REST=true
    export CAIRN_KMS_KEY_IDS="$KEY_ID"
  else
    unset CAIRN_ENCRYPT_AT_REST || true
    unset CAIRN_KMS_KEY_IDS || true
  fi

  boot="$("$BIN" bootstrap)" || fail "bootstrap failed (leg $leg)"
  AK="$(echo "$boot" | awk '/Access Key Id/ {print $NF}')"
  SK="$(echo "$boot" | awk '/Secret Access Key/ {print $NF}')"
  [ -n "$AK" ] && [ -n "$SK" ] || fail "could not parse bootstrap credentials (leg $leg)"

  "$BIN" serve >"$dir/server.log" 2>&1 &
  SRV=$!
  local up=""
  for _ in $(seq 1 100); do
    curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && { up=yes; break; }
    kill -0 "$SRV" 2>/dev/null || fail "server exited at startup (leg $leg); log: $(cat "$dir/server.log")"
    sleep 0.1
  done
  [ -n "$up" ] || fail "server never became healthy (leg $leg); log: $(cat "$dir/server.log")"

  local wal_glob="${CAIRN_DB_PATH}*-wal"
  (
    while kill -0 "$SRV" 2>/dev/null; do
      rss="$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' ')"
      wq="$(curl -fsS "http://127.0.0.1:$PORT/metrics" 2>/dev/null | awk '/^cairn_writer_queue_depth/ {print $2; exit}')"
      fd="$(ls "/proc/$SRV/fd" 2>/dev/null | wc -l)"
      th="$(ls "/proc/$SRV/task" 2>/dev/null | wc -l)"
      # shellcheck disable=SC2086
      wal="$(stat --format=%s $wal_glob 2>/dev/null | awk '{s+=$1} END {print s+0}')"
      cpu="$(awk '{print $14 + $15}' "/proc/$SRV/stat" 2>/dev/null)"
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
        "${rss:-0}" "${wq:-0}" "${fd:-0}" "${wal:-0}" "${th:-0}" "${cpu:-0}" >>"$samples"
      sleep 1
    done
  ) &
  SAMPLER=$!

  local mode; [ "$leg" = "B" ] && mode="ENCRYPTED (CAIRN_ENCRYPT_AT_REST=true)" || mode="PLAINTEXT (baseline)"
  printf '\n=== LEG %s — %s : pid %s on 127.0.0.1:%s ===\n' "$leg" "$mode" "$SRV" "$PORT"

  local fivexx_start bytes_start
  fivexx_start="$(scrape_5xx)"; fivexx_start="${fivexx_start:-0}"
  bytes_start="$(scrape_sum '^cairn_bytes_(received|sent)_total')"

  run_phase "$leg" WRITE      put "$WRITE_SIZE" --duration "$DURATION"
  run_phase "$leg" READ_SMALL get "$READ_SMALL" --objects "$OBJECTS" --duration "$DURATION"
  run_phase "$leg" READ_LARGE get "$READ_LARGE" --objects "$OBJECTS" --duration "$DURATION"

  # Correctness-under-load + explicit-SSE arm — encrypted leg only, while the server is still hot.
  R["$leg,correctness"]="n/a"
  if [ "$leg" = "B" ]; then
    if [ -n "$SKIP_SSE_ARM" ]; then
      R["$leg,correctness"]="skipped"
      if [ -n "$SSE_ARM_SKIP_REQUESTED" ]; then
        printf '\n  --- correctness arm SKIPPED (operator-requested SKIP_SSE_ARM) ---\n'
      else
        printf '\n  --- correctness arm SKIPPED (boto3 unavailable) — this FAILS the run ---\n'
        GATE_FAIL="$GATE_FAIL correctness_arm_skipped"
      fi
    else
      printf '\n  --- correctness under concurrency (byte-exact round-trip + on-disk ciphertext) ---\n'
      if "$PY" "$ROOT/conformance/stress_encrypted.py" "$AK" "$SK" "http://127.0.0.1:$PORT" \
           "$CAIRN_DATA_DIR" "$KEY_ID" "$SSE_OBJECTS" "$SSE_WORKERS"; then
        R["$leg,correctness"]="pass"
      else
        R["$leg,correctness"]="FAIL"
        GATE_FAIL="$GATE_FAIL encrypted_correctness"
      fi
    fi
  fi

  local fivexx_end bytes_end
  fivexx_end="$(scrape_5xx)"; fivexx_end="${fivexx_end:-0}"
  bytes_end="$(scrape_sum '^cairn_bytes_(received|sent)_total')"
  R["$leg,http_5xx"]=$(( fivexx_end - fivexx_start < 0 ? 0 : fivexx_end - fivexx_start ))
  R["$leg,commit_p99"]="$(scrape_commit_max)"; [ -z "${R["$leg,commit_p99"]}" ] && R["$leg,commit_p99"]=0
  local bsum bcnt
  bsum="$(scrape_val cairn_writer_batch_size_sum)"; bcnt="$(scrape_val cairn_writer_batch_size_count)"
  R["$leg,batch_mean"]="$(awk -v s="${bsum:-0}" -v c="${bcnt:-0}" 'BEGIN{printf "%.2f", (c>0? s/c : 0)}')"
  R["$leg,requests"]="$(scrape_sum '^cairn_requests_total')"

  local alive="no"
  kill -0 "$SRV" 2>/dev/null && curl -fsS -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && alive="yes"
  R["$leg,alive"]="$alive"

  sleep 1
  kill "$SAMPLER" 2>/dev/null; SAMPLER=""
  kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""

  R["$leg,rss_peak"]="$(col_peak 1 "$samples")"
  R["$leg,wq_peak"]="$(col_peak 2 "$samples")"
  R["$leg,fd_peak"]="$(col_peak 3 "$samples")"
  R["$leg,wal_peak"]="$(col_peak 4 "$samples")"
  R["$leg,thread_peak"]="$(col_peak 5 "$samples")"
  local cpu_first cpu_last
  cpu_first="$(head -1 "$samples" 2>/dev/null | cut -f6)"; cpu_last="$(tail -1 "$samples" 2>/dev/null | cut -f6)"
  R["$leg,cpu_secs"]="$(awk -v a="${cpu_first:-0}" -v b="${cpu_last:-0}" -v t="$CLK_TCK" \
    'BEGIN{printf "%.1f", (b-a)/(t>0?t:100)}')"
  R["$leg,cpu_per_gib"]="$(awk -v c="${R["$leg,cpu_secs"]:-0}" -v b0="${bytes_start:-0}" -v b1="${bytes_end:-0}" \
    'BEGIN{g=(b1-b0)/1073741824.0; printf "%.1f", (g>0? c/g : 0)}')"
  R["$leg,gib"]="$(awk -v b0="${bytes_start:-0}" -v b1="${bytes_end:-0}" 'BEGIN{printf "%.2f", (b1-b0)/1073741824.0}')"
}

run_leg A
run_leg B

# --- gates -----------------------------------------------------------------------------------
# Every gate below is valid REGARDLESS of how much load the runner actually absorbed: correctness,
# error/5xx counts, liveness, absolute ceilings, and a collapse floor two orders of magnitude below
# real AES overhead. Nothing here compares an absolute throughput or a %-climb across changing load.
for leg in A B; do
  [ "${R["$leg,alive"]}" = "yes" ] || GATE_FAIL="$GATE_FAIL leg${leg}_server_dead"
  [ "${R["$leg,http_5xx"]:-0}" -gt 0 ] 2>/dev/null && GATE_FAIL="$GATE_FAIL leg${leg}_http_5xx(${R["$leg,http_5xx"]})"
  [ "${R["$leg,rss_peak"]:-0}" -gt "$RSS_CEILING_KIB" ] 2>/dev/null &&
    GATE_FAIL="$GATE_FAIL leg${leg}_rss_ceiling(${R["$leg,rss_peak"]}>$RSS_CEILING_KIB)"
  [ "${R["$leg,fd_peak"]:-0}" -gt "$FD_CEILING" ] 2>/dev/null &&
    GATE_FAIL="$GATE_FAIL leg${leg}_fd_ceiling(${R["$leg,fd_peak"]}>$FD_CEILING)"
  [ "${R["$leg,thread_peak"]:-0}" -gt "$THREAD_CEILING" ] 2>/dev/null &&
    GATE_FAIL="$GATE_FAIL leg${leg}_thread_ceiling(${R["$leg,thread_peak"]}>$THREAD_CEILING)"
  [ "${R["$leg,wal_peak"]:-0}" -gt "$WAL_CEILING_BYTES" ] 2>/dev/null &&
    GATE_FAIL="$GATE_FAIL leg${leg}_wal_ceiling(${R["$leg,wal_peak"]}>$WAL_CEILING_BYTES)"
done

# --- report ------------------------------------------------------------------------------------
printf '\n=== A/B throughput (ADVISORY — absolute numbers are hardware-bound; %s build) ===\n' \
  "$BUILD_PROFILE"
printf '  %-11s %14s %14s %10s\n' phase "plaintext obj/s" "encrypted obj/s" "enc/plain"
ratio_report=""
for label in WRITE READ_SMALL READ_LARGE; do
  a="${R["A,${label}_objs"]:-0}"; b="${R["B,${label}_objs"]:-0}"
  pct="$(awk -v a="$a" -v b="$b" 'BEGIN{printf "%.1f", (a>0? b*100.0/a : 0)}')"
  printf '  %-11s %14s %14s %9s%%\n' "$label" "$a" "$b" "$pct"
  ratio_report="${ratio_report}${label}:${pct}% "
  # A phase that produced NO parsed throughput is itself a failure (warp `put` exits 0 even when its
  # output format drifts and the obj/s parse yields 0) — and without this the collapse gate below
  # would silently no-op on a==0. Load-independent: zero measured throughput is zero at any load.
  for side in A B; do
    v="${R["$side,${label}_objs"]:-0}"
    awk "BEGIN{exit !($v <= 0)}" && GATE_FAIL="$GATE_FAIL leg${side}_${label}_no_throughput_parsed"
  done
  # Collapse floor — only a catastrophic regression trips it, and only when GATED (release builds).
  if [ "$ENC_RATIO_GATED" = yes ] && awk "BEGIN{exit !($a > 0 && $pct < $ENC_MIN_RATIO_PCT)}"; then
    GATE_FAIL="$GATE_FAIL enc_collapse_${label}(${pct}%<${ENC_MIN_RATIO_PCT}%)"
  fi
done

printf '\n=== per-leg stability ===\n'
printf '  %-9s %10s %8s %8s %14s %10s %12s %8s\n' leg "RSS KiB" fd threads "WAL bytes" "CPU s" "CPU-s/GiB" "5xx"
for leg in A B; do
  name="plaintext"; [ "$leg" = "B" ] && name="encrypted"
  printf '  %-9s %10s %8s %8s %14s %10s %12s %8s\n' "$name" \
    "${R["$leg,rss_peak"]}" "${R["$leg,fd_peak"]}" "${R["$leg,thread_peak"]}" "${R["$leg,wal_peak"]}" \
    "${R["$leg,cpu_secs"]}" "${R["$leg,cpu_per_gib"]}" "${R["$leg,http_5xx"]}"
done
printf '  ceilings (GATED): RSS %s KiB, fd %s, threads %s, WAL %s B — per leg\n' \
  "$RSS_CEILING_KIB" "$FD_CEILING" "$THREAD_CEILING" "$WAL_CEILING_BYTES"

cpu_ratio="$(awk -v a="${R["A,cpu_per_gib"]:-0}" -v b="${R["B,cpu_per_gib"]:-0}" \
  'BEGIN{printf "%.2f", (a>0? b/a : 0)}')"
rss_delta="$(awk -v a="${R["A,rss_peak"]:-0}" -v b="${R["B,rss_peak"]:-0}" \
  'BEGIN{printf "%.1f", (a>0? (b-a)*100.0/a : 0)}')"
fd_delta=$(( ${R["B,fd_peak"]:-0} - ${R["A,fd_peak"]:-0} ))
th_delta=$(( ${R["B,thread_peak"]:-0} - ${R["A,thread_peak"]:-0} ))
printf '\n=== ADVISORY (never gating) ===\n'
printf '  encrypted CPU cost: %s vs %s CPU-s/GiB (x%s)   bytes moved: %s / %s GiB\n' \
  "${R["B,cpu_per_gib"]}" "${R["A,cpu_per_gib"]}" "$cpu_ratio" "${R["A,gib"]}" "${R["B,gib"]}"
printf '  peak RSS delta enc-vs-plain: %s%%   fd delta: %s   thread delta: %s\n' \
  "$rss_delta" "$fd_delta" "$th_delta"
printf '  writer commit p99 max: plaintext=%ss encrypted=%ss   mean batch size: %s / %s\n' \
  "${R["A,commit_p99"]}" "${R["B,commit_p99"]}" "${R["A,batch_mean"]}" "${R["B,batch_mean"]}"
printf '  peak writer queue depth: plaintext=%s encrypted=%s   requests served: %s / %s\n' \
  "${R["A,wq_peak"]}" "${R["B,wq_peak"]}" "${R["A,requests"]}" "${R["B,requests"]}"
if [ "$ENC_RATIO_GATED" = yes ]; then
  printf '  throughput ratios: %s(floor %s%%, GATED as collapse-only)\n' "$ratio_report" "$ENC_MIN_RATIO_PCT"
else
  printf '  throughput ratios: %s(ADVISORY on a %s build — debug crypto is unoptimized)\n' \
    "$ratio_report" "$BUILD_PROFILE"
fi

if [ -n "${STRESS_ENC_OUT:-}" ]; then
  printf '{"plain":{"write_obj_s":%s,"read_small_obj_s":%s,"read_large_obj_s":%s,"write_mib_s":%s,"read_small_mib_s":%s,"read_large_mib_s":%s,"rss_peak_kib":%s,"fd_peak":%s,"thread_peak":%s,"wal_peak_bytes":%s,"cpu_secs":%s,"cpu_secs_per_gib":%s,"gib_moved":%s,"http_5xx":%s,"wq_peak":%s,"commit_p99":%s,"batch_mean":%s},"encrypted":{"write_obj_s":%s,"read_small_obj_s":%s,"read_large_obj_s":%s,"write_mib_s":%s,"read_small_mib_s":%s,"read_large_mib_s":%s,"rss_peak_kib":%s,"fd_peak":%s,"thread_peak":%s,"wal_peak_bytes":%s,"cpu_secs":%s,"cpu_secs_per_gib":%s,"gib_moved":%s,"http_5xx":%s,"wq_peak":%s,"commit_p99":%s,"batch_mean":%s},"ratio_pct":{"write":%s,"read_small":%s,"read_large":%s},"cpu_per_gib_ratio":%s,"rss_delta_pct":%s,"fd_delta":%s,"thread_delta":%s,"correctness":"%s","errors":%s,"min_ratio_pct":%s,"build_profile":"%s"}\n' \
    "${R["A,WRITE_objs"]:-0}" "${R["A,READ_SMALL_objs"]:-0}" "${R["A,READ_LARGE_objs"]:-0}" \
    "${R["A,WRITE_mibs"]:-0}" "${R["A,READ_SMALL_mibs"]:-0}" "${R["A,READ_LARGE_mibs"]:-0}" \
    "${R["A,rss_peak"]:-0}" "${R["A,fd_peak"]:-0}" "${R["A,thread_peak"]:-0}" "${R["A,wal_peak"]:-0}" \
    "${R["A,cpu_secs"]:-0}" "${R["A,cpu_per_gib"]:-0}" "${R["A,gib"]:-0}" "${R["A,http_5xx"]:-0}" \
    "${R["A,wq_peak"]:-0}" "${R["A,commit_p99"]:-0}" "${R["A,batch_mean"]:-0}" \
    "${R["B,WRITE_objs"]:-0}" "${R["B,READ_SMALL_objs"]:-0}" "${R["B,READ_LARGE_objs"]:-0}" \
    "${R["B,WRITE_mibs"]:-0}" "${R["B,READ_SMALL_mibs"]:-0}" "${R["B,READ_LARGE_mibs"]:-0}" \
    "${R["B,rss_peak"]:-0}" "${R["B,fd_peak"]:-0}" "${R["B,thread_peak"]:-0}" "${R["B,wal_peak"]:-0}" \
    "${R["B,cpu_secs"]:-0}" "${R["B,cpu_per_gib"]:-0}" "${R["B,gib"]:-0}" "${R["B,http_5xx"]:-0}" \
    "${R["B,wq_peak"]:-0}" "${R["B,commit_p99"]:-0}" "${R["B,batch_mean"]:-0}" \
    "$(awk -v a="${R["A,WRITE_objs"]:-0}" -v b="${R["B,WRITE_objs"]:-0}" 'BEGIN{printf "%.1f",(a>0?b*100/a:0)}')" \
    "$(awk -v a="${R["A,READ_SMALL_objs"]:-0}" -v b="${R["B,READ_SMALL_objs"]:-0}" 'BEGIN{printf "%.1f",(a>0?b*100/a:0)}')" \
    "$(awk -v a="${R["A,READ_LARGE_objs"]:-0}" -v b="${R["B,READ_LARGE_objs"]:-0}" 'BEGIN{printf "%.1f",(a>0?b*100/a:0)}')" \
    "$cpu_ratio" "$rss_delta" "$fd_delta" "$th_delta" \
    "${R["B,correctness"]}" "$TOTAL_ERRORS" "$ENC_MIN_RATIO_PCT" "$BUILD_PROFILE" >"$STRESS_ENC_OUT"
  note "results written to $STRESS_ENC_OUT"
fi

printf '\n=== verdict ===\n'
[ "$TOTAL_ERRORS" -gt 0 ] && GATE_FAIL="$GATE_FAIL warp_operation_errors($TOTAL_ERRORS)"
if [ -z "$GATE_FAIL" ]; then
  printf 'PASS: encrypted-path stress. Correctness under load: %s (byte-exact round-trip + on-disk\n' \
    "${R["B,correctness"]}"
  printf '      VERSION_ENCRYPTED CRNB, plaintext marker absent). 0 op-errors, 0 HTTP 5xx, alive in\n'
  printf '      both legs, under every ceiling. Encrypted/plaintext obj/s: %s\n' "$ratio_report"
  exit 0
fi
printf 'FAIL: gates:%s\n' "$GATE_FAIL" >&2
# The EXIT trap removes $DATA, so dump the server logs HERE or a CI failure is undiagnosable.
case "$GATE_FAIL" in
  *server_dead*|*no_throughput_parsed*)
    for leg in A B; do
      log="$DATA/leg$leg/server.log"
      [ -f "$log" ] && printf '\n--- leg %s server.log (tail) ---\n%s\n' "$leg" "$(tail -20 "$log")" >&2
    done
    ;;
esac
exit 1
