#!/usr/bin/env bash
# Cairn CONCURRENT-MULTIPART stress harness (ARCH 27 + ARCH 30) — the missing load coverage for the
# multipart write path. `multipart.sh` walks the session-state contract serially, `encryption.sh`
# proves one SSE multipart is ciphertext on disk, and the warp-driven stress harnesses
# (`stress.sh`, `stress_encrypted.sh`) never issue a CompleteMultipartUpload at all — so the
# concurrent path has had zero coverage. That path is exactly where the interesting contention is:
#
#   * `LocalBlobStore::assemble` holds ONE of the 64 blob-WRITE permits across its entire SERIAL
#     `assemble_into` loop, and with part-level encryption (ARCH 27 Increment 3a) that loop is a
#     decrypt-then-re-encrypt pass over every staged part. Single-part PUTs (`stage`) draw from the
#     SAME 64-permit pool, so N concurrent assembles are the shape that could starve ordinary writes.
#   * `ClaimMultipart` is the complete/abort race; `RecordPart` is an `INSERT OR REPLACE` supersede
#     race; `UploadPartCopy` stages through the seal path under load. None were exercised anywhere.
#   * Every staged part is its own CRNB blob under its own random per-part DEK, and Complete opens
#     every part key BEFORE claiming the session, so a bad key leaves the upload retryable.
#
# The driver (`stress_multipart.py`, boto3 + threads) runs four scenarios against one hot node while
# this script samples the server process every second, exactly like `stress.sh`:
#
#   1. BARRIER-FIRED CONCURRENT COMPLETE vs a steady single-part PUT stream (the headline).
#   2. FAIL-CLOSED under concurrency: one session in that same barrier has a staged part corrupted
#      on disk (one flipped ciphertext byte) before the barrier fires.
#   3. SESSION-STATE RACES: Complete vs Abort on one upload id; RecordPart supersede.
#   4. UPLOADPARTCOPY under concurrency, with the staged-part on-disk ciphertext proof.
#
# GATED (hard, and every one of them valid REGARDLESS OF OFFERED LOAD — the lesson `stress.sh` and
# `stress_encrypted.sh` encode, so these hold on a contended 2-core runner):
#   * CORRECTNESS — every concurrent Complete returns 200 and its object GETs back BYTE-EXACT;
#     every background single-part PUT succeeds and reads back byte-exact.
#   * FAIL-CLOSED ISOLATION — the poisoned session's Complete errors, NO object is committed for its
#     key, and every other session in the same barrier still completes byte-exact.
#   * EXACT ERROR CODES — the loser of the complete/abort race returns a well-formed S3 code
#     (NoSuchUpload / InvalidRequest / InvalidPart / NoSuchKey); a superseded part ETag is rejected
#     `InvalidPart` 400; and after every race the upload id is RESOLVED (ListParts -> NoSuchUpload)
#     with NO staging orphans left on disk.
#   * ALL-OR-NOTHING — a failed Complete never leaves an object for the key; a successful one is
#     byte-exact. Never a torn object, under any interleaving.
#   * PROGRESS, not throughput — ordinary single-part PUTs must complete AT ALL during the barrier
#     (a count > 0, never a rate), which is what "concurrent assembles did not starve writes" means
#     in a load-independent way.
#   * LIVENESS — `/healthz` never stops answering (a 60 s per-probe WEDGE timeout, deliberately not a
#     latency budget: 12 concurrent debug-build assembles really do starve a 2-core runner, and
#     gating on probe latency would be gating on offered load); server alive at the end.
#   * ABSOLUTE CEILINGS — RSS / open fds / threads / summed WAL bytes (same knobs and defaults as
#     `stress.sh`, so the harnesses agree on what "runaway" means).
#   * HTTP 5xx == EXACTLY the count the driver DECLARED. Two operations here may legitimately answer
#     5xx and both are declared: scenario 2's poisoned Complete (a tampered stored part is an
#     integrity failure, so `BlobError::Corruption` -> `Error::Internal` -> 500 is the CORRECT
#     fail-closed answer), and the known gap below. Gating on equality keeps it exact — a 5xx from
#     any other request still fails the run.
#
# KNOWN GAP (pinned and reported by the driver, deliberately NOT gated — this is a harness-only
# change, so a pre-existing product gap is recorded, not turned into red CI): when Abort wins the
# `ClaimMultipart` race AFTER Complete has claimed the session and entered `assemble`, the abort's
# `delete_session` removes the staged part files out from under the running assembly and the loser
# answers 500 InternalError instead of the AWS-shaped NoSuchUpload. It is fail-closed (the harness
# still gates that nothing is committed, nothing is torn, and no staging orphan survives) — only the
# error code is wrong. Fixing it belongs to a product change.
#
# ADVISORY (printed + written to STRESS_MP_OUT, never gating): background-PUT throughput with vs
# without the Complete barrier and their ratio, Complete wall times (min/med/max), writer commit p99
# and mean batch size, peak writer queue depth, CPU-seconds, staging time and bytes. All of those
# move with runner contention AND with the build profile — CI drives the shared DEBUG artifact,
# whose AES-GCM is unoptimized software crypto and whose assemble is a decrypt+re-encrypt pass, so a
# crypto-throughput number here says nothing about the code. Reported, never gated.
#
# PART-SIZE TRADEOFF (why the default profile is what it is): S3 requires every NON-FINAL part to be
# >= 5 MiB, so a genuinely multi-part Complete cannot be made cheap. The default profile pays that
# 5 MiB exactly where a multi-part assemble is under test (scenarios 1/2/4 and the supersede
# session) and uses a small final tail part everywhere else; the complete/abort race sessions are
# single-part, because what they test is session state, not assembly. Everything is env-tunable.
#
# Usage:
#   conformance/stress_multipart.sh                             # default profile (~2-4 min)
#   BIN=target/release/cairn conformance/stress_multipart.sh
#   MP_SESSIONS=24 MP_BG_WORKERS=12 conformance/stress_multipart.sh
#   STRESS_MP_OUT=/tmp/cairn-stress-mp.json conformance/stress_multipart.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9104}"
KEY_ID="${KEY_ID:-alias/cairn-mpstress}"

# Absolute ceilings — same knobs/defaults as stress.sh / stress_encrypted.sh.
RSS_CEILING_KIB="${RSS_CEILING_KIB:-1048576}"
FD_CEILING="${FD_CEILING:-4096}"
THREAD_CEILING="${THREAD_CEILING:-512}"
WAL_CEILING_BYTES="${WAL_CEILING_BYTES:-$((512*1024*1024))}"

DATA="$(mktemp -d)"
export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
export CAIRN_WEB_ADDR=off
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"
export CAIRN_MASTER_KEY="${CAIRN_MASTER_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"
# The sessions request `aws:kms`, so the key id must be on the write-time allow-list (label-only KMS).
export CAIRN_KMS_KEY_IDS="$KEY_ID"
# Pin the server request timeout well above any plausible assemble time on a contended runner. If
# left ambient, a slow box could yield a server-side 503 RequestTimeout, which would break BOTH the
# "every Complete returned 200" gate and the exact-5xx-count gate at once — a load-dependent flake.
export CAIRN_REQUEST_TIMEOUT_SECS="${CAIRN_REQUEST_TIMEOUT_SECS:-600}"

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
# boto3 is NOT optional here: it IS the harness. A CI box that quietly lost boto3 must go red rather
# than green-with-no-coverage (the provenance lesson from stress_encrypted.sh).
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by '$PY' — this harness IS the boto3 driver"

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
col_peak() { awk -F'\t' -v c="$1" 'BEGIN{m=0} {if($c>m)m=$c} END{print m+0}' "$SAMPLES" 2>/dev/null; }
# A JSON scalar out of the driver's advisory file (flat object, no nesting) — no jq dependency.
jget() { # <key>
  "$PY" - "$1" "$MP_JSON" <<'PYEOF' 2>/dev/null
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
printf '\n=== cairn (pid %s) on 127.0.0.1:%s — %s, %s cores ===\n' \
  "$SRV" "$PORT" "$(uname -m)" "$(nproc)"

# --- 2. per-second sampler (same six columns as stress.sh) ------------------------------------
SAMPLES="$DATA/samples.tsv"
WAL_GLOB="${CAIRN_DB_PATH}*-wal"
(
  while kill -0 "$SRV" 2>/dev/null; do
    rss="$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' ')"
    wq="$(curl -fsS --max-time 5 "http://127.0.0.1:$PORT/metrics" 2>/dev/null | awk '/^cairn_writer_queue_depth/ {print $2; exit}')"
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

fivexx_start="$(scrape_5xx)"; fivexx_start="${fivexx_start:-0}"
bytes_start="$(scrape_sum '^cairn_bytes_(received|sent)_total')"

# --- 3. the driver -----------------------------------------------------------------------------
MP_JSON="$DATA/driver.json"
DRIVER_RC=0
"$PY" "$ROOT/conformance/stress_multipart.py" "$AK" "$SK" "http://127.0.0.1:$PORT" \
  "$CAIRN_DATA_DIR" "$KEY_ID" "$MP_JSON" || DRIVER_RC=$?

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
kill -0 "$SRV" 2>/dev/null && curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && alive="yes"

sleep 1
kill "$SAMPLER" 2>/dev/null; SAMPLER=""
rss_peak="$(col_peak 1)"; wq_peak="$(col_peak 2)"; fd_peak="$(col_peak 3)"
wal_peak="$(col_peak 4)"; thread_peak="$(col_peak 5)"
cpu_first="$(head -1 "$SAMPLES" 2>/dev/null | cut -f6)"; cpu_last="$(tail -1 "$SAMPLES" 2>/dev/null | cut -f6)"
cpu_secs="$(awk -v a="${cpu_first:-0}" -v b="${cpu_last:-0}" -v t="$CLK_TCK" \
  'BEGIN{printf "%.1f", (b-a)/(t>0?t:100)}')"
gib="$(awk -v b0="${bytes_start:-0}" -v b1="${bytes_end:-0}" 'BEGIN{printf "%.2f", (b1-b0)/1073741824.0}')"

declared_5xx="$(jget declared_5xx)"; declared_5xx="${declared_5xx:-0}"
case "$declared_5xx" in ''|*[!0-9]*) declared_5xx=0 ;; esac

# --- 5. gates -------------------------------------------------------------------------------------
GATE_FAIL=""
[ "$DRIVER_RC" -eq 0 ] || GATE_FAIL="$GATE_FAIL driver_assertions(rc=$DRIVER_RC)"
[ "$alive" = "yes" ] || GATE_FAIL="$GATE_FAIL server_dead"
# Zero UNEXPECTED 5xx. Exactly two operations here may legitimately answer 5xx — scenario 2's
# fail-closed integrity rejection, and the pinned known gap where Abort deletes a losing Complete's
# staged bytes mid-assemble — and the driver reports how many of those it actually OBSERVED. Gating
# on equality keeps the check exact: any 5xx from any OTHER request still fails the run.
[ "$fivexx_delta" -ne "$declared_5xx" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL unexpected_http_5xx($fivexx_delta!=$declared_5xx)"
[ "${rss_peak:-0}" -gt "$RSS_CEILING_KIB" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL rss_ceiling(${rss_peak}>$RSS_CEILING_KIB)"
[ "${fd_peak:-0}" -gt "$FD_CEILING" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL fd_ceiling(${fd_peak}>$FD_CEILING)"
[ "${thread_peak:-0}" -gt "$THREAD_CEILING" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL thread_ceiling(${thread_peak}>$THREAD_CEILING)"
[ "${wal_peak:-0}" -gt "$WAL_CEILING_BYTES" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL wal_ceiling(${wal_peak}>$WAL_CEILING_BYTES)"

# --- 6. report ---------------------------------------------------------------------------------
printf '\n=== server stability (GATED on absolute ceilings) ===\n'
printf '  %10s %8s %8s %14s %10s %8s\n' "RSS KiB" fd threads "WAL bytes" "CPU s" "5xx"
printf '  %10s %8s %8s %14s %10s %8s\n' \
  "${rss_peak:-0}" "${fd_peak:-0}" "${thread_peak:-0}" "${wal_peak:-0}" "${cpu_secs:-0}" "$fivexx_delta"
printf '  ceilings: RSS %s KiB, fd %s, threads %s, WAL %s B; 5xx budget %s (declared fail-closed)\n' \
  "$RSS_CEILING_KIB" "$FD_CEILING" "$THREAD_CEILING" "$WAL_CEILING_BYTES" "$declared_5xx"

printf '\n=== ADVISORY (never gating — contention- and build-profile-bound) ===\n'
printf '  background single-part PUT: %s ops/s alone -> %s ops/s during the Complete barrier (%s%%)\n' \
  "$(jget bg_put_baseline_ops_s)" "$(jget bg_put_during_barrier_ops_s)" "$(jget bg_put_barrier_ratio_pct)"
printf '  Complete wall time (%s concurrent sessions): min %ss  median %ss  max %ss   barrier span %ss\n' \
  "$(jget sessions)" "$(jget complete_wall_min)" "$(jget complete_wall_med)" \
  "$(jget complete_wall_max)" "$(jget barrier_wall_secs)"
printf '  staging: %s B in %ss   complete/abort race outcomes: %s\n' \
  "$(jget staged_bytes)" "$(jget stage_secs)" "$(jget race_outcomes)"
printf '  writer commit p99 max: %ss   mean batch size: %s   peak queue depth: %s\n' \
  "$commit_p99" "$batch_mean" "${wq_peak:-0}"
printf '  requests served: %s   bytes moved: %s GiB   /healthz probes: %s (worst %s ms)\n' \
  "${requests:-0}" "$gib" "$(jget healthz_probes)" "$(jget healthz_worst_ms)"
printf '  poisoned Complete rejected with: HTTP %s %s   driver wall time: %ss\n' \
  "$(jget poison_status)" "$(jget poison_code)" "$(jget driver_secs)"
printf '  known gaps pinned (reported, NOT gated): %s\n' "$(jget known_gaps)"

if [ -n "${STRESS_MP_OUT:-}" ]; then
  # Values are passed as ARGV STRINGS and coerced in Python, never interpolated into source: a
  # Prometheus summary can legitimately render NaN / +Inf, which are not Python literals, so the old
  # heredoc-interpolation would NameError and — with `set -uo pipefail` (no -e) — still print
  # "results written" and exit 0 with no file. A silent missing artifact is the worst outcome.
  "$PY" - "$MP_JSON" "$STRESS_MP_OUT" \
      "${rss_peak:-0}" "${fd_peak:-0}" "${thread_peak:-0}" "${wal_peak:-0}" "${wq_peak:-0}" \
      "${cpu_secs:-0}" "${gib:-0}" "${fivexx_delta:-0}" "${requests:-0}" \
      "${commit_p99:-0}" "${batch_mean:-0}" "${DRIVER_RC}" "${GATE_FAIL:-none}" <<'PYEOF'
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
adv.update({
    "rss_peak_kib": num(a[3]), "fd_peak": num(a[4]), "thread_peak": num(a[5]),
    "wal_peak_bytes": num(a[6]), "wq_peak": num(a[7]), "cpu_secs": num(a[8]),
    "gib_moved": num(a[9]), "http_5xx": num(a[10]), "requests": num(a[11]),
    "commit_p99": num(a[12]), "batch_mean": num(a[13]),
    "driver_rc": num(a[14]), "gates": a[15],
})
with open(sys.argv[2], "w", encoding="utf-8") as fh:
    json.dump(adv, fh, indent=1)
PYEOF
  note "results written to $STRESS_MP_OUT"
fi

printf '\n=== verdict ===\n'
if [ -z "$GATE_FAIL" ]; then
  printf 'PASS: concurrent multipart. %s barrier-fired SSE Completes all byte-exact alongside a live\n' \
    "$(jget sessions)"
  printf '      single-part PUT stream (%s PUTs, %s during the barrier, 0 errors); the poisoned session\n' \
    "$(jget bg_put_total)" "$(jget bg_put_during_barrier_ops)"
  printf '      failed CLOSED (HTTP %s %s, no object committed) while %s siblings completed; complete/abort\n' \
    "$(jget poison_status)" "$(jget poison_code)" "$(jget poison_isolated_ok)"
  printf '      + supersede races resolved with exact S3 codes; UploadPartCopy parts ciphertext on disk.\n'
  printf '      0 unexpected 5xx, alive, under every ceiling.\n'
  exit 0
fi
printf 'FAIL: gates:%s\n' "$GATE_FAIL" >&2
case "$GATE_FAIL" in
  *server_dead*|*driver_assertions*|*unexpected_http_5xx*) log_tail ;;
esac
exit 1
