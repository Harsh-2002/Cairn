#!/usr/bin/env bash
# Cairn ADVERSARIAL / LIMIT stress harness (ARCH 21 + ARCH 25 + ARCH 30) — the missing coverage for
# the REJECTION paths under load. Every limit and abuse path in this repo is otherwise probed
# SERIALLY, one request at a time: `blob_limits.sh` fills a tmpfs with one PUT, `objects.py` sends
# one bad Content-MD5, `sts.py` tampers with one token, `listing.py` walks one continuation loop on
# an otherwise idle server. Nothing drives a limit or a malformed request CONCURRENTLY — which is
# exactly where a check can be raced past, where a rejected request can damage a healthy one in
# flight beside it, and where a bounded loop can stop being bounded.
#
# The driver (`stress_adversarial.py`) runs five scenarios against one hot node while this script
# samples the server process every second, exactly like `stress.sh` / `stress_multipart.sh`:
#
#   1. The SigV4 aws-chunked STREAMING DECODER (crates/cairn-protocol/src/chunked.rs — the F-5
#      component: getting it wrong corrupts objects SILENTLY). One barrier releases many concurrent
#      streaming uploads, MIXED valid (signed and unsigned framing) and deliberately MALFORMED
#      (bad chunk signature, truncated stream, chunk length over- and under-declared, oversized
#      chunk header, non-hex chunk size, missing per-chunk signature). Raw sockets + hand-rolled
#      SigV4 — no SDK will emit a malformed chunk stream, and no SDK will hand back the response to
#      a body the server rejected mid-write.
#   2. QUOTA and CAIRN_MAX_OBJECT_SIZE under concurrent writes.
#   3. DEEP PAGINATION under concurrent mutation of the very prefix being listed.
#   4. STS session-credential CHURN (mint / use / tamper / revoke, all concurrent).
#   5. LARGE-FANOUT DeleteObjects with a mix of present, absent, invalid and duplicated keys.
#
# GATED (hard, and every one valid REGARDLESS OF OFFERED LOAD — the lesson `stress.sh` and
# `stress_multipart.sh` encode, so these hold on a contended 2-core runner):
#   * DECODER — every valid streaming upload returns 200 and GETs back BYTE-EXACT while malformed
#     streams race beside it (a malformed stream must never corrupt a concurrent valid one); every
#     malformed upload is rejected with its EXACT (status, S3 code) pair — never "any 4xx" — never
#     2xx, and commits NO object.
#   * QUOTA — stored bytes NEVER exceed the quota, and with equal-sized objects the admitted count
#     is EXACTLY quota/size (a race that let one extra write through fails even if the byte sum
#     still fits); every rejection is EXACTLY 507 InsufficientStorage; clearing the quota re-admits
#     writes (a rejection is not a wedge). Same shape for CAIRN_MAX_OBJECT_SIZE: every oversized PUT
#     is EXACTLY 400 EntityTooLarge, nothing is committed, a fitting write still round-trips.
#   * PAGINATION — every continuation-token pass TERMINATES inside a bound derived from the
#     KEYSPACE (not from offered load: the churn workers cycle a FIXED key ring, so the number of
#     keys that can exist is fixed no matter how fast the box is), returns no duplicate key in one
#     pass, is strictly increasing, accepts every token, and returns every key that existed for the
#     whole pass.
#   * STS — an in-scope GET always succeeds byte-exact; an ungranted action and a bucket outside the
#     policy are EXACTLY 403 AccessDenied (a session never widens to its parent admin); a tampered
#     token is EXACTLY 403 SignatureDoesNotMatch, an absent one EXACTLY 400 InvalidArgument, and a
#     REVOKED one EXACTLY 403 InvalidAccessKeyId.
#   * DELETEOBJECTS — every request entry yields exactly one outcome, the present/absent split is
#     exact (an absent key is a SUCCESS, S3's delete is idempotent), the single structurally invalid
#     key is the ONLY error and is coded EXACTLY InvalidArgument, a 3×-duplicated key is reported
#     3× without error, and a control namespace nobody asked about survives intact and byte-exact.
#   * LIVENESS — `/healthz` never stops answering (a 60 s per-probe WEDGE timeout, deliberately not
#     a latency budget: gating on probe latency would be gating on offered load); alive at the end.
#   * ABSOLUTE CEILINGS — RSS / open fds / threads / summed WAL bytes (same knobs and defaults as
#     `stress.sh`, so the harnesses agree on what "runaway" means), plus a SAMPLER-SANITY gate: a
#     column that reads ZERO for the whole run FAILS, because a zero peak silently passes a ceiling.
#   * HTTP 5xx == EXACTLY the count the driver DECLARED (see below).
#
# THE 5xx BUDGET, AND WHY IT IS AN EQUALITY. A malformed request should never be a 5xx. Two classes
# here legitimately are, and both are declared by the driver: `507 InsufficientStorage` is the
# S3-correct answer to a quota rejection and simply lives in the 5xx range, and the pinned decoder
# FINDINGS below. Gating on equality keeps it exact — a 5xx from any OTHER request still fails.
#
# FINDINGS (pinned, reported LOUDLY, deliberately NOT gated — this is a harness-only change, so a
# pre-existing product gap is recorded rather than turned into red CI): four of the malformed
# framings (chunk length under-declared, oversized chunk header, non-hex chunk size, missing
# per-chunk signature) answer 500 InternalError instead of a 4xx. They are FAIL-CLOSED — the harness
# still gates that nothing is committed and that no other status is ever returned — but a
# client-caused framing error is surfacing as a server fault: `DecodeError` travels as
# `BlobError::Body(BodyError::Transport(..))`, and only `BodyError::Truncated` has a 4xx arm in
# `impl From<BlobError> for Error` (crates/cairn-types/src/error.rs); everything else falls into the
# blanket `other => Error::Internal`. Fixing that belongs to a product change. The driver reports
# which cases actually deviated, and their count is gated to stay within the rounds run.
#
# ADVISORY (printed + written to STRESS_ADV_OUT, never gating): barrier wall time, pages listed,
# churn ops, driver wall time, writer commit p99 and mean batch size, peak writer queue depth,
# CPU-seconds, bytes moved. All move with runner contention and with the build profile (CI drives
# the DEBUG artifact, whose AES-GCM is unoptimized software crypto), so they are reported, never
# gated.
#
# Usage:
#   conformance/stress_adversarial.sh                            # default profile (~1-4 min)
#   BIN=target/release/cairn conformance/stress_adversarial.sh
#   ADV_PAGE_KEYS=5000 ADV_DEL_PRESENT=400 conformance/stress_adversarial.sh
#   STRESS_ADV_OUT=/tmp/cairn-stress-adv.json conformance/stress_adversarial.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT="${PORT:-9106}"
UIPORT="${UIPORT:-9107}"

# Absolute ceilings — same knobs/defaults as stress.sh / stress_multipart.sh.
RSS_CEILING_KIB="${RSS_CEILING_KIB:-1048576}"
FD_CEILING="${FD_CEILING:-4096}"
THREAD_CEILING="${THREAD_CEILING:-512}"
WAL_CEILING_BYTES="${WAL_CEILING_BYTES:-$((512*1024*1024))}"

DATA="$(mktemp -d)"
export CAIRN_DATA_DIR="$DATA/data"
export CAIRN_DB_PATH="$DATA/data/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$PORT"
# The management API is REQUIRED here (bucket quota + STS mint/revoke), so unlike the other stress
# harnesses the console listener is ON.
export CAIRN_WEB_ADDR="127.0.0.1:$UIPORT"
export CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"
export CAIRN_MASTER_KEY="${CAIRN_MASTER_KEY:-00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff}"
# The object-size ceiling is UNDER TEST, so it is pinned small and handed to the driver as the same
# number. A mismatch would make scenario 2b assert against a ceiling the server does not have.
ADV_MAX_OBJECT_SIZE="${ADV_MAX_OBJECT_SIZE:-1048576}"
export ADV_MAX_OBJECT_SIZE
export CAIRN_MAX_OBJECT_SIZE="$ADV_MAX_OBJECT_SIZE"
# Pin the request timeout well above any plausible operation on a contended runner: left ambient, a
# slow box could answer a server-side 503 RequestTimeout, which would break the exact-5xx-count gate
# AND rewrite a deliberate 4xx assertion — a load-dependent flake in a harness whose whole point is
# load-independent gates.
export CAIRN_REQUEST_TIMEOUT_SECS="${CAIRN_REQUEST_TIMEOUT_SECS:-600}"
# Disable the auth cache so a REVOKED session credential is refused on the very next request. With
# the 30 s default this would be a race against cache expiry — i.e. a timing-dependent gate.
export CAIRN_AUTH_CACHE_TTL_SECS="${CAIRN_AUTH_CACHE_TTL_SECS:-0}"

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
# boto3 is NOT optional: it IS half the harness (the raw-socket decoder scenario is stdlib, but
# every verification path is boto3). A CI box that quietly lost boto3 must go red rather than
# green-with-no-coverage (the provenance lesson from stress_encrypted.sh).
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by '$PY' — this harness needs it"

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
jget() { # <key> — a JSON scalar out of the driver's advisory file (no jq dependency)
  "$PY" - "$1" "$ADV_JSON" <<'PYEOF' 2>/dev/null
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
# The management API is Bearer-authenticated with a dedicated `cairn_<id>.<secret>` token, printed
# on the bootstrap "Authorization: Bearer …" line (see sts.sh).
BEARER="$(echo "$BOOT" | awk '/Authorization: Bearer/ {print $3}')"
[ -n "$AK" ] && [ -n "$SK" ] && [ -n "$BEARER" ] || fail "could not parse bootstrap credentials"
"$BIN" serve >"$DATA/server.log" 2>&1 &
SRV=$!
up=""
for _ in $(seq 1 100); do
  curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$PORT/healthz" 2>/dev/null && { up=yes; break; }
  kill -0 "$SRV" 2>/dev/null || { printf 'FAIL: server exited at startup\n' >&2; log_tail; exit 1; }
  sleep 0.1
done
[ -n "$up" ] || { printf 'FAIL: server never became healthy\n' >&2; log_tail; exit 1; }
printf '\n=== cairn (pid %s) on 127.0.0.1:%s (console :%s) — %s, %s cores ===\n' \
  "$SRV" "$PORT" "$UIPORT" "$(uname -m)" "$(nproc)"

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
ADV_JSON="$DATA/driver.json"
DRIVER_RC=0
"$PY" "$ROOT/conformance/stress_adversarial.py" "$AK" "$SK" "$BEARER" \
  "http://127.0.0.1:$PORT" "http://127.0.0.1:$UIPORT" "$ADV_JSON" || DRIVER_RC=$?

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
samples="$(wc -l <"$SAMPLES" 2>/dev/null | tr -d ' ')"; samples="${samples:-0}"
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
# Zero UNEXPECTED 5xx. The driver declares exactly the ones it provoked on purpose (507 quota
# rejections, plus the pinned malformed-framing InternalErrors); gating on EQUALITY keeps the check
# exact — a 5xx from any other request still fails the run.
[ "$fivexx_delta" -ne "$declared_5xx" ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL unexpected_http_5xx($fivexx_delta!=$declared_5xx)"
# SAMPLER SANITY, gated before the ceilings: a column that read ZERO all run would pass every
# ceiling below (0 < ceiling) and silently disarm them. A live server always has RSS, fds, threads.
{ [ "${samples:-0}" -lt 3 ] || [ "${rss_peak:-0}" -le 0 ] || [ "${fd_peak:-0}" -le 0 ] \
  || [ "${thread_peak:-0}" -le 0 ]; } 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL sampler_read_nothing(n=$samples,rss=$rss_peak,fd=$fd_peak,thr=$thread_peak)"
[ "${requests:-0}" -le 0 ] 2>/dev/null &&
  GATE_FAIL="$GATE_FAIL no_requests_counted($requests)"
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
printf '  %10s %8s %8s %14s %10s %8s %8s\n' "RSS KiB" fd threads "WAL bytes" "CPU s" "5xx" samples
printf '  %10s %8s %8s %14s %10s %8s %8s\n' \
  "${rss_peak:-0}" "${fd_peak:-0}" "${thread_peak:-0}" "${wal_peak:-0}" "${cpu_secs:-0}" \
  "$fivexx_delta" "${samples:-0}"
printf '  ceilings: RSS %s KiB, fd %s, threads %s, WAL %s B; 5xx budget %s (declared: 507 quota + pinned findings)\n' \
  "$RSS_CEILING_KIB" "$FD_CEILING" "$THREAD_CEILING" "$WAL_CEILING_BYTES" "$declared_5xx"

printf '\n=== ADVISORY (never gating — contention- and build-profile-bound) ===\n'
printf '  chunked decoder: %s valid + %s malformed uploads, barrier span %ss\n' \
  "$(jget chunk_valid)" "$(jget chunk_malformed)" "$(jget chunk_barrier_secs)"
printf '  quota: %s B admitted %s PUTs / rejected %s; stored %s B   max-object-size: %s B\n' \
  "$(jget quota_bytes)" "$(jget quota_admitted)" "$(jget quota_rejected)" \
  "$(jget quota_stored_bytes)" "$ADV_MAX_OBJECT_SIZE"
printf '  pagination: %s pages listed under %s concurrent mutations (%ss)\n' \
  "$(jget page_pages_total)" "$(jget page_churn_ops)" "$(jget page_pass_secs)"
printf '  STS sessions churned: %s   DeleteObjects: %s batches × %s keys = %s\n' \
  "$(jget sts_sessions)" "$(jget del_workers)" "$(jget del_keys_per_batch)" "$(jget del_keys_total)"
printf '  writer commit p99 max: %ss   mean batch size: %s   peak queue depth: %s\n' \
  "$commit_p99" "$batch_mean" "${wq_peak:-0}"
printf '  requests served: %s   bytes moved: %s GiB   /healthz probes: %s (worst %s ms)\n' \
  "${requests:-0}" "$gib" "$(jget healthz_probes)" "$(jget healthz_worst_ms)"
printf '  driver wall time: %ss\n' "$(jget driver_secs)"
printf '  FINDINGS pinned (reported, NOT gated): %s\n' "$(jget findings)"

if [ -n "${STRESS_ADV_OUT:-}" ]; then
  # Values are passed as ARGV STRINGS and coerced in Python, never interpolated into source: a
  # Prometheus summary can legitimately render NaN / +Inf, which are not Python literals, so a
  # heredoc interpolation would NameError and — with `set -uo pipefail` (no -e) — still print
  # "results written" and exit 0 with no file. A silent missing artifact is the worst outcome.
  "$PY" - "$ADV_JSON" "$STRESS_ADV_OUT" \
      "${rss_peak:-0}" "${fd_peak:-0}" "${thread_peak:-0}" "${wal_peak:-0}" "${wq_peak:-0}" \
      "${cpu_secs:-0}" "${gib:-0}" "${fivexx_delta:-0}" "${requests:-0}" \
      "${commit_p99:-0}" "${batch_mean:-0}" "${samples:-0}" "${DRIVER_RC}" "${GATE_FAIL:-none}" <<'PYEOF'
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
    "commit_p99": num(a[12]), "batch_mean": num(a[13]), "samples": num(a[14]),
    "driver_rc": num(a[15]), "gates": a[16],
})
with open(sys.argv[2], "w", encoding="utf-8") as fh:
    json.dump(adv, fh, indent=1)
PYEOF
  note "results written to $STRESS_ADV_OUT"
fi

printf '\n=== verdict ===\n'
if [ -z "$GATE_FAIL" ]; then
  printf 'PASS: adversarial limits under load. %s malformed aws-chunked streams each rejected with\n' \
    "$(jget chunk_malformed)"
  printf '      their EXACT expected code (or a pinned, declared deviation) while %s valid streaming\n' \
    "$(jget chunk_valid)"
  printf '      uploads round-tripped BYTE-EXACT beside them; the quota admitted exactly %s PUTs and\n' \
    "$(jget quota_admitted)"
  printf '      never held more than %s B; every oversized PUT was 400 EntityTooLarge; %s listing pages\n' \
    "$(jget quota_bytes)" "$(jget page_pages_total)"
  printf '      terminated with no duplicate key under %s concurrent mutations; %s STS sessions never\n' \
    "$(jget page_churn_ops)" "$(jget sts_sessions)"
  printf '      exceeded their scope and were refused exactly once revoked; %s bulk-deleted keys split\n' \
    "$(jget del_keys_total)"
  printf '      exactly right with the control namespace intact. Alive, under every ceiling, 5xx == declared.\n'
  exit 0
fi
printf 'FAIL: gates:%s\n' "$GATE_FAIL" >&2
case "$GATE_FAIL" in
  *server_dead*|*driver_assertions*|*unexpected_http_5xx*) log_tail ;;
esac
exit 1
