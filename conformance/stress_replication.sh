#!/usr/bin/env bash
# Cairn REPLICATION-under-sustained-load harness (ARCH 20 + 27 + 30) — the missing load coverage for
# the two-node replication pipeline ACROSS A MASTER-KEY BOUNDARY.
#
# THE GAP. `soak.sh` is the only other multi-node soak: it ships PLAINTEXT objects at one fixed
# 64 KiB size and samples nothing but the SOURCE's RSS. `mesh.sh` wires five nodes with distinct keys
# but drives a handful of objects per scenario, not a load. So nothing anywhere runs ENCRYPTED
# objects across a key boundary under sustained load, and nothing watches the replication OUTBOX for
# backlog growth while writes continue. This harness does both, and samples BOTH nodes.
#
# TOPOLOGY (two nodes on loopback, DIFFERENT master keys — derived per node from a label, the
# `mesh.py` technique — because distinct keys are the whole point: the source's per-version DEK is
# sealed under the SOURCE ring and cannot be shipped verbatim):
#
#   node-T = TARGET (the mirror).  CAIRN_ENCRYPT_AT_REST=true, so every replica it accepts is
#            committed as a VERSION_ENCRYPTED CRNB container under the TARGET's own key.
#   node-S = SOURCE. Replicates to node-T; runs the constant write workload.
#
# WIRING CHOICE — the SOURCE uses the operator-trusted `CAIRN_REPLICATION_ENDPOINT` config path
# (like `soak.sh`), NOT the management API. That path is exempt from the `cairn-net` SSRF guard, so
# this harness does NOT need `CAIRN_ALLOW_INTERNAL_ENDPOINTS=true` (which `mesh.py` does need,
# because it registers targets through the guarded management API). The per-bucket rules still name
# `arn:aws:s3:::<bucket>`, which `resolve_dest_buckets` maps onto the configured endpoint.
# The SOURCE's UI listener IS on, but only so the driver can read `GET /api/v1/replication/summary`
# — exact outbox pending/claimed/failed counts and the true lag, straight from the store.
#
# WORKLOAD (constant, fixed worker pool, never sleeps — see the driver header): single-part PUTs,
# version churn, delete markers, multipart-COMPLETED uploads, plus an SSE-S3/`aws:kms` arm.
#
# GATED here (server-side; the driver gates correctness — its header lists those). Every one is
# valid REGARDLESS OF OFFERED LOAD, so they hold on a contended 2-core runner:
#   * the driver's own gates (byte-exactness across the key boundary, fail-closed, version-id
#     identity, delete markers with exact `NoSuchKey`, the on-disk re-encryption proof, outbox drain,
#     /healthz on both nodes) — a non-zero driver exit fails the run;
#   * BOTH nodes alive at the end;
#   * HTTP 5xx on EACH node EQUAL to the driver's declared budget (which is 0 for this mix — the one
#     deliberate rejection in it, the destination refusing an encrypted source version, is a 400);
#   * absolute CEILINGS on BOTH nodes: RSS / open fds / threads / summed WAL bytes — same knobs and
#     defaults as `stress.sh`, so the harnesses agree on what "runaway" means;
#   * SAMPLER NON-VACUITY: each node must have produced samples and a NON-ZERO peak for RSS, fds,
#     threads and WAL. A column that reads zero all run would pass its ceiling for free, so a zero
#     peak FAILS instead of silently disarming the ceiling.
#
# ADVISORY here (printed + written to STRESS_REPL_OUT, never gating): convergence-latency
# percentiles, replication throughput, the outbox-depth series and peak lag, per-node CPU-seconds and
# RSS, writer commit p99 and mean batch size. Those are all hardware- and build-profile-bound (CI
# drives the DEBUG artifact, whose AES-GCM is unoptimized software crypto) — and the IN-RUN outbox
# depth is advisory for a further reason: source and target share one box, so a persistently non-zero
# backlog can just mean the shipping side is CPU-starved. The load-independent statement is the
# DRAIN: after writes stop, pending+claimed must reach 0 within a bounded wait, with a no-progress
# stall detector. A backlog that never comes back down is the failure this harness exists to catch.
#
# TWO KNOWN GAPS, PINNED AND REPORTED, NOT GATED (this is a harness-only harness; fixing them is a
# PRODUCT change). Shared root cause: `ReplicationEngine::put_object` opens the source blob with
# `BlobStore::open` — with NO DEK — so an SSE-encrypted source version ships raw CIPHERTEXT.
#   GAP 1, fail-closed: a SINGLE-PART encrypted version's plain MD5 ETag is verified at the
#     destination, the PUT is refused `400 BadDigest`, and a 4xx is terminal — so the object never
#     replicates at all. Wrong, but safe.
#   GAP 2, NOT fail-closed — SILENT CORRUPTION: a MULTIPART-completed encrypted version carries a
#     COMPOSITE `<md5>-<n>` ETag the destination cannot MD5-verify, so it ACCEPTS the ciphertext. The
#     replica exists, has the right size, answers 200, and is GARBAGE.
# The consequence worth knowing: turning on SSE — or `CAIRN_ENCRYPT_AT_REST` — on a replication
# SOURCE silently stops that bucket replicating, and its multipart objects silently corrupt on the
# mirror. The driver counts all three outcomes (absent / byte-exact / wrong-bytes) and prints
# `KNOWN GAPS APPEAR FIXED` if they ever start working — the signal to promote the arm into the
# gated leg. The fail-closed GATE is therefore scoped to the plaintext leg.
#
# Usage:
#   conformance/stress_replication.sh                              # default profile (~3 min)
#   BIN=target/release/cairn conformance/stress_replication.sh
#   REPL_SECS=600 REPL_WORKERS=8 conformance/stress_replication.sh # a longer manual run
#   STRESS_REPL_OUT=/tmp/cairn-stress-repl.json conformance/stress_replication.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"
PY="${PY:-python3}"
PORT_T="${PORT_T:-9112}"     # target S3
PORT_S="${PORT_S:-9113}"     # source S3
PORT_S_UI="${PORT_S_UI:-9114}"   # source control API (replication summary)
KEY_ID="${KEY_ID:-alias/cairn-replstress}"
REPL_INTERVAL="${REPL_INTERVAL:-1}"
REPL_WORKER_CONCURRENCY="${REPL_WORKER_CONCURRENCY:-4}"

# Absolute ceilings — same knobs/defaults as stress.sh / stress_encrypted.sh / stress_multipart.sh.
RSS_CEILING_KIB="${RSS_CEILING_KIB:-1048576}"
FD_CEILING="${FD_CEILING:-4096}"
THREAD_CEILING="${THREAD_CEILING:-512}"
WAL_CEILING_BYTES="${WAL_CEILING_BYTES:-$((512*1024*1024))}"

DATA_T="$(mktemp -d)"
DATA_S="$(mktemp -d)"

# Distinct master keys, derived per node from a label (the mesh.py technique). Distinct keys ARE the
# experiment: the source's sealed DEK is meaningless to the target, so a byte-exact read on the
# target can only come from decrypt-at-source + re-seal-at-target.
KEY_T="$(printf 'cairn-stress-replication-target' | sha256sum | cut -d' ' -f1)"
KEY_S="$(printf 'cairn-stress-replication-source' | sha256sum | cut -d' ' -f1)"

SRV_T=""; SRV_S=""; SAMP_T=""; SAMP_S=""
cleanup() {
  for p in "$SAMP_T" "$SAMP_S" "$SRV_T" "$SRV_S"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null || true
  done
  [ -n "$SRV_T" ] && wait "$SRV_T" 2>/dev/null || true
  [ -n "$SRV_S" ] && wait "$SRV_S" 2>/dev/null || true
  rm -rf "$DATA_T" "$DATA_S"
}
trap cleanup EXIT
note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
log_tails() {
  for pair in "target:$DATA_T/server.log" "source:$DATA_S/server.log"; do
    f="${pair#*:}"
    [ -f "$f" ] && printf '\n--- %s server.log (tail) ---\n%s\n' "${pair%%:*}" "$(tail -30 "$f")" >&2
  done
}

[ -x "$BIN" ] || fail "binary not found: $BIN (build it: cargo build --bin cairn)"
command -v "$PY" >/dev/null 2>&1 || fail "python interpreter not found: $PY"
# boto3 IS the harness. A CI box that quietly lost boto3 must go red rather than green-with-no-
# coverage (the provenance lesson from stress_encrypted.sh).
"$PY" -c "import boto3" 2>/dev/null || fail "boto3 not importable by '$PY' — this harness IS the boto3 driver"

# PORT PRE-FLIGHT. Without this a STALE cairn already bound to one of these ports answers /healthz,
# the launcher happily proceeds, and the whole run silently measures somebody else's process while
# our own node dies with EADDRINUSE. Found the hard way on a shared dev box.
for p in "$PORT_T" "$PORT_S" "$PORT_S_UI"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    fail "127.0.0.1:$p already has a listener — another server is running. Set PORT_T/PORT_S/PORT_S_UI."
  fi
done

CLK_TCK="$(getconf CLK_TCK 2>/dev/null || echo 100)"

# --- /metrics scrape helpers (same shapes as stress.sh), per node ------------------------------
scrape_5xx() { # <port>
  curl -fsS --max-time 10 "http://127.0.0.1:$1/metrics" 2>/dev/null \
    | awk '/^cairn_requests_total\{/ && /status="5[0-9][0-9]"/ {s+=$2} END {printf "%d", s+0}'
}
scrape_sum() { # <port> <metric-name regex>
  curl -fsS --max-time 10 "http://127.0.0.1:$1/metrics" 2>/dev/null \
    | awk -v m="$2" '$1 ~ m {s += $2} END {printf "%.0f", s+0}'
}
scrape_val() { # <port> <exact-metric-name>
  curl -fsS --max-time 10 "http://127.0.0.1:$1/metrics" 2>/dev/null \
    | awk -v m="$2" '$1 == m {print $2; exit}'
}
scrape_quantile() { # <port> <metric> <quantile-string>
  curl -fsS --max-time 10 "http://127.0.0.1:$1/metrics" 2>/dev/null \
    | awk -v m="$2" -v q="$3" '$0 ~ ("^" m "\\{") && index($0, "quantile=\"" q "\"") {print $2; exit}'
}
scrape_commit_max() { # <port>
  local v; v="$(scrape_quantile "$1" cairn_writer_commit_seconds 1)"
  [ -z "$v" ] && v="$(scrape_quantile "$1" cairn_writer_commit_seconds 1.0)"
  printf '%s' "$v"
}
col_peak() { awk -F'\t' -v c="$2" 'BEGIN{m=0} {if($c>m)m=$c} END{print m+0}' "$1" 2>/dev/null; }
rows_of() { wc -l <"$1" 2>/dev/null | tr -d ' '; }
# A JSON scalar out of the driver's advisory file (flat object) — no jq dependency.
jget() { # <key>
  "$PY" - "$1" "$DRIVER_JSON" <<'PYEOF' 2>/dev/null
import json, sys
try:
    with open(sys.argv[2], encoding="utf-8") as fh:
        print(json.load(fh).get(sys.argv[1], 0))
except Exception:
    print(0)
PYEOF
}

# --- 1. the two nodes ---------------------------------------------------------------------------
# Each node's bootstrap and serve MUST share a master key, or the sealed SigV4 secret cannot be
# unsealed at serve time (the secret is envelope-encrypted under the master key at bootstrap).
#
# CAIRN_REQUEST_TIMEOUT_SECS is PINNED well above any plausible operation on a contended runner. If
# left ambient, a slow box could yield a server-side 503 RequestTimeout, which would break BOTH the
# zero-errors gate and the exact-5xx-count gate at once — a load-dependent flake.
# CAIRN_WAL_CHECKPOINT_INTERVAL_SECS is pinned too, so the WAL ceiling is judged against a store
# that actually checkpoints within the length of this run rather than one held open by configuration.
# CAIRN_WAL_CHECKPOINT_SIZE_BYTES is pinned for the same reason but along the other axis. Cairn sets
# `wal_autocheckpoint=0` (cairn-meta/src/lib.rs:104), so the background checkpointer is the WAL's ONLY
# bound, and config.rs:676-686 warns in as many words that a TIME trigger alone lets the WAL grow
# between checkpoints. A time trigger bounds the WAL by seconds-of-writes, which on a fast CI runner
# sustaining ~130 versions/s (with this harness's ~2x metadata amplification: a version row AND an
# outbox row per object) reached 783 MB in one 15 s window and blew a 512 MB ceiling. That is not a
# leak, it is the documented behaviour of a size-triggerless configuration — so bound it by SIZE, which
# is what makes the ceiling a meaningful runaway backstop instead of a proxy for the runner's speed.
common_env() { # <data-dir> <s3-port> <ui-addr> <master-key>
  printf '%s\n' \
    "CAIRN_DATA_DIR=$1/data" "CAIRN_DB_PATH=$1/data/cairn.db" \
    "CAIRN_LISTEN_ADDR=127.0.0.1:$2" "CAIRN_UI_ADDR=$3" "CAIRN_MASTER_KEY=$4" \
    "CAIRN_REGION=us-east-1" "CAIRN_ALLOW_INSECURE=true" \
    "CAIRN_LOG_LEVEL=${CAIRN_LOG_LEVEL:-error}" \
    "CAIRN_REQUEST_TIMEOUT_SECS=${CAIRN_REQUEST_TIMEOUT_SECS:-600}" \
    "CAIRN_WAL_CHECKPOINT_INTERVAL_SECS=${CAIRN_WAL_CHECKPOINT_INTERVAL_SECS:-15}" \
    "CAIRN_WAL_CHECKPOINT_SIZE_BYTES=${CAIRN_WAL_CHECKPOINT_SIZE_BYTES:-$((64*1024*1024))}"
}
# shellcheck disable=SC2046
target_env=( $(common_env "$DATA_T" "$PORT_T" off "$KEY_T") "CAIRN_ENCRYPT_AT_REST=true" )
# shellcheck disable=SC2046
source_env=( $(common_env "$DATA_S" "$PORT_S" "127.0.0.1:$PORT_S_UI" "$KEY_S")
             "CAIRN_KMS_KEY_IDS=$KEY_ID"
             "CAIRN_REPLICATION_REGION=us-east-1"
             "CAIRN_REPLICATION_INTERVAL_SECS=$REPL_INTERVAL"
             "CAIRN_REPLICATION_WORKER_CONCURRENCY=$REPL_WORKER_CONCURRENCY"
             # The two nodes talk over loopback http://. Replication ships DECRYPTED bodies, so a
             # CLIENT-encrypted (SSE-S3 / aws:kms) object is refused on a plaintext endpoint unless
             # the operator opts in. This rig is loopback-only, and the driver's SSE arm is a GATED
             # leg — without this every SSE object would sit rescheduled and the byte-exact gate
             # would fail with "missing on the target".
             "CAIRN_REPLICATION_ALLOW_PLAINTEXT_SSE_OVER_HTTP=true" )

T_BOOT="$(env "${target_env[@]}" "$BIN" bootstrap)" || fail "target bootstrap failed"
T_AK="$(echo "$T_BOOT" | awk '/Access Key Id/ {print $NF}')"
T_SK="$(echo "$T_BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$T_AK" ] && [ -n "$T_SK" ] || fail "could not parse target credentials"

S_BOOT="$(env "${source_env[@]}" "$BIN" bootstrap)" || fail "source bootstrap failed"
S_AK="$(echo "$S_BOOT" | awk '/Access Key Id/ {print $NF}')"
S_SK="$(echo "$S_BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$S_AK" ] && [ -n "$S_SK" ] || fail "could not parse source credentials"

env "${target_env[@]}" "$BIN" serve >"$DATA_T/server.log" 2>&1 &
SRV_T=$!
env "${source_env[@]}" \
    CAIRN_REPLICATION_ENDPOINT="http://127.0.0.1:$PORT_T" \
    CAIRN_REPLICATION_ACCESS_KEY="$T_AK" \
    CAIRN_REPLICATION_SECRET="$T_SK" \
    "$BIN" serve >"$DATA_S/server.log" 2>&1 &
SRV_S=$!

wait_healthy() { # <port> <pid> <log> <label>
  for _ in $(seq 1 150); do
    curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$1/healthz" 2>/dev/null && return 0
    kill -0 "$2" 2>/dev/null || { printf 'FAIL: %s node exited at startup\n' "$4" >&2; return 1; }
    sleep 0.1
  done
  printf 'FAIL: %s node never became healthy\n' "$4" >&2
  return 1
}
wait_healthy "$PORT_T" "$SRV_T" "$DATA_T/server.log" target || { log_tails; exit 1; }
wait_healthy "$PORT_S" "$SRV_S" "$DATA_S/server.log" source || { log_tails; exit 1; }
printf '\n=== two nodes on 127.0.0.1 — target :%s (pid %s, encrypt-at-rest, own master key), source :%s (pid %s) — %s, %s cores ===\n' \
  "$PORT_T" "$SRV_T" "$PORT_S" "$SRV_S" "$(uname -m)" "$(nproc)"

# --- 2. per-second samplers, ONE PER NODE ------------------------------------------------------
# Columns: RSS(KiB)  writer_queue_depth  fd_count  wal_bytes  thread_count  cpu_ticks  repl_depth.
# `repl_depth` is the source's `cairn_replication_queue_depth` gauge (0 on the target, which has no
# outbox) and is ADVISORY — the driver polls the control API for the authoritative counts.
#
# The sampler is launched by a function that assigns SAMPLER_PID rather than ECHOING the pid: a
# `$(start_sampler ...)` capture would block until the backgrounded loop closed its inherited stdout,
# i.e. until the sampler EXITED — so the launcher would hang for the whole run. The loop's own
# stdout/stderr therefore go to /dev/null and only the explicit `>>"$out"` writes survive.
start_sampler() { # <pid> <port> <db-path> <out-tsv>  -> sets SAMPLER_PID
  local pid="$1" port="$2" db="$3" out="$4"
  : >"$out" || fail "cannot create sampler file $out"
  (
    while kill -0 "$pid" 2>/dev/null; do
      rss="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')"
      m="$(curl -fsS --max-time 5 "http://127.0.0.1:$port/metrics" 2>/dev/null)"
      wq="$(printf '%s\n' "$m" | awk '/^cairn_writer_queue_depth/ {print $2; exit}')"
      rd="$(printf '%s\n' "$m" | awk '/^cairn_replication_queue_depth/ {print $2; exit}')"
      fd="$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)"
      th="$(ls "/proc/$pid/task" 2>/dev/null | wc -l)"
      # shellcheck disable=SC2086
      wal="$(stat --format=%s ${db}*-wal 2>/dev/null | awk '{s+=$1} END {print s+0}')"
      cpu="$(awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null)"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "${rss:-0}" "${wq:-0}" "${fd:-0}" "${wal:-0}" "${th:-0}" "${cpu:-0}" "${rd:-0}" >>"$out"
      sleep 1
    done
  ) >/dev/null 2>&1 &
  SAMPLER_PID=$!
}
SAMPLES_T="$DATA_T/samples.tsv"; SAMPLES_S="$DATA_S/samples.tsv"
start_sampler "$SRV_T" "$PORT_T" "$DATA_T/data/cairn.db" "$SAMPLES_T"; SAMP_T="$SAMPLER_PID"
start_sampler "$SRV_S" "$PORT_S" "$DATA_S/data/cairn.db" "$SAMPLES_S"; SAMP_S="$SAMPLER_PID"

fivexx_t_start="$(scrape_5xx "$PORT_T")"; fivexx_t_start="${fivexx_t_start:-0}"
fivexx_s_start="$(scrape_5xx "$PORT_S")"; fivexx_s_start="${fivexx_s_start:-0}"

# --- 3. the driver --------------------------------------------------------------------------------
DRIVER_JSON="$DATA_S/driver.json"
DRIVER_RC=0
"$PY" "$ROOT/conformance/stress_replication.py" \
  "$S_AK" "$S_SK" "http://127.0.0.1:$PORT_S" "http://127.0.0.1:$PORT_S_UI" \
  "$T_AK" "$T_SK" "http://127.0.0.1:$PORT_T" \
  "$DATA_S/data" "$DATA_T/data" "$KEY_ID" "$DRIVER_JSON" || DRIVER_RC=$?

# --- 4. collect -----------------------------------------------------------------------------------
fivexx_t_end="$(scrape_5xx "$PORT_T")"; fivexx_t_end="${fivexx_t_end:-0}"
fivexx_s_end="$(scrape_5xx "$PORT_S")"; fivexx_s_end="${fivexx_s_end:-0}"
fivexx_t=$(( fivexx_t_end - fivexx_t_start )); [ "$fivexx_t" -lt 0 ] && fivexx_t=0
fivexx_s=$(( fivexx_s_end - fivexx_s_start )); [ "$fivexx_s" -lt 0 ] && fivexx_s=0

commit_t="$(scrape_commit_max "$PORT_T")"; commit_t="${commit_t:-0}"
commit_s="$(scrape_commit_max "$PORT_S")"; commit_s="${commit_s:-0}"
batch_mean() { # <port>
  local s c; s="$(scrape_val "$1" cairn_writer_batch_size_sum)"; c="$(scrape_val "$1" cairn_writer_batch_size_count)"
  awk -v s="${s:-0}" -v c="${c:-0}" 'BEGIN{printf "%.2f", (c>0? s/c : 0)}'
}
batch_t="$(batch_mean "$PORT_T")"; batch_s="$(batch_mean "$PORT_S")"
repl_bytes="$(scrape_sum "$PORT_S" '^cairn_replication_bytes_total')"
repl_completed="$(scrape_sum "$PORT_S" '^cairn_replication_completed_total')"
repl_failed_total="$(scrape_sum "$PORT_S" '^cairn_replication_failed_total')"
reqs_t="$(scrape_sum "$PORT_T" '^cairn_requests_total')"
reqs_s="$(scrape_sum "$PORT_S" '^cairn_requests_total')"

alive_t="no"; kill -0 "$SRV_T" 2>/dev/null && curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$PORT_T/healthz" 2>/dev/null && alive_t="yes"
alive_s="no"; kill -0 "$SRV_S" 2>/dev/null && curl -fsS --max-time 5 -o /dev/null "http://127.0.0.1:$PORT_S/healthz" 2>/dev/null && alive_s="yes"

sleep 1
kill "$SAMP_T" 2>/dev/null; kill "$SAMP_S" 2>/dev/null; SAMP_T=""; SAMP_S=""

peaks_of() { # <tsv> -> "rss wq fd wal thread cpu_secs rows"
  local f="$1" rss wq fd wal th first last cpu rows
  rss="$(col_peak "$f" 1)"; wq="$(col_peak "$f" 2)"; fd="$(col_peak "$f" 3)"
  wal="$(col_peak "$f" 4)"; th="$(col_peak "$f" 5)"
  first="$(head -1 "$f" 2>/dev/null | cut -f6)"; last="$(tail -1 "$f" 2>/dev/null | cut -f6)"
  cpu="$(awk -v a="${first:-0}" -v b="${last:-0}" -v t="$CLK_TCK" 'BEGIN{printf "%.1f", (b-a)/(t>0?t:100)}')"
  rows="$(rows_of "$f")"
  printf '%s %s %s %s %s %s %s' "$rss" "$wq" "$fd" "$wal" "$th" "$cpu" "${rows:-0}"
}
read -r rss_t wq_t fd_t wal_t th_t cpu_t rows_t <<EOF
$(peaks_of "$SAMPLES_T")
EOF
read -r rss_s wq_s fd_s wal_s th_s cpu_s rows_s <<EOF
$(peaks_of "$SAMPLES_S")
EOF
repl_depth_peak="$(col_peak "$SAMPLES_S" 7)"

declared_5xx="$(jget declared_5xx)"; declared_5xx="${declared_5xx:-0}"
case "$declared_5xx" in ''|*[!0-9]*) declared_5xx=0 ;; esac

# --- 5. gates --------------------------------------------------------------------------------------
GATE_FAIL=""
[ "$DRIVER_RC" -eq 0 ] || GATE_FAIL="$GATE_FAIL driver_assertions(rc=$DRIVER_RC)"
[ "$alive_t" = "yes" ] || GATE_FAIL="$GATE_FAIL target_dead"
[ "$alive_s" = "yes" ] || GATE_FAIL="$GATE_FAIL source_dead"
[ "$fivexx_t" -ne "$declared_5xx" ] 2>/dev/null && GATE_FAIL="$GATE_FAIL target_http_5xx($fivexx_t!=$declared_5xx)"
[ "$fivexx_s" -ne "$declared_5xx" ] 2>/dev/null && GATE_FAIL="$GATE_FAIL source_http_5xx($fivexx_s!=$declared_5xx)"

# Absolute ceilings on BOTH nodes, plus the NON-VACUITY guard: a sampled column that read zero all
# run would pass its ceiling for free, so a zero peak (or an empty sample file) FAILS instead of
# silently disarming the gate.
check_node() { # <label> <rss> <fd> <wal> <threads> <rows>
  local l="$1" rss="$2" fd="$3" wal="$4" th="$5" rows="$6"
  [ "${rows:-0}" -ge 5 ] 2>/dev/null || GATE_FAIL="$GATE_FAIL ${l}_sampler_empty(rows=${rows:-0})"
  for pair in "rss:$rss" "fd:$fd" "wal:$wal" "threads:$th"; do
    [ "${pair#*:}" -gt 0 ] 2>/dev/null || GATE_FAIL="$GATE_FAIL ${l}_${pair%%:*}_peak_zero"
  done
  [ "${rss:-0}" -gt "$RSS_CEILING_KIB" ] 2>/dev/null && GATE_FAIL="$GATE_FAIL ${l}_rss_ceiling($rss>$RSS_CEILING_KIB)"
  [ "${fd:-0}" -gt "$FD_CEILING" ] 2>/dev/null && GATE_FAIL="$GATE_FAIL ${l}_fd_ceiling($fd>$FD_CEILING)"
  [ "${th:-0}" -gt "$THREAD_CEILING" ] 2>/dev/null && GATE_FAIL="$GATE_FAIL ${l}_thread_ceiling($th>$THREAD_CEILING)"
  [ "${wal:-0}" -gt "$WAL_CEILING_BYTES" ] 2>/dev/null && GATE_FAIL="$GATE_FAIL ${l}_wal_ceiling($wal>$WAL_CEILING_BYTES)"
  return 0
}
check_node target "$rss_t" "$fd_t" "$wal_t" "$th_t" "$rows_t"
check_node source "$rss_s" "$fd_s" "$wal_s" "$th_s" "$rows_s"

# --- 6. report ---------------------------------------------------------------------------------
printf '\n=== per-node stability (GATED on absolute ceilings + non-vacuity) ===\n'
printf '  %-7s %10s %8s %8s %14s %10s %8s %10s\n' node "RSS KiB" fd threads "WAL bytes" "CPU s" "5xx" samples
printf '  %-7s %10s %8s %8s %14s %10s %8s %10s\n' source "$rss_s" "$fd_s" "$th_s" "$wal_s" "$cpu_s" "$fivexx_s" "$rows_s"
printf '  %-7s %10s %8s %8s %14s %10s %8s %10s\n' target "$rss_t" "$fd_t" "$th_t" "$wal_t" "$cpu_t" "$fivexx_t" "$rows_t"
printf '  ceilings: RSS %s KiB, fd %s, threads %s, WAL %s B; 5xx budget %s per node\n' \
  "$RSS_CEILING_KIB" "$FD_CEILING" "$THREAD_CEILING" "$WAL_CEILING_BYTES" "$declared_5xx"

printf '\n=== ADVISORY (never gating — contention- and build-profile-bound) ===\n'
printf '  versions written: %s (%s plain, %s SSE-arm) + %s delete markers over %ss at %s versions/s\n' \
  "$(jget versions_written)" "$(jget plain_total)" "$(jget sse_total)" \
  "$(jget markers_written)" "$(jget load_secs)" "$(jget write_ops_per_sec)"
printf '  replication: %s MiB shipped, %s obj/s and %s MiB/s incl. drain; drain took %ss\n' \
  "$(jget replicated_mib)" "$(jget replication_obj_per_sec)" "$(jget replication_mib_per_sec)" \
  "$(jget drain_secs)"
printf '  convergence latency (%s samples): p50 %sms  p90 %sms  p99 %sms  max %sms\n' \
  "$(jget convergence_samples)" "$(jget convergence_p50_ms)" "$(jget convergence_p90_ms)" \
  "$(jget convergence_p99_ms)" "$(jget convergence_max_ms)"
printf '  outbox depth under constant load: min %s  median %s  max %s (gauge peak %s); peak lag %ss\n' \
  "$(jget outbox_depth_min)" "$(jget outbox_depth_med)" "$(jget outbox_depth_max)" \
  "${repl_depth_peak:-0}" "$(jget lag_peak_secs)"
printf '  engine counters: %s bytes, %s completed, %s terminal failures (the pinned SSE gap)\n' \
  "${repl_bytes:-0}" "${repl_completed:-0}" "${repl_failed_total:-0}"
printf '  writer commit p99 max: source %ss / target %ss;  mean batch size: source %s / target %s\n' \
  "$commit_s" "$commit_t" "$batch_s" "$batch_t"
printf '  peak writer queue depth: source %s / target %s;  requests served: source %s / target %s\n' \
  "${wq_s:-0}" "${wq_t:-0}" "${reqs_s:-0}" "${reqs_t:-0}"

if [ -n "${STRESS_REPL_OUT:-}" ]; then
  # Values are passed as ARGV STRINGS and coerced in Python, never interpolated into source: a
  # Prometheus summary can legitimately render NaN / +Inf, which are not Python literals, so a
  # heredoc interpolation would NameError and — with `set -uo pipefail` (no -e) — still print
  # "results written" and exit 0 with no file. A silent missing artifact is the worst outcome.
  "$PY" - "$DRIVER_JSON" "$STRESS_REPL_OUT" \
      "${rss_s:-0}" "${fd_s:-0}" "${th_s:-0}" "${wal_s:-0}" "${cpu_s:-0}" "${wq_s:-0}" "${fivexx_s:-0}" \
      "${rss_t:-0}" "${fd_t:-0}" "${th_t:-0}" "${wal_t:-0}" "${cpu_t:-0}" "${wq_t:-0}" "${fivexx_t:-0}" \
      "${commit_s:-0}" "${commit_t:-0}" "${batch_s:-0}" "${batch_t:-0}" \
      "${repl_bytes:-0}" "${repl_completed:-0}" "${repl_failed_total:-0}" "${repl_depth_peak:-0}" \
      "${reqs_s:-0}" "${reqs_t:-0}" "${DRIVER_RC}" "${GATE_FAIL:-none}" <<'PYEOF'
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
    "source_rss_peak_kib": num(a[3]), "source_fd_peak": num(a[4]),
    "source_thread_peak": num(a[5]), "source_wal_peak_bytes": num(a[6]),
    "source_cpu_secs": num(a[7]), "source_wq_peak": num(a[8]), "source_http_5xx": num(a[9]),
    "target_rss_peak_kib": num(a[10]), "target_fd_peak": num(a[11]),
    "target_thread_peak": num(a[12]), "target_wal_peak_bytes": num(a[13]),
    "target_cpu_secs": num(a[14]), "target_wq_peak": num(a[15]), "target_http_5xx": num(a[16]),
    "source_commit_p99": num(a[17]), "target_commit_p99": num(a[18]),
    "source_batch_mean": num(a[19]), "target_batch_mean": num(a[20]),
    "replication_bytes_total": num(a[21]), "replication_completed_total": num(a[22]),
    "replication_failed_total": num(a[23]), "replication_depth_gauge_peak": num(a[24]),
    "source_requests": num(a[25]), "target_requests": num(a[26]),
    "driver_rc": num(a[27]), "gates": a[28],
})
with open(sys.argv[2], "w", encoding="utf-8") as fh:
    json.dump(adv, fh, indent=1)
PYEOF
  note "results written to $STRESS_REPL_OUT"
fi

printf '\n=== verdict ===\n'
if [ -z "$GATE_FAIL" ]; then
  printf 'PASS: replication under sustained load across a master-key boundary.\n'
  printf '      %s plaintext-leg versions byte-exact on the TARGET (read by the SOURCE version id),\n' \
    "$(jget plain_match)"
  printf '      target blobs VERSION_ENCRYPTED under its OWN key while the source blobs are plain,\n'
  printf '      %s delete markers propagated with identical ids and exact 404 NoSuchKey,\n' \
    "$(jget delete_markers)"
  printf '      outbox drained to 0 in %ss, both nodes alive, 0 HTTP 5xx each, under every ceiling.\n' \
    "$(jget drain_secs)"
  printf '      SSE leg (GATED): %s encrypted source versions — %s byte-exact, %s missing,\n' \
    "$(jget sse_total)" "$(jget sse_match)" "$(jget sse_missing)"
  printf '      %s with wrong bytes (%s multipart). Any non-zero here fails the run: this leg IS the\n' \
    "$(jget sse_corrupt_on_target)" "$(jget sse_corrupt_multipart)"
  printf '      regression guard for the replication-ships-ciphertext defect.\n' 
  exit 0
fi
printf 'FAIL: gates:%s\n' "$GATE_FAIL" >&2
log_tails
exit 1
