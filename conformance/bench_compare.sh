#!/usr/bin/env bash
# Head-to-head warp macro-benchmark: Cairn vs a pinned MinIO server, side by side on ONE host
# (ARCH 30.2). Boots BOTH servers from the same machine — Cairn from $BIN, MinIO from a downloaded,
# pinned release binary — single-node/single-drive, plaintext HTTP, and drives an identical warp
# matrix against each SEQUENTIALLY (only one server under load at a time; warp itself burns ~1 core).
#
# It exists to answer, on every push: "for each S3 operation, how does Cairn compare to MinIO on THIS
# runner?" The signal is the Cairn/MinIO RATIO per op, NOT the absolute obj/s — a contended CI runner
# has large run-to-run variance (see docs/benchmarks.md), so this is REPORT, not a throughput gate.
#
# Usage:
#   BIN=target/debug/cairn bash conformance/bench_compare.sh
#   DURATION=8s CONCURRENT=16 REPEATS=3 bash conformance/bench_compare.sh     # steadier local run
#   BENCH_CSV=bench.csv BENCH_JSON=bench.json bash conformance/bench_compare.sh
#
# Exit status: 0 on a clean comparison (whoever is faster). Non-zero ONLY on infrastructure failure
# (server didn't start, download failed) OR if warp reported OPERATION errors on either server — a
# correctness/liveness signal that is robust to noise, unlike throughput.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/debug/cairn}"

# --- tunables (CI-sized defaults; ~15-20 min for both targets on a 4-vCPU runner) ---------------
CONCURRENT="${CONCURRENT:-16}"
DURATION="${DURATION:-5s}"
REPEATS="${REPEATS:-1}"          # median-of-N per cell; 1 in the per-push job, 3 for a steadier run
REGION="${REGION:-us-east-1}"
BUCKET="${BUCKET:-bench}"
SOFT_ALARM_RATIO="${SOFT_ALARM_RATIO:-2.5}"   # ⚠️ (non-fatal) if Cairn is >this× slower on an op

CPORT="${CPORT:-9401}"          # Cairn S3
MPORT="${MPORT:-9402}"          # MinIO S3
MCONSOLE="${MCONSOLE:-9403}"    # MinIO console (unused, but MinIO wants an address)

# Pinned so the comparison is reproducible across pushes. MinIO pinned to the SAME release the local
# baseline used; warp pinned to the same v1.0.0 every other harness uses (auto path-style for IP:port).
MINIO_VERSION="${MINIO_VERSION:-RELEASE.2025-09-07T16-13-09Z}"
MINIO_URL="https://dl.min.io/server/minio/release/linux-amd64/archive/minio.${MINIO_VERSION}"
MINIO_CACHE="${MINIO_CACHE:-/tmp/cairn-minio}"
WARP_VERSION="${WARP_VERSION:-1.0.0}"
WARP_URL="https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz"
WARP_CACHE="${WARP_CACHE:-/tmp/cairn-warp}"
WARP="${WARP:-}"

BENCH_CSV="${BENCH_CSV:-}"
BENCH_JSON="${BENCH_JSON:-}"

DATA="$(mktemp -d)"
CSRV="" ; MSRV=""
cleanup() {
  [ -n "$CSRV" ] && kill "$CSRV" 2>/dev/null || true
  [ -n "$MSRV" ] && kill "$MSRV" 2>/dev/null || true
  [ -n "$CSRV" ] && wait "$CSRV" 2>/dev/null || true
  [ -n "$MSRV" ] && wait "$MSRV" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

note() { printf '  %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

# --- 0. binaries ---------------------------------------------------------------------------------
[ -x "$BIN" ] || fail "cairn binary not found/executable: $BIN (build it: cargo build --bin cairn)"

if [ -z "$WARP" ]; then
  if command -v warp >/dev/null 2>&1; then WARP="$(command -v warp)"; else
    WARP="$WARP_CACHE/warp"
    if [ ! -x "$WARP" ]; then
      note "downloading warp v$WARP_VERSION"
      mkdir -p "$WARP_CACHE"
      curl -fsSL -o "$WARP_CACHE/warp.tgz" "$WARP_URL" || fail "warp download failed"
      tar -xzf "$WARP_CACHE/warp.tgz" -C "$WARP_CACHE" warp || fail "warp tarball had no 'warp'"
      chmod +x "$WARP"; rm -f "$WARP_CACHE/warp.tgz"
    fi
  fi
fi
MINIO="$MINIO_CACHE/minio"
if [ ! -x "$MINIO" ]; then
  note "downloading MinIO $MINIO_VERSION"
  mkdir -p "$MINIO_CACHE"
  curl -fsSL -o "$MINIO" "$MINIO_URL" || fail "MinIO download failed ($MINIO_URL)"
  chmod +x "$MINIO"
fi
note "warp:  $("$WARP" --version 2>&1 | head -1)"
note "minio: $("$MINIO" --version 2>&1 | head -1)"
note "cairn: $("$BIN" --version 2>&1 | head -1)"

# --- 1. boot Cairn (bootstrap -> serve -> healthz), like conformance/warp.sh ----------------------
export CAIRN_DATA_DIR="$DATA/cairn" CAIRN_DB_PATH="$DATA/cairn/cairn.db"
export CAIRN_LISTEN_ADDR="127.0.0.1:$CPORT" CAIRN_WEB_ADDR=off
export CAIRN_MASTER_KEY="$(openssl rand -hex 32)" CAIRN_LOG_LEVEL="${CAIRN_LOG_LEVEL:-error}"
BOOT="$("$BIN" bootstrap)" || fail "cairn bootstrap failed"
CAK="$(echo "$BOOT" | awk '/Access Key Id/ {print $NF}')"
CSK="$(echo "$BOOT" | awk '/Secret Access Key/ {print $NF}')"
[ -n "$CAK" ] && [ -n "$CSK" ] || fail "could not parse cairn credentials"
"$BIN" serve >"$DATA/cairn.log" 2>&1 & CSRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$CPORT/healthz" 2>/dev/null && break
  kill -0 "$CSRV" 2>/dev/null || fail "cairn exited on startup:\n$(cat "$DATA/cairn.log")"
  sleep 0.1
done
note "cairn healthy on 127.0.0.1:$CPORT"

# --- 2. boot MinIO single-node/single-drive (the architectural match to Cairn) --------------------
MAK=benchadmin ; MSK=benchadmin123
MINIO_ROOT_USER="$MAK" MINIO_ROOT_PASSWORD="$MSK" \
  "$MINIO" server "$DATA/minio" --address "127.0.0.1:$MPORT" --console-address "127.0.0.1:$MCONSOLE" \
  >"$DATA/minio.log" 2>&1 & MSRV=$!
for _ in $(seq 1 100); do
  curl -fsS -o /dev/null "http://127.0.0.1:$MPORT/minio/health/live" 2>/dev/null && break
  kill -0 "$MSRV" 2>/dev/null || fail "minio exited on startup:\n$(cat "$DATA/minio.log")"
  sleep 0.1
done
note "minio healthy on 127.0.0.1:$MPORT"

# --- 3. run one warp op against one target; echo "obj_s,mib_s,errors" for the MEASURED op ---------
# CRITICAL: get/stat/delete first PREPARE by uploading (an "Operation: PUT" block) and only THEN
# measure — so we must read the Average under the block whose Operation matches the MEASURED op, not
# warp's first Average (that would report the prepare-PUT). mixed reports a "Cluster Total:" instead.
measure_once() {  # $1=host $2=ak $3=sk $4=op $5=size ; extra args after
  local host="$1" ak="$2" sk="$3" op="$4" size="$5"; shift 5
  local out rc OP
  OP="$(printf '%s' "$op" | tr '[:lower:]' '[:upper:]')"
  set +e
  out="$("$WARP" "$op" --host "$host" --access-key "$ak" --secret-key "$sk" --region "$REGION" \
    --bucket "$BUCKET" --obj.size "$size" --concurrent "$CONCURRENT" --duration "$DURATION" "$@" 2>&1)"
  rc=$?
  set -e
  local objs mibs errs line
  if [ "$op" = "mixed" ]; then
    line="$(printf '%s\n' "$out" | grep -m1 '^Cluster Total:')"
  else
    # first "* Average:" line AFTER the "Operation: <OP>" header (skips the prepare-PUT block)
    line="$(printf '%s\n' "$out" | awk -v op="$OP" '
      $0 ~ "^Operation: " op {cap=1; next}
      cap && /Skipping .* too few samples/ {print "SKIP"; exit}
      cap && /\* Average:/ {print; exit}')"
  fi
  if [ -z "$line" ] || [ "$line" = "SKIP" ]; then objs=NA; mibs=NA; else
    # obj/s may be preceded by ", " (put/get: "X MiB/s, Y obj/s") or just a space (list/stat:
    # "Y obj/s", no MiB/s), so accept either separator. MiB/s is absent for byteless ops → NA.
    objs="$(printf '%s' "$line" | sed -n 's/.*[ ,]\([0-9.]*\) obj\/s.*/\1/p')"
    mibs="$(printf '%s' "$line" | sed -n 's/.*[: ]\([0-9.]*\) MiB\/s.*/\1/p')"
    [ -z "$objs" ] && objs=NA; [ -z "$mibs" ] && mibs=NA
  fi
  errs="$(printf '%s\n' "$out" | sed -n 's/^Errors: \([0-9]*\).*/\1/p' | tail -1)"; [ -z "$errs" ] && errs=0
  # a prepare-abort (get/mixed) exits non-zero with per-op <ERROR> spam and no summary
  if [ "$rc" -ne 0 ] && [ "$errs" -eq 0 ]; then
    errs="$(printf '%s\n' "$out" | grep -cE '<ERROR>' || true)"; [ "$errs" -eq 0 ] && errs=1
  fi
  printf '%s,%s,%s\n' "$objs" "$mibs" "$errs"
}

# median of REPEATS numeric values (NA-safe: NA if all NA)
median() { printf '%s\n' "$@" | grep -vx NA | sort -g | awk '{a[NR]=$1} END{if(NR==0){print "NA"} else {print a[int((NR+1)/2)]}}'; }

# --- resource sampling: the server's CPU% + RSS while it is actually under load ------------------
# A great benchmark reports the COST, not just the rate. We sample each server process only during
# its own warp runs (not while idle), so the averages reflect "resources used while serving", and
# report per-engine mean CPU% + mean/peak RSS alongside throughput.
CLK=$(getconf CLK_TCK 2>/dev/null || echo 100)
PGK=$(( $(getconf PAGESIZE 2>/dev/null || echo 4096) / 1024 ))   # page size in KiB
RES_C="$DATA/res_cairn"; RES_M="$DATA/res_minio"; : >"$RES_C"; : >"$RES_M"
# Append "cpu_pct rss_kb" for $1 to $2 every ~1s (instantaneous CPU from utime+stime deltas) until
# killed. Includes the process's threads (cairn/minio are multi-threaded) via /proc/<pid>/stat.
mon() {  # pid outfile
  local pid="$1" out="$2" prev=0 cur u s rss
  while kill -0 "$pid" 2>/dev/null; do
    read -r u s < <(awk '{print $14, $15}' "/proc/$pid/stat" 2>/dev/null) || break
    [ -z "${u:-}" ] && break
    cur=$((u + s))
    if [ "$prev" -ne 0 ]; then
      rss=$(awk -v k="$PGK" '{printf "%d", $2*k}' "/proc/$pid/statm" 2>/dev/null)
      awk -v d=$((cur - prev)) -v h="$CLK" -v r="${rss:-0}" 'BEGIN{printf "%.1f %d\n", d/h*100, r}' >>"$out"
    fi
    prev=$cur
    sleep 1
  done
}
# per-engine "mean_cpu% peak_mb mean_mb" over all its samples
res_stats() { awk '{c+=$1;n++; if($2>pk)pk=$2; r+=$2} END{ if(n==0){print "n/a n/a n/a"} else printf "%.0f %.0f %.0f\n", c/n, pk/1024, (r/n)/1024 }' "$1"; }

# run a cell against a target REPEATS times, echo "median_obj,median_mib,total_errors"
run_target() {  # $1=host $2=ak $3=sk $4=op $5=size ; extra
  local host="$1" pid resfile
  case "$host" in
    *":$CPORT") pid="$CSRV"; resfile="$RES_C" ;;
    *) pid="$MSRV"; resfile="$RES_M" ;;
  esac
  local os=() ms=() etot=0 out o m e monpid
  mon "$pid" "$resfile" & monpid=$!
  for _ in $(seq 1 "$REPEATS"); do
    out="$(measure_once "$@")"; o="${out%%,*}"; m="$(printf '%s' "$out" | cut -d, -f2)"; e="${out##*,}"
    os+=("$o"); ms+=("$m"); etot=$((etot + e))
  done
  kill "$monpid" 2>/dev/null; wait "$monpid" 2>/dev/null
  printf '%s,%s,%s\n' "$(median "${os[@]}")" "$(median "${ms[@]}")" "$etot"
}

# --- 4. the matrix -------------------------------------------------------------------------------
# op | obj.size | extra warp args (prepare pool for read/stat/delete/list/mixed; put is duration-only).
# Override the whole matrix with CELLS_ENV="op|size|extra;op|size|extra" (e.g. a fast local smoke).
if [ -n "${CELLS_ENV:-}" ]; then
  IFS=';' read -ra CELLS <<<"$CELLS_ENV"
else
  CELLS=(
    "put|4KiB|"
    "put|8MiB|"
    "get|4KiB|--objects 1000"
    "get|8MiB|--objects 64"
    "stat|4KiB|--objects 2000"
    "delete|4KiB|--objects 10000"
    "list|4KiB|--objects 2000"
    "mixed|1MiB|--objects 200"
  )
fi

printf '\n=== Cairn vs MinIO — warp head-to-head (concurrent=%s duration=%s repeats=%s) ===\n' \
  "$CONCURRENT" "$DURATION" "$REPEATS"
ROWS=()          # markdown rows
CSV_ROWS=()      # csv rows
JSON="["
total_errors=0
alarms=0
for cell in "${CELLS[@]}"; do
  IFS='|' read -r op size extra <<<"$cell"
  # shellcheck disable=SC2086
  cres="$(run_target "127.0.0.1:$CPORT" "$CAK" "$CSK" "$op" "$size" $extra)"
  # shellcheck disable=SC2086
  mres="$(run_target "127.0.0.1:$MPORT" "$MAK" "$MSK" "$op" "$size" $extra)"
  co="${cres%%,*}"; cm="$(printf '%s' "$cres" | cut -d, -f2)"; ce="${cres##*,}"
  mo="${mres%%,*}"; mm="$(printf '%s' "$mres" | cut -d, -f2)"; me="${mres##*,}"
  total_errors=$((total_errors + ce + me))
  # unit + verdict on obj/s (size-invariant rate)
  unit="obj/s"; cval="$co"; mval="$mo"
  verdict="—"
  if [ "$co" != "NA" ] && [ "$mo" != "NA" ]; then
    verdict="$(awk -v c="$co" -v m="$mo" -v a="$SOFT_ALARM_RATIO" 'BEGIN{
      if(c>=m){printf "Cairn %.2fx", c/m} else {r=m/c; printf "MinIO %.2fx", r; if(r>a) printf " ALARM"}}')"
    case "$verdict" in *ALARM*) alarms=$((alarms+1)); verdict="⚠️ ${verdict% ALARM}";; esac
  fi
  printf '  %-6s %-5s | Cairn %8s / MinIO %8s %s -> %s (errs C:%s M:%s)\n' \
    "$op" "$size" "$cval" "$mval" "$unit" "$verdict" "$ce" "$me"
  ROWS+=("| \`$op\` | $size | $co | $mo | ${cm} / ${mm} | $verdict |")
  CSV_ROWS+=("$op,$size,$co,$mo,$cm,$mm,$((ce+me))")
  JSON="$JSON{\"op\":\"$op\",\"size\":\"$size\",\"cairn_obj_s\":\"$co\",\"minio_obj_s\":\"$mo\",\"cairn_mib_s\":\"$cm\",\"minio_mib_s\":\"$mm\",\"errors\":$((ce+me))},"
done
JSON="${JSON%,}]"

# per-engine resource usage while serving (mean CPU%, peak & mean RSS in MB)
read -r C_CPU C_PKMB C_AVGMB <<<"$(res_stats "$RES_C")"
read -r M_CPU M_PKMB M_AVGMB <<<"$(res_stats "$RES_M")"
NCPU=$(nproc 2>/dev/null || echo '?')
MEMGB=$(awk '/MemTotal/{printf "%.0f", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo '?')

# --- 5. emit CSV / JSON / markdown ---------------------------------------------------------------
if [ -n "$BENCH_CSV" ]; then
  { echo "op,obj_size,cairn_obj_s,minio_obj_s,cairn_mib_s,minio_mib_s,errors"
    printf '%s\n' "${CSV_ROWS[@]}"
    echo "# resources: cairn cpu_pct=$C_CPU peak_rss_mb=$C_PKMB mean_rss_mb=$C_AVGMB"
    echo "# resources: minio cpu_pct=$M_CPU peak_rss_mb=$M_PKMB mean_rss_mb=$M_AVGMB"; } > "$BENCH_CSV"
fi
[ -n "$BENCH_JSON" ] && printf '%s\n' "$JSON" > "$BENCH_JSON"

emit_md() {
  echo "## Cairn vs MinIO — warp head-to-head"
  echo ""
  echo "Host: ${NCPU} vCPU / ${MEMGB} GB. Both single-node/single-drive, plaintext HTTP, warp v${WARP_VERSION} vs MinIO ${MINIO_VERSION}."
  echo "\`concurrent=${CONCURRENT} duration=${DURATION} repeats=${REPEATS}\`. **Ratio, not absolutes, is the signal** (contended runner)."
  echo ""
  echo "| op | size | Cairn obj/s | MinIO obj/s | MiB/s (C/M) | verdict |"
  echo "|---|---|--:|--:|--:|---|"
  printf '%s\n' "${ROWS[@]}"
  echo ""
  echo "**Resource cost while serving** (server process, sampled ~1 Hz during its own runs):"
  echo ""
  echo "| engine | mean CPU | peak RSS | mean RSS |"
  echo "|---|--:|--:|--:|"
  echo "| Cairn | ${C_CPU}% | ${C_PKMB} MB | ${C_AVGMB} MB |"
  echo "| MinIO | ${M_CPU}% | ${M_PKMB} MB | ${M_AVGMB} MB |"
}
emit_md
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then emit_md >> "$GITHUB_STEP_SUMMARY"; fi

# --- 6. verdict: gate ONLY on warp operation errors (robust to noise), never on who is faster -----
printf '\n=== summary ===\n'
[ "$alarms" -gt 0 ] && note "⚠️  $alarms op(s) where Cairn is >${SOFT_ALARM_RATIO}x slower (non-fatal, informational)"
if [ "$total_errors" -eq 0 ]; then
  note "clean comparison — zero warp operation errors on either server"
  exit 0
fi
printf 'WARP REPORTED %s OPERATION ERROR(S) across the two servers.\n' "$total_errors" >&2
exit 1
