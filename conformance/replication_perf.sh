#!/usr/bin/env bash
# Controlled two-node replication throughput benchmark (ARCH 20, 30).
#
# Starts a target Cairn on 127.0.0.1:9101 and a source on 127.0.0.1:9102 under
# /var/tmp, then drives Warp `put` against a replicated bucket and a control
# bucket in interleaved pairs. Every arm records source PUT rate, shipped
# bytes/objects, live lag and queue depth, source CPU and a byte-for-byte sample
# verification. The Python driver is conformance/replication_perf.py; this
# wrapper only supplies defaults and takes the host-wide benchmark lock.
#
# Usage:
#   conformance/replication_perf.sh
#   conformance/replication_perf.sh --pairs 2 --duration 25
#   conformance/replication_perf.sh --shape 1m --pairs 1 --duration 10
#   conformance/replication_perf.sh --keep-data
#
# The lock is shared with every other storage benchmark on this host
# (/tmp/cairn-deep-perf.lock). flock blocks, so this waits for an in-flight
# campaign instead of interfering with it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-/home/dev/Cairn/target/release/cairn}"
PY="${PY:-/tmp/cairnvenv/bin/python}"
WARP="${WARP:-/tmp/cairn-perf-tools/warp}"
LOCK="${LOCK:-/tmp/cairn-deep-perf.lock}"
OUTPUT="${OUTPUT:-$ROOT/docs/performance-evidence/2026-11/replication-perf-$(date +%Y-%m-%d).json}"

[ -x "$BIN" ] || { printf 'FAIL: cairn binary not executable: %s\n' "$BIN" >&2; exit 1; }
[ -x "$PY" ] || { printf 'FAIL: python interpreter not found: %s\n' "$PY" >&2; exit 1; }
[ -x "$WARP" ] || { printf 'FAIL: warp not executable: %s\n' "$WARP" >&2; exit 1; }
"$PY" -c "import boto3" 2>/dev/null || {
  printf 'FAIL: boto3 is not importable by %s\n' "$PY" >&2; exit 1;
}

args=("$@")
has_output=0
for arg in "${args[@]}"; do
  case "$arg" in
    --output|--output=*) has_output=1 ;;
  esac
done
if [ "$has_output" -eq 0 ]; then
  args+=(--output "$OUTPUT")
fi

command="$(printf '%q ' "$PY" "$ROOT/conformance/replication_perf.py" \
  --binary "$BIN" --warp "$WARP" "${args[@]}")"
printf '  waiting for host-wide benchmark lock %s (flock blocks)\n' "$LOCK"
exec flock -x "$LOCK" -c "$command"
