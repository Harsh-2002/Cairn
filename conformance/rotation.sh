#!/usr/bin/env bash
# End-to-end gate for master-key rotation (audit #29): the full key lifecycle — add a key,
# re-wrap stored secrets onto it, retire the old key — plus the fail-closed startup retire-gate,
# all under sharding, driven against a real cairn binary. The Python driver owns the server
# lifecycle (it restarts the server with different CAIRN_MASTER_KEY_RING configs).
#
# Usage: BIN=target/debug/cairn PY=python3 conformance/rotation.sh
set -euo pipefail

BIN="${BIN:-target/debug/cairn}"
PY="${PY:-python3}"
DATA="$(mktemp -d)"
cleanup() { rm -rf "$DATA"; }
trap cleanup EXIT

BIN="$BIN" DATA="$DATA" PORT="${PORT:-9079}" SHARDS="${SHARDS:-4}" \
  "$PY" "$(dirname "$0")/rotation.py"
