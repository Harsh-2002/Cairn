#!/bin/sh
# Laboratory arguments configure the coordinator, never the production server.
set -eu
LAB_SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
if [ "${1-}" = fanout ]; then
    shift
    exec python3 "$LAB_SCRIPT_DIR/fanout.py" "$@"
fi
if [ "${1-}" = recovery-cost ]; then
    shift
    exec python3 "$LAB_SCRIPT_DIR/recovery.py" "$@"
fi
if [ "${1-}" = packing-measure ]; then
    shift
    exec python3 "$LAB_SCRIPT_DIR/packing_measure.py" "$@"
fi
if [ "${1-}" = packing ]; then
    shift
    exec python3 "$LAB_SCRIPT_DIR/packing.py" "$@"
fi
if [ "${1-}" = metadata-capacity ]; then
    shift
    exec python3 "$LAB_SCRIPT_DIR/metadata_capacity.py" "$@"
fi
if [ "${1-}" = metadata-alternative ]; then
    shift
    exec python3 "$LAB_SCRIPT_DIR/metadata_alternative.py" "$@"
fi
exec python3 "$LAB_SCRIPT_DIR/lab.py" "$@"
