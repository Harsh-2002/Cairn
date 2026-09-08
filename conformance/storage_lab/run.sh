#!/bin/sh
# Laboratory arguments configure the coordinator, never the production server.
set -eu
LAB_SCRIPT_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$LAB_SCRIPT_DIR/lab.py" "$@"
