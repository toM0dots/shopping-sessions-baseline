#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
# Shell parameter expansion avoids launching dirname on every timed run.
case "$0" in
  */*) APP_DIR=${0%/*} ;;
  *) APP_DIR=. ;;
esac
exec "$APP_DIR/build/shopping-sessions"
