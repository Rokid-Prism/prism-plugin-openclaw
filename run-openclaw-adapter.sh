#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -x ./bin/openclaw-adapter ]; then
  echo "openclaw adapter binary is missing; run: go build -o ./bin/openclaw-adapter ./cmd/openclaw-adapter" >&2
  exit 1
fi

exec ./bin/openclaw-adapter "$@"
