#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -x ./bin/prism-plugin-openclaw ]; then
  echo "OpenClaw adapter binary is missing; run: go build -o ./bin/prism-plugin-openclaw ./cmd/openclaw-adapter" >&2
  exit 1
fi

exec ./bin/prism-plugin-openclaw "$@"
