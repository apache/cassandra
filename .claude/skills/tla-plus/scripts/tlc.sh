#!/bin/bash
set -euo pipefail

# TLC Model Checker Runner
# Usage: tlc.sh <spec.tla> [--config spec.cfg] [--workers auto] [extra TLC flags...]

SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$SKILL_DIR/lib/tla2tools.jar"

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: tla2tools.jar not found. Run setup.sh first."
  exit 1
fi

SPEC=""
CONFIG=""
WORKERS="auto"
EXTRA_ARGS=()
NO_DEADLOCK=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config|-config)
      CONFIG="$2"; shift 2;;
    --workers|-workers)
      WORKERS="$2"; shift 2;;
    --no-deadlock)
      NO_DEADLOCK="1"; shift;;
    *.tla)
      SPEC="$1"; shift;;
    *)
      EXTRA_ARGS+=("$1"); shift;;
  esac
done

if [ -z "$SPEC" ]; then
  echo "Usage: tlc.sh <spec.tla> [--config spec.cfg] [--workers auto] [--no-deadlock]"
  exit 1
fi

# Derive config from spec name if not provided
if [ -z "$CONFIG" ]; then
  CFG_CANDIDATE="${SPEC%.tla}.cfg"
  if [ -f "$CFG_CANDIDATE" ]; then
    CONFIG="$CFG_CANDIDATE"
  fi
fi

CMD=(java -XX:+UseParallelGC -cp "$JAR_PATH" tlc2.TLC)
CMD+=(-workers "$WORKERS")

if [ -n "$CONFIG" ]; then
  CMD+=(-config "$CONFIG")
fi

if [ -n "$NO_DEADLOCK" ]; then
  CMD+=(-deadlock)
fi

CMD+=(-noGenerateSpecTE)
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
  CMD+=("${EXTRA_ARGS[@]}")
fi
CMD+=("$SPEC")

echo "Running: ${CMD[*]}"
echo "---"
exec "${CMD[@]}"
