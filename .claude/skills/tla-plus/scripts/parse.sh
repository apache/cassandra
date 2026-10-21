#!/bin/bash
set -euo pipefail

# SANY Parser - Parse and validate TLA+ specs
# Usage: parse.sh <spec.tla>

SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$SKILL_DIR/lib/tla2tools.jar"

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: tla2tools.jar not found. Run setup.sh first."
  exit 1
fi

if [ -z "${1:-}" ]; then
  echo "Usage: parse.sh <spec.tla>"
  exit 1
fi

exec java -cp "$JAR_PATH" tla2sany.SANY "$1"
