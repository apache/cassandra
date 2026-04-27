#!/bin/bash
set -euo pipefail

# PlusCal Translator
# Usage: translate.sh <spec.tla> [-nocfg]

SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$SKILL_DIR/lib/tla2tools.jar"

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: tla2tools.jar not found. Run setup.sh first."
  exit 1
fi

if [ -z "${1:-}" ]; then
  echo "Usage: translate.sh <spec.tla> [-nocfg]"
  exit 1
fi

SPEC="$1"
shift

exec java -cp "$JAR_PATH" pcal.trans "$@" "$SPEC"
