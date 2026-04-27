#!/bin/bash
set -euo pipefail

# Run a complete TLA+ check: parse, translate PlusCal (if present), then model-check
# Usage: check.sh <spec.tla> [--config spec.cfg] [--workers auto]

SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$SKILL_DIR/lib/tla2tools.jar"

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: tla2tools.jar not found. Run: bash $SKILL_DIR/scripts/setup.sh"
  exit 1
fi

SPEC="${1:?Usage: check.sh <spec.tla> [--config spec.cfg]}"
shift

# Step 1: Translate PlusCal if the spec contains an algorithm
if grep -q '\-\-algorithm\|--fair algorithm' "$SPEC" 2>/dev/null; then
  echo "=== Translating PlusCal ==="
  java -cp "$JAR_PATH" pcal.trans -nocfg "$SPEC" 2>&1
  echo ""
fi

# Step 2: Parse with SANY
echo "=== Parsing with SANY ==="
java -cp "$JAR_PATH" tla2sany.SANY "$SPEC" 2>&1
echo ""

# Step 3: Run TLC
echo "=== Model Checking with TLC ==="
exec bash "$SKILL_DIR/scripts/tlc.sh" "$SPEC" "$@"
