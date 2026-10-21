#!/bin/bash
set -euo pipefail

# TLA+ Tools Setup Script
# Downloads tla2tools.jar and verifies Java 11+ is available

SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="$SKILL_DIR/lib/tla2tools.jar"
TLA2TOOLS_VERSION="1.8.0"
TLA2TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA2TOOLS_VERSION}/tla2tools.jar"
MAVEN_URL="https://repo1.maven.org/maven2/org/lamport/tla2tools/${TLA2TOOLS_VERSION}/tla2tools-${TLA2TOOLS_VERSION}.jar"

# Check Java version
if ! command -v java &>/dev/null; then
  echo "ERROR: Java not found. TLA+ tools require Java 11+."
  echo "Install with: brew install openjdk@11"
  exit 1
fi

JAVA_VER=$(java -version 2>&1 | head -1 | sed 's/.*"\([0-9]*\)\..*/\1/')
if [ "$JAVA_VER" -lt 11 ]; then
  echo "ERROR: Java $JAVA_VER found, but TLA+ tools require Java 11+."
  exit 1
fi
echo "OK: Java $JAVA_VER found."

# Download tla2tools.jar if missing
mkdir -p "$SKILL_DIR/lib"
if [ -f "$JAR_PATH" ]; then
  echo "OK: tla2tools.jar already present at $JAR_PATH"
else
  echo "Downloading tla2tools.jar v${TLA2TOOLS_VERSION}..."
  # Try GitHub releases first, then Maven Central
  if curl -fsSL -L -o "$JAR_PATH" "$TLA2TOOLS_URL" 2>/dev/null; then
    echo "OK: Downloaded from GitHub releases."
  elif curl -fsSL -L -o "$JAR_PATH" "$MAVEN_URL" 2>/dev/null; then
    echo "OK: Downloaded from Maven Central."
  else
    echo "Download failed. Trying to build from local tlaplus source..."
    # Try building from the tlaplus repo if present nearby
    TLAPLUS_SRC=""
    for candidate in \
      "$SKILL_DIR/../../tlaplus/tlaplus" \
      "$SKILL_DIR/../../../tlaplus" \
      "$HOME/p/ai/tlaplus/tlaplus" \
      "./tlaplus"; do
      if [ -f "$candidate/tlatools/org.lamport.tlatools/customBuild.xml" ]; then
        TLAPLUS_SRC="$(cd "$candidate" && pwd)"
        break
      fi
    done

    if [ -n "$TLAPLUS_SRC" ] && command -v ant &>/dev/null; then
      echo "Found tlaplus source at: $TLAPLUS_SRC"
      cd "$TLAPLUS_SRC/tlatools/org.lamport.tlatools"
      mkdir -p test-class
      ant -f customBuild.xml compile dist 2>&1 | tail -5
      if [ -f "dist/tla2tools.jar" ]; then
        cp dist/tla2tools.jar "$JAR_PATH"
        echo "OK: Built tla2tools.jar from source."
      else
        echo "ERROR: Build failed."
        exit 1
      fi
    else
      echo ""
      echo "ERROR: Could not download or build tla2tools.jar."
      echo ""
      echo "Please download it manually and place at:"
      echo "  $JAR_PATH"
      echo ""
      echo "Download from:"
      echo "  https://github.com/tlaplus/tlaplus/releases/download/v${TLA2TOOLS_VERSION}/tla2tools.jar"
      echo ""
      echo "Or clone the tlaplus repo and build with: ant -f customBuild.xml dist"
      exit 1
    fi
  fi
fi

# Verify the jar works
if java -cp "$JAR_PATH" tlc2.TLC -h 2>&1 | head -3; then
  echo ""
  echo "Setup complete. JAR at: $JAR_PATH"
else
  echo "ERROR: tla2tools.jar appears corrupted. Delete it and re-run setup."
  rm -f "$JAR_PATH"
  exit 1
fi
