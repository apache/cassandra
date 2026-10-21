#!/usr/bin/env bash
# Detect project context: language, build system, test framework, test command.
# Prints key=value pairs to stdout.

set -euo pipefail

DIR="${1:-.}"

detect_language() {
    if [ -f "$DIR/Cargo.toml" ]; then echo "rust"
    elif [ -f "$DIR/pom.xml" ] || [ -f "$DIR/build.gradle" ] || [ -f "$DIR/build.gradle.kts" ]; then echo "java"
    elif [ -f "$DIR/pyproject.toml" ] || [ -f "$DIR/setup.py" ] || [ -f "$DIR/setup.cfg" ]; then echo "python"
    elif [ -f "$DIR/go.mod" ]; then echo "go"
    elif [ -f "$DIR/package.json" ]; then echo "javascript"
    elif [ -f "$DIR/CMakeLists.txt" ] || [ -f "$DIR/Makefile" ]; then echo "c_cpp"
    elif [ -f "$DIR/mix.exs" ]; then echo "elixir"
    elif [ -f "$DIR/build.sbt" ]; then echo "scala"
    elif [ -f "$DIR/project.clj" ] || [ -f "$DIR/deps.edn" ]; then echo "clojure"
    else echo "unknown"
    fi
}

detect_build_system() {
    if [ -f "$DIR/Cargo.toml" ]; then echo "cargo"
    elif [ -f "$DIR/pom.xml" ]; then echo "maven"
    elif [ -f "$DIR/build.gradle" ] || [ -f "$DIR/build.gradle.kts" ]; then echo "gradle"
    elif [ -f "$DIR/pyproject.toml" ]; then echo "pyproject"
    elif [ -f "$DIR/setup.py" ]; then echo "setuptools"
    elif [ -f "$DIR/go.mod" ]; then echo "go_modules"
    elif [ -f "$DIR/package.json" ]; then
        if [ -f "$DIR/pnpm-lock.yaml" ]; then echo "pnpm"
        elif [ -f "$DIR/yarn.lock" ]; then echo "yarn"
        else echo "npm"
        fi
    elif [ -f "$DIR/CMakeLists.txt" ]; then echo "cmake"
    elif [ -f "$DIR/Makefile" ]; then echo "make"
    else echo "unknown"
    fi
}

detect_test_framework() {
    local lang
    lang=$(detect_language)
    case "$lang" in
        rust) echo "cargo_test" ;;
        java)
            if grep -rql "junit-jupiter\|org.junit.jupiter" "$DIR/pom.xml" "$DIR/build.gradle" "$DIR/build.gradle.kts" 2>/dev/null; then
                echo "junit5"
            elif grep -rql "junit\|org.junit" "$DIR/pom.xml" "$DIR/build.gradle" "$DIR/build.gradle.kts" 2>/dev/null; then
                echo "junit4"
            else
                echo "junit5"  # default for java
            fi
            ;;
        python)
            if grep -rql "pytest" "$DIR/pyproject.toml" "$DIR/setup.cfg" "$DIR/tox.ini" 2>/dev/null; then
                echo "pytest"
            elif [ -f "$DIR/tox.ini" ]; then
                echo "tox"
            else
                echo "pytest"  # default for python
            fi
            ;;
        go) echo "go_test" ;;
        javascript)
            if grep -q '"vitest"' "$DIR/package.json" 2>/dev/null; then echo "vitest"
            elif grep -q '"jest"' "$DIR/package.json" 2>/dev/null; then echo "jest"
            elif grep -q '"mocha"' "$DIR/package.json" 2>/dev/null; then echo "mocha"
            else echo "jest"
            fi
            ;;
        *) echo "unknown" ;;
    esac
}

detect_test_command() {
    local framework
    framework=$(detect_test_framework)
    case "$framework" in
        cargo_test) echo "cargo test" ;;
        junit5|junit4)
            if [ -f "$DIR/pom.xml" ]; then echo "mvn test"
            else echo "gradle test"
            fi
            ;;
        pytest) echo "pytest" ;;
        tox) echo "tox" ;;
        go_test) echo "go test ./..." ;;
        vitest) echo "npx vitest run" ;;
        jest) echo "npx jest" ;;
        mocha) echo "npx mocha" ;;
        *) echo "unknown" ;;
    esac
}

detect_test_dirs() {
    local lang
    lang=$(detect_language)
    case "$lang" in
        java)
            if [ -d "$DIR/src/test" ]; then echo "src/test/java"
            elif [ -d "$DIR/test" ]; then echo "test"
            fi
            ;;
        python)
            if [ -d "$DIR/tests" ]; then echo "tests"
            elif [ -d "$DIR/test" ]; then echo "test"
            fi
            ;;
        rust) echo "src (inline) or tests/" ;;
        go) echo "alongside source (*_test.go)" ;;
        javascript)
            if [ -d "$DIR/__tests__" ]; then echo "__tests__"
            elif [ -d "$DIR/tests" ]; then echo "tests"
            elif [ -d "$DIR/test" ]; then echo "test"
            fi
            ;;
        *) echo "unknown" ;;
    esac
}

echo "language=$(detect_language)"
echo "build_system=$(detect_build_system)"
echo "test_framework=$(detect_test_framework)"
echo "test_command=$(detect_test_command)"
echo "test_dirs=$(detect_test_dirs)"
