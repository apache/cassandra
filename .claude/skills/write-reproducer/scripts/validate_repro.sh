#!/usr/bin/env bash
# Validate a reproducer: run it and check the oracle.
#
# Usage:
#   validate_repro.sh COMMAND [--pattern REGEX] [--exit-code N] [--runs N]
#
# Examples:
#   validate_repro.sh "pytest tests/test_repro.py -v" --pattern "AssertionError"
#   validate_repro.sh "mvn test -pl core -Dtest=ReproTest" --exit-code 1
#   validate_repro.sh "cargo test repro_issue" --pattern "panicked" --runs 10

set -euo pipefail

COMMAND=""
PATTERN=""
EXPECTED_EXIT=""
RUNS=1
TIMEOUT=120

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pattern)    PATTERN="$2"; shift 2 ;;
        --exit-code)  EXPECTED_EXIT="$2"; shift 2 ;;
        --runs)       RUNS="$2"; shift 2 ;;
        --timeout)    TIMEOUT="$2"; shift 2 ;;
        *)
            if [ -z "$COMMAND" ]; then
                COMMAND="$1"
            else
                COMMAND="$COMMAND $1"
            fi
            shift
            ;;
    esac
done

if [ -z "$COMMAND" ]; then
    echo "Usage: validate_repro.sh COMMAND [--pattern REGEX] [--exit-code N] [--runs N]" >&2
    exit 1
fi

FAILURES=0
PASSES=0
ERRORS=0

for i in $(seq 1 "$RUNS"); do
    OUTPUT_FILE=$(mktemp)
    EXIT_CODE=0

    if timeout "$TIMEOUT" bash -c "$COMMAND" > "$OUTPUT_FILE" 2>&1; then
        EXIT_CODE=0
    else
        EXIT_CODE=$?
    fi

    COMBINED=$(cat "$OUTPUT_FILE")
    rm -f "$OUTPUT_FILE"

    # Check exit code
    if [ -n "$EXPECTED_EXIT" ] && [ "$EXIT_CODE" != "$EXPECTED_EXIT" ]; then
        ERRORS=$((ERRORS + 1))
        if [ "$RUNS" -eq 1 ]; then
            echo "UNEXPECTED EXIT CODE: expected $EXPECTED_EXIT, got $EXIT_CODE"
        fi
        continue
    fi

    # Check if the test failed (non-zero exit)
    if [ "$EXIT_CODE" -eq 0 ]; then
        PASSES=$((PASSES + 1))
        continue
    fi

    # Check pattern if specified
    if [ -n "$PATTERN" ]; then
        if echo "$COMBINED" | grep -qE "$PATTERN"; then
            FAILURES=$((FAILURES + 1))
        else
            ERRORS=$((ERRORS + 1))
            if [ "$RUNS" -eq 1 ]; then
                echo "WRONG FAILURE: test failed but pattern '$PATTERN' not found in output"
                echo "--- output ---"
                echo "$COMBINED" | head -20
                echo "--- end ---"
            fi
        fi
    else
        FAILURES=$((FAILURES + 1))
    fi
done

echo ""
echo "=== Validation Report ==="
echo "Command:  $COMMAND"
echo "Runs:     $RUNS"
echo "Failures: $FAILURES (issue-relevant)"
echo "Passes:   $PASSES"
echo "Errors:   $ERRORS (wrong failure or unexpected exit)"

if [ "$FAILURES" -gt 0 ]; then
    if [ "$RUNS" -eq 1 ]; then
        echo ""
        echo "RESULT: REPRODUCED"
    else
        RATE=$(echo "scale=1; $FAILURES * 100 / $RUNS" | bc)
        echo "Rate:     ${RATE}% ($FAILURES/$RUNS)"
        echo ""
        echo "RESULT: REPRODUCED (${RATE}% failure rate)"
    fi
    exit 0
elif [ "$PASSES" -eq "$RUNS" ]; then
    echo ""
    echo "RESULT: NOT REPRODUCED (all runs passed)"
    exit 1
else
    echo ""
    echo "RESULT: INCONCLUSIVE (failures were not issue-relevant)"
    exit 2
fi
