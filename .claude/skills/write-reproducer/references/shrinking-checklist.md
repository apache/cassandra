# Shrinking Checklist

Use this checklist after every shrinking step to prevent over-minimization — shrinking into a different bug than the one described.

## After each removal, verify ALL of the following

-  **Same failure criterion**: the failure matches the Phase 2 failure criterion verbatim
-  **Same exception type**: if the oracle is an exception, it's the same class (not a different exception from broken test setup)
-  **Same assertion message fragment**: the assertion failure message contains the same key words
-  **Same exit code**: if the oracle is an exit code, it hasn't changed
-  **Same log line**: if the oracle is a log grep, the same line still appears
-  **Same stack trace root**: the deepest application frame in the stack trace is the same (test framework frames above it may differ)

If ANY answer is "no", **revert the last shrinking step**. That element was load-bearing.

## Trace features

For ambiguous cases, define a small set of "trace features" before starting shrinking:

1. The exception class name
2. The first application stack frame (file:line or class.method)
3. A key phrase from the error message
4. The exit code or signal

All trace features must match after every shrinking step. If any changes, the shrink went too far.

## Common over-minimization traps

### Trap: Removing setup causes a different crash
You removed a setup step, the test still fails, but now it's a `NullPointerException` in initialization instead of the `IndexOutOfBoundsException` described in the bug. **Revert** — the setup step was needed to reach the real bug.

### Trap: Reducing concurrency removes the race
You reduced from 10 threads to 1, and the test still fails... but now it fails for a sequential logic bug, not the race condition. The concurrency was essential. **Revert** and try reducing to 2 threads instead.

### Trap: Shrinking the input changes the code path
You reduced the input from 100 items to 1 item, and the test still fails... but the code now takes a different branch (e.g., a "small input" fast path instead of the "large input" path where the bug lives). **Revert** and find the minimum input size that takes the same code path.

### Trap: Removing configuration hides the bug
You reverted a non-default config to its default, and the test passes. That config was a trigger condition. **Keep it** and document why it's needed.

## When to stop shrinking

Stop when:
1. Removing any single element causes the test to pass (or fail differently)
2. All the shrinking checklist items pass
3. Every remaining line is load-bearing — it either sets up the trigger, executes the trigger, or checks the oracle

A good minimal repro has three clear sections (TRIGGER, HARNESS, ORACLE) and nothing else.

## When to use automated reducers instead

If the failing input is large (>100 lines of text, >1KB of binary), consider:
- **C-Reduce / creduce**: for C/C++ source files
- **Perses**: for any language with an ANTLR grammar (language-agnostic)
- **cargo-fuzz tmin**: for Rust fuzz corpus entries
- **libFuzzer -minimize_crash=1**: for libFuzzer corpus entries
- **`scripts/shrink_text.py`**: for line-level ddmin on text files

These tools automate the "remove and re-test" loop and are much faster than manual reduction for large inputs.
