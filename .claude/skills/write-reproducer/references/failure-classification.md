# Failure Classification

Use this reference to classify test results and distinguish issue-relevant failures from noise.

## Why classification matters

The most common failure mode in LLM-generated repros is declaring success on a non-relevant failure: a test fails, but for the wrong reason (setup error, missing dependency, hallucinated API). Classification prevents this.

## Classification scheme

### REPRODUCED
The test fails with a failure that directly corresponds to the described bug.

Requirements:
- The failure matches the Phase 2 failure criterion
- The failure occurs in the code under test, not in test setup
- The failure message/exception can be explained in terms of the bug report
- Removing the buggy behavior would make the test pass

### CANDIDATE
The test fails with behavior *consistent* with the report, but certainty is incomplete.

Common reasons for CANDIDATE instead of REPRODUCED:
- Expected behavior was inferred (not stated by the user)
- The failure matches the description but the assertion tests a proxy, not the exact symptom
- The bug is nondeterministic and the test only triggered once

### BLOCKED
The repro cannot be executed or the oracle is too ambiguous.

Common reasons:
- Missing external service that can't be emulated
- Missing data/schema/configuration
- The bug report is too vague to construct an oracle
- The repro requires infrastructure not available locally

### Classification of individual test runs

When executing a candidate repro, classify the result as one of:

| Classification | Meaning | Next action |
|---------------|---------|-------------|
| PASS | Test passes, does not reproduce | Strengthen the oracle or scenario |
| COMPILE_ERROR | Code doesn't compile | Fix imports, types, dependencies |
| TEST_DISCOVERY_ERROR | Test framework can't find/load the test | Fix test placement, naming, annotations |
| SETUP_ERROR | Test infrastructure fails (DB connection, missing fixture) | Fix setup, add missing dependencies |
| UNRELATED_RUNTIME_ERROR | Runtime error not matching the bug | Fix the test; the error is a test problem |
| UNRELATED_ASSERTION | Assertion fails but for wrong reason | Rewrite the assertion to match the real bug |
| RELATED_RUNTIME_ERROR | Runtime error matching the bug description | This may be the repro — verify it matches Phase 2 |
| RELATED_ASSERTION | Assertion fails for the right reason | This is likely the repro — verify against Phase 2 |
| FLAKY_OR_NONDETERMINISTIC | Inconsistent results across runs | Wrap in N-iteration harness, report rate |
| TIMEOUT | Test did not complete | May indicate hang/deadlock (if that's the bug) or may indicate broken test setup |
| NEEDS_EXTERNAL_SYSTEM | Requires a service not available | Escalate tier or use testcontainers/docker-compose |
| ORACLE_AMBIGUOUS | Multiple interpretations of correct behavior | Ask the user to clarify expected behavior |

## How to distinguish issue-relevant failures from setup errors

A failure is issue-relevant if:

1. The exception/assertion occurs in the **code under test**, not in test infrastructure
2. The stack trace passes through the **suspected code path** described in the bug report
3. The failure **message** contains the same keywords/values as the bug description
4. The failure **persists** after fixing test scaffolding problems
5. The failure **disappears** when the described bug is fixed (if a fix is available)

A failure is a setup error if:

1. The exception occurs before the trigger code runs (in `@Before`, fixture setup, import resolution)
2. The exception is about **missing classes, methods, or dependencies** (not about the bug)
3. The exception is about **connection refused, port not available, service unavailable**
4. The test fails the same way regardless of whether the bug is present

## Recording results

For each candidate run, record:

```
Command:        <exact command>
Exit code:      <number>
Classification: <one of the above>
Relevance:      <why this matches or doesn't match the bug>
Stdout excerpt: <first relevant lines>
Stderr excerpt: <first relevant lines>
Stack trace:    <top 5 frames if applicable>
Next action:    <what to do next>
```
