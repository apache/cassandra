# Output Contract

Every repro run must produce one of three statuses: REPRODUCED, CANDIDATE, or BLOCKED.

## REPRODUCED

Use only when there is an executable failure that matches the described bug.

Required elements:
- **Status**: REPRODUCED
- **Files**: list of created/modified files with their paths
- **Run command**: exact command to execute the repro
- **Failure**: the observed failure message, exception, or assertion
- **Why this matches**: one sentence connecting the failure to the bug description
- **Failure criterion**: verbatim from Phase 2
- **Minimization notes**: what was removed during shrinking, what was kept and why

Optional:
- Seed (if randomized)
- Failure rate (if nondeterministic)
- Environment notes

Example:
```
Status: REPRODUCED

Files:
- src/test/java/com/example/ReproIssue42Test.java

Run command:
mvn test -pl core -Dtest=ReproIssue42Test

Failure:
AssertionError: expected [50] but was [40]
at ReproIssue42Test.testSumExcludesDeletedRows(ReproIssue42Test.java:38)

Why this matches:
The bug report says SUM should include rows 2 and 3 (20+30=50) but returns 40,
matching the described symptom of deleted row 1's value being subtracted twice.

Failure criterion (Phase 2):
assert sum == 50, actual sum is 40

Minimization:
Reduced from 10 rows to 3. Removed unrelated columns (name, email).
Removed non-default compaction config (not needed to trigger).

This test should fail on the current code with:
"AssertionError: expected [50] but was [40]"
```

## CANDIDATE

Use when the repro is plausible but not fully proven.

Required elements:
- **Status**: CANDIDATE
- **Reason**: why this is CANDIDATE rather than REPRODUCED
- **Files**: list of created files
- **Run command**: exact command
- **Observed failure**: what happened
- **What needs confirmation**: specific questions for the user

Example:
```
Status: CANDIDATE

Reason:
The expected value was inferred from existing tests, not from the bug report.
The test fails with behavior consistent with the report, but the oracle may
be testing a proxy rather than the exact symptom.

Files:
- tests/test_repro_issue_42.py

Run command:
pytest tests/test_repro_issue_42.py -v

Observed failure:
AssertionError: assert result == 50, got 40

What needs confirmation:
- Is the expected sum 50? The bug report says "wrong result" but doesn't state the correct value.
- Should deleted rows be excluded from SUM? The test assumes yes.
```

## BLOCKED

Use when execution is impossible or the oracle is too ambiguous to construct a meaningful repro.

Required elements:
- **Status**: BLOCKED
- **Reason**: what prevented successful reproduction
- **What was attempted**: list of approaches tried
- **What was produced**: any partial artifacts (standalone harness, mocked scenario)
- **What's needed**: specific information or infrastructure that would unblock
- **Next best path**: recommendation for the user

Example:
```
Status: BLOCKED

Reason:
The bug requires a running Kafka cluster with specific topic configuration,
and no Kafka infrastructure is available locally or via testcontainers.

What was attempted:
1. Tried to reproduce with an in-memory mock — bug requires real partition rebalancing
2. Tried testcontainers-kafka — image pull failed (no Docker daemon)

What was produced:
- tests/test_repro_issue_42.py (test logic is correct, needs Kafka)
- docker-compose-kafka.yml (untested, would need Docker)

What's needed:
- Docker daemon running, or
- Access to a Kafka cluster with admin permissions, or
- Kafka broker logs from the original failure

Next best path:
Run docker-compose-kafka.yml in an environment with Docker,
then execute: pytest tests/test_repro_issue_42.py -v
```
