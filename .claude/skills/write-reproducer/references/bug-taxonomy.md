# Bug Taxonomy

Use this reference when Phase 1 classification is unclear. Each class includes defining characteristics, common signals, and the typical oracle form.

## Crash

The process terminates abnormally.

**Signals**: segfault, SIGABRT, unhandled exception propagating to main, panic, core dump.
**Oracle**: exit code != 0, signal number, or specific panic/exception message.
**Typical tier**: Tier 2-3 (single file or single test).

## Incorrect output (silent wrong result)

The program completes normally but produces the wrong answer.

**Signals**: wrong return value, missing rows, extra rows, wrong aggregation, wrong serialization output.
**Oracle**: `assert expected == actual` with concrete values.
**Typical tier**: Tier 2-3. For database bugs, often Tier 5-6 with a SQL script or metamorphic oracle.

This is the hardest class to repro because there is no crash to trigger — you must know the correct answer. Prefer pass-then-invert when the correct value is known. For database bugs, consider metamorphic testing (NoREC, TLP, PQS) when the correct answer is hard to compute.

## Hang / deadlock

The process stops making progress.

**Signals**: no output, CPU at 0% (deadlock) or 100% (livelock/infinite loop), timeout.
**Oracle**: test timeout + thread dump showing blocked threads, or absence of expected output within time bound.
**Typical tier**: Tier 3 (unit test with timeout) or Tier 6 (cluster with timeout).

Distinguish deadlock (threads waiting on each other) from livelock (threads running but not progressing) from infinite loop (single thread never terminating). Thread dumps are diagnostic for the first two.

## Race condition

The outcome depends on thread/process scheduling.

**Signals**: intermittent failures, "works on my machine", different results on different runs, Heisenbugs that disappear under debugging.
**Oracle**: assertion that fails intermittently; often needs N-iteration harness.
**Typical tier**: Tier 3 (jcstress for JMM races), Tier 6 (cluster test for distributed races), Tier 7 (deterministic simulation).

See `concurrency.md` for detailed guidance.

## Assertion / invariant violation

A documented or implicit invariant is violated.

**Signals**: AssertionError in production code, negative values where only positive expected, duplicates where uniqueness required, sum doesn't balance, ordering violated.
**Oracle**: state the invariant and the violation. Property-based tests are the natural repro form.
**Typical tier**: Tier 3 (property test) or Tier 6 (cluster invariant check).

## Performance regression

An operation became measurably slower or uses more resources than expected.

**Signals**: increased latency, higher CPU/memory usage, benchmark regression.
**Oracle**: `assert duration < threshold` or `assert memory < threshold` with concrete values from a known-good baseline.
**Typical tier**: Tier 3 (microbenchmark) or Tier 4 (benchmark project).

Performance repros are inherently noisy. Pin the hardware, use statistical tests (e.g., compare distributions, not single runs), and document the baseline.

## Resource leak

Resources are not released after use.

**Signals**: growing memory, file descriptor exhaustion, connection pool depletion, thread count growth.
**Oracle**: resource count after N iterations exceeds baseline by more than a threshold, or OOM/EMFILE error.
**Typical tier**: Tier 3 (loop test that measures resources).

## Serialization / encoding bug

Data is incorrectly encoded, decoded, or transformed between representations.

**Signals**: corrupt bytes, wrong field values after deserialization, version incompatibility, schema mismatch.
**Oracle**: round-trip property (`deserialize(serialize(x)) == x`) or specific field assertion.
**Typical tier**: Tier 3 (property test with round-trip check).

## Consistency anomaly (distributed systems)

A distributed system violates its stated consistency guarantee.

**Subtypes** (using Adya/Kingsbury terminology):
- **G0 (dirty write)**: two transactions both modify the same object, neither sees the other
- **G1a (aborted read)**: read data from an aborted transaction
- **G1b (intermediate read)**: read intermediate state of another transaction
- **G1c (circular information flow)**: dependency cycle between committed transactions
- **G2 (anti-dependency cycle)**: serialization anomaly involving anti-dependencies
- **Lost update**: concurrent writes lose one update
- **Stale read**: read returns a value older than a committed write that should be visible
- **Dirty read**: read uncommitted data from another transaction

**Signals**: Jepsen/Elle/Knossos/Porcupine checker failures, data loss after recovery, divergent replicas.
**Oracle**: linearizability checker returns false, Elle reports specific anomaly type, Porcupine finds illegal history.
**Typical tier**: Tier 6 (Jepsen, cluster dtest) or Tier 7 (deterministic simulation).

See `distributed-systems.md` for detailed guidance.

## Metadata / schema bug

The system's metadata (schema, catalog, configuration state) becomes inconsistent.

**Signals**: schema says X but data says Y, dropped column still queryable, phantom table, inconsistent metadata across nodes.
**Oracle**: metadata query returns unexpected result, or operation succeeds that should fail (or vice versa).
**Typical tier**: Tier 3 (single-instance) or Tier 6 (cluster for distributed schema).
