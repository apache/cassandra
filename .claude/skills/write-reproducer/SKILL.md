---
name: write-reproducer
version: "1.0.0"
description: "Write minimal, reliable bug reproducers (repros) for any project. Use when: (1) a bug report or issue needs a test that reliably demonstrates the failure, (2) converting a described failure scenario into runnable code, (3) minimizing an existing complex test to its essential trigger conditions. Covers the full repro workflow: failure characterization, scope selection, writing the repro, verifying it fails for the right reason, and minimizing to the smallest possible trigger."
---

# Write Bug Reproducer

Your goal is to turn a bug description into a minimal, self-contained, runnable reproduction that triggers a specific, machine-checkable failure criterion.

A reproducer has three parts — keep them distinct in your head and in your code:

- **Trigger**: the minimum input, state, schedule, or environment that causes the bug
- **Harness**: how the trigger is executed (test runner, script, container, cluster test)
- **Oracle**: the machine-checkable failure criterion (assertion, exit code, log pattern, invariant violation)

Follow the five phases below in order. Do not skip ahead. Write the failure criterion before writing any repro code.

## Phase 1: Understand and classify

Restate the bug in one sentence: "The bug is..."

Classify along three axes:

- **Bug class**: crash, incorrect output, hang/deadlock, race condition, assertion/invariant violation, performance regression, resource leak, serialization bug, consistency anomaly. See `references/bug-taxonomy.md` for the full taxonomy when classification is unclear.
- **Scope**: pure function, module, single process, multiple processes, cluster.
- **Determinism**: deterministic, flaky (state what makes it flaky), requires rare schedule or timing.

If the user gave only symptoms with no code, list the 2-4 most likely causes before proceeding. Do not pick one yet — write hypotheses, not conclusions.

If the user included code or pointed at a codebase, read the suspect region and identify the likely code path before moving on.

## Phase 2: Define the failure criterion

This is the most important phase. Before writing any repro code, write out the oracle in one of these machine-checkable forms:

- **Assertion with concrete values**: `assert expected == actual` with both spelled out
- **Exit code**: `expected: 0, actual: 139 (SIGSEGV)`
- **Regex against stdout/stderr**: exact string fragment to match
- **Log grep**: file and pattern
- **Exception type and message fragment**: the specific error and location
- **Linearizability/isolation check result**: checker verdict (e.g., linearizability violation, cycle detected)
- **Invariant violation**: state the invariant in one line; state what breaks it

Record the failure criterion verbatim. It will be used in Phase 4 (to write the assertion) and Phase 5 (to prevent shrinking into a different bug).

Without an explicit oracle, shrinking can produce something that "fails" but not in the described way, and reviewers cannot tell whether the repro is successful.

## Phase 3: Detect context and choose repro form

Inspect the working directory for build-system markers and prefer the project's existing test framework and conventions. Explore existing tests to understand helpers, fixtures, naming patterns, and how integration tests are structured.

Pick the lowest tier on the minimality lattice that can reliably exhibit the failure. See `references/minimality-lattice.md` for tier definitions and decision rules.

- Tier 1: expression / REPL snippet
- Tier 2: single-file snippet with `main`
- Tier 3: single test in the project's existing framework
- Tier 4: multi-file project fixture
- Tier 5: containerized environment + test script
- Tier 6: ephemeral cluster
- Tier 7: deterministic simulation

Write one sentence naming the tier and why, based on what you found in the project.

For concurrency bugs: `references/concurrency.md`
For distributed-systems bugs: `references/distributed-systems.md`
For invariant bugs where a property test is the natural form: `references/property-based-testing.md`
For bugs found by fuzzing or where a fuzz harness is the natural repro: `references/fuzzing.md`

## Phase 4: Draft the repro

### Structure

Label the three parts in comments:

```
// TRIGGER: <what causes the bug>
// HARNESS: <how we run it>
// ORACLE:  <what we check>
```

Use the project's test framework and conventions. Match existing test style. Do not introduce a new framework unless the existing one cannot express the bug.

### The pass-then-invert strategy

For subtle assertion bugs where the correct behavior is easy to state but the failing test is hard to get right, use **pass-then-invert**: first write a test that *passes* on the current buggy behavior (mirroring the observed wrong output in its assertions), then invert the assertions so they encode the *correct* expectation and therefore fail on the bug. This reduces hallucinated APIs and setup errors compared to writing a failing test directly.

Use pass-then-invert when:
- The bug produces a wrong value (not a crash or exception)
- The correct expected value is known
- Direct "write a failing test" attempts keep failing for setup reasons

Do not use pass-then-invert when:
- The bug is a crash, exception, or hang — just trigger it directly
- The oracle is an absence check (no exception, no crash) — inversion doesn't apply

### Verification before presenting

The repro must compile or parse cleanly. Resolve imports against the project's actual dependencies. Do not hallucinate APIs — if unsure of a method signature, read the library source or ask.

If execution is available, run the repro and confirm the failure criterion triggers. If not, trace through the code mentally and state your confidence level.

### Generating multiple candidates

When the bug is unclear or the first attempt fails, generate 2-5 candidate repro strategies:
- Smallest direct unit test
- Integration test if state/config matters
- Standalone script if no test harness is obvious
- Property-based test if the bug is about an invariant
- Fuzz seed / deterministic scenario for correctness bugs

Execute each candidate with a timeout. Classify each result using `references/failure-classification.md`. Refine until one produces an issue-relevant failure.

### Documentation

The repro must include a comment or docstring stating:
- Issue reference and one-line bug title
- Expected (correct) behavior
- Actual (buggy) behavior
- Failure criterion (verbatim from Phase 2)

## Phase 5: Shrink and validate

### Validate first

Run the repro (or trace through it mentally) and confirm:
1. The failure criterion from Phase 2 triggers
2. The failure is the *described* bug, not a test setup error
3. The failure message matches what Phase 2 specified

See `references/failure-classification.md` for how to classify test results and distinguish issue-relevant failures from setup errors.

### Then shrink

Use the subtraction method from `references/minimization.md`. Work through dimensions in order (cheapest first):
1. Reduce infrastructure scale (instances, partitions, replication)
2. Reduce data volume (items, size, batches)
3. Remove non-default configuration
4. Remove setup steps
5. Replace real sleeps with controlled time or polling
6. Simplify trigger sequence

See `references/shrinking-checklist.md` for the guardrail checklist.

### Over-minimization guardrail

After each shrinking step, re-check that the failure is still *the same failure* — same assertion, same exception type, same message fragment from Phase 2. If the failure changed, revert that step. This prevents shrinking into a different bug.

### For nondeterministic bugs

Wrap the trigger in an N-iteration harness with a retry budget. Report the observed failure rate (e.g., "fails 23/1000 iterations"). Do not use sleep-based synchronization — it makes tests flaky on different hardware. Prefer event-based scheduling, explicit synchronization primitives, or deterministic simulation.

Always print and record seeds for randomized tests so failing scenarios can be replayed.

## Output

Always produce:

1. **Repro file(s)** with `// TRIGGER`, `// HARNESS`, `// ORACLE` comments. Correct file placement matching the project's test layout.
2. **Status**: one of `REPRODUCED` / `CANDIDATE` / `BLOCKED`. See `references/output-contract.md` for what each status means and requires.
3. **Run command**: the exact command to execute the repro.
4. **Failure summary**: "This test should fail on the current code with: `<expected failure message>`"
5. **For flaky bugs**: failure rate and seed.
6. **For blocked repros**: what's missing, what was attempted, next best direction.

## Hard rules

- **Write the failure criterion before the repro code.** Without an explicit oracle, you cannot distinguish a successful repro from a coincidental failure. This is the single most common failure mode.

- **Do not hallucinate APIs.** If unsure whether a method, class, or flag exists, read the source or ask. Hallucinated APIs are the most frequent reason LLM-generated repros fail to compile.

- **Assert expected behavior, not current broken behavior.** The oracle encodes what *should* happen. The test fails because reality doesn't match.

- **Do not shrink past the bug.** After every shrinking step, re-verify the failure matches Phase 2. See `references/shrinking-checklist.md`.

- **Flaky repros must declare their flakiness.** A repro that reproduces 23/1000 times must say so and wrap itself in a retry loop.

- **Match the project's conventions.** Use the test framework, style, and helpers that already exist. Do not introduce a new framework unless the existing one cannot express the bug.

- **Do not fix the bug.** Create tests/scripts/harnesses only. Do not edit production code unless explicitly asked.

- **Do not use `:latest` image tags or unpinned dependency versions.** A repro is not reproducible if its dependencies drift.

- **Do not confuse setup errors with reproduced bugs.** An error in test setup is not the same as the error caused by the bug. See `references/failure-classification.md`.

## When to escalate to higher tiers

Escalate to Tier 5+ (containerized, cluster, deterministic simulation) when:

- The bug involves two or more processes (client + server, replica + leader)
- The bug requires a specific network condition (partition, delay, drop)
- The bug requires clock skew or node failure scenarios
- The bug is about isolation, linearizability, or consensus
- The user mentions Cassandra, Kafka, or any distributed system

For these cases read `references/distributed-systems.md` before drafting.

## When to use a property-based test

Use a property-based test when:

- The bug is about an invariant (round-trip, idempotence, commutativity, ordering, bounded sums)
- Shrinking comes for free in the property framework
- You want the test to serve as a regression test with seed persistence

See `references/property-based-testing.md`.

## When to use a fuzz harness

Use a fuzz harness when:

- The bug was originally found by fuzzing and a crashing input exists
- The input is a byte stream or structured binary format
- A single example fails but many nearby inputs are interesting for regression

See `references/fuzzing.md`.
