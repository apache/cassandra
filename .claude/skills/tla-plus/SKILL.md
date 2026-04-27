---
name: tla-plus
description: Create, run, and verify TLA+ and PlusCal formal specifications. Use for modeling distributed systems, protocols, concurrent algorithms, state machines. Can compose specs from code, find divergences between spec and implementation, spot concurrency bugs and invariant violations. Use when asked to "write a TLA+ spec", "model check", "verify protocol", "find race conditions", or "formal verification".
---

# TLA+ Formal Specification Skill

Write non-trivial TLA+ specifications, run the TLC model checker, and bridge the gap between formal specs and implementation code.

## Setup (Run Once)

```bash
bash SKILL_DIR/scripts/setup.sh
```

This downloads `tla2tools.jar` (v1.8.0) and verifies Java 11+. The JAR is stored at `SKILL_DIR/lib/tla2tools.jar`.

## Quick Reference

Load the right reference for your task:

| Task | Reference |
|------|-----------|
| TLA+ syntax, operators, types | [references/language.md](references/language.md) |
| PlusCal syntax, processes, labels | [references/pluscal.md](references/pluscal.md) |
| Distributed systems patterns | [references/patterns/distributed-systems.md](references/patterns/distributed-systems.md) |
| Protocol specifications | [references/patterns/protocols.md](references/patterns/protocols.md) |
| Invariants, safety, liveness | [references/patterns/invariants.md](references/patterns/invariants.md) |
| Code ↔ spec mapping, bug finding | [references/patterns/code-to-spec.md](references/patterns/code-to-spec.md) |

**Load references on-demand** — read the ones relevant to the current task before writing any spec.

## Templates

Start from a template when creating a new spec:

| Template | Use For |
|----------|---------|
| [templates/basic.tla](templates/basic.tla) | Pure TLA+ state machine |
| [templates/pluscal.tla](templates/pluscal.tla) | PlusCal algorithm with processes |
| [templates/distributed.tla](templates/distributed.tla) | Distributed system with messages |

## Running Specs

### Full Check (Recommended)

Translates PlusCal (if present), parses, and model-checks in one step:

```bash
bash SKILL_DIR/scripts/check.sh spec.tla --config spec.cfg
```

### Individual Tools

```bash
# Parse only (syntax check)
bash SKILL_DIR/scripts/parse.sh spec.tla

# Translate PlusCal to TLA+
bash SKILL_DIR/scripts/translate.sh spec.tla -nocfg

# Model check with TLC
bash SKILL_DIR/scripts/tlc.sh spec.tla --config spec.cfg --workers auto
bash SKILL_DIR/scripts/tlc.sh spec.tla --no-deadlock    # suppress deadlock check
```

### Direct Java Invocation

```bash
JAR="SKILL_DIR/lib/tla2tools.jar"

# Parse
java -cp "$JAR" tla2sany.SANY spec.tla

# Translate PlusCal
java -cp "$JAR" pcal.trans -nocfg spec.tla

# Model check
java -cp "$JAR" tlc2.TLC -workers auto -config spec.cfg spec.tla

# REPL
java -cp "$JAR" tlc2.REPL

# Dump state graph (for visualization)
java -cp "$JAR" tlc2.TLC -dump dot,actionlabels,colorize states.dot spec.tla
```

## Workflow: Writing a Spec

### 1. Understand the System

Before writing any TLA+:
- Identify the **concurrent agents** (processes, nodes, threads)
- Identify **shared mutable state** (queues, databases, locks, counters)
- Identify **the key safety property** ("what must never happen?")
- Identify **the key liveness property** ("what must eventually happen?")

### 2. Choose PlusCal vs Pure TLA+

**Use PlusCal when:**
- The system is naturally sequential/imperative
- You're modeling code (threads, goroutines, async tasks)
- You need `await`, `while`, `goto` semantics
- The audience is programmers

**Use Pure TLA+ when:**
- You need fine-grained fairness control
- The system is naturally a state machine
- You need interruptible/restartable actions
- You need refinement mappings
- A single label would need to update a variable twice

### 3. Start Small, Then Expand

1. Write the **simplest possible spec** that captures the core behavior
2. Add a **type invariant** immediately
3. Run TLC with small constants (2-3 nodes, 2-3 messages)
4. Add **safety invariants** one at a time, running after each
5. Add **liveness properties** with fairness
6. Increase constants to expand coverage

### 4. Config File

Every spec needs a `.cfg` file. Create one alongside the `.tla`:

```
SPECIFICATION Spec

\* Properties to check
INVARIANT TypeInvariant
INVARIANT Safety

\* Uncomment for liveness (slower)
\* PROPERTY Liveness

\* Constants
CONSTANT
  NumNodes = 3
  MaxMsgs = 5
  NULL = NULL

\* Uncomment to bound state space
\* CONSTRAINT StateConstraint

\* Uncomment to ignore deadlocks
\* CHECK_DEADLOCK FALSE
```

## Workflow: Spec From Code

When the user has existing code and wants a spec:

1. **Read the code** — understand concurrency structure, shared state, synchronization
2. **Read** [references/patterns/code-to-spec.md](references/patterns/code-to-spec.md)
3. **Map constructs** — threads→processes, locks→await, channels→queues
4. **Abstract** — don't model every line, only concurrency-relevant operations
5. **Write spec** — start with the concurrent skeleton, add detail iteratively
6. **Run TLC** — find deadlocks, invariant violations, liveness failures
7. **Report** — translate any TLC error traces back to code execution paths

## Workflow: Finding Bugs

When comparing a spec to code, or looking for bugs:

1. **Read** [references/patterns/code-to-spec.md](references/patterns/code-to-spec.md)
2. **Write a spec** modeling the suspected buggy area
3. **Run with small constants** — TLC is exhaustive, small state space is fine
4. **Interpret error traces** — map TLA+ state sequences to code execution paths
5. **Check atomicity** — does the code do atomically what the spec's label does?
6. **Check completeness** — does the code handle all `either/or` branches?
7. **Check ordering** — does the code enforce the same happens-before relations?

If TLC finds a violation:
- The error trace is a **concrete counterexample** — a specific sequence of events
- Map each state in the trace to concrete code state
- The transition that causes the violation is the bug location

## Interpreting TLC Output

### Success
```
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val: 0
```

### Invariant Violation
```
Error: Invariant SafetyInvariant is violated.
The following behavior constitutes a counter-example:
State 1: <Initial predicate>
  /\ var1 = ...
State 2: <Action line X>
  /\ var1 = ... (changed values in red)
```

### Deadlock
```
Error: Deadlock reached.
```
→ No process can make progress. Check `await` conditions and process completion.

### Liveness Violation
```
Error: Temporal properties were violated.
```
→ Check fairness settings. Add `WF_vars` or `SF_vars`. Trace may include a "stuttering" suffix.

## Key Principles

1. **Type invariants first** — catch spec bugs before checking real properties
2. **Small constants** — 3 nodes finds most bugs; 10 nodes takes 1000× longer
3. **Safety before liveness** — invariants are faster to check
4. **One property at a time** — add incrementally, run after each addition
5. **Fairness is opt-in** — TLA+ assumes everything can crash by default
6. **`=>` with `\A`, `/\` with `\E`** — never use `=>` with `\E`
7. **Sequences are 1-indexed** — always
8. **Every action must specify all variables** — use `UNCHANGED` for untouched ones
