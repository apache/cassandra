# Code-to-Spec and Spec-to-Code Patterns

## When to Write a Spec From Code

1. **Concurrent code**: Locks, channels, async workflows, thread pools
2. **Distributed systems**: Consensus, replication, failover
3. **State machines**: Protocol handlers, workflow engines, parsers
4. **Complex invariants**: Financial calculations, resource allocation
5. **Bug reproduction**: Reproduce race conditions or deadlocks

## Code → Spec Translation Strategy

### Step 1: Identify the Abstraction Level

Don't model every line. Model the **concurrency-relevant** operations:
- Shared state mutations
- Message sends/receives
- Lock acquisitions/releases
- State transitions
- Decision points

### Step 2: Map Code Constructs to TLA+

| Code Construct | TLA+ Equivalent |
|----------------|-----------------|
| Thread / goroutine / async task | `process` |
| Mutex lock | `await lock = NULL; lock := self` |
| Channel send (buffered) | `await Len(chan) < cap; chan := Append(chan, msg)` |
| Channel send (unbuffered) | `procedure` with two-phase handshake |
| Condition variable / await | `await predicate` |
| Atomic CAS | Single label with conditional update |
| try-finally / defer | Explicit label for cleanup |
| Loop | `while` |
| Map/Dict | `function [key \in Keys |-> val]` |
| Enum / tagged union | Set of strings + discriminator field |
| Null / None / nil | `NULL` constant (model value) |
| Thread-local variable | Process-local `variables` |
| Global shared state | Module-level `variables` |

### Step 3: Write the Spec

```tla
\* Example: Go worker pool with bounded channel
---- MODULE WorkerPool ----
EXTENDS Integers, Sequences, TLC

CONSTANTS NumWorkers, NumJobs, ChanSize, NULL

Workers == 1..NumWorkers
Jobs == 1..NumJobs

(*--algorithm worker_pool
variables
  jobChan = <<>>;
  results = [j \in Jobs |-> NULL];
  submitted = 0;
  completed = 0;

define
  TypeInvariant ==
    /\ Len(jobChan) <= ChanSize
    /\ submitted <= NumJobs
    /\ completed <= submitted
  
  AllJobsDone == completed = NumJobs
  Correct == AllJobsDone => \A j \in Jobs: results[j] # NULL
end define;

process producer = 0
variables nextJob = 1;
begin
  Submit:
    while nextJob <= NumJobs do
      await Len(jobChan) < ChanSize;
      jobChan := Append(jobChan, nextJob);
      submitted := submitted + 1;
      nextJob := nextJob + 1;
    end while;
end process;

fair process worker \in Workers
variables job = NULL;
begin
  Work:
    while completed < NumJobs do
      Dequeue:
        await jobChan # <<>>;
        job := Head(jobChan);
        jobChan := Tail(jobChan);
      Process:
        results[job] := job * 10;    \* abstract computation
        completed := completed + 1;
    end while;
end process;
end algorithm; *)
====
```

## Spec → Code Verification

### Finding Differences Between Spec and Code

1. **List all state variables** in the spec. Map each to code variables/fields.
2. **List all actions** in the spec. Map each to code functions/methods.
3. **Check atomicity**: Does the code perform what one TLA+ label does in a single atomic step?
4. **Check ordering**: Does the code enforce the same ordering constraints?
5. **Check completeness**: Does the code handle all `either/or` branches?

### Common Divergence Points

| Spec Says | Code Does | Bug? |
|-----------|-----------|------|
| Atomic label | Multiple statements without lock | Race condition |
| `await condition` | Polling without check | Missed wakeup |
| `either A or B` | Only implements A | Missing error handling |
| `\E x \in S` | Picks first/hardcoded | Correctness depends on choice |
| Set of messages | FIFO queue | May hide ordering bugs |
| Bounded variable | Unbounded counter | Overflow |
| `UNCHANGED rest` | Modifies extra state | Side effect bug |
| Fair process | Thread can starve | Liveness violation |

### Systematic Code Review Against Spec

```markdown
## Verification Checklist

### State Mapping
- [ ] Variable X in spec → field Y in code
- [ ] Type constraints match (spec range ⊆ code type range)

### Action Mapping
- [ ] Action A in spec → method M in code
  - [ ] Precondition (await/guard) correctly checked
  - [ ] State update matches
  - [ ] Atomicity level matches
  - [ ] All UNCHANGED variables truly unchanged

### Invariant Mapping
- [ ] TypeInvariant → type system + assertions
- [ ] SafetyInvariant → defensive checks / tests
- [ ] Liveness → timeout / retry / monitoring

### Edge Cases
- [ ] Empty collections handled (spec: `\A x \in {}: TRUE`)
- [ ] Concurrent access protected
- [ ] Error/exception paths modeled
```

## Spotting Bugs in Code Using a Spec

### Step 1: Write a Minimal Spec
Focus on the suspected buggy area. You don't need to model the entire system.

### Step 2: Run with Small Constants
```
CONSTANT
  NumNodes = 3
  MaxMsgs = 5
  NULL = NULL
```

### Step 3: Look for These Symptoms

| TLC Says | Likely Code Bug |
|----------|----------------|
| Deadlock | Missing wakeup, lock ordering issue |
| Invariant violated | Logic error, missing check |
| Liveness violated | Starvation, missing fairness, live-lock |
| State space explosion | Model too detailed — abstract more |
| Assert failed | Impossible state reached — logic error |

### Step 4: Translate Error Trace to Code Path

TLC gives you a step-by-step state sequence. Walk through your code following the same sequence of events to reproduce the bug.

## Modeling Existing Protocols

When verifying an implementation of a known protocol (Raft, Paxos, 2PC, etc.):

1. Start from the paper's pseudocode or description
2. Model one step at a time, checking after each addition
3. Use the **same variable names** as the paper when possible
4. Write invariants from the paper's correctness claims
5. Run with small constants first, increase gradually

### Bounding the Model

Real systems have unbounded state. TLA+ models must be finite.

```tla
CONSTANTS
  MaxTerm = 3        \* bound election terms
  MaxLogLen = 4      \* bound log length
  MaxMsgs = 10       \* bound messages in flight

\* Use as state constraint in .cfg:
\* CONSTRAINT StateConstraint
StateConstraint ==
  /\ \A n \in Nodes: term[n] <= MaxTerm
  /\ \A n \in Nodes: Len(log[n]) <= MaxLogLen
  /\ Cardinality(msgs) <= MaxMsgs
```
