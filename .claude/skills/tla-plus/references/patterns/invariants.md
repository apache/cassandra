# Invariants and Properties Guide

## Types of Properties

| Type | Checks | Example |
|------|--------|---------|
| **Invariant** | Every state individually | `counter >= 0` |
| **Action property** | Every transition | `counter' >= counter` |
| **Safety (temporal)** | No bad behavior sequence | `\E s \in S: [](s \in online)` |
| **Liveness** | Good things eventually happen | `<>(status = "done")` |

## Type Invariants

The first invariant to write. Constrains each variable to its valid domain.

```tla
TypeInvariant ==
  /\ counter \in 0..MaxVal
  /\ lock \in Processes \union {NULL}
  /\ queue \in Seq(MessageType)
  /\ state \in [Nodes -> {"idle", "active", "done"}]
  /\ seen \subseteq AllItems
  /\ pc \in [Processes -> {"Init", "Work", "Done"}]
```

**Rules**:
- Be as tight as possible. `counter \in 0..MaxVal` beats `counter \in Int`.
- Include `pc` if you need label-dependent reasoning.
- Type invariants catch spec bugs early — always write them first.

## Safety Invariants

Assert that dangerous states never occur.

### Mutual Exclusion
```tla
MutualExclusion ==
  \A p1, p2 \in Processes:
    (in_cs[p1] /\ in_cs[p2]) => p1 = p2

\* Equivalent with cardinality:
MutualExclusion ==
  Cardinality({p \in Processes : in_cs[p]}) <= 1
```

### No Overdraft
```tla
NoOverdraft ==
  \A a \in Accounts: balance[a] >= 0
```

### Conservation (Closed System)
```tla
\* Total money in system never changes
Conservation ==
  LET total == SumAll(balance)
  IN  total = InitialTotal
```

### Consistency Across Replicas
```tla
\* If a value is committed, all replicas agree
Consistency ==
  \A k \in Keys:
    committed[k] =>
      \A r1, r2 \in Replicas:
        data[r1][k] = data[r2][k]
```

## Conditional Invariants with `=>`

Use implication to restrict when an invariant applies.

```tla
\* Only check after algorithm completes
Correct == pc = "Done" => result = ExpectedResult

\* Only check under certain conditions
QueueBound == is_running => Len(queue) <= MaxQueueSize

\* Process-specific
WorkerCorrect ==
  \A w \in Workers:
    pc[w] = "Done" => output[w] = f(input[w])
```

## Action Properties

Check transitions, not individual states. Use `[][...]_vars` syntax.

```tla
\* Monotonically increasing counter
CounterMonotonic == [][counter' >= counter]_counter

\* Lock ownership can't jump between processes
LockNotStolen ==
  [][lock \in Processes => (lock' = lock \/ lock' = NULL)]_lock

\* Version numbers only increase
VersionMonotonic ==
  [][\A n \in Nodes: version[n]' >= version[n]]_version
```

**Remember**: `[][A]_v` means `[](A \/ UNCHANGED v)` — stuttering is always allowed.

## Liveness Properties

Assert that the system makes progress.

### Eventually Done
```tla
\* All processes eventually finish
Termination == <>(\A p \in Processes: pc[p] = "Done")
```

### Eventually Consistent
```tla
EventualConsistency ==
  <>(\A r1, r2 \in Replicas: data[r1] = data[r2])
```

### Leads-to
```tla
\* Every request eventually gets a response
RequestHandled ==
  \A r \in Requests:
    r \in pending ~> r \in completed

\* Every enqueued item eventually dequeued
QueueDrained ==
  \A item \in Items:
    item \in Range(queue) ~> item \notin Range(queue)
```

### Starvation Freedom
```tla
\* Every process eventually enters critical section
NoStarvation ==
  \A p \in Processes:
    pc[p] = "Waiting" ~> pc[p] = "InCS"
```

## Composing Temporal Operators

| Pattern | Meaning | Use Case |
|---------|---------|----------|
| `<>[]P` | P becomes permanently true | Convergence, termination |
| `[]<>P` | P keeps happening | Repeated service, heartbeats |
| `[]<>P /\ []<>Q` | Both keep happening | Fair scheduling |
| `P ~> Q` | P always leads to Q | Request-response guarantee |
| `P ~> <>[]Q` | P leads to permanent Q | Recovery guarantee |

## Fairness Requirements

Liveness properties almost always need fairness to avoid trivial stuttering counterexamples.

```tla
\* Weak fairness: if action is continuously enabled, it eventually executes
Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* Per-action fairness
Spec == Init /\ [][Next]_vars
  /\ \A p \in Processes: WF_vars(Process(p))

\* Strong fairness for lock-waiting actions
Spec == Init /\ [][Next]_vars
  /\ \A p \in Processes: SF_vars(AcquireLock(p))
  /\ \A p \in Processes: WF_vars(ReleaseLock(p))
```

**When to use which**:
- **WF**: action is continuously enabled (unconditional progress)
- **SF**: action is repeatedly enabled/disabled (e.g., waiting for a lock)

## Debugging Properties

### Print and Assert
```tla
\* In PlusCal (needs EXTENDS TLC)
assert x > 0;
print <<"x =", x, "y =", y>>;
```

### Canary Invariants
```tla
\* Force a trace to see what states exist
DEBUG_SeenAllStates == Cardinality(explored) < MaxStates

\* Force a trace that reaches a specific state
DEBUG_NeverReaches == ~(state = "target_state")
```

### ALIAS in Config
```tla
\* In .cfg file: ALIAS DebugAlias
\* In .tla file:
DebugAlias == [
  state |-> state,
  state_next |-> state',
  queue_len |-> Len(queue),
  msg_count |-> Cardinality(msgs)
]
```

## Common Mistakes

1. **Using `=>` with `\E`**: `\E x: P(x) => Q(x)` is almost certainly wrong. Use `\E x: P(x) /\ Q(x)`.
2. **Forgetting fairness**: Liveness properties fail trivially without fair processes.
3. **Invariant too strong**: Checking `result = expected` globally when it only holds at end — use `pc = "Done" => ...`.
4. **Missing `UNCHANGED`**: In pure TLA+, every action must specify all variables.
5. **Quantifier over indices**: `\A i, j \in 1..Len(s): s[i] # s[j]` fails when `i = j`. Use `i # j =>`.
