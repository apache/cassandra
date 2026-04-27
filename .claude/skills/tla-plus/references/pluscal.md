# PlusCal Reference

PlusCal is a higher-level language that compiles to TLA+. It provides imperative-style syntax while retaining full TLA+ expressiveness for operators and invariants.

## Algorithm Structure

```tla
---- MODULE MySpec ----
EXTENDS Integers, Sequences, TLC

\* Constants and operators can go here

(* --algorithm algorithm_name
variables
  x = 0;
  y \in 1..5;   \* nondeterministic: TLC checks ALL starting values

define
  \* Operators that can reference PlusCal variables
  TypeInvariant == x \in Nat /\ y \in 1..5
end define;

begin
  Label1:
    x := x + 1;
  Label2:
    y := y - 1;
end algorithm; *)

\* BEGIN TRANSLATION  (auto-generated TLA+)
\* END TRANSLATION

\* Post-translation operators (can reference pc, stack, etc.)
====
```

## Variables

```tla
variables
  counter = 0;                       \* fixed initial value
  seq = <<1, 2, 3>>;                 \* sequence
  record = [a |-> 1, b |-> 2];      \* structure
  x \in 1..10;                       \* nondeterministic: all values tested
  items \in SUBSET {"a", "b", "c"};  \* all subsets tested
```

Using `\in` creates **multiple starting states**. TLC checks every combination.

## Labels

Labels define the **grain of atomicity**. Everything in one label happens in a single step.

### Rules
1. Algorithm body must start with a label
2. `while` loops must start with a label
3. Each variable can only be updated **once per label** (use `||` for simultaneous assignment)
4. `goto` must be followed by a label
5. `macro` and `with` blocks cannot contain labels
6. If any branch in `if`/`either` has a label, the end of the block must be followed by a label

### Simultaneous Assignment

```tla
Label:
  seq[1] := seq[1] + 1 ||
  seq[2] := seq[2] - 1;
```

## Control Flow

### if-elsif-else

```tla
  if condition then
    x := 1;
  elsif other_condition then
    x := 2;
  else
    x := 3;
  end if;
```

### while

```tla
  Loop:                      \* REQUIRED label before while
    while condition do
      \* body (nonatomic — each iteration is a separate step)
    end while;
```

### either-or (nondeterministic choice)

```tla
  either
    x := 1;
  or
    x := 2;
  or
    x := 3;
  end either;
```

### with (temporary bindings and nondeterminism)

```tla
  \* Deterministic
  with tmp = x do
    y := tmp + 1;
  end with;

  \* Nondeterministic — picks any element from set
  with val \in {1, 2, 3} do
    x := val;
  end with;
```

### goto, skip, assert, print

```tla
  A:
    goto C;    \* must be followed by a label
  B:
    skip;      \* noop
  C:
    assert x > 0;    \* fails model check if false (needs EXTENDS TLC)
    print x;          \* debug output
```

### await (blocking)

```tla
  WaitForLock:
    await lock = NULL;     \* label only enabled when condition is TRUE
    lock := self;
```

If no process can proceed → **deadlock** (reported as error by TLC).

## Macros

Simple textual substitution. Must be defined before `begin`. Cannot contain labels.

```tla
macro send(queue, msg) begin
  queue := Append(queue, msg);
end macro;

macro recv(queue, var) begin
  await queue # <<>>;
  var := Head(queue);
  queue := Tail(queue);
end macro;
```

## Procedures

Like macros but can contain labels. Use `call` to invoke, `return` to exit.

```tla
procedure enqueue(item)
begin
  Enqueue:
    queue := Append(queue, item);
    return;
end procedure;

\* In a process:
  call enqueue("hello");
```

## Processes

### Single Process

```tla
process worker = 1
begin
  Work:
    \* ...
end process;
```

### Process Set

```tla
process worker \in 1..NumWorkers
variables local_var = 0;    \* each process gets its own copy
begin
  Work:
    local_var := self;       \* self = process value (1, 2, ... NumWorkers)
end process;
```

### Rules
- All processes must have **comparable types** (all ints, all strings, or all model values)
- Different processes **cannot share label names**
- `self` keyword returns the process's identifier value

### Fairness

```tla
fair process worker \in Workers     \* weak fairness — guaranteed to not stop forever
fair+ process worker \in Workers    \* strong fairness — runs if repeatedly enabled

\* Per-label strong fairness:
  CriticalStep:+
    x := x + 1;
```

## The `pc` Variable

The translator creates `pc` to track the current label:
- Single process: `pc` is a string (`"LabelName"` or `"Done"`)
- Multiple processes: `pc` is a function (`pc[self]`)

Common pattern for post-completion invariants:
```tla
IsCorrect == pc = "Done" => result = ExpectedValue

\* With multiple processes:
AllDone == \A p \in Procs: pc[p] = "Done"
Correct == AllDone => result = Expected
```

## Translation

PlusCal algorithms must be translated to TLA+ before model checking:

```bash
java -cp tla2tools.jar pcal.trans -nocfg spec.tla
```

The translation appears between `\* BEGIN TRANSLATION` and `\* END TRANSLATION` markers in the same file. Do not edit translation output by hand.

## Define Block

Operators in `define` can reference PlusCal variables. They appear after `variables` and before `begin`:

```tla
define
  TypeInvariant ==
    /\ counter \in 0..MaxVal
    /\ lock \in Processes \union {NULL}

  IsUnique(s) ==
    \A i, j \in 1..Len(s):
      i # j => s[i] # s[j]
end define;
```
