# TLA+ Language Reference

## Module Structure

```tla
---- MODULE ModuleName ----
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS Const1, Const2
VARIABLES var1, var2

\* Operator definitions
\* ...

====
```

The module name MUST match the filename (without `.tla`).

## Types and Values

### Primitives
- **Integers**: `0`, `1`, `-5`. Requires `EXTENDS Integers` for arithmetic.
- **Strings**: `"hello"`. Only `=` and `#` work on strings. Used as opaque identifiers.
- **Booleans**: `TRUE`, `FALSE`.
- **Model values**: Uninterpreted constants, defined in cfg file.

### Complex Types
- **Sets**: `{1, 2, 3}`, `{}`. Unordered, unique elements. Same-type elements only.
- **Sequences (Tuples)**: `<<1, 2, 3>>`, `<<>>`. Ordered, 1-indexed.
- **Structures (Records)**: `[name |-> "Alice", age |-> 30]`. Access: `s.name` or `s["name"]`.
- **Functions**: `[x \in 1..10 |-> x * x]`. Access: `f[3]`.

## Operators

```tla
\* No-argument operator (constant)
MaxSize == 10

\* Parameterized operator
Max(a, b) == IF a > b THEN a ELSE b

\* LET for local definitions
ThreeMax(a, b, c) ==
  LET M(x, y) == IF x > y THEN x ELSE y
  IN  M(M(a, b), c)
```

## Boolean Logic

| Logic | Symbol | Math |
|-------|--------|------|
| and   | `/\`   | ∧    |
| or    | `\/`   | ∨    |
| not   | `~`    | ¬    |
| implies | `=>` | ⇒    |
| iff   | `<=>`  | ⇔    |

**Bullet-point notation** — whitespace-sensitive vertical layout:
```tla
/\ A
/\ \/ B
   \/ C
/\ D
\* Means: A /\ (B \/ C) /\ D
```

## Set Operations

| Operation | Syntax |
|-----------|--------|
| Membership | `x \in S` |
| Not member | `x \notin S` |
| Subset-or-equal | `S \subseteq T` |
| Union | `S \union T` |
| Intersection | `S \intersect T` |
| Set difference | `S \ T` |
| Cardinality | `Cardinality(S)` (needs `FiniteSets`) |
| Power set | `SUBSET S` |
| Cartesian product | `S \X T` |
| Integer range | `a..b` |
| All booleans | `BOOLEAN` |
| Map | `{f(x) : x \in S}` |
| Filter | `{x \in S : P(x)}` |

## Sequence Operations (EXTENDS Sequences)

| Operation | Syntax |
|-----------|--------|
| Append | `Append(s, e)` |
| Concat | `s1 \o s2` |
| Head | `Head(s)` |
| Tail | `Tail(s)` (all but first) |
| Length | `Len(s)` |
| Subsequence | `SubSeq(s, from, to)` |
| Index | `s[i]` (1-indexed!) |

Helper: `Range(s) == {s[i] : i \in 1..Len(s)}`

## Function Operations

```tla
\* Function definition
f == [x \in 1..10 |-> x * x]

\* Function set (all functions from S to T)
[S -> T]

\* EXCEPT for updating functions
f' = [f EXCEPT ![key] = newval]
f' = [f EXCEPT ![k1] = v1, ![k2] = v2]
f' = [f EXCEPT ![k] = @ + 1]   \* @ = old value
```

## Structures (Records)

```tla
\* Create
msg == [type |-> "request", from |-> "A", data |-> 42]

\* Access
msg.type        \* "request"
msg["type"]     \* "request"

\* Set of all possible records
MsgType == [type: {"request", "response"}, from: Servers, data: Nat]

\* Update with EXCEPT
msg' = [msg EXCEPT !.data = 99]
```

## Quantifiers

```tla
\* For all
\A x \in S: P(x)

\* There exists
\E x \in S: P(x)

\* Multiple variables
\A x \in S, y \in T: P(x, y)

\* CHOOSE — deterministic selection
CHOOSE x \in S: P(x)
```

**Critical rules**:
- `\A x \in {}: P(x)` is always TRUE
- `\E x \in {}: P(x)` is always FALSE
- Use `=>` with `\A`, use `/\` with `\E` (never `=>` with `\E`)

## IF-THEN-ELSE and CASE

```tla
IF cond THEN expr1 ELSE expr2

CASE x = 1 -> "one"
  [] x = 2 -> "two"
  [] OTHER -> "many"
```

## Temporal Operators

| Operator | Name | Meaning |
|----------|------|---------|
| `[]P` | Always (box) | P is true in every state |
| `<>P` | Eventually (diamond) | P is true in at least one state |
| `<>[]P` | Eventually always | P becomes permanently true |
| `[]<>P` | Infinitely often | P is true infinitely often |
| `P ~> Q` | Leads-to | If P is true, Q is eventually true |

## Actions (Primed Variables)

```tla
\* x' is the value of x in the NEXT state
Next == x' = x + 1

\* UNCHANGED — variable stays the same
UNCHANGED x
UNCHANGED <<x, y, z>>

\* Box action formula
[][A]_v  ==  [](A \/ UNCHANGED v)

\* Angle action formula
<<A>>_v  ==  A /\ (v # v')
```

## Spec Structure (Pure TLA+)

```tla
vars == <<x, y, z>>
Init == x = 0 /\ y = 0 /\ z = 0
Next == Action1 \/ Action2 \/ Action3
Spec == Init /\ [][Next]_vars

\* With fairness
Spec == Init /\ [][Next]_vars /\ WF_vars(Next)
```

## Fairness

```tla
WF_v(A)   \* Weak fairness: if A is eventually always enabled, it eventually happens
SF_v(A)   \* Strong fairness: if A is always eventually enabled, it eventually happens
ENABLED A \* True if action A can happen in current state
```

## Standard Modules

| Module | Provides |
|--------|----------|
| `Integers` | `+`, `-`, `*`, `\div`, `%`, `..`, `Int`, `Nat` |
| `Naturals` | Same as Integers but only naturals |
| `Sequences` | `Append`, `Head`, `Tail`, `Len`, `SubSeq`, `\o`, `Seq(S)` |
| `FiniteSets` | `Cardinality`, `IsFiniteSet` |
| `TLC` | `Print`, `Assert`, `:>`, `@@` (function constructors) |
| `Bags` | Bag operations |

## TLC-specific Operators (EXTENDS TLC)

```tla
\* Function constructor: key :> value
f == "a" :> 1 @@ "b" :> 2   \* Record-like function

\* Print (debugging)
Print(expr, val)  \* prints expr, evaluates to val

\* Assert
Assert(cond, msg)
```

## Config File (.cfg)

```
SPECIFICATION Spec

INVARIANT TypeInvariant
INVARIANT SafetyInvariant

PROPERTY Liveness

CONSTANT
  NumServers = 3
  NULL = NULL

CHECK_DEADLOCK FALSE
```
