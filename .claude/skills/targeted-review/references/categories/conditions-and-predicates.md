# Category: Conditions and Predicates

Bugs where a comparison, guard, predicate, or branching condition is logically wrong: wrong operator polarity, wrong constant or field, wrong direction flag, asymmetric vs symmetric comparator confusion, missing replay-time filter, conflated conditions, or a sentinel guard that tests the wrong variable.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- New or modified comparison operators (`<`, `<=`, `>`, `>=`, `==`, `!=`) in a guard, loop condition, or boundary test
- A boolean expression that gates a side effect, branch, early return, or filter step
- An `equals()`, `hashCode()`, `compareTo()`, or `Comparator` implementation (added, modified, or its field set changed)
- A predicate method (`isXxx`, `hasXxx`, `canXxx`, `matches`, `contains`) added or modified
- A `switch` over an enum or type discriminator (especially with `default` branches or fall-through)
- A direction/order flag (ascending/descending, forward/reverse, oldest/newest) passed to a helper
- An error message or log statement that quotes a constant, threshold, or field name
- A composite key access where indexed positions or named components are read
- A `filter`, `where`, `predicate`, or `Predicate<T>` lambda added, removed, or re-targeted
- A type check via `instanceof`, `getClass() ==`, or a custom type-discriminator predicate
- Changes to membership tests (`contains`, `containsKey`, `Set.of`, allowlist/denylist)
- Sentinel value handling (`-1`, `null`, `Long.MAX_VALUE`, `EMPTY_BUFFER`) in a comparison or guard

## Findings

### F-01: Wrong polarity comparison in availability/threshold check
A comparison uses `!=` or strict-vs-non-strict in the wrong direction (e.g. `!=` instead of `<`, `>` instead of `>=`), so a value satisfying the intended predicate triggers the failure branch and vice versa. Common in replica-count, quorum, capacity, and threshold checks.
**Look for:** `if (count != required)` where intent is "fewer than required"; `if (size > limit)` after append where intent is "would exceed"; tombstone/cleanup `<=` vs `<` at boundary.

### F-02: Asymmetric vs symmetric comparator confusion in binary search
A symmetric comparator is used to binary-search a collection of intervals (or vice versa) with a point key, leaving equality cases ambiguous so the search lands on the wrong side of a boundary.
**Look for:** `Collections.binarySearch` or `Arrays.binarySearch` over interval/range structures; comparator used both for sort order and for skip/seek operations.

### F-03: Comparison reads wrong field of composite key/object
A containment, equality, or eligibility check accesses the wrong indexed component (or wrong named field) of a composite key or struct, producing incorrect routing decisions with no runtime error.
**Look for:** `key.get(i)` / `tuple._2` / `components[N]` inside an `equals`/`compareTo`/`contains` body; index constant (`0`, `1`) used in field-position lookups.

### F-04: Wrong direction/ordering flag passed to helper
A boolean direction flag (forward/reverse, ascending/descending, oldest-first/newest-first) is passed with the wrong literal to an ordered-read or scan helper, causing diffs to be applied in reverse order or pages to scan the wrong end.
**Look for:** `boolean reversed`, `boolean ascending`, `Direction.FORWARD/REVERSE` arguments at call sites; reversed-scan code paths that don't swap start/end bounds.

### F-05: Error or log message quotes a similarly-named but wrong constant
Diagnostic output references one constant while the preceding condition enforces a different (similarly-named) one, misleading operators about what threshold actually fired.
**Look for:** `String.format(... %s ..., CONSTANT_A)` immediately following `if (x > CONSTANT_B)`; error messages that hardcode enum names copied from sibling branches.

### F-06: Inverted boolean condition (`!flag` vs `flag`)
A negation is added or removed by mistake, causing the guarded branch to fire under the opposite circumstances. Often hidden in `isEmpty()`/`!isEmpty()`, `isEnabled()`/`!isEnabled()`, error-vs-success classifiers, and `var x = a ? b : c` ternaries with branches swapped.
**Look for:** `if (!isEmpty(...))` flips, ternary with single-letter variable in test, recently changed boolean predicate methods that return `!expr`.

### F-07: Conflated AND/OR in compound guard
A skip-guard combines two independent safety conditions with `AND` instead of `OR` (or vice versa), permitting inputs that satisfy only one condition to bypass; or two orthogonal conditions are conflated into a single boolean.
**Look for:** `if (a && b)` short-circuiting in skip/abort logic; recently modified compound conditions where one side reads as defense-in-depth.

### F-08: Missing replay-time / state-restore filter
A filter that removes superseded or already-applied entries is skipped on the startup/replay path while applied on the live write path, leaving stale entries visible as current state after restart.
**Look for:** Replay/recovery loops in `start()`, `loadFromDisk()`, `replay()` paths; filters present in writer but absent in reader.

### F-09: Sentinel guard tests the wrong variable
A null/sentinel/disabled-state check is wired to an outer-scope or unrelated variable, so the guard never fires for the variable it is meant to protect, allowing the sentinel value to flow downstream.
**Look for:** Guard like `if (other != null)` immediately followed by `if (... self ...)` field access; loop-sentinel check on outer-iteration var while body uses inner-loop var.

### F-10: equals/hashCode omits a semantically-significant field
After a new field is added to a class, `equals()` and/or `hashCode()` are not updated, so logically distinct instances collide in caches/sets, change-detection misses updates, and notifications fail to fire.
**Look for:** Added field that does not appear in nearby `equals`/`hashCode` methods; record-style classes with manual `equals` overrides.

### F-11: Predicate too broad — accepts inputs it should reject
A type or eligibility check uses a broader supertype/wildcard than necessary, accepting inputs that share a class hierarchy but not the contract the caller assumes; or a switch's `default` swallows cases that should be explicit.
**Look for:** `instanceof BaseType` where a specific subtype was intended; `default:` branch that should be unreachable; check on count instead of membership.

### F-12: Predicate too narrow — rejects valid inputs
A predicate or filter excludes a category it should accept (a new enum variant, a wrapper type, a previously-unhandled subtype), silently dropping or misrouting valid input.
**Look for:** Hand-maintained type/enum dispatches recently extended in one place but not another; allowlist/denylist sets missing newly-added entries.

### F-13: Type-identity check ignores decorator/wrapper
`instanceof X` or `getClass() == X.class` is applied to a wrapped value without first unwrapping the decorator (e.g. reversed-order, frozen-collection wrappers), so the predicate returns false for wrapped instances of X.
**Look for:** `instanceof` on a column type, expression type, or AST node where decorator wrappers exist; missing `.unwrap()`/`getBaseType()` call before the test.

### F-14: Recursive/inherited predicate's base case is wrong
A type-hierarchy predicate returns the wrong default at the base class and is overridden only on leaf types, so container/decorator/wrapper types inherit the base-class default and are never recognized.
**Look for:** Boolean methods named `isComplex`, `containsX`, `referencesY` defined on base type with `return false` and only sometimes overridden.

### F-15: Symmetric ordering applied to asymmetric / circular type
A range or comparator operation assumes total ordering, but the underlying type's order is partial, circular, or NaN-bearing; sorting "for convenience" or using min/max on such values produces wrong/empty results.
**Look for:** `Math.min`/`Math.max` on raw token / wrap-around values; comparators on collections containing `NaN`, `null`, or sentinel boundary subtypes; sort applied before a query against circular-token ranges.

### F-16: Comparator ignores per-column reversed sort direction
A range-bounds check or paging cursor uses an operator without flipping it for `DESC`-ordered columns, producing inverted results for queries against reversed-order data.
**Look for:** Hardcoded `>`/`<` in range filters that touch column metadata; missing `reverseIfNeeded()`/operator flip when iterating over `ClusteringComparator` with `isReversed()` columns.

### F-17: equals() compares against `this` or wrong object reference
An `equals()` bridge or delegated comparator passes `this` instead of the cast argument, or compares a field to itself, so the method always returns true for distinct instances or always returns false for equal ones.
**Look for:** `bridge.equals(this, other)` patterns where one argument should be the casted parameter; `field == field` rather than `field == other.field`.

### F-18: Comparator reads buffer with side-effecting (positional) form
A comparator or equality method uses the relative `getInt()`/`get()` form on a shared `ByteBuffer` without first duplicating it, advancing the buffer position as a side effect and corrupting subsequent reads.
**Look for:** `buffer.getInt()` / `buffer.get()` inside a `Comparator` or `equals` method without a preceding `.duplicate()` or `.slice()`.

### F-19: Boundary check at exact-equal / degenerate-range case
A range non-overlap check, intersection skip-guard, or "might-match" predicate uses strict `<` where `<=` is needed (or vice versa), missing the exact-equal endpoint case and either issuing the same value twice or failing to make forward progress.
**Look for:** Iterator skip guards `current >= target` instead of `current > target`; range non-overlap tests that never trigger when ranges share an endpoint.

### F-20: Empty-buffer / empty-collection treated as null or special value
A check uses `buffer.equals(EMPTY)` or `field == null` interchangeably with emptiness, but the type uses zero-length encoding to mean a real zero value (e.g. integer zero), so legitimate values are silently dropped from the index/result set.
**Look for:** `buffer.remaining() == 0`, `value == EMPTY_BYTE_BUFFER` checks in indexing/serialization paths; dual interpretation of null vs empty.

### F-21: Membership/lookup uses different representation than insertion
A `Map`/`Set` is built using one string/key representation (e.g. address without port, lowercased name) and queried with another, so every lookup misses and the guarded counter, status, or filter is silently bypassed.
**Look for:** Two distinct address-formatting/normalization functions used at insert vs lookup; case-insensitive comparison on one side only; `String` keys built with different formatters.

### F-22: Filter/exclusion never matches due to hidden suffix or normalization
An exact-name filter checks bare logical names, but on-disk or runtime names carry a suffix (UUID, version, generation, separator), so the check never matches and the filter is silently bypassed.
**Look for:** `set.contains(rawName)` / `name.equals(constant)` against directory or table names; missing `startsWith` + delimiter combination for prefix checks.

### F-23: Switch over enum is missing a case (silent fall-through)
A switch statement omits a case for a newly-added enum constant, falling through to a `default:` that throws, returns null, or silently does nothing — the missing case becomes a runtime failure or silent drop.
**Look for:** `switch (someEnum) { ... default: ... }` without an exhaustive set of `case` labels; recently-added enum constants near switch dispatches.

### F-24: Branches swapped in if/else or ternary
The if-branch and else-branch are exchanged, so live data is routed through the deletion handler (or vice versa), or a null-handling ternary dereferences the null operand.
**Look for:** `cond ? maybeNull.field : default` (NPE risk), `if (isDeleted) writeLive() else writeTombstone()` ordering inversions during merges.

### F-25: Membership-by-iteration in a hot path with O(n) cost
A membership test iterates an entire live collection instead of using an indexed lookup, producing quadratic complexity in startup or hot loops; or a count comparison is used where membership semantics are required.
**Look for:** `for (entry : map) if (entry.equals(x))` patterns inside frequent loops; comparison of `list.size()` against expected count rather than `list.containsAll(...)`.

### F-26: Unconditional "always true / always false" validator
A validation method returns true for all inputs (stub left in) or false at base class with sparse overrides, so invalid inputs pass silently or valid inputs are uniformly rejected.
**Look for:** Methods that just `return true;` / `return false;` whose name implies a real check; predicate returning constant under one configuration.

### F-27: Compatibility-direction check inverted
A "is X compatible with Y" check passes the arguments in the wrong order (asks if old can read new instead of new can read old, or whether the candidate can reach the existing instead of the reverse).
**Look for:** `isCompatible(a, b)` / `canRead(a, b)` calls on schema, type, or version objects where direction matters; type-compatibility checks during migration paths.

### F-28: Lexicographic vs numeric comparison
A version, count, or numeric value stored as a string is compared lexicographically (so `"10" < "9"`), causing version dispatch to misclassify two-digit versions as older or newer than intended.
**Look for:** Shell `[ "$x" \> "$y" ]` or `String.compareTo` against version strings or numeric tokens; sort over stringified IDs.

### F-29: Sentinel value participates in arithmetic without unwrapping
A "disabled" or "uninitialized" sentinel (e.g. `-1`, `Long.MAX_VALUE`) flows into comparisons or arithmetic that don't recognize it, producing nonsensical thresholds, immediate timeouts, or infinite loops.
**Look for:** Subtractions involving fields documented as sentinel-bearing; `Math.min(timeout, ...)` without filtering out the disabled marker; counter initialized to `-1` compared with `<` against a positive count.

### F-30: Identity-equality on freshly-constructed singleton
An identity check (`==`) is used on values from a factory that does not return a canonical singleton for the special case (e.g. empty buffer), so logically-identical instances compare unequal and the special path is missed.
**Look for:** `value == EMPTY_INSTANCE` / `instance == SENTINEL` checks where the sentinel is constructed via a factory that may not intern; `value.equals(EMPTY)` would be safer.

### F-31: Local-DC vs remote-DC filter polarity flipped
A "from local DC" / "from outside local DC" filter is implemented with the wrong sign, so in a single-DC cluster every response is discarded (or the wrong subset is counted) and operations time out or reach quorum on wrong votes.
**Look for:** `if (replica.getDatacenter().equals(local))` skip vs include conditions; consistency-level filters that exclude based on locality.

### F-32: Guard fires after the side effect it was meant to gate
An irreversible side effect (file creation, increment, allocation) is performed before the guard that decides whether to do it, so revoking the guard later cannot undo the effect and may permanently break subsequent operations.
**Look for:** `createHardLink(); if (!shouldCreate) ...` ordering inversions; metric increments before the guard check; mutations followed by validation that "fixes" by throwing.

### F-33: Compound predicate validated in isolation; cross-predicate satisfiability not checked
Multiple sibling predicates on the same field (e.g. `x > 5 AND x < 3`) are accepted at definition time because each is individually well-formed, allowing contradictory combinations to be persisted and later produce unexpected empty results or runtime errors.
**Look for:** Constraint/policy registration that validates entries one-at-a-time; missing `Range.intersect(...)`/satisfiability checks between sibling predicates.

### F-34: Stale or coincidental field used as control flag
A control flag is recomputed from a value comparison that coincidentally matches the default, or an enum field is reused with two distinct meanings, so callers in the second meaning silently take the first meaning's path.
**Look for:** Boolean fields whose initialization comment says "for X" but read site uses for Y; double-purpose timestamp/flag fields; flags that conflate "started" with "in progress".

### F-35: Exclusion set lookup uses path-equality but exclusions differ in prefix
An exclusion list contains objects with path-based equality, but the items being checked differ only by directory prefix (or vice versa), so the lookup always misses despite representing the same logical entity.
**Look for:** `excluded.contains(file)` where `file` was constructed from a different root than the entries; canonical-vs-relative path mixing.

### F-36: Inclusion-predicate polarity mirrored incorrectly between paired methods
Two complementary methods (include/exclude, accept/reject, isAlive/isDead) have inverted polarities on one path, so one method accepts what the other would reject — producing inconsistent results across read/write or upgrade boundaries.
**Look for:** Pairs of methods like `acceptsX`/`rejectsX`, `shouldKeep`/`shouldDrop`, recently changed in only one location.

### F-37: Equality / membership uses `Objects.hash()` on byte arrays
`Objects.hash(byteArray)` invokes identity-based array hashing while `equals` does a content comparison (or vice versa), violating the equals/hashCode contract and producing wrong results in hash-based collections.
**Look for:** `Objects.hash(byteArrayField)` rather than `Arrays.hashCode(byteArrayField)`; arrays passed to varargs hash methods.

### F-38: `equals()` only checks for non-null instead of correct type
An `equals` short-circuits on `other != null` rather than `other instanceof MyClass`, returning true for any non-null object of any type, producing false-positive equality across unrelated classes.
**Look for:** `if (other == null) return false; ... return field.equals(other.field);` without an `instanceof` check.
