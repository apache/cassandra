# Deep Logic & Types — Extended Checklist
---

## Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Class hierarchy**: What does this class extend? What interfaces does it implement?
2. **Sibling classes**: Are there parallel implementations that should mirror this code?
3. **Callers**: Who calls the methods being changed? How do they use the return values?
4. **Lifecycle**: When is this object created, used, and destroyed?
5. **Invariants**: What contracts does this class maintain? Read Javadoc, assertions, tests.

---

## Condition & Comparison

### Type-mismatch in equals/contains (weight: high)
-  For every `equals()` or `contains()` call: do BOTH sides have the same runtime type? Check for subtype vs supertype, wrapper vs unwrapped form, domain object vs its key.
-  For map lookups: does the key type match the map's key type exactly? A type mismatch causes a silent lookup miss.
-  Does `Arrays.asList()` wrap an already-List, creating `List<List<T>>`? `contains()` then compares wrong element type.

### else-if mutual exclusion (weight: medium)
-  For every `else if`: can BOTH conditions be true simultaneously? If yes, the second is silently skipped.
-  Does `continue` in a multi-concern loop body suppress unrelated work?

### Map lookup direction (weight: medium)
-  Is the map queried with value where key is expected (or vice versa)?

### Catch clause scope (weight: medium)
-  Is the catch clause too broad? `catch (Exception)` that swallows errors making retry loops infinite.
-  Is it too narrow, missing a related exception subtype?

### Conditional counting (weight: low)
-  Is a counter incremented per item in a collection that may contain duplicates?
-  Is a "remaining" counter passed as "completed" to a progress API expecting monotonically increasing values?

### Dead code / unused accumulation (weight: low)
-  Does a collector accumulate state but no code reads the result?
-  Does a predicate always return a hard-coded constant instead of computing from fields?

---

## Sentinel & Default Values

### Sentinel flows into arithmetic without guard (weight: high)
-  Does a sentinel value (`Long.MIN_VALUE`, `Integer.MAX_VALUE`, `-1`, `0`) flow into arithmetic or comparison without a guard?
-  Does `Map.getOrDefault` return a zero/empty default that is then treated as real data?
-  Does a "always-present" filter break callers who rely on genuine negative results?

### Hardcoded value where caller context should flow (weight: medium)
-  Is a hardcoded constant (timeout, mode, level) used where the caller's context should be forwarded?
-  Is a derived default computed before the field it depends on is set?

---

## Variable & Operand

### Wrong variable, field, or argument (weight: high)
-  Does comparison or operation use a field from outer scope instead of the loop variable?
-  Does code use source buffer instead of destination?
-  Does getter read derived state from child instead of authoritative field?
-  Does a constructor or factory argument end up in the wrong slot? Read the call as a sentence to verify each argument matches its role.

### Error messages reference wrong object (weight: high)
-  Format string references wrong variable or swaps argument order?
-  Error message names wrong flag/field or uses full object instead of `.name`?

### Logger class copy-pasted (weight: high)
-  Is `LoggerFactory.getLogger()` passing the correct enclosing class, not copy-pasted from another file?

### Constructor overload resolution (weight: high)
-  Does subclass delegate to wrong parent overload (parameter meanings swapped)?
-  Is a required parameter not forwarded after signature change?
-  Does overload resolution ambiguity from class hierarchy cause wrong binding?

### Copy-paste in parallel registrations (weight: medium)
-  When registering similar items in sequence: does one entry duplicate a neighbor's argument?
-  In adjacent metric registrations: are name/supplier pairs swapped?

### add() vs addAll() after collection type change (weight: low)
When a map value type changes from a single element to a `Collection`, callers still using `add(entry.getValue())` add the entire collection as one element.

### Boolean parameter conflates two orthogonal concerns (weight: low)
A boolean parameter is repurposed as a proxy for a different concern, so a caller needing different values for each has no valid representation.

---

## Iterator & Loop

### Sub-iterator advancement (weight: medium)
-  Does chained sub-iterator use `if` instead of `while` to skip empties?
-  Is a shared iterator consumed across multiple input items?

### Separator logic (weight: low)
-  Is "skip separator on first element" pattern applied when separator is actually a terminator?
-  Is the wrong iterator checked for `hasNext()` when two parallel iterators run?

### Mutable iterator in lambda (weight: low)
-  Is `.next()` called inside a lambda passed to `forEach`, advancing past unintended elements?

### Parallel data structure loop bounded by wrong size (weight: low)
When iterating two parallel arrays, using only one's length as the bound silently over- or under-processes the other.

### Loop guard evaluates outer-container field instead of loop variable (weight: low)
Inside a loop, a null-check or sentinel-guard references a field of the outer container, evaluating the same way every iteration. Common after rename where outer variable was renamed but inner references were missed.

---

## Refactoring Artifacts

### Stale identifier after rename (weight: high)
After a rename: have ALL references been updated — string literals, error messages, scripts, `Class.forName()` string-typed names?

### Merge artifacts (weight: high)
After merge/rebase: is the resolution semantically correct (not just syntactically)? Does merge leave wrong variable name, wrong import, wrong constant, duplicate declaration, inverted condition, or dead code from the other branch?

### Refactored guard moved to wrong scope (weight: medium)
Moving a guard during refactoring places it in a utility method that fires for all callers, not just the intended one.

### Helper extracted to utility loses caller-specific empty-input guard (weight: low)
When a utility is extracted from one caller and reused by others: does it still depend on a guard that only the original caller applied at its call site?

### Deprecated API drops parameters when delegating to new form (weight: low)
When a deprecated API path forwards to a shared builder, any option the old path accepted but does not forward causes a silent no-op.

---

## Inverted / Negated Conditions

### Inverted boolean guard (weight: high)
-  For EVERY boolean guard: read aloud as a sentence. Does the polarity match the intent?
-  Is `!` where there should be none? `<` instead of `>=`? `==` instead of `!=`?

### Short-circuit loop semantics inverted (weight: low)
Early-return on negative match with default true flips semantics, silently producing wrong results.

---

## Filtering & Selection

### Missing guard for empty input (weight: high)
-  What happens when the collection/iterator has zero elements?
-  Does empty credentials, config, or options cause a different (broken) code path?

### Result ordering driven by wrong source (weight: high)
-  Is ordering driven by an authoritative ordered list, not `Map.values()` or `Map.keySet()`?

### Missing or overly broad filter conflates enablement with fast-path (weight: medium)
A single boolean guard used for both "should this subsystem activate" and "should the inner fast path short-circuit".

### Scope or namespace not included in cache key or identifier resolution (weight: medium)
A cache is keyed on a local name without its owning namespace, allowing collisions across namespaces.

### Wrong collection queried for lookup (weight: medium)
Code looks up a value in the wrong map (e.g., primary-key set instead of regular-attribute map).

### Assertion used where defensive check needed for legitimate runtime state (weight: medium)
An `assert` guards against a state that legitimately arises at runtime, causing crashes in production.

### Mutable shared object passed to multiple consumers without copy (weight: medium)
A stateful object with mutable counters/cursors/flags is constructed once and passed to multiple independent consumers in a loop.

### Transformation / pipeline stage ordering error (weight: medium)
In a chained iterator pipeline, stages are applied in the wrong order so a dependent stage misses events from an extension it requires.

### Aggregate returns wrong result on empty input (weight: low)
An aggregate function fails to return a row on empty input, violating expected semantics.

### Filter predicate applied to one branch of union but not both (weight: low)
A canonical view is built by merging two collections with a filtering step on only one.

### Boolean predicate named for one context misused in another (weight: low)
A predicate named for context A is used in context B where its boolean sense inverts.

### Mutable object from cache modified in-place, affecting all holders (weight: low)
A mutable object retrieved from a cache or factory is modified in-place by one consumer, affecting all other consumers sharing the same reference.

---

## Type Dispatch & Enum Coverage

### Incomplete switch / enum dispatch — missing case (weight: medium)
A switch statement or if-chain over an enum does not cover a newly added or uncommon value, causing fallthrough or skipping the needed action.

### Wrong type returned by fallback/default in type-dispatch (weight: medium)
A switch on type ends with a hardcoded fallback type that is wrong for the uncommon case.

### Non-exhaustive switch lacks meaningful default (weight: low)
A `switch` on an enum without exhaustive cases or a meaningful `default` crashes or falls through silently.

---

## Arithmetic & Units

### Unit mismatch in time / size arithmetic (weight: high)
A duration, timestamp, or size is computed by mixing incompatible units (nanoseconds added to milliseconds, int packed into long without widening) without explicit conversion.

### Unit-conversion factor applied in wrong direction (weight: medium)
-  Does the multiply/divide direction match the source→target units? `bytes * 1024` instead of `bytes / 1024` produces a result 2^10 too high.

### Progress / ratio uses mismatched numerator and denominator (weight: medium)
-  Does progress report values in different units for numerator vs denominator?
-  Are `bytesRead` / `totalBytes` swapped, producing inverted completion percentage?

### Bits-vs-bytes unit label or conversion factor wrong (weight: medium)
Throughput display or throttle confuses bits and bytes, or the unit label says "MB/s" when the value is in "Mb/s".

### Comparator / ordering logic incorrect (weight: high)
A custom comparator is asymmetric, uses subtraction instead of `Integer.compare()`, handles sentinel values inconsistently, or does not cover all three cases.

### Lambda comparator argument order reversed (weight: low)
Comparators intended for descending order that use `(o1, o2)` instead of `(o2, o1)` silently sort in the wrong direction.

### Capacity check uses element count instead of byte count (weight: low)
An upper-bound check expressed in count units rather than byte units allows the structure to exceed its byte-capacity limit.

### Conditional decrement chain miscounts across branches (weight: low)
Computing an index by starting from a base and applying conditional decrements across branching logic applies the wrong number of adjustments.

### Elapsed time computed as `now - (now - timestamp)` = timestamp (weight: low)
The expression simplifies to `timestamp` itself (not elapsed duration), causing duration comparisons to produce nonsensical results.

### Two different size metrics conflated (weight: low)
A value returned as "bytes read from file" is used as "bytes sent over network" — the two differ for compressed or encoded content.

### float/double loses precision before BigDecimal construction (weight: low)
Widening a `float` to `double` or passing through floating-point as an intermediate step introduces spurious digits or truncates large values.

### Timezone offset derived from current time instead of timestamp being converted (weight: low)
A helper derives a timezone offset from `now` instead of the timestamp being converted.

---

## Control Flow & Dispatch

### Two code paths for same operation diverge (weight: low)
When parallel code paths exist (NIO vs non-NIO, streaming vs local), one path omits a required step that the other performs.

### Scheduling loop misses re-arm on one branch (weight: low)
A periodic task's scheduling loop misses re-arming the next event on the "work done" branch, causing the task to stop running.

### Inner worker void return hides failure from dispatch loop (weight: low)
A `void` helper inside a dispatch loop cannot communicate failure back to the loop controller, so the loop continues past failures.

### Unconditional resource acquisition before mode/config check (weight: low)
A connection or resource is acquired unconditionally before a branch that checks whether it is needed, failing on environments where the unused mode is disabled.

### Mutable state overwritten between evaluation sites (weight: low)
An object carries mutable evaluation state and is called from multiple sites; a later call's state overwrites the earlier one.

### Feature removal leaves leftover conditional references (weight: low)
After removing a feature, leftover references to the removed concept in conditional guards invert or short-circuit important logic.

---

## Logging & Diagnostics

### Eager evaluation in disabled log/debug path (weight: medium)
A log statement's arguments include method calls (collection formatting, type serialization, `String.format`) that are evaluated even when the log level is disabled.

### SLF4J `{}` vs printf `%s`/`%d` mismatch (weight: medium)
A logger call uses printf placeholders instead of SLF4J `{}`, or has more tokens than arguments.

### Tool exits 0 on failure / error swallowed silently (weight: low)
A CLI tool logs an error but returns exit code 0, or I/O errors are logged and swallowed instead of propagated.

### Typo or misspelling in user-facing string (weight: low)
An error message, log line, or CLI output contains a misspelled word, duplicate word, or wrong technical term.

### Documentation or help text not updated for new/renamed feature (weight: medium)
When a feature is added, renamed, or removed, its documentation — help strings, CLI usage text, example commands — is not updated.

---

## Performance (detectable in review)

### Hot-path re-derivation (weight: low)
A method called in an inner loop delegates to an expensive operation on every invocation instead of caching.

### Unnecessary auto-boxing (weight: low)
A generic container parameterized with boxed types auto-boxes on every comparison in a tight loop.

### O(n) in hot path (weight: low)
A method called in a hot path iterates a collection where a reverse lookup map would give O(1).

### Scheduling at wrong granularity (weight: low)
A recurring task that should run once globally is scheduled per-item, creating N copies. Or a throttle fires on logical units instead of at the byte-transfer layer.

---

## Structural Patterns from Multi-Project Corpus

### Assertion guards only one variant of several that legitimately reach the site (weight: medium)
-  For every `assert` on state / subtype: enumerate every valid value that legitimately reaches this call site.
-  Alternative modes, concurrent-not-found, zero-output paths, and edge-mode subclasses all fire spurious `AssertionError` when only one variant was considered.

### ByteBuffer position-advancing read without duplicate (weight: medium)
-  For every relative `get()` / `put()` / `read()` on a shared or caller-supplied buffer: is it preceded by `.duplicate()` / `.slice()`?
-  Position advances silently corrupt subsequent reads; singleton/cached buffers are permanently exhausted.
-  Is `rewind()` used where `flip()` was intended? `rewind()` leaves limit at capacity, exposing uninitialized tail bytes.

### Reversed-iteration bound inclusion flipped (weight: medium)
-  For every bound-inclusion check on a reversed-iteration path: is the LOGICAL bound reversed to match iteration direction? Exclusivity applied to the wrong endpoint silently returns wrong rows.
-  For direction-sensitive skip guards in a reverse context: is the comparator's sense flipped? Guards fire when they shouldn't, re-emitting already-seen items on subsequent pages.

### CAS / retry loop reuses already-mutated source (weight: medium)
-  Does a CAS retry pass the SOURCE object to a mutating merge? First iteration mutates it; subsequent retries operate on altered data.
-  Does a distribution loop iterate a shared collection without removing consumed entries, so every consumer receives the full set instead of a disjoint subset?

### Unit-conversion factor in wrong direction (weight: medium)
-  Does the multiply/divide direction match the source→target units? `bytes * 1024` instead of `bytes / 1024` produces a result 2^10 too high.

### Progress ratio uses mismatched numerator and denominator (weight: medium)
-  Does a ratio sample numerator and denominator at different moments, allowing negative or >100% aggregate values?

### In-flight counter incremented without matching decrement on error path (weight: medium)
-  Is the counter incremented before work is submitted, and does the rejection / cancel / exception path decrement?
-  Does a metric counter stay permanently divergent from actual state after a failed async write?

### Equal-bounds degenerate case skipped in range predicate (weight: medium)
-  When `min == max == value`, can a range predicate prove exclusion or match? Many "might match" defaults never prune the provably-disjoint single-value case.

### Derived cache not refreshed after configuration reload (weight: low)
-  Is a derived cache populated at startup but never invalidated when its source configuration is reloaded?
-  Is a setup-time configuration resolved before the per-target entity is known, freezing settings at global defaults?

### Inline field initializer survives constructor / setter (weight: low)
-  When a field has a hardcoded inline initializer and the constructor is supposed to overwrite it, does the assignment actually fire on all code paths?
-  Does a copy constructor propagate a time-sensitive field (expiry, timestamp) unchanged when the new object represents a distinct lifetime?

### Unconditional defensive-wrap chain grows on each rebuild (weight: low)
-  Does a constructor/rebuild path wrap its input in an unmodifiable / defensive wrapper without first checking whether the input is already wrapped?
-  Repeated rebuilds build a linearly-growing delegation chain that overflows the stack on iteration.

### Version / threshold string compared lexicographically (weight: low)
-  For every version or threshold comparison: is it numeric-aware? String compare silently admits `"9" > "11"`.

### Mutable data structure returns construction-time constant instead of current state (weight: low)
A method that should reflect current state instead returns a construction-time constant.

### Subclass identity not tested in type-check method (weight: low)
When a subclass represents a semantically distinct variant, type-check methods on the parent that ignore the subclass identity misclassify instances.

### Base set used after augmented set was constructed (weight: low)
Code builds an "all" set by augmenting a base set, then subsequent operations use the base set instead of the augmented one.

### Reference equality used instead of value equality (weight: medium)
Code uses `==` to compare Strings or other value objects that may not be the same instance. After any refactoring that changes object lifecycle, identity checks break silently.

### Two-pass algorithm uses different reference time per pass (weight: low)
When a two-pass algorithm calls `currentTimeMillis()` independently in each pass, time-sensitive decisions can differ between passes.

### Cross-constraint satisfiability not checked at definition time (weight: low)
Compound declarative predicates are validated individually but not cross-checked for contradictions or redundancies.

### Method has multiple paths but only some fulfill a shared postcondition (weight: low)
When all branches of a method must meter or track a resource but only some branches contain the tracking call, the others silently skip it.

### Paired min/max config fields but only one forwarded (weight: low)
When a class has parallel min/max config fields, forwarding only one silently drops the other setting.

### Method return value ignored by caller assuming in-place mutation (weight: low)
When a method both mutates an argument and returns a value, callers that ignore the return value silently lose data.

### Direct field access in compareTo/equals breaks subclasses (weight: low)
A comparison implementation that reads final fields directly instead of through accessor methods cannot be overridden.

### Async future wrapping conflates cancellation with execution failure (weight: low)
An async future wrapper converts both `CancellationException` and execution failures into the same exception type.

### Deduplication / visited-set populated from wrong source (weight: low)
When merging two data sources with deduplication, the "already-seen" set is populated from the wrong source.
