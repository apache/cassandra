# Logic & Types — Specialist Checklist

30 highest-signal questions. 52% of all bugs fall in this domain.

---

## Control Flow & Return Values

1. [ ] Does a non-void method call appear on its own statement line WITHOUT `return`? `foo();` where `return foo();` was intended discards the result and falls through.
   → Also: check every `if/else` branch in non-void methods — each must return, throw, or break.

2. [ ] Does this code use `else if` where independent `if` statements are needed? If two conditions can both be true, `else if` silently skips the second.
   → Also: `continue` in multi-concern loop bodies suppresses unrelated work the same way.

3. [ ] Does a `switch` have `throw`/`return` after the closing brace instead of inside a `default:` case? It executes unconditionally after any matched branch.

4. [ ] Does `orElse(sideEffect())` appear where `orElseGet(() -> sideEffect())` is needed? `orElse` evaluates eagerly regardless of Optional presence.

5. [ ] Does a helper or initializer throw a generic `RuntimeException` / fire `assert` for a state that legitimately arises (snapshot files, counter verbs, static rows, concurrent not-found, zero-output compactions, alternative mode subclass)? Callers treat spurious assertion failures as hard errors.

## Comparisons & Equality

6. [ ] Does this code use `==` to compare objects with value semantics — strings from config, metadata after copy, boxed Long/Integer outside -128..127?
   → Also: `Objects.hash(array)` uses identity hash; need `Arrays.hashCode`.

7. [ ] Does this code compare raw bytes for typed data instead of using the type's own `compare()` method? Reversed types, composites, and floats all have special comparison semantics.

8. [ ] Does a single field or variable represent two semantically distinct events? E.g., `lastTime` set at operation START but tested as if it records FINISH. Each event needs its own field.
   → Also: initial value (0 or `now`) causes wrong elapsed-time on first run.

9. [ ] Does a range/overlap predicate skip the equal-bounds degenerate case (`min == max`) or treat a bound-inclusion as absolute when iteration is reverse-ordered? `left-inclusive` semantics flip under `ReversedType` / descending scans; slice exclusivity then applies to the wrong endpoint.
   → Also: version/threshold strings compared lexicographically report `"9" > "11"`.

## Constants & Defaults

10. [ ] Does a constant's unit match the expected unit? Does a `static final` capture a config value at class-load time, defeating runtime changes? Does a hardcoded literal after config parsing silently override file-based configuration?

11. [ ] When adding a new constant to a numbered enum or registry, does the numeric ID collide with an existing one? Scan ALL existing entries for duplicate IDs. Numbering gaps suggest deleted entries whose IDs must not be reused.

12. [ ] Does a disabled-feature path return a non-empty collection (`Arrays.asList(noOp)`, `singletonList(noOp)`) instead of `Collections.emptyList()`? Callers iterating the result execute spurious elements.

13. [ ] Does a unit-conversion expression multiply/divide in the WRONG direction (`bytes * 1024` for bytes→KB, `fraction * 100` where the caller expects a fraction)? Does a constructor silently ignore its own argument because the field is hardcoded at inline initializer and never reassigned?

## Parameters & Variables

14. [ ] Does this method accept multiple parameters of the same type where swapping them compiles but produces wrong results? (`min`/`max`, `start`/`end`, `expected`/`actual`, `coordinator`/`replica`)

15. [ ] When code is moved within a diff, do all variable references still hold at the new location? A variable non-null inside `if (x != null)` becomes potentially null if code moves outside that block.
   → Also: imports or local variables visible at old location but shadowed or absent at new.

16. [ ] After extracting code into a helper method, does the call site retain an operation the helper also performs? (Double-write, double-close from extract-method refactoring.)
   → Also: does the new helper miss an empty-input / not-null guard that only the original caller applied locally?

17. [ ] Does a retry / distribution / CAS loop pass a SOURCE object directly to a mutating helper, so later iterations operate on already-mutated data? Does a two-phase constructor compute a derived value (serialized size, routing target) BEFORE the second phase populates its inputs?

## Boolean & Predicate Errors

18. [ ] Does a boolean flag have inverted semantics relative to its name — `enabled` set when feature is off, `preload` true when preloading is skipped?

19. [ ] For a boolean guard whose name implies a compound reason (`bumpEpoch`, `hasChanged`, `needsRebalance`): does the assignment have fewer OR-conditions than the gated operation requires?
   → Also: compare against sibling guards in same method; count distinct actions the operation performs.

20. [ ] Does a predicate in a recursive type / wrapper / decorator hierarchy return `false` at the base class and get overridden only on leaf types? Container and decorator types then inherit the `false` default and are never recognized as containing the relevant sub-type.

## Time & Units

21. [ ] Does this code mix time units — microseconds added to nanosecond clock, nanoseconds passed to API expecting milliseconds? Does a nanoTime deadline use addition that overflows near `Long.MAX_VALUE`?

22. [ ] Does a progress / ratio display swap numerator and denominator, sample them at different moments, or mix compressed-bytes against uncompressed-total? Reported percentages become inverted, impossible, or wildly skewed.

## Iterators & Loops

23. [ ] Does this code advance an iterator with a single `if` where a `while` is needed — when chaining sub-iterators or skipping empty segments? Does a `pop()`/consume call process only one element when all should be consumed?
   → Also: recursive `computeNext()`/`hasNext()` overflows the stack on large inputs.

24. [ ] Inside a loop body, does a null-check / sentinel-guard expression reference a field of the OUTER container (or enclosing scope) instead of the freshly-fetched LOOP variable? The outer field evaluates the same every iteration; the per-element check never fires. Common after rename refactors.

25. [ ] Does a constructor / rebuild path wrap its input in an unmodifiable / defensive / lazy-union wrapper without first checking whether it is already wrapped? Repeated rebuilds build a linearly-growing delegation chain that overflows the stack on traversal.

## Sentinel Values

26. [ ] Does a sentinel value for "absent/unknown" overlap with a legitimate data value? (Zero-length meaning both "not set" and "empty but valid"; `-1` used for both EOF and valid index.)
   → Also: every code path must check sentinels (-1, null, 0) BEFORE using the return in arithmetic.

27. [ ] Is a counter/refcount/in-flight metric incremented on entry path with NO matching decrement on the cancel / reject / error / RejectedExecutionException path? In-flight counters silently grow unbounded when submissions fail.

## Wrapper & Stream Usage

28. [ ] After creating a wrapper stream (tee, counting, checked), does ALL subsequent I/O use the WRAPPER? A read/write bypassing the wrapper loses tracking, caching, or checksum guarantees.

29. [ ] Does a `serializedSize()` computation use a flag value or enum ordinal where `TypeSizes.sizeof(...)` is needed? Numeric coincidence breaks if the flag value ever changes.

30. [ ] Is a derived cache / topology / endpoint map populated at registration or construction time but never invalidated / refreshed when its source configuration is reloaded or the schema is altered?

---

## False Positives — Do NOT Flag

- Intentional no-op in documented disabled path returning a sentinel
- SLF4J `{}` with trivial arguments (`.toString()`, `.name()`)
- Package-private visibility on non-public API implementation classes
- Static final for JVM-lifetime settings (system properties, server ports)
- Lazy init without null guard in single-threaded context (per-partition, per-request)
