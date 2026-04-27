# Logic & Types — Specialist Checklist

20 highest-signal questions. 52% of all bugs fall in this domain.

---

## Control Flow & Return Values

1. [ ] Does a non-void method call appear on its own statement line WITHOUT `return`? `foo();` where `return foo();` was intended discards the result and falls through.
   → Also: check every `if/else` branch in non-void methods — each must return, throw, or break.

2. [ ] Does this code use `else if` where independent `if` statements are needed? If two conditions can both be true, `else if` silently skips the second.
   → Also: `continue` in multi-concern loop bodies suppresses unrelated work the same way.

3. [ ] Does a `switch` have `throw`/`return` after the closing brace instead of inside a `default:` case? It executes unconditionally after any matched branch.

4. [ ] Does `orElse(sideEffect())` appear where `orElseGet(() -> sideEffect())` is needed? `orElse` evaluates eagerly regardless of Optional presence.

## Comparisons & Equality

5. [ ] Does this code use `==` to compare objects with value semantics — strings from config, metadata after copy, boxed Long/Integer outside -128..127?
   → Also: `Objects.hash(array)` uses identity hash; need `Arrays.hashCode`.

6. [ ] Does this code compare raw bytes for typed data instead of using the type's own `compare()` method? Reversed types, composites, and floats all have special comparison semantics.

7. [ ] Does a single field or variable represent two semantically distinct events? E.g., `lastTime` set at operation START but tested as if it records FINISH. Each event needs its own field.
   → Also: initial value (0 or `now`) causes wrong elapsed-time on first run.

## Constants & Defaults

8. [ ] Does a constant's unit match the expected unit? Does a `static final` capture a config value at class-load time, defeating runtime changes? Does a hardcoded literal after config parsing silently override file-based configuration?

9. [ ] When adding a new constant to a numbered enum or registry, does the numeric ID collide with an existing one? Scan ALL existing entries for duplicate IDs. Numbering gaps suggest deleted entries whose IDs must not be reused.

10. [ ] Does a disabled-feature path return a non-empty collection (`Arrays.asList(noOp)`, `singletonList(noOp)`) instead of `Collections.emptyList()`? Callers iterating the result execute spurious elements.

## Parameters & Variables

11. [ ] Does this method accept multiple parameters of the same type where swapping them compiles but produces wrong results? (`min`/`max`, `start`/`end`, `expected`/`actual`, `coordinator`/`replica`)

12. [ ] When code is moved within a diff, do all variable references still hold at the new location? A variable non-null inside `if (x != null)` becomes potentially null if code moves outside that block.
   → Also: imports or local variables visible at old location but shadowed or absent at new.

13. [ ] After extracting code into a helper method, does the call site retain an operation the helper also performs? (Double-write, double-close from extract-method refactoring.)

## Boolean & Predicate Errors

14. [ ] Does a boolean flag have inverted semantics relative to its name — `enabled` set when feature is off, `preload` true when preloading is skipped?

15. [ ] For a boolean guard whose name implies a compound reason (`bumpEpoch`, `hasChanged`, `needsRebalance`): does the assignment have fewer OR-conditions than the gated operation requires?
   → Also: compare against sibling guards in same method; count distinct actions the operation performs.

## Time & Units

16. [ ] Does this code mix time units — microseconds added to nanosecond clock, nanoseconds passed to API expecting milliseconds? Does a nanoTime deadline use addition that overflows near `Long.MAX_VALUE`?

## Iterators & Loops

17. [ ] Does this code advance an iterator with a single `if` where a `while` is needed — when chaining sub-iterators or skipping empty segments? Does a `pop()`/consume call process only one element when all should be consumed?
   → Also: recursive `computeNext()`/`hasNext()` overflows the stack on large inputs.

## Sentinel Values

18. [ ] Does a sentinel value for "absent/unknown" overlap with a legitimate data value? (Zero-length meaning both "not set" and "empty but valid"; `-1` used for both EOF and valid index.)
   → Also: every code path must check sentinels (-1, null, 0) BEFORE using the return in arithmetic.

## Wrapper & Stream Usage

19. [ ] After creating a wrapper stream (tee, counting, checked), does ALL subsequent I/O use the WRAPPER? A read/write bypassing the wrapper loses tracking, caching, or checksum guarantees.

20. [ ] Does a `serializedSize()` computation use a flag value or enum ordinal where `TypeSizes.sizeof(...)` is needed? Numeric coincidence breaks if the flag value ever changes.

---

## False Positives — Do NOT Flag

- Intentional no-op in documented disabled path returning a sentinel
- SLF4J `{}` with trivial arguments (`.toString()`, `.name()`)
- Package-private visibility on non-public API implementation classes
- Static final for JVM-lifetime settings (system properties, JMX port)
- Lazy init without null guard in single-threaded context (per-partition, per-request)
