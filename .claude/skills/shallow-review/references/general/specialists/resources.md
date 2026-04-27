# Resources & Serialization — Specialist Checklist

18 highest-signal questions. 15% of all bugs fall in this domain.

---

## Serialization

1. [ ] Do `serialize`, `deserialize`, and `serializedSize` write, read, and measure fields in exactly the same order under the same conditional guards?
   → Also: CRC/checksum must cover the same fields; version-gated branches must have matching size arithmetic.

2. [ ] Does `serializedSize()` return a hardcoded byte count? Verify it matches the actual type: 4 for int/float/LocalDate, 8 for long/double. If the represented type changed during refactoring, was the size constant updated?

3. [ ] Does a `serializedSize()` computation use a flag value or semantic constant where `TypeSizes.sizeof(...)` is needed? Numeric coincidence breaks if the flag value changes.

4. [ ] After wrapping a stream for monitoring/caching/tee (e.g., `TeeDataInputPlus`, `CountingOutputStream`), does ALL subsequent I/O use the WRAPPER, not the original? Bypass loses tracking/checksum.

## Resource Leaks

5. [ ] Does this code allocate an `AutoCloseable` resource without try-with-resources? Trace each exception exit point — is the resource closed on every path?
   → Also: multiple resources acquired in sequence need chaining; first leaks if second allocation fails.

6. [ ] When a `ByteBuffer` is obtained via `nioBuffer()`, `slice()`, `duplicate()` — is the ORIGINAL allocation returned to the pool, not the derived view? Pools cannot recycle views.

7. [ ] Does a shared `AutoCloseable` resource (stored in cache, map, or registry) use reference counting? Without it, the first consumer to `close()` invalidates all others.
   → Also: `close()` triggered from cache eviction while other threads hold references.

8. [ ] Does `close()` have an idempotency guard? Side-effecting callbacks (metrics, counters, listeners) fire twice on double-close.

## Metric Types

9. [ ] Does a latency metric use `Histogram` where `Timer` is correct? `Histogram` records dimensionless values; `Timer` records durations with time-unit support. Check consistency within the same class.

## Background Tasks & Lifecycle

10. [ ] Does a `static` initializer or `static final` field start a background scheduled task unconditionally? It fires during unit tests causing flakiness. Check for a disable/shutdown mechanism.

11. [ ] Does a `static final` capture a config value at class-load time that should support runtime reconfiguration? Check if the config has a setter, listener, or reload path.

## Test & Simulation Determinism

12. [ ] Does this code call a factory/parser without optional determinism parameters (randomizers, clocks, RNG seeds) needed for fuzz/simulation reproducibility?
    → Also: `Math.random()`, `new Random()`, `ThreadLocalRandom` in simulation-reachable paths break replay.

## Retry & Idempotency

13. [ ] Does this code path handle retry or replay? If so, does it exclude non-idempotent operations (counters, CAS) that produce wrong results when applied more than once?

## Overrides

14. [ ] Does a subclass adding new fields override every copy-producing method (`sharedCopy()`, `clone()`, copy constructor)? Does every method intended to override carry `@Override`?
    → Also: after superclass signature change, the old override silently becomes dead code without `@Override`.

## File I/O

15. [ ] Does this code write with `WRITE | CREATE` without `TRUNCATE_EXISTING` on a pre-existing file? Old content past new EOF remains as garbage.

## Version Compatibility

16. [ ] When a class is moved between modules, do the new serialize/deserialize methods handle ALL version-gated fields from the original? Compare field-by-field under each version guard.

## Configuration

17. [ ] Does a constructor accept a parameter that is never assigned to a field, silently discarding config?

18. [ ] Does a feature flag guard the main operation but not setup, teardown, or secondary paths?

---

## False Positives — Do NOT Flag

- Single-use try-with-resources for one AutoCloseable (sufficient for simple cases)
- Static final for JVM-lifetime settings (system properties, JVM flags)
- Histogram for count-based distributions (partition sizes, key counts) — only flag for TIME
- Missing randomizer in non-simulation production code paths
