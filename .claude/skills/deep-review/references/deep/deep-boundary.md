# Deep Boundary & I/O — Extended Checklist
---

## Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Data ranges**: What are the valid ranges for indices, sizes, positions in this code?
2. **Sentinel values**: What does -1, 0, null, MAX_VALUE mean in this context?
3. **Buffer lifecycle**: Who allocates, positions, slices, and releases buffers?
4. **I/O contracts**: What does the underlying read/write API guarantee? Partial reads?
5. **Arithmetic units**: What units are sizes, timestamps, and positions in?

---

## Off-by-One & Range Depth

### Time unit at API boundary (weight: high)
-  Does a timestamp cross an API boundary in the wrong unit?
-  Is a time value displayed or returned in a unit that differs from what the caller expects?
-  Is the wrong `TimeUnit` enum argument passed?

### Boundary comparison (weight: high)
-  State the boundary condition in English. Does the operator (`<` vs `<=`) match?
-  Is an inclusive bound treated as exclusive (or vice versa)?
-  Is the equality case at the boundary missed?
-  Does a chunk-boundary alignment assertion fire for legitimate partial chunks?

### Integer overflow (weight: high)
-  Does multiplying `int` config by 1024 or 1024*1024 overflow without long promotion?
-  Does a `Comparator` use integer subtraction (overflow risk)?
-  Does a narrowing cast on large values (file positions, byte counts) silently truncate?

### Comparator bugs (weight: medium)
-  Does the comparator use `*.compare()` methods (not subtraction)?
-  Does it handle equal elements correctly?
-  Does it satisfy the transitivity contract?
-  Are operands consistent in ordering across comparisons?

### Recursive iterator overflow (weight: medium)
-  Does `computeNext()`, `hasNext()`, or a helper call itself recursively?
-  Will it overflow the stack on large inputs?

### Recursive wrapping chain causes stack overflow (weight: low)
A defensive wrapper (e.g., `unmodifiableCollection`) re-applied on every rebuild without checking if the input is already wrapped. Chain depth grows with invocation count and overflows the stack during traversal.

### MB/GB to bytes overflow (weight: medium)
-  When converting MB/GB to bytes: is at least one operand `long` before multiplication?
-  Does the intermediate product overflow at 2 GB?

### Loop stop condition (weight: medium)
-  Does the loop use `> limit` instead of `>= limit` (or vice versa)?
-  Does it process one too many or one too few items?

### Buffer offset adjustment (weight: medium)
-  Does `buffer.array()` account for `position()` and `arrayOffset()`?
-  After `slice()`, is the non-zero offset accounted for?

### Binary search post-adjustment off-by-one (weight: low)
After binary search in a sorted range structure, adjusting the returned index by +1 for an exact match is wrong when the invariant requires the element to stay at the matched position.

### Binary search return value not adjusted for non-zero fromIndex base (weight: low)
When `binarySearch` accepts a non-zero `fromIndex`, a custom implementation may return a relative index while the caller expects absolute.

### Binary search with transposed forward / reverse sublist ranges (weight: low)
When a binary search is split into forward and reversed halves, are the correct sublist ranges passed to each direction? Transposed ranges silently skip valid matches.

### Int intermediate in file-position arithmetic overflows (weight: low)
When an array or buffer offset is computed from a multi-gigabyte file position, an early `(int)` cast silently truncates.

### For-each loop with manual counter incremented at body start is off-by-one (weight: low)
A manual counter incremented at the top of a for-each loop body is always one ahead of the current element when used as an array index.

### Compressed-section boundary at exact chunk multiple includes extra chunk (weight: low)
An endpoint that falls exactly on a chunk boundary does not require the next chunk; failing to check this sends one extra chunk.

### Accumulated counter with constant increment exceeds valid input domain (weight: low)
The accumulated value can drift past the boundary of the valid input range.

### Loop intended to check one beyond interval uses wrong comparator (weight: low)
A scan loop meant to "check one extra entry beyond the interval" silently skips that entry when the boundary uses `<` instead of `<=`.

---

## Null & Bounds Check Depth

### Map / registry lookup null (weight: high)
-  Is the result of a map or registry lookup null-checked before use?
-  Can a background task race with a concurrent removal of the entry it just looked up?
-  Do paths that accept user-supplied keys null-guard the lookup result?

### Collection/array bounds (weight: high)
-  `list.get(0)` without `isEmpty()`?
-  `array[idx]` without length check?
-  `Optional.get()` without `isPresent()`?
-  Parallel-list length mismatch?

### indexOf/substring without -1 check (weight: high)
-  Is the return value of `indexOf` / `lastIndexOf` used as an offset into `substring(...)` without a `!= -1` guard?
-  Does `split(...)` return a single element when the delimiter is absent, and does the caller assume a multi-element result?

### Nullable lifecycle fields (weight: high)
-  Is a field initialized in `start()`/`setup()` rather than constructor?
-  Is it dereferenced during shutdown or startup race?
-  Is it accessed from a wrong construction path?

### Infinite loops (weight: high)
-  Does a `while(true)` or `while(!done)` have an explicit escape path?
-  Is there a timeout, maximum-retry count, or interrupt check?
-  Can the condition EVER become true given possible upstream stalls?

### Missing instanceof before cast (weight: high)
-  Is every cast preceded by a matching `instanceof` guard?
-  When a new implementation is added, does it pass existing type checks?

### Empty collection edge cases (weight: high)
-  What happens with zero items? Does the code guard against empty inputs?
-  Does empty credentials, config, or options cause a different (broken) code path?

### Empty or impossible range not detected before execution (weight: low)
A logically empty range (e.g., start > end, or bounds that can never overlap) is not detected before execution, causing wasted work or incorrect results.

### Nullable subtype-specific field accessed without subtype check (weight: low)
A field populated only for certain subtypes is accessed without checking the subtype, causing NPE.

### Null-accumulator first-encounter guard skips initialization (weight: low)
Guards of the form `if (field != null && ...)` incorrectly skip the first-encounter case when the field starts as null.

### Claim / lock method returns null (weight: low)
A "mark for exclusive use" or "claim" method returns null meaning nothing available, but callers dereference directly.

### Constructor parameter assigned as null instead of passed value (weight: low)
`this.field = null` instead of `this.field = parameter` silently discards the configuration.

### Update validation rejects no-op changes due to missing backfill (weight: medium)
When an update enforces immutability, it must copy the existing value into the incoming object so a no-change update does not trigger a spurious "field was changed" error.

### Helper silently returns empty on invalid input (weight: low)
A helper logs a warning and returns an empty collection on invalid input; callers block waiting for work that was never submitted.

### File.listFiles() null (weight: medium)
-  Is `File.listFiles()` result checked for null before iteration?
-  Is it called on a non-directory path?

### File.length() returns 0 for directories (weight: low)
When computing disk usage by summing `file.length()`, directories return 0 instead of recursive content size.

### poll() before processing prevents retry on exception (weight: low)
In a cleanup loop, `poll()` removes an item before processing finishes; any exception leaves it unprocessed.

### Division by zero (weight: low)
-  Is a runtime-derived divisor (e.g., `collection.size()`) checked for zero?

---

## ByteBuffer & I/O Depth

### Position-advancing get (specific pattern)
-  Does no-arg `ByteBuffer.get()` advance position inside a comparator or shared view?
-  Is the buffer owned by this code, or is it a shared/read-only view?

### Shared buffer passed to helper without snapshot (weight: medium)
-  Is a shared buffer handed to a helper whose relative reads advance position as a side effect?
-  Is there a subsequent reader of the same buffer that will read from the wrong offset?
-  Does a rewind-to-restore pattern race with concurrent readers of the same buffer?

### Write-side buffer capacity not checked before put (weight: medium)
-  Does a write of a fixed-width trailer / marker / end-of-segment token precede a `remaining() >= N` check?
-  Does an encode loop exit on input-consumed instead of output-flushed — so when output exceeds input, the tail is silently truncated?
-  Does a decompressor variant that does not validate output bounds run on untrusted network data?

### Zero-remaining buffer not guarded before length-prefix read (weight: low)
Before reading a length prefix (or fixed-width header) from a buffer, is `remaining() >= headerSize` checked? A legitimately-empty component otherwise throws an underflow exception instead of being treated as "no content".

### rewind() instead of flip() after write (weight: low)
After populating a buffer for handoff to a reader, `flip()` sets limit = position then resets position to 0. Using `rewind()` instead resets position only, exposing uninitialized bytes beyond the written region.

### NIO read completeness (weight: low)
-  Does `Channel.read()` assume a single call fills the entire buffer?
-  Is there a loop until `remaining() == 0` or EOF?

### EOF handling (specific pattern)
-  Does NIO `read()` distinguish -1 (EOF) from 0 (empty buffer)?
-  Does `InputStream.read()` return 0 at end-of-stream instead of -1?

### Partial file left on exception during write (weight: low)
-  Does an exception in the write path leave a half-written file without an abort / delete step? A subsequent read treats it as complete.
-  Does writing to an existing file fail to truncate first, leaving stale bytes beyond the new content?

### Retry/backoff unbounded (weight: medium)
-  Does exponential backoff have a ceiling?
-  Can the lower bound grow past the upper bound?

### Sets.union lazy view (weight: low)
-  Is `Sets.union` accumulated in a loop, creating chain depth = iteration count?
-  Should the view be materialized into a concrete set?

---

## Interval & Range Depth

### Interval endpoint operator (weight: low)
-  At shared boundaries: is `< 0` vs `<= 0` consistent with open/closed convention?
-  Does the documented convention match the implementation?

### Half-open to closed conversion (weight: low)
-  When wrapping `[a, b)` API for `[a, b]` range: is upper bound shifted by +1?

### Range bound partial enforcement (weight: low)
-  Is start boundary enforced but finish boundary ignored (or vice versa)?

### Wrap-around range causes infinite linear scan (weight: low)
In a backward or reverse iteration, does the loop guard use strict `<` (or `>`) against an endpoint that may lie on the wrong side of the start in a wrapping range?

---

## Arithmetic Pitfalls

### Missing parentheses (weight: low)
-  Does `n + 1 / 2` lack parentheses, evaluating as `n + 0`?

### Floating-point loop drift (weight: low)
-  Does a float/double accumulator loop from 0.0 to 1.0 miss the final value?

### Ceiling division (specific pattern)
-  Is `(n + d - 1) / d` used correctly for ceiling division?
-  Is `round-down-to-multiple` producing zero when value < N?

### Capacity vs read bounds conflated (weight: low)
-  Is buffer capacity treated as exclusive for both reads AND seeks?
-  Should seek-to-end (`<= capacity`) be allowed while read-at-end is not?

### Compressed vs uncompressed size (weight: medium)
-  Is the compressed or uncompressed size used for file-split decisions?
-  Are progress numerator and denominator using the same size metric?

### nanoTime deadline addition overflows (weight: low)
Computing a deadline as `nanoTime() + duration` overflows near `Long.MAX_VALUE`; the safe pattern is `nanoTime() - start < duration`.

### Arithmetic on Integer.MAX_VALUE sentinel overflows without guard (weight: low)
Any `limit + 1` pattern where `limit` may be `Integer.MAX_VALUE` silently wraps to negative.

### Probabilistic gate does not short-circuit on boundary values (weight: low)
A probability check always calls the RNG even when configured to 0.0 or 1.0.

### Expiry / deadline computed from raw sentinel without substitution (weight: low)
-  Is a configuration value like `retention = -1` (meaning "infinite") passed as-is to `now + duration`, producing an already-expired instant?
-  Is an epoch-offset constant defined as negative and then subtracted (double-negation) instead of added?

### Serialized size miscounted against actual write (weight: medium)
-  Does the size calculation add a field's length prefix but forget the payload bytes?
-  Does a conditional field get counted unconditionally AND again inside its branch — declared size > actual bytes written?
-  Does the field order used by the size calculation diverge from the write path so a refactor changes one but not the other?
