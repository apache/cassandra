# Category: Boundaries and Numbers

Bugs involving numeric arithmetic, integer overflow, off-by-one errors, unit/dimensional mismatches, buffer-bound violations, sentinel/special-value confusion, and floating-point pitfalls — wherever a number, index, or quantity is computed, compared, or interpreted incorrectly.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- Arithmetic operators (`+`, `-`, `*`, `/`, `%`, `<<`, `>>`) on `int`/`long`/`short` fields, especially before a widening cast
- Index expressions (`a[i]`, `list.get(i)`), `subList`, `slice`, or pre/post-increment in a loop body
- Comparison operators (`<`, `<=`, `>`, `>=`, `==`) used as loop bounds, threshold checks, or boundary conditions
- Casts between numeric types (`(int)`, `(short)`, `(long)`, `toInt`, `toLong`)
- Unit-bearing names or constants (`ms`, `Ms`, `Nanos`, `Seconds`, `MB`, `KB`, `Bytes`, `MiB`, `MILLIS_PER_*`, `BITS_PER_*`)
- `TimeUnit`, `Duration`, `Instant`, `currentTimeMillis()`, `nanoTime()`
- ByteBuffer operations: `position()`, `limit()`, `flip()`, `rewind()`, `slice()`, `duplicate()`, `arrayOffset()`, `array()`, `getInt()`, `putShort()`
- Buffer/array allocation sized from an external length or count (`new byte[len]`, `ByteBuffer.allocate(...)`)
- Sentinel constants like `-1`, `Long.MAX_VALUE`, `Integer.MIN_VALUE`, `MAX_VALUE`, `Short.MAX_VALUE`
- Division operations that may produce zero from integer truncation or divide by an unguarded denominator
- Floating-point accumulators, percentage/ratio calculations, `Math.round`, `setScale`, `BigDecimal.toPlainString`
- Histogram/percentile/bucket/level/tier code with bucket array indexing
- TTL, expiry, deadline, timeout, backoff, throttle, or rate-limit calculations
- Length-prefixed encoding/decoding with explicit width (2-byte, 4-byte, varint) or `serializedSize()` methods
- New configuration fields with numeric defaults or unit suffixes

## Findings

### F-01: Int overflow on multiply-then-widen
A computation multiplies two `int` operands (or shifts an int) and only widens the product to `long` after the multiplication, so values above ~2 GB silently wrap to a negative number that is then stored as a size, offset, or threshold.
**Look for:** `long x = a * b` where `a`/`b` are `int`; `MB << 20`, `kb * 1024`, `count * sizeOf` patterns missing an explicit `(long)` cast on either operand.

### F-02: Per-level/per-bucket accumulator wraps int
Per-shard, per-level, or per-bucket counts are summed into a 32-bit accumulator that exceeds `Integer.MAX_VALUE` for large datasets, wrapping to a negative value reported as pending tasks, byte position, or row count.
**Look for:** `int total +=` inside a loop iterating shards/levels/segments; a status/aggregation field that accumulates across many partitions.

### F-03: Off-by-one loop counter as index
A loop counter is pre-incremented at the start of the body and then used as an array index in the same body, so every access is one position ahead of the intended element.
**Look for:** `i++` near the top of a loop body followed by `arr[i]` later in the same iteration; or `++i` in the middle of an expression that also indexes an array.

### F-04: Strict vs non-strict boundary comparator
A boundary or threshold check uses `<` where `<=` is required (or vice versa), so the element exactly at the boundary is included/excluded incorrectly: duplicate emissions on page boundaries, infinite loops at empty quotients, premature EOF, last-bucket dropped, retained-when-equal segments not recycled.
**Look for:** Boundary helpers that compare `pos > limit`, `count >= max`, `current < end` near pagination, slice-bound, segment recycle, or merge eligibility code.

### F-05: BufferOverflow on exact-full buffer write
A fixed-size record (end-of-segment marker, header, footer) is written without first checking that enough space remains, so when the buffer is exactly full at the start of the write the operation throws `BufferOverflowException`.
**Look for:** A series of `put*` calls into a `ByteBuffer` without a preceding `remaining() >= N` check, especially at segment rotation/finalization boundaries.

### F-06: Floating accumulator drift past domain
A floating-point accumulator is stepped by a fixed delta inside a count-bounded sampling loop; accumulated rounding error causes the loop to reach a value past the valid domain on the final iteration, passing an out-of-domain argument (e.g., `log(0)`, `asin(1.0+eps)`) to a sampled function.
**Look for:** `for (int i=0; i<n; i++) { x += step; sample(x); }` patterns where `n*step` should equal a domain boundary.

### F-07: Loop runs one iteration too many
A numerical sampling or chunking loop terminates one iteration past intent, feeding the boundary value to a function that may be undefined, infinite, or duplicate-producing at that boundary.
**Look for:** `for (int i=0; i<=n; i++)` where the loop should be `<n`; cumulative-division loops emitting one extra partition.

### F-08: Rate metric sampling-window vs reporting-unit mismatch
A rate or percentile metric is sampled over a window (e.g., 60 s) but reported in a different unit (e.g., 1 hour), so the displayed value is a fixed multiple too high or low.
**Look for:** Sensor/timer registration where the configured window duration and the reported "per X" unit are independent constants; metrics named `*_per_hour` derived from a `60s` window.

### F-09: Dimensionless parameter receives unit-bearing value
A parameter declared as a dimensionless fraction or count receives a unit-bearing value (e.g., a millisecond integer like 300000 used as a backoff multiplier), producing wildly wrong results.
**Look for:** Method calls passing a positional numeric argument where the parameter name suggests a fraction (`*Factor`, `*Ratio`, `*Probability`) but the call site has a long ms/ns value.

### F-10: Time-unit mismatch in compare or convert
A timestamp expressed in milliseconds is compared against, added to, or subtracted from one expressed in microseconds (or seconds, or nanoseconds), producing comparisons off by 1000× or 1,000,000×.
**Look for:** Two timestamp variables compared without unit normalization; constants like `1000`, `1_000_000`, `1_000_000_000` near `System.currentTimeMillis` or `System.nanoTime`; `TimeUnit.NANOSECONDS.toX(...)` paired with a value already in another unit.

### F-11: Wrong unit factor in conversion
A unit-conversion expression multiplies by a constant that converts in the opposite direction (e.g., bits-per-byte instead of bytes-per-bit), producing values 8×, 1024×, or 60× off.
**Look for:** Hardcoded conversion constants (`8`, `1024`, `1024*1024`, `60`, `3600`) where the symbol/name suggests one direction but the operation goes the other way.

### F-12: Integer division silently yields zero
An expression divides a small numerator by a larger denominator using integer arithmetic, truncating to zero and then either looping forever, sizing a buffer to zero, treating the quotient as "unlimited", or producing zero progress.
**Look for:** `(small int) / (large int)` patterns at the head of loops, before `>>`, before `Math.log`, or before being applied as a stride or per-element budget; `bytesBudget / count` patterns.

### F-13: Divisor not zero-checked
A counter or denominator is used in a divide/modulo without a zero-check, throwing `ArithmeticException` on first invocation, on empty input, or after all rows are tombstones.
**Look for:** `x / y` or `x % y` where `y` is read from a collection size, a counter, or a method that can return 0; especially in retry-formula code, hash-bucketing, or page-size computation.

### F-14: Double-applied conversion or formula
A helper already applies a formula (quorum, scaling, conversion) and the caller applies the same formula again to the helper's result, producing values that are too small, too large, or doubled.
**Look for:** Two consecutive applications of the same constant (`*1024`, `(n/2)+1`); chained calls through wrapper methods that each scale the same value.

### F-15: Premature truncation before scaling
An integer divide is evaluated before multiplication by a scale factor, truncating the result to zero or losing precision.
**Look for:** `a/b * c` patterns where `c` is the scale; missing parentheses around `(a*c)/b`.

### F-16: Cast to narrower type loses high bits
A `long` value is cast to `int` (or `int` to `short`) before further use as an index, length, or position, silently producing wrong or negative values for large inputs.
**Look for:** `(int)(long)`, `(int) channel.size()`, `(int) (pos / chunkSize)`, especially in code that handles files, offsets, or capacities larger than 2 GB.

### F-17: Type narrower than actual domain
A field, parameter, or counter is typed narrower (`short`, `int`) than the domain of values it can carry; under sustained use a counter overflows the type, producing negative values or wrap-around behavior.
**Look for:** `short` fields incremented per timeout/event; `int` byte-position accumulators in code that handles multi-GB data; numeric overload selection that picks a narrower variant.

### F-18: Byte-budget vs item-count mismatch
A check enforces an item-count limit on data that should be measured in bytes (or vice versa), so large items exhaust memory before the count limit fires, or the reservoir contains far fewer than expected.
**Look for:** `count > bytesLimit` or `bytes > countLimit` style guards; histogram/buffer pool sizing where the limit unit mismatches the loop variable's unit.

### F-19: Fixed-width length prefix overflow
A length-prefixed encoding uses a 2-byte (or fixed-width) field but the actual payload exceeds the prefix's maximum (e.g., > 65535 bytes), causing silent truncation, framing corruption, or runtime exception.
**Look for:** `writeShort(payload.length)` style calls; `Short.MAX_VALUE` or `0xFFFF` next to length validation; user-controlled string concatenation that feeds into a length-prefixed serializer.

### F-20: Asymmetric size-read / size-write width
A serializer writes a length prefix at one width (e.g., 4 bytes) and a deserializer reads it at a different width (e.g., 2 bytes) — or signed vs unsigned varint variants — corrupting all subsequent bytes in the stream.
**Look for:** Paired `writeInt`/`readShort` (or `varInt`/`varSignedInt`); mismatched constants in `serializedSize()` and the actual write loop.

### F-21: Histogram bucket-overflow / bucket-too-small
A percentile or histogram computation does not handle observations beyond the largest bucket, throwing `ArrayIndexOutOfBoundsException` when all observations exceed the ceiling, or returning the lower bound of the highest bucket as the maximum.
**Look for:** Histogram percentile/max methods without an "overflow bucket" branch; merging two histograms whose lengths differ; fixed `BUCKET_COUNT` constants.

### F-22: Sentinel sign/value collides with valid domain
A negative sentinel like `-1` (meaning "uninitialized" or "no timestamp") compares as a normal integer; valid values can equal the sentinel, or the sentinel's negative value participates in arithmetic and breaks comparisons / arithmetic / array indexing.
**Look for:** `Long.MIN_VALUE`/`-1` used as an "absent" marker compared with `<` or `>`; sentinel passed to `nextInt(n)` or used as an exponent / array index.

### F-23: NaN guard missing in min/max shortcut
A min/max bounds shortcut reads precomputed column bounds without a NaN guard and returns a wrong answer because NaN values are unordered and excluded from stored bounds.
**Look for:** Range-pruning helpers reading `min`/`max` columns of floating-point types; `Math.min`/`Math.max` over `double` inputs without `isNaN`/`isFinite` checks.

### F-24: Modulo can produce negative result
A modulo expression on a hash code or on a value that may be negative produces a negative result that is then used directly as an array index, causing `ArrayIndexOutOfBoundsException`.
**Look for:** `hash % capacity` or `value % bucketCount` used as an index without `Math.floorMod` or `& (cap-1)` (when capacity is power-of-two).

### F-25: Sign-extension on byte read
Reading a signed `byte` and treating it as an unsigned value (without `& 0xFF`) flips the high-bit-set bytes to negative, causing length prefixes, version codes, or flags to be misinterpreted.
**Look for:** `int b = stream.read()` followed by arithmetic without masking; `getLong()`/`getInt()` that should be byte-swap-aware; `ByteBuffer.getLong()` without setting byte order explicitly.

### F-26: Comparator subtraction overflow
A comparator returns `(long)a - (long)b` cast to `int`, or returns `a - b` for `int`s where the difference may exceed `Integer.MAX_VALUE`, causing wrong ordering for far-apart values.
**Look for:** `compare()` methods that return `(int) (longA - longB)` or `valA - valB`; replace with `Long.compare`/`Integer.compare`.

### F-27: Range with equal endpoints treated as full range
A range constructor accepts equal left and right endpoints and treats the result as the entire ring/space rather than a degenerate single-point range, causing `intersects` to always succeed and queries to fan out cluster-wide.
**Look for:** `new Range(min, max)` from a dynamically computed min/max with no `min.equals(max)` short-circuit; wrap-around range logic.

### F-28: Inverted operands or direction flag
A subtraction (or unit conversion) is written with operands in the wrong order — `current - deadline` instead of `deadline - current`, or `epoch - now` instead of `now - epoch` — producing negative durations, immediate timeouts, infinite loops, or absurd elapsed-time logs.
**Look for:** Elapsed-time, retry-count, and TTL formulas with subtraction; sleep/timeout computations whose result is logged as negative.

### F-29: Deadline overflow when adding duration to clock
A deadline is computed by adding a duration to a monotonic clock value near `Long.MAX_VALUE`, causing the addition to overflow and produce a deadline in the past so the loop exits immediately.
**Look for:** `nanoTime() + timeoutNanos` or `currentTimeMillis() + duration` without saturating-add; `Math.addExact` rare; `if (deadline < now)` paths.

### F-30: Empty/zero passed to API that rejects empty/zero
A library API rejects a zero count, empty collection, or zero-length argument with `IllegalArgumentException` (e.g., `RateLimiter.acquire(0)`, `Random.nextInt(0)`, `BigDecimal` scale ops), and the caller does not guard against the degenerate case.
**Look for:** Pre-existing collection size or counter passed unchecked into `nextInt`, `acquire`, `setScale`, or other domain-restricted APIs.

### F-31: BigDecimal toPlainString blows up memory on large exponent
Calling `toPlainString()` on a `BigDecimal` with a large negative scale expands the number to a string of billions of characters, causing OOM.
**Look for:** `BigDecimal.toPlainString()` on user-supplied input; large negative exponent on serialization; `setScale(0, ...)` on values exceeding `long`.

### F-32: Array-offset / position not added in slice arithmetic
A buffer accessor computes an absolute position from a relative offset without adding the buffer's `position()` (or `arrayOffset()`), returning garbage or out-of-bounds values for any sliced or advanced buffer.
**Look for:** `buffer.array()` without adding `arrayOffset`; `getInt(i)` or `get(i)` with hardcoded `0` on a buffer whose position may be non-zero; `digest.update(buf.array(), 0, buf.remaining())`.

### F-33: Position not duplicated before relative read
A relative `getInt`/`get` is called on a shared buffer without first calling `duplicate()` (or `slice()`), advancing the original's position as a side effect and corrupting all subsequent reads from the same buffer.
**Look for:** `buf.getX()` without explicit `buf.duplicate().getX()` on shared/cached buffers; comparator methods that call relative-get on instance fields.

### F-34: rewind/flip confusion on output buffer
A buffer is `rewind()`-ed instead of `flip()`-ped (or neither) before being returned for reading, exposing uninitialized bytes beyond the written data or producing zero readable bytes.
**Look for:** Methods returning a `ByteBuffer` filled by writes without a `flip()` immediately before return; mark/reset paths that restore some fields but not the dirty flag.

### F-35: Backoff/retry has no ceiling or no floor
A backoff formula's growth term has no upper bound (or no lower bound), so at high attempt counts it can exceed sane limits, hit a `Math.min` of zero, or invert a clamp; alternatively the first delay is inflated by using `base^attempts` with a 1-based counter so attempt 1 already multiplies by `base`.
**Look for:** Exponential-backoff helpers without `Math.min(cap, ...)`; `Math.pow(base, attempt)` with `attempt` not zeroed; backoff multiplier bounded by an unrelated session timeout.

### F-36: Configuration value truncates before write to wider field
A configuration value held as `int` is multiplied by a unit factor in `int` arithmetic and then assigned to a `long` field, truncating before the widening assignment.
**Look for:** `long fieldBytes = configMb * 1024 * 1024` where `configMb` is `int`; replace with `(long) configMb * ...`.

### F-38: Deep dereference / recursive accumulator stack overflow
Recursive descent over a long flat input (CRTP-style or tail-recursion via `Iterables.concat`) consumes one stack frame per element, overflowing the JVM stack on large inputs.
**Look for:** Recursive `computeNext()`, `Iterables.concat` accumulated in a loop, recursive split helpers without iterative replacement.

### F-39: Off-by-one in length-vs-index check
A bounds guard uses strict greater-than against array length (`i > arr.length`) when greater-or-equal is required, allowing `i == arr.length` to pass into an indexing operation.
**Look for:** `if (i > arr.length)` then `arr[i]`; `if (size > limit)` checks placed after insertion instead of before.

### F-41: Unit-mismatched comparison via shadow constant
A timestamp stored in seconds is compared against another in milliseconds (or microseconds), or a millisecond TimeUnit is paired with a value computed in nanoseconds, causing unit-induced wrong comparisons or premature expiry by a 1000× factor.
**Look for:** Seconds-resolution `localDeletionTime` compared against microsecond-resolution `writeTime`; `TimeUnit.MILLISECONDS.sleep(nanos)` patterns.

### F-42: Empty-default treated as "smallest" in min-finder
A min-finding loop initializes the accumulator to an empty collection (size 0) and only updates when a strictly smaller is found; no collection is smaller than empty, so no candidate ever wins.
**Look for:** `min = Collections.emptyList()` or `min = empty`; subsequent loop comparisons with strict `<`.

### F-43: Over-1.0 / over-100% percentage
A progress ratio uses decompressed-byte counts in the numerator while the denominator is compressed bytes (or live count vs total estimate), making the percentage exceed 100% or go negative.
**Look for:** Progress display computing `done/total` where `done` and `total` come from different layers; ratio comparisons sensitive to dimension.

### F-44: Splitting at fixed size never reduces below threshold
A retry-on-overflow loop always splits at the same size and never reduces below the rejection threshold, so the same item is rejected and re-split forever, producing an infinite loop or stack overflow.
**Look for:** Split-and-retry helpers using a constant chunk size; recursion with no halving on the retry path.

### F-45: Read full unsigned value, then narrow without validation
A wide unsigned (or large signed) value is read from the wire into the correct type, then immediately cast to a narrower type without first validating that the value fits, silently truncating large values to wrong/negative numbers.
**Look for:** `int x = (int) wireLong` patterns near deserialization; numeric configuration fields reduced to a smaller type after reading.

### F-46: Misaligned index from stored vs actual array length
Code uses an index stored at write time but the corresponding array length at read time is shorter (e.g., schema evolved, fewer parallel-array entries), causing `IndexOutOfBoundsException`.
**Look for:** Loops bounded by one collection's `size()` while indexing another; comparator/parallel-list iterations.

### F-47: Half-open / open-closed interval mishandled
Interval intersection, containment, or merge code uses an open-closed interval where closed-closed is required (or vice versa), so points exactly at the boundary fall on the wrong side; alternatively a comparator treats wraparound as full-ring.
**Look for:** `interval.contains` helpers comparing `x > start && x < end` vs `>=`/`<=`; range-merge helpers around intersection of intervals.

### F-49: Counter overflow for short types
A counter typed as `short` is incremented per event (timeout, retry); after enough events it overflows to a negative value, wedging the producer with `IllegalArgumentException` or `NegativeArraySizeException`.
**Look for:** `short` fields incremented in retry/timeout paths; cumulative counters without periodic reset.
