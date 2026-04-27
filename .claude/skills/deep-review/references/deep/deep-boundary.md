# Deep Boundary & I/O — Extended Checklist

Full-depth checklist for deep review. Extends the shallow 15-item specialist checklist with
all 30+ boundary patterns and null/bounds check patterns from the catalog.

---

## Phase 0: Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Data ranges**: What are the valid ranges for indices, sizes, positions in this code?
2. **Sentinel values**: What does -1, 0, null, MAX_VALUE mean in this context?
3. **Buffer lifecycle**: Who allocates, positions, slices, and releases buffers?
4. **I/O contracts**: What does the underlying read/write API guarantee? Partial reads?
5. **Arithmetic units**: What units are sizes, timestamps, and positions in?

---

## Off-by-One & Range Depth

### Time unit at API boundary (11 known bugs)
- [ ] Does a timestamp cross an API boundary in the wrong unit? `currentTimeMillis()` where microseconds expected?
- [ ] Are seconds displayed as milliseconds in SSTable metadata?
- [ ] Is the wrong `TimeUnit` enum argument passed?
- [ ] Is subtraction used instead of addition for UUID epoch offset?

### Boundary comparison (8 known bugs)
- [ ] State the boundary condition in English. Does the operator (`<` vs `<=`) match?
- [ ] Is an inclusive bound treated as exclusive (or vice versa)?
- [ ] Is the equality case at the boundary missed?
- [ ] Does a chunk-boundary alignment assertion fire for legitimate partial chunks?

### Paging state (8 known bugs)
- [ ] Are paging boundary keys re-emitted due to missing exclusivity check on left bound?
- [ ] Does page-boundary trimming clone with wrong sort order?
- [ ] Does paging state track column name without considering liveness?
- [ ] Do phantom static-only partitions appear on page boundaries?
- [ ] Does DISTINCT paging override query-level row count with pageSize?

### Integer overflow (7 known bugs)
- [ ] Does multiplying `int` config by 1024 or 1024*1024 overflow without long promotion?
- [ ] Does a `Comparator` use integer subtraction (overflow risk)?
- [ ] Does `Ints.checkedCast()` on filesystem sizes truncate?

### Full-ring / wrap-around (4 known bugs)
- [ ] Does token-range arithmetic handle `left == right` (full ring)?
- [ ] Does wrap-around past minimum token work correctly?
- [ ] Does ownership percentage use modular arithmetic for wrapping ranges?

### Comparator bugs (4 known bugs)
- [ ] Does the comparator use `*.compare()` methods (not subtraction)?
- [ ] Does it handle equal elements correctly?
- [ ] Does it satisfy the transitivity contract?
- [ ] Are operands consistent in ordering across comparisons?

### Recursive iterator overflow (3 known bugs)
- [ ] Does `computeNext()`, `hasNext()`, or a helper call itself recursively?
- [ ] Will it overflow the stack on large inputs?

### MB/GB to bytes overflow (3 known bugs)
- [ ] When converting MB/GB to bytes: is at least one operand `long` before multiplication?
- [ ] Does the intermediate product overflow at 2 GB?

### Loop stop condition (3 known bugs)
- [ ] Does the loop use `> limit` instead of `>= limit` (or vice versa)?
- [ ] Does it process one too many or one too few items?

### ByteBuffer offset adjustment (3 known bugs)
- [ ] Does `ByteBufferUtil.get*` or `buffer.array()` add `position()` or `arrayOffset()`?
- [ ] After `slice()`, is the offset nonzero and accounted for?

---

## Null & Bounds Check Depth

### Map/schema lookup null (14 known bugs from map.get alone)
- [ ] After `Schema.getKSMetaData()`, `cfMetaData().get()`, `getColumnDefinition()`: is the result null-checked?
- [ ] Does a background task race with concurrent DROP TABLE/KEYSPACE?
- [ ] Does a JMX path accept arbitrary user-supplied names without null guard?

### Collection/array bounds (19 known bugs)
- [ ] `list.get(0)` without `isEmpty()`?
- [ ] `array[idx]` without length check?
- [ ] Empty BATCH statement? Empty IN values?
- [ ] Tuple with too many elements?
- [ ] `Optional.get()` without `isPresent()`?
- [ ] Parallel-list length mismatch?

### Nullable lifecycle fields (17 known bugs)
- [ ] Is a field initialized in `start()`/`setup()` rather than constructor?
- [ ] Is it dereferenced during shutdown, JMX access, or startup race?
- [ ] Is it accessed from a wrong construction path?

### Infinite loops (14 known bugs)
- [ ] Does a `while(true)` or `while(!done)` have an explicit escape path?
- [ ] Is there a timeout, maximum-retry count, or interrupt check?
- [ ] Can the condition EVER become true given possible upstream stalls?

### Lazy singleton before init (13 known bugs)
- [ ] Is `ClusterMetadata.current()`, `Keyspace.open()`, `Schema.instance` accessed before full startup?
- [ ] Is this code reachable from offline tools or early bootstrap?

### Missing instanceof before cast (12 known bugs)
- [ ] Is every cast preceded by a matching `instanceof` guard?
- [ ] When a new implementation is added, does it pass existing type checks?

### Empty collection edge cases (11 known bugs)
- [ ] What happens with zero items: empty data_file_directories, empty keyspace, zero SSTables?
- [ ] Does empty credentials cause a different code path?
- [ ] Does empty IN values produce correct results?

### File.listFiles() null (3 known bugs)
- [ ] Is `File.listFiles()` result checked for null before iteration?
- [ ] Is it called on a non-directory path?

### Division by zero (2 known bugs)
- [ ] Is a runtime-derived divisor (`collection.size()`, `getMeanColumns()`) checked for zero?

---

## ByteBuffer & I/O Depth

### Position-advancing get (specific pattern)
- [ ] Does no-arg `ByteBuffer.get()` advance position inside a comparator or shared view?
- [ ] Is the buffer owned by this code, or is it a shared/read-only view?

### NIO read completeness (2 known bugs)
- [ ] Does `Channel.read()` assume a single call fills the entire buffer?
- [ ] Is there a loop until `remaining() == 0` or EOF?

### EOF handling (specific pattern)
- [ ] Does NIO `read()` distinguish -1 (EOF) from 0 (empty buffer)?
- [ ] Does `InputStream.read()` return 0 at end-of-stream instead of -1?

### Cache key dangling reference (2 known bugs)
- [ ] Does a cache key hold a reference into a reusable I/O buffer?
- [ ] Will the next read overwrite the cached data?

### Retry/backoff unbounded (4 known bugs)
- [ ] Does exponential backoff have a ceiling?
- [ ] Can the lower bound grow past the upper bound?

### Sets.union lazy view (1 known bug)
- [ ] Is `Sets.union` accumulated in a loop, creating chain depth = iteration count?
- [ ] Should the view be materialized into a concrete set?

---

## Interval & Range Depth

### Interval endpoint operator (2 known bugs)
- [ ] At shared boundaries: is `< 0` vs `<= 0` consistent with open/closed convention?
- [ ] Does the documented convention match the implementation?

### Half-open to closed conversion (1 known bug)
- [ ] When wrapping `[a, b)` API for `[a, b]` range: is upper bound shifted by +1?

### Range bound partial enforcement (2 known bugs)
- [ ] Is start boundary enforced but finish boundary ignored (or vice versa)?

### Wrapping token range (2 known bugs)
- [ ] Is a wrapping `Range<Token>` passed to overlap/contains without `unwrap()`/`normalize()`?

---

## Arithmetic Pitfalls

### Missing parentheses (1 known bug)
- [ ] Does `n + 1 / 2` lack parentheses, evaluating as `n + 0`?

### Floating-point loop drift (1 known bug)
- [ ] Does a float/double accumulator loop from 0.0 to 1.0 miss the final value?

### Ceiling division (specific pattern)
- [ ] Is `(n + d - 1) / d` used correctly for ceiling division?
- [ ] Is `round-down-to-multiple` producing zero when value < N?

### Capacity vs read bounds conflated (1 known bug)
- [ ] Is buffer capacity treated as exclusive for both reads AND seeks?
- [ ] Should seek-to-end (`<= capacity`) be allowed while read-at-end is not?

### Compressed vs uncompressed size (4 known bugs)
- [ ] Is the compressed or uncompressed size used for file-split decisions?
- [ ] Are progress numerator and denominator using the same size metric?

---

## Missing from §14: Boundary — Specific Patterns

### Binary search post-adjustment off-by-one (2 known bugs)
After binary search in a sorted range structure, adjusting the returned index by +1 for an exact match is wrong when the invariant requires the element to stay at the matched position.

### Int intermediate in file-position arithmetic overflows for large SSTables (2 known bugs)
When an array or buffer offset is computed by dividing a multi-gigabyte file position by a chunk size and then multiplying, an early `(int)` cast silently truncates.

### poll() Before Processing Prevents Retry on Exception (1 known bug)
In a cleanup loop, poll() removes an item before cleanup finishes; any exception leaves it unprocessed.

### For-each loop with manual counter incremented at body start is off-by-one (1 known bug)
A manual counter incremented at the top of a for-each loop body is always one ahead of the current element when used as an array index.

### Binary search return value not adjusted for non-zero fromIndex base (1 known bug)
When `binarySearch` accepts a non-zero `fromIndex`, a custom implementation may return a relative index while the caller expects absolute.

### nanoTime deadline addition overflows (1 known bug)
Computing a deadline as `nanoTime() + duration` overflows near `Long.MAX_VALUE`; the safe pattern is `nanoTime() - start < duration`.

### Compressed-section boundary at exact chunk multiple includes extra chunk (1 known bug)
An endpoint that falls exactly on a chunk boundary does not require the next chunk; failing to check this sends one extra chunk.

### Arithmetic on Integer.MAX_VALUE sentinel overflows without guard (1 known bug)
Any `limit + 1` pattern where `limit` may be `Integer.MAX_VALUE` silently wraps to negative.

### Probabilistic gate does not short-circuit on boundary values (1 known bug)
A probability check always calls the RNG even when configured to 0.0 or 1.0.

### Accumulated counter with constant increment exceeds valid input domain (1 known bug)
The accumulated value can drift past the boundary of the valid input range.

### Loop intended to check one beyond interval uses wrong comparator (1 known bug)
A scan loop meant to "check one extra entry beyond the interval" silently skips that entry when the boundary uses `<` instead of `<=`.

---

## Missing from §17: Null, Bounds, Sentinel — Specific Patterns

### Update validation rejects no-op changes due to missing backfill (3 known bugs)
When an update enforces immutability, it must copy the existing value into the incoming object so a no-change update does not trigger a spurious "field was changed" error.

### Deleted row's ColumnFamily is null but iteration assumes non-null (3 known bugs)
When iterating rows from a range scan, `row.cf` can be null for rows with no live data.

### SSTable schema lookup for secondary-index SSTables resolves wrong CFMetaData (2 known bugs)
Schema lookup using a raw SSTable descriptor does not handle the secondary-index CF name format (`parent.indexName`).

### markAllCompacting() or resource-claim method returns null (2 known bugs)
A "mark for exclusive use" method returns null meaning "nothing available," but callers dereference directly.

### Nullable subtype-specific field accessed without subtype check (2 known bugs)
A field populated only for certain subtypes is accessed without checking the subtype, causing NPE.

### Constructor parameter assigned as null instead of passed value (2 known bugs)
`this.field = null` instead of `this.field = parameter` silently discards the configuration.

### Null-accumulator first-encounter guard skips initialization (1 known bug)
Guards of the form `if (field != null && ...)` incorrectly skip the first-encounter case when the field starts as null.

### Empty or impossible range not detected before execution (1 known bug)
Queries with guaranteed-empty slice bounds still execute because filter is non-null but semantically empty.

### Missing upper-bound guard on array-indexed level scan (1 known bug)
A level-scanning loop that schedules compaction for `level + 1` without checking maximum index produces AIOOBE.

### Helper logs warning and continues on invalid input, returning empty collection (1 known bug)
Callers (e.g., repair) hang waiting for work that was never submitted because the helper returned empty.

### File.length() returns 0 for directories (1 known bug)
When computing disk usage by summing `file.length()`, directories return 0 instead of recursive content size.
