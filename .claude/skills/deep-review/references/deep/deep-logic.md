# Deep Logic & Types — Extended Checklist

Full-depth checklist for deep review. Extends the shallow 20-item specialist checklist with
all 115+ logic patterns from the catalog. Use when the reviewer has identified specific files
for deep investigation.

---

## Phase 0: Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Class hierarchy**: What does this class extend? What interfaces does it implement?
2. **Sibling classes**: Are there parallel implementations that should mirror this code?
3. **Callers**: Who calls the methods being changed? How do they use the return values?
4. **Lifecycle**: When is this object created, used, and destroyed?
5. **Invariants**: What contracts does this class maintain? Read Javadoc, assertions, tests.

---

## Condition & Comparison Depth

### Type-mismatch in equals/contains (7 known bugs)
- [ ] For every `equals()` or `contains()` call: do BOTH sides have the same runtime type? Check for `InetAddressAndPort` vs `InetAddress`, `String.toString()` vs domain accessor, `ByteBuffer.equals` vs `isEmpty()`.
- [ ] For map lookups: does the key type match the map's key type exactly? `ColumnIdentifier` vs `ByteBuffer` mismatch causes silent lookup miss.
- [ ] Does `Arrays.asList()` wrap an already-List, creating `List<List<T>>`? `contains()` then compares wrong element type.

### Composite/compound names (6 known bugs)
- [ ] Does this code use a full composite byte buffer where a single component is expected? Check column-definition lookup, restriction array access, comparator selection, partition key EOC markers.
- [ ] After decomposing a composite: is the correct component index used?

### Catch clause scope (5 known bugs)
- [ ] Is the catch clause too broad? `catch (Exception)` that swallows errors making retry loops infinite.
- [ ] Is it too narrow? `catch (AssertionError)` missing `LinkageError` from class loading.
- [ ] Does `Futures.getUnchecked()` conflate cancellation with failure?

### else-if mutual exclusion (3 known bugs)
- [ ] For every `else if`: can BOTH conditions be true simultaneously? If yes, the second is silently skipped.
- [ ] Does `continue` in a multi-concern loop body suppress unrelated work?

### Map lookup direction (3 known bugs)
- [ ] Is the map queried with value where key is expected (or vice versa)?
- [ ] Does a permission check use the wrong scope (column-family name instead of keyspace)?

### Conditional counting (2 known bugs)
- [ ] Is a counter incremented per item in a collection that may contain duplicates?
- [ ] Is a "remaining" counter passed as "completed" to a progress API expecting monotonically increasing values?

### Dead code / unused accumulation (2 known bugs)
- [ ] Does a collector accumulate state but no code reads the result?
- [ ] Does a predicate always return a hard-coded constant instead of computing from fields?

---

## Constant & Default Depth

### Sentinel values (6 known bugs)
- [ ] Does `Long.MIN_VALUE` for "unknown maxTimestamp" cause data skipping?
- [ ] Does `Integer.MAX_VALUE` for gcBefore delete live hints?
- [ ] Is Murmur3 MINIMUM set to 0 instead of `Long.MIN_VALUE`?
- [ ] Does `Map.getOrDefault` return a default that matches absent-key semantics?
- [ ] Does a sentinel value treated as normal data enter arithmetic without a guard?
- [ ] Does `AlwaysPresentFilter` break callers treating return as real negative?

### Config default divergence (5 known bugs)
- [ ] Does the Java `Config` initializer default match the YAML template?
- [ ] Is a derived default read before the field it depends on is set?
- [ ] Does a hardcoded literal after config parsing override file-based configuration?

### Digest computation (4 known bugs)
- [ ] Does digest computation diverge across replicas due to differing read limits?
- [ ] Does memtable vs SSTable value-replacement differ for fetched-but-not-queried columns?
- [ ] Does Merkle tree include GC-able sentinel data?

### Time unit mismatches (4 known bugs)
- [ ] Internal timestamps (microseconds) compared against `Date.getTime()` (milliseconds)?
- [ ] `SimpleDateFormat` used for pre-1582 dates where `java.time.Instant` needed?

---

## Variable & Operand Depth

### Error messages (13 known bugs)
- [ ] Format string references wrong variable or swaps `%s` argument order?
- [ ] Error message names wrong flag/column or uses full object instead of `.name`?
- [ ] Timeout error embeds wrong consistency level?

### Logger class (7 known bugs)
- [ ] Is `LoggerFactory.getLogger()` passing the correct enclosing class, not copy-pasted from another file?

### Constructor overloads (7 known bugs)
- [ ] Does subclass delegate to wrong parent overload (parameter meanings swapped)?
- [ ] Is a required parameter not forwarded after signature change?
- [ ] Does overload resolution ambiguity from class hierarchy cause wrong binding?

### Copy-paste in parallel registration (5 known bugs)
- [ ] When registering similar items: does one entry duplicate a neighbor's argument?
- [ ] In adjacent metric registrations: are name/supplier pairs swapped?

---

## Iterator & Loop Depth

### Sub-iterator advancement (4 known bugs)
- [ ] Does chained sub-iterator use `if` instead of `while` to skip empties?
- [ ] Is a shared iterator consumed across multiple input items?
- [ ] Is a loop counter bounded by the collection's size?

### Separator logic (2 known bugs)
- [ ] Is "skip separator on first element" pattern applied when separator is a terminator?
- [ ] Is the wrong iterator checked for `hasNext()` when two parallel iterators run?

### Mutable iterator in lambda (1 known bug)
- [ ] Is `.next()` called inside a lambda passed to `forEach`, advancing past unintended elements?

---

## Refactoring Artifacts

### Stale identifier after rename (13 known bugs)
- [ ] After a rename: have ALL references been updated — string literals, JMX names, error messages, scripts, `Class.forName()` string-typed names?

### Wrong variable / wrong field (12 known bugs)
- [ ] Does comparison use a field from outer scope instead of loop variable?
- [ ] Does code use source buffer instead of destination?
- [ ] Does getter read derived state from child instead of authoritative field?

### Merge artifacts (16 known bugs)
- [ ] After merge/rebase: is the resolution semantically correct (not just syntactically)?
- [ ] Does merge leave wrong variable name, wrong import, wrong constant, duplicate declaration, inverted condition, or dead code from the other branch?

---

## Filtering & Query Result Depth

### Inverted boolean (24 known bugs)
- [ ] For EVERY boolean guard: read aloud as a sentence. Does the polarity match the intent?
- [ ] Is `!` where there should be none? `<` instead of `>=`? `==` instead of `!=`?

### Missing guard for empty input (13 known bugs)
- [ ] What happens when the collection/iterator has zero elements?
- [ ] PK-only table? Zero SSTables? Empty credentials? Empty filtered result?

### Secondary index routing (12 known bugs)
- [ ] Does index selection use correct method for membership testing?
- [ ] CONTAINS KEY vs VALUES distinction correct?
- [ ] IN treated as single-valued equality?

### Static row handling (8 known bugs)
- [ ] Are static rows handled differently from regular rows for LIMIT, DISTINCT, paging, index lookup, writetime/ttl?

### Result column order (8 known bugs)
- [ ] Is column ordering driven by the authoritative ordered list, not `Map.values()` or `Map.keySet()`?

### ReversedType unwrapping (7 known bugs)
- [ ] Is `instanceof`, `isUDT()`, `compareForCQL()` called on a type that might be wrapped in `ReversedType`?
- [ ] Are DESC clustering columns unwrapped before comparison?

### LIMIT granularity (6 known bugs)
- [ ] Does LIMIT count CQL rows vs internal columns vs partitions correctly for the table type?
- [ ] Are static rows counted the same as regular rows in clustering-filtered queries?

---

## Distributed Systems Logic

### Tombstone handling (12 known bugs)
- [ ] Are tombstones stripped by filters before reconciliation needs them?
- [ ] Do range tombstones survive type-unaware cell discard loops?
- [ ] Do tombstoned partitions consume LIMIT slots?
- [ ] Does compaction lose row tombstones via container `clear()`?

### Schema operations (3 known bugs)
- [ ] Does shared code path serve INSERT and UPDATE without type-specific guard?
- [ ] Is validation logic shared between local and remote mutation paths?

### Feature flags (2 known bugs)
- [ ] Does feature flag guard the main operation but not setup, teardown, or secondary paths?

---

## Performance Anti-Patterns (detectable in review)

### Hot-path re-derivation (2 known bugs)
- [ ] Does a method called in an inner loop delegate to an expensive operation on every invocation instead of caching?

### Unnecessary auto-boxing (2 known bugs)
- [ ] Does a tracker parameterized with `Long`/`Integer` auto-box on every comparison in a tight loop?

### O(n) in hot path (2 known bugs)
- [ ] Does a method called in a hot path iterate a collection where a reverse lookup map gives O(1)?

---

## Missing from Baseline §1: Logic Errors — Specific Patterns

### Eager evaluation in disabled log/debug path (5 known bugs)
A log statement's arguments include method calls (collection formatting, type serialization, String.format) that are evaluated even when the log level is disabled, or SLF4J `{}` and `+` concatenation are mixed. Spot by checking log calls with non-trivial arguments for an `isXxxEnabled()` guard.

### SLF4J `{}` vs printf `%s`/`%d` mismatch (4 known bugs)
A logger call uses printf placeholders instead of SLF4J `{}`, or has more tokens than arguments, or a logback pattern uses `%caller` producing unexpected multi-line output.

### Wrong timeout or verb category for the operation (4 known bugs)
Read-repair bounded by write timeout, repair verbs using generic RPC timeout, CAS messages using wrong expiring-map timeout, or batchlog/hint delivery that should not produce new hints on failure.

### Bits-vs-bytes or megabits-vs-megabytes unit label or conversion factor wrong (4 known bugs)
Throughput display or throttle confuses bits and bytes, or the unit label says "MB/s" when the value is in "Mb/s".

### Compressed vs uncompressed size used for file-split/threshold decisions (4 known bugs)
When enforcing file-size limits or computing compaction estimates for compressed SSTables, code uses the uncompressed logical file pointer instead of the actual on-disk (compressed) byte count, causing premature splits or wildly wrong estimates.

### Tombstone purge in partial compaction resurrects shadowed data (3 known bugs)
When `removeDeleted()` or `isEmpty()` purges tombstones based on `gcBefore` without checking `shouldPurge()` to verify all versions of the row are in the compaction set, tombstones are removed while shadowed data remains in uncompacted SSTables.

### One-time batch operation triggers per-item side-effect (3 known bugs)
When multiple items are added in a batch operation and each item addition triggers a side-effect (like scheduling compaction), the result is N identical side-effects instead of one batched operation.

### Unconditional wait after stream initiation hangs when transfer has zero files (3 known bugs)
When a streaming session sends an initiation message and then unconditionally waits for completion, sessions with zero files never signal completion, causing an indefinite hang.

### Hardcoded IPv4 literal or IPv6 characters breaking structured names (5 known bugs)
`"127.0.0.1"` used where a configurable address is needed, `.toString()` produces leading slash breaking comparisons, or IPv6 colons break URI parsing and JMX ObjectName syntax.

### Typo or misspelling in user-facing string (5 known bugs)
An error message, log line, or CLI output contains a misspelled word, duplicate word, or wrong technical term that misleads operators.

### Comparator argument order / receiver-argument inversion (2 known bugs)
Arguments to a comparator, compatibility predicate, or constructor are transposed (keyspace in the columnfamily slot, dividend in the divisor slot). Spot by reading the method as a sentence and confirming each argument matches its role.

### Deduplication / visited-set populated from wrong source (2 known bugs)
When merging two data sources with deduplication, the "already-seen" set is populated from the wrong source. Spot by confirming the visited set tracks the secondary source.

### float-to-double widening before BigDecimal construction loses precision (2 known bugs)
Widening a `float` to `double` or passing through floating-point as an intermediate step introduces spurious digits or truncates large values.

### Ephemeral / internal entity accessible through user-facing path (2 known bugs)
A new internal property (ephemeral snapshots) is not checked by the user-facing deletion path, or a table-level operation accidentally affects entities belonging to other column families sharing the same scope.

### Purge / drop decision made before all contributing data is consumed (2 known bugs)
A tombstone purge decision is made using metadata from a partial subset of SSTables at construction time, before all contributing data has been iterated, potentially purging data that is still live.

### Tool exits 0 on failure / error swallowed silently (2 known bugs)
A CLI tool logs an error but returns exit code 0, or native I/O errors are logged and swallowed instead of propagated.

### Compressed vs uncompressed byte count mismatch in progress reporting (2 known bugs)
Progress numerator counts compressed bytes while denominator uses uncompressed (or vice versa), or scanner position compared against wrong byte count type.

### BigDecimal precision silently truncated or OOM from unbounded toPlainString() (2 known bugs)
`MathContext.DECIMAL128` silently truncates wider values, or `toPlainString()` with user-controlled exponent allocates a multi-gigabyte string.

### Scheduling or throttling at wrong granularity (2 known bugs)
A global recurring task scheduled once per column-family-store creating N copies, or I/O throttle firing on row counts rather than at the byte-transfer layer.

### Address string reused across protocols without port stripping or IPv6 escaping (2 known bugs)
Address strings gathered for CQL (with port) reused for JMX, or IPv6 address assembled by concatenation without bracket escaping.

### Schema Flags / Options Rebuilt From Scratch, Dropping Some (2 known bugs)
A schema-altering operation rebuilds a flags or options set wholesale instead of toggling individual members, silently dropping flags (like COUNTER) that the table previously carried.

### Operation Reads from Global Singleton During Sequential Replay (2 known bugs)
Code that reads from a global singleton (Schema.instance, ClusterMetadata.current()) during a sequential replay or migration phase observes state that is ahead of or behind the point being reconstructed.

### Blacklisted/suspect entries cause infinite loop in candidate selection (2 known bugs)
When a compaction or scheduling strategy iterates candidates and must skip blacklisted/suspect entries, the iteration does not properly wrap around or terminate, causing a busy-loop.

### Retry/inflation mechanism leaks inflated count into user-visible results (2 known bugs)
When a retry mechanism inflates a request count for short-read protection, the inflated count is propagated as the `originalCount` of further retries, silently growing the user-visible result.

### Assertion assumes exactly one output from operation that can produce zero (2 known bugs)
When a compaction helper asserts `size(output) == 1`, operations like cleanup or scrub that legitimately produce zero output files violate the assertion.

### Tie-breaking logic for equal-timestamp columns inconsistent across two code paths (2 known bugs)
When timestamp tie-breaking logic is duplicated between a `comparePriority` method and a reconciler, the authoritative path may fall through without checking for tombstones.

### add() used instead of addAll() when collection type changes (2 known bugs)
When a map value type changes from a single element to a `Collection`, callers that still use `add(entry.getValue())` add the entire collection as one element.

### Data structure selected unconditionally when modes need different semantics (2 known bugs)
A data structure (map type, collection type, comparator) is chosen without checking the mode or variant, but different modes require different semantics.

### Mixed-concern flag overloaded for two independent properties (1 known bug)
A single boolean controls two orthogonal properties, so a new use case that needs different values for each has no valid representation.

### Schema-derived property re-computed instead of stored, drifts after DDL (1 known bug)
A structural property (column layout) is inferred from the current column set rather than stored at creation time, so it changes after schema alterations.

### CAS operation treats [applied]=false as error without inspecting returned row (1 known bug)
A LWT that returns `[applied] = false` is immediately converted to an exception without checking whether the existing row already has the desired state.

### Delegating setter creates infinite recursion when target is self (1 known bug)
A setter that propagates to a sibling object's same setter does not guard against the case where the sibling is `this`.

### Assertion in base-class constructor fires for valid subclass usage (1 known bug)
An assertion in a base-class constructor enforces a constraint only valid for a subset of callers.

### Security policy checked on only one side of connection (1 known bug)
TLS enforcement is checked outbound but not inbound, allowing unencrypted connections when encryption is required.

### Cross-constraint satisfiability not checked at definition time (1 known bug)
Compound declarative predicates are validated individually but not cross-checked for contradictions or redundancies.

### Base set used after augmented set was constructed (1 known bug)
Code builds an "all" set by augmenting a base set, then subsequent operations use the base set instead of the augmented one.

### Streaming protocol missing ACK or drain on failure (1 known bug)
A request/reply protocol does not send the ACK on one side or does not drain the input stream before connection reuse on failure.

### Passive liveness detection (one-directional) in asymmetric network (1 known bug)
A node is marked alive based on received traffic without confirming two-way reachability, producing false-positive liveness behind firewalls.

### Scheduling loop misses re-arm on one branch (1 known bug)
A periodic task's scheduling loop misses re-arming the next scheduled event on the "work done" branch, causing the task to stop running.

### Inner worker void return hides failure from dispatch loop (1 known bug)
A `void` helper inside a dispatch loop cannot communicate failure back to the loop controller, so the loop continues past failures.

### Unconditional resource acquisition before mode/config check (1 known bug)
A connection or resource is acquired unconditionally before a branch that checks whether it is needed, failing on environments where the unused protocol is disabled.

### Mutable state overwritten between evaluation sites (1 known bug)
An object carries mutable state and evaluation logic called from multiple sites, so a later call's state overwrites the earlier one.

### Feature removal leaves leftover conditional references (1 known bug)
After removing a feature, leftover references to the removed concept in conditional guards invert or short-circuit important logic.

### Async future wrapping conflates cancellation with failure (1 known bug)
`Futures.getUnchecked()` converts both `CancellationException` and `ExecutionException` into `UncheckedExecutionException`.

### Resource name delimiter conflicts with user-supplied identifier content (1 known bug)
Function resource names parsed using delimiter that can appear inside function names.

### Feature predicate distributed across subclasses with stub implementations (1 known bug)
Prepared statement invalidation requires boolean predicate on every node in object graph, but stubs return hardcoded `false`.

### Multi-column restriction dispatch based only on first element type (1 known bug)
Dispatch branches on type of first restriction only, failing when collection contains mixed types.

### Point-wise vs span-wise overlap test (1 known bug)
L0 overlap test checks individual SSTable overlap instead of collective span coverage.

### SSTable metadata from foreign node trusted for local decisions (1 known bug)
SSTables streamed from other nodes carry metadata valid only for source node; local replay trusts it for truncation decisions.

### Missing sizeof(byte) overload causes silent widening to sizeof(int) (1 known bug)
Absent byte overload in TypeSizes causes compiler to widen to int, returning 4 instead of 1.

### Zero-copy buffer optimization violates parent class independence contract (1 known bug)
Overriding extractFrame returns zero-copy slice when parent requires independent buffer, causing data corruption.

### Return type mismatch in polymorphic Object-typed handle (1 known bug)
createPreparedStatement returns Object; paired execute casts to wrong concrete type.

### Mutable collection shared via copy() without defensive copy (1 known bug)
copy() passes EnumSet/HashSet by reference, causing mutations to propagate back to original.

### Negative Delta Passed to Non-Negative Allocation Tracker (1 known bug)
A before/after size difference that can be negative is passed to a memory-tracking primitive that asserts non-negative size.

### Boolean Parameter Conflates Two Orthogonal Concerns (1 known bug)
A boolean parameter (like `offline`) is repurposed as a proxy for a different concern (`keepOriginals`).

### Partition-to-Bucket Loop Re-reads Source from Beginning (1 known bug)
A collection is partitioned into output buckets inside a loop, but the source is iterated from the beginning for each bucket, duplicating elements.

### Parallel data structure loop bounded by wrong structure's size (1 known bug)
When iterating over two parallel arrays using only one's length as the loop bound, the other may have fewer elements.

### Lambda comparator argument order reversed (ascending vs descending) (1 known bug)
Comparators intended for descending order that use `(o1, o2)` instead of `(o2, o1)` silently sort in the wrong direction.

### Capacity check uses element count instead of element count times element size (1 known bug)
An upper-bound check expressed in count units rather than byte units allows the structure to exceed its byte-capacity limit.

### Conditional decrement chain across multiple if/else branches miscounts (1 known bug)
Computing an index by starting from a base and applying conditional decrements across branching logic applies the wrong number of adjustments.

### Float arithmetic on 64-bit token space loses precision (1 known bug)
Token-space arithmetic on Murmur3Partitioner using `float` or `double` loses precision; `BigInteger`/`BigDecimal` must be used.

### Mutable data structure returns construction-time constant instead of current state (1 known bug)
A method that should reflect the current state of a mutable structure instead returns a schema-level or construction-time constant.

### Subclass carries additional semantic identity not tested in type-check method (1 known bug)
When a subclass represents a semantically distinct variant, type-check methods on the parent that do not account for the subclass identity misclassify instances.

### Composite-object scan missing check at intermediate nesting level (1 known bug)
A scan that handles the outermost and innermost levels but skips the middle level (row tombstone) silently misses propagation of row-level deletions.

### Deprecated API delegates to new builder but drops some parameters (1 known bug)
When a deprecated API path forwards to a shared builder, any option the old path accepted but does not forward causes a silent no-op.

### Method has multiple execution paths but only some fulfill a shared postcondition (1 known bug)
When all branches of a method must meter or track a resource but only some branches contain the tracking call, the others silently skip it.

### Paired min/max configuration fields but only one is forwarded (1 known bug)
When a class has parallel min/max config fields, forwarding only one silently drops the other setting.

### Method return value silently ignored by caller who assumes in-place mutation (1 known bug)
When a method both mutates an argument and returns a value, callers that ignore the return value silently lose data.

### Elapsed time computed as `now - (now - timestamp)` which equals `timestamp` (1 known bug)
The expression simplifies to `timestamp` itself (not elapsed time), causing duration comparisons to produce nonsensical results.

### Hadoop predicate emptiness check uses wrong boolean operator (&& vs ||) (1 known bug)
Using `&&` (both absent) instead of `||` (either absent) means a one-sided range is incorrectly treated as non-empty.

### Response handler uses wrong contains() due to byte-array vs address type mismatch (1 known bug)
When checking `waitList.contains(message.getFrom())`, list type and address representation must match.

### Two-pass algorithm uses different reference time for each pass (1 known bug)
When a two-pass algorithm calls `System.currentTimeMillis()` independently in each pass, TTL expiry decisions can differ between passes.

### JMX MBean method name does not follow JavaBean naming convention (1 known bug)
Non-conforming names make the attribute invisible to JMX clients.

### Two different size metrics conflated (file bytes vs wire bytes) (1 known bug)
A function returning "bytes read from file" is used as "bytes sent over network" but the two differ.

### Two code paths for same operation diverge — one omits a required step (1 known bug)
When parallel code paths exist (NIO vs non-NIO, streaming vs local), one path omits a step that the other performs.

### Direct field access in compareTo/equals breaks subclasses and mocks (1 known bug)
A comparison implementation that reads final fields directly instead of through accessor methods cannot be overridden.

---

## Inverted / Negated Conditions (§2)

### Short-circuit loop semantics inverted (OR vs NOT-AND) (1 known bug)
Early-return on negative match with default true flips semantics, silently producing wrong IN-clause results.

---

## Wrong Variable, Field, or Operand — Additional (§3)

### Data structure or identifier misused: wrong backing collection type, wrong map key type, or CQL case mismatch (3 known bugs)
`ArrayBackedSortedColumns` with unsorted insertion, ColumnIdentifier-vs-ByteBuffer map key mismatch causes silent lookup miss, or mixed-case CQL identifiers silently disagree with server.

### Password hash comparison hashes the wrong operand (1 known bug)
The code hashes the stored value and compares it to the raw plaintext instead of hashing the supplied plaintext and comparing to the stored hash.

---

## Incomplete Dispatch (§5)

### Incomplete switch / enum dispatch — missing case (4 known bugs)
A switch statement or if-chain over an enum does not cover a newly added or uncommon value (`die`, `PREPARED`, counter verb, duplicate `case` label), causing fallthrough to default or skipping the needed action.

### Wrong type returned by fallback/default in type-dispatch method (3 known bugs)
A switch on table type ends with a hardcoded fallback type wrong for the uncommon case: UTF8 for static compact tables, wrong dense/sparse flag, or wrong cellNameType for super columns.

### Non-exhaustive switch on enum lacks default case for new or rare values (1 known bug)
A `switch` on an enum without exhaustive cases or a meaningful `default` crashes or falls through silently.

---

## Type Mismatches and Unsafe Casts (§6)

### Missing unwrap of ReversedType before type check (10 known bugs)
Code performs `instanceof`, `isCollection()`, `isMap()`, or similar type-identity checks on a column's AbstractType without first calling `unwrap()` or peeling `ReversedType`. DESC clustering columns always wrap the real type in ReversedType, so direct checks silently fail.

### Frozen vs non-frozen collection conflated (5 known bugs)
Code checks `isCollection()` when it should check `isMultiCell()`, or vice versa. Frozen collections are stored as opaque blobs, not as multiple cells, so treating them like non-frozen ones corrupts serialization.

---

## Wrong Constants and Default Values (§21)

### Bad merge leaves wrong symbol, class name, import, or logic (16 known bugs)
After a merge or rebase, conflict markers are gone but the resolution left the wrong variable name, wrong import, wrong constant, duplicate declaration, inverted condition, or dead code from the other branch.

### Unit mismatch in time / size arithmetic (9 known bugs)
A duration, timestamp, or size is computed by mixing incompatible units (nanoseconds added to milliseconds, int packed into long without widening, bit-field reconstruction losing high bits) without explicit conversion.

### Comparator / ordering logic incorrect (9 known bugs)
A custom comparator is asymmetric, uses subtraction instead of `Integer.compare()`, handles sentinel values inconsistently, does not cover all three cases, or bypasses `Cell.reconcile()` with hand-rolled LWW logic.

### Consistency level or sentinel value hardcoded where caller's context should be forwarded (2 known bugs)
A commit path hardcodes `QUORUM` instead of using caller's `LOCAL_SERIAL`, or CAS timeout exceptions carry hard-coded sentinel counts instead of computed values.

---

## Incorrect Filtering and Query Results (§22)

### Secondary index query routes to wrong index, wrong column, or wrong operator (12 known bugs)
Index selection uses a method whose contract is broader than membership testing, CONTAINS KEY routed as VALUES, IN treated as single-valued equality, non-primary-key columns silently accepted for IN, composite index inserts skip non-primary columns, SASI range comparison hardcodes literal mode.

### Guard condition correct for one mode but wrong for another (8 known bugs)
A guard like `isNormal()`, `isCQLTable()`, or version-equality is correct for the common mode but becomes a false positive or negative for an alternative mode (gossip-only member, reversed query, COMPACT STORAGE, rolling upgrade, SuperColumn table).

### Static row/column handling inconsistent with regular row logic (8 known bugs)
Static rows occupy a different position in the clustering order and have different semantics for LIMIT counting, DISTINCT deduplication, paging boundaries, partition existence checks, index lookups, writetime/ttl.

### Result column order or identity derived from wrong source (8 known bugs)
Column ordering driven by `Map.values()` or `Map.keySet()` instead of the authoritative ordered column list, ORDER BY resolved from schema metadata instead of SELECT list, result metadata built from deduplicating map.

### LIMIT or row-count applied at wrong granularity (6 known bugs)
LIMIT counts internal storage columns instead of CQL rows, counts rows instead of partitions for compact static tables, counts static rows the same as regular rows for clustering-filtered queries, or SELECT COUNT with LIMIT scans only LIMIT rows.

### Legacy or upgrade format reader does not handle all structural variants (6 known bugs)
Legacy SSTable reader missing static-name branch, super column upgrade hits MarshalException, comparator mismatch between old index entries and query names, Thrift-to-CQL range tombstone routing bypasses LegacyLayout.

### Scope or namespace not included in cache key, authorization check, or identifier resolution (4 known bugs)
Prepared statement cache keyed by query text alone without keyspace, BATCH authorization deduplicates by table name alone, UDF/UDA identifiers not validated against statement keyspace.

### Wrong collection or map queried for lookup (3 known bugs)
Code looks up a value in the wrong map (primary-key columns instead of regular-columns, internal strategy index instead of disk boundaries).

### Transformation / pipeline stage ordering error (3 known bugs)
In a chained iterator pipeline, stages are applied in the wrong order so a dependent stage misses events from the extension it depends on.

### Assertion used where defensive check is needed for legitimate runtime state (3 known bugs)
An `assert` guards against a state that legitimately arises: tombstone expiry during compaction, empty cell names with heap buffers.

### Missing or overly broad filter guard conflates enablement with fast-path (3 known bugs)
A single boolean guard used for both "should this subsystem activate" and "should the inner fast path short-circuit".

### Counter cell or commutative type needs special decode/digest path (3 known bugs)
Counter cells hashed as raw encoded bytes with per-replica shards instead of using type-aware normalizer, counter update cell misidentified as pre-2.1 local shard.

### Mutable Shared Object Passed to Multiple Consumers Without Copy (3 known bugs)
A stateful object (filter, query descriptor, message, ByteBuffer) with mutable counters/cursors/flags is constructed once and passed to multiple independent consumers in a loop.

### Wrong String.format / CQL bind-marker usage (2 known bugs)
A CQL query uses `%s` format specifiers with `executeInternal(query, values)` which expects `?` bind markers.

### Seek / rewind in format-aware iterator does not account for already-consumed data (2 known bugs)
A reader that consumes data during construction leaves the cursor past that data, but a subsequent seek/rewind goes back to a position that re-reads the already-consumed data.

### Wrong return value or outcome constant for the failed condition (2 known bugs)
IMH acquireCapacity returns wrong outcome on endpoint-reserve exhaustion, or DESCRIBE statement uses original collection instead of filtered copy.

### Bloom filter or index pre-computation wrong (2 known bugs)
Bloom filter sized from sum of all partitions instead of per-SSTable count, or batch-mode placeholder assigned before initializer that later overwrites it.

### Mixed-version or rolling upgrade counting/filtering divergence (2 known bugs)
Limit counts rows instead of partitions because older node wire representation differs, or LegacyLayout over-trims by counting tombstones same as live cells.

### Mutable Shared Object Modified by Cached/Shared Reference (2 known bugs)
A mutable object retrieved from a cache or factory is modified in-place by one consumer, affecting all other consumers sharing the same reference.

### Wrapping token range fed to interval query without unwrap check (2 known bugs)
Code that passes a `Range<Token>` directly to an overlap or contains query without checking `strictlyWrapsAround` silently misses SSTables.

### Boolean predicate named for one context used with inverted meaning in another (2 known bugs)
A predicate named for context A is used in context B where its boolean sense inverts.

### Timezone offset derived from current time instead of timestamp being converted (1 known bug)
A helper derives a timezone offset from `time.time()` instead of the timestamp being converted.

### Filter predicate applied to one branch of union but not both (1 known bug)
A canonical view is built by merging two collections with a filtering step on only one.

### nowInSec computed multiple times or propagated inconsistently (1 known bug)
`nowInSec` computed late or in multiple places, allowing divergence between QueryOptions and ReadQuery.

### Aggregate function returns wrong result on empty input (1 known bug)
Aggregate function fails to return a row on empty table, violating SQL semantics.

### Schema filtering excludes marked-for-delete rows that have live columns (1 known bug)
Using OR logic (`isMarkedForDelete() || isEmpty()`) instead of AND incorrectly excludes re-created entities.

---

## State Cleanup — Logic-Domain Patterns (§19)

### Stale identifier after rename/refactor (13 known bugs)
A rename leaves behind references to the old name in callers, log messages, string literals, tool registrations, example code, or string-typed class names that survive because `Class.forName()` is not checked at compile time.

### Wrong variable / wrong field used in comparison (12 known bugs)
The code compares or operates on the wrong variable — a field from the outer scope instead of the loop variable, the source buffer instead of the destination, a stale local instead of the freshly-fetched value, or the getter reads derived state from child objects instead of the authoritative field.

### Tombstone or deletion incorrectly discarded, not counted, or not propagated (12 known bugs)
Filters strip tombstones before reconciliation needs them, range tombstones dropped by type-unaware cell discard loops, partition-level deletions not recognized as completing a row for early-exit, tombstoned partitions consume LIMIT slots, tombstone counters over-count, compaction loses row tombstones via container clear(), deletion timestamp sourced from wrong object, or reverse-query iteration fails to preserve metadata-before-data ordering.

### Reference equality (==) used instead of value equality (.equals) (4 known bugs)
Code uses `==` to compare Strings, TableMetadata, or other value objects that may not be the same instance. After any refactoring that changes object lifecycle, identity checks break silently.

### Documentation, help text, or feature list not updated for new or renamed feature (4 known bugs)
When a feature is added, renamed, or removed, its documentation — help strings, CLI usage text, example commands — is not updated.

### DDL type stored as String, causing broken instanceof checks and double-parse (1 known bug)
When column type information is stored as a `String` and re-parsed at multiple use sites, `instanceof` checks against the String value silently fail.
