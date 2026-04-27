# Deep Resources & Serialization — Extended Checklist

Full-depth checklist for deep review. Extends the shallow 18-item specialist checklist with
all serialization, resource, lifecycle, and crash-safety patterns from the catalog.

---

## Phase 0: Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Serialization contract**: Read both `serialize()` and `deserialize()` end-to-end. List every field.
2. **Version gates**: What protocol/format versions exist? Which fields are gated?
3. **Resource ownership**: Who creates, who closes? Is ownership transferred?
4. **Error paths**: Trace every exception exit. What resources are held at each exit?
5. **Lifecycle**: When are resources acquired and released? What cleanup runs on shutdown?

---

## Serialization Depth

### Field count/order mismatch (10 known bugs)
- [ ] Compare `serialize()`, `deserialize()`, and `serializedSize()` line by line. Do they write, read, and measure the SAME fields in the SAME order under the SAME conditional guards?
- [ ] Is a field present in `serialize()` but missing from `serializedSize()` (or vice versa)?
- [ ] Is a `sizeof()` result computed but never accumulated (dead code)?
- [ ] Is a length field missing from the symmetric read/write contract?

### Wire format mismatch (10 known bugs)
- [ ] Does custom payload use `byte[]` where protocol specifies `ByteBuffer`?
- [ ] Are column names encoded as ASCII instead of UTF-8?
- [ ] Is Thrift wire format assumed when native protocol is in use (or vice versa)?
- [ ] Does nodetool use `String.getBytes` instead of the key validator?

### Empty/null buffers (9 known bugs)
- [ ] Can an empty buffer reach a serializer validate method?
- [ ] Can a zero-length value enter code assuming non-empty content?
- [ ] Does the code handle UNSET sentinel values alongside null?

### Wrong stream/variable (8 known bugs)
- [ ] After creating a wrapper stream (tee, counting, checked): does ALL subsequent I/O use the WRAPPER?
- [ ] Is the original field used instead of a locally transformed variable?
- [ ] Is `writableBytes()` called where `readableBytes()` is needed?

### Version compatibility (8 known bugs)
- [ ] Does the serializer restrict to current version only, breaking rolling upgrades?
- [ ] Has an enum ordinal been renumbered by removing middle constants?
- [ ] Do new schema columns break older peers?
- [ ] Is the digest computation version-aware?

### Nullable dereferencing (23 known bugs)
- [ ] Can individual UDT fields be absent?
- [ ] Can frozen collection elements be null?
- [ ] Can bind values be null or UNSET?
- [ ] Is a method return value discarded where it should replace the parameter?

### Checksum/digest range (4 known bugs)
- [ ] Does the CRC cover ALL serialized fields (including newly added ones)?
- [ ] Is `ByteBuffer.duplicate()` taken before write (not after)?
- [ ] Is the digest computation version-aware across upgrade boundaries?

### Collection serialization order (3 known bugs)
- [ ] Are frozen maps serialized via `SortedMap` with correct type comparator?
- [ ] Are JSON map keys wrapped in strings?

### VInt signed/unsigned (1 known bug)
- [ ] Is `computeVIntSize` used where `computeUnsignedVIntSize` is needed (or vice versa)?

### Length prefix width (1 known bug)
- [ ] Is a 4-byte length prefix written but 2-byte prefix expected on deserialization?

### Enum deserialization (1 known bug)
- [ ] Does enum-switched deserialization preserve a previously assigned field for a case where it should be absent?

---

## Resource Leak Depth

### Exception mid-sequence (7 known bugs)
- [ ] When multiple AutoCloseable resources are acquired in sequence: if the second allocation fails, is the first released?
- [ ] Is there a try/catch or try-with-resources chain guarding stepwise allocation?

### Loop-built closeables (3 known bugs)
- [ ] When Closeable resources are built in a loop: if an exception occurs partway, are already-created resources closed?
- [ ] Is each individual `release()` in a cleanup loop independently try/caught?

### Double-counting/release (2 known bugs)
- [ ] Is a resource counter incremented/decremented twice for the same event?
- [ ] Is a "free" operation using a pre-mutation delta?

### Close side effects (2 known bugs)
- [ ] Does `close()` trigger side-effecting callbacks without an idempotency guard?
- [ ] Could close fire twice (once from try-with-resources, once from caller)?

### Short-circuit cleanup (2 known bugs)
- [ ] Does a cleanup loop stop at the first exception instead of continuing to release all?
- [ ] Is each cleanup in the loop independently guarded?

### Ref-counted asymmetry (2 known bugs)
- [ ] Is a reference count + lifecycle transaction released as a pair on error?
- [ ] Does one half leak on error paths?

### View returned to pool (pattern)
- [ ] When a `ByteBuffer` is obtained via `nioBuffer()`, `slice()`, `duplicate()`: is the ORIGINAL returned to the pool?
- [ ] Does the pool receive a view it can't recycle?

### Filtered-out items (1 known bug)
- [ ] When an iterator of closeable objects is filtered: are filtered-out items closed?

### Metric registered without deregistration (1 known bug)
- [ ] Does every `addMetric` have a matching `removeMetric` in `close()`?

---

## Lifecycle & Configuration Depth

### Config not threaded (6 known bugs)
- [ ] Is a config knob parsed but never threaded to the builder?
- [ ] Is `null` passed where config should flow?
- [ ] Are streaming encryption settings dropped?

### Throwable gotchas (5 known bugs)
- [ ] Can `getMessage()` return null (no-arg exception constructor)?
- [ ] Can `getCause()` be null (no chained cause)?
- [ ] Can `getStackTrace()` return zero elements (`writableStackTrace=false`)?

### Stale/phantom entries (7 known bugs)
- [ ] Do records written during bootstrap, replacement, or failed repair persist after the phase ends?
- [ ] Are LEFT nodes removed from token maps?
- [ ] Are failed bootstrap entries removed from `system.peers`?

### Success path missing cleanup (5 known bugs)
- [ ] Does the success path miss cleanup that the error path has?
- [ ] Is session removal, counter decrement, or map eviction only on the error branch?

### State persisted too early (4 known bugs)
- [ ] Is persistent state written when an operation starts rather than when it commits?
- [ ] Could a crash leave stale state that prevents retry?

### Memory measurement (6 known bugs)
- [ ] Does `memorySize()` omit subclass fields?
- [ ] Does deep-measurement double-count shared singletons?
- [ ] Are `sizeOfEmpty` constants used for variable-length objects?

---

## File I/O & Crash Safety Depth

### File write modes (specific patterns)
- [ ] `WRITE | CREATE` without `TRUNCATE_EXISTING` on pre-existing file: old content past new EOF remains.
- [ ] `Files.copy()` without `REPLACE_EXISTING` in code that can run twice.
- [ ] Jackson `writeValue(OutputStream)` auto-closes: is write-to-temp, fsync, rename, fsync-parent used instead?

### Multi-file atomicity (pattern)
- [ ] When multiple related files are persisted: are ALL flushed and synced before the directory sync?
- [ ] Is there a missing flush on any one file in the set?

### Correlated state changes (pattern)
- [ ] Are multiple correlated state changes announced as separate operations instead of bundled atomically?

---

## Metrics & Observability Depth

### Metric timing (7 known bugs)
- [ ] Is a metric updated before the operation it measures?
- [ ] Is it after a different operation?
- [ ] Is it outside the guard that controls the measured operation?

### Metric type (specific patterns)
- [ ] Is `Histogram` used for latency (should be `Timer`)?
- [ ] Is the metric type consistent within the same class?
- [ ] Are count metrics → Counter/Meter, latency → Timer, distribution → Histogram?

### Progress counter (2 known bugs)
- [ ] Does progress increment per inner-loop page instead of per outer logical unit?
- [ ] Is the count off by orders of magnitude for wide rows?

---

## Platform & Encoding Depth

### Locale/charset (5 known bugs)
- [ ] Is `DecimalFormat` used without `Locale.ROOT`?
- [ ] Is `String.getBytes()` called without explicit charset?
- [ ] Does `Boolean.valueOf()` need to be `parseBoolean()`?

### I/O stream (specific patterns)
- [ ] Does `InputStream.read()` return raw signed byte without `& 0xFF`?
- [ ] Is `EINTR` handled in blocking system calls?
- [ ] Is `MessageDigest` shared across threads without synchronization?

---

## Filesystem Path Depth

### Path comparison (3 known bugs)
- [ ] Is `String.startsWith()` used for path containment instead of `Path.startsWith()`?
- [ ] Do symlinks break path comparisons?

### Wrong directory level (3 known bugs)
- [ ] Does `directory.getName()` return the wrong component after hierarchy changes?
- [ ] Is a directory level omitted in path construction?

### Layout migration (2 known bugs)
- [ ] Does a directory layout change include migration of existing data from old location?
- [ ] Are files unreachable through the new path abstraction?

---

## Missing from §18: Serialization — Specific Patterns

### Transient, non-serializable, or JMX-incompatible field in Serializable class (3 known bugs)
Transient field set only in constructor silently null after deserialization, Guava Multimap returned from JMX MBean not JMX-serializable, or third-party library types fail remote deserialization.

### Validation delegated to wrong serializer, wrong validator package, or bypassed entirely (3 known bugs)
Nested tuple/UDT validation routed to BytesSerializer instead of type-aware serializer, collection validate() called from legacy CQL2 package.

### Config unit or bound type mismatch: DataStorageSpec bound, TypeCodec size (2 known bugs)
A `DataStorageSpec` validation uses the wrong unit-granularity bound class (widening range by 1024x), or a `TypeCodec.serializedSize()` returns a mismatched constant.

### Endianness or byte-order not accounted for in bit-shift arithmetic (2 known bugs)
ByteBuffer.getLong feeds into bit-shift arithmetic assuming big-endian on little-endian buffers, or unsafe put/get does not account for BIG_ENDIAN on s390x.

### Protocol field order violation (encode vs decode sequence mismatch) (2 known bugs)
Encode writes flags before consistency level while decode reads them in opposite order, or paging state buffer not flipped before being read after write.

### Deserialization-order sensitivity not enforced (drain instead of throw) (1 known bug)
A streaming deserializer throws `IllegalStateException` when a caller short-circuits iteration, instead of draining unconsumed items.

### Data-model rename not propagated across all language layers (1 known bug)
Column alias renamed in Java but Python layer still queries old field name.

### ByteBuffer heap vs direct path divergence (1 known bug)
VectorCodec deserialization using slice() and rewind() on heap buffers produces different behavior than on direct buffers.

### Deserialization silently swallows exception, corrupting byte-position tracking (1 known bug)
Legacy SSTable reader catches exception inside loop; skipped bytes credited to next record's position counter.

### Variadic or positional argument swap in serialization call (1 known bug)
Two same-typed arguments swapped in format call, silently producing reversed serialized fields.

### Negative-index sentinel encoding mishandled at position zero (1 known bug)
Using `(-1 - x)` as a not-found sentinel conflates the not-found-at-zero case with valid positions.

### Role or identity derived from heuristic instead of explicit field (1 known bug)
Encoding role as a derived heuristic ("if X == 0, I must be the receiver") is fragile.

### Multi-branch size calculation omits a component in one path (1 known bug)
When `serializedSize()` has fast-path and slow-path branches, a component written in all paths may be counted in only one branch.

---

## Missing from §20: Resource Leaks — Specific Patterns

### close() / release called recursively or at wrong lifecycle point (1 known bug)
A `close()` method that performs both bookkeeping and native-resource release is called mid-operation, causing double-free or use-after-free.

### Fallback code path missing stateful transitions present in primary path (1 known bug)
A fallback code path mirrors a primary path but omits seek, flush, or close calls.

### abort() Not Overridden in Subclass That Adds Resources (1 known bug)
A subclass adds new Closeable resources but inherits the parent's `abort()`, which does not know about the new resources.

### Superclass Method Signature Change Silently Orphans Override (1 known bug)
A superclass changes a method signature and the old override silently becomes unreachable dead code without `@Override`.

### Iterator next() Result Not Closed on All Paths (1 known bug)
An iterator yields AutoCloseable items but `next()` results are passed directly to a consumer without try-with-resources.

### Half-open to closed range conversion missing +1 shift on upper bound (1 known bug)
Wrapping a `[a, b)` distribution API for a `[a, b]` range without shifting the upper bound silently excludes the maximum value.

### Resource ownership split between task body and lifecycle callback (1 known bug)
A resource owner submitted to an executor has close() in task body try/finally, but if the task is cancelled, cleanup never runs.

### Resource opened outside normal lifecycle tracking system (1 known bug)
A ref-counted resource opened from a non-standard path (snapshot, offline tool) bypasses tracking and is never closed.

### Double-close of shared AutoCloseable fires side-effects twice (1 known bug)
When both try-with-resources and caller close, side-effects (metric increments, counter decrements) fire twice.

---

## Missing from §12: Filesystem Paths — Specific Patterns

### Missing validation for schema feature combination or filesystem constraint (4 known bugs)
DDL does not check prerequisites: static columns require clustering columns, COMPACT STORAGE incompatible with collections, table name not validated against filesystem length.

### Wrong path, directory, or base resolution (3 known bugs)
SSTable written to wrong data directory (bare disk path, not keyspace/table), trigger directory resolved from JAR location.

### Name stripping / directory parsing fails on unexpected suffix (2 known bugs)
Stripping a file extension does not remove additional well-known suffixes (`-tmp`, `-old`), or a path-construction step omits a directory level.

### SSTable ordering by generation not enforced when filesystem order assumed (1 known bug)
A loop iterates `SSTableLister.list().entrySet()` without sorting, relying on undefined filesystem traversal order.

---

## State Cleanup — Resources-Domain Patterns (§19)

### Memory/heap size measurement miscounts (6 known bugs)
`memorySize()` omits subclass fields, double-counts shared singletons via jamm, uses `getSizeWithRef()` inside `getFieldSize()`, or relies on `sizeOfEmpty` constants for variable-length objects.

### Metric invisible to reporters, double-counted, stale at read time (4 known bugs)
Metric implemented as bare interface invisible to JMX exporters, chunk cache double-counted, disk-space metric computed at wrong time, or metrics library upgrade silently changes unit contract.

### Hinting code path not audited for non-idempotent mutation types (1 known bug)
A new mutation type (counter write) is added without auditing the hinting path, which attempts to hint a non-idempotent operation.

### Multiple related files not all flushed/synced before directory sync (1 known bug)
When a segment transition persists multiple related files, a missing flush on any one file leaves a partially-durable state.

### Early-Return Path Skips Cursor/Tracking State Update (1 known bug)
An early-return path inside an accumulation loop skips the normal "advance the cursor / update tracking state" step.
