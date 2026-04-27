# Deep Resources & Serialization — Extended Checklist
---

## Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Serialization contract**: Read both `serialize()` and `deserialize()` end-to-end. List every field.
2. **Version gates**: What protocol/format versions exist? Which fields are gated?
3. **Resource ownership**: Who creates, who closes? Is ownership transferred?
4. **Error paths**: Trace every exception exit. What resources are held at each exit?
5. **Lifecycle**: When are resources acquired and released? What cleanup runs on shutdown?

---

## Serialization Depth

### Field count/order mismatch (weight: high)
-  Compare `serialize()`, `deserialize()`, and `serializedSize()` line by line. Do they write, read, and measure the SAME fields in the SAME order under the SAME conditional guards?
-  Is a field present in `serialize()` but missing from `serializedSize()` (or vice versa)?
-  Is a `sizeof()` result computed but never accumulated (dead code)?
-  Is a length field missing from the symmetric read/write contract?

### Wire format mismatch (weight: high)
-  Does custom payload use `byte[]` where protocol specifies `ByteBuffer`?
-  Are column names encoded as ASCII instead of UTF-8?
-  Is Thrift wire format assumed when native protocol is in use (or vice versa)?
-  Does nodetool use `String.getBytes` instead of the key validator?

### Empty/null buffers (weight: high)
-  Can an empty buffer reach a serializer validate method?
-  Can a zero-length value enter code assuming non-empty content?
-  Does the code handle UNSET sentinel values alongside null?

### Wrong stream/variable (weight: high)
-  After creating a wrapper stream (tee, counting, checked): does ALL subsequent I/O use the WRAPPER?
-  Is the original field used instead of a locally transformed variable?
-  Is `writableBytes()` called where `readableBytes()` is needed?

### Version compatibility (weight: high)
-  Does the serializer restrict to current version only, breaking rolling upgrades?
-  Has an enum ordinal been renumbered by removing middle constants?
-  Do new schema columns break older peers?
-  Is the digest computation version-aware?

### Nullable dereferencing (weight: high)
-  Can individual UDT fields be absent?
-  Can frozen collection elements be null?
-  Can bind values be null or UNSET?
-  Is a method return value discarded where it should replace the parameter?

### Checksum/digest range (weight: medium)
-  Does the CRC cover ALL serialized fields (including newly added ones)?
-  Is `ByteBuffer.duplicate()` taken before write (not after)?
-  Is the digest computation version-aware across upgrade boundaries?

### Collection serialization order (weight: medium)
-  Are frozen maps serialized via `SortedMap` with correct type comparator?
-  Are JSON map keys wrapped in strings?

### VInt signed/unsigned (weight: low)
-  Is `computeVIntSize` used where `computeUnsignedVIntSize` is needed (or vice versa)?

### Length prefix width (weight: low)
-  Is a 4-byte length prefix written but 2-byte prefix expected on deserialization?

### Enum deserialization (weight: low)
-  Does enum-switched deserialization preserve a previously assigned field for a case where it should be absent?

---

## Resource Leak Depth

### Exception mid-sequence (weight: high)
-  When multiple AutoCloseable resources are acquired in sequence: if the second allocation fails, is the first released?
-  Is there a try/catch or try-with-resources chain guarding stepwise allocation?

### Loop-built closeables (weight: medium)
-  When Closeable resources are built in a loop: if an exception occurs partway, are already-created resources closed?
-  Is each individual `release()` in a cleanup loop independently try/caught?

### Double-counting/release (weight: low)
-  Is a resource counter incremented/decremented twice for the same event?
-  Is a "free" operation using a pre-mutation delta?

### Close side effects (weight: low)
-  Does `close()` trigger side-effecting callbacks without an idempotency guard?
-  Could close fire twice (once from try-with-resources, once from caller)?

### Short-circuit cleanup (weight: low)
-  Does a cleanup loop stop at the first exception instead of continuing to release all?
-  Is each cleanup in the loop independently guarded?

### Ref-counted asymmetry (weight: low)
-  Is a reference count + lifecycle transaction released as a pair on error?
-  Does one half leak on error paths?

### View returned to pool (pattern)
-  When a `ByteBuffer` is obtained via `nioBuffer()`, `slice()`, `duplicate()`: is the ORIGINAL returned to the pool?
-  Does the pool receive a view it can't recycle?

### Filtered-out items (weight: low)
-  When an iterator of closeable objects is filtered: are filtered-out items closed?

### Metric registered without deregistration (weight: low)
-  Does every `addMetric` have a matching `removeMetric` in `close()`?

---

## Lifecycle & Configuration Depth

### Config not threaded (weight: high)
-  Is a config knob parsed but never threaded to the builder?
-  Is `null` passed where config should flow?
-  Are streaming encryption settings dropped?

### Throwable gotchas (weight: medium)
-  Can `getMessage()` return null (no-arg exception constructor)?
-  Can `getCause()` be null (no chained cause)?
-  Can `getStackTrace()` return zero elements (`writableStackTrace=false`)?

### Success path missing cleanup (weight: medium)
-  Does the success path miss cleanup that the error path has?
-  Is session removal, counter decrement, or map eviction only on the error branch?

### State persisted too early (weight: medium)
-  Is persistent state written when an operation starts rather than when it commits?
-  Could a crash leave stale state that prevents retry?

---

## File I/O & Crash Safety Depth

### File write modes (specific patterns)
-  `WRITE | CREATE` without `TRUNCATE_EXISTING` on pre-existing file: old content past new EOF remains.
-  `Files.copy()` without `REPLACE_EXISTING` in code that can run twice.
-  Jackson `writeValue(OutputStream)` auto-closes: is write-to-temp, fsync, rename, fsync-parent used instead?

### Multi-file atomicity (pattern)
-  When multiple related files are persisted: are ALL flushed and synced before the directory sync?
-  Is there a missing flush on any one file in the set?

### Correlated state changes (pattern)
-  Are multiple correlated state changes announced as separate operations instead of bundled atomically?

---

## Metrics & Observability Depth

### Metric timing (weight: high)
-  Is a metric updated before the operation it measures?
-  Is it after a different operation?
-  Is it outside the guard that controls the measured operation?

### Progress counter (weight: low)
-  Does progress increment per inner-loop page instead of per outer logical unit?
-  Is the count off by orders of magnitude for wide rows?

---

## Platform & Encoding Depth

### Locale/charset (weight: medium)
-  Is `DecimalFormat` used without `Locale.ROOT`?
-  Is `String.getBytes()` called without explicit charset?
-  Does `Boolean.valueOf()` need to be `parseBoolean()`?

### I/O stream (specific patterns)
-  Does `InputStream.read()` return raw signed byte without `& 0xFF`?
-  Is `EINTR` handled in blocking system calls?
-  Is `MessageDigest` shared across threads without synchronization?

---

## Filesystem Path Depth

### Path comparison (weight: medium)
-  Is `String.startsWith()` used for path containment instead of `Path.startsWith()`?
-  Do symlinks break path comparisons?

### Wrong directory level (weight: medium)
-  Does `directory.getName()` return the wrong component after hierarchy changes?
-  Is a directory level omitted in path construction?

### Layout migration (weight: low)
-  Does a directory layout change include migration of existing data from old location?
-  Are files unreachable through the new path abstraction?

---

## Missing from §18: Serialization — Specific Patterns

### Transient or non-serializable field in Serializable class (weight: medium)
Transient field set only in constructor silently null after deserialization, or third-party library types fail remote deserialization.

### Validation delegated to wrong serializer, wrong validator package, or bypassed entirely (weight: medium)
Nested tuple/UDT validation routed to BytesSerializer instead of type-aware serializer, collection validate() called from legacy CQL2 package.

### Endianness or byte-order not accounted for in bit-shift arithmetic (weight: low)
ByteBuffer.getLong feeds into bit-shift arithmetic assuming big-endian on little-endian buffers, or unsafe put/get does not account for BIG_ENDIAN on s390x.

### Protocol field order violation (encode vs decode sequence mismatch) (weight: low)
Encode writes flags before consistency level while decode reads them in opposite order, or paging state buffer not flipped before being read after write.

### Deserialization-order sensitivity not enforced (drain instead of throw) (weight: low)
A streaming deserializer throws `IllegalStateException` when a caller short-circuits iteration, instead of draining unconsumed items.

### ByteBuffer heap vs direct path divergence (weight: low)
VectorCodec deserialization using slice() and rewind() on heap buffers produces different behavior than on direct buffers.

### Variadic or positional argument swap in serialization call (weight: low)
Two same-typed arguments swapped in format call, silently producing reversed serialized fields.

### Negative-index sentinel encoding mishandled at position zero (weight: low)
Using `(-1 - x)` as a not-found sentinel conflates the not-found-at-zero case with valid positions.

### Role or identity derived from heuristic instead of explicit field (weight: low)
Encoding role as a derived heuristic ("if X == 0, I must be the receiver") is fragile.

### Multi-branch size calculation omits a component in one path (weight: low)
When `serializedSize()` has fast-path and slow-path branches, a component written in all paths may be counted in only one branch.

---

## Missing from §20: Resource Leaks — Specific Patterns

### close() / release called recursively or at wrong lifecycle point (weight: low)
A `close()` method that performs both bookkeeping and native-resource release is called mid-operation, causing double-free or use-after-free.

### Fallback code path missing stateful transitions present in primary path (weight: low)
A fallback code path mirrors a primary path but omits seek, flush, or close calls.

### abort() Not Overridden in Subclass That Adds Resources (weight: low)
A subclass adds new Closeable resources but inherits the parent's `abort()`, which does not know about the new resources.

### Superclass Method Signature Change Silently Orphans Override (weight: low)
A superclass changes a method signature and the old override silently becomes unreachable dead code without `@Override`.

### Iterator next() Result Not Closed on All Paths (weight: low)
An iterator yields AutoCloseable items but `next()` results are passed directly to a consumer without try-with-resources.

### Half-open to closed range conversion missing +1 shift on upper bound (weight: low)
Wrapping a `[a, b)` distribution API for a `[a, b]` range without shifting the upper bound silently excludes the maximum value.

### Resource ownership split between task body and lifecycle callback (weight: low)
A resource owner submitted to an executor has close() in task body try/finally, but if the task is cancelled, cleanup never runs.

### Resource opened outside normal lifecycle tracking system (weight: low)
A ref-counted resource opened from a non-standard path (snapshot, offline tool) bypasses tracking and is never closed.

### Double-close of shared AutoCloseable fires side-effects twice (weight: low)
When both try-with-resources and caller close, side-effects (metric increments, counter decrements) fire twice.

---

## Missing from §12: Filesystem Paths — Specific Patterns

### Missing validation for schema feature combination or filesystem constraint (weight: medium)
DDL does not check prerequisites: static columns require clustering columns, COMPACT STORAGE incompatible with collections, table name not validated against filesystem length.

### Wrong path, directory, or base resolution (weight: medium)
SSTable written to wrong data directory (bare disk path, not keyspace/table), trigger directory resolved from JAR location.

### Name stripping / directory parsing fails on unexpected suffix (weight: low)
Stripping a file extension does not remove additional well-known suffixes (`-tmp`, `-old`), or a path-construction step omits a directory level.

---

## State Cleanup — Resources-Domain Patterns (§19)

### Metric invisible to reporters, double-counted, stale at read time (weight: medium)
Metric implemented as bare interface invisible to monitoring exporters, chunk cache double-counted, disk-space metric computed at wrong time, or metrics library upgrade silently changes unit contract.

### Hinting code path not audited for non-idempotent mutation types (weight: low)
A new mutation type (counter write) is added without auditing the hinting path, which attempts to hint a non-idempotent operation.

### Early-Return Path Skips Cursor/Tracking State Update (weight: low)
An early-return path inside an accumulation loop skips the normal "advance the cursor / update tracking state" step.

---

## Additional Patterns from Multi-Project Corpus

### Shared ByteBuffer position side-effect on read (weight: high)
-  Is a ByteBuffer obtained from a static singleton, cached entry, registry map, method parameter, or cross-thread queue `duplicate()`d before ANY relative `get()`, `getInt`, `slice()`, `get(byte[])`, comparator read, or checksum update?
-  Is a no-argument relative `get()` used in a comparator that can be called repeatedly on the same shared key?
-  Does a deserialization helper return a reference to a shared singleton buffer whose position the caller will advance?
-  Does a digest / array copy from a heap ByteBuffer use `array()` + `position()` without ALSO adding `arrayOffset()`? Sliced heap buffers silently produce wrong digests.

### Buffer flip vs rewind / channel read completeness (weight: medium)
-  After writing into a ByteBuffer, is `flip()` called (not `rewind()`)? `rewind()` leaves limit = capacity and exposes uninitialized bytes past the write point.
-  Is a channel `read()` return value checked (>=0) before `flip()` + `get()`? Closed-channel reads return 0 and cause `BufferUnderflowException`.
-  Is a write buffer flipped before being returned from a `serialize(...)` helper (otherwise position == end and no bytes are readable)?

### serializedSize double-count / variant confusion (weight: medium)
-  Does `serializedSize()` delegate to the on-disk / format-specific `serializedSize` variant, not the in-memory variant, when the output is on-disk or wire format?
-  For variable-length fields: does the size include BOTH the length prefix AND the encoded bytes?
-  Is `serializedSize()` updated after every format change, or does it still describe the prior format?
-  Is a field's byte count added unconditionally AND re-added inside a conditional branch?
-  Is the same `size()` result reused for both the total-record size and an embedded sub-record length prefix when the two require different values?

### Reference count not acquired before access (weight: medium)
-  Is a ref-counted resource's counter incremented BEFORE it is read (not after the null check, not after `containsKey`, not after `get`)?
-  Does a CAS-deserialize path acquire the ref while holding the map slot, not after the atomic read?
-  Does a read-only scan (cleanup scan, cardinality estimate, diagnostic dump) acquire a ref before traversing backing files?
-  Is an "observe-then-acquire" retry loop aware that the resource can be freed between the two steps?

### Close-on-shared-dependency / composite close (weight: medium)
-  Does `close()` release EVERY Closeable field it holds, not just the primary? Audit for omitted delegates when fields were added.
-  If `close()` shuts down a shared static executor / connection pool / registry, is this instance the owner? Non-owning close corrupts sibling instances.
-  Does a composite appender / container define its own `stop` / `close` that orders children explicitly rather than relying on child-cascade shutdown?

### Cancellation / early-return / partial-construction resource loss (weight: medium)
-  If the constructor fails mid-sequence, does `close()` tolerate null fields (no NPE during best-effort cleanup)?
-  Is a field opened in the constructor assigned to the owner field BEFORE any throw / early return?
-  Does the submit-after-shutdown-check race release resources held by the partially-constructed task when submit throws `RejectedExecutionException`?
-  Are resources held in a cancellation / exception-handler callback released even when the task body's try / finally never runs?
-  Is an iterator returned to a caller still owning its Closeables, or was it wrapped in try-with-resources whose `close` fires before the caller reads?

### File writer durability / partial-file-as-complete (weight: low)
-  Does a `BufferedWriter` or serializing writer call `flush()` + `fsync()` before `close()` — not just `close()`?
-  Is an error path guaranteed to call `abort()` on the writer so a partial file is distinguishable from a complete one on restart?

### Background task in constructor / static-init / short-lived entrypoint (weight: medium)
-  Does a constructor start a background thread that might observe partially-constructed state (non-final fields read on the thread before init completes)?
-  Does a CLI entry point or short-lived tool call a process-exit helper to reap non-daemon threads before returning?
-  Is a periodic task submitted from a `static` initializer that may fire before its target executor is started?
-  Is there a strong reference cycle (long-lived owner ↔ scheduled task / leak-detector callback) that prevents garbage collection?

### Re-arming / reschedule condition wrong (weight: medium)
-  Is the re-arm decision driven by "did work occur" rather than by a transient collection being non-empty or by a side-effecting method's return value?
-  For a one-shot scheduled refresh: does it reschedule itself after each successful execution?

### Config resolved at construction / cached with no fallback (weight: medium)
-  Is a config value resolved lazily per target (supplier / factory), not frozen at construction from a global context where per-target overrides were not yet applied?
-  When the underlying config source (file, secret manager) is temporarily unavailable, does the resolver fall back to the last-known-good cached value rather than throw?
-  Is a `static final` captured at class-load time from a subsystem that the caller must configure first?

### Close semantics: null means both closed and not-yet-initialized (weight: low)
-  Does a lazy wrapper that clears its delegate to null on close also cause post-close calls to silently re-initialize rather than fail?
-  Distinguish "closed" from "not yet initialized" via an explicit state flag.

### Management endpoint rename without alias (weight: low)
-  Does a rename of a registered metric or management endpoint also register the prior name as a deprecated alias?
-  Existing monitoring clients otherwise silently break.

### Transient or non-serializable field in Serializable class (weight: low)
-  Does a field declared `transient` or backed by a non-serializable library type (ByteBuffer, Guava collection, native handle) survive Java serialization, or is it silently null on the far side?
-  Are dependencies captured in a holder class dropped during serialization, deferring the failure to first use post-deserialization?

### Metric present on one branch, missing on sibling (weight: low)
-  When a metric is recorded on the success (or timeout) path, is an equivalent recording also present on the paired failure (or success) path?
-  Is a metric recorded at the moment the measured condition first holds, rather than inside a timeout callback that fires later?
