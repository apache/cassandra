# Categories index

Quick-scan view of all categories with their diff signals. Use this in Phase 2 of the
workflow to decide which categories to load. After picking categories, read the full
file for each (e.g., `serialization-and-versioning.md`) in Phase 3.

Each category file follows the same structure:
1. `# Category: Name`
2. One-paragraph description
3. `## Diff signals (when to load this category)` — bullet list of patch shapes
4. `## Findings` — numbered findings, each with body and `**Look for:**` hint

---

## api-contracts-and-completeness

Bugs where adding a new field, type, event, or capability requires symmetrical updates across multiple sites — overrides, equality/hashCode, builders, dispatch tables, switch cases, register/deregister pairs — and one or more required updates are silently omitted, leaving the system structurally inconsistent.

**Diff signals:**
- A new field added to a class that has `equals`, `hashCode`, `toString`, `compareTo`, copy, clone, or builder methods
- A new enum constant or new subclass added (look for missing cases in `switch` statements or `instanceof` chains elsewhere)
- A new method added to an interface or abstract class (look for missing overrides in implementations)
- A new event/message/verb type registered (look for unhandled cases in dispatchers)
- New `register`, `addListener`, `subscribe`, `addCloseable`, `acquire`, or symmetric pair operations
- A new serializer/deserializer overload, signature change, or wire-format field
- New constructor parameter, builder method, or option that must be propagated to factory call sites
- Changes to a serialization size method, write method, or read method (the three must stay in sync)
- A `default` method in an interface that returns a hard-coded constant (subclasses may need to override)
- A new metric, management endpoint, sensor, or gauge registration (verify deregistration paths)
- A new field in a request/response/copy builder (verify it's copied in `copy`/`from`/`toBuilder` paths)
- New configuration property added to a class (verify it's forwarded through every constructor and factory)
- A subclass adds fields, resources, or dependencies (verify base-class lifecycle methods are overridden)

## boundaries-and-numbers

Bugs involving numeric arithmetic, integer overflow, off-by-one errors, unit/dimensional mismatches, buffer-bound violations, sentinel/special-value confusion, and floating-point pitfalls — wherever a number, index, or quantity is computed, compared, or interpreted incorrectly.

**Diff signals:**
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

## concurrency-and-locking

Bugs caused by races, ordering hazards, lock-discipline violations, publication-safety gaps, and circular waits between cooperating threads or async tasks.

**Diff signals:**
- New or changed `synchronized`, `Lock`, `ReadWriteLock`, `ReentrantLock`, `mutex`, semaphore, latch, or condition-variable usage
- `volatile`, `Atomic*`, `AtomicReference`, `compareAndSet`, `getAndSet`, or memory-fence-related changes
- `Future`, `CompletableFuture`, `await`, `get()`, `join()`, callbacks chained on async results
- A new background thread, executor submission, scheduler, or task pool — especially with bounded queues or single-threaded executors
- Changes to publish-then-signal sequences (set flag / count-down / notify / complete a future after writing shared state)
- Iteration over a shared/concurrent collection without explicit lock acquisition, or methods returning live views of shared collections
- Lazy or double-checked initialization of fields read from multiple threads
- Reference-counted resource lifecycle calls (`acquire`, `release`, `retain`, refcount inc/dec)
- Cancellation, abort, shutdown, or close paths that interact with in-flight work
- Reading "current state" / membership / counters as separate steps from acting on them (check-then-act, observe-then-acquire)
- Comparators or sort keys that read mutable fields, metrics, or time
- Atomic swap-then-drain, swap-then-clear, or replace-then-process patterns

## conditions-and-predicates

Bugs where a comparison, guard, predicate, or branching condition is logically wrong: wrong operator polarity, wrong constant or field, wrong direction flag, asymmetric vs symmetric comparator confusion, missing replay-time filter, conflated conditions, or a sentinel guard that tests the wrong variable.

**Diff signals:**
- New or modified comparison operators (`<`, `<=`, `>`, `>=`, `==`, `!=`) in a guard, loop condition, or boundary test
- A boolean expression that gates a side effect, branch, early return, or filter step
- An `equals()`, `hashCode()`, `compareTo()`, or `Comparator` implementation (added, modified, or its field set changed)
- A predicate method (`isXxx`, `hasXxx`, `canXxx`, `matches`, `contains`) added or modified
- A `switch` over an enum or type discriminator (especially with `default` branches or fall-through)
- A direction/order flag (ascending/descending, forward/reverse, oldest/newest) passed to a helper
- An error message or log statement that quotes a constant, threshold, or field name
- A composite key access where indexed positions or named components are read
- A `filter`, `where`, `predicate`, or `Predicate<T>` lambda added, removed, or re-targeted
- A type check via `instanceof`, `getClass() ==`, or a custom type-discriminator predicate
- Changes to membership tests (`contains`, `containsKey`, `Set.of`, allowlist/denylist)
- Sentinel value handling (`-1`, `null`, `Long.MAX_VALUE`, `EMPTY_BUFFER`) in a comparison or guard

## io-and-crash-safety

Bugs in durable persistence, flush/sync ordering, atomic file replacement, partial reads/writes, checksum handling, and journal/log replay where crashes, errors, or restarts can corrupt data, lose writes, or resurrect deleted state.

**Diff signals:**
- New or changed file writes, especially overwrite-in-place, rename/move, or `FileChannel.write` / `OutputStream.write` paths
- Calls to `fsync`, `force()`, `flush()`, `sync()`, `close()` on file/channel/writer objects, or removal/addition of any of these
- Use of `FileChannel.write`, `read`, `read(ByteBuffer)`, `InputStream.read(byte[])` without explicit "read fully" / "write fully" loops
- Checksum / CRC / digest computation tied to a file or record (`CRC32`, `Adler32`, `MessageDigest`, `Hashing`, custom digest update over a buffer)
- Commit-log, journal, write-ahead-log, replay, recovery, or checkpoint code paths
- Truncation, snapshot, or restore operations on files or persistent state
- Auto-closing serializer / try-with-resources around a writer that owns the file lifecycle
- Atomic-rename helpers (`Files.move`, `ATOMIC_MOVE`, `renameTo`, `replace`) or marker-file presence checks for operation completion
- Buffer pool / off-heap memory release coordinated with disk I/O completion
- Multiple related files written together (data + index, data + summary, manifest + segments)
- File-open flags such as `TRUNCATE_EXISTING`, `CREATE_NEW`, `APPEND`, `O_DSYNC`, or their absence

## lifecycle-and-ordering

Bugs caused by performing operations in the wrong sequence relative to a component's lifecycle: registering listeners after events fire, exposing services before initialization completes, tearing down dependencies in the wrong order, polling without deadlines, or assuming async work is done when it isn't.

**Diff signals:**
- New `register*`/`addListener`/`subscribe`/`addObserver` calls or changes to where they are placed
- Changes to constructor or `init()`/`start()` ordering, or new initialization phases
- Changes to `close()`/`shutdown()`/`stop()` ordering, drain loops, or teardown sequencing
- New management endpoint/metrics registration, or unregistration paths
- Polling loops (`while (!ready)`, `await*Until*`, retry-on-condition) without explicit deadlines
- New asynchronous tasks (`submit`, `schedule`, `Future`, `CompletableFuture`) that produce state observable elsewhere
- Conditional readiness flags, "is started/initialized/ready" guards, or status-broadcast methods
- Changes to bootstrap/join/handshake/handoff/quarantine sequences
- Background thread spawning (especially inside constructors or factory methods)
- Static initializers / class-load-time evaluation of configuration or singletons

## null-and-type-safety

Bugs where null-valued fields, return values, or unset optional fields are dereferenced without a guard, and bugs where a runtime type assumption (cast, instanceof, generic narrowing) does not hold for all values that actually flow through the code path.

**Diff signals:**
- A new `cast`, `(SomeType) x`, `instanceof`, or generics narrowing on a value that crosses an API boundary or comes from a polymorphic source.
- A new lookup result (`map.get`, `registry.lookup`, `find`, schema/metadata lookup, `socket.getChannel`, `File.list`) chained or dereferenced without a null guard.
- A new optional / nullable field added to a config, schema, protocol record, or wire format (especially "introduce nullable union", "make field optional", "schema evolution").
- New `Optional` usage (`.get()`, `.orElse(null)`, `orElseThrow`, eager-evaluated `orElse(...)` arguments).
- Changes to constructors / lifecycle (`close()`, `cleanup()`, `shutdown()`) where a field may be null on early-failure or never-initialized paths.
- Changes that move a null check, change `&&`/`||` connecting null-checks, or add/remove `containsKey` before `get`.
- Refactor that widens a return type to a supertype or moves a method onto a more abstract type while existing callers/fields keep the concrete type.
- New `toArray()`, `Collectors.toMap`, auto-unboxing of `Map.get`, or `Long`/`Integer` reference-equality (`==`) comparisons.
- A new factory or builder whose return type is wider/narrower than callers assume.

## refactor-aftermath

Bugs whose root cause is incomplete or inconsistent refactoring — leftover guards, stale references after a rename, narrowed/broadened types not propagated to all sites, dead branches preserved past their owning feature, deprecated overloads with stale defaults, mixed-version mismatches between the old and new shape, and parsers/regexes that handle only the new format.

**Diff signals:**
- A symbol rename, method rename, or package move where some call sites are still updated by hand.
- A new overload added next to an existing one (especially if the old one is `@Deprecated` or kept as a shim).
- A type narrowing or broadening (e.g., concrete → interface, primitive → wrapper, scalar → collection) on a public field/parameter/return type.
- A new parser, regex, or format reader that replaces a hand-rolled one (especially for paths, keys, identifiers, or wire formats).
- Removal of a feature flag, config option, or guard, with surrounding setup/teardown left intact.
- A serialization/wire-format version bump, new field added to one side only, or a digest/identity computation that changed shape.
- Lambdas added inside hot loops where the surrounding class already exposes a context object.
- A call site where the new code is structurally similar to a sibling/copy in another module that was the original target of the fix.
- An `equals`/`hashCode`/comparator touched after fields are added or renamed.
- Tests, callers, or override hooks that still reference an old method name or old field set.

## serialization-and-versioning

Bugs in encoding/decoding symmetry, wire and on-disk formats, version-gated paths, schema evolution, mixed-version compatibility, and switch- or enum-based decode dispatch.

**Diff signals:**
- A `serialize`, `deserialize`, `serializedSize`, `read`, or `write` method, or any pair of these together
- A `switch` or `if/else if` ladder that dispatches on a type tag, enum, or kind discriminator
- A version comparison: `if (version >= ...)`, `protocolVersion`, `apiVersion`, `formatVersion`, `MessagingService.VERSION_*`, or any version-gated branch
- An enum used as a wire-protocol or on-disk discriminator (especially with explicit ordinal/code values, or `values()[i]` decoding)
- A `ByteBuffer` operation: `array()`, `arrayOffset()`, `position()`, `slice()`, `duplicate()`, `flip()`, `rewind()`, or any relative read/write
- A length prefix (`writeInt`, `writeShort`, `writeUnsignedVInt`) followed/preceded by a payload write/read
- A digest or checksum computation over fields, buffers, or sub-records (`MessageDigest.update`, `XXHash`, `CRC32`)
- Schema-evolution markers: nullable→non-nullable changes, new fields added to records, `taggedFields`, `nullableVersions`, `flexibleVersions`, `ignorable`
- Auto-generated message class changes (Avro, Thrift, Kafka JSON message protocol, Protobuf)
- A "compatibility", "legacy", "pre-X.Y", or "old format" code path
- Header / context / metadata propagation through wrapper serializers (`Headers headers` arg added or removed)
- `getBytes()` / `String.getBytes()` / charset-related calls in a serialization context
- Charset, locale, or byte-order assumptions (`UTF_8`, `Locale.getDefault`, `ByteOrder.nativeOrder`)
- A deserializer constructor that takes raw bytes plus a type tag, kind flag, or context object

## state-and-resource-cleanup

Stale state, leaked references, and orphaned resources that arise when teardown, cancellation, removal, or replay paths fail to fully restore the system to a clean baseline.

**Diff signals:**
- `close()`, `release()`, `unref()`, `decrementReferenceCount()`, `dispose()`, `shutdown()` calls
- `try`/`finally`, `try-with-resources`, or `AutoCloseable` introduction or removal
- Cancellation, abort, timeout, rejection, or error-path branches that release/return resources
- `clear()`, `remove()`, `deregister()`, `unregister()`, `retainAll()`, `evict()` on long-lived collections, registries, caches, or maps
- Tombstone, purge, GC, expiry, eviction, retention, or compaction predicates
- Counter increment/decrement pairs, latches, semaphores, in-flight counters, pending queues
- Replay, restore-from-log, snapshot loading, or restart bootstrap of in-memory state
- New background threads, periodic tasks, scheduled futures, or executors with lifecycle management
- Reference counting, off-heap buffer management, native handle ownership
- Listener/callback registration without matching deregistration in lifecycle hooks
- New or renamed lifecycle methods (`onLeave`, `onRemoved`, `onDelete`, `dispose`, `cleanup`)
- Iterator/cursor/stream code that may be returned past its enclosing scope

## validation-and-input-handling

Bugs where input validators, parsers, format detectors, and pre-conversion guards either miss cases, anchor on the wrong delimiter, fail to account for downstream length/charset/OS limits, or skip pre-use checks (null, -1, empty buffer, length, type) before consuming a value.

**Diff signals:**
- New or modified `validate*`, `check*`, `verify*`, `assert*`, `isValid*`, `parse*`, `tokenize*`, `decode*` methods
- Calls to `indexOf`, `lastIndexOf`, `String.split`, `Pattern.compile`, `Matcher`, `String.replace*`, `substring`, `startsWith`/`endsWith`, `getBytes`/`new String`
- Construction or modification of regex character classes, anchors, escapes, or quantifiers
- File path / URI / address parsing: `getFileName`, `Paths.get`, `URI`, `InetAddress`, `InetSocketAddress`, host:port splitting, IPv4/IPv6 literals
- Functions appending suffixes/prefixes (`-`, `_`, UUID, extension, port) before passing names to filesystem, network, or registry calls
- Code that consumes an `Optional`, nullable lookup, or map `get()` result without `.isPresent()`/`!= null`/`getOrDefault`
- Conditions calling `indexOf(...)` then `substring(...)` without a `-1` guard
- Pre-conversion / pre-deserialization filters that drop or pass through unsupported attribute types, sentinels, or empty values
- New constraint, length, charset, locale, or boundary checks; or removal of an existing one
- Conversion of user-supplied identifiers / option strings / config values to typed objects (enum, UUID, BigDecimal, Date)
- Length, capacity, or quantity bounds that interact with appended fixed-size data (suffixes, length prefixes, framing headers)

