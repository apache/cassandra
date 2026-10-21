# Category: Null and Type Safety

Bugs where null-valued fields, return values, or unset optional fields are dereferenced without a guard, and bugs where a runtime type assumption (cast, instanceof, generic narrowing) does not hold for all values that actually flow through the code path.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- A new `cast`, `(SomeType) x`, `instanceof`, or generics narrowing on a value that crosses an API boundary or comes from a polymorphic source.
- A new lookup result (`map.get`, `registry.lookup`, `find`, schema/metadata lookup, `socket.getChannel`, `File.list`) chained or dereferenced without a null guard.
- A new optional / nullable field added to a config, schema, protocol record, or wire format (especially "introduce nullable union", "make field optional", "schema evolution").
- New `Optional` usage (`.get()`, `.orElse(null)`, `orElseThrow`, eager-evaluated `orElse(...)` arguments).
- Changes to constructors / lifecycle (`close()`, `cleanup()`, `shutdown()`) where a field may be null on early-failure or never-initialized paths.
- Changes that move a null check, change `&&`/`||` connecting null-checks, or add/remove `containsKey` before `get`.
- Refactor that widens a return type to a supertype or moves a method onto a more abstract type while existing callers/fields keep the concrete type.
- New `toArray()`, `Collectors.toMap`, auto-unboxing of `Map.get`, or `Long`/`Integer` reference-equality (`==`) comparisons.
- A new factory or builder whose return type is wider/narrower than callers assume.

## Findings

### F-01: Method on field that can be absent from config file
A method (`length`, `toString`, `iterator`) is called directly on a configuration field that is null when the user omits it from the config file. Some call sites null-check the field but others do not.
**Look for:** `config.someField.xxx()` where `someField` has no default initializer in the config class — search the class for any sibling site that does `if (config.someField != null)`.

### F-02: Map.get / registry lookup result dereferenced without null check
Result of a `map.get(...)` or registry/metadata/schema lookup is chained or passed to a downstream method that immediately dereferences it; the lookup can legitimately return null for unknown, removed, or not-yet-initialized keys.
**Look for:** `something.lookup(x).field` or `map.get(k).method(...)` with no `if (... != null)` between them; especially in startup, recovery, streaming, and topology paths.

### F-03: Field nulled between check and use (DCL / TOCTOU)
A nullable field is read into a local with a null check, then re-read directly (instead of the local) for the dereference, allowing a concurrent writer to clear it between reads. Variant: `volatile Optional` checked with `isPresent()` then `get()`.
**Look for:** `if (this.x != null) this.x.foo()` (rather than `var x = this.x; if (x != null) x.foo()`); also `opt.isPresent() && ... opt.get()` on volatile / shared fields.

### F-04: Null guard's closing brace misplaced — dependent code falls outside
A method call requiring the guarded value to be non-null sits one line below the closing brace of the `if (x != null) { ... }` block, executing unconditionally.
**Look for:** Statement immediately after a null-guarded block that still references the guarded variable; review brace alignment in any patch that touches a null guard.

### F-05: Factory return value cast to unsupported subtype
A factory method's return is unconditionally cast to a specific subtype, but at runtime the factory may return a different implementation (e.g., a different replication strategy, partitioner, or visitor-variant subclass).
**Look for:** `(ConcreteSubtype) factory.create(...)` or `(ConcreteSubtype) field` where `field`/return type is the abstract supertype; check whether other implementations exist in the same hierarchy.

### F-06: Range / value constructed with concrete key type where abstract position type expected
A constructor for a range or container is invoked with a concrete key type, and a downstream cast expects the abstract position/bound type. The runtime cast throws on the concrete value.
**Look for:** `new Range<ConcreteKey>(...)` flowing into code that does `(Position) range.left` or similar; verify generic parameter narrowing is consistent end-to-end.

### F-07: Array field declared with concrete type post-refactor expects abstraction
An array or collection field declared with a concrete element type was valid before a refactor; afterwards code expects a broader abstract element type and casts fail on the legacy concrete values.
**Look for:** `ConcreteImpl[] field` with usage code that does `(AbstractType) field[i]` or stores values via an abstract-typed setter.

### F-08: Nullable union/optional field deserialized without null check
A schema-evolved record's nullable union (or thrift/protobuf optional) field is dereferenced (often `.toString()` or method call) when older senders omit it, throwing NPE on receipt.
**Look for:** Newly-introduced `nullableVersions`, optional thrift field, or Avro union with null; any read site that calls a method on the field without `isSet`/null check.

### F-09: Avro union field: null default requires null as first union member
An Avro union field declared with a `null` default has the non-null type listed first; the Avro spec requires `null` to be the first member when it is the default, so deserialization fails.
**Look for:** Avro schema definitions where the union is `["string", "null"]` with `"default": null`; the correct form is `["null", "string"]`.

### F-10: Conditionally-initialized field guarded only by the original flag
Fields initialized only when a feature flag is true are dereferenced later guarded only by the same flag. Toggling the flag at runtime (or hot reload) leaves fields null while the flag is false.
**Look for:** `if (feature) field.method()` where `field = ...` only inside a parallel `if (feature)` constructor branch; check if the flag is dynamically toggleable.

### F-11: Lazy-initialized field accessed concurrently without synchronization
Lazy-initialized fields are accessed on multiple threads without happens-before; one thread observes a null intermediate state during another's initialization.
**Look for:** Non-volatile field with single-checked lazy init; or `compareAndSet` initialization where readers do not retry.

### F-12: Close() / cleanup() dereferences uninitialized fields after partial construction
A constructor throws midway, leaving some fields null; the `close()` / `cleanup()` / `shutdown()` path dereferences them unconditionally, throwing NPE that masks the original error.
**Look for:** Field assignments late in the constructor; matching `close()` that calls `field.close()` without `if (field != null)`.

### F-13: Lazy resource initialized only when data arrives, close called unconditionally
A resource is allocated only when at least one record is processed; `close()` always calls a method on it. Empty input → NPE in close.
**Look for:** Field assignment guarded by data presence (`if (firstBatch) { field = ... }`); paired close that does not guard.

### F-14: Auto-unboxing Map.get returning null
A primitive variable receives `map.get(key)` for a `Map<K, Long>`/`<K, Double>`; absent key returns null and unboxing throws NPE. Often paired with arithmetic (`/=`, `+`).
**Look for:** `long x = someMap.get(...)`, `int y = otherMap.get(...)`; switch to `getOrDefault` or `containsKey` guard.

### F-15: Collectors.toMap with null values
`Collectors.toMap` rejects null values with NPE; legitimate "no value yet" entries crash the collection.
**Look for:** `.collect(Collectors.toMap(...))` where the value mapper can return null; replace with `HashMap` + `put`.

### F-16: Optional.orElse with side-effect-bearing argument
`Optional.orElse(expensiveCall())` evaluates the argument eagerly even when the Optional is present, causing double processing or unintended side effects.
**Look for:** `.orElse(...)` where the argument calls a constructor, fetcher, or any non-trivial expression; prefer `orElseGet`.

### F-17: Optional.get() without isPresent
`.get()` called on an Optional without `isPresent()` (or via `orElse(null)` followed by an unguarded dereference). Common with `Optional`-wrapped timeouts.
**Look for:** `.get()` on Optional or `orElse(null)` and immediate `.method()`.

### F-18: Polymorphic loop with unconditional downcast
A loop iterates a heterogeneous collection and casts each element to one specific subtype; introducing a second registered subtype throws ClassCastException at runtime.
**Look for:** `for (Item it : items) { ((Concrete) it).foo() }` where `items` element type is a sealed/sub-typed interface; verify all subtype instances are handled.

### F-19: Downcast before instanceof guard
A type-narrowing downcast is performed before the type-guard condition that would justify it; a non-matching object throws CCE before the guard runs.
**Look for:** `Concrete c = (Concrete) x; if (x instanceof Concrete) { ... }` — reorder to test first.

### F-20: instanceof tests parent type, cast targets subtype
`instanceof Parent` guards a cast `(Subtype) x`; passing a Parent that is not the Subtype satisfies the guard but throws CCE on cast.
**Look for:** Mismatch between the type in `instanceof` and the type in the subsequent cast.

### F-21: Collection.toArray() returns Object[] cast to typed array
`stream.toArray()` (no-arg) or `Collection.toArray()` returns `Object[]`; casting to `T[]` throws CCE at runtime.
**Look for:** `(T[]) coll.toArray()` or `(T[]) stream.toArray()` — should use `toArray(T[]::new)` or pass a typed array.

### F-22: Reference equality on boxed numeric types
`==` comparison between two `Long`/`Integer` boxes (e.g., from autoboxing or `Map.get`) holds only inside the JVM integer cache range; outside that range every comparison is false.
**Look for:** `==` between two non-primitive numeric typed expressions; replace with `.equals()`.

### F-23: TreeSet / sorted collection over non-Comparable elements
Constructing a `TreeSet` / `TreeMap` (or any sorted structure) without a comparator over elements that don't implement `Comparable` compiles but throws CCE on the second insertion. Variant: passing a comparator from one type hierarchy to a container of another.
**Look for:** `new TreeSet()` / `new TreeMap()` without a comparator argument and elements lacking a known Comparable contract.

### F-24: Decorator wrapper not unwrapped before instanceof / cast
`instanceof` or cast applied directly to a wrapped/decorated type (e.g., reversed-order column type, frozen-collection wrapper) without first unwrapping; check returns false or cast throws.
**Look for:** `x instanceof Concrete` or `(Concrete) x` on a value known to be wrapped by a `ReversedType`/`FrozenType`/decorator pattern.

### F-25: Filesystem / Socket API returning null treated as collection
`File.listFiles`, `File.list`, `Path.toFile().list`, or `Socket.getChannel()` returns null on error or non-NIO socket; result is iterated with for-each or passed to a constructor that does not accept null.
**Look for:** `for (File f : dir.listFiles())` or `new X(socket.getChannel())` with no null guard.

### F-26: Lookup that may return null fed into addSuppressed / collection that rejects null
A nullable lookup result is added to a futures list, suppressed-exception chain, or null-rejecting collection; iteration or aggregation later NPEs.
**Look for:** `futures.add(maybeNull)` followed by `futures.forEach(f -> f.get())`; `addSuppressed(null)`.

### F-27: Field marked transient with no readObject reconstruction
A `Serializable` class marks a field `transient` but has no custom `readObject` — the field is silently null after deserialization, causing NPEs at runtime.
**Look for:** `transient` modifier on a field used unconditionally after deserialization; check for missing `readObject`.

### F-28: Static field used in earlier static initializer
A static field referenced inside another static field's initializer is declared after it; JVM static initialization order leaves it null when the dependent initializer runs.
**Look for:** `private static final X X1 = X2.something()` where `X2` is declared lower in the same class — reorder, or initialize in `<clinit>`.

### F-29: Singleton accessed during early startup before populated
A static `current()` singleton is invoked during early-startup code that runs before the singleton is set up; `current()` returns null and dereferences NPE.
**Look for:** `Foo.current().something()` invoked from constructors, class loading, or pre-initialization paths.

### F-30: Empty buffer / zero-length array deserialized without guard
A deserializer constructs a value type from a raw byte array without checking emptiness; the constructor throws on an empty array. Variant: `toString()` not null-guarded and NPEs when deserialize returns null.
**Look for:** `new ValueType(bytes)` or `deserialize(buf)` followed by unguarded method call; verify empty/zero-length input is handled.

### F-31: Missing override after method signature change drifts subclass to dead code
A subclass method becomes unreachable when the superclass signature changes because no `@Override` annotation flagged the drift; runtime calls dispatch to the wrong implementation, often producing null returns or wrong types downstream.
**Look for:** New parameter or different return type on a base method; subclass implementations missing `@Override`; verify subclass signature matches.

### F-32: Visitor / dispatcher missing case for new variant
A visitor or switch over a polymorphic/enum hierarchy adds a new variant in one place but a sibling visitor/handler returns null or falls through to a default that crashes the caller.
**Look for:** New enum constant or new `Type` subclass; search for switches/visitors handling its siblings and check the new value is covered.

### F-33: Map keyed by wrapper type, queried with byte[] / different type
A map keyed on a wrapper type is queried with a raw byte[] / different wrapper; lookup uses reference equality and always returns null. Variant: `Map.get(Object)` accepts wrong-type silently.
**Look for:** `map.get(rawBytes)` where map's key type is `ByteBuffer`/`Wrapper`; `Map.get` of a value not of K's type.

### F-34: Generic type parameter narrowed by inference, breaking metric / mock layer
A lambda registered without explicit generic witness is inferred as a wider wrapper type; downstream consumer boxes it incorrectly and throws CCE. Variant: mock serializer typed `byte[]` versus interceptor expecting `String`.
**Look for:** `register(x -> someValue)` where `x` is a generic and the inferred type is wider than callers expect; consider explicit type witness `register(this.<Long>foo(...))`.

### F-35: Empty-collection vs null-collection conflated
Some callers treat empty and null as equivalent absence; others distinguish them. A null sneaks into code that checks emptiness via `.isEmpty()` and NPEs, or vice versa. Variant: cache returns null for absent vs empty for "no data", and downstream cache treats them inconsistently.
**Look for:** `isEmpty()` calls on values that other call sites null-check; conflicting documentation about null vs empty.

### F-36: Cast inside a sort comparator on sparse / null cell values
A sort comparator dereferences cell values without null guard; sparse rows trigger NPE.
**Look for:** `comparator.compare(a, b)` body that calls `.field()` on `a` or `b` without checking null; especially in `Collections.sort` of objects with optional fields.

### F-37: equals() / hashCode() omits new field after refactor
Equality comparison ignores a recently-added field, so two objects differing only in that field compare equal. Variant: equals checks only non-null instead of correct type.
**Look for:** `equals` / `hashCode` methods that have not been updated after a field added; compare against the constructor field list.
