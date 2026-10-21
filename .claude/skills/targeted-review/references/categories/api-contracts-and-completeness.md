# Category: API Contracts and Completeness

Bugs where adding a new field, type, event, or capability requires symmetrical updates across multiple sites — overrides, equality/hashCode, builders, dispatch tables, switch cases, register/deregister pairs — and one or more required updates are silently omitted, leaving the system structurally inconsistent.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
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

## Findings

### F-01: New field omitted from equals/hashCode
A class adds a new field with semantic significance, but its `equals` and/or `hashCode` are not updated, so two instances differing only in that field compare equal and collide in hash-based collections. Schema-change notifications, cache lookups, and de-dup operations silently fire on the wrong identity.
**Look for:** Classes with `equals`/`hashCode` overrides plus a new field added in the diff — verify the field appears in both methods.

### F-02: equals overridden without hashCode (or vice versa)
A class overrides one of `equals`/`hashCode` but not the other (or includes a new field in one but not the other), violating the language contract and causing instances to behave inconsistently in hash maps and sets.
**Look for:** Modified `equals` without a matching `hashCode` change (or vice versa); also look for `hashCode` calling `Objects.hash()` on byte arrays where `Arrays.hashCode()` is required.

### F-03: hashCode includes the same field twice
A `hashCode` repeats one field in place of another, so objects differing only on the omitted field collide and are treated equal in hash collections.
**Look for:** `Objects.hash(...)` calls in the diff where two arguments look like they were copied; check parity with the `equals` field list.

### F-04: equals delegates to typed comparator without instanceof guard
An `equals(Object)` implementation casts or delegates without first checking the runtime type of the operand, throwing a `ClassCastException` or `RuntimeException` for cross-type comparisons instead of returning `false`.
**Look for:** `equals` methods that call `compareTo` or a typed helper before any `instanceof` test on the argument.

### F-05: equals compares this to itself or to wrong field
An `equals` bridge method casts the argument but mistakenly passes `this` to a helper, or compares one operand's field to itself rather than the other operand's corresponding field. Comparison is constant (always true or always false) regardless of inputs.
**Look for:** `equals` bodies where every field comparison reads from the same operand, or methods that pass `this` rather than the cast parameter.

### F-06: New enum constant without matching switch case
A new enum constant or message-type constant is added to the type but not to one or more switch statements that map enum values to behavior. The unhandled case falls through to `default`, throws `UnsupportedOperationException`, or silently no-ops.
**Look for:** New enum entry in the diff — grep for `switch` statements over that enum type elsewhere in the codebase and verify each handles the new case.

### F-07: Builder copy/from method omits a new field
A request, mutation, or configuration builder gains a new field, but the `copy`, `toBuilder`, `from`, or `clone` constructor that propagates source-to-destination fields is not updated, so caller-specified values are silently discarded.
**Look for:** Diff adds a new field to a class with a copy-constructor or `toBuilder` method — check that the new field is copied in every such method.

### F-08: Builder/factory accepts parameter but never applies it
A constructor or builder method accepts an option (encryption settings, throttle rate, headers, callbacks, frame size) but never assigns it to the constructed object or forwards it to the underlying library, silently discarding the caller's value.
**Look for:** Constructor parameters with no corresponding field assignment, or setters that never store their input — search for the parameter name on the right-hand side of an assignment.

### F-09: Setter writes wrong field; getter never wired
A field is exposed via a setter but the assignment targets the wrong field, or a getter is hardcoded to return `null` / a constant rather than reading the backing field, silently dropping the stored data.
**Look for:** Setters whose assignment target name does not match the parameter name, and accessors with no `return field;` body.

### F-10: New copy/clone-introduced field shares mutable reference
A copy method copies a field by reference (rather than deep-copying a mutable sub-collection), so mutations through the copy propagate back to the original and corrupt subsequent uses.
**Look for:** New `copy`/`clone`/copy-constructor methods — verify mutable collections, maps, and arrays are defensively copied.

### F-11: Subclass overrides one method but not its sibling
A subclass overrides a data-access method (e.g., `iterator`) but inherits a paired method (`size`, `count`, `isEmpty`, `toString`) from the superclass, returning answers derived from a different (often empty or pre-mutation) state.
**Look for:** Subclasses that override one of a logically paired set of methods — check that all related methods are overridden consistently.

### F-12: Subclass adds resources/fields but no override of lifecycle method
A subclass adds new closeable resources, fields, or dependencies, but does not override `close`, `abort`, `interrupt`, `stop`, `enable/disable`, or memory-measurement methods inherited from the parent. The base implementation runs and silently misses the subclass's state, leaking resources or reporting wrong sizes.
**Look for:** Subclasses with new `Closeable`/`AutoCloseable` fields or background threads — verify they override all relevant lifecycle methods.

### F-13: Override silently dead due to renamed/missing @Override
A subclass override loses its annotation, gets misspelled, or has a drifted signature relative to the parent; without `@Override` the compiler accepts it, but the inherited base method runs at runtime and the override is dead code.
**Look for:** New or modified methods in subclasses without `@Override` annotation — verify the signature exactly matches the intended parent method.

### F-14: Default interface method returns hard-coded constant
A `default` method on an interface returns a hard-coded sentinel ("never matches", `Optional.empty()`, `false`, `null`) instead of inspecting state. Mutable implementations that legitimately need a real answer inherit the constant unless they remember to override.
**Look for:** Interface `default` methods returning literals — every concrete implementer must override unless the constant is truly correct.

### F-15: Decorator/wrapper omits override for new interface method
When an interface gains a new method, decorator and forwarding subclasses that wrap a delegate must override the new method to forward to the delegate; absent overrides reach a `default` that throws or returns wrong values.
**Look for:** Interface gains a new method — find every wrapper/decorator class and verify it overrides the new method to delegate.

### F-16: Subclass missing factory-style return type override
A factory-style method like `create`, `with`, or `copy` declared on a base class returns a base-class instance; subclasses that should preserve their identity must override to return the subclass type, otherwise the base method silently returns the wrong concrete type.
**Look for:** Subclasses inheriting `Self`-returning methods from a base — check they override to preserve their type.

### F-17: Register without deregister — leak / stale subscription
A constructor registers an instance with a metric registry, event bus, schema listener, management endpoint, or observer, but the corresponding `close`/`stop` method is missing the matching deregister call. Entries accumulate across lifecycle cycles.
**Look for:** New `register*`, `addListener`, `subscribe`, `recordSensor`, `addCloseable` calls — verify the matching `deregister*`/`removeListener`/`unsubscribe` exists in `close`/`stop`.

### F-18: Register/deregister name mismatch
A metric, sensor, or management endpoint is registered under one name but the deregister call uses a different name (typo, stale rename, missing namespace component); the deregister silently does nothing and the registration leaks indefinitely.
**Look for:** Compare the string/key used in `register*` vs `deregister*`/`unregister*` calls — they must match exactly.

### F-19: New event/verb/handler not added to dispatch table
A new RPC verb, message type, or event family is defined but the dispatcher's routing map, switch, or interceptor chain is not updated, so the message is silently dropped, asserts at runtime, or maps to the wrong handler.
**Look for:** New verb/event constants in diff — grep for the central dispatcher and verify the new constant is added.

### F-20: New serializer registered with no deserializer counterpart
A new serializer or wire-format writer is added but the corresponding read path or version handler is missing, so the new field round-trips correctly only on the sender; the receiver mis-decodes.
**Look for:** Diff modifies a `write*`/`serialize` method without touching the matching `read*`/`deserialize` method — the two must change together.

### F-21: Serializer write/size/read methods drift
A field is added or reordered in the `serialize` method but the parallel `serializedSize` or `deserialize` method is not updated. Buffer allocation is wrong, or fields are read from the wrong byte offsets, corrupting all subsequent fields.
**Look for:** Any change to a serialization method — locate the matching size and deserialization methods and verify field order, presence, and width all match.

### F-22: Metric / management endpoint registered only under new name after rename
A metric or management endpoint is renamed but only the new name is registered. Existing monitoring tools, dashboards, and alerting that reference the old name silently break.
**Look for:** Metric/endpoint rename — preserve a deprecated alias registration unless the diff explicitly removes the old name.

### F-23: Hardcoded enumeration not updated when new entries added
An iteration over a hard-coded list (function registrations, schema objects, system tables, well-known verbs) misses dynamically-added or newly-introduced entries, so any operation derived from that enumeration silently omits the new entries.
**Look for:** Static arrays/lists of enumeration entries — verify the new entry is appended.

### F-24: Type-dispatch method missing branch for new variant
An `if/else if` chain or `instanceof` cascade that classifies polymorphic inputs is not extended for a newly added subtype; the new variant falls through to a wrong default branch and is decoded, serialized, or routed incorrectly.
**Look for:** `instanceof` cascades or type-tag dispatchers in the diff or referenced from changed types — verify all subtypes are enumerated.

### F-25: Switch statement falls through to throw on new enum value
A `switch` over an enum lacks a case for a newer constant; the `default` branch throws `UnsupportedOperationException` or `AssertionError` rather than returning a meaningful result, crashing on the first invocation that exercises the new value.
**Look for:** `switch` statements over enums where the `default` throws — verify exhaustiveness against the current enum definition.

### F-26: Validation present in one path, missing in parallel path
A guard, validation, or normalization step lives in one entry point (e.g., the local-apply path) but is omitted from a parallel path (the remote-announce path), allowing invalid or non-canonical inputs to bypass the check.
**Look for:** Two parallel call sites that should apply the same check — search for the validation function and verify both invoke it.

### F-27: New field omitted from human/wire serialization symmetry
A field exists in the human-readable / textual output path but not in the structured/wire output (or vice versa); machine consumers see no field while operators do, or vice versa.
**Look for:** Both `toString`/`toJson`/`toDisplayString` and binary serialize methods on the same class — verify all fields appear in both.

### F-28: Constructor accepts but ignores parameter (silent default)
A class's constructor accepts a parameter (credentials, callback, configuration object), but never stores or forwards it; the object silently runs with the default value of that parameter.
**Look for:** Constructor parameters that don't appear on the right-hand side of any field assignment in the constructor body.

### F-29: Method overload with wider type masks intended narrower call
A method gains a new overload (e.g., generic vs. concrete, Object vs. typed); existing call sites silently dispatch to the new (or old) overload due to overload resolution rules, applying wrong semantics without any compile error.
**Look for:** New overloads of an existing method name — audit all call sites to confirm they bind to the intended overload.

### F-30: Async method's contract mismatch — exception thrown vs failed future
A method declared to return a future or `CompletableFuture` throws synchronously instead of returning a failed future, or documents one error mode but emits another. Callers using future-chained error handling miss the failure entirely.
**Look for:** `throw` statements inside methods returning a future-like type — failures should generally be wrapped in a failed future.

### F-31: Setter mutates intended-immutable field, bypassing rebuild
A class designed for immutability gets a setter that mutates a field in place, bypassing the version-bump or copy-on-write mechanism that would propagate the change to derived state.
**Look for:** New setters on classes that have a `withFoo`/`copy` builder pattern — the setter likely shouldn't exist.

### F-32: Snapshot-friendly accessor silently uses live-read
A method documented or contractually expected to return a snapshot (immutable view) reads from a live mutable reference, so a concurrent mutation between the read and downstream use produces inconsistent behavior.
**Look for:** Methods named `snapshot`, `view`, or `current` that don't actually defensively copy or wrap their result.

### F-33: Live mutable view returned where defensive copy expected
A getter returns a live internal collection or mutable map (rather than an unmodifiable view or copy), allowing external callers to silently corrupt internal state.
**Look for:** Getters that `return this.someCollection` rather than `Collections.unmodifiableSet(...)` or a copy.

### F-34: Visitor/dispatch missing case for new type variant
A concrete visitor subclass omits an override for a newly added type variant; the inherited base implementation returns null or a wrong default, producing a `NullPointerException` at the caller.
**Look for:** New visitor types added — check every subclass of the visitor base for a corresponding new override.

### F-35: Hand-rolled cache key omits a value-determining field
A cache key's equality and hash methods omit one or more fields that influence the cached value, so distinct logical configurations collide on a single entry and reuse the wrong value.
**Look for:** Cache key classes with new fields added — verify they participate in the key's `equals` and `hashCode`.

### F-36: New configuration property accepted at one layer, dropped before use
A config option is parsed and validated at the public entry point but never threaded to the constructor or builder that actually applies it; the setting is silently ignored and the default takes effect.
**Look for:** New config keys appearing in parsers — trace through the constructor chain to the place that actually configures the underlying behavior.

### F-37: Validation loosened in entry-point but enforced in inner factory
Validation exists at the high-level public entry point but lower-level factories or alternate construction paths bypass it, allowing invalid inputs to be persisted.
**Look for:** Multiple constructor or factory methods on the same class — verify validation runs uniformly across all of them.

### F-38: Constraints assumed by callsite contract violated by new caller
A new call site uses a method in a way that violates an undocumented or weakly-documented contract (e.g., passing an unmodifiable collection where mutation is required, calling close from outside the lifecycle owner, calling outside the synchronized context).
**Look for:** New call sites of existing helpers that pass `Collections.emptyList()`, `Collections.unmodifiableMap()`, or unusual sentinel arguments.

### F-39: Field added but omitted from serialization helper / persistence query
A new field is added to a record, but the serialization-to-persistence helper, DDL emitter, or schema-mutation query is not updated, so the field is silently never persisted and reverts to its default after restart.
**Look for:** New fields on records mapped to schema rows — verify both the persist path and the load path read/write the new column.

### F-40: Subclass field-shadowing instead of method override
A subclass redeclares a field with the same name as the parent rather than overriding methods that read/write that field; queries on the parent's field always return stale values.
**Look for:** Subclasses that re-declare a field with a name already present on the parent — almost always a bug.

### F-41: Call-site forgotten after rename / signature change
A method is renamed or its return type narrowed; some call sites are updated but stale ones still reference the old name (resolved against an unrelated overload that compiles) or wrap the new return type in an obsolete conversion. Wrong logic runs silently.
**Look for:** Any rename or signature change — search for every call site of both the old and new names in the entire codebase.

### F-42: Dispatch / subtype check enumerates only first variant
A predicate, switch, or `instanceof` chain checks one subtype but does not handle a newly-introduced sibling subtype; the missing case falls through to a generic default that has wrong semantics for the unhandled type.
**Look for:** `instanceof X` checks where the diff added a sibling class `Y` — verify `Y` is handled.

### F-43: Bridge/no-op stub introduced as a default still allows old API path
A `default` method is introduced as a bridge to a renamed API; existing implementers can keep using the deprecated method silently. Removing the default without making the new method abstract leaves wrong runtime behavior unguarded.
**Look for:** New `default` methods on interfaces that delegate to deprecated alternates — these silently mask incomplete migrations.

### F-44: Wrapper/serde missing forwarding of newly added parameter
Wrapper serializers, deserializers, or interceptors that delegate to inner instances are not updated to forward a newly-added parameter (e.g., `Headers`, `topic`, `context`); the inner call receives a default and silently drops the data.
**Look for:** Wrapper classes with delegate calls — when the inner interface gains a parameter, every wrapper must forward it.

### F-45: Schema gains field but parallel descriptor arrays not extended
A structured record adds a new field but parallel type-descriptor or name-descriptor arrays used in a generic deserializer are not extended; reads pick up the wrong array slot and cast to the wrong type.
**Look for:** Parallel arrays indexed by field position — verify they're updated together.

### F-46: New version constant or compatibility check incomplete
A new protocol version, file format, or schema version is added but compatibility checks (gates, migrations, fall-back paths) are only partially updated; nodes mixing old and new versions misbehave silently.
**Look for:** New version constants — search for all `if (version >= ...)` checks and verify completeness.

### F-47: serializedSize accumulator loses an added field's contribution
A field is added (or its byte width changes) in the write path, but the `serializedSize` calculator drops or ignores the new contribution; output framing is off, allocation is wrong, and downstream readers misalign.
**Look for:** Any modification to a write method — verify the accompanying `serializedSize` totals the same fields with the same widths.

### F-48: Copy-paste callsite carries argument from sibling method
A new method copy-pasted from a sibling retains an argument, name, or constant from the original that no longer applies in the new scope; the wrong context value is forwarded silently.
**Look for:** Newly added methods that look like copies of nearby siblings — verify every reference inside has been updated for the new context.

## Summary

These patterns share a structural shape: a system has multiple sites that must change in lockstep when a new field, type, event, or capability is introduced. A diff that touches one site without touching the symmetric site introduces a silent incorrectness — the code compiles, runs, and even passes most tests, but produces wrong results, leaks resources, or skips work because the contract between the sites is no longer maintained. The diff signals above are the strongest indicator that this category applies; once it does, the review checklist is dominated by "find every other site that should also change."
