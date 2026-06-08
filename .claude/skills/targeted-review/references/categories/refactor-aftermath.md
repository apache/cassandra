# Category: Refactor Aftermath

Bugs whose root cause is incomplete or inconsistent refactoring — leftover guards, stale references after a rename, narrowed/broadened types not propagated to all sites, dead branches preserved past their owning feature, deprecated overloads with stale defaults, mixed-version mismatches between the old and new shape, and parsers/regexes that handle only the new format.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
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

## Findings

### F-01: Guard removed but teardown left unconditional
A refactor removes an option-flag or precondition guard from a costly operation, but the matching teardown/cleanup step remains unconditional and now runs even when the guarded operation never executed.
**Look for:** A removed `if (flag)` or `if (enabled)` around a setup block whose paired `close`/`release`/`reset`/`undo` is still outside any guard.

### F-02: New parser/regex requires field only present in modern format
A re-implemented path/key/identifier parser uses a regex or grammar that treats a field present only in the new format as required, silently skipping or rejecting all legacy inputs that omit that field.
**Look for:** New `Pattern.compile(...)`, split-on-delimiter loops, or grammar rules introduced alongside an existing format; check whether the old format had fewer/optional components.

### F-03: Stale call sites still reference old API path/name
After a method, accessor, or predicate is moved, renamed, or replaced, some call sites continue to compile against the old symbol (often via leftover imports or static helpers), silently invoking the obsolete behavior.
**Look for:** Two coexisting symbols with overlapping names (e.g., `getX`/`getXValue`, `Foo.bar`/`FooHelper.bar`); grep for the old name and check that every hit has been updated.

### F-04: Deprecated overload never populates the parameter the new one carries
A deprecated overload kept for binary compatibility hardcodes a sentinel/null/default for a parameter that the new overload requires callers to supply, so the operation completes against an empty/missing input and silently no-ops.
**Look for:** `@Deprecated` overloads that delegate to the new overload while passing `null`, `0`, `EMPTY`, or `Range.empty()`.

### F-05: `equals`/`hashCode` not updated for new fields
After fields are added or renamed, the equality/hash methods are only partially updated (or not updated at all), so logically distinct objects compare equal, hash collisions appear, or change-notification logic stops firing.
**Look for:** New fields in a class whose `equals`/`hashCode`/comparator method body does not mention them; also fields that were removed but still referenced inside `equals`.

### F-06: Mixed-version digest computed over structurally different fields
Two software versions independently compute an identity digest/hash/checksum; after one side adds, removes, or reorders the fields, hard equality between the two digests permanently fails during rolling deployments.
**Look for:** Digest functions over a field list that differs from the on-the-wire representation, or that includes fields one side omits (e.g., tombstones, optional metadata).

### F-07: Lambda in hot loop captures locals when a context object exists
A lambda introduced in a refactor captures multiple locals and is passed into a tight loop, forcing per-iteration heap allocation, when the surrounding class already exposes a context object that could be threaded through instead.
**Look for:** New lambdas inside `for`/`while` bodies that close over more than one local variable; check whether a `this`-style context object is available.

### F-08: Concrete-typed field/array now expects broadened abstraction
A field, array, or parameter declared with the original concrete type was valid pre-refactor; after the refactor it is expected to hold a broader abstraction, so still-extant concrete values fail the runtime cast or `ArrayStoreException`.
**Look for:** `T[]` arrays filled by methods returning `Supertype`; downcasts immediately after lookups whose return type was widened.

### F-09: Subclass override silently disconnected after parent signature drift
A parent class changes a method signature/name during refactor; subclasses retain the old name/signature without `@Override`, so the intended override becomes unreachable dead code and the inherited base method runs instead.
**Look for:** Override-style methods missing `@Override`; matching method names in a hierarchy where the parent was recently touched.

### F-10: Old field still set in some constructors but not all after split
A monolithic constructor split into multiple paths leaves one path setting a flag/field that another no longer touches; later code unconditionally consults that field and observes a stale or default value.
**Look for:** Two constructors with divergent field-assignment lists; init flags toggled in only one branch after a method-extraction refactor.

### F-11: Static config cached at class load freezes pre-refactor value
A static-final cache of a configuration value or singleton handle is computed at class load before later refactored code repopulates the source; the cache freezes a stale/uninitialized value that ignores all later updates.
**Look for:** `static final` fields initialized from a global registry/config; check whether the registry is now populated lazily or by a startup phase that may run later.

### F-12: Cache or fallback never populated after producer was removed
A refactor removes the code that populated a fallback cache, derived view, or secondary lookup; remaining consumers still query it and silently receive default/empty/null answers.
**Look for:** Code that reads from a structure whose only known producer was deleted in the same change; `containsKey` checks that always return false.

### F-13: Old-format end-marker / sentinel skipped by new reader
A rewritten deserializer or scanner does not recognize an end-of-section marker, sentinel, or framing byte that was valid in the legacy format, so valid records following the marker are silently skipped or misframed.
**Look for:** New deserializers that only handle the current version's framing; legacy formats with EOF/end-of-record markers not enumerated in the new switch.

### F-14: New variant added to enum/type but old switch/dispatch not updated
A new constant or subtype is added but a sibling switch, dispatch table, or instanceof chain is not extended, so the new case falls through to the default (often throwing or silently producing wrong output).
**Look for:** Recently-added enum constants or subclasses; grep for switches over the enum/base type and confirm every case is handled, especially in serialization and routing.

### F-15: Wire-format field added on one side only
A protocol field is read by the new version but never written by the old, or written by the new and not handled by the old; mixed-version peers misframe everything after the missing/extra field.
**Look for:** Symmetric serialize/deserialize pairs where one side gained a field guarded only by a single-sided version check; missing `nullableVersions`/`ignorable` annotations.

### F-16: Hard assertion / `UnsupportedOperationException` for legacy data
After a format upgrade, a deserializer or comparator throws unconditionally on legacy inputs that the prior implementation handled, crashing reads of pre-upgrade data instead of falling back.
**Look for:** Newly-added `assert version == CURRENT`, `throw new UnsupportedOperationException`, or strict `instanceof` checks at deserialization entry points.

### F-17: Removed feature's flag still consulted by surviving code
A feature removed in a refactor leaves its enable/disable flag, mode constant, or branch condition behind; remaining code paths consult the flag and route into now-unreachable or wrong behavior.
**Look for:** Boolean fields, enum constants, or string mode names whose only writers were deleted; conditions that can never become true (or never false).

### F-18: New parameter hardcoded at internal call site
A method gains a new parameter; an internal call site (or refactored shim) passes a fixed literal instead of forwarding the caller's value, silently overriding per-call configuration with a global default.
**Look for:** Recently-added parameters where some call sites pass `null`, `false`, `0`, or `Version.LATEST`; bridge/adapter methods that omit forwarding the new field.

### F-19: Bridge `default` method silently keeps old implementations alive
An interface method gets a `default` implementation as part of a bridging refactor; subclasses that should have been forced to update silently inherit the bridge and never adopt the new behavior.
**Look for:** New `default` methods on interfaces with many implementers; combined with a "deprecated" sibling whose behavior the default delegates to.

### F-20: Type wrapped in richer wrapper but call sites still compare raw
A primitive or simple type is wrapped in a richer container during refactor; call sites that previously compared with `==` or `.equals()` against the raw form silently never match, causing map lookups to miss and conditions to never fire.
**Look for:** Recently introduced wrapper types (e.g., `Token`, `Identifier`, `Version`); grep for raw-type comparisons against the wrapper.

### F-21: Field omitted from copy/clone after being added to source
A new field added to a class is not added to the matching copy/clone/builder/`with`-style helper, so derived objects silently lose the field even though the source retains it.
**Look for:** `copy()`, `clone()`, `toBuilder()`, `merge()` whose body lists fields explicitly; new fields not yet enumerated there.

### F-22: Existing serializer's size method stale after format change
A serializer's body is updated to write a new field or shape, but the matching `serializedSize`/length-prefix method is not, so framing/length-prefix values diverge from actual bytes written.
**Look for:** `serialize`/`serializedSize` pairs touched in the same diff where one method gained a field the other did not.

### F-23: Refactor accidentally removes a single property-assignment line
A reorganization accidentally drops a single `this.x = arg.x` (or equivalent setter call), so a user-configured value is silently replaced with the field's default initializer for the lifetime of the object.
**Look for:** Constructors and copy methods touched in the same diff where one previously-assigned field is no longer mentioned; check builder chains for missing forwards.

### F-24: Side-effect setter no longer invoked from new factory path
After splitting a single creation path into multiple paths (factory + constructor + builder), one path skips a required side-effect call (registration, listener attach, default validator install), so objects produced via that path are missing initialization others rely on.
**Look for:** Multiple creation paths producing the same type; check that each calls every step the original monolithic code did.

### F-25: Stale comparator/sort key after underlying fields changed shape
A sort comparator continues reading a field via the pre-refactor accessor (e.g., raw byte form, ordinal, or stale name), so ordering is invariant or wrong even though the data has moved to a new representation.
**Look for:** Comparators that read fields directly rather than via accessors that were updated; comparators bound to a no-longer-implemented interface.
