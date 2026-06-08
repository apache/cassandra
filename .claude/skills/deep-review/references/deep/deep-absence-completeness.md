# Deep Absence & Completeness — Extended Checklist
---

## Context Gathering (REQUIRED)

For each target file, gather:

1. **All interfaces/superclasses**: What contracts must this class fulfill?
2. **All sibling implementations**: What parallel classes exist for the same interface?
3. **All callers**: Who creates and uses this object?
4. **Serialization family**: Does this class participate in serialize/deserialize/equals/toString?
5. **Registration set**: What metrics, listeners, handlers does this class register?

---

## Absence: Registration & Lifecycle Depth

### Listener/metric leak search (systematic)
For EVERY `addListener`, `register`, `subscribe`, `addMetric`, `createSensor`, `put` into registry:
-  Search the same class for matching `remove`/`deregister`/`removeSensor`
-  Check BOTH `close()` and `stop()` and `destroy()` paths
-  Check the failure/exception path separately from success
-  If multiple registrations happen in sequence: does cleanup handle partial failure?

### Self-registering class loading (specific)
-  For `static final INSTANCE = new Foo()`: is the class actually loaded from the startup path?
-  Grep for references to the class name. If only optional code paths reference it, static init never runs.

### Background task lifecycle (specific)
-  For every `scheduleWithFixedDelay`, `scheduleAtFixedRate`, `Timer`, `ScheduledExecutor`:
  - Is there a `shutdown()`/`cancel()` mechanism?
  - Is it called from test teardown?
  - Is it gated on a feature flag?

---

## Absence: Event & Handler Coverage Depth

### Enum constant dispatch search (systematic)
For EVERY new enum constant or event type:
-  `grep -rn "switch.*EnumType\|case.*ENUM_CONSTANT"` across the entire codebase
-  List ALL switch/dispatch/if-else chains. Is the new constant handled in EACH?
-  Check `default:` clauses — is `break` (silent skip) or `throw` (fail-fast) correct?

### State machine handler completeness (specific)
For each `onTimeout()`, `onError()`, `onFailure()`, `onRetry()` override:
-  Read sibling classes implementing the same interface
-  What follow-up action do they perform?
-  Does THIS implementation perform the equivalent action or just log/no-op?
-  Will a no-op leave the state machine stuck?

---

## Absence: Field Completeness Depth

### New field propagation search (systematic)
For EVERY new field added to a class:
-  Is the field written in the serialization method (serialize/write/encode/marshal)?
-  Is the field read in the deserialization method (deserialize/read/decode/unmarshal)?
-  Is the field measured in the size method (serializedSize/encodedSize/size), if one exists?
-  Symmetry: do all three handle the field in the same order and under the same conditional guards?
-  Search for `equals` method — is the field compared?
-  Search for `hashCode` method — is the field hashed?
-  Search for `toString` method — is the field printed?
-  Search for copy constructor, `clone()`, `sharedCopy()`, `toBuilder()` — is the field copied?
-  Search for `describe()`, `toMap()`, snapshot methods — is the field included?

### Parallel method consistency (specific)
-  Compare field lists across `equals`, `hashCode`, `toString`, `serialize`. Do they agree?
-  If one method has N fields and another has N-1, which field is missing and why?

---

## Absence: Parallel Path Coverage Depth

### Structural symmetry search (systematic)
For each modification to path A:
-  Identify structurally parallel path B (same class, same interface, same event family)
-  Does path B have the corresponding modification?
-  Specifically check:
  - If a guard `if (X) return` was added to path A: does path B have it?
  - If a method call was added to one if-branch: does the else-branch need it?
  - If a field was added to one describe/serialize: do sibling methods have it?
  - If one state callback was modified: are all sibling callbacks updated?

### Fix-patch parallel path search
When reviewing a bug-fix:
-  What was missing from the original code?
-  Did the fix add it EVERYWHERE?
-  Same lookup on parallel path (fast/slow, NIO/fallback)?
-  Other catch blocks or early-return paths?
-  Other version guards?

---

## Absence: Scope & Visibility Depth

### Private/package-private access search
For every new `private` or package-private member:
-  Is `@VisibleForTesting` used on a singleton accessed cross-package?

### Feature flag / version guard coverage search
For every feature flag check and every `version >= X` guard:
-  Does it guard the main operation?
-  Does it guard setup?
-  Does it guard teardown?
-  Does it guard secondary/dependent operations?
-  For version guards: is the same version condition used consistently, or do different paths use different thresholds?

---

## Completeness: Interface Implementation Depth

### Predicate method override search
For every new class implementing an interface:
-  List ALL abstract/default methods in the interface
-  For each predicate (`isX()`, `hasX()`, `canX()`):
  - What is the default return value?
  - Is that default CORRECT for this implementation?
  - If the class has state that makes the predicate true, the override is MANDATORY

### Lifecycle method override search
-  Does the class hold resources the parent doesn't know about?
-  Does it need to override `close()`, `release()`, `abort()`?
-  Does it need to override `sharedCopy()`, `clone()`, copy constructor?

---

## Completeness: Factory & Dispatch Depth

### Factory method type routing
For each factory dispatching on a type parameter:
-  List ALL possible values of the discriminator
-  For each value: verify the returned concrete class is CORRECT
-  If a generic `else`/`default` handles multiple values: verify each is correct
-  After consolidation: does each value produce the same type as before?

### Overload binding after signature change
-  When a method adds a new parameter with default-providing overload:
  - List ALL callers
  - Do callers that need the new behavior call the new signature?
  - Old callers silently bind to old overload

### Extract-method refactoring
-  After extracting code into a helper:
  - Does the call site retain an operation the helper also performs?
  - Double-write? Double-close? Double-init?

---

## Completeness: Accumulation & Constants Depth

### Accumulation operator (= vs +=)
For every `field = expression` in a loop:
-  Is the intent accumulation (`+=`) or replacement (`=`)?
-  Is this per-partition/per-item where total is needed?
-  For compound stats (min, max, sum, count): is each updated with the correct operator?

### Constant vs accessor
-  Is `Foo.bar()` called where `Foo.BAR` (constant) is correct?
-  Is `Foo.BAR` referenced where `Foo.bar()` (dynamic accessor) is needed?

### Constructor parameter not stored
-  Does a constructor accept a parameter never assigned to a field?
-  Does `this.field = null` appear instead of `this.field = parameter`?

---

## Additional Absence Patterns from Multi-Project Corpus

### Registered-object retention (weight: medium)
-  For `registry.put(...)` / `manager.add(...)` inside factory or builder methods: is the registered object stored in a field of the enclosing owner?
-  If the object is only local-scoped: trace who calls `deregister` at shutdown — is the lookup key still reachable?
-  Does the owner of the registry outlive the registered object's natural lifetime?

### Idempotency on re-entrant lifecycle (weight: medium)
-  For every `init`, `close`, `open`, `start`, `stop`: can it be called twice? (schema reload, listener callback, reset path)
-  Is there a has-initialized or has-closed flag?
-  If cleanup fires side effects (metrics, callbacks, observer notifications): does a second call re-fire them?
-  Does a CAS-style initialize treat "already initialized by another thread" as success, not error?

### Rollback / counter-decrement on failure path (weight: medium)
-  For every new increment, add, or `put` on the success path:
  - Does the cancellation path decrement / remove?
  - Does the task-rejection path decrement / remove?
  - Does the exception-caught path decrement / remove?
  - Does the timeout path decrement / remove?
-  For state-session maps cleared on failure: is there also a clear on success?
-  For metric counters incremented before async submit: is the counter reverted if the submit throws `RejectedExecutionException`?

### Missing metric / notification on sibling branch (weight: medium)
-  Compare the new code against every sibling if/else, switch case, and cache-hit fast path
-  Does each branch record the metric that one branch records?
-  For state-transition listeners: does every state branch fire the same side-effect propagation calls?
-  For close-path metric recording: is the recording inside `try` or inside `finally`?

### Configuration / parameter propagation depth (weight: high)
For every setting parsed from config, DDL, or builder input:
-  Trace the value: parse → validate → store → copy into builder → constructor → factory call → downstream consumer
-  At each hop, does the next layer actually read and apply the value, or does it fall back to a default?
-  For wrapper / decorator classes: are child objects constructed with the parent's per-operation state, or with globals?
-  For third-party builder chains (Netty, Jackson, Thrift): is every caller-relevant setter called, or does the library default silently win?
-  For refactored constructors: does every existing call site still pass the now-required parameter?

### Version / runtime fallback depth (weight: medium)
-  For new wire-format fields: is there a `version >= X` guard on both write and read?
-  For new protocol features: is the cluster-wide minimum checked, not just the local capability?
-  For reflective access to JVM internals: is there a fallback for the next Java version, or is the path guarded by a try/catch that includes the renamed-field exception?
-  For native-library loads: does the catch handler include every exception the loader can throw (`LinkageError`, `UnsatisfiedLinkError`, `NoClassDefFoundError`)?

### Drain of trailing bytes on early-exit (weight: medium)
-  For every `break` / `return` / `continue` added inside a framed-stream loop (checksummed read, length-prefixed records):
  - Does the exit path advance past the checksum / footer?
  - Does the exit path update the stream's logical position tracker?
  - Does the exit path match what the normal-completion path does?
-  For deserializers with partial-consume semantics: is there a drain step before returning?

### Directory / cross-file sync depth (weight: medium)
-  Before a file is listed or deleted: has the directory been fsync'd since the last creation / removal?
-  Before closing a buffered writer: is flush + fsync performed, or only close?
-  When flushing a set of interdependent files (parent + child, index + data): is the order deterministic and is each flush complete before the next?
-  Does the atomic-rename path rely on the directory fsync that may be missing?

---

## Additional Completeness Patterns from Multi-Project Corpus

### Subclass missing override of measurement / copy / abort / interrupt / factory method (weight: medium)
-  When a subclass adds instance fields or resources: does it override every method whose inherited parent-layout implementation would silently under-measure, under-copy, or no-op?
-  Specifically look for: `unsharedHeapSize` / `estimateSize`, `abort()`, `interrupt()`, `sharedCopy()` / `clone()`, factory-style methods returning the same type.
-  A parent default `return false` / `return 0` / no-op inherited by a stateful subclass silently corrupts the contract.

### Decorator / wrapper missing override of default-throwing method (weight: low)
-  For every interface method whose default implementation throws `UnsupportedOperationException` or `NotImplementedError`: does the decorator override it?
-  If the decorator only forwards the "core" methods and silently inherits default-throwing stubs for auxiliary ones, the first caller hitting the missed method crashes.

### New subtype / variant unhandled in type-dispatch (weight: medium)
-  When a new subtype is introduced, grep ALL `instanceof` / `case` / visitor `visitXxx` chains in the codebase.
-  Is the new variant handled in each? Silent fallthrough to default usually returns null or the raw unconverted value.
-  Check Scala `sealed` trait usage from Java callers — exhaustiveness guarantees are lost across languages.

### Parameter forwarding dropped through wrapper / delegation (weight: medium)
-  When a wrapper or factory forwards a call to an inner implementation: are ALL parameters propagated?
-  Do tracing context, credentials, or consistency level get silently dropped through overload delegation to a narrower signature?
-  When a field value is forwarded through N layers, does each layer carry it? A single layer that hardcodes the default / null silently drops caller intent.

### Constructor path bypasses registration / setup (weight: medium)
-  If a class has multiple constructors: does each perform all required registration / validation / init steps?
-  Secondary constructors that delegate with omitted arguments may skip `addToRegistry`, `validate`, `start`, or `postConstruct` calls.
-  Factory method with two branches (e.g. from-bytes vs from-memory): does each branch apply the same post-creation setup?

### Field silently overwritten by hardcoded initializer / setter never called (weight: medium)
-  Does a field's inline initializer assign a constant that is never overwritten in the constructor body — so the caller's constructor argument is discarded?
-  Does a builder declare an option but the terminal method never read it (parallel execution, custom handler, etc.)?
-  Does a result builder compute a field but forget to call `setXxx` before `build()`?

### equals / hashCode asymmetry with cache / identity fields (weight: medium)
-  Does `equals` omit a field that determines the cached value, so distinct configurations collide?
-  Does `equals` identify objects by display name only, omitting the stable unique identifier?
-  Does `equals` delegate to a type-specific comparator without a type guard, throwing on mismatched operand types?
-  Does `equals()` call `Objects.equal(this, x)` or a reference-equality utility, creating identity comparison?

### New field default-value / first-read visibility (weight: low)
-  Does the newly added field have a non-null default?
-  Can monitoring, `toString`, or a debug accessor read the field before startup assigns it?
-  Is the field accessed on every construction path, or only on one?
