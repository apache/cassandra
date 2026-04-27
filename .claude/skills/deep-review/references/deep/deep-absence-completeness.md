# Deep Absence & Completeness — Extended Checklist

Full-depth checklist for deep review. Combines the absence and completeness domains with
codebase-search methodology for thorough investigation.

---

## Phase 0: Context Gathering (REQUIRED)

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
- [ ] Search the same class for matching `remove`/`deregister`/`removeSensor`
- [ ] Check BOTH `close()` and `stop()` and `destroy()` paths
- [ ] Check the failure/exception path separately from success
- [ ] If multiple registrations happen in sequence: does cleanup handle partial failure?

### Self-registering class loading (specific)
- [ ] For `static final INSTANCE = new Foo()`: is the class actually loaded from the startup path?
- [ ] Grep for references to the class name. If only optional code paths reference it, static init never runs.

### Background task lifecycle (specific)
- [ ] For every `scheduleWithFixedDelay`, `scheduleAtFixedRate`, `Timer`, `ScheduledExecutor`:
  - Is there a `shutdown()`/`cancel()` mechanism?
  - Is it called from test teardown?
  - Is it gated on a feature flag?

---

## Absence: Event & Handler Coverage Depth

### Enum constant dispatch search (systematic)
For EVERY new enum constant or event type:
- [ ] `grep -rn "switch.*EnumType\|case.*ENUM_CONSTANT"` across the entire codebase
- [ ] List ALL switch/dispatch/if-else chains. Is the new constant handled in EACH?
- [ ] Check `default:` clauses — is `break` (silent skip) or `throw` (fail-fast) correct?

### State machine handler completeness (specific)
For each `onTimeout()`, `onError()`, `onFailure()`, `onRetry()` override:
- [ ] Read sibling classes implementing the same interface
- [ ] What follow-up action do they perform?
- [ ] Does THIS implementation perform the equivalent action or just log/no-op?
- [ ] Will a no-op leave the state machine stuck?

---

## Absence: Field Completeness Depth

### New field propagation search (systematic)
For EVERY new field added to a class:
- [ ] Search for `serialize` method — is the field written?
- [ ] Search for `deserialize` method — is the field read?
- [ ] Search for `serializedSize` method — is the field measured?
- [ ] Search for `equals` method — is the field compared?
- [ ] Search for `hashCode` method — is the field hashed?
- [ ] Search for `toString` method — is the field printed?
- [ ] Search for copy constructor, `clone()`, `sharedCopy()`, `toBuilder()` — is the field copied?
- [ ] Search for `describe()`, `toMap()`, snapshot methods — is the field included?

### Parallel method consistency (specific)
- [ ] Compare field lists across `equals`, `hashCode`, `toString`, `serialize`. Do they agree?
- [ ] If one method has N fields and another has N-1, which field is missing and why?

---

## Absence: Parallel Path Coverage Depth

### Structural symmetry search (systematic)
For each modification to path A:
- [ ] Identify structurally parallel path B (same class, same interface, same event family)
- [ ] Does path B have the corresponding modification?
- [ ] Specifically check:
  - If a guard `if (X) return` was added to path A: does path B have it?
  - If a method call was added to one if-branch: does the else-branch need it?
  - If a field was added to one describe/serialize: do sibling methods have it?
  - If one state callback was modified: are all sibling callbacks updated?

### Fix-patch parallel path search
When reviewing a bug-fix:
- [ ] What was missing from the original code?
- [ ] Did the fix add it EVERYWHERE?
- [ ] Same lookup on parallel path (fast/slow, NIO/fallback)?
- [ ] Other catch blocks or early-return paths?
- [ ] Other version guards?

---

## Absence: Scope & Visibility Depth

### Private/package-private access search
For every new `private` or package-private member:
- [ ] Search for subclasses of this class. Do any reference the member?
- [ ] Search for cross-package classes that reference the member.
- [ ] Is `@VisibleForTesting` used on a singleton accessed cross-package?

### Feature flag coverage search
For every feature flag check:
- [ ] Does it guard the main operation?
- [ ] Does it guard setup?
- [ ] Does it guard teardown?
- [ ] Does it guard secondary/dependent operations?

---

## Completeness: Interface Implementation Depth

### Predicate method override search
For every new class implementing an interface:
- [ ] List ALL abstract/default methods in the interface
- [ ] For each predicate (`isX()`, `hasX()`, `canX()`):
  - What is the default return value?
  - Is that default CORRECT for this implementation?
  - If the class has state that makes the predicate true, the override is MANDATORY

### Lifecycle method override search
- [ ] Does the class hold resources the parent doesn't know about?
- [ ] Does it need to override `close()`, `release()`, `abort()`?
- [ ] Does it need to override `sharedCopy()`, `clone()`, copy constructor?

---

## Completeness: Factory & Dispatch Depth

### Factory method type routing
For each factory dispatching on a type parameter:
- [ ] List ALL possible values of the discriminator
- [ ] For each value: verify the returned concrete class is CORRECT
- [ ] If a generic `else`/`default` handles multiple values: verify each is correct
- [ ] After consolidation: does each value produce the same type as before?

### Overload binding after signature change
- [ ] When a method adds a new parameter with default-providing overload:
  - List ALL callers
  - Do callers that need the new behavior call the new signature?
  - Old callers silently bind to old overload

### Extract-method refactoring
- [ ] After extracting code into a helper:
  - Does the call site retain an operation the helper also performs?
  - Double-write? Double-close? Double-init?

---

## Completeness: Accumulation & Constants Depth

### Accumulation operator (= vs +=)
For every `field = expression` in a loop:
- [ ] Is the intent accumulation (`+=`) or replacement (`=`)?
- [ ] Is this per-partition/per-item where total is needed?
- [ ] For compound stats (min, max, sum, count): is each updated with the correct operator?

### Constant vs accessor
- [ ] Is `Foo.bar()` called where `Foo.BAR` (constant) is correct?
- [ ] Is `Foo.BAR` referenced where `Foo.bar()` (dynamic accessor) is needed?

### Constructor parameter not stored
- [ ] Does a constructor accept a parameter never assigned to a field?
- [ ] Does `this.field = null` appear instead of `this.field = parameter`?

---

## Completeness: Version-Gated Field Omission

When a class is moved between modules:
- [ ] Read the schema file for the primary serialized object
- [ ] List all fields and their minimum version
- [ ] For each field with minVersion > 0: verify the serialization method has version guard
- [ ] Compare field-by-field: does the new code set every field the old code set?
- [ ] Under the same version guards?
