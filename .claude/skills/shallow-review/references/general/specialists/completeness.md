# API Completeness & Contracts — Specialist Checklist

22 highest-signal questions.

---

## Interface & Override Completeness

1. [ ] When a new class implements an interface or extends an abstract class, does it override every behavioral predicate method (`isX()`, `hasX()`, `canX()`) whose default (false/no-op) is wrong for this implementation?
   → Also: list ALL abstract/default methods; a class with TTL must override `isExpired()`.

2. [ ] Does a new class override all lifecycle methods (`close()`, `release()`, `abort()`) when it holds resources the parent doesn't know about?

3. [ ] When a new subtype / variant is added to a type family, is it handled in every type-dispatch site — not only enum switches, but also `instanceof` chains, visitor `visitXxx` methods, type-converter maps, and pattern-match cases? Silent fallthrough to default returns null or the wrong value.

4. [ ] Does a subclass that adds instance fields override the memory-measurement method (`unsharedHeapSize`, `estimateSize`, size query)? Does a decorator/wrapper override every interface method whose default implementation throws `UnsupportedOperationException`?

## Field Completeness

5. [ ] When a new field is added to a class, is it present in: serialize/deserialize/serializedSize, equals/hashCode, toString, copy constructors, builder `build()`, describe/toMap?

6. [ ] Does a descriptor/serializer method include ALL fields that sibling methods include? Compare field-by-field against `equals`, `hashCode`, `toString`, `serialize`.

7. [ ] Do `equals` / `hashCode` include EVERY field that determines identity (not only the display name or primary key), and does `equals` guard against mismatched operand types (or delegate to a type-specific comparator with a type guard)? Is a `Serializable` class's declared field type itself serializable — or is a `transient` field read without a custom `readObject` reconstructor?

## Registration Symmetry

8. [ ] For every registration (`addListener`, `register`, `subscribe`, `put` into registry, `addMetric`): is there a matching removal on every shutdown/close/destroy path — both success AND failure?
   → Also: if multiple registrations happen in sequence, does cleanup remove all even if one throws?

9. [ ] Is a self-registering class (`static final INSTANCE = new Foo()`) actually LOADED from the startup path? If nothing references the class, static init never runs, registrations are silently absent.

## Visibility

10. [ ] When a field/method is declared `private` or package-private: does any class outside the package or any subclass need access? `@VisibleForTesting` only widens to package-private, not beyond.
   → Also: was the member previously `protected` and narrowed by this patch? Check all subclasses.

## Accumulation

11. [ ] For `field = expression` where `field` tracks a running total across iterations: should it be `field += expression` instead? Check metric accumulators, counters, and aggregated stats in loops.

## Event & Dispatch Completeness

12. [ ] When a new enum constant or event type is added, is it handled in ALL switch statements, dispatch maps, and handler registrations?
   → Also: silent `default: break` may swallow events that should be errors.

13. [ ] Does a state machine handler (`onTimeout`, `onError`, `onRetry`) enqueue the appropriate follow-up action, or does it only log and return, leaving the state machine stuck?

14. [ ] When a new error code / exception class / response status is introduced, is it added to the error-MAPPING table (separate from the dispatch switch)? An unmapped code typically converts retriable to fatal — or fatal to silently-swallowed success.

## Method Binding & Refactoring

15. [ ] When a method adds a new parameter with a default-providing overload, do ALL callers that need the new behavior call the new signature? Old callers silently bind to the old overload.

16. [ ] After extracting code into a helper, does the call site retain an operation the helper also performs? (Double-write, double-close from extract-method refactoring.)

17. [ ] When a wrapper / delegate forwards a call to an inner implementation, are ALL arguments propagated — including "auxiliary" ones like tracing context, consistency level, auth credentials, client options — not just the primary value? A single layer that falls through to a no-arg overload silently drops caller intent.

18. [ ] When a code block is duplicated across versioned subdirectories (format variants, protocol-version-gated copies, legacy vs current implementations), has a bug-fix applied to one copy been propagated to every sibling? Grep for the fixed symbol across peer directories to find stale copies.

## Factory Routing

19. [ ] When a factory dispatches on a type parameter to create a concrete object: for each discriminator value, is the returned class CORRECT? After consolidating multiple factories, verify each value produces the same type as before.

20. [ ] If a class has multiple constructors or factory paths (primary vs secondary, from-bytes vs from-memory, copy-of vs fresh), does each path perform every required registration / validation / post-construct setup step, or is a required step skipped in the secondary path?

## Constants

21. [ ] Does this code call `Foo.bar()` where `Foo.BAR` (constant) is correct, or vice versa? Does it call the wrong overload when a more specific one enforces a required value?

22. [ ] Does a constructor accept a parameter that is never assigned to a field, silently discarding it? Does a field's inline initializer assign a constant that the constructor never overwrites, so caller-supplied values vanish?
