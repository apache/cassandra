# API Completeness & Contracts — Specialist Checklist

15 highest-signal questions.

---

## Interface & Override Completeness

1. [ ] When a new class implements an interface or extends an abstract class, does it override every behavioral predicate method (`isX()`, `hasX()`, `canX()`) whose default (false/no-op) is wrong for this implementation?
   → Also: list ALL abstract/default methods; a class with TTL must override `isExpired()`.

2. [ ] Does a new class override all lifecycle methods (`close()`, `release()`, `abort()`) when it holds resources the parent doesn't know about?

## Field Completeness

3. [ ] When a new field is added to a class, is it present in: serialize/deserialize/serializedSize, equals/hashCode, toString, copy constructors, builder `build()`, describe/toMap?

4. [ ] Does a descriptor/serializer method include ALL fields that sibling methods include? Compare field-by-field against `equals`, `hashCode`, `toString`, `serialize`.

## Registration Symmetry

5. [ ] For every registration (`addListener`, `register`, `subscribe`, `put` into registry, `addMetric`): is there a matching removal on every shutdown/close/destroy path — both success AND failure?
   → Also: if multiple registrations happen in sequence, does cleanup remove all even if one throws?

6. [ ] Is a self-registering class (`static final INSTANCE = new Foo()`) actually LOADED from the startup path? If nothing references the class, static init never runs, registrations are silently absent.

## Visibility

7. [ ] When a field/method is declared `private` or package-private: does any class outside the package or any subclass need access? `@VisibleForTesting` only widens to package-private, not beyond.
   → Also: was the member previously `protected` and narrowed by this patch? Check all subclasses.

## Accumulation

8. [ ] For `field = expression` where `field` tracks a running total across iterations: should it be `field += expression` instead? Check metric accumulators, counters, and aggregated stats in loops.

## Event & Dispatch Completeness

9. [ ] When a new enum constant or event type is added, is it handled in ALL switch statements, dispatch maps, and handler registrations?
   → Also: silent `default: break` may swallow events that should be errors.

10. [ ] Does a state machine handler (`onTimeout`, `onError`, `onRetry`) enqueue the appropriate follow-up action, or does it only log and return, leaving the state machine stuck?

## Method Binding & Refactoring

11. [ ] When a method adds a new parameter with a default-providing overload, do ALL callers that need the new behavior call the new signature? Old callers silently bind to the old overload.

12. [ ] After extracting code into a helper, does the call site retain an operation the helper also performs? (Double-write, double-close from extract-method refactoring.)

## Factory Routing

13. [ ] When a factory dispatches on a type parameter to create a concrete object: for each discriminator value, is the returned class CORRECT? After consolidating multiple factories, verify each value produces the same type as before.

## Constants

14. [ ] Does this code call `Foo.bar()` where `Foo.BAR` (constant) is correct, or vice versa? Does it call the wrong overload when a more specific one enforces a required value?

15. [ ] Does a constructor accept a parameter that is never assigned to a field, silently discarding it?
