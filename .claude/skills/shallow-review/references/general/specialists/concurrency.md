# Concurrency & State — Specialist Checklist

16 highest-signal questions. 16% of all bugs fall in this domain.

---

## Races & Atomicity

1. [ ] Does this code read a shared field then conditionally write without holding a lock across both? (TOCTOU — check-then-act without atomicity.)
   → Also: compound operations on ConcurrentHashMap (get-then-put, check-then-act) across multiple maps need external synchronization.

2. [ ] Does this code iterate a shared mutable collection returned from a getter (`map.entrySet()`, `transaction.originals()`, `list.subList()`) without copying it first?
   → Also: `collection.remove()` inside for-each over same collection → ConcurrentModificationException.

3. [ ] Does `putIfAbsent()` return value get ignored, with caller using its own argument instead of the race winner?

4. [ ] Does a signal/notify (latch countDown, future complete) fire BEFORE the data structures that woken threads will read have been fully updated?

## Visibility & Fields

5. [ ] Does a non-final, non-volatile shared field get written on one thread and read on another without a lock?
   → Also: shared field read twice without local capture → concurrent nullification between check and use.

6. [ ] Does a `Comparator` read from a live mutable field during sort? Concurrent writes violate transitivity.

## Locking & Deadlocks

7. [ ] Does a `synchronized` method call into another class that also has `synchronized`? Trace every path from within a `synchronized` block — if any blocks on I/O or acquires another lock, flag it.
   → Also: `static synchronized` calling code that loads another class → static initializer deadlock.

8. [ ] Does a message handler or task block on `future.get()` whose completion depends on the same thread pool?

## Lifecycle & Ordering

9. [ ] Does a constructor spawn a thread, register a JMX bean, or start an executor capturing `this` before the constructor finishes?

10. [ ] Does this code register a listener AFTER the event-producing operation has started? Register first, then read state.

## State Management

11. [ ] Does this code unconditionally `map.put(key, newValue)` where the map may already contain state that should be preserved or merged?
    → Also: creating a new object to represent an entity without checking whether a previous instance exists.

12. [ ] Does a reset/truncate/clear path update EVERY companion field atomically? (map + counter, union cache + source collections, dirty flag + buffer offset)

13. [ ] Does a teardown step, accounting update, or notification sit OUTSIDE the same `if` block as the operation it relates to? (Scope mismatch.)

## Counters & Accounting

14. [ ] Does this code increment a counter before an operation that may fail, without rollback on failure?
    → Also: success path missing cleanup that error path performs (session removal, counter decrement, map eviction).

## State Machines

15. [ ] Does a state-machine handler have branches that omit a required side-effect that all other branches perform?
    → Also: periodic task short-circuits on `!enabled` without retracting previously published state.

16. [ ] Does `condition.await()` or `Object.wait()` lack a `while(predicate)` loop guard against spurious wakeups?

---

## False Positives — Do NOT Flag

- CopyOnWriteArrayList/ConcurrentHashMap iteration (designed for safe concurrent access)
- Immutable `final` field set in constructor then read from other threads
- Volatile boolean flag for simple shutdown signal
- ThreadLocal in request-scoped code without async handoff
