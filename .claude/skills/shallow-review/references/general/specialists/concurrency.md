# Concurrency & State — Specialist Checklist

24 highest-signal questions. 16% of all bugs fall in this domain.

---

## Races & Atomicity

1. [ ] Does this code read a shared field then conditionally write without holding a lock across both? (TOCTOU — check-then-act without atomicity.)
   → Also: compound operations on ConcurrentHashMap (get-then-put, check-then-act) across multiple maps need external synchronization.

2. [ ] Does this code iterate a shared mutable collection returned from a getter (`map.entrySet()`, `transaction.originals()`, `list.subList()`) without copying it first?
   → Also: `collection.remove()` inside for-each over same collection → ConcurrentModificationException.

3. [ ] Does `putIfAbsent()` return value get ignored, with caller using its own argument instead of the race winner?

4. [ ] Does a signal/notify (latch countDown, future complete) fire BEFORE the data structures that woken threads will read have been fully updated? Does a cache invalidation / tombstone broadcast fire BEFORE the underlying write has committed, letting a racing reader repopulate the cache with the pre-change value?

5. [ ] Does this code reuse a shared / pooled / singleton `ByteBuffer` via relative `get()` / `read()` without first calling `duplicate()` or `slice()`? Position advances as a side effect and corrupts subsequent readers on the same thread or concurrent observers.

6. [ ] Does a reader access a shared reference-counted resource (off-heap buffer, SSTable, Chunk, cache entry) without acquiring a refcount BEFORE the read? A concurrent evictor can free the resource between existence check and use.

7. [ ] After an atomic swap replaces a shared mutable collector / accumulator / landmark, can producers that already resolved to the OLD reference continue writing to the retired instance before it is drained?

8. [ ] Does a commit / compare-and-swap path re-read a version token or precondition value instead of reusing the one captured at the start of the transaction? A concurrent modification between the two reads bypasses conflict detection.

## Visibility & Fields

9. [ ] Does a non-final, non-volatile shared field get written on one thread and read on another without a lock?
   → Also: shared field read twice without local capture → concurrent nullification between check and use.

10. [ ] Does a `Comparator` read from a live mutable field during sort? Concurrent writes violate transitivity.

11. [ ] Is a shared field captured into a local under lock, but the downstream call inside the same critical section re-reads the original unsynchronized reference (e.g., `this.field.foo()` instead of `local.foo()`)? The snapshot is defeated.

## Locking & Deadlocks

12. [ ] Does a `synchronized` method call into another class that also has `synchronized`? Trace every path from within a `synchronized` block — if any blocks on I/O or acquires another lock, flag it.
   → Also: `static synchronized` calling code that loads another class → static initializer deadlock.

13. [ ] Does a message handler or task block on `future.get()` whose completion depends on the same thread pool?
   → Also: code running on a single-threaded / confined executor submitting new work to that SAME executor and blocking on the result.

14. [ ] Does a schema / metadata mutation path skip the per-store flush or compaction lock held by background tasks? Concurrent flushes can otherwise write files/index entries for a now-invalidated schema.

## Lifecycle & Ordering

15. [ ] Does a constructor spawn a thread, register a management callback, or start an executor capturing `this` before the constructor finishes?

16. [ ] Does this code register a listener AFTER the event-producing operation has started? Register first, then read state.

17. [ ] Does a shutdown step send a non-blocking "stop" flag/signal to a background task and then proceed to clear, truncate, or delete state the task depends on — without joining or awaiting the task? Does a coordinator process replies before the dispatch loop has finished registering all expected recipients?

## State Management

18. [ ] Does this code unconditionally `map.put(key, newValue)` where the map may already contain state that should be preserved or merged?
    → Also: creating a new object to represent an entity without checking whether a previous instance exists.

19. [ ] Does a reset/truncate/clear path update EVERY companion field atomically? (map + counter, union cache + source collections, dirty flag + buffer offset)

20. [ ] Does a teardown step, accounting update, or notification sit OUTSIDE the same `if` block as the operation it relates to? (Scope mismatch.)

21. [ ] Does a query / read consult only an immutable snapshot collection while a parallel "live current" pointer exists that has not yet been registered into the snapshot? A registration window returns a stale or empty answer.

## Counters & Accounting

22. [ ] Does this code increment a counter before an operation that may fail, without rollback on failure?
    → Also: success path missing cleanup that error path performs (session removal, counter decrement, map eviction).

## State Machines

23. [ ] Does a state-machine handler have branches that omit a required side-effect that all other branches perform?
    → Also: periodic task short-circuits on `!enabled` without retracting previously published state.

24. [ ] Does `condition.await()` or `Object.wait()` lack a `while(predicate)` loop guard against spurious wakeups?

---

## False Positives — Do NOT Flag

- CopyOnWriteArrayList/ConcurrentHashMap iteration (designed for safe concurrent access)
- Immutable `final` field set in constructor then read from other threads
- Volatile boolean flag for simple shutdown signal
- ThreadLocal in request-scoped code without async handoff
