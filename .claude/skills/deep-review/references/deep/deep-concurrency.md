# Deep Concurrency & State — Extended Checklist
---

## Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Thread model**: Which threads access this object? Is it confined to one thread?
2. **Lock inventory**: What locks protect this state? What is the lock ordering?
3. **Lifecycle**: Who creates, starts, stops, and destroys this object?
4. **Shared state**: What fields are read/written from multiple threads?
5. **State machine**: What states can this object be in? What transitions are valid?

---

## TOCTOU & Atomicity Depth

### TOCTOU on shared mutable state (weight: high)
-  Is a field read in one step and acted upon in a separate unsynchronized step?
-  Is a boolean flag checked then used without holding a lock across both operations?
-  Is a state-machine check separate from the state transition?
-  Is an existence check separate from the create/insert operation?
-  Is a volatile field or concurrent collection read twice without local capture? Concurrent nullification between check and use.

### Non-atomic compound operations (weight: high)
-  Are get-then-put, check-then-act operations on ConcurrentHashMap done without external lock?
-  Are operations across multiple concurrent maps done without a shared lock?
-  Is `putIfAbsent` return value ignored, with caller using its own argument?

### Producer-consumer aliasing (weight: medium)
-  Does the producer retain a reference to an object after handing it off via a queue?
-  After `queue.put(obj)`, does the producer continue modifying `obj`?

### Atomic swap with concurrent writers still holding old instance (weight: low)
-  After `AtomicReference.getAndSet(newCollector)`, is the old collector drained on a path that can race with writers who already resolved to the old reference?
-  Does a "replace landmark" / "replace accumulator" pattern leave concurrent updaters writing to the retired instance, dropping or duplicating entries?

### Snapshot-under-lock defeated by downstream unsynchronized re-read (weight: low)
-  Is a shared field captured into a local under lock, but a subsequent call in the same critical section re-reads the original unsynchronized reference (e.g., `this.field.foo()` instead of `local.foo()`)?
-  Does a helper called with the local variable internally dereference the owning object's field directly?

---

## Collection Concurrency Depth

### ConcurrentModificationException (weight: high)
-  Is a for-each loop iterating a shared mutable collection while another thread modifies it?
-  Is `collection.remove()` called inside a for-each over the same collection?
-  Is a method inside the loop body modifying the same collection being iterated?

### Unsynchronized reads of shared collections (weight: high)
-  Is a shared `HashMap`/`ArrayList` written under synchronization but read without?
-  Is `.keySet()`, `.entrySet()`, or `.values()` returned as a live view without copying?
-  Is the collection iterated outside the lock that protects it?

### Live view iteration (specific pattern)
-  Does this code iterate `map.entrySet()` or `list.subList()` without copying first?
-  Does a getter return internal state (Map, Set, List field) that's iterated outside the lock?

### Shared ByteBuffer position mutated as read side-effect (weight: high)
-  Is a shared or pooled `ByteBuffer` read via relative `get()` / relative-get helpers without calling `duplicate()` or `slice()` first?
-  Does a deserialization helper return a reference to a static/singleton buffer rather than a fresh copy, letting callers permanently exhaust it?
-  Does a comparator or serializer advance position on a caller-supplied buffer and then try to rewind/flip, leaving concurrent observers exposed?
-  Is a heap ByteBuffer sliced but `arrayOffset()` omitted when indexing into the backing array?

---

## Visibility & Memory Model Depth

### Missing volatile (weight: medium)
-  Is a non-final, non-volatile shared field written on one thread and read on another?
-  Is a lazily-initialized cached field shared across threads without volatile?
-  Is a mutable singleton reference shared without visibility guarantee?

### Signal before publish (weight: medium)
-  Does a latch countDown or future complete fire BEFORE the data structures woken threads will read are fully updated?
-  Is the data published with a release fence (volatile write, synchronized exit)?

### Comparator reads live state (weight: medium)
-  Does a Comparator read from a volatile, shared, or continuously-updating field during sort?
-  Could concurrent writes change values mid-sort, violating transitivity?

---

## Lifecycle & Initialization Depth

### Constructor publishes this (weight: medium)
-  Does the constructor spawn a thread capturing `this`?
-  Does the constructor register a listener, management callback, or observer with `this`?
-  Does the constructor start an executor that references `this`?

### Double-checked locking (weight: medium)
-  Is the field re-read after leaving the synchronized block?
-  Are null-check and value-read separate field accesses?
-  Could another thread observe an intermediate state?

### Listener registration timing (weight: medium)
-  Is an event listener registered AFTER an initial state snapshot is taken?
-  Is a listener registered AFTER the async operation is initiated?
-  Is there a window where events between snapshot and registration are missed?

### Shutdown ordering (weight: high)
-  Does shutdown mirror startup in reverse order?
-  Is a flag set before the guarded operation completes?
-  Do static initializers fire too early?
-  Do offline tools assume a live cluster?

### Non-blocking stop signal followed by destructive state change (weight: medium)
-  Does shutdown send a "stop" flag/signal to a background task and then proceed to clear, truncate, or delete the state the task reads/writes without joining, awaiting, or confirming the task exited?
-  Does a feature-disable path unregister or null-out collaborators while periodic tasks are still mid-iteration?

### Reference count not acquired before concurrent read (weight: high)
-  Does a reader access a shared reference-counted resource without first acquiring a reference?
-  Can a concurrent evictor / deleter / compactor free the resource between the null / existence check and the read, causing use-after-free?
-  Is the refcount increment combined with a CAS such that a losing participant leaves the refcount inflated?

---

## Counter & Accounting Depth

### Increment without matching decrement (weight: medium)
-  Is a counter incremented before an operation that can fail?
-  Is the rollback path present on every failure branch?
-  Is the counter decremented before associated cleanup finishes?

### Metric at wrong point (weight: high)
-  Is a metric updated before the operation it measures (early-exit paths inflate)?
-  Is a metric updated after a different operation (measures wrong thing)?
-  Is a metric outside the guard that controls the measured operation?
-  Is the metric in an estimation loop that diverges from the actual selection loop?

### Size/count accumulator gaps (pattern)
-  Is a size accumulator updated in SOME write paths but not ALL?
-  Do tombstones, metadata, and index blocks all contribute to the accumulator?

---

## State Machine Depth

### Missing side-effect in branch (weight: high)
-  List ALL branches of the state-machine handler. Does every branch perform the required side-effect?
-  Specifically check: gossip sync, metric update, status propagation, transport stop.

### State cleanup (weight: high)
-  Does reset/clear/truncate update EVERY companion structure?
-  Does lazy-init `if (x == null) x = init()` conflate "not yet initialized" with "already closed"?
-  Does a success path miss cleanup that the error path has?
-  Does persistent state get written when operation starts rather than commits?
-  Does a periodic task short-circuit on `!enabled` without retracting published state?

---

## Deadlock Depth

### Blocking get in handler (weight: low)
-  Does `future.get()` inside an RPC verb handler starve the handler thread pool?
-  Does the completion of the awaited future depend on the same pool?

### Lock ordering (specific patterns)
-  Does a locked section delegate to a public API that independently acquires the same lock?
-  Does a `synchronized` method call into Netty/IO callback that re-enters the same lock?
-  Does `static synchronized` call code that loads another class with its own static init?

### Thread pool starvation (specific patterns)
-  Does `SynchronousQueue` with `CallerRunsPolicy` block the submitter?
-  Does a bounded blocking queue in Netty pipeline block I/O threads needed to drain it?

### Missing lock on shared metadata mutation (weight: low)
-  Does a metadata-change path skip a flush or compaction lock held by concurrent background tasks?
-  Can the background task write files, index entries, or offsets against a now-invalidated metadata object?

---

## Scope & Guard Mismatch Depth

### Scope mismatch (weight: high)
-  Is a teardown step outside the `if` block of the operation it relates to?
-  Is a notification unconditional when it should be inside a conditional?
-  Is an invariant guard inside an optional branch instead of enclosing scope?
-  Is validation logic in one path (local-apply) but not another (remote-announce)?

### Conditional decrement chain (pattern)
-  Does a conditional decrement chain across multiple if/else branches miscount?
-  Is a dirty flag set after a write loop instead of inside it?

---

## Thread Pool & Dispatch Depth

### Wrong executor stage (weight: medium)
-  Is a message handler, task, or state mutation dispatched to the wrong executor stage?
-  Does it run outside its thread-confinement contract?

### Counter scope (weight: medium)
-  Is a counter increment outside the conditional brace, firing unconditionally?
-  Is it supposed to count only matching items but counts every iteration?

### Self-dispatch on confined / single-threaded executor (weight: medium)
-  Does code already running on a single-threaded executor (or thread-confined actor) submit new work to that same executor and block on the result?
-  Does a coordinator-and-replica handler that runs on one executor dispatch back to the same executor for the local replica?

---

## Reentrancy & Async Hazards

### Reentrancy (weight: low)
-  Does a method holding a lock call back into code that modifies the same shared state?
-  Is this common in cancellation and eviction paths?

### ThreadLocal across async boundary (weight: low)
-  Is thread-local state (tracing, request context) set on submitter but read in async task?
-  Could the submitter clear the thread-local before the task reads it?

### CAS retry safety (weight: low)
-  Is an unbounded CAS retry loop used where contention is not brief?
-  Does a boolean CAS elect a single writer while others continue with stale state?

### Interrupt mid-execution corrupts shared I/O channel (weight: low)
-  Can a task be interrupted while holding or using a shared network channel?
-  Does a subsequent blocking call on the shared channel throw `ClosedByInterruptException`, propagating the interrupt beyond the interrupted task's scope?
-  Should the channel be dedicated per-task or the interrupt absorbed locally?

---

## Mutability and Immutability Violations

### Immutable collection returned where caller expects mutable (weight: low)
A method returns `Arrays.asList()`, `Collections.singletonList()`, or an unmodifiable list but the caller calls `add()` or `remove()` on it, throwing `UnsupportedOperationException`.

---

## Specific Concurrency Patterns

### Version-gated conditional field read or written in only one branch (weight: high)
A conditional field guarded by message variant/type/status appears in only one branch of serialize or deserialize, serializedSize early-return bypasses shared trailing field, protocol version not threaded through to collection element decoder, flag consumed before conditional early-return, or version-gated field written redundantly without matching deserialize logic.

### Configuration option silently dropped: null passed where config should flow (weight: high)
A config knob is parsed but not threaded to the builder: cipher suites passed as null, streaming encryption settings dropped, max frame size not set.

### Throwable.getMessage(), getCause(), or empty stack trace used without null check (weight: medium)
`getMessage()` returns null for no-arg constructors; `getCause()` is null with no chained cause; stack trace array has zero elements with `writableStackTrace=false`.

### Copy-paste error: duplicate column name, duplicate code block (weight: medium)
A copy-pasted block references the same variable, column name, or literal twice instead of the intended distinct values.

### Atomic Counter and Collection Out of Sync (weight: medium)
A liveness or completion check uses an atomic counter while the actual data lives in a separate collection; the counter advances ahead due to CPU reordering.

### Race Between Registration and Resource Readiness (weight: medium)
A resource tracking set registration or availability marker is set before or after the resource is actually ready.

### Exception Handler Catch Type Mismatch Bypasses Cleanup (weight: medium)
A try-block allocates a pooled resource and the catch block only handles one exception type; exceptions thrown as a different type escape the catch entirely.

### Getter method mutates state (weight: low)
A method named `getCompletedTasks()` calls `incrementAndGet()` on an atomic counter instead of `get()`.

### Thread-Local Used for Instance-Scoped State (weight: low)
A ThreadLocal caches a value that logically belongs to a specific object instance; when multiple instances share threads, the cached value cross-contaminates.

### Shared Cursor / Traversal State at Instance Level (weight: low)
A class holds a mutable traversal cursor as an instance field; concurrent callers corrupt each other's state.

### Decrement Before Cleanup Completes (weight: low)
A resource-admission counter is decremented before associated cleanup (socket close, thread exit) finishes.

### putIfAbsent Return Value Ignored (weight: low)
`putIfAbsent` called without checking its return value; subsequent code operates on newly created object instead of the winner.

### Idempotency Not Handled in Retry / Re-Entry Path (weight: low)
An operation that can be retried does not distinguish where idempotency is required from where a duplicate indicates a bug.

### Global / Static Flag Toggled in Test Without finally Reset (weight: low)
A global flag toggled before assertions is reset at end of try body instead of `finally`; assertion failures leave it dirty.

### Singleton Response Object Reused Across Concurrent Requests (weight: low)
A protocol message class carrying per-request mutable fields is exposed as a static singleton.

### Concurrent Directory Creation Race (weight: low)
`mkdirs()` result drives an error path without considering a concurrent creator could race.

### Lock Acquired But Critical Work Done Between lock() and try (weight: low)
Mutations are placed between `lock.lock()` and the paired `try` block, running outside the critical section.

### finally Block Throws, Suppressing Cleanup of Subsequent Resources (weight: low)
A call that can throw inside `finally` suppresses the original exception and prevents subsequent cleanup.

### Losing Race Participant Does Not Dispose Resource (weight: low)
An object holding a resource is constructed before putIfAbsent; the loser does not dispose.

### Cache-line contention in array of independently-updated counters (weight: low)
Adjacent atomic counters suffer false sharing. Check for padding or striped design.

### Fire-and-forget async operation with no in-flight deduplication (weight: low)
An async fetch submitted to an executor is not tracked; a second caller triggers a parallel operation.

### Broadcast invalidation causes thundering-herd recomputation (weight: low)
A cache invalidation broadcast causes all holders to race to repopulate concurrently.

---

## State Cleanup — Concurrency-Domain Patterns

### Coupled Data Structures Only Partially Reset / Cleared (weight: high)
When "reset", "clear", or "truncate" touches one of several coupled structures, some are missed — derived counters, companion maps, version trackers, or cached unions.

### Merge or library-upgrade breaks interface method signature (weight: high)
After a merge, one or more implementing classes retain the old signature; without `@Override`, the mismatch is silent.

### Success Path Missing Cleanup That Error Path Has (weight: medium)
Cleanup logic exists in the error handler but is missing from the normal completion path.

### Stale Snapshot Used for Deletion / Cleanup Logic (weight: medium)
Code that computes "what changed" by diffing snapshots misses changes when a snapshot is skipped.

### Virtual method called from base-class constructor returns stale default (weight: low)
An overridable method called from the base-class constructor returns a default because the subclass hasn't finished initialization.

### Open Marker / Cursor Cleared Between Segments (weight: low)
An iterator state machine resets "currently open" state as side-effect of reading it, losing information spanning iteration boundaries.

### Wrapper class enable/disable lifecycle not propagated to inner delegates (weight: low)
State changes only affect the wrapper's flag, not the delegates.

---

### Guard removal unsafe for mixed-mode rolling upgrade (weight: medium)
A guard is removed, but older nodes in a rolling upgrade still depend on the guarded behavior.

### Clear-and-refill on shared collection creates race window (weight: low)
A shared collection is cleared then refilled; concurrent readers between clear and refill see empty state.

### Constructor parameter accepted but never stored or used (weight: low)
A constructor accepts a parameter that is never assigned to a field.
