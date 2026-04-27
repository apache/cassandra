# Deep Concurrency & State — Extended Checklist

Full-depth checklist for deep review. Extends the shallow 16-item specialist checklist with
all 75+ concurrency patterns from the catalog. Use when the reviewer has identified specific
files for deep investigation.

---

## Phase 0: Context Gathering (REQUIRED)

Before applying the checklist, read the TARGET FILES (not just the diff) to understand:

1. **Thread model**: Which threads access this object? Is it confined to one thread?
2. **Lock inventory**: What locks protect this state? What is the lock ordering?
3. **Lifecycle**: Who creates, starts, stops, and destroys this object?
4. **Shared state**: What fields are read/written from multiple threads?
5. **State machine**: What states can this object be in? What transitions are valid?

---

## TOCTOU & Atomicity Depth

### TOCTOU on shared mutable state (14 known bugs)
- [ ] Is a field read in one step and acted upon in a separate unsynchronized step?
- [ ] Is a boolean flag checked then used without holding a lock across both operations?
- [ ] Is a state-machine check separate from the state transition?
- [ ] Is an existence check separate from the create/insert operation?
- [ ] Is a volatile field or concurrent collection read twice without local capture? Concurrent nullification between check and use.

### Non-atomic compound operations (7 known bugs)
- [ ] Are get-then-put, check-then-act operations on ConcurrentHashMap done without external lock?
- [ ] Are operations across multiple concurrent maps done without a shared lock?
- [ ] Is `putIfAbsent` return value ignored, with caller using its own argument?

### Producer-consumer aliasing (3 known bugs)
- [ ] Does the producer retain a reference to an object after handing it off via a queue?
- [ ] After `queue.put(obj)`, does the producer continue modifying `obj`?

---

## Collection Concurrency Depth

### ConcurrentModificationException (11 known bugs)
- [ ] Is a for-each loop iterating a shared mutable collection while another thread modifies it?
- [ ] Is `collection.remove()` called inside a for-each over the same collection?
- [ ] Is a method inside the loop body modifying the same collection being iterated?

### Unsynchronized reads of shared collections (9 known bugs)
- [ ] Is a shared `HashMap`/`HashBiMap`/`ArrayList` written under synchronization but read without?
- [ ] Is `.keySet()`, `.entrySet()`, or `.values()` returned as a live view without copying?
- [ ] Is the collection iterated outside the lock that protects it?

### Live view iteration (specific pattern)
- [ ] Does this code iterate `map.entrySet()`, `transaction.originals()`, or `list.subList()` without copying first?
- [ ] Does a getter return internal state (Map, Set, List field) that's iterated outside the lock?

---

## Visibility & Memory Model Depth

### Missing volatile (5 known bugs)
- [ ] Is a non-final, non-volatile shared field written on one thread and read on another?
- [ ] Is a lazily-initialized cached field shared across threads without volatile?
- [ ] Is a mutable singleton reference shared without visibility guarantee?

### Signal before publish (5 known bugs)
- [ ] Does a latch countDown or future complete fire BEFORE the data structures woken threads will read are fully updated?
- [ ] Is the data published with a release fence (volatile write, synchronized exit)?

### Comparator reads live state (5 known bugs)
- [ ] Does a Comparator read from a volatile, shared, or continuously-updating field during sort?
- [ ] Could concurrent writes change values mid-sort, violating transitivity?

---

## Lifecycle & Initialization Depth

### Constructor publishes this (4 known bugs)
- [ ] Does the constructor spawn a thread capturing `this`?
- [ ] Does the constructor register a JMX bean, listener, or callback with `this`?
- [ ] Does the constructor start an executor that references `this`?

### Double-checked locking (4 known bugs)
- [ ] Is the field re-read after leaving the synchronized block?
- [ ] Are null-check and value-read separate field accesses?
- [ ] Could another thread observe an intermediate state?

### Listener registration timing (4 known bugs)
- [ ] Is an event listener registered AFTER an initial state snapshot is taken?
- [ ] Is a listener registered AFTER the async operation is initiated?
- [ ] Is there a window where events between snapshot and registration are missed?

### Shutdown ordering (10 known bugs)
- [ ] Does shutdown mirror startup in reverse order?
- [ ] Is a flag set before the guarded operation completes?
- [ ] Do static initializers fire too early?
- [ ] Do offline tools assume a live cluster?

---

## Counter & Accounting Depth

### Increment without matching decrement (4 known bugs)
- [ ] Is a counter incremented before an operation that can fail?
- [ ] Is the rollback path present on every failure branch?
- [ ] Is the counter decremented before associated cleanup finishes?

### Metric at wrong point (7 known bugs)
- [ ] Is a metric updated before the operation it measures (early-exit paths inflate)?
- [ ] Is a metric updated after a different operation (measures wrong thing)?
- [ ] Is a metric outside the guard that controls the measured operation?
- [ ] Is the metric in an estimation loop that diverges from the actual selection loop?

### Size/count accumulator gaps (pattern)
- [ ] Is a size accumulator updated in SOME write paths but not ALL?
- [ ] Do tombstones, metadata, and index blocks all contribute to the accumulator?

---

## State Machine Depth

### Missing side-effect in branch (8 known bugs)
- [ ] List ALL branches of the state-machine handler. Does every branch perform the required side-effect?
- [ ] Specifically check: gossip sync, metric update, status propagation, transport stop.

### State cleanup (10 known bugs)
- [ ] Does reset/clear/truncate update EVERY companion structure?
- [ ] Does lazy-init `if (x == null) x = init()` conflate "not yet initialized" with "already closed"?
- [ ] Does a success path miss cleanup that the error path has?
- [ ] Does persistent state get written when operation starts rather than commits?
- [ ] Does a periodic task short-circuit on `!enabled` without retracting published state?

---

## Deadlock Depth

### Blocking get in handler (2 known bugs)
- [ ] Does `future.get()` inside an RPC verb handler starve the handler thread pool?
- [ ] Does the completion of the awaited future depend on the same pool?

### Lock ordering (specific patterns)
- [ ] Does a locked section delegate to a public API that independently acquires the same lock?
- [ ] Does a `synchronized` method call into Netty/IO callback that re-enters the same lock?
- [ ] Does `static synchronized` call code that loads another class with its own static init?

### Thread pool starvation (specific patterns)
- [ ] Does `SynchronousQueue` with `CallerRunsPolicy` block the submitter?
- [ ] Does a bounded blocking queue in Netty pipeline block I/O threads needed to drain it?

---

## Distributed Systems Depth

### Topology checks (3 known bugs)
- [ ] Does a node-count/RF check aggregate across DCs instead of per-DC?
- [ ] Does a loop over replicas send to self without `isSelf()` check?
- [ ] Does a speculative retry select from already-contacted replicas?

### Address representation (2 known bugs)
- [ ] Does address comparison mix IP vs IP+port vs DNS representations?
- [ ] Are token-based lookups consistent with the address type used for registration?

### Consensus & coordination (3 known bugs)
- [ ] Does a "read-only" consensus path skip commit without advancing a durable marker?
- [ ] Does a store-then-remove pattern stall because some paths skip acknowledgement?
- [ ] Does a replica selection loop set source without `break`, selecting all subsequent replicas?

### Rolling upgrade safety (2 known bugs)
- [ ] Does guard removal assume all nodes are at the same version?
- [ ] Does a new schema entity serialize on all gossip/exchange paths?

---

## Scope & Guard Mismatch Depth

### Scope mismatch (12 known bugs)
- [ ] Is a teardown step outside the `if` block of the operation it relates to?
- [ ] Is a notification unconditional when it should be inside a conditional?
- [ ] Is an invariant guard inside an optional branch instead of enclosing scope?
- [ ] Is validation logic in one path (local-apply) but not another (remote-announce)?

### Conditional decrement chain (pattern)
- [ ] Does a conditional decrement chain across multiple if/else branches miscount?
- [ ] Is a dirty flag set after a write loop instead of inside it?

---

## Thread Pool & Dispatch Depth

### Wrong executor stage (5 known bugs)
- [ ] Is a message handler, task, or state mutation dispatched to the wrong executor stage?
- [ ] Does it run outside its thread-confinement contract?

### Counter scope (4 known bugs)
- [ ] Is a counter increment outside the conditional brace, firing unconditionally?
- [ ] Is it supposed to count only matching items but counts every iteration?

---

## Reentrancy & Async Hazards

### Reentrancy (2 known bugs)
- [ ] Does a method holding a lock call back into code that modifies the same shared state?
- [ ] Is this common in cancellation and eviction paths?

### ThreadLocal across async boundary (2 known bugs)
- [ ] Is thread-local state (tracing, request context) set on submitter but read in async task?
- [ ] Could the submitter clear the thread-local before the task reads it?

### CAS retry safety (2 known bugs)
- [ ] Is an unbounded CAS retry loop used where contention is not brief?
- [ ] Does a boolean CAS elect a single writer while others continue with stale state?

---

## Mutability and Immutability Violations (§8)

### Schema Object Mutated In-Place Instead of Through Builder (3 known bugs)
An immutable-by-contract schema object (TableMetadata, ColumnMetadata) is mutated in-place via a `set*` method instead of going through the unbuild/rebuild cycle, bypassing epoch-stamping and replication.

### Immutable collection returned where caller expects mutable (2 known bugs)
A method returns `Arrays.asList()`, `Collections.singletonList()`, or an unmodifiable list but the caller calls `add()` or `remove()` on it, throwing `UnsupportedOperationException`.

---

## Missing from §15: Concurrency — Specific Patterns

### Version-gated conditional field read or written in only one branch (9 known bugs)
A conditional field guarded by message variant/type/status appears in only one branch of serialize or deserialize, serializedSize early-return bypasses shared trailing field, protocol version not threaded through to collection element decoder, flag consumed before conditional early-return, or version-gated field written redundantly without matching deserialize logic.

### Configuration option silently dropped: null passed where config should flow (6 known bugs)
A config knob is parsed but not threaded to the builder: cipher suites passed as null, streaming encryption settings dropped, entireSSTable throttle CLI options not wired, max Thrift frame size not set.

### Concurrent topology/gossip state change makes endpoint lookup return null (5 known bugs)
A gossip-state, endpoint-to-hostId, DC/rack, or token-allocation lookup returns null because the node departed, was replaced, or has not yet joined. Background tasks and failure-detector code are most affected.

### Pooled / Off-Heap Buffer Not Released on Replacement or Error (5 known bugs)
A direct ByteBuffer or pooled buffer field is reassigned without calling `FileUtils.clean()` or returning it to the pool first, or a derived view is returned to the pool instead of the original tracked object.

### Throwable.getMessage(), getCause(), or empty stack trace used without null check (5 known bugs)
`getMessage()` returns null for no-arg constructors; `getCause()` is null with no chained cause; stack trace array has zero elements with `writableStackTrace=false`.

### Copy-paste error: duplicate column name, duplicate code block (3 known bugs)
A copy-pasted block references the same variable, column name, or literal twice instead of the intended distinct values.

### Atomic Counter and Collection Out of Sync (3 known bugs)
A liveness or completion check uses an atomic counter while the actual data lives in a separate collection; the counter advances ahead due to CPU reordering.

### Race Between Registration and Resource Readiness (3 known bugs)
A resource tracking set registration or availability marker is set before or after the resource is actually ready.

### Exception Handler Catch Type Mismatch Bypasses Cleanup (3 known bugs)
A try-block allocates a pooled resource and the catch block only handles one exception type; exceptions thrown as a different type escape the catch entirely.

### Getter method mutates state (2 known bugs)
A method named `getCompletedTasks()` calls `incrementAndGet()` on an atomic counter instead of `get()`.

### Liveness derived from row-level marker instead of live cells (2 known bugs)
Index or visibility code uses the row's `LivenessInfo` to decide whether to emit an entry, but the row-level marker can be expired while individual cells are live.

### Thread-Local Used for Instance-Scoped State (2 known bugs)
A ThreadLocal caches a value that logically belongs to a specific object instance; when multiple instances share threads, the cached value cross-contaminates.

### Shared Cursor / Traversal State at Instance Level (2 known bugs)
A class holds a mutable traversal cursor as an instance field; concurrent callers corrupt each other's state.

### Decrement Before Cleanup Completes (2 known bugs)
A resource-admission counter is decremented before associated cleanup (socket close, thread exit) finishes.

### putIfAbsent Return Value Ignored (2 known bugs)
`putIfAbsent` called without checking its return value; subsequent code operates on newly created object instead of the winner.

### Idempotency Not Handled in Retry / Re-Entry Path (2 known bugs)
An operation that can be retried does not distinguish where idempotency is required from where a duplicate indicates a bug.

### Global / Static Flag Toggled in Test Without finally Reset (2 known bugs)
A global flag toggled before assertions is reset at end of try body instead of `finally`; assertion failures leave it dirty.

### Singleton Response Object Reused Across Concurrent Requests (1 known bug)
A protocol message class carrying per-request mutable fields is exposed as a static singleton.

### Event Ordering Bug: DELETE Timestamp Can Beat INSERT (1 known bug)
A cache eviction listener issues a DELETE with a wall-clock timestamp; if eviction fires immediately after insertion, the DELETE wins.

### Concurrent Directory Creation Race (1 known bug)
`mkdirs()` result drives an error path without considering a concurrent creator could race.

### UUID / Ballot Generation Not Monotonic Under Contention (1 known bug)
Ballot generation using wall-clock time alone produces duplicate IDs when two threads execute within the same tick.

### Lock Acquired But Critical Work Done Between lock() and try (1 known bug)
Mutations are placed between `lock.lock()` and the paired `try` block, running outside the critical section.

### finally Block Throws, Suppressing Cleanup of Subsequent Resources (1 known bug)
A call that can throw inside `finally` suppresses the original exception and prevents subsequent cleanup.

### Losing Race Participant Does Not Dispose Resource (1 known bug)
An object holding a resource is constructed before putIfAbsent; the loser does not dispose.

### Cache-line contention in array of independently-updated counters (1 known bug)
Adjacent atomic counters suffer false sharing. Check for padding or striped design.

### Fire-and-forget async operation with no in-flight deduplication (1 known bug)
An async fetch submitted to an executor is not tracked; a second caller triggers a parallel operation.

### Broadcast invalidation causes thundering-herd recomputation (1 known bug)
A cache invalidation broadcast causes all holders to race to repopulate concurrently.

---

## State Cleanup — Concurrency-Domain Patterns (§19)

### Coupled Data Structures Only Partially Reset / Cleared (10 known bugs)
When "reset", "clear", or "truncate" touches one of several coupled structures, some are missed — derived counters, companion maps, version trackers, or cached unions.

### Merge or library-upgrade breaks interface method signature (10 known bugs)
After a merge, one or more implementing classes retain the old signature; without `@Override`, the mismatch is silent.

### Stale/Phantom Entries Survive After Lifecycle Transition (7 known bugs)
Records written during bootstrap, replacement, or failed repair persist after the phase ends.

### Stale or absent metadata used for decisions on moved/deleted entities (6 known bugs)
Iteration over recorded identifiers does not account for entities that no longer exist or have changed.

### Success Path Missing Cleanup That Error Path Has (5 known bugs)
Cleanup logic exists in the error handler but is missing from the normal completion path.

### Multi-phase consensus step skipped, allowing stale proposals (3 known bugs)
A "read-only" consensus path skips the commit phase without advancing a durable marker.

### Stale Snapshot Used for Deletion / Cleanup Logic (3 known bugs)
Code that computes "what changed" by diffing snapshots misses changes when a snapshot is skipped.

### Metadata object lookup uses stale/wrong schema object (3 known bugs)
DDL operations look up a ColumnDefinition from the original schema registry instead of the mutable working copy.

### Periodic Task Disablement Does Not Retract Published State (2 known bugs)
A feature-flag-gated periodic task short-circuits without retracting previously published state.

### Replay / Recovery Applies Stale Watermarks (2 known bugs)
Current watermarks are not applied to loaded data before making it visible.

### Virtual method called from base-class constructor returns stale default (1 known bug)
An overridable method called from the base-class constructor returns a default because the subclass hasn't finished initialization.

### Open Marker / Cursor Cleared Between Segments (1 known bug)
An iterator state machine resets "currently open" state as side-effect of reading it, losing information spanning iteration boundaries.

### Wrapper class enable/disable lifecycle not propagated to inner delegates (1 known bug)
State changes only affect the wrapper's flag, not the delegates.

---

## Bootstrap and Topology Timing (§28)

### Token registration before data transfer completes (3 known bugs)
The node is inserted into the live ring before bootstrap data is fully transferred.

### Guard removal unsafe for mixed-mode rolling upgrade (3 known bugs)
A guard is removed, but older nodes in a rolling upgrade still depend on the guarded behavior.

### Dead endpoints counted in barrier/latch sizing (2 known bugs)
A CountDownLatch is sized from all endpoints including dead ones, causing hangs.

### Schema version not checked before cross-node data transfer (2 known bugs)
Mutations shipped without verifying schema agreement corrupt data on incompatible schema.

### Gossip state deletion not propagated (2 known bugs)
Local state deletion is not propagated because the gossip protocol lacks a delete primitive.

### Clear-and-refill on shared collection creates race window (2 known bugs)
A shared collection is cleared then refilled; concurrent readers between clear and refill see empty state.

### Constructor parameter accepted but never stored or used (2 known bugs)
A constructor accepts a parameter that is never assigned to a field.
