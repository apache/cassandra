# Category: Concurrency and Locking

Bugs caused by races, ordering hazards, lock-discipline violations, publication-safety gaps, and circular waits between cooperating threads or async tasks.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- New or changed `synchronized`, `Lock`, `ReadWriteLock`, `ReentrantLock`, `mutex`, semaphore, latch, or condition-variable usage
- `volatile`, `Atomic*`, `AtomicReference`, `compareAndSet`, `getAndSet`, or memory-fence-related changes
- `Future`, `CompletableFuture`, `await`, `get()`, `join()`, callbacks chained on async results
- A new background thread, executor submission, scheduler, or task pool — especially with bounded queues or single-threaded executors
- Changes to publish-then-signal sequences (set flag / count-down / notify / complete a future after writing shared state)
- Iteration over a shared/concurrent collection without explicit lock acquisition, or methods returning live views of shared collections
- Lazy or double-checked initialization of fields read from multiple threads
- Reference-counted resource lifecycle calls (`acquire`, `release`, `retain`, refcount inc/dec)
- Cancellation, abort, shutdown, or close paths that interact with in-flight work
- Reading "current state" / membership / counters as separate steps from acting on them (check-then-act, observe-then-acquire)
- Comparators or sort keys that read mutable fields, metrics, or time
- Atomic swap-then-drain, swap-then-clear, or replace-then-process patterns

## Findings

### F-01: Atomic swap then drain window
A shared mutable collector is drained after atomically swapping in a replacement, but writers in flight when the swap occurred append to the old instance after the drain completes, dropping or duplicating entries.
**Look for:** A `getAndSet`/CAS replacing a mutable container followed by a separate read of the previous instance, with no fence or coordination ensuring writers have observed the swap before draining.

### F-02: Inconsistent lock acquisition order between components
Two cooperating components acquire each other's locks in opposite orders under concurrent execution, producing a deadlock.
**Look for:** Two methods on different classes that each call into the other while holding their own monitor; synchronized methods that invoke a peer's synchronized method.

### F-03: Cancel/abort path bypasses serialization queue under same lock
A cancellation path bypasses the normal serialized work queue and directly mutates shared state while already holding its governing lock, causing reentrant corruption.
**Look for:** A `cancel`/`abort`/`close` method that acquires the same lock used by the regular task pipeline and calls handlers inline rather than enqueuing.

### F-04: Coordinator awaits future whose precondition needs its own notification
A coordinator blocks on a future whose completion depends on a notification only the coordinator can send — classic self-wait deadlock.
**Look for:** `future.get()`/`await()` followed by code that would have triggered the resolving event; single-threaded executors blocked on tasks that need to run on the same executor.

### F-05: Self-deadlock by re-acquiring same monitor
Calling a public synchronized method from within a section already holding that monitor deadlocks if the lock is non-reentrant or recurses unsafely if it is.
**Look for:** Synchronized method that calls another synchronized method on the same singleton or class object.

### F-06: Lock held across blocking I/O whose callback needs the same lock
A coarse-grained lock is held while blocking on an I/O response whose completion handler also tries to acquire the same lock, deadlocking the calling thread and the I/O-completion thread.
**Look for:** `synchronized`/`lock.lock()` enclosing `future.get()`, `channel.read()`, RPC `await()`, or blocking queue `put()` whose handler acquires the same monitor.

### F-07: Mutable shared field neither final nor volatile
A field shared across threads is neither `final` nor `volatile`, so the JMM gives no visibility guarantee — readers may observe stale values indefinitely.
**Look for:** Plain non-final field assigned by one thread and read by others without synchronization or atomic wrapper.

### F-08: Publication ordering — signal set before guarded state ready
A "publishing" flag/counter signals readers that dependent state is ready, but the signal is assigned before the dependent fields, so a concurrent reader sees the signal while the data is still null. Equivalently: a condition variable is signalled before shared state is updated; an atomic counter reaches its threshold before the backing collection is fully published, so a woken thread observes fewer responses than expected.
**Look for:** A volatile/atomic write (counter `incrementAndGet() == N`, flag set, future complete, `signal()`, `countDown()`) executed before assignments to fields the consumer will read after observing the signal.

### F-09: TOCTOU between flag and guarded resource
A check of a flag and the subsequent retrieval of the resource it guards happen under no common lock; concurrent removal can observe the flag as true but find the resource null or destroyed.
**Look for:** `if (enabled) { resource.use(); }` or `if (map.containsKey(k)) map.get(k).foo()` with no lock spanning both calls.

### F-10: Comparator reads live mutable state, breaking total order
A comparator reads a field that mutates concurrently (a metric, rate, or shared map), so it returns inconsistent results across calls and violates the comparator contract — `IllegalArgumentException` from the sort.
**Look for:** Comparator implementations that dereference instance fields or shared maps mutated by other threads during the sort.

### F-11: Snapshot read inside lock, but downstream re-reads original
Inside a critical section, code reads a shared mutable reference into a local snapshot but then passes the original (un-snapshotted) reference downstream, defeating the snapshot.
**Look for:** `Foo local = this.foo; doStuff(this.foo);` instead of `doStuff(local);` inside a synchronized block.

### F-12: Double-checked field accessed twice with race between
Lazy-init / DCL pattern reads a nullable field twice with the null check on the first read and dereference on the second; a concurrent write to null between the reads NPEs despite the guard.
**Look for:** `if (this.field != null) this.field.method();` instead of capturing the field into a local first.

### F-13: Unsynchronized iteration of a shared collection
Iterating a non-thread-safe collection (`HashMap`, `ArrayList`) — or returning a live view of one — while another thread mutates it throws `ConcurrentModificationException` or yields torn reads. Even `Collections.synchronizedX` requires explicit external synchronization for iteration.
**Look for:** Fields of type `HashMap`/`ArrayList` accessed across threads; methods returning `map.values()`, `keySet()`, or `entrySet()` directly; iteration of synchronized wrappers without surrounding `synchronized(map)`.

### F-14: Cache invalidation fires before commit
A cache-invalidation notification is sent before the underlying state change is committed; a thread re-reading between invalidation and commit caches the pre-change value permanently.
**Look for:** `invalidate(k); store.put(k, v);` patterns instead of `store.put(k, v); invalidate(k);`.

### F-15: Compound check-then-act on concurrent collection
`isEmpty()` / `containsKey()` / `size()` on a concurrent collection followed by an action assuming the result still holds; concurrent modification between calls produces a stale view, NPE, or `NoSuchElementException`.
**Look for:** `if (!set.isEmpty()) set.first();`, `if (map.containsKey(k)) doX(map.get(k));`, `if (counter.get() > 0) decrement()`.

### F-16: Counter decremented before associated cleanup completes
A capacity / reference / in-flight counter is decremented before the resources tied to the slot are fully released; an admission loop sees the new capacity and submits work that races the still-running cleanup.
**Look for:** `counter.decrementAndGet()` followed by additional cleanup; release callbacks that update accounting before tearing down underlying resources.

### F-17: Eviction frees resource while consumers hold raw pointers
A cache eviction or release callback unconditionally frees native/off-heap memory while consumers hold raw references obtained without ref-counting, producing use-after-free.
**Look for:** Eviction listeners that `free`/`close` resources accessed via plain references rather than ref-count handles; reads of off-heap memory not preceded by `acquire()`/`retain()`.

### F-18: Observe-then-acquire window for shared resource
A resource view is observed and references are acquired in a separate step; a concurrent free between observation and acquisition causes use-after-free.
**Look for:** `Resource r = lookup(); r.acquire();` (instead of an atomic `tryAcquire`); or two-step retain patterns across a lock boundary.

### F-19: Cancellation signal is non-blocking; clearing proceeds before task finishes
A background task is signalled to stop but the signal is non-blocking; a destructive state-clearing operation proceeds before the task observes the signal, allowing the task to write into already-deleted state.
**Look for:** `task.cancel()` immediately followed by state mutation, with no `awaitTermination`/`join` between.

### F-20: Background thread started inside constructor — escape of `this`
A background thread is started inside a constructor; the thread observes a partially-constructed owner object before all fields are assigned.
**Look for:** `new Thread(...).start()` or executor submission inside a constructor, especially in non-final classes; passing `this` to external registries before the constructor completes.

### F-21: Shared mutable scratch buffer treated as instance-scoped
A scratch buffer, builder, or thread-local "hint" stored as an instance field is reset and overwritten by concurrent callers, corrupting each other's results.
**Look for:** Mutable byte buffers, `StringBuilder`s, or thread-locals declared as instance fields, used by multiple threads.

### F-22: Latch never decrements on exception path
A latch's `countDown()` is positioned after a setup block that can throw; on exception the latch is never decremented and the waiter hangs forever.
**Look for:** `countDown()` outside a `finally` block; especially after I/O, allocation, or constructor calls that may throw.

### F-23: Submission counter incremented before submit; rejection forgets decrement
An in-flight task counter is incremented before submission; the rejection-handling path omits the matching decrement, leaving the counter permanently inflated.
**Look for:** `pending.incrementAndGet(); executor.submit(task);` without a corresponding decrement in the catch/reject path.

### F-24: Bounded executor blocks producer; consumer needs producer thread
All threads in a bounded pool block on futures whose continuations are queued behind the blocking threads — pool deadlock. Equivalently: a synchronous-handoff queue blocks the submitter when the pool saturates.
**Look for:** Code submitting tasks that block on `future.get()` of other tasks dispatched to the same executor / single-threaded stage; `SynchronousQueue` in `ThreadPoolExecutor` constructors.

### F-25: CAS retry mutates source argument or rolls back via plain set
A CAS retry passes the source object directly to a mutating merge that modifies it in place; subsequent iterations operate on already-mutated data. Equivalently: a shared atomic counter is rolled back by a plain `set()` instead of a CAS.
**Look for:** `while (!ref.compareAndSet(old, merge(old, src)))` where `merge` mutates inputs; `counter.set(local + delta)` instead of `addAndGet`.

### F-26: CAS spin loop without progress-detection or backoff
An unbounded CAS retry has no fallback to blocking under contention; threads spin without progress, consuming CPU in a livelock.
**Look for:** `while (!field.compareAndSet(...))` with no iteration cap, no backoff, and no escape if the condition can never be met.

### F-27: Two correlated values updated non-atomically
Two paired counters / fields representing one logical pair are updated and read in separate unsynchronized steps; a torn read produces inconsistent state (e.g., zero denominator in a ratio, or one map updated and the sibling stale).
**Look for:** Two `Atomic*` fields or two related maps mutated in separate statements rather than under a common lock or atomic record.

### F-28: Two-step state-machine read-then-write across threads
Reading the current state and writing the new state execute as separate unsynchronized operations; a concurrent thread can change state between them, producing a stale write or invalid transition.
**Look for:** `if (state == A) state = B;` patterns; non-atomic state machines outside any lock or `compareAndSet`.

### F-29: Shutdown stops downstream before draining in-flight work
Shutting down a consumer subsystem before its message source races with in-flight messages; or shutdown waits only for the work queue, not the executor producing into it. In-flight messages arrive at partially-torn-down state and corrupt or lose data.
**Look for:** `consumer.stop(); source.stop();` ordering; `queue.drain(); resource.close();` without first stopping the producer-side executor.

### F-30: Shutdown guard check and submit are not atomic
A shutdown flag is checked and a task submitted in separate steps; concurrent shutdown between them causes the submit to throw and the partially-constructed task to leak its acquired resources.
**Look for:** `if (!shutdown) executor.submit(...)` outside any synchronized region, especially when the task holds resources.

### F-31: Lock released before snapshot of shared collection is iterated
A lock is released before iterating a snapshot of a shared collection; a concurrent writer deletes underlying resources during iteration, producing missed data or use-after-free.
**Look for:** Pattern: `lock.lock(); copy = new ArrayList(shared); lock.unlock(); for (x : copy) x.use();` where iterated objects may be freed.

### F-32: Check-then-put on concurrent map is not atomic
Two threads call `containsKey` then `put` on a concurrent map; both insert, and one silently overwrites the other.
**Look for:** `if (!map.containsKey(k)) map.put(k, v);` instead of `putIfAbsent` / `computeIfAbsent`.

### F-33: New object published before its internal resources are ready
An object is exposed via shared state before its constructor / initializer has finished assigning internal resources; threads observing the published reference between init steps find an incomplete object and throw.
**Look for:** Fields published to a shared map / volatile field before all the constructor work has run; factory methods that register `this` before final assignments.

### F-34: Thread-local context cleared while async work still references it
An async task captures contextual state cleared in a `finally` when the coordinating operation completes; a response arriving after completion finds the context null and NPEs.
**Look for:** `ThreadLocal` clears in `finally` blocks while async tasks remain in flight referencing them.

### F-35: Spurious wakeup not handled in condition wait
A condition-wait loop uses a single `await` without re-checking the predicate on wakeup; spurious wakeup causes the caller to return null before the actual response arrives.
**Look for:** `cond.await()` followed by use of guarded state without a surrounding `while (!predicate)` loop.

### F-36: Single shared mutable message reused across concurrent sends
A request message object is created once and reused for all endpoints in a fan-out loop; all requests share the same ID/headers and concurrent responses collide.
**Look for:** Messages constructed outside a per-recipient loop, then mutated and dispatched repeatedly.

### F-37: Pooled buffer returned while in-flight write still uses it
A pooled buffer is returned to the pool before the in-flight network write completes, allowing it to be overwritten and corrupting the original payload.
**Look for:** `pool.release(buf)` placed in a synchronous path while I/O write is still pending asynchronously.

### F-38: Shared buffer's position advanced as side effect of a read
A read method on a shared buffer (`get`, `getInt`, `getLong`) advances the position as a side effect; subsequent readers from the same buffer see wrong bytes.
**Look for:** `buf.getInt()` / `buf.get()` / `buf.array()` on a shared `ByteBuffer` without a prior `duplicate()` or `slice()`.

### F-39: Heartbeat / liveness reply sent after shutdown started
A liveness-probe response handler sends a reply unconditionally without checking whether the local node is shutting down, causing peers to re-mark it as alive after a shutdown notification.
**Look for:** Heartbeat / probe handlers that omit a `shuttingDown` guard; reply paths whose state should depend on lifecycle.

### F-40: Cleanup loop fails first item and abandons rest
A cleanup loop does not wrap each iteration in its own exception handler; the first failed release terminates the loop, leaving remaining resources permanently unreleased.
**Look for:** `for (X x : resources) x.close();` without per-iteration `try/catch` or `addSuppressed` handling.

