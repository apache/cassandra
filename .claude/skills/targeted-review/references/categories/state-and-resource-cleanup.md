# Category: State and Resource Cleanup

Stale state, leaked references, and orphaned resources that arise when teardown, cancellation, removal, or replay paths fail to fully restore the system to a clean baseline.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- `close()`, `release()`, `unref()`, `decrementReferenceCount()`, `dispose()`, `shutdown()` calls
- `try`/`finally`, `try-with-resources`, or `AutoCloseable` introduction or removal
- Cancellation, abort, timeout, rejection, or error-path branches that release/return resources
- `clear()`, `remove()`, `deregister()`, `unregister()`, `retainAll()`, `evict()` on long-lived collections, registries, caches, or maps
- Tombstone, purge, GC, expiry, eviction, retention, or compaction predicates
- Counter increment/decrement pairs, latches, semaphores, in-flight counters, pending queues
- Replay, restore-from-log, snapshot loading, or restart bootstrap of in-memory state
- New background threads, periodic tasks, scheduled futures, or executors with lifecycle management
- Reference counting, off-heap buffer management, native handle ownership
- Listener/callback registration without matching deregistration in lifecycle hooks
- New or renamed lifecycle methods (`onLeave`, `onRemoved`, `onDelete`, `dispose`, `cleanup`)
- Iterator/cursor/stream code that may be returned past its enclosing scope

## Findings

### F-01: Counter incremented before submit, no decrement on rejection
A pending/in-flight counter is incremented before submitting work to an executor, but the cancellation or rejection path that fires when the submit throws omits the matching decrement, so the counter grows unboundedly.
**Look for:** `counter.increment()` / `pending++` followed by `executor.submit(...)` with no decrement in the catch/onReject branch.

### F-02: Reference released only on success path
A reference-counted resource, lock, latch, or pending acknowledgement is released only in the success branch; failure, timeout, or no-ack paths skip the release and leak the handle indefinitely.
**Look for:** `ref.release()` or `lock.unlock()` reachable only after a happy-path `if (ok)` rather than from `finally`.

### F-03: Resource opened then leaked when next call throws
A pooled resource, file handle, or buffer is allocated and immediately used by a call that can throw, with no try/finally to release on the exception path; the resource is permanently leaked.
**Look for:** allocation followed by a fallible call without surrounding `try { ... } finally { resource.release(); }`.

### F-04: Multi-step init missing partial cleanup on mid-way failure
A constructor or initialization sequence acquires multiple resources serially; when a later step throws, already-acquired earlier resources are not released, so each failed startup leaks them.
**Look for:** sequential `open(...)` / `new ...()` calls in `init`/`start`/constructors with no progressive try/catch unwind.

### F-05: Subclass adds resources but does not override abort/close
A subclass adds new closeable fields but does not override the parent's abort or close method; the parent implementation never sees the new fields and they leak on every shutdown or failure.
**Look for:** new closeable field in a subclass with no matching `@Override close()` / `abort()`.

### F-06: Cleanup override silently dead after rename
A subclass override targets the old name (or signature) of a parent cleanup method that has since been renamed/changed; without `@Override` the override is no longer reachable and resources leak silently. Same shape applies to listener registrations using stale interface method names.
**Look for:** override-style methods without `@Override`; recently renamed lifecycle methods on the parent; anonymous listener implementations whose method names don't match the interface.

### F-07: try-with-resources missing on inline-opened streams
A stream/reader/connection is opened inline as a method argument (or assigned to a local that is never used in a finally block); descriptor leaks on every exception thrown before close.
**Look for:** `new FileInputStream(...)` / `Files.newInputStream(...)` / `getConnection()` not wrapped in try-with-resources.

### F-08: Resource not registered with owning container's close tracker
A resource created inside a lifecycle-managed object is wrapped and returned (or stored) without being registered with the container's close-tracker, so the container's own cleanup path skips it.
**Look for:** new resource returned from a factory in a "transaction" / "txn" / "tracker" / "scope" without an `addCloseable` / `register` call.

### F-09: Cleanup loop terminates on first failure, leaks rest
A loop that releases multiple resources does not wrap each iteration in its own try/catch; the first throwing release aborts the loop and leaves all subsequent resources permanently held.
**Look for:** `for (var r : resources) r.close();` without per-iteration try/catch.

### F-10: Background thread spawned without owner reference
A long-lived background thread (or scheduled future) is started without storing a reference; on shutdown or early-close there is no handle to interrupt or join it, so it leaks indefinitely.
**Look for:** `new Thread(...).start()` or `executor.scheduleAtFixedRate(...)` whose return value is ignored.

### F-11: Listener / metric / sensor registered with no matching deregistration
A listener, sensor, or metric is registered when an entity is created but the corresponding remove path does not deregister it, so registrations accumulate unboundedly across churn or restarts.
**Look for:** `registry.register(...)` / `addListener(...)` / `metrics.add(...)` in constructor with no symmetric `unregister`/`removeListener`/`metrics.remove(...)` in close/onRemove/onLeave.

### F-12: Per-entity caches never released on entity removal
In-memory per-entity state (map/cache keyed on entity ID) is populated on add/join but never cleared on remove/leave, causing unbounded memory growth under churn.
**Look for:** `map.put(entityId, ...)` in onJoin/onAdd with no `map.remove(entityId)` in onLeave/onRemove/onExpire.

### F-13: Terminal-state entries never purged because deadline never converges
Each replica/node independently derives a cleanup deadline for a terminal-state entry, and propagation of the terminal state is suppressed; the deadline never converges and the entry is never garbage-collected.
**Look for:** local-only timestamps used as purge deadlines on entries that should reach a globally-terminal state.

### F-14: Replay skips superseded-entry filter; stale state visible after restart
When restoring state from a persistent log, the filter that removes entries already superseded by an advanced progress marker is skipped; old entries appear current after restart.
**Look for:** restore/replay paths that call `add(...)` on each record without first checking it against the highest-applied progress/epoch.

### F-16: Cleanup callback fires before guarded resource is constructed
A completion callback shared between paths is invoked unconditionally even on the path that did not yet create the guarded resource; cleanup runs against partially-built or absent state. Same shape: a cleanup listener is registered before the resource is published, so it orphans if the object is discarded before use.
**Look for:** completion handler scheduled before the resource's construction completes; `addOnClose(...)` in a constructor before the protected resource is fully published.

### F-17: Reset clears one of two coupled structures, leaves the other stale
A reset/clear operation zeroes out one structure but leaves a sibling collection, derived counter, parallel chain, or wait-list intact; subsequent operations see a half-reset state. Includes failing to reset derived counters after collection clear, and walking only one chain in a builder reset.
**Look for:** `reset()` / `clear()` methods that touch one field of a pair without explicitly clearing the related field; `collection.clear()` without `count = 0`.

### F-18: Stop flag set after queue is cleared; loop never re-checks
A shutdown flag is set after a shared queue has been drained, but the worker loop never re-checks the flag at the outer iteration level; the worker continues processing newly-arriving items past close.
**Look for:** `stopped = true` placed after `queue.clear()` with no `while (!stopped)` guard at the outer loop.

### F-19: Reset clears state but does not unblock waiters on invalidated state
A reset clears coupled data structures but does not signal/notify threads waiting on the now-invalidated condition; waiters block indefinitely on stale mappings.
**Look for:** `reset()` / `invalidate()` methods that clear shared state without `notifyAll()` / `signalAll()` on associated condition variables.

### F-20: Periodic task short-circuits on disabled flag, never retracts published state
A periodic task that exits early when a feature flag is off never retracts state it previously published; downstream components remain permanently blocked by stale entries.
**Look for:** early-return on feature-flag-off in periodic tasks that previously called publish/announce/register paths.

### F-21: Dead-node pointers retained after unlink; unbounded growth
A linked structure retains internal dead-node pointers after unlink operations; sustained add/remove workload causes monotonic memory growth.
**Look for:** custom linked-list/tree implementations whose unlink path nulls only adjacent fields without clearing back-pointers / forward-pointers on the removed node.

### F-22: Use-after-free: ref count not incremented before access
An off-heap buffer is read or compare-and-swapped without first incrementing its reference count; a concurrent eviction frees the underlying memory between lookup and access, producing a use-after-free.
**Look for:** `cache.get(...)` or `map.get(...)` returning a buffer/handle followed by access without `acquire()` / `incrementReferenceCount()`.

### F-23: Eviction frees resource while consumers hold raw pointers
A cache eviction callback unconditionally frees a resource while consumers hold raw (non-counted) pointers; eviction races with active reads cause use-after-free.
**Look for:** eviction listeners that call `free`/`close` without first checking ref count or quiescence; callers that hold raw pointers without counted handles.

### F-24: Drained-but-leaked: drain after atomic swap
A shared collector is drained after atomically swapping in a replacement; the window between swap and drain allows concurrent writers to append to the old instance, silently dropping or duplicating entries.
**Look for:** `collector.set(newInstance)` followed by `oldInstance.drain()` without quiescence or ordering barrier.

### F-25: Cleanup loop reads from one source to delete in another
A selective-delete cleanup reads from one source to decide what to delete in another, missing orphaned entries when the two sources are out of sync; truncate-and-repopulate is more correct.
**Look for:** `for (k in source1.keys()) target.remove(k)` patterns; consider whether `target` may contain keys not in `source1`.

### F-26: Index/snapshot rebuild without dropping stale entries
An index rebuild adds new entries but does not first drop stale on-disk or in-memory entries for the items being rebuilt; old entries survive and corrupt subsequent queries.
**Look for:** rebuild paths that `add` / `merge` without a preceding `delete` / `truncate` of the rebuilt subset.

### F-27: Cancellation indistinguishable from successful completion
A background job writes "completed" status unconditionally at the end regardless of cancellation; downstream observers cannot tell the two apart and skip needed cleanup.
**Look for:** unconditional `status = COMPLETED` at end of run paths; no branch on `cancelled`/`interrupted` flag.

### F-28: In-flight dedup flag set on dispatch, cleared only on success
An in-flight deduplication flag is set when a request is dispatched but cleared only on the success path; failures leave the flag set forever, permanently blocking retries. Same shape: sticky boolean "active" flag set on add but never cleared on remove.
**Look for:** `inFlight.add(key)` followed by `inFlight.remove(key)` only inside an `if (success)` callback; `hasActive = true` with no path back to `false`.

### F-29: Map entries never removed when value becomes empty
Map entries whose values become empty (empty collection, drained queue) are never removed, so stale keys persist and corrupt membership/size queries.
**Look for:** `map.get(k).remove(v)` patterns that don't follow with `if (map.get(k).isEmpty()) map.remove(k);`.

### F-30: Static cache keyed on path never invalidated on file replacement
A static cache keyed on file path is never evicted after the underlying file is replaced; subsequent operations on the new file receive stale metadata.
**Look for:** static `Map<Path, Metadata>` populated on first access with no invalidation hook tied to file overwrite/replace.

### F-31: Iterator returned past close
An iterator is returned to the caller after being closed in a finally block, or after its underlying resource has been released; the caller's first `next()` reads from a closed iterator.
**Look for:** `try { ... return iter; } finally { iter.close(); }` patterns; iterator escaping the scope of its underlying resource.

### F-32: Filtered-out closeable items never closed
An iterator-style filter silently skips items without closing them; filtered-out objects hold open file/buffer handles that are never released. Same shape: `CloseableIterable` assigned to a plain `Iterable`, erasing the close contract.
**Look for:** `.filter(...)` over `Closeable`/`AutoCloseable` streams without explicit `close` on rejected items; downcast/upcast that drops `Closeable`.

### F-33: Resource opened in constructor not assigned to field before throwing
A handle opened inside a constructor is not assigned to its field before an early return or thrown exception; the close method cannot reach it and the descriptor leaks.
**Look for:** `Resource r = open()` followed by potentially-throwing work before `this.field = r`.

### F-34: Background factory creates new instance without checking previous still running
A constructor used for both transient and durable instances unconditionally starts a periodic background task; frequent transient creation accumulates unbounded background work.
**Look for:** `start()` / `schedule(...)` calls in constructors with no idempotence/active check; transient lifetimes for the same class.

### F-35: Lazy cache ignores changing input parameters
A lazily-initialized cache stores the result of the first computation and reuses it regardless of changed inputs, returning stale results.
**Look for:** `if (cache == null) cache = compute(input);` patterns inside methods whose `input` differs across calls.

### F-36: Cleanup gated on liveness check; skipped after entity already gone
A cleanup or unsubscribe operation is gated behind a membership/liveness check; when membership has already advanced to EMPTY/DEAD the cleanup is silently skipped, leaving stale state to outlive the entity it was supposed to follow.
**Look for:** `if (group.exists()) cleanup()` patterns where `group` may already be empty/expired but its associated state still needs cleanup.

### F-37: Half-released paired resources on rejection
When a task submission is rejected, only half of a paired resource (e.g., reference count released, transaction left open) is released; the other half leaks.
**Look for:** rejection handlers that release one resource handle while leaving a related handle (txn, lock, ref) untouched.

### F-38: Two-phase cleanup counter never decremented on fast-path
A reference counter initialized to total task count expects each completion to decrement it; a fast-path that bypasses the response handler never decrements, leaving the pending record permanently un-removed.
**Look for:** `counter = N` followed by handlers that decrement on slow-path; verify all bypass / short-circuit paths also decrement.

### F-39: Failed remote cleanup silently dropped, no retry
A remote cleanup call (e.g., delete on another node) that fails is silently swallowed without retry or alerting; orphaned remote resources accumulate forever.
**Look for:** `try { remote.delete(...); } catch (Exception e) { log.warn(...); }` with no retry queue or escalation.

### F-40: Session removed from cache before its resources are released
A session is removed from a cache before releasing its associated resources; the release code then operates on already-removed state and silently skips cleanup. Same shape: pending-init queues whose entries are never closed when the parent collection is cleared on shutdown.
**Look for:** `cache.remove(sessionId)` followed by `session.release()`; verify pending/bootstrapping queues are drained-and-closed in `shutdown()`.
