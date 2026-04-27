# Category: Lifecycle and Ordering

Bugs caused by performing operations in the wrong sequence relative to a component's lifecycle: registering listeners after events fire, exposing services before initialization completes, tearing down dependencies in the wrong order, polling without deadlines, or assuming async work is done when it isn't.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- New `register*`/`addListener`/`subscribe`/`addObserver` calls or changes to where they are placed
- Changes to constructor or `init()`/`start()` ordering, or new initialization phases
- Changes to `close()`/`shutdown()`/`stop()` ordering, drain loops, or teardown sequencing
- New management endpoint/metrics registration, or unregistration paths
- Polling loops (`while (!ready)`, `await*Until*`, retry-on-condition) without explicit deadlines
- New asynchronous tasks (`submit`, `schedule`, `Future`, `CompletableFuture`) that produce state observable elsewhere
- Conditional readiness flags, "is started/initialized/ready" guards, or status-broadcast methods
- Changes to bootstrap/join/handshake/handoff/quarantine sequences
- Background thread spawning (especially inside constructors or factory methods)
- Static initializers / class-load-time evaluation of configuration or singletons

## Findings

### F-01: State reported before listener registered (lost-update window)
Component reports its current state to a caller and then registers an event listener; updates that arrive between the snapshot and the registration are silently lost.
**Look for:** sequence of `getState()` / `getCurrentX()` followed by `addListener(...)` / `subscribe(...)` on the same source — should be reversed (register first, then snapshot).

### F-02: Listener registered after async operation already started
A handler/listener is wired up after the producer has begun emitting events; events fired in the gap between start and registration are permanently missed.
**Look for:** `start()` / `submit()` / `connect()` / `kickOff()` calls that precede the matching `addEventListener(...)` / `onComplete(...)` / `register(...)` for the same component.

### F-03: Management interface registered at end of long initialization
Metrics, REST, or similar management surface is registered in the final step of multi-phase init — operators cannot observe or control the system during the preceding phases.
**Look for:** Management endpoint or metrics registry registration calls, or HTTP endpoint binding placed after multiple heavyweight init steps in `start()` / `init()`.

### F-04: Async-rebuild visibility window (queryable before ready)
Component is marked queryable / "available" when the request arrives, but the underlying data structure is still being asynchronously rebuilt or populated, so queries succeed at the routing layer but fail or return empty.
**Look for:** `setReady(true)` / status flag set immediately after submitting an async rebuild task, or readiness derived from registration rather than completion.

### F-05: Startup polling loop without deadline
A polling loop waits for an external readiness condition (peer, file, network, downstream service) without a timeout or maximum-attempt guard, hanging forever if upstream never converges.
**Look for:** `while (!ready) { sleep(...); }` / `await(...)` / `pollUntil(...)` with no deadline parameter, no maximum iteration count, and no way for the caller to abort.

### F-06: Consumer torn down before its message source
Shutdown sequence stops the message handler/consumer before stopping the producer or transport that feeds it, so in-flight messages arrive at partially-disposed state and produce errors or silent loss.
**Look for:** `close()` / `shutdown()` blocks where consumer/handler is stopped first and the inbound channel/queue/socket/listener is stopped after; correct order is usually source-first.

### F-07: Background thread init failure not signalled to spawning thread
A worker thread fails during initialization but the spawning thread is not notified; subsequent shutdown waits indefinitely on the worker that never came up.
**Look for:** new `Thread(...)`/`executor.submit(...)` followed by `thread.join()` or latch wait in shutdown without an exception channel back to the spawner.

### F-08: Background thread started before owner is fully constructed
A constructor spawns a thread that immediately reads `this.someField`; if construction has not finished, the thread observes an uninitialized field and crashes or sees stale state.
**Look for:** `new Thread(this::run).start()` or `executor.submit(this::work)` inside a constructor body before all field assignments complete.

### F-09: Liveness/identity announced before component is ready to serve
A node or component registers itself as healthy / available before the underlying service (transport, data, dependencies) is ready; clients route requests to it and they fail.
**Look for:** Health-check registration, heartbeat publication, or service-discovery enrollment placed earlier in startup than the corresponding bind / data-load / dependency-init step.

### F-10: Liveness probe replies during shutdown
A liveness response handler responds "alive" without checking whether the local node has begun shutting down, causing peers to re-mark the node as alive after a shutdown notification was already broadcast.
**Look for:** ping/heartbeat handlers that send a positive reply unconditionally, with no check of the local `isShuttingDown` / `state == STOPPING` flag.

### F-11: Service-stop ordering leaves a window where node looks down but still accepts work
Shutdown stops the liveness/advertising channel before closing client-facing transport, leaving an interval where peers think the node is gone but it still accepts (and then drops) client connections.
**Look for:** `gossip.stop()` / `failureDetector.stop()` placed before `clientTransport.close()` in shutdown sequences.

### F-12: Async cleanup not awaited before next operation proceeds
Code triggers an async cleanup/close and immediately starts the next phase, racing against the cleanup that has not yet released resources or state.
**Look for:** `closeAsync()` / `removeAsync()` / fire-and-forget delete followed by re-creation or restart of the same identifier in the same method.

### F-13: Capacity counter decremented before resources fully released
A slot/permit counter is decremented to "free" the slot before the associated resources are actually released; a concurrent admitter sees the free slot and submits new work that races the cleanup.
**Look for:** `slots.release()` / `count.decrementAndGet()` placed before `resource.close()` / `cleanup()` calls in finally blocks or completion callbacks.

### F-14: Shutdown drain waits only on queue, not on producers
Shutdown waits for a work queue to drain but does not wait for the executor or upstream stage that produces into the queue, racing close against in-flight writes.
**Look for:** `queue.awaitEmpty()` / latch-on-queue without a corresponding `producerExecutor.awaitTermination()` or producer-side join in shutdown paths.

### F-15: Static initializer evaluates config before subsystem configures it
A `static final` field is initialized at class-load time by reading from a global singleton or configuration that the caller has not yet populated, freezing the constant at a stale (often default) value for the JVM lifetime.
**Look for:** `static final X = SomeService.get*()` / `Config.read(...)` in field initializers; class-load triggers run before the configurer.

### F-16: Background task scheduled in static initializer
A periodic/scheduled task is submitted from a static initializer that runs whenever the class is first loaded; the target executor or runtime may not yet be initialized, causing a startup race or NPE.
**Look for:** `executor.scheduleAtFixedRate(...)` / `Timer.schedule(...)` calls in `static {}` blocks or static field initializers.

### F-17: Lifecycle method runs before initialization completes
An `init()`, factory `create()`, or registration step is interleaved so that a `stop()`, callback, or query method can be called between construction and full setup, finding partially-built state and crashing or silently no-opping.
**Look for:** an `init()`-set readiness flag inside `run()` (rather than the constructor) — an early `stop()` is silently overwritten when `run()` begins; or a query method that hits a not-yet-initialized field.

### F-18: Two-stage initialization where the readiness flag is observable too early
A flag like `isReady` / `started` is set right after starting an async init, not after init completes; readers see "ready" but the underlying object is still being built.
**Look for:** `this.ready = true;` placed after `executor.submit(initTask)` / `service.startAsync()` rather than inside the task's completion callback.

### F-19: One-shot scheduled task never reschedules itself
A periodic refresh job uses a one-shot schedule and updates state once, but never reschedules; the periodic cycle silently stops after the first execution.
**Look for:** `scheduler.schedule(task, delay)` (one-shot) where `scheduleAtFixedRate` / `scheduleWithFixedDelay` was intended; or a task body that omits its own re-arm call.

### F-20: Periodic task gates re-arm on flush/return value rather than time
A periodic loop decides whether to re-arm based on the side-effect return of the work it just did instead of always re-arming on a clock; an unusual return value silently halts the schedule.
**Look for:** `if (worked) scheduleNext()` patterns at the bottom of scheduled handlers — re-arm should be unconditional or tied to lifecycle state, not work outcome.

### F-21: Cleanup callback registered before resource is committed
A cleanup/close callback is wired up in a constructor before the underlying resource is committed to long-lived storage; if the partially-built object is discarded, the callback is orphaned and cleanup never runs.
**Look for:** `addCloseListener(...)` / `onClose(...)` in a constructor that runs before `commit()` / `register(this)` succeeds.

### F-22: Resource registration after submission to executor (race window)
A resource is submitted to an executor and only afterwards registered into a tracking set; a concurrent completion can observe an absent entry, drop cleanup, or allow a duplicate submission.
**Look for:** `executor.submit(task)` followed by `tracker.put(task.id, task)` — registration must precede submission or be performed by the task itself before yielding.

### F-23: Static initializer triggers cyclic class load that deadlocks
A static initializer calls a startup method that throws; subsequent class access re-triggers the initializer, which tries to acquire locks already held by the interrupted initialization, deadlocking.
**Look for:** `static { ... Service.initialize(); ... }` blocks where `initialize()` can throw and the class is referenced from any thread that may resume before recovery.

### F-24: Constructor passes `this` to external observers before construction completes
A constructor registers `this` with an external listener, registry, or another thread before all fields are assigned, so observers see a partially-constructed object.
**Look for:** `registry.register(this)` / `executor.submit(this::work)` / `bus.subscribe(this)` calls inside a constructor body, especially before final field assignments.

### F-25: Migration / setup path adds new entry before removing old one
A migration that replaces one entry with another adds the new entry before deleting the old; a reader observing the intermediate state sees both, causing double-processing or ambiguous lookups.
**Look for:** `add(new); remove(old);` ordering in migration helpers — should usually be `remove(old); add(new);` or use atomic replace.

### F-26: Bootstrap / data transfer skipped when guard inverted
A guard around bootstrap (`if (!schemaPresent)`) silently skips the data-transfer phase under conditions the operator did not anticipate, leaving the node joined-but-empty.
**Look for:** any `bootstrap()` / `joinRing()` / `loadInitialState()` call gated by a single boolean derived from local-only state — combine with explicit operator intent.

### F-27: Async write/initialization not awaited before continuation
Caller invokes an async write / `submit` / `executeAsync()` and continues without awaiting; the next step assumes the operation has completed, producing wrong results or losing writes.
**Look for:** discarded `Future` / `CompletableFuture` return values, especially in initialization, schema-update, or migration code paths.

### F-28: Initialization step depends on virtual method called before subclass init
A base-class constructor calls a virtual / overridable method whose subclass override depends on subclass fields not yet assigned; the call returns stale defaults and a critical step is skipped.
**Look for:** virtual method invocations in superclass constructors / `init()` that subclasses override.

### F-29: Field initialization order — dependent field declared before its dependency
A `static` field references another `static` field declared later in the source; initialization order causes the dependent field to read null/zero, often triggering NPE in subsequent calls (e.g., logger usage).
**Look for:** static field declarations where one initializer calls a method on another static field declared further down in the file.

### F-30: Setter triggers side effect that reads not-yet-set field
A setter invoked from within a constructor triggers a side effect (notification, recompute) that reads a field assigned later in the same constructor, producing NPE or a wrong derived value.
**Look for:** field setters in constructors that fire change-notifications / recomputes; reorder to assign all fields first, then notify.

### F-31: Termination notifications fire from a state machine that can re-enter
A "became active / available" notification fires from a state-machine transition that can occur multiple times (e.g., during a transient resigned state), causing downstream consumers to receive false positives.
**Look for:** notify/event-fire calls in state-transition handlers — move to the actual single-shot lifecycle event that fires once on first reachability.

### F-32: Component dependency initialized after dependent component
A dependency is initialized after a dependent component that has already started to use it, so the dependent silently skips checks (e.g., authorization) or operates with default behavior.
**Look for:** ordering of `start*()` calls in a top-level bootstrap method where component A depends on component B but B is started later.

### F-33: Shutdown sequence references field that was never initialized
A `close()` / `shutdown()` method dereferences a field that is only assigned if initialization completed successfully; an early-failure shutdown throws NPE that masks the original startup error.
**Look for:** unguarded field accesses in `close()` / `cleanup()` paths — guard with `if (field != null)` or initialize fields to safe defaults.

### F-34: Startup decision made after fixed-duration sleep instead of explicit convergence wait
A startup or initialization decision is made after `Thread.sleep(constant)` waiting for external state to converge; if convergence is slower than the sleep, the node acts on stale/incomplete state.
**Look for:** `Thread.sleep(N)` in startup paths followed by reading peer/membership/config state — replace with an explicit readiness signal, condition variable, or polling loop with a timeout.

### F-35: Snapshot taken before write barrier issued (lost durability window)
A durability/flush boundary position is sampled before the write barrier or atomic position publish, so concurrent writers can land past the sample yet still be in the flushing batch and lost on truncation.
**Look for:** `lastWritePos = currentPos()` / `snapshot()` calls placed before the corresponding `barrier()` / `fence()` / atomic publish.

### F-36: Liveness arrival not reported on first observation
A failure-detector or arrival-reporter is bypassed on an endpoint's very first observation, so the detector starts with no samples and permanently considers the endpoint unreachable.
**Look for:** First-observation paths that skip the `reportArrival()` / `recordSample()` call that the steady-state path performs.

### F-37: Listener removed on first response when concurrent in-flight requests share it
A listener / session is unregistered on the first response, but concurrent in-flight requests share the same listener and lose their reference before completion, silently dropping later events.
**Look for:** `tracker.remove(id)` inside a single-response callback when more than one outstanding request can map to the same `id`.

### F-38: Termination flag and queue cleared in non-atomic steps
A termination handler sets a "stopped" flag and clears a work queue in separate steps; a message received between the two causes an assertion failure or work submission to a half-shut-down state.
**Look for:** `this.stopped = true;` followed (or preceded) by `queue.clear()` in shutdown paths without a single critical section guarding both.

### F-39: Worker pool deadlocks because submit blocks waiting on its own continuation
A bounded executor's tasks submit further work back to the same executor; under contention, all worker threads are blocked waiting for queue space while the only thing that can free slots is themselves.
**Look for:** `executor.submit(...)` calls made from inside tasks already running on the same executor — refactor to a separate executor or use a non-blocking handoff.

### F-40: Blocking call inside a callback that runs on the only thread that can complete it
A callback synchronously waits on a future whose completion requires the same single-threaded stage that's running the callback, deadlocking the stage.
**Look for:** `future.get()` / `await()` calls inside completion handlers, RPC handlers, or single-threaded stage callbacks.

### F-41: Subscription state mutated by unintended thread
Shared subscription / membership state is mutated on the calling (application) thread while a background thread expects to own it, producing torn reads and inconsistent assignments between calls.
**Look for:** direct mutation of subscription / assignment / state-machine fields from public API methods rather than enqueueing the change to the owning thread.

### F-42: Init readiness check uses stale proxy rather than authoritative signal
A "ready" condition is derived from a proxy (e.g., presence of pending entries) rather than directly comparing committed-vs-applied counters, releasing dependent operations before the system is truly ready.
**Look for:** readiness checks that test indirect side-effects (queue size, presence flag) instead of the canonical "X applied through Y" condition.

### F-43: Background scheduling fires before required state is established
An initialization task is gated by a fixed-duration timer rather than an explicit lifecycle event, so it can fire before required state (e.g., topology, schema) is established.
**Look for:** `scheduleOnce(initTask, delay)` patterns where the delay is hoping a separate prerequisite will have completed; use explicit dependency / ready-event instead.

### F-44: Application reports ready/running before async setup completes
A service publishes its `running` / `ready` status before the asynchronous setup it spawned completes, giving callers a false signal of initialization completion.
**Look for:** `ready.complete()` / `setStatus(RUNNING)` calls placed immediately after `submitAsync(setup)` rather than inside the setup's completion callback.
