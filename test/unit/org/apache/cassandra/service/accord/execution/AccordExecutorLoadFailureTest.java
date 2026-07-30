/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord.execution;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.ExecutionContext;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.QuadFunction;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;
import com.google.common.util.concurrent.Uninterruptibles;

import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;

/**
 * This test has been authored entirely by Claude.
 *
 * A task that is submitted from within another task running on the same command store inherits that task's references
 * ({@link SafeTask#preSetup}) instead of acquiring its own. {@link SafeTask#completePresetupExclusive} handles an
 * inherited entry that is still loading - it queues on it - but, unlike
 * {@code setupExclusive}, it does not add it to {@code waitingForState}. So the task believes everything it requires is
 * loaded, goes straight to {@code waitOnCacheQueuesExclusive} and trips
 * {@code Invariants.require(optional)} there, failing with an {@link IllegalStateException}.
 *
 * <p>This is only reachable from a batched ({@code ASYNC}/{@code INCR}) parent, as that is the only kind of task that
 * runs while still holding references to keys it has not yet loaded. It is benign for a batched child, whose keys are
 * "optional" and which is therefore notified when the load completes; it is fatal for a {@code SYNC} child.
 */
public class AccordExecutorLoadFailureTest
{
    private static final int TIMEOUT_SECONDS = 30;

    static class InjectedLoadFailure extends RuntimeException
    {
        InjectedLoadFailure(Object key)
        {
            super("injected load failure for " + key);
        }
    }

    /** thrown by a registered {@link AccordCache.Listener} as a task releases a key it modified; see {@link #completionFailureReleasesPositionsTest} */
    static class InjectedListenerFailure extends RuntimeException
    {
        InjectedListenerFailure(Object key)
        {
            super("injected listener failure while releasing " + key);
        }
    }

    /** records everything reported to the agent, as any report here is an internal error, not a failed operation */
    static class RecordingAgent extends TestAgent
    {
        final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        @Override
        public void onException(Throwable t)
        {
            exceptions.add(t);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            onException(t);
        }
    }

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // run batched tasks over batches of one key, so that a task runs with one key while another is still loading
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = 1;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = 1;
    }

    /**
     * {@link AccordExecutor#onLoadedExclusive} handles a failed load by failing every task that was waiting on the
     * entry and then calling {@code AccordCache.failedToLoad}, which requires the entry to be unreferenced. That
     * assumes every reference belongs to a task that can be failed there and then, which is not true of a batched
     * ({@code ASYNC}/{@code INCR}) task: its keys are optional, so it runs - and holds its references - while some of
     * them are still loading, and {@code tryFailAndCompleteExclusive} cannot fail a task that is already running. The
     * {@code Invariants.require(node.references() == 0)} then fails inside the executor loop, and the resulting
     * {@link IllegalStateException} is reported to the agent and handed to whichever task is waiting on the load.
     */
    @Test
    public void loadFailureWhileBatchedTaskHoldsEntryTest() throws InterruptedException
    {
        test(LoadKeys.INCR, true);
    }

    /**
     * An ASYNC task processes one batch and drops the rest, so it needs nothing further from a key whose load fails: it
     * completes successfully and simply releases the reference.
     */
    @Test
    public void loadFailureWhileAsyncTaskHoldsEntryTest() throws InterruptedException
    {
        test(LoadKeys.ASYNC, false);
    }

    private void test(LoadKeys loadKeys, boolean expectFailure) throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey ready = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0)));
        RoutingKey failing = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(1)));
        Condition loading = Condition.newOneTimeCondition();
        Condition release = Condition.newOneTimeCondition();
        Condition running = Condition.newOneTimeCondition();

        RecordingAgent agent = new RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, key) -> {
            if (failing.equals(key))
            {
                // block until the task is running with its other key, so that it holds this reference when we fail
                loading.signal();
                release.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                throw new InjectedLoadFailure(key);
            }
            return null;
        }));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Condition done = Condition.newOneTimeCondition();
        try
        {
            ExecutionContext context = ExecutionContext.contextFor(TxnId.fromValues(1, 1, 0, new Id(1)), null, RoutingKeys.of(ready, failing),
                                                                  loadKeys, LoadKeysFor.READ_WRITE, "batched");
            store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                running.signal();
                // hold the reference to the failing key until its load has failed
                release.signal();
                Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
            }, (success, fail) -> {
                failure.set(fail);
                done.signal();
            });

            assertTrue("the failing key never started loading", loading.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the task never ran", running.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            // give the failed load time to be processed, then check the agent first: failing the load may legitimately
            // fail the task, but reporting an internal invariant failure is a bug
            done.await(5, TimeUnit.SECONDS);
            for (Throwable t : agent.exceptions)
                assertTrue("the agent was told " + t + " (" + stack(t) + ')', isInjected(t));
            assertTrue("the task failed with " + failure.get(), failure.get() == null || isInjected(failure.get()));
            if (expectFailure)
                assertTrue("the task should have been failed by the load failure", isInjected(failure.get()));
            else
                assertTrue("the task should not have been failed: it needed nothing more from the entry", failure.get() == null);
            assertTrue("the task was never notified: it is still waiting for a key whose load failed",
                       done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        finally
        {
            release.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * R6: a task in {@code TERMINAL_FAILURE} must hold no position on any cache entry, or everything queued behind it
     * waits for a task that will never run again.
     *
     * <p>{@code SafeTask.completeExclusiveMayThrow} releases the task's references as its <em>final</em> act, and
     * several statements ahead of that release can throw: {@code nonSync().postRunExclusive(this)}, the
     * {@code refs.forEach(setAbandoned)} of a failed non-sync task, and {@code waitOnKeysExclusive()} for an INCR round
     * that is not the last. {@code Task.completeExclusiveNoExcept} catches all of them, and used only to report the
     * throwable and {@code failExclusive(t, FAILED)} - neither of which releases anything. So the task stayed FAILED
     * for ever while holding a reference and a queue position on every entry left in {@code refs}, and every task that
     * later claimed one of those entries queued behind a corpse. Meanwhile the {@code finally} still called
     * {@code completedTaskExclusive}, so the executor's own accounting said the task was gone.
     *
     * <p>The statement provoked here is the first of those: {@code nonSync().postRunExclusive(this)}. It releases the
     * keys of the batch just run, and {@code AccordCache.Instance.release} notifies the commands-for-key cache
     * listeners ({@code node.notifyListeners(Listener::onUpdate)}) for a key the run modified - <em>before</em> it
     * removes the task's position. A listener that throws there is not a synthetic failure: the range scanner's
     * {@code RangeTxnAndKeyScanner.KeyWatcher} is such a listener, and its {@code onUpdate} both throws
     * {@code AssertionError} on an unhandled status and calls {@code adoptCachedKeyExclusive}, whose
     * {@code Invariants.require}s can fail. We register one that throws for the key of the batch, so the throw lands in
     * production code exactly where a real one would.
     *
     * <p>The task is ASYNC over two loaded keys with a batch size of one, so it locks and runs with one key and keeps
     * its position on the other. The throw therefore happens with the second key (and the task's Command entry) still
     * in {@code refs} - untouched, cleanly claimed, and leaked in full pre-fix. The test then requires that a later
     * task on that second key runs and is notified within {@link #TIMEOUT_SECONDS}: pre-fix it waits for ever behind
     * the FAILED task's position, post-fix {@code releaseResourcesOnFailureExclusive} has dropped it. The same is
     * asserted introspectively, by reading {@code AccordCacheEntry.contains(task)} under the executor lock, as a
     * failure there names R6 directly.
     *
     * <p>Nothing is asserted about the key of the batch itself: the throw interrupts the middle of <em>its</em> release,
     * which no fix on the catch path can complete, so it keeps its reference and its lock either way. That is the
     * (separate) hazard of throwing from a listener, not the leak under test.
     */
    @Test
    public void completionFailureReleasesPositionsTest() throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey[] keys = { new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0))),
                              new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(1))) };

        RecordingAgent agent = new RecordingAgent();
        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, agent);
        // no schema behind us, so a key loads to a non-null CommandsForKey (so that the entry is worth caching, rather
        // than evicted as soon as it is unreferenced) and a modified one is persisted by a no-op
        executor.cacheUnsafe().types().forEach(AccordExecutorLoadFailureTest::setInMemoryFunctions);
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        // the key the batched task locks, and the task itself, both discovered by the run rather than assumed
        AtomicReference<RoutingKey> batched = new AtomicReference<>();
        AtomicReference<SafeTask<?>> failed = new AtomicReference<>();
        AtomicReference<Integer> batchSize = new AtomicReference<>();
        List<RoutingKey> threwFor = new CopyOnWriteArrayList<>();
        try
        {
            executor.executeDirectlyWithLock(() ->
                store.cachesUnsafe().commandsForKeys().register(new AccordCache.Listener<RoutingKey, CommandsForKey>()
                {
                    @Override
                    public void onUpdate(AccordCacheEntry<RoutingKey, CommandsForKey, ?> entry)
                    {
                        // once, and only for the key of the batch the task under test just ran
                        if (!entry.key().equals(batched.get()) || !threwFor.isEmpty())
                            return;
                        threwFor.add(entry.key());
                        throw new InjectedListenerFailure(entry.key());
                    }
                }));

            // both keys must be loaded before the batched task sets up, so that it takes a position on both and then
            // locks only one of them; a key still loading is optional and takes no position at all
            for (RoutingKey key : keys)
                loadIntoCache(executor, store, key);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            Condition done = Condition.newOneTimeCondition();
            ExecutionContext context = ExecutionContext.contextFor(TxnId.fromValues(1, 1, 0, new Id(1)), null, RoutingKeys.of(keys),
                                                                   LoadKeys.ASYNC, LoadKeysFor.READ_WRITE, "batched");
            store.execute(context, (Consumer<? super SafeCommandStore>) safeStore -> {
                SafeTask<?> task = ((SaferCommandStore) safeStore).task;
                failed.set(task);
                RoutingKeys active = task.nonSync().active;
                batchSize.set(active.size());
                RoutingKey key = active.get(0);
                batched.set(key);
                // modify the key we hold, so that releasing it notifies the cache listeners. SafeCommandsForKey.hasChanged
                // compares the byId/unmanageds arrays by identity, so a value built with fresh (empty) ones is a change
                ((SaferCommandsForKey) task.refs.get(key)).set(
                    CommandsForKey.SerializerSupport.create(key, new TxnInfo[0], 1000, new Unmanaged[0], TxnId.NONE,
                                                           CommandsForKey.NO_BOUNDS_INFO, false));
            }, (success, fail) -> {
                failure.set(fail);
                done.signal();
            });

            assertTrue("the batched task never ran", done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            // the run itself succeeds - the callback fires from within it - and only completing it throws
            assertTrue("the batched task failed with " + failure.get(), failure.get() == null);
            assertTrue("the batched task locked " + batchSize.get() + " of its two keys, so nothing is left behind in refs "
                       + "and this case proves nothing: check queue_nonsync_max_batch_size",
                       batchSize.get() != null && batchSize.get() == 1);
            assertTrue("the listener never threw, so the completion path did not fail and this case proves nothing",
                       await(() -> !threwFor.isEmpty()));
            // and it must have escaped completeExclusiveMayThrow, i.e. reached completeExclusiveNoExcept's catch, which
            // reports it to the agent; otherwise we are testing a throw that production code swallowed
            assertTrue("the injected failure was never reported to the agent, so it did not escape "
                       + "completeExclusiveMayThrow; the agent was told " + agent.exceptions,
                       await(() -> agent.exceptions.stream().anyMatch(AccordExecutorLoadFailureTest::isInjectedListener)));

            // the key the task kept a position on, and never processed
            RoutingKey kept = keys[0].equals(batched.get()) ? keys[1] : keys[0];
            AtomicReference<String> stillHeld = new AtomicReference<>();
            executor.executeDirectlyWithLock(() -> {
                AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(kept);
                if (entry != null && (entry.contains(failed.get()) || !entry.isUnclaimed()))
                    stillHeld.set(entry + " (contains the failed task: " + entry.contains(failed.get())
                                  + ", unclaimed: " + entry.isUnclaimed() + ')');
            });

            Condition afterDone = Condition.newOneTimeCondition();
            AtomicReference<Throwable> afterFailure = new AtomicReference<>();
            ExecutionContext after = ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(kept),
                                                                 LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after");
            store.execute(after, (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { afterFailure.set(fail); afterDone.signal(); });
            assertTrue("a task on " + kept + " was never run or notified: the task whose completion threw is FAILED and "
                       + "still holds its position there, so anything queued behind it waits for ever (R6)",
                       afterDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the task after the failed completion failed with " + afterFailure.get(), afterFailure.get() == null);
            assertTrue("R6: " + failed.get() + " is FAILED but still claims " + kept + ": " + stillHeld.get(),
                       stillHeld.get() == null);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** run a task on {@code key} and wait until its entry reports loaded, so that a later task takes a position on it */
    private static void loadIntoCache(AccordExecutor executor, AccordCommandStore store, RoutingKey key) throws InterruptedException
    {
        Condition ready = Condition.newOneTimeCondition();
        ExecutionContext context = ExecutionContext.contextFor(TxnId.fromValues(1, 100, 0, new Id(1)), null, RoutingKeys.of(key),
                                                               LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "preload");
        store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {}, (success, fail) -> ready.signal());
        assertTrue("preloading " + key + " never completed", ready.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        AtomicReference<Boolean> loaded = new AtomicReference<>(false);
        assertTrue("the entry for " + key + " never finished loading", await(() -> {
            executor.executeDirectlyWithLock(() -> {
                AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(key);
                loaded.set(entry != null && entry.isLoaded());
            });
            return loaded.get();
        }));
    }

    /** raw, because the cache types are heterogeneous: a key loads to an empty CommandsForKey, and saves are no-ops */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setInMemoryFunctions(AccordCache.Type type)
    {
        type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) ->
            k instanceof RoutingKey ? new CommandsForKey((RoutingKey) k) : null);
        type.unsafeSetSaveFunction((QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (ignoreStore, k, v, identity) -> () -> {});
    }

    /** poll {@code until} for up to {@link #TIMEOUT_SECONDS}; the conditions here are set by the executor's loop thread */
    private static boolean await(BooleanSupplier until) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline)
        {
            if (until.getAsBoolean())
                return true;
            Thread.sleep(1);
        }
        return false;
    }

    private static boolean isInjectedListener(Throwable t)
    {
        for (Throwable cur = t ; cur != null ; cur = cur.getCause())
        {
            if (cur instanceof InjectedListenerFailure)
                return true;
        }
        return false;
    }

    private static String stack(Throwable t)
    {
        StackTraceElement[] stack = t.getStackTrace();
        StringBuilder out = new StringBuilder();
        for (int i = 0 ; i < Math.min(4, stack.length) ; ++i)
            out.append(i == 0 ? "" : " <- ").append(stack[i]);
        return out.toString();
    }

    private static boolean isInjected(Throwable t)
    {
        for (Throwable cur = t ; cur != null ; cur = cur.getCause())
        {
            if (cur instanceof InjectedLoadFailure)
                return true;
        }
        return false;
    }

    /**
     * A command store with an in-memory journal and no persistence, so that we need no schema, cluster metadata or
     * commit log. We deliberately do not use {@code AccordAgent}, as reporting an exception there initialises
     * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
     */
    private static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor)
    {
        AtomicLong clock = new AtomicLong();
        LongSupplier now = clock::incrementAndGet;
        Id nodeId = new Id(1);
        NodeCommandStoreService node = new NodeCommandStoreService()
        {
            private final ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);
            private long stamp = 0;

            @Override public AsyncExecutor someExecutor() { return null; }
            @Override public ExclusiveAsyncExecutor someExclusiveExecutor() { return null; }
            @Override public accord.api.Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public DurabilityService durability() { return null; }
            @Override public Id id() { return nodeId; }
            @Override public long epoch() { return 1; }
            @Override public long now() { return now.getAsLong(); }
            @Override public long uniqueNow(long atLeast) { return now.getAsLong(); }
            @Override public long elapsed(TimeUnit units) { return elapsed.applyAsLong(units); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public Coordinations coordinations() { return new Coordinations(); }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() { ++stamp; }
            @Override public boolean isReplaying() { return false; }
            @Override public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, Timestamp applyAt, Writes writes, Result result) {}
        };

        return new AccordCommandStore(0, node, new TestAgent(), null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
