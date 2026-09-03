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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import org.junit.After;
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.ControllableRangeIndex;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test has been authored entirely by Claude.
 *
 * The range-scan half of a task's lifecycle, driven deterministically.
 *
 * <p>A task that declares {@link LoadKeysFor#RECOVERY} starts a {@code SafeTask.RangeTxnScanner} before it sets its keys
 * up: it passes through {@code SCANNING_RANGES}, and setup <em>re-enters</em> {@code onSetupOrScannedExclusive} when the
 * scan completes. That gives two things nothing else in the lifecycle does: a second setup pass, and a window in which
 * the task is neither loading nor waiting in the ordinary sense.
 *
 * <p>Both halves of the interleaving are under the test's control: the key's load blocks in
 * {@code unsafeSetLoadFunction}, and the scan blocks in the {@link ControllableRangeIndex.Decide} it is given, which
 * {@code ControllableLoader.load} calls off the executor thread. So "the load finished while we were still scanning" is
 * arranged rather than hoped for.
 */
public class AccordExecutorRangeScanTest
{
    private static final int TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        // run batched tasks over batches of one key, so a multi-key ASYNC task really does batch: it runs as soon as it
        // leads one key, and NonSyncState.prepareExclusive locks a batch rather than everything. AccordExecutor reads these
        // once, statically, so they must be set before we create one.
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = 1;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = 1;
    }

    @After
    public void clearRangeIndex()
    {
        AccordCommandStore.unsafeRangeIndexFactory = null;
    }

    /**
     * A <em>required</em> key load that completes while the task is in {@code SCANNING_RANGES}.
     *
     * <p>{@code onLoadOneExclusive}'s non-optional branch is guarded on {@code isState(LOADING_OR_WAITING_REQUIRED)}, so
     * unless {@code SCANNING_RANGES} is in that set the decrement is skipped and {@code waitingForState} never returns to
     * zero: the task is parked in {@code LOADING_REQUIRED} with a wait nothing will ever decrement, and never runs. Hence
     * this asserts completion rather than any particular internal state.
     */
    @Test
    public void requiredKeyLoadCompletesDuringScanTest() throws InterruptedException
    {
        Fixture f = new Fixture();
        try
        {
            f.blockScan();
            f.blockLoadOf(f.key);

            f.submitRecoveryTask(LoadKeys.SYNC, f.key);

            assertTrue("the key never started loading", f.loadStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the scan never started", f.scanStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // let the load complete, and wait until it really has, so that the decrement we are testing happens while the
            // task is still SCANNING_RANGES rather than after the scan
            f.releaseLoad.signal();
            f.awaitLoaded(f.key);

            f.releaseScan.signal();
            f.assertRanCleanly();
        }
        finally
        {
            f.close();
        }
    }

    /** the plain lifecycle: keys already loaded, scan completes, task runs */
    @Test
    public void scanCompletesThenTaskRunsTest() throws InterruptedException
    {
        Fixture f = new Fixture();
        try
        {
            f.blockScan();
            f.submitRecoveryTask(LoadKeys.SYNC, f.key);
            assertTrue("the scan never started", f.scanStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            f.releaseScan.signal();
            f.assertRanCleanly();
        }
        finally
        {
            f.close();
        }
    }

    /** and for a batched task, whose keys are optional and which therefore takes a different notification path */
    @Test
    public void scanCompletesThenBatchedTaskRunsTest() throws InterruptedException
    {
        Fixture f = new Fixture();
        try
        {
            f.blockScan();
            f.submitRecoveryTask(LoadKeys.ASYNC, f.key);
            assertTrue("the scan never started", f.scanStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            f.releaseScan.signal();
            f.assertRanCleanly();
        }
        finally
        {
            f.close();
        }
    }

    /**
     * A failing scan must abandon and release cleanly. The task itself is expected to fail - that is the point - but a
     * <em>later</em> task on the same key must still run, which is what proves the failed task did not keep its positions:
     * anything queued behind a task that will never run waits for ever.
     */
    @Test
    public void scanFailureReleasesPositionsTest() throws InterruptedException
    {
        Fixture f = new Fixture();
        try
        {
            f.failScan();
            f.submitRecoveryTask(LoadKeys.SYNC, f.key);
            assertTrue("the scanning task was never notified", f.done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNotNull("a failing scan should have failed its task", f.failure.get());
            assertTrue("unexpected failure: " + f.failure.get(),
                       hasCause(f.failure.get(), ControllableRangeIndex.InjectedScanFailure.class));

            // the key must be usable afterwards
            Condition secondDone = Condition.newOneTimeCondition();
            AtomicReference<Throwable> secondFailure = new AtomicReference<>();
            ExecutionContext context = ExecutionContext.contextFor(TxnId.fromValues(1, 2, 0, new Id(1)), null, RoutingKeys.of(f.key),
                                                                  LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "after");
            f.store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {},
                            (success, fail) -> { secondFailure.set(fail); secondDone.signal(); });
            assertTrue("a task on the same key never ran after a failed scan - the failed task kept its positions",
                       secondDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNull("the task after the failed scan failed", secondFailure.get());
        }
        finally
        {
            f.close();
        }
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type)
    {
        for (Throwable cur = t ; cur != null ; cur = cur.getCause())
        {
            if (type.isInstance(cur))
                return true;
        }
        return false;
    }

    /** one executor, one command store, one key, and the two blocking points */
    private static class Fixture
    {
        final TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        final RoutingKey key = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0)));
        /** three keys for the batching cases; {@link #key} is the first of them */
        final RoutingKey[] keys = { key,
                                   new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(1))),
                                   new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(2))) };
        final TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));

        final Condition loadStarted = Condition.newOneTimeCondition();
        final Condition releaseLoad = Condition.newOneTimeCondition();
        final Condition scanStarted = Condition.newOneTimeCondition();
        final Condition releaseScan = Condition.newOneTimeCondition();
        final Condition done = Condition.newOneTimeCondition();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        private final AccordExecutor executor;
        final AccordCommandStore store;
        private volatile ControllableRangeIndex.Outcome outcome = ControllableRangeIndex.Outcome.NOTHING;
        private volatile boolean blockScan;
        private volatile RoutingKey blockLoadKey;

        Fixture()
        {
            executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
            // installed before the store is built, as the store picks its index up in its constructor
            AccordCommandStore.unsafeRangeIndexFactory = cs -> new ControllableRangeIndex(cs, primaryTxnId -> {
                scanStarted.signal();
                if (blockScan)
                    releaseScan.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                return outcome;
            });
            // every test needs a load function: without one the cache tries to load for real, and with no schema, data
            // store or commit log behind it that is an NPE rather than a miss. Returning null is "not present".
            executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, k) -> {
                RoutingKey held = blockLoadKey;
                if (held != null && held.equals(k))
                {
                    loadStarted.signal();
                    releaseLoad.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                return null;
            }));
            store = commandStore(tableId, partitioner, executor);
            executor.executeDirectlyWithLock(() -> {
                executor.setCapacity(8 << 20);
                executor.setWorkingSetSize(4 << 20);
            });
        }

        void blockScan()
        {
            blockScan = true;
        }

        void failScan()
        {
            outcome = ControllableRangeIndex.Outcome.FAIL;
        }

        /** hold the load of {@code held} until {@link #releaseLoad} is signalled */
        void blockLoadOf(RoutingKey held)
        {
            blockLoadKey = held;
        }

        void submitRecoveryTask(LoadKeys loadKeys, RoutingKey... keys)
        {
            ExecutionContext context = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(keys),
                                                                   loadKeys, LoadKeysFor.RECOVERY, "scanning");
            store.execute(context, (Consumer<? super SafeCommandStore>) ignore -> {},
                          (success, fail) -> { failure.set(fail); done.signal(); });
        }

        /** poll until the entry for {@code k} reports loaded, read under the executor lock */
        void awaitLoaded(RoutingKey k) throws InterruptedException
        {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (System.nanoTime() < deadline)
            {
                AtomicReference<Boolean> loaded = new AtomicReference<>(false);
                executor.executeDirectlyWithLock(() -> {
                    AccordCacheEntry<?, ?, ?> entry = store.cachesUnsafe().commandsForKeys().getUnsafe(k);
                    loaded.set(entry != null && entry.isLoaded());
                });
                if (loaded.get())
                    return;
                Thread.sleep(1);
            }
            throw new AssertionError("the entry for " + k + " never finished loading");
        }

        void assertRanCleanly() throws InterruptedException
        {
            assertTrue("the task never completed - a wait was counted that nothing decremented",
                       done.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNull("the task failed", failure.get());
        }

        void close() throws InterruptedException
        {
            releaseLoad.signal();
            releaseScan.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /**
     * An in-memory journal and no persistence, so we need no schema, cluster metadata or commit log, and
     * {@link TestAgent} rather than {@code AccordAgent}, whose exception reporting initialises
     * {@code AccordSystemMetrics} and requires a started {@code AccordService}.
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
            @Override public Scheduler scheduler() { return null; }
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
