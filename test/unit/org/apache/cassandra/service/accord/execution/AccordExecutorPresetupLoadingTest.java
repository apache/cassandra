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
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.junit.Assert.assertNull;
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
public class AccordExecutorPresetupLoadingTest
{
    private static final int TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        // run batched tasks over batches of one key, so that a task runs with one key while another is still loading;
        // AccordExecutor reads these once, statically, so they must be set before we create one
        DatabaseDescriptor.getAccord().queue_nonsync_min_batch_size = 1;
        DatabaseDescriptor.getAccord().queue_nonsync_max_batch_size = 1;
    }

    /** a SYNC task nested inside an ASYNC task, declaring a key its parent has not finished loading */
    @Test
    public void syncNestedInAsyncWithLoadingKeyTest() throws InterruptedException
    {
        test(LoadKeys.SYNC);
    }

    /** the same with a batched child, which is expected to pass */
    @Test
    public void asyncNestedInAsyncWithLoadingKeyTest() throws InterruptedException
    {
        test(LoadKeys.ASYNC);
    }

    private void test(LoadKeys nestedLoadKeys) throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        // the parent declares both keys but will run with only the first, as the second blocks in its load function
        RoutingKey ready = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0)));
        RoutingKey slow = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(1)));
        Condition released = Condition.newOneTimeCondition();
        Condition loading = Condition.newOneTimeCondition();

        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, key) -> {
            if (slow.equals(key))
            {
                loading.signal();
                released.awaitUninterruptibly(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            return null;
        }));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        AtomicReference<Throwable> nestedFailure = new AtomicReference<>();
        AtomicReference<SafeTask<?>> nestedTask = new AtomicReference<>();
        Condition nestedDone = Condition.newOneTimeCondition();
        Condition parentDone = Condition.newOneTimeCondition();
        try
        {
            ExecutionContext parentContext = ExecutionContext.contextFor(TxnId.fromValues(1, 1, 0, new Id(1)), null, RoutingKeys.of(ready, slow),
                                                                        LoadKeys.ASYNC, LoadKeysFor.READ_WRITE, "parent");
            store.execute(parentContext, (Consumer<? super SafeCommandStore>) safeStore -> {
                SafeTask<?> parent = ((SaferCommandStore) safeStore).task;
                if (parent.nonSync().active.contains(slow))
                    return; // we are running with the slow key, so it is loaded and there is nothing to test

                ExecutionContext nested = ExecutionContext.contextFor(null, null, RoutingKeys.of(slow), nestedLoadKeys, LoadKeysFor.READ_WRITE, "nested");
                nestedTask.set((SafeTask<?>) store.execute(nested, (Consumer<? super SafeCommandStore>) ignore -> {},
                                                          (success, fail) -> {
                                                              nestedFailure.set(fail);
                                                              nestedDone.signal();
                                                          }));
            }, (success, fail) -> parentDone.signal());

            // the parent runs with the ready key while the slow one is still loading, and submits the nested task while
            // holding a reference to the loading entry; the nested task is a consequence, so it is registered when the
            // parent completes - we wait for that before letting the load finish, so that the nested task really is set
            // up while its entry is loading
            assertTrue("the slow key never started loading", loading.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the parent never completed", parentDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (System.nanoTime() < deadline && (nestedTask.get() == null || nestedTask.get().is(Task.State.UNREGISTERED)))
                Thread.sleep(1);
            assertTrue("the nested task was never submitted", nestedTask.get() != null && !nestedTask.get().is(Task.State.UNREGISTERED));

            released.signal();
            assertTrue("the nested task was never notified", nestedDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertNull("the nested task failed", nestedFailure.get());
        }
        finally
        {
            released.signal();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
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
