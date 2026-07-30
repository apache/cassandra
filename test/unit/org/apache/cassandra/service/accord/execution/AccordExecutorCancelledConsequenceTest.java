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

import static org.junit.Assert.assertNotNull;
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
public class AccordExecutorCancelledConsequenceTest
{
    private static final int TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * When a task fails, each of its consequences is cancelled with
     * {@code failExclusive(new CancellationException("Parent task failed"), CANCELLED)}. That neither completes the task
     * nor releases its resources, so the cache references {@link SafeTask#preSetup} already acquired for it - a
     * consequence submitted from within a running task on the same command store inherits its parent's references
     * eagerly, before it is ever registered - are leaked for the lifetime of the process. The entry can then never be
     * evicted, as it is permanently referenced.
     */
    @Test
    public void cancelledConsequenceReleasesReferencesTest() throws InterruptedException
    {
        test(true);
    }

    /** the same with a parent that succeeds, which is expected to pass */
    @Test
    public void completedConsequenceReleasesReferencesTest() throws InterruptedException
    {
        test(false);
    }

    private void test(boolean parentFails) throws InterruptedException
    {
        TableId tableId = TableId.fromUUID(new java.util.UUID(0, 1));
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        RoutingKey key = new TokenKey(tableId, partitioner.getToken(Int32Type.instance.decompose(0)));
        TxnId txnId = TxnId.fromValues(1, 1, 0, new Id(1));

        AccordExecutor executor = new AccordExecutorSignalLoop(0, RUN_WITHOUT_LOCK, 2, -1, -1, TimeUnit.MICROSECONDS, i -> "Loop" + i, new TestAgent());
        executor.cacheUnsafe().types().forEach(type -> type.unsafeSetLoadFunction((ignoreStore, ignoreKey) -> null));
        AccordCommandStore store = commandStore(tableId, partitioner, executor);
        executor.executeDirectlyWithLock(() -> {
            executor.setCapacity(8 << 20);
            executor.setWorkingSetSize(4 << 20);
        });

        Condition parentDone = Condition.newOneTimeCondition();
        Condition nestedDone = Condition.newOneTimeCondition();
        AtomicReference<Throwable> nestedFailure = new AtomicReference<>();
        try
        {
            ExecutionContext parentContext = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "parent");
            store.execute(parentContext, (Consumer<? super SafeCommandStore>) safeStore -> {
                // submitted from within the parent, on the parent's store, so it inherits the parent's references
                ExecutionContext nested = ExecutionContext.contextFor(txnId, null, RoutingKeys.of(key), LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "nested");
                store.execute(nested, (Consumer<? super SafeCommandStore>) ignore -> {},
                              (success, fail) -> {
                                  nestedFailure.set(fail);
                                  nestedDone.signal();
                              });
                if (parentFails)
                    throw new RuntimeException("parent fails");
            }, (success, fail) -> parentDone.signal());

            assertTrue("the parent was never notified", parentDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("the nested task was never notified", nestedDone.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            if (parentFails)
                assertNotNull("the nested task should have been cancelled", nestedFailure.get());
            else
            {
                // a failed task also releases its references and also invokes its callback, so without this the happy
                // path is satisfied by a consequence that failed - including with the IllegalStateException this class
                // exists to rule out
                assertNull("the nested consequence failed: " + nestedFailure.get(), nestedFailure.get());
            }

            // the executors are idle, so anything still referenced has been leaked
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            String referenced;
            while ((referenced = stillReferenced(store)) != null && System.nanoTime() < deadline)
                Thread.sleep(1);
            assertNull(referenced, referenced);
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static String stillReferenced(AccordCommandStore store)
    {
        try (AccordCommandStore.ExclusiveCaches caches = store.lockCaches())
        {
            for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
            }
            for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
            {
                if (entry.references() != 0)
                    return "leaked " + entry.references() + " references to " + entry;
            }
        }
        return null;
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
