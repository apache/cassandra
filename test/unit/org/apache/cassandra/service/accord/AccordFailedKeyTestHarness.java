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

package org.apache.cassandra.service.accord;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.Coordinations;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink;
import accord.impl.DefaultRemoteListeners;
import accord.impl.TestAgent;
import accord.impl.basic.InMemoryJournal;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.TimeService;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.execution.AccordExecutor;

/**
 * A command store with an in-memory journal and no persistence, so that these tests need no schema, cluster metadata or
 * commit log. Deliberately not {@code AccordAgent}: reporting an exception there initialises
 * {@code AccordSystemMetrics}, which requires a started {@code AccordService}.
 */
public class AccordFailedKeyTestHarness
{
    /** records what the executor reports, so a test can assert on what reached the agent as well as the callback */
    public static class RecordingAgent extends TestAgent
    {
        public final java.util.List<Throwable> exceptions = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void onException(Throwable t)
        {
            exceptions.add(t);
        }

        @Override
        public void onException(Throwable t, String context)
        {
            exceptions.add(t);
        }

        public boolean saw(Class<? extends Throwable> type)
        {
            return exceptions.stream().anyMatch(type::isInstance);
        }
    }

    /** raw, because the cache types are heterogeneous: a key loads to an empty CommandsForKey, a TxnId to null */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void setInMemoryFunctions(org.apache.cassandra.service.accord.execution.AccordCache.Type type)
    {
        type.unsafeSetLoadFunction((java.util.function.BiFunction<AccordCommandStore, Object, Object>) (ignoreStore, k) ->
            k instanceof accord.api.RoutingKey ? new accord.local.cfk.CommandsForKey((accord.api.RoutingKey) k) : null);
        type.unsafeSetSaveFunction((accord.utils.QuadFunction<AccordCommandStore, Object, Object, Object, Runnable>) (s, k, v, i) -> () -> {});
    }

    /** run a task on {@code key} so that its entry is resident and loaded */
    public static void loadIntoCache(AccordExecutor executor, AccordCommandStore store, accord.api.RoutingKey key) throws InterruptedException
    {
        org.apache.cassandra.utils.concurrent.Condition ready = org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition();
        store.execute(accord.local.ExecutionContext.contextFor(TxnId.fromValues(1, 999, 0, new Id(1)), null,
                                                               accord.primitives.RoutingKeys.of(key),
                                                               accord.local.LoadKeys.SYNC, accord.local.LoadKeysFor.READ_WRITE, "preload"),
                      (java.util.function.Consumer<? super accord.local.SafeCommandStore>) ignore -> {},
                      (success, fail) -> ready.signal());
        if (!ready.await(30, TimeUnit.SECONDS))
            throw new AssertionError("preloading " + key + " never completed");
    }

    public static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor, TestAgent agent)
    {
        return commandStore(tableId, partitioner, executor, agent, null);
    }

    /** @param journal an override, for a test that needs to see what the store journals (and when); else in-memory */
    public static AccordCommandStore commandStore(TableId tableId, IPartitioner partitioner, AccordExecutor executor, TestAgent agent,
                                                  @javax.annotation.Nullable accord.api.Journal journal)
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

        return new AccordCommandStore(0, node, agent, null,
                                      cs -> new ProgressLog.NoOpProgressLog(),
                                      cs -> new DefaultLocalListeners(null, new DefaultRemoteListeners.NoOpRemoteListeners(), new NotifySink.NoOpNotifySink()),
                                      new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, partitioner))),
                                      journal != null ? journal : new InMemoryJournal(nodeId, new DefaultRandom(1)),
                                      executor);
    }
}
