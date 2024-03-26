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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.messages.BeginRecovery;
import accord.messages.Message;
import accord.messages.PreAccept;
import accord.messages.TxnRequest;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

class SimulatedAccordCommandStore implements AutoCloseable
{
    private final List<Throwable> failures = new ArrayList<>();
    private final SimulatedExecutorFactory globalExecutor;
    private final CommandStore.EpochUpdateHolder updateHolder;
    private final BooleanSupplier shouldEvict, shouldFlush, shouldCompact;

    public final NodeTimeService timeService;
    public final AccordCommandStore store;
    public final Node.Id nodeId;
    public final Topology topology;
    public final MockJournal journal;
    public final ScheduledExecutorPlus unorderedScheduled;
    public final List<String> evictions = new ArrayList<>();

    SimulatedAccordCommandStore(RandomSource rs)
    {
        globalExecutor = new SimulatedExecutorFactory(rs.fork(), fromQT(Generators.TIMESTAMP_GEN.map(java.sql.Timestamp::getTime)).mapToLong(TimeUnit.MILLISECONDS::toNanos).next(rs), failures::add);
        this.unorderedScheduled = globalExecutor.scheduled("ignored");
        ExecutorFactory.Global.unsafeSet(globalExecutor);
        Stage.READ.unsafeSetExecutor(unorderedScheduled);
        Stage.MUTATION.unsafeSetExecutor(unorderedScheduled);
        for (Stage stage : Arrays.asList(Stage.MISC, Stage.ACCORD_MIGRATION))
            stage.unsafeSetExecutor(globalExecutor.configureSequential("ignore").build());

        this.updateHolder = new CommandStore.EpochUpdateHolder();
        this.nodeId = AccordTopology.tcmIdToAccord(ClusterMetadata.currentNullable().myNodeId());
        this.timeService = new NodeTimeService()
        {
            private final ToLongFunction<TimeUnit> unixWrapper = NodeTimeService.unixWrapper(TimeUnit.NANOSECONDS, this::now);

            @Override
            public Node.Id id()
            {
                return nodeId;
            }

            @Override
            public long epoch()
            {
                return ClusterMetadata.current().epoch.getEpoch();
            }

            @Override
            public long now()
            {
                return globalExecutor.nanoTime();
            }

            @Override
            public long unix(TimeUnit unit)
            {
                return unixWrapper.applyAsLong(unit);
            }

            @Override
            public Timestamp uniqueNow(Timestamp atLeast)
            {
                var now = Timestamp.fromValues(epoch(), now(), nodeId);
                if (now.compareTo(atLeast) < 0)
                    throw new UnsupportedOperationException();
                return now;
            }
        };

        this.journal = new MockJournal();
        this.store = new AccordCommandStore(0,
                                            timeService,
                                            new TestAgent.RethrowAgent()
                                            {
                                                @Override
                                                public boolean isExpired(TxnId initiated, long now)
                                                {
                                                    return false;
                                                }
                                            },
                                            null,
                                            ignore -> AccordTestUtils.NOOP_PROGRESS_LOG,
                                            updateHolder,
                                            journal,
                                            new AccordStateCacheMetrics("test"));

        store.cache().instances().forEach(i -> {
            i.register(new AccordStateCache.Listener()
            {
                @Override
                public void onAdd(AccordCachingState state)
                {
                }

                @Override
                public void onRelease(AccordCachingState state)
                {
                }

                @Override
                public void onEvict(AccordCachingState state)
                {
                    evictions.add(i + " evicted " + state);
                }
            });
        });

        this.topology = AccordTopology.createAccordTopology(ClusterMetadata.current());
        var rangesForEpoch = new CommandStores.RangesForEpoch(topology.epoch(), topology.ranges(), store);
        updateHolder.add(topology.epoch(), rangesForEpoch, topology.ranges());
        updateHolder.updateGlobal(topology.ranges());

        shouldEvict = boolSource(rs.fork());
        shouldFlush = boolSource(rs.fork());
        shouldCompact = boolSource(rs.fork());
    }

    private static BooleanSupplier boolSource(RandomSource rs)
    {
        var gen = Gens.bools().mixedDistribution().next(rs);
        return () -> gen.next(rs);
    }

    public TxnId nextTxnId(Txn.Kind kind, Routable.Domain domain)
    {
        return new TxnId(timeService.epoch(), timeService.now(), kind, domain, nodeId);
    }

    public void maybeCacheEvict(Keys keys, Ranges ranges)
    {
        AccordStateCache cache = store.cache();
        cache.forEach(state -> {
            Class<?> keyType = state.key().getClass();
            if (TxnId.class.equals(keyType))
            {
                Command command = (Command) state.state().get();
                if (command.known().definition.isKnown()
                    && (command.partialTxn().keys().intersects(keys) || ranges.intersects(command.partialTxn().keys()))
                    && shouldEvict.getAsBoolean())
                    cache.maybeEvict(state);
            }
            else if (RoutableKey.class.isAssignableFrom(keyType))
            {
                RoutableKey key = (RoutableKey) state.key();
                if ((keys.contains(key) || ranges.intersects(key))
                    && shouldEvict.getAsBoolean())
                    cache.maybeEvict(state);
            }
            else
            {
                throw new AssertionError("Unexpected key type: " + state.key().getClass());
            }
        });

        for (var store : Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
        {
            Memtable memtable = store.getCurrentMemtable();
            if (memtable.partitionCount() == 0 || !intersects(store, memtable, keys, ranges))
                continue;
            if (shouldFlush.getAsBoolean())
                store.forceBlockingFlush(UNIT_TESTS);
        }
        for (var store : Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
        {
            if (store.getLiveSSTables().size() > 5 && shouldCompact.getAsBoolean())
            {
                // compaction no-op since auto-compaction is disabled... so need to enable quickly
                store.enableAutoCompaction();
                try
                {
                    FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(store));
                }
                finally
                {
                    store.disableAutoCompaction();
                }
            }
        }
    }

    private static boolean intersects(ColumnFamilyStore store, Memtable memtable, Keys keys, Ranges ranges)
    {
        if (keys.isEmpty() && ranges.isEmpty()) // shouldn't happen, but just in case...
            return false;
        switch (store.name)
        {
            case "commands_for_key":
                // pk = (store_id, key_token, key)
                // since this is simulating a single store, store_id is a constant, so check key
                try (var it = memtable.partitionIterator(ColumnFilter.NONE, DataRange.allData(store.getPartitioner()), null))
                {
                    while (it.hasNext())
                    {
                        var key = AccordKeyspace.CommandsForKeysAccessor.getKey(it.next().partitionKey());
                        if (keys.contains(key) || ranges.intersects(key))
                            return true;
                    }
                }
                break;
        }
        return false;
    }

    public void checkFailures()
    {
        if (Thread.interrupted())
            failures.add(new InterruptedException());
        if (failures.isEmpty()) return;
        AssertionError error = new AssertionError("Unexpected exceptions found");
        failures.forEach(error::addSuppressed);
        failures.clear();
        throw error;
    }

    public <T> T process(TxnRequest<T> request) throws ExecutionException, InterruptedException
    {
        return process(request, request::apply);
    }

    public <T> T process(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function) throws ExecutionException, InterruptedException
    {
        var result = processAsync(loadCtx, function);
        processAll();
        return AsyncChains.getBlocking(result);
    }

    public <T> AsyncResult<T> processAsync(TxnRequest<T> request)
    {
        return processAsync(request, request::apply);
    }

    public <T> AsyncResult<T> processAsync(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        if (loadCtx instanceof Message)
            journal.appendMessageBlocking((Message) loadCtx);
        return store.submit(loadCtx, function).beginAsResult();
    }

    public Pair<TxnId, AsyncResult<PreAccept.PreAcceptOk>> enqueuePreAccept(Txn txn, FullRoute<?> route)
    {
        TxnId txnId = nextTxnId(txn.kind(), txn.keys().domain());
        PreAccept preAccept = new PreAccept(nodeId, new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, topology), txnId, txn, route);
        return Pair.create(txnId, processAsync(preAccept, safe -> {
            var reply = preAccept.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            return (PreAccept.PreAcceptOk) reply;
        }));
    }

    public Pair<TxnId, AsyncResult<BeginRecovery.RecoverOk>> enqueueBeginRecovery(Txn txn, FullRoute<?> route)
    {
        TxnId txnId = nextTxnId(txn.kind(), txn.keys().domain());
        Ballot ballot = Ballot.fromValues(timeService.epoch(), timeService.now(), nodeId);
        BeginRecovery br = new BeginRecovery(nodeId, new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, topology), txnId, txn, route, ballot);

        return Pair.create(txnId, processAsync(br, safe -> {
            var reply = br.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            return (BeginRecovery.RecoverOk) reply;
        }).beginAsResult());
    }

    public void processAll()
    {
        while (processOne())
        {
        }
    }

    private boolean processOne()
    {
        boolean result = globalExecutor.processOne();
        checkFailures();
        return result;
    }

    @Override
    public void close() throws Exception
    {
        store.shutdown();
    }
}
