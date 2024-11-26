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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RemoteListeners;
import accord.api.RoutingKey;
import accord.api.Timeouts;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultTimeouts;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.basic.SimulatedFault;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.BeginRecovery;
import accord.messages.PreAccept;
import accord.messages.Reply;
import accord.messages.TxnRequest;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Gens;
import accord.utils.PersistentField;
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
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class SimulatedAccordCommandStore implements AutoCloseable
{
    private final List<Throwable> failures = new ArrayList<>();
    private final SimulatedExecutorFactory globalExecutor;
    private final CommandStore.EpochUpdateHolder updateHolder;
    private final BooleanSupplier shouldEvict, shouldFlush, shouldCompact;

    public final NodeCommandStoreService storeService;
    public final AccordCommandStore commandStore;
    public final Node.Id nodeId;
    public final Topology topology;
    public final Topologies topologies;
    public final IJournal journal;
    public final ScheduledExecutorPlus unorderedScheduled;
    public final List<String> evictions = new ArrayList<>();
    public Predicate<Throwable> ignoreExceptions = ignore -> false;

    public interface FunctionWrapper
    {
        <I1, I2, O> BiFunction<I1, I2, O> wrap(BiFunction<I1, I2, O> f);

        static <I1, I2, O> BiFunction<I1, I2, O> identity(BiFunction<I1, I2, O> f) { return f; }
        static FunctionWrapper identity() { return FunctionWrapper::identity; }
    }


    public SimulatedAccordCommandStore(RandomSource rs)
    {
        this(rs, FunctionWrapper.identity());
    }

    public SimulatedAccordCommandStore(RandomSource rs, FunctionWrapper loadFunctionWrapper)
    {
        globalExecutor = new SimulatedExecutorFactory(rs.fork(), fromQT(Generators.TIMESTAMP_GEN.map(java.sql.Timestamp::getTime)).mapToLong(TimeUnit.MILLISECONDS::toNanos).next(rs), failures::add);
        this.unorderedScheduled = globalExecutor.scheduled("ignored");
        ExecutorFactory.Global.unsafeSet(globalExecutor);
        for (Stage stage : Arrays.asList(Stage.READ, Stage.MUTATION))
            stage.unsafeSetExecutor(unorderedScheduled);
        for (Stage stage : Arrays.asList(Stage.MISC, Stage.ACCORD_MIGRATION, Stage.READ, Stage.MUTATION))
            stage.unsafeSetExecutor(globalExecutor.configureSequential("ignore").build());

        this.updateHolder = new CommandStore.EpochUpdateHolder();
        this.nodeId = AccordTopology.tcmIdToAccord(ClusterMetadata.currentNullable().myNodeId());
        this.storeService = new NodeCommandStoreService()
        {
            private final ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.NANOSECONDS, this::now);
            final Timeouts timeouts = new DefaultTimeouts(this);

            @Override public Timeouts timeouts() { return timeouts; }

            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }

            @Override
            public Timestamp uniqueNow()
            {
                return uniqueNow(Timestamp.NONE);
            }

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
            public long elapsed(TimeUnit unit)
            {
                return elapsed.applyAsLong(unit);
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

        this.journal = new InMemoryJournal(nodeId);
        TestAgent.RethrowAgent agent = new TestAgent.RethrowAgent()
        {
            @Override
            public long preAcceptTimeout()
            {
                return Long.MAX_VALUE;
            }

            @Override
            public void onUncaughtException(Throwable t)
            {
                if (ignoreExceptions.test(t)) return;
                super.onUncaughtException(t);
            }
        };
        this.commandStore = new AccordCommandStore(0,
                                                   storeService,
                                                   agent,
                                                   null,
                                                   ignore -> new ProgressLog.NoOpProgressLog(),
                                                   cs -> new DefaultLocalListeners(new RemoteListeners.NoOpRemoteListeners(), new DefaultLocalListeners.NotifySink()
                                                   {
                                                       @Override public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener) {}
                                                       @Override public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, LocalListeners.ComplexListener listener) { return false; }
                                                   }),
                                                   updateHolder,
                                                   journal,
                                                   new AccordExecutorSimple(0, CommandStore.class.getSimpleName() + '[' + 0 + ']', new AccordCacheMetrics("test"), agent));
        this.commandStore.executor().executeDirectlyWithLock(() -> {
            commandStore.executor().setCapacity(8 << 20);
            commandStore.executor().setWorkingSetSize(4 << 20);
        });
        this.topology = AccordTopology.createAccordTopology(ClusterMetadata.current());
        this.topologies = new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, topology);
        CommandStores.RangesForEpoch rangesForEpoch = new CommandStores.RangesForEpoch(topology.epoch(), topology.ranges());
        updateHolder.add(topology.epoch(), rangesForEpoch, topology.ranges());
        updateHolder.updateGlobal(topology.ranges());
        commandStore.unsafeUpdateRangesForEpoch();

        shouldEvict = boolSource(rs.fork());
        {
            // tests used to take 1m but after many changes in accord they now take many minutes and its due to flush... so lower the frequency of flushing
            RandomSource fork = rs.fork();
            shouldFlush = () -> fork.decide(.01);
        }
        shouldCompact = boolSource(rs.fork());

        commandStore.executor().cacheUnsafe().types().forEach(i -> {
            updateLoadFunction(i, loadFunctionWrapper);
            i.register(new AccordCache.Listener()
            {
                @Override
                public void onEvict(AccordCacheEntry state)
                {
                    evictions.add(i + " evicted " + state);
                }
            });
        });
    }

    private final class InMemoryJournal extends accord.impl.basic.InMemoryJournal implements IJournal
    {
        public InMemoryJournal(Node.Id id)
        {
            super(id);
        }

        public PersistentField.Persister<DurableBefore, DurableBefore> durableBeforePersister()
        {
            throw new IllegalArgumentException("Not implemented");
        }
    }

    private <K, V> void updateLoadFunction(AccordCache.Type<K, V, ?> i, FunctionWrapper wrapper)
    {
        i.unsafeSetLoadFunction(wrapper.wrap(i.unsafeGetLoadFunction()));
    }

    private static BooleanSupplier boolSource(RandomSource rs)
    {
        var gen = Gens.bools().mixedDistribution().next(rs);
        return () -> gen.next(rs);
    }

    public TxnId nextTxnId(Txn.Kind kind, Routable.Domain domain)
    {
        return new TxnId(storeService.epoch(), storeService.now(), kind, domain, nodeId);
    }

    public void maybeCacheEvict(Unseekables<?> keysOrRanges)
    {
        switch (keysOrRanges.domain())
        {
            case Key:
                maybeCacheEvict((AbstractUnseekableKeys) keysOrRanges, Ranges.EMPTY);
                break;
            case Range:
                maybeCacheEvict(RoutingKeys.EMPTY, (Ranges) keysOrRanges);
                break;
            default:
                throw new UnsupportedOperationException("Unknown domain: " + keysOrRanges.domain());
        }
    }

    public void maybeCacheEvict(Unseekables<RoutingKey> keys, Ranges ranges)
    {
        try (AccordExecutor.ExclusiveGlobalCaches caches = commandStore.executor().lockCaches())
        {
            AccordCache cache = caches.global;
            cache.evictionQueue().forEach(state -> {
                Class<?> keyType = state.key().getClass();
                if (TxnId.class.equals(keyType))
                {
                    Command command = (Command) state.getExclusive();
                    if (command != null && command.known().definition.isKnown()
                        && (command.partialTxn().keys().intersects(keys) || ranges.intersects(command.partialTxn().keys()))
                        && shouldEvict.getAsBoolean())
                        cache.tryEvict(state);
                }
                else if (RoutableKey.class.isAssignableFrom(keyType))
                {
                    RoutableKey key = (RoutableKey) state.key();
                    if ((keys.contains(key) || ranges.intersects(key))
                        && shouldEvict.getAsBoolean())
                        cache.tryEvict(state);
                }
                else
                {
                    throw new AssertionError("Unexpected key type: " + state.key().getClass());
                }
            });
        }

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

    private static boolean intersects(ColumnFamilyStore store, Memtable memtable, Unseekables<RoutingKey> keys, Ranges ranges)
    {
        if (keys.isEmpty() && ranges.isEmpty()) // shouldn't happen, but just in case...
            return false;
        switch (store.name)
        {
            case "commands_for_key":
                // pk = (store_id, routing_key)
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
        failures.removeIf(f -> f instanceof CancellationException || f instanceof SimulatedFault);
        if (failures.isEmpty()) return;
        AssertionError error = new AssertionError("Unexpected exceptions found");
        failures.forEach(error::addSuppressed);
        failures.clear();
        throw error;
    }

    public <T extends Reply> T process(TxnRequest<T> request) throws ExecutionException, InterruptedException
    {
        return process(request, request::apply);
    }

    public <T extends Reply> T process(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function) throws ExecutionException, InterruptedException
    {
        var result = processAsync(loadCtx, function);
        processAll();
        return AsyncChains.getBlocking(result);
    }

    public <T extends Reply> AsyncResult<T> processAsync(TxnRequest<T> request)
    {
        return processAsync(request, request::apply);
    }

    public <T extends Reply> AsyncResult<T> processAsync(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return commandStore.submit(loadCtx, function).beginAsResult();
    }

    public Pair<TxnId, AsyncResult<PreAccept.PreAcceptOk>> enqueuePreAccept(Txn txn, FullRoute<?> route)
    {
        TxnId txnId = nextTxnId(txn.kind(), txn.keys().domain());
        PreAccept preAccept = new PreAccept(nodeId, topologies, txnId, txn, route);
        return Pair.create(txnId, processAsync(preAccept, safe -> {
            var reply = preAccept.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            return (PreAccept.PreAcceptOk) reply;
        }));
    }

    public Pair<TxnId, AsyncResult<BeginRecovery.RecoverOk>> enqueueBeginRecovery(Txn txn, FullRoute<?> route)
    {
        TxnId txnId = nextTxnId(txn.kind(), txn.keys().domain());
        Ballot ballot = Ballot.fromValues(storeService.epoch(), storeService.now(), nodeId);
        BeginRecovery br = new BeginRecovery(nodeId, topologies, txnId, null, txn, route, ballot);

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
        commandStore.shutdown();
    }
}
