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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.messages.PreAccept;
import accord.messages.TxnRequest;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.transformations.AddAccordTable;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.wrapInTxn;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;

public class AccordCommandStoreFuzzTest extends CQLTester
{
    static
    {
        CassandraRelevantProperties.ENABLE_ACCORD_THREAD_CHECKS.setBoolean(false);
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
    }

    private static TableMetadata intTbl, reverseTokenTbl;
    private static Node.Id nodeId;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
    }

    @Before
    public void init()
    {
        if (intTbl != null)
            return;
        createKeyspace("CREATE KEYSPACE test WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }");
        createTable("test", "CREATE TABLE test.tbl1 (pk int PRIMARY KEY, value int)");
        intTbl = Schema.instance.getTableMetadata("test", "tbl1");
        AddAccordTable.addTable(intTbl.id);

        createTable("test", "CREATE TABLE test.tbl2 (pk vector<bigint, 2> PRIMARY KEY, value int)");
        reverseTokenTbl = Schema.instance.getTableMetadata("test", "tbl2");
        AddAccordTable.addTable(reverseTokenTbl.id);

        nodeId = AccordTopology.tcmIdToAccord(ClusterMetadata.current().myNodeId());

        ServerTestUtils.markCMS();
    }

    @Test
    public void emptyTxns()
    {
        qt().withExamples(10).check(rs -> {
           clearSystemTables();
            try (var instance = new Instance(rs))
            {
                for (int i = 0, examples = 100; i < examples; i++)
                {
                    TxnId id = AccordGens.txnIds().next(rs);
                    instance.process(PreLoadContext.contextFor(id), (safe) -> {
                        var safeCommand = safe.get(id, id, Ranges.EMPTY);
                        var command = safeCommand.current();
                        Assertions.assertThat(command.saveStatus()).isEqualTo(SaveStatus.Uninitialised);
                        return null;
                    });
                }
            }

        });
    }

    @Test
    public void keyConflicts()
    {
        TableMetadata tbl = intTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            int key = rs.nextInt();
            PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(Int32Type.instance.decompose(key)));
            Keys keys = Keys.of(pk);
            FullKeyRoute route = keys.toRoute(pk.toUnseekable());
            Txn txn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));
            try (var instance = new Instance(rs))
            {
                List<TxnId> conflicts = new ArrayList<>(numSamples);
                boolean concurrent = rs.nextBoolean();
                List<AsyncChain<?>> asyncs = !concurrent ? null : new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict(keys, Ranges.EMPTY);
                    if (concurrent)
                    {
                        var pair = assertPreAcceptAsync(instance, txn, route, conflicts, keys);
                        conflicts.add(pair.left);
                        asyncs.add(pair.right);
                    }
                    else
                    {
                        conflicts.add(assertPreAccept(instance, txn, route, conflicts, keys));
                    }
                }
                if (concurrent)
                {
                    instance.processAll();
                    for (var chain : asyncs)
                        AsyncChains.getBlocking(chain);
                }
            }
        });
    }

    @Test
    public void simpleRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        Ranges wholeRange = Ranges.of(fullRange(tbl.id));
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new Instance(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - 1, token));
                boolean useWholeRange = rs.nextBoolean();
                Ranges ranges = useWholeRange ? wholeRange : partialRange;
                FullRangeRoute rangeRoute = ranges.toRoute(pk.toUnseekable());
                Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, ranges);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    try
                    {
                        instance.maybeCacheEvict(keys, ranges);
                        keyConflicts.add(assertPreAccept(instance, keyTxn, keyRoute, keyConflicts, keys));
                        rangeConflicts.add(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts, keys, rangeConflicts, ranges));
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", isWholeRange=" + useWholeRange + ", range=" + ranges.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
            }
        });
    }

    @Test
    public void expandingRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 100;

        qt().withSeed(4760793912722218623L).withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new Instance(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE + numSamples + 1, Long.MAX_VALUE - numSamples);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - i - 1, token + i));
                    try
                    {
                        instance.maybeCacheEvict(keys, partialRange);
                        keyConflicts.add(assertPreAccept(instance, keyTxn, keyRoute, keyConflicts, keys));

                        FullRangeRoute rangeRoute = partialRange.toRoute(pk.toUnseekable());
                        Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);
                        rangeConflicts.add(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts, keys, rangeConflicts, partialRange));
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", range=" + partialRange.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
            }
        });
    }

    @Test
    public void overlappingRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            clearSystemTables();
            try (var instance = new Instance(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE + numSamples + 1, Long.MAX_VALUE - numSamples);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                Ranges left = Ranges.of(tokenRange(tbl.id, token - 10, token + 5));
                Ranges right = Ranges.of(tokenRange(tbl.id, token - 5, token + 10));

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    Ranges partialRange = rs.nextBoolean() ? left : right;
                    try
                    {
                        instance.maybeCacheEvict(keys, partialRange);
                        keyConflicts.add(assertPreAccept(instance, keyTxn, keyRoute, keyConflicts, keys));

                        FullRangeRoute rangeRoute = partialRange.toRoute(pk.toUnseekable());
                        Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);
                        rangeConflicts.add(assertPreAccept(instance, rangeTxn, rangeRoute, keyConflicts, keys, rangeConflicts, partialRange));
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", range=" + partialRange.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
            }
        });
    }

    private static TokenRange fullRange(TableId id)
    {
        return new TokenRange(AccordRoutingKey.SentinelKey.min(id), AccordRoutingKey.SentinelKey.max(id));
    }

    private static TokenRange tokenRange(TableId id, long start, long end)
    {
        return new TokenRange(start == Long.MIN_VALUE ? AccordRoutingKey.SentinelKey.min(id) : tokenKey(id, start), tokenKey(id, end));
    }

    private static AccordRoutingKey.TokenKey tokenKey(TableId id, long token)
    {
        return new AccordRoutingKey.TokenKey(id, new LongToken(token));
    }

    private static TxnId assertPreAccept(Instance instance,
                                         Txn txn, FullRoute<?> route,
                                         List<TxnId> keyConflicts, Keys keys) throws ExecutionException, InterruptedException
    {
        return assertPreAccept(instance, txn, route, keyConflicts, keys, Collections.emptyList(), Ranges.EMPTY);
    }

    private static TxnId assertPreAccept(Instance instance,
                                         Txn txn, FullRoute<?> route,
                                         List<TxnId> keyConflicts, Keys keys,
                                         List<TxnId> rangeConflicts, Ranges ranges) throws ExecutionException, InterruptedException
    {
        var pair = assertPreAcceptAsync(instance, txn, route, keyConflicts, keys, rangeConflicts, ranges);
        instance.processAll();
        AsyncChains.getBlocking(pair.right);

        return pair.left;
    }

    private static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(Instance instance,
                                                                      Txn txn, FullRoute<?> route,
                                                                      List<TxnId> keyConflicts, Keys keys)
    {
        return assertPreAcceptAsync(instance, txn, route, keyConflicts, keys, Collections.emptyList(), Ranges.EMPTY);
    }

    private static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(Instance instance,
                                                                      Txn txn, FullRoute<?> route,
                                                                      List<TxnId> keyConflicts, Keys keys,
                                                                      List<TxnId> rangeConflicts, Ranges ranges)
    {
        int kConflictSize = keyConflicts.size();
        int rConflictSize = rangeConflicts.size();
        TxnId txnId = new TxnId(instance.timeService.epoch(), instance.timeService.now(), txn.kind(), txn.keys().domain(), nodeId);
        var preAccept = new PreAccept(instance.nodeId, new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, instance.topology), txnId, txn, route);
        return Pair.create(txnId, instance.processAsync(preAccept, safe -> {
            var reply = preAccept.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            return (PreAccept.PreAcceptOk) reply;
        }).map(success -> {
            assertDeps(keyConflicts.subList(0, kConflictSize), keys, rangeConflicts.subList(0, rConflictSize), ranges, success);
            return null;
        }).beginAsResult());
    }

    private static void assertDeps(List<TxnId> keyConflicts, Keys keys,
                                   List<TxnId> rangeConflicts, Ranges ranges,
                                   PreAccept.PreAcceptOk success)
    {
        if (rangeConflicts.isEmpty())
        {
            Assertions.assertThat(success.deps.rangeDeps.isEmpty()).describedAs("rangeDeps was not empty").isTrue();
        }
        else
        {
            Assertions.assertThat(success.deps.rangeDeps.rangeCount()).describedAs("Expected ranges size").isEqualTo(ranges.size());
            for (int i = 0; i < ranges.size(); i++)
            {
                var expected = Ranges.of(ranges.get(i));
                var actual = success.deps.rangeDeps.ranges(i);
                Assertions.assertThat(actual).isEqualTo(expected);
                var conflict = success.deps.rangeDeps.txnIdsForRangeIndex(i);
                Assertions.assertThat(conflict).describedAs("Expected range %s to have different conflicting txns", expected).isEqualTo(rangeConflicts);
            }
        }
        if (keyConflicts.isEmpty())
        {
            Assertions.assertThat(success.deps.keyDeps.isEmpty()).describedAs("keyDeps was not empty").isTrue();
        }
        else
        {
            Assertions.assertThat(success.deps.keyDeps.keys()).describedAs("Keys").isEqualTo(keys);
            for (var key : keys)
                Assertions.assertThat(success.deps.keyDeps.txnIds(key)).describedAs("Txns for key %s", key).isEqualTo(keyConflicts);
        }
    }

    private static void clearSystemTables()
    {
        for (var store : Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.truncateBlockingWithoutSnapshot();
    }

    private static class Instance implements AutoCloseable
    {
        private final RandomSource rs;
        private final List<Throwable> failures = new ArrayList<>();
        private final SimulatedExecutorFactory globalExecutor;
        private final ExecutorPlus orderedExecutor;
        private final ScheduledExecutorPlus unorderedScheduled;
        private final AccordCommandStore store;
        private final CommandStore.EpochUpdateHolder updateHolder;
        private final NodeTimeService timeService;
        private final Node.Id nodeId;
        private final Topology topology;
        private final BooleanSupplier shouldEvict;

        private Instance(RandomSource rs)
        {
            this.rs = rs;
            globalExecutor = new SimulatedExecutorFactory(rs, fromQT(Generators.TIMESTAMP_GEN.map(java.sql.Timestamp::getTime)).mapToLong(TimeUnit.MILLISECONDS::toNanos).next(rs), failures::add);
            orderedExecutor = globalExecutor.configureSequential("ignore").build();
            unorderedScheduled = globalExecutor.scheduled("ignored");
            ExecutorFactory.Global.unsafeSet(globalExecutor);
            Stage.READ.unsafeSetExecutor(unorderedScheduled);
            Stage.MUTATION.unsafeSetExecutor(unorderedScheduled);

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

            this.store = new AccordCommandStore(0,
                                                timeService,
                                                new TestAgent.RethrowAgent(),
                                                null,
                                                ignore -> AccordTestUtils.NOOP_PROGRESS_LOG,
                                                updateHolder,
                                                new MockJournal(),
                                                new AccordStateCacheMetrics("test"));

            this.topology = AccordTopology.createAccordTopology(ClusterMetadata.current());
            var rangesForEpoch = new CommandStores.RangesForEpoch(topology.epoch(), topology.ranges(), store);
            updateHolder.add(topology.epoch(), rangesForEpoch, topology.ranges());
            updateHolder.updateGlobal(topology.ranges());

            int evictSelection = rs.nextInt(0, 3);
            switch (evictSelection)
            {
                case 0: // uniform 50/50
                    shouldEvict = rs::nextBoolean;
                    break;
                case 1: // variable frequency
                    var freq = rs.nextFloat();
                    shouldEvict = () -> rs.decide(freq);
                    break;
                case 2: // fixed result
                    boolean result = rs.nextBoolean();
                    shouldEvict = () -> result;
                    break;
                default:
                    throw new IllegalStateException("Unexpected int for evict selection: " + evictSelection);
            }
        }

        public void maybeCacheEvict(Keys keys, Ranges ranges)
        {
            AccordStateCache cache = store.cache();
            cache.forEach(state -> {
                if (!state.canEvict())
                    return;
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
                    //TODO (now, coverage): this is broken and conflicts with Benedict's work to make this stable... hold off testing this for now
                    // this is some form of CommandsForKey... possible matches are: TimestampsForKey, DepsCommandsForKey, AllCommandsForKey,and UpdatesForKey
//                    RoutableKey key = (RoutableKey) state.key();
//                    if (keys.contains(key) && rs.nextBoolean())
//                        cache.maybeEvict(state);
                }
                else if (Range.class.isAssignableFrom(keyType))
                {
                    Ranges key = Ranges.of((Range) state.key());
                    if ((key.intersects(keys) || key.intersects(ranges))
                        && shouldEvict.getAsBoolean())
                        cache.maybeEvict(state);
                }
                else
                {
                    throw new AssertionError("Unexpected key type: " + state.key().getClass());
                }
            });
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
            var result = store.submit(loadCtx, function).beginAsResult();
            processAll();
            return AsyncChains.getBlocking(result);
        }

        public <T> AsyncResult<T> processAsync(TxnRequest<T> request)
        {
            return processAsync(request, request::apply);
        }

        public <T> AsyncResult<T> processAsync(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
        {
            return store.submit(loadCtx, function).beginAsResult();
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
}
