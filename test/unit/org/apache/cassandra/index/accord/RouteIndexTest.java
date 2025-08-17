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

package org.apache.cassandra.index.accord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import accord.api.Journal;
import accord.api.RoutingKey;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.MaxDecidedRX.DecidedRX;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullKeyRoute;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property.Command;
import accord.utils.Property.UnitCommand;
import accord.utils.RandomSource;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ForwardingExecutorFactory;
import org.apache.cassandra.concurrent.ForwardingExecutorPlus;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OptionaldPositiveInt;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordJournal;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.IAccordService.AccordCompactionInfo;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LazyToString;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import static accord.local.RedundantStatus.SomeStatus.NONE;
import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;

public class RouteIndexTest extends CQLTester
{
    private static final Node.Id NODE = new Node.Id(42);
    private static final int MIN_TOKEN = 0;
    private static final int MAX_TOKEN = 1 << 18;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final int MAX_STORES = 10;
    private static final Gen.IntGen NUM_STORES_GEN = Gens.ints().between(1, MAX_STORES);
    private static final Gen<Gen.IntGen> TOKEN_DISTRIBUTION = Gens.mixedDistribution(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Gen.IntGen> RANGE_SIZE_DISTRIBUTION = Gens.mixedDistribution(10, (int) (TOKEN_RANGE_SIZE * .01));

    @BeforeClass
    public static void setUpClass()
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setAccordTransactionsEnabled(true);
        // disable journal compaction so the test can control when it happens
        DatabaseDescriptor.getAccord().enable_journal_compaction = false;
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        DatabaseDescriptor.setAutoSnapshot(false);

        CQLTester.prePrepareServer();

        // Journal will async release segment references and close files... this adds possible race condition issues with truncate
        // so make these steps happen inline.
        // Simulatored executors for the journal would be ideal, but given the blocking nature only a few executors can be simulated
        // without The Simulator, so full simulation is out of scope of this test.
        ExecutorFactory.Global.unsafeSet(new ForwardingExecutorFactory(ExecutorFactory.Global.executorFactory()) {
            @Override
            public SequentialExecutorPlus sequential(String name)
            {
                if (name.endsWith("-releaser") || name.endsWith("-closer"))
                    return new ForwardingExecutorPlus(ImmediateExecutor.INSTANCE);
                return super.sequential(name);
            }
        });

        CQLTester.InMemory.prepareServer();
    }

    private static TableId tableId = null;

    @Before
    public void setupTable()
    {
        if (tableId != null) return;
        String name = createTable("CREATE TABLE %s (pk int primary key) WITH " + TransactionalMode.full.asCqlParam());
        tableId = Keyspace.open(KEYSPACE).getColumnFamilyStore(name).metadata().id;
    }

    private Command<State, Sut, ?> insert(RandomSource rs, State state)
    {
        Domain domain = state.domainGen.next(rs);
        TxnId txnId = state.nextTxnId(domain);
        Route<?> route = createRoute(state, rs, domain, rs.nextInt(1, 20));
        int storeId = state.accordService.node().commandStores().unsafeForKey(route.homeKey()).id();
        return new InsertTxn(storeId, txnId, SaveStatus.PreAccepted, route);
    }

    private static KeySearch keySearchExisting(RandomSource rs, State state)
    {
        int storeId = rs.pickUnorderedSet(state.storeToTableToRangesToTxns.keySet());
        var tables = state.storeToTableToRangesToTxns.get(storeId);
        TableId tableId = rs.pickUnorderedSet(tables.keySet());
        var ranges = tables.get(tableId);
        TokenRange range = selectExistingRange(rs, ranges);

        // have a key, so find a key within the range
        long start = range.start().isMin() ? Long.MIN_VALUE : ((LongToken) range.start().token()).token;
        long end = range.end().isMax() ? Long.MAX_VALUE : ((LongToken) range.end().token()).token;
        long token = 1 + rs.nextLong(start, end);
        @Nullable DecidedRX decidedRX = state.nextDecidedRX(rs);
        return new KeySearch(storeId, new TokenKey(tableId, new LongToken(token)), state.nextTxnRange(rs), decidedRX);
    }

    private static RangeSearch rangeSearchExisting(RandomSource rs, State state)
    {
        int storeId = rs.pickUnorderedSet(state.storeToTableToRangesToTxns.keySet());
        var tables = state.storeToTableToRangesToTxns.get(storeId);
        TableId tableId = rs.pickUnorderedSet(tables.keySet());
        var ranges = tables.get(tableId);
        @Nullable DecidedRX decidedRX = state.nextDecidedRX(rs);
        return new RangeSearch(storeId, selectExistingRange(rs, ranges), state.nextTxnRange(rs), decidedRX);
    }

    private static Command<State, Sut, ?> rangeSearch(RandomSource rs, State state)
    {
        @Nullable DecidedRX decidedRX = state.nextDecidedRX(rs);
        return new RangeSearch(rs.nextInt(0, state.numStores), state.rangeGen.next(rs), state.nextTxnRange(rs), decidedRX);
    }

    private static Command<State, Sut, ?> keySearch(RandomSource rs, State state)
    {
        @Nullable DecidedRX decidedRX = state.nextDecidedRX(rs);
        return new KeySearch(rs.nextInt(0, state.numStores), new TokenKey(rs.pick(state.tables), new LongToken(state.tokenGen.nextInt(rs))), state.nextTxnRange(rs), decidedRX);
    }

    @Test
    public void test()
    {
        cfs().disableAutoCompaction(); // let the test control compaction
        //TODO (coverage): include with the ability to mark ranges as durable for compaction cleanup
        stateful().withExamples(10).withSteps(500).check(commands(() -> State::new, Sut::new)
                                          .destroyState(State::close)
                                          .destroySut(Sut::close)
                                          .addIf(State::mayFlush, CLOSE)
                                          .add(COMPACTOR)
                                          .addIf(State::mayCompact, COMPACT)
                                          .add(PURGE)
                                          .add(RESTART)
                                          .add(this::insert)
                                          .add(RouteIndexTest::rangeSearch)
                                          .add(RouteIndexTest::keySearch)
                                          .addIf(state -> !state.storeToTableToRangesToTxns.isEmpty(), RouteIndexTest::rangeSearchExisting)
                                          .addIf(state -> !state.storeToTableToRangesToTxns.isEmpty(), RouteIndexTest::keySearchExisting)
                                          .build());
    }

    private static class InsertTxn implements UnitCommand<State, Sut>
    {
        private final int storeId;
        private final TxnId txnId;
        private final SaveStatus saveStatus;
        private final StoreParticipants participants;

        private InsertTxn(int storeId, TxnId txnId, SaveStatus saveStatus, Route<?> route)
        {
            this.storeId = storeId;
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.participants = StoreParticipants.all(route);
        }

        @Override
        public void applyUnit(State state)
        {
            state.insertTxn(storeId, txnId, participants.route());
        }

        @Override
        public void runUnit(Sut sut)
        {
            sut.insertTxn(storeId, txnId, saveStatus, participants);
        }

        @Override
        public String toString()
        {
            return "InsertTxn{" +
                   "storeId=" + storeId +
                   ", txnId=" + txnId +
                   ", saveStatus=" + saveStatus +
                   ", participants=" + participants +
                   '}';
        }
    }

    private static class KeySearch implements Command<State, Sut, Set<TxnId>>
    {
        private final int storeId;
        private final TokenKey key;
        private final TxnRange txnRange;
        private final @Nullable DecidedRX decidedRX;

        private KeySearch(int storeId, TokenKey key, TxnRange txnRange, @Nullable DecidedRX decidedRX)
        {
            this.storeId = storeId;
            this.key = key;
            this.txnRange = txnRange;
            this.decidedRX = decidedRX;
        }

        @Override
        public Set<TxnId> apply(State state) throws Throwable
        {
            var tables = state.storeToTableToRangesToTxns.get(storeId);
            if (tables == null) return Collections.emptySet();
            var ranges = tables.get(key.table());
            if (ranges == null) return Collections.emptySet();
            Set<TxnId> matches = new HashSet<>();
            ranges.searchToken(key, e -> {
                TxnId txnId = e.getValue();
                if (decidedRX != null && txnId.is(Txn.Kind.ExclusiveSyncPoint) && decidedRX.excludeDecided(txnId))
                    return;
                if (txnRange.includes(txnId))
                    matches.add(txnId);
            });
            return matches;
        }

        @Override
        public Set<TxnId> run(Sut sut) throws Throwable
        {
            Set<TxnId> result = new ObjectHashSet<>();
            sut.journal.get().rangeSearcher().search(storeId, key, txnRange.minTxnId, txnRange.maxTxnId, decidedRX).consume(result::add);
            return result;
        }

        @Override
        public void checkPostconditions(State state, Set<TxnId> expected,
                                        Sut sut, Set<TxnId> actual)
        {
            Assertions.assertThat(actual).describedAs("Unexpected txns for key %s", key).isEqualTo(expected);
        }

        @Override
        public String toString()
        {
            return "KeySearch{" +
                   "storeId=" + storeId +
                   ", key=" + key +
                   ", decidedRX=" + decidedRX +
                   '}';
        }
    }

    private static class RangeSearch implements Command<State, Sut, Set<TxnId>>
    {
        private final int storeId;
        private final TokenRange range;
        private final TxnRange txnRange;
        private final DecidedRX decidedRX;

        private RangeSearch(int storeId, TokenRange range, TxnRange txnRange, DecidedRX decidedRX)
        {
            this.storeId = storeId;
            this.range = range;
            this.txnRange = txnRange;
            this.decidedRX = decidedRX;
        }

        @Override
        public Set<TxnId> apply(State state) throws Throwable
        {
            var tables = state.storeToTableToRangesToTxns.get(storeId);
            if (tables == null) return Collections.emptySet();
            var ranges = tables.get(range.table());
            if (ranges == null) return Collections.emptySet();
            Set<TxnId> matches = new HashSet<>();
            ranges.search(range, e -> {
                TxnId txnId = e.getValue();
                if (decidedRX != null && txnId.is(Txn.Kind.ExclusiveSyncPoint) && decidedRX.excludeDecided(txnId)) return;
                if (txnRange.includes(txnId))
                    matches.add(txnId);
            });
            return matches;
        }

        @Override
        public Set<TxnId> run(Sut sut) throws Throwable
        {
            Set<TxnId> result = new ObjectHashSet<>();
            sut.journal.get().rangeSearcher().search(storeId, range, txnRange.minTxnId, txnRange.maxTxnId, decidedRX).consume(result::add);
            return result;
        }

        @Override
        public void checkPostconditions(State state, Set<TxnId> expected,
                                        Sut sut, Set<TxnId> actual)
        {
            Assertions.assertThat(actual).describedAs("Unexpected txns for range %s; missing %s, added %s", range, LazyToString.lazy(() -> Sets.difference(expected, actual).toString()), LazyToString.lazy(() -> Sets.difference(actual, expected).toString())).isEqualTo(expected);
        }

        @Override
        public String toString()
        {
            return "RangeSearch{" +
                   "storeId=" + storeId +
                   ", range=" + range +
                   ", minDecidedId=" + decidedRX +
                   '}';
        }
    }

    private static abstract class CassandraCommand implements UnitCommand<State, Sut>
    {
        private final String name;

        protected CassandraCommand(String name)
        {
            this.name = name;
        }

        @Override
        public void applyUnit(State state)
        {
            // no-op
        }

        @Override
        public String detailed(State state)
        {
            return name;
        }
    }

    private static final CassandraCommand CLOSE = new CassandraCommand("Close Current Segment")
    {
        @Override
        public void runUnit(Sut sut)
        {
            sut.journal.get().closeCurrentSegmentForTestingIfNonEmpty();
        }
    };

    private static final CassandraCommand COMPACTOR = new CassandraCommand("Compactor")
    {
        @Override
        public void runUnit(Sut sut)
        {
            sut.journal.get().runCompactorForTesting();
        }
    };

    private static final CassandraCommand COMPACT = new CassandraCommand("Compact")
    {
        @Override
        public void runUnit(Sut sut)
        {
            try
            {
                sut.cfs.enableAutoCompaction();
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(sut.cfs));
            }
            finally
            {
                sut.cfs.disableAutoCompaction();
            }
        }
    };

    private static final CassandraCommand PURGE = new CassandraCommand("Purge")
    {
        @Override
        public void runUnit(Sut sut)
        {
            sut.journal.get().purge(sut.stores.get(), () -> 0);
        }
    };

    private static final UnitCommand<State, Sut> RESTART = new UnitCommand<State, Sut>()
    {
        @Override
        public void applyUnit(State state) throws Throwable
        {
            state.restartAccord();
        }

        @Override
        public void runUnit(Sut sut)
        {
            // no-op
        }

        @Override
        public String detailed(State state)
        {
            return "Restart Accord";
        }
    };

    private static class State implements AutoCloseable
    {
        private static final int MIN_TIMESTAMP = 1000;

        private final Int2ObjectHashMap<Map<TableId, Long2ObjectHashMap<List<TxnId>>>> storeToTableToRoutingKeysToTxns = new Int2ObjectHashMap<>();
        private final Int2ObjectHashMap<Map<TableId, RangeTree<TokenKey, TokenRange, TxnId>>> storeToTableToRangesToTxns = new Int2ObjectHashMap<>();
        private final Int2ObjectHashMap<RangesForEpoch> storeRangesForEpochs = new Int2ObjectHashMap<>();
        private final RedundantBefore emptyRedundantBefore = RedundantBefore.create(Ranges.of(TokenRange.fullRange(tableId, getPartitioner())), TxnId.NONE, NONE);

        private final int numStores;
        private final List<TableId> tables;
        private final Gen.IntGen tokenGen;
        private final Gen<TokenRange> rangeGen;
        private final Gen<Domain> domainGen;
        private final ColumnFamilyStore journalTable;
        private final float minDecidedIdNull;
        private final long txnWriteFrequency;
        private AccordService accordService;
        private int hlc = MIN_TIMESTAMP;

        public State(RandomSource rs)
        {
            numStores = NUM_STORES_GEN.nextInt(rs);
            DatabaseDescriptor.getAccord().command_store_shard_count = new OptionaldPositiveInt(numStores);
            tables = Collections.singletonList(tableId);
            tokenGen = TOKEN_DISTRIBUTION.next(rs);
            rangeGen = rangeGen(rs, tables);
            domainGen = ignore -> Domain.Range; // we shouldn't be saving/searching key transactions against ranges
            journalTable = Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL);

            for (int i = 0 ; i < numStores ; ++i)
                storeRangesForEpochs.put(i, new RangesForEpoch(1, Ranges.of(TokenRange.fullRange(tableId, getPartitioner()))));

            accordService = startAccord();
            accordService.epochReady(ClusterMetadata.current().epoch).awaitUninterruptibly();

            minDecidedIdNull = rs.nextFloat();
            txnWriteFrequency = rs.pickInt(1, // every txn is a Write
                                           2, // every other txn is a Write
                                           10, // Write txn every 10 txn
                                           100 // Write txn every 100 txn; in most cases this disables write txn
            );
        }

        AccordService startAccord()
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            AccordService.MetadataChangeListener.instance.resetForTesting(metadata);
            NodeId tcmNodeId = metadata.myNodeId();
            AccordService.unsafeSetNewAccordService(null);
            return AccordService.startup(tcmNodeId);
        }

        TxnId nextTxnId(Domain domain)
        {
            return idFor(domain, hlc++);
        }

        TxnId idFor(Domain domain, long hlc)
        {
            Txn.Kind kind = hlc % txnWriteFrequency == 0 ? Txn.Kind.Write : Txn.Kind.ExclusiveSyncPoint;
            return new TxnId(1, hlc, kind, domain, NODE);
        }

        TxnRange nextTxnRange(RandomSource rs)
        {
            long maxKnown = hlc;
            long minKnown = MIN_TIMESTAMP;
            return TxnRange.next(rs, minKnown, maxKnown, hlc -> idFor(Domain.Key, hlc));
        }

        private @Nullable DecidedRX nextDecidedRX(RandomSource rs)
        {
            if (rs.decide(minDecidedIdNull)) return null;
            long maxKnown = hlc;
            long minKnown = MIN_TIMESTAMP;
            TxnId txnId = minKnown == maxKnown ? idFor(Domain.Range, maxKnown)
                                               : idFor(Domain.Range, rs.nextLong(minKnown, maxKnown));
            return new DecidedRX(txnId, txnId);
        }

        void insertTxn(int storeId, TxnId txnId, Route<?> route)
        {
            for (var u : Objects.requireNonNull(route))
            {
                switch (u.domain())
                {
                    case Key:
                    {
                        TokenKey key = (TokenKey) u;
                        var table = key.table();
                        var token = key.token().getLongValue();
                        storeToTableToRoutingKeysToTxns.computeIfAbsent(storeId, ignore -> new HashMap<>())
                                                             .computeIfAbsent(table, ignore -> new Long2ObjectHashMap<>())
                                                             .computeIfAbsent(token, ignore -> new ArrayList<>())
                                                             .add(txnId);
                    }
                    break;
                    case Range:
                    {
                        TokenRange range = (TokenRange) u;
                        var table = range.table();
                        storeToTableToRangesToTxns.computeIfAbsent(storeId, ignore -> new HashMap<>())
                                                        .computeIfAbsent(table, ignore -> rangeTree())
                                                        .add(range, txnId);
                    }
                    break;
                    default:
                        throw new AssertionError("Unexpected domain: " + u.domain());
                }
            }
        }

        public boolean mayFlush()
        {
            return accordService.journal().inMemorySize() > 0;
        }

        public boolean mayCompact()
        {
            return journalTable.getLiveSSTables().size() > 1;
        }

        @Override
        public String toString()
        {
            return "State{" +
                   "numStores=" + numStores +
                   ", tables=" + tables +
                   '}';
        }

        @Override
        public void close()
        {
            accordService.shutdown();
        }

        private void restartAccord()
        {
            accordService.journal().closeCurrentSegmentForTestingIfNonEmpty();
            accordService.shutdown();
            accordService = startAccord();
        }
    }

    public static class Sut implements AutoCloseable
    {
        private final ColumnFamilyStore cfs;
        private final Supplier<CommandStores> stores;
        private final Supplier<AccordJournal> journal;

        public Sut(State state)
        {
            cfs = cfs();
            this.stores = () -> state.accordService.node().commandStores();
            this.journal = () -> state.accordService.journal();
        }

        void insertTxn(int storeId, TxnId txnId, SaveStatus saveStatus, StoreParticipants participants)
        {
            Txn txn = toTxn(txnId, participants);
            AccordGenerators.CommandBuilder builder = new AccordGenerators.CommandBuilder(txnId, txn, txnId, txn.slice(participants.owns().toRanges(), true), PartialDeps.NONE, Ballot.ZERO, Ballot.ZERO, accord.local.Command.WaitingOn.none(txnId.domain(), Deps.NONE));
            var cmd = builder.build(saveStatus);
            CountDownLatch latch = CountDownLatch.newCountDownLatch(1);
            journal.get().saveCommand(storeId, new Journal.CommandUpdate(null, cmd), () -> latch.decrement());
            latch.awaitThrowUncheckedOnInterrupt();
        }

        private static Txn toTxn(TxnId txnId, StoreParticipants participants)
        {
            Ranges ranges = Objects.requireNonNull(participants.route()).toRanges();
            return AccordTestUtils.createTxn(txnId.kind(), ranges);
        }

        @Override
        public void close() throws Exception
        {
            journal.get().truncateForTesting();
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            cfs.truncateBlocking();
        }
    }

    private static RangeTree<TokenKey, TokenRange, TxnId> rangeTree()
    {
        return RTree.create(ACCESSOR);
    }

    private static final RangeTree.Accessor<TokenKey, TokenRange> ACCESSOR = new RangeTree.Accessor<>()
    {
        @Override
        public TokenKey start(TokenRange tokenRange)
        {
            return tokenRange.start();
        }

        @Override
        public TokenKey end(TokenRange tokenRange)
        {
            return tokenRange.end();
        }

        @Override
        public boolean contains(TokenKey start, TokenKey end, TokenKey tokenKey)
        {
            return TokenRange.create(start, end).contains(tokenKey);
        }

        @Override
        public boolean intersects(TokenRange tokenRange, TokenKey start, TokenKey end)
        {
            return tokenRange.compareIntersecting(TokenRange.create(start, end)) == 0;
        }
    };

    private static IAccordService.AccordCompactionInfos emptyCompactionInfo(TableId tableId, RedundantBefore redundantBefore, Int2ObjectHashMap<RangesForEpoch> storeRangesForEpoch)
    {
        IAccordService.AccordCompactionInfos compactionInfos = new IAccordService.AccordCompactionInfos(DurableBefore.EMPTY, 0);
        for (int i = 0; i < storeRangesForEpoch.size(); i++)
            compactionInfos.put(i, new AccordCompactionInfo(i, redundantBefore, storeRangesForEpoch.get(i), tableId));
        return compactionInfos;
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME)
                       .getColumnFamilyStore(AccordKeyspace.JOURNAL);
    }

    private static Gen<TokenRange> rangeGen(RandomSource rand, List<TableId> tables)
    {
        Gen.IntGen tokenGen = TOKEN_DISTRIBUTION.next(rand);
        Gen<TableId> tableIdGen = Gens.mixedDistribution(tables).next(rand);
        switch (rand.nextInt(0, 3))
        {
            case 0: // pure random
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int b = tokenGen.nextInt(rs);
                    while (a == b)
                        b = tokenGen.nextInt(rs);
                    if (a > b)
                    {
                        int tmp = a;
                        a = b;
                        b = tmp;
                    }
                    TableId tableId = tableIdGen.next(rs);
                    return TokenRange.create(new TokenKey(tableId, new LongToken(a)),
                                             new TokenKey(tableId, new LongToken(b)));
                };
            case 1: // small range
                Gen.IntGen rangeSizeGen = RANGE_SIZE_DISTRIBUTION.next(rand);
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int rangeSize = rangeSizeGen.nextInt(rs);
                    int b = a + rangeSize;
                    if (b > MAX_TOKEN)
                    {
                        b = a;
                        a = b - rangeSize;
                    }
                    TableId tableId = tableIdGen.next(rs);
                    return TokenRange.create(new TokenKey(tableId, new LongToken(a)),
                                             new TokenKey(tableId, new LongToken(b)));
                };
            case 2: // single element
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int b = a + 1;
                    TableId tableId = tableIdGen.next(rs);
                    return TokenRange.create(new TokenKey(tableId, new LongToken(a)),
                                             new TokenKey(tableId, new LongToken(b)));
                };
            default:
                throw new AssertionError();
        }
    }

    private static Route<?> createRoute(State state, RandomSource rs, Domain domain, int numKeys)
    {
        switch (domain)
        {
            case Key:
            {
                TreeSet<TokenKey> keys = new TreeSet<>();
                while (keys.size() < numKeys)
                {
                    var table = rs.pick(state.tables);
                    var token = new LongToken(state.tokenGen.nextInt(rs));
                    keys.add(new TokenKey(table, token));
                }
                return new FullKeyRoute(keys.first(), keys.toArray(RoutingKey[]::new));
            }
            case Range:
            {
                TreeSet<TokenRange> set = new TreeSet<>(Range::compareTo);
                while (set.size() < numKeys)
                    set.add(state.rangeGen.next(rs));
                return Ranges.ofSorted(set.toArray(Range[]::new)).toRoute(set.first().end());
            }
            default:
                throw new IllegalArgumentException("Unknown domain: " + domain);
        }
    }

    private static TokenRange selectExistingRange(RandomSource rs, RangeTree<TokenKey, TokenRange, TxnId> ranges)
    {
        TreeSet<TokenRange> distinctRanges = ranges.stream().map(Map.Entry::getKey).collect(Collectors.toCollection(() -> new TreeSet<>(TokenRange::compareTo)));
        TokenRange range;
        if (distinctRanges.size() == 1)
        {
            range = Iterables.getFirst(distinctRanges, null);
        }
        else
        {
            switch (rs.nextInt(0, 2))
            {
                case 0: // perfect match
                    range = rs.pickOrderedSet(distinctRanges);
                    break;
                case 1: // mutli-match
                {
                    TokenRange a = rs.pickOrderedSet(distinctRanges);
                    TokenRange b = rs.pickOrderedSet(distinctRanges);
                    while (a.equals(b))
                        b = rs.pickOrderedSet(distinctRanges);
                    if (b.compareTo(a) < 0)
                    {
                        TokenRange tmp = a;
                        a = b;
                        b = tmp;
                    }
                    range = TokenRange.create(a.start(), b.end());
                }
                break;
                default:
                    throw new AssertionError();
            }
        }
        return range;
    }
}