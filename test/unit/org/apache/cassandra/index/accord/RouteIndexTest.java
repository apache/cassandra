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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.primitives.FullKeyRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Route;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Property.Command;
import accord.utils.Property.Commands;
import accord.utils.Property.UnitCommand;
import accord.utils.RandomSource;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.stateful;

public class RouteIndexTest extends CQLTester.InMemory
{
    static
    {
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
    }

    private static final Node.Id NODE = new Node.Id(42);
    private static final int MIN_TOKEN = 0;
    private static final int MAX_TOKEN = 1 << 18;
    private static final int TOKEN_RANGE_SIZE = MAX_TOKEN - MIN_TOKEN + 1;
    private static final Gen.IntGen NUM_STORES_GEN = Gens.ints().between(1, 10);
    private static final Gen.IntGen NUM_TABLES_GEN = Gens.ints().between(1, 10);
    private static final Gen<Gen.IntGen> TOKEN_DISTRIBUTION = Gens.mixedDistribution(MIN_TOKEN, MAX_TOKEN + 1);
    private static final Gen<Gen.IntGen> RANGE_SIZE_DISTRIBUTION = Gens.mixedDistribution(10, (int) (TOKEN_RANGE_SIZE * .01));
    private static final Gen<Gen<Domain>> DOMAIN_DISTRIBUTION = Gens.mixedDistribution(Domain.values());
    private static RoutesSearcher ROUTES_SEARCHER = null;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.InMemory.setUpClass();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        ROUTES_SEARCHER = new RoutesSearcher();
    }

    @Test
    public void test()
    {
        cfs().disableAutoCompaction(); // let the test control compaction
        //TODO (coverage): include with the ability to mark ranges as durable for compaction cleanup
        AccordService.unsafeSetNoop(); // disable accord service since compaction touches it.  It would be nice to include this for cleanup support....
        stateful().withExamples(50).check(new Commands<State, ColumnFamilyStore>()
        {
            @Override
            public Gen<State> genInitialState()
            {
                return rs -> new State(rs);
            }

            @Override
            public ColumnFamilyStore createSut(State state)
            {
                return cfs();
            }

            @Override
            public Gen<Command<State, ColumnFamilyStore, ?>> commands(State state)
            {
                Map<Gen<Command<State, ColumnFamilyStore, ?>>, Integer> possible = new HashMap<>();
                possible.put(ignore -> FLUSH, 1);
                possible.put(ignore -> COMPACT, 1);
                possible.put(rs -> {
                    int storeId = rs.nextInt(0, state.numStores);
                    Domain domain = state.domainGen.next(rs);
                    TxnId txnId = state.nextTxnId(domain);
                    Route<?> route = createRoute(state, rs, domain, rs.nextInt(1, 20));
                    return new InsertTxn(storeId, txnId, SaveStatus.PreAccepted, Durability.NotDurable, route);
                }, 10);
                possible.put(rs -> new RangeSearch(rs.nextInt(0, state.numStores), state.rangeGen.next(rs)), 1);
                if (!state.storeToTableToRangesToTxns.isEmpty())
                {
                    possible.put(rs -> {
                        int storeId = rs.pick(state.storeToTableToRangesToTxns.keySet());
                        var tables = state.storeToTableToRangesToTxns.get(storeId);
                        TableId tableId = rs.pick(tables.keySet());
                        var ranges = tables.get(tableId);
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
                                    range = rs.pick(distinctRanges);
                                    break;
                                case 1: // mutli-match
                                {
                                    TokenRange a = rs.pick(distinctRanges);
                                    TokenRange b = rs.pick(distinctRanges);
                                    while (a.equals(b))
                                        b = rs.pick(distinctRanges);
                                    if (b.compareTo(a) < 0)
                                    {
                                        TokenRange tmp = a;
                                        a = b;
                                        b = tmp;
                                    }
                                    range = new TokenRange((AccordRoutingKey) a.start(), (AccordRoutingKey) b.end());
                                }
                                break;
                                default:
                                    throw new AssertionError();
                            }
                        }
                        return new RangeSearch(storeId, range);
                    }, 5);
                }
                return Gens.oneOf(possible);
            }

            @Override
            public void destroySut(ColumnFamilyStore sut)
            {
                cfs().truncateBlocking();
            }
        });
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME)
                       .getColumnFamilyStore(AccordKeyspace.COMMANDS);
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
                    return new TokenRange(new TokenKey(tableId, new LongToken(a)),
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
                    return new TokenRange(new TokenKey(tableId, new LongToken(a)),
                                          new TokenKey(tableId, new LongToken(b)));
                };
            case 2: // single element
                return rs -> {
                    int a = tokenGen.nextInt(rs);
                    int b = a + 1;
                    TableId tableId = tableIdGen.next(rs);
                    return new TokenRange(new TokenKey(tableId, new LongToken(a)),
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
                TreeSet<AccordRoutingKey> keys = new TreeSet<>();
                while (keys.size() < numKeys)
                {
                    var table = rs.pick(state.tables);
                    var token = new LongToken(state.tokenGen.nextInt(rs));
                    keys.add(new TokenKey(table, token));
                }
                return new FullKeyRoute(keys.first(), true, keys.toArray(RoutingKey[]::new));
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

    private class InsertTxn implements UnitCommand<State, ColumnFamilyStore>
    {
        private static final String cql = "INSERT INTO system_accord.commands (store_id, domain, txn_id, status, route, durability) VALUES (?, ?, ?, ?, ?, ?)";
        private final int storeId;
        private final TxnId txnId;
        private final SaveStatus saveStatus;
        private final Durability durability;
        private final Route<?> route;

        private InsertTxn(int storeId, TxnId txnId, SaveStatus saveStatus, Durability durability, Route<?> route)
        {
            this.storeId = storeId;
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.durability = durability;
            this.route = route;
        }

        @Override
        public void applyUnit(State state)
        {
            for (var u : route)
            {
                switch (u.domain())
                {
                    case Key:
                    {
                        AccordRoutingKey key = (AccordRoutingKey) u;
                        var table = key.table();
                        var token = key.token().getLongValue();
                        state.storeToTableToRoutingKeysToTxns.computeIfAbsent(storeId, ignore -> new HashMap<>())
                                                             .computeIfAbsent(table, ignore -> new Long2ObjectHashMap<>())
                                                             .computeIfAbsent(token, ignore -> new ArrayList<>())
                                                             .add(txnId);
                    }
                    break;
                    case Range:
                    {
                        TokenRange range = (TokenRange) u;
                        var table = range.table();
                        state.storeToTableToRangesToTxns.computeIfAbsent(storeId, ignore -> new HashMap<>())
                                                        .computeIfAbsent(table, ignore -> rangeTree())
                                                        .add(range, txnId);
                    }
                    break;
                    default:
                        throw new AssertionError("Unexpected domain: " + u.domain());
                }
            }
        }

        @Override
        public void runUnit(ColumnFamilyStore sut) throws Throwable
        {
            execute(cql, storeId, txnId.domain().ordinal(), AccordKeyspace.serializeTimestamp(txnId), saveStatus.ordinal(), AccordKeyspace.serializeRoute(route), durability.ordinal());
        }

        @Override
        public String toString()
        {
            return "InsertTxn{" +
                   "storeId=" + storeId +
                   ", txnId=" + txnId +
                   ", saveStatus=" + saveStatus +
                   ", durability=" + durability +
                   ", route=" + route +
                   '}';
        }
    }

    private class RangeSearch implements Command<State, ColumnFamilyStore, Set<TxnId>>
    {
        private final int storeId;
        private final TokenRange range;

        private RangeSearch(int storeId, TokenRange range)
        {
            this.storeId = storeId;
            this.range = range;
        }

        @Override
        public Set<TxnId> apply(State state) throws Throwable
        {
            var tables = state.storeToTableToRangesToTxns.get(storeId);
            if (tables == null) return Collections.emptySet();
            var ranges = tables.get(range.table());
            if (ranges == null) return Collections.emptySet();
            Set<TxnId> matches = new HashSet<>();
            ranges.search(range, e -> matches.add(e.getValue()));
            return matches;
        }

        @Override
        public Set<TxnId> run(ColumnFamilyStore sut) throws Throwable
        {
            return ROUTES_SEARCHER.intersects(storeId, range);
        }

        @Override
        public void checkPostconditions(State state, Set<TxnId> expected,
                                        ColumnFamilyStore sut, Set<TxnId> actual)
        {
            Assertions.assertThat(actual).describedAs("Unexpected txns for range %s", range).isEqualTo(expected);
        }

        @Override
        public String toString()
        {
            return "RangeSearch{" +
                   "storeId=" + storeId +
                   ", range=" + range +
                   '}';
        }
    }

    private static abstract class CassandraCommand implements UnitCommand<State, ColumnFamilyStore>
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

    private static final CassandraCommand FLUSH = new CassandraCommand("Flush")
    {
        @Override
        public void runUnit(ColumnFamilyStore sut)
        {
            sut.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
    };

    private static final CassandraCommand COMPACT = new CassandraCommand("Compact")
    {
        @Override
        public void runUnit(ColumnFamilyStore sut)
        {
            try
            {
                sut.enableAutoCompaction();
                FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(sut));
            }
            finally
            {
                sut.disableAutoCompaction();
            }
        }
    };

    private static class State
    {
        private final Int2ObjectHashMap<Map<TableId, Long2ObjectHashMap<List<TxnId>>>> storeToTableToRoutingKeysToTxns = new Int2ObjectHashMap<>();
        private final Int2ObjectHashMap<Map<TableId, RangeTree<AccordRoutingKey, TokenRange, TxnId>>> storeToTableToRangesToTxns = new Int2ObjectHashMap<>();

        private final int numStores;
        private final List<TableId> tables;
        private final Gen.IntGen tokenGen;
        private final Gen<TokenRange> rangeGen;
        private final Gen<Domain> domainGen;
        private int hlc = 1000;

        public State(RandomSource rs)
        {
            numStores = NUM_STORES_GEN.nextInt(rs);
            tables = IntStream.range(0, NUM_TABLES_GEN.nextInt(rs))
                              .mapToObj(i -> TableId.fromUUID(new UUID(0, i)))
                              .collect(Collectors.toList());
            tokenGen = TOKEN_DISTRIBUTION.next(rs);
            rangeGen = rangeGen(rs, tables);
            domainGen = DOMAIN_DISTRIBUTION.next(rs);
        }

        TxnId nextTxnId(Domain domain)
        {
            return new TxnId(1, hlc++, Txn.Kind.Write, domain, NODE);
        }

        @Override
        public String toString()
        {
            return "State{" +
                   "numStores=" + numStores +
                   ", tables=" + tables +
                   '}';
        }
    }

    private static RangeTree<AccordRoutingKey, TokenRange, TxnId> rangeTree()
    {
        return RTree.create(ACCESSOR);
    }

    private static final RangeTree.Accessor<AccordRoutingKey, TokenRange> ACCESSOR = new RangeTree.Accessor<AccordRoutingKey, TokenRange>()
    {
        @Override
        public AccordRoutingKey start(TokenRange tokenRange)
        {
            return (AccordRoutingKey) tokenRange.start();
        }

        @Override
        public AccordRoutingKey end(TokenRange tokenRange)
        {
            return (AccordRoutingKey) tokenRange.end();
        }

        @Override
        public boolean contains(AccordRoutingKey start, AccordRoutingKey end, AccordRoutingKey accordRoutingKey)
        {
            return new TokenRange(start, end).contains(accordRoutingKey);
        }

        @Override
        public boolean intersects(TokenRange tokenRange, AccordRoutingKey start, AccordRoutingKey end)
        {
            return tokenRange.compareIntersecting(new TokenRange(start, end)) == 0;
        }
    };
}