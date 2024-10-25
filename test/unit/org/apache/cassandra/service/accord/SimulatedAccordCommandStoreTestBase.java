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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.BeforeClass;

import accord.api.RoutingKey;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.messages.BeginRecovery;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.LatestDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public abstract class SimulatedAccordCommandStoreTestBase extends CQLTester
{
    static
    {
        CassandraRelevantProperties.TEST_ACCORD_STORE_THREAD_CHECKS_ENABLED.setBoolean(false);
        // since this test does frequent truncates, the info table gets updated and forced flushed... which is 90% of the cost of this test...
        // this flag disables that flush
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);
        // The plan is to migrate away from SAI, so rather than hacking around timeout issues; just disable for now
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);
    }

    protected enum DepsMessage
    {PreAccept, BeginRecovery, PreAcceptThenBeginRecovery}

    protected static final Gen<Gen<Routable.Domain>> mixedDomainGen = Gens.enums().allMixedDistribution(Routable.Domain.class);
    protected static final Gen<Gen.LongGen> mixedTokenGen = top -> {
        switch (top.nextInt(0, 3))
        {
            case 0: // all
                return rs -> rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
            case 1: // small
                return rs -> rs.nextLong(0, 100);
            case 2: // medium
                return rs -> rs.nextLong(0, Long.MAX_VALUE);
            default:
                throw new AssertionError();
        }
    };

    protected static TableMetadata intTbl, reverseTokenTbl;
    protected static Node.Id nodeId;

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
        createTable("test", "CREATE TABLE test.tbl1 (pk int PRIMARY KEY, value int) WITH transactional_mode='full'");
        intTbl = Schema.instance.getTableMetadata("test", "tbl1");

        createTable("test", "CREATE TABLE test.tbl2 (pk vector<bigint, 2> PRIMARY KEY, value int) WITH transactional_mode='full'");
        reverseTokenTbl = Schema.instance.getTableMetadata("test", "tbl2");

        nodeId = AccordTopology.tcmIdToAccord(ClusterMetadata.current().myNodeId());

        // tests may flush, which triggers compaction... since compaction is not simulated this adds a form of non-deterministic behavior
        for (var store : Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.disableAutoCompaction();

        AccordService.unsafeSetNoop();

        ServerTestUtils.markCMS();
    }

    protected static TokenRange fullRange(TableId id)
    {
        return new TokenRange(AccordRoutingKey.SentinelKey.min(id), AccordRoutingKey.SentinelKey.max(id));
    }

    protected static TokenRange tokenRange(TableId id, long start, long end)
    {
        return new TokenRange(start == Long.MIN_VALUE ? AccordRoutingKey.SentinelKey.min(id) : tokenKey(id, start), tokenKey(id, end));
    }

    protected static AccordRoutingKey.TokenKey tokenKey(TableId id, long token)
    {
        return new AccordRoutingKey.TokenKey(id, new Murmur3Partitioner.LongToken(token));
    }

    protected static Map<RoutingKey, List<TxnId>> keyConflicts(List<TxnId> list, Unseekables<RoutingKey> keys)
    {
        if (list.isEmpty()) return Collections.emptyMap();
        Map<RoutingKey, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(keys.size());
        for (RoutingKey key : keys)
            kc.put(key, list);
        return kc;
    }

    protected static Map<Range, List<TxnId>> rangeConflicts(List<TxnId> list, Ranges ranges)
    {
        if (list.isEmpty()) return Collections.emptyMap();
        Map<Range, List<TxnId>> kc = Maps.newHashMapWithExpectedSize(ranges.size());
        for (Range range : ranges)
            kc.put(range, list);
        return kc;
    }

    protected static void assertDepsMessage(SimulatedAccordCommandStore instance,
                                            DepsMessage messageType,
                                            Txn txn, FullRoute<?> route,
                                            DepsModel model) throws ExecutionException, InterruptedException
    {
        TxnId id = assertDepsMessage(instance, messageType, txn, route,
                                     model.keyConflicts(txn.keys()),
                                     model.rangeConflicts(txn.keys()));
        model.register(id, txn);
    }

    protected static TxnId assertDepsMessage(SimulatedAccordCommandStore instance,
                                             DepsMessage messageType,
                                             Txn txn, FullRoute<?> route,
                                             Map<RoutingKey, List<TxnId>> keyConflicts) throws ExecutionException, InterruptedException
    {
        return assertDepsMessage(instance, messageType, txn, route, keyConflicts, Collections.emptyMap());
    }

    protected static TxnId assertDepsMessage(SimulatedAccordCommandStore instance,
                                             DepsMessage messageType,
                                             Txn txn, FullRoute<?> route,
                                             Map<RoutingKey, List<TxnId>> keyConflicts,
                                             Map<Range, List<TxnId>> rangeConflicts) throws ExecutionException, InterruptedException
    {
        var pair = assertDepsMessageAsync(instance, messageType, txn, route, keyConflicts, rangeConflicts);
        instance.processAll();
        AsyncChains.getBlocking(pair.right);

        return pair.left;
    }

    protected static Pair<TxnId, AsyncResult<?>> assertDepsMessageAsync(SimulatedAccordCommandStore instance,
                                                                        DepsMessage messageType,
                                                                        Txn txn, FullRoute<?> route,
                                                                        Map<RoutingKey, List<TxnId>> keyConflicts,
                                                                        Map<Range, List<TxnId>> rangeConflicts)
    {
        switch (messageType)
        {
            case PreAccept:
                return assertPreAcceptAsync(instance, txn, route, keyConflicts, rangeConflicts);
            case BeginRecovery:
                return assertBeginRecoveryAsync(instance, txn, route, keyConflicts, rangeConflicts);
            case PreAcceptThenBeginRecovery:
                return assertBeginRecoveryAfterPreAcceptAsync(instance, txn, route, keyConflicts, rangeConflicts);
            default:
                throw new IllegalArgumentException("Unknown message type: " + messageType);
        }
    }

    protected static Pair<TxnId, AsyncResult<?>> assertPreAcceptAsync(SimulatedAccordCommandStore instance,
                                                                      Txn txn, FullRoute<?> route,
                                                                      Map<RoutingKey, List<TxnId>> keyConflicts,
                                                                      Map<Range, List<TxnId>> rangeConflicts)
    {
        Map<RoutingKey, List<TxnId>> cloneKeyConflicts = keyConflicts.entrySet().stream()
                                                              .filter(e -> !e.getValue().isEmpty())
                                                              .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList(e.getValue())));
        Map<Range, List<TxnId>> cloneRangeConflicts = rangeConflicts.entrySet().stream()
                                                                    .filter(e -> !e.getValue().isEmpty())
                                                                    .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList(e.getValue())));
        var pair = instance.enqueuePreAccept(txn, route);
        return Pair.create(pair.left, pair.right.map(success -> {
            assertDeps(success.txnId, success.deps, cloneKeyConflicts, cloneRangeConflicts);
            return null;
        }).beginAsResult());
    }

    protected static Pair<TxnId, AsyncResult<?>> assertBeginRecoveryAsync(SimulatedAccordCommandStore instance,
                                                                          Txn txn, FullRoute<?> route,
                                                                          Map<RoutingKey, List<TxnId>> keyConflicts,
                                                                          Map<Range, List<TxnId>> rangeConflicts)
    {
        Map<RoutingKey, List<TxnId>> cloneKeyConflicts = keyConflicts.entrySet().stream()
                                                              .filter(e -> !e.getValue().isEmpty())
                                                              .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList(e.getValue())));
        Map<Range, List<TxnId>> cloneRangeConflicts = rangeConflicts.entrySet().stream()
                                                                    .filter(e -> !e.getValue().isEmpty())
                                                                    .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList(e.getValue())));
        var pair = instance.enqueueBeginRecovery(txn, route);
        return Pair.create(pair.left, pair.right.map(success -> {
            Deps proposeDeps = LatestDeps.mergeProposal(Collections.singletonList(success), ok -> ok.deps);
            assertDeps(success.txnId, proposeDeps, cloneKeyConflicts, cloneRangeConflicts);
            return null;
        }).beginAsResult());
    }

    protected static Pair<TxnId, AsyncResult<?>> assertBeginRecoveryAfterPreAcceptAsync(SimulatedAccordCommandStore instance,
                                                                                        Txn txn, FullRoute<?> route,
                                                                                        Map<RoutingKey, List<TxnId>> keyConflicts,
                                                                                        Map<Range, List<TxnId>> rangeConflicts)
    {
        Map<RoutingKey, List<TxnId>> cloneKeyConflicts = keyConflicts.entrySet().stream()
                                                              .filter(e -> !e.getValue().isEmpty())
                                                              .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList(e.getValue())));
        Map<Range, List<TxnId>> cloneRangeConflicts = rangeConflicts.entrySet().stream()
                                                                    .filter(e -> !e.getValue().isEmpty())
                                                                    .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList(e.getValue())));

        TxnId txnId = instance.nextTxnId(txn.kind(), txn.keys().domain());
        PreAccept preAccept = new PreAccept(nodeId, new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, instance.topology), txnId, txn, route);

        var preAcceptAsync = instance.processAsync(preAccept, safe -> {
            var reply = preAccept.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            PreAccept.PreAcceptOk success = (PreAccept.PreAcceptOk) reply;
            assertDeps(success.txnId, success.deps, cloneKeyConflicts, cloneRangeConflicts);
            return success;
        });
        var delay = preAcceptAsync.flatMap(ignore -> AsyncChains.ofCallable(instance.unorderedScheduled, () -> {
            Ballot ballot = Ballot.fromValues(instance.storeService.epoch(), instance.storeService.now(), nodeId);
            return new BeginRecovery(nodeId, new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, instance.topology), txnId, null, txn, route, ballot);
        }));
        var recoverAsync = delay.flatMap(br -> instance.processAsync(br, safe -> {
            var reply = br.apply(safe);
            Assertions.assertThat(reply.isOk()).isTrue();
            BeginRecovery.RecoverOk success = (BeginRecovery.RecoverOk) reply;
            Deps proposeDeps = LatestDeps.mergeProposal(Collections.singletonList(success), ok -> ok.deps);
            assertDeps(success.txnId, proposeDeps, cloneKeyConflicts, cloneRangeConflicts);
            return success;
        }));

        return Pair.create(txnId, recoverAsync.beginAsResult());
    }

    protected static void assertDeps(TxnId txnId, Deps deps,
                                     Map<RoutingKey, List<TxnId>> keyConflicts,
                                     Map<Range, List<TxnId>> rangeConflicts)
    {
        if (rangeConflicts.isEmpty())
        {
            Assertions.assertThat(deps.rangeDeps.isEmpty()).describedAs("Txn %s rangeDeps was not empty; %s", txnId, deps.rangeDeps).isTrue();
        }
        else
        {
            List<Range> actualRanges = IntStream.range(0, deps.rangeDeps.rangeCount()).mapToObj(deps.rangeDeps::range).collect(Collectors.toList());
            Assertions.assertThat(Ranges.of(actualRanges.toArray(Range[]::new)))
                      .describedAs("Txn %s had different ranges than expected", txnId)
                      .isEqualTo(Ranges.of(rangeConflicts.keySet().toArray(Range[]::new)));
            AssertionError errors = null;
            for (int i = 0; i < rangeConflicts.size(); i++)
            {
                try
                {
                    var range = deps.rangeDeps.range(i);
                    Assertions.assertThat(rangeConflicts).describedAs("Txn %s had an unexpected range", txnId).containsKey(range);
                    var conflict = deps.rangeDeps.txnIdsForRangeIndex(i);
                    List<TxnId> expectedConflict = rangeConflicts.get(range);
                    Assertions.assertThat(conflict).describedAs("Txn %s Expected range %s to have different conflicting txns", txnId, range).isEqualTo(expectedConflict);
                }
                catch (AssertionError e)
                {
                    if (errors == null)
                        errors = e;
                    else
                        errors.addSuppressed(e);
                }
            }
            if (errors != null)
                throw errors;
        }
        if (keyConflicts.isEmpty())
        {
            Assertions.assertThat(deps.keyDeps.isEmpty()).describedAs("Txn %s keyDeps was not empty", txnId).isTrue();
        }
        else
        {
            Assertions.assertThat(deps.keyDeps.keys()).describedAs("Txn %s Keys", txnId).isEqualTo(RoutingKeys.of(keyConflicts.keySet()));
            for (var key : keyConflicts.keySet())
                Assertions.assertThat(deps.keyDeps.txnIds(key)).describedAs("Txn %s for key %s", txnId, key).isEqualTo(keyConflicts.get(key));
        }
    }

    protected static Gen<Pair<Txn, FullRoute<?>>> randomTxn(Gen<Routable.Domain> domainGen, Gen.LongGen tokenGen)
    {
        TableMetadata tbl = reverseTokenTbl;
        Invariants.checkArgument(tbl.partitioner == Murmur3Partitioner.instance, "Only murmur partitioner is supported; given %s", tbl.partitioner.getClass());
        Gen<PartitionKey> keyGen = rs -> new PartitionKey(tbl.id, tbl.partitioner.decorateKey(Murmur3Partitioner.LongToken.keyForToken(tokenGen.nextLong(rs))));
        Gen<Range> rangeGen = rs -> {
            long a = tokenGen.nextLong(rs);
            long b = tokenGen.nextLong(rs);
            while (a == b)
                b = tokenGen.nextLong(rs);
            if (a > b)
            {
                long tmp = a;
                a = b;
                b = tmp;
            }
            return tokenRange(tbl.id, a, b);
        };
        return rs -> {
            Routable.Domain domain = domainGen.next(rs);
            switch (domain)
            {
                case Key:
                {
                    Keys keys = Keys.of(Gens.lists(keyGen).unique().ofSizeBetween(1, 5).next(rs));
                    List<String> inserts = new ArrayList<>(keys.size());
                    List<Object> binds = new ArrayList<>(keys.size());
                    for (int i = 0; i < keys.size(); i++)
                    {
                        inserts.add(String.format("INSERT INTO %s (pk) VALUES (?)", tbl));
                        binds.add(((PartitionKey) keys.get(i)).partitionKey().getKey());
                    }
                    Txn txn = createTxn(wrapInTxn(inserts), binds);
                    FullRoute<?> route = keys.toRoute(keys.get(0).toUnseekable());
                    return Pair.create(txn, route);
                }
                case Range:
                {
                    Ranges ranges = Ranges.of(Gens.arrays(Range.class, rangeGen).unique().ofSizeBetween(1, 5).next(rs));
                    Txn txn = createTxn(Txn.Kind.ExclusiveSyncPoint, ranges);
                    FullRangeRoute route = ranges.toRoute(ranges.get(0).end());
                    return Pair.create(txn, route);
                }
                default:
                    throw new UnsupportedOperationException(domain.name());
            }
        };
    }

    public static class DepsModel
    {
        private final Map<RoutingKey, List<TxnId>> keyConflicts = new HashMap<>();
        private final RangeTree<RoutingKey, Range, TxnId> rangeConflicts = RTree.create(RangeTreeRangeAccessor.instance);
        private final Ranges storeRanges;

        public DepsModel(Ranges storeRanges)
        {
            this.storeRanges = storeRanges;
        }

        public Map<RoutingKey, List<TxnId>> keyConflicts(Seekables<?, ?> keysOrRanges)
        {
            keysOrRanges = keysOrRanges.slice(storeRanges, Routables.Slice.Minimal);
            switch (keysOrRanges.domain())
            {
                case Key:
                {
                    Keys keys = (Keys) keysOrRanges;
                    Map<RoutingKey, List<TxnId>> expectedConflicts = new HashMap<>();
                    keys.forEach(k -> expectedConflicts.put(k.toUnseekable(), keyConflicts.getOrDefault(k.toUnseekable(), Collections.emptyList())));
                    return expectedConflicts;
                }
                case Range:
                {
                    Ranges ranges = (Ranges) keysOrRanges;
                    return keyConflicts.entrySet().stream()
                                       .filter(e -> ranges.contains(e.getKey()))
                                       .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }

                default:
                    throw new UnsupportedOperationException();
            }
        }

        public Map<Range, List<TxnId>> rangeConflicts(Seekables<?, ?> keysOrRanges)
        {
            // there is a patch pending to add range support for keys... that isn't here yet so not handled
            if (keysOrRanges.domain() != Routable.Domain.Range)
                return Collections.emptyMap();
            keysOrRanges = keysOrRanges.slice(storeRanges, Routables.Slice.Minimal);

            Ranges ranges = (Ranges) keysOrRanges;
            Map<Range, List<TxnId>> conflicts = new HashMap<>();
            ranges.forEach(r -> rangeConflicts.search(r, e -> {
                for (Range range : Ranges.single(e.getKey()).slice(ranges, Routables.Slice.Minimal))
                    conflicts.computeIfAbsent(range, ignore -> new ArrayList<>()).add(e.getValue());
            }));
            // need to dedup/sort txns
            conflicts.values().forEach(l -> {
                var sortedDedup = new ArrayList<>(new TreeSet<>(l));
                l.clear();
                l.addAll(sortedDedup);
            });
            return conflicts;
        }

        public void register(TxnId txnId, Txn txn)
        {
            for (var s : txn.keys())
            {
                switch (s.domain())
                {
                    case Key:
                        keyConflicts.computeIfAbsent(s.asKey().toUnseekable(), i -> new ArrayList<>()).add(txnId);
                        break;
                    case Range:
                        rangeConflicts.add(s.asRange(), txnId);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
    }
}
