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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;

import static accord.utils.Property.qt;
import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class SimulatedMultiKeyAndRangeTest extends SimulatedAccordCommandStoreTestBase
{
    @Test
    public void test()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 300;
        long minToken = 0;
        long maxToken = 100;
        Gen<Gen.LongGen> tokenDistribution = Gens.mixedDistribution(minToken, maxToken + 1);
        Gen<Gen.IntGen> keyDistribution = Gens.mixedDistribution(1, 5);
        Gen<Gen.IntGen> rangeDistribution = Gens.mixedDistribution(1, 5);
        Gen<Gen<Domain>> domainDistribution = Gens.mixedDistribution(Domain.values());
        Gen<Gen<DepsMessage>> msgDistribution = Gens.mixedDistribution(DepsMessage.values());

        qt().withExamples(100).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                Gen.LongGen tokenGen = tokenDistribution.next(rs);
                Gen<Domain> domainGen = domainDistribution.next(rs);
                Gen<DepsMessage> msgGen = msgDistribution.next(rs);
                Map<Key, List<TxnId>> keyConflicts = new HashMap<>();
                RangeTree<RoutingKey, Range, TxnId> rangeConflicts = RTree.create(RangeTreeRangeAccessor.instance);
                List<AsyncResult<?>> asyncs = new ArrayList<>(numSamples);

                Gen.IntGen keyCountGen = keyDistribution.next(rs);
                Gen.IntGen rangeCountGen = rangeDistribution.next(rs);

                for (int i = 0; i < numSamples; i++)
                {
                    switch (domainGen.next(rs))
                    {
                        case Key:
                        {
                            int numKeys = keyCountGen.nextInt(rs);
                            TreeSet<Key> set = new TreeSet<>();
                            while (set.size() != numKeys)
                                set.add(new PartitionKey(tbl.id, tbl.partitioner.decorateKey(keyForToken(tokenGen.nextLong(rs)))));
                            Keys keys = Keys.of(set);
                            List<String> inserts = IntStream.range(0, numKeys).mapToObj(ignore -> "INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)").collect(Collectors.toList());
                            List<Object> binds = new ArrayList<>(numKeys * 2);
                            keys.forEach(k -> {
                                binds.add(((PartitionKey) k.asKey()).partitionKey().getKey());
                                binds.add(42);
                            });
                            Txn txn = createTxn(wrapInTxn(inserts), binds);
                            FullRoute<?> route = keys.toRoute(keys.get(0).toUnseekable());

                            Map<Key, List<TxnId>> expectedConflicts = new HashMap<>();
                            keys.forEach(k -> expectedConflicts.put(k, keyConflicts.computeIfAbsent(k, ignore -> new ArrayList<>())));

                            var p = assertDepsMessageAsync(instance, msgGen.next(rs), txn, route, expectedConflicts, Collections.emptyMap());
                            keys.forEach(k -> keyConflicts.get(k).add(p.left));
                            asyncs.add(p.right);
                        }
                        break;
                        case Range:
                        {
                            int numRanges = rangeCountGen.nextInt(rs);
                            Set<Range> set = new HashSet<>();
                            while (set.size() != numRanges)
                            {
                                long token = tokenGen.nextLong(rs);
                                int offset = rs.nextInt(1, 10);
                                long start, end;
                                if (token + offset > maxToken)
                                {
                                    end = token;
                                    start = end - offset;
                                }
                                else
                                {
                                    start = token;
                                    end = start + offset;
                                }
                                set.add(tokenRange(tbl.id, start, end));
                            }
                            // The property ranges.size() == numRanges is not true as this logic will sort + deoverlap
                            // so if the ranges were overlapped, we could have more or less than numRanges
                            Ranges ranges = Ranges.of(set.toArray(Range[]::new));
                            FullRangeRoute route = ranges.toRoute(ranges.get(0).end());
                            Txn txn = createTxn(Txn.Kind.ExclusiveSyncPoint, ranges);

                            Map<Key, List<TxnId>> expectedKeyConflicts = keyConflicts.entrySet().stream()
                                                                                     .filter(e -> ranges.contains(e.getKey()))
                                                                                     .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                            Map<Range, List<TxnId>> expectedRangeConflicts = new HashMap<>();
                            ranges.forEach(r ->
                                           rangeConflicts.search(r, e ->
                                                                    expectedRangeConflicts.computeIfAbsent(e.getKey(), ignore -> new ArrayList<>()).add(e.getValue())));
                            // need to dedup/sort txns
                            expectedRangeConflicts.values().forEach(l -> {
                                var sortedDedup = new ArrayList<>(new TreeSet<>(l));
                                l.clear();
                                l.addAll(sortedDedup);
                            });
                            var p = assertDepsMessageAsync(instance, msgGen.next(rs), txn, route, expectedKeyConflicts, expectedRangeConflicts);
                            asyncs.add(p.right);
                            ranges.forEach(r -> rangeConflicts.add(r, p.left));
                        }
                        break;
                        default:
                            throw new AssertionError();
                    }
                }
                instance.processAll();
                safeBlock(asyncs);
            }
        });
    }
}
