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
package org.apache.cassandra.dht.tokenallocator;

import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.junit.Assert;
import org.junit.Ignore;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Base class for {@link Murmur3ReplicationAwareTokenAllocatorTest} and {@link RandomReplicationAwareTokenAllocatorTest},
 * we need to separate classes to avoid timeous in case flaky tests need to be repeated, see CASSANDRA-12784.
 */
@Ignore
abstract class AbstractReplicationAwareTokenAllocatorTest extends TokenAllocatorTestBase
{
    static class SimpleReplicationStrategy implements TestReplicationStrategy
    {
        int replicas;

        public SimpleReplicationStrategy(int replicas)
        {
            super();
            this.replicas = replicas;
        }

        public List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            List<Unit> endpoints = new ArrayList<Unit>(replicas);

            token = sortedTokens.ceilingKey(token);
            if (token == null)
                token = sortedTokens.firstKey();
            Iterator<Unit> iter = Iterables.concat(sortedTokens.tailMap(token, true).values(), sortedTokens.values()).iterator();
            while (endpoints.size() < replicas)
            {
                if (!iter.hasNext())
                    return endpoints;
                Unit ep = iter.next();
                if (!endpoints.contains(ep))
                    endpoints.add(ep);
            }
            return endpoints;
        }

        public Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens)
        {
            Set<Unit> seenUnits = Sets.newHashSet();
            int unitsFound = 0;

            for (Map.Entry<Token, Unit> en : Iterables.concat(
                                                             sortedTokens.headMap(token, false).descendingMap().entrySet(),
                                                             sortedTokens.descendingMap().entrySet()))
            {
                Unit n = en.getValue();
                // Same group as investigated unit is a break; anything that could replicate in it replicates there.
                if (n == unit)
                    break;

                if (seenUnits.add(n))
                {
                    if (++unitsFound == replicas)
                        break;
                }
                token = en.getKey();
            }
            return token;
        }

        public void addUnit(Unit n)
        {
        }

        public void removeUnit(Unit n)
        {
        }

        public String toString()
        {
            return String.format("Simple %d replicas", replicas);
        }

        public int replicas()
        {
            return replicas;
        }

        public boolean sameGroup(Unit n1, Unit n2)
        {
            return false;
        }

        public Unit getGroup(Unit unit)
        {
            // The unit is the group.
            return unit;
        }

        public double spreadExpectation()
        {
            return 1;
        }
    }

    static abstract class GroupReplicationStrategy implements TestReplicationStrategy
    {
        final int replicas;
        final Map<Unit, Integer> groupMap;

        public GroupReplicationStrategy(int replicas)
        {
            this.replicas = replicas;
            this.groupMap = Maps.newHashMap();
        }

        public List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            List<Unit> endpoints = new ArrayList<Unit>(replicas);
            BitSet usedGroups = new BitSet();

            if (sortedTokens.isEmpty())
                return endpoints;

            token = sortedTokens.ceilingKey(token);
            if (token == null)
                token = sortedTokens.firstKey();
            Iterator<Unit> iter = Iterables.concat(sortedTokens.tailMap(token, true).values(), sortedTokens.values()).iterator();
            while (endpoints.size() < replicas)
            {
                // For simlicity assuming list can't be exhausted before finding all replicas.
                Unit ep = iter.next();
                int group = groupMap.get(ep);
                if (!usedGroups.get(group))
                {
                    endpoints.add(ep);
                    usedGroups.set(group);
                }
            }
            return endpoints;
        }

        public Token lastReplicaToken(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            BitSet usedGroups = new BitSet();
            int groupsFound = 0;

            token = sortedTokens.ceilingKey(token);
            if (token == null)
                token = sortedTokens.firstKey();
            for (Map.Entry<Token, Unit> en :
            Iterables.concat(sortedTokens.tailMap(token, true).entrySet(),
                             sortedTokens.entrySet()))
            {
                Unit ep = en.getValue();
                int group = groupMap.get(ep);
                if (!usedGroups.get(group))
                {
                    usedGroups.set(group);
                    if (++groupsFound >= replicas)
                        return en.getKey();
                }
            }
            return token;
        }

        public Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens)
        {
            // replicated ownership
            int unitGroup = groupMap.get(unit);   // unit must be already added
            BitSet seenGroups = new BitSet();
            int groupsFound = 0;

            for (Map.Entry<Token, Unit> en : Iterables.concat(
                                                             sortedTokens.headMap(token, false).descendingMap().entrySet(),
                                                             sortedTokens.descendingMap().entrySet()))
            {
                Unit n = en.getValue();
                int ngroup = groupMap.get(n);
                // Same group as investigated unit is a break; anything that could replicate in it replicates there.
                if (ngroup == unitGroup)
                    break;

                if (!seenGroups.get(ngroup))
                {
                    if (++groupsFound == replicas)
                        break;
                    seenGroups.set(ngroup);
                }
                token = en.getKey();
            }
            return token;
        }

        public String toString()
        {
            Map<Integer, Integer> idToSize = instanceToCount(groupMap);
            Map<Integer, Integer> sizeToCount = Maps.newTreeMap();
            sizeToCount.putAll(instanceToCount(idToSize));
            return String.format("%s strategy, %d replicas, group size to count %s", getClass().getSimpleName(), replicas, sizeToCount);
        }

        @Override
        public int replicas()
        {
            return replicas;
        }

        public boolean sameGroup(Unit n1, Unit n2)
        {
            return groupMap.get(n1).equals(groupMap.get(n2));
        }

        public void removeUnit(Unit n)
        {
            groupMap.remove(n);
        }

        public Integer getGroup(Unit unit)
        {
            return groupMap.get(unit);
        }

        public double spreadExpectation()
        {
            return 1.5;   // Even balanced racks get disbalanced when they lose nodes.
        }
    }

    private static <T> Map<T, Integer> instanceToCount(Map<?, T> map)
    {
        Map<T, Integer> idToCount = Maps.newHashMap();
        for (Map.Entry<?, T> en : map.entrySet())
        {
            Integer old = idToCount.get(en.getValue());
            idToCount.put(en.getValue(), old != null ? old + 1 : 1);
        }
        return idToCount;
    }

    /**
     * Group strategy spreading units into a fixed number of groups.
     */
    static class FixedGroupCountReplicationStrategy extends GroupReplicationStrategy
    {
        int groupId;
        int groupCount;

        public FixedGroupCountReplicationStrategy(int replicas, int groupCount)
        {
            super(replicas);
            assert groupCount >= replicas;
            groupId = 0;
            this.groupCount = groupCount;
        }

        public void addUnit(Unit n)
        {
            groupMap.put(n, groupId++ % groupCount);
        }
    }

    /**
     * Group strategy with a fixed number of units per group.
     */
    static class BalancedGroupReplicationStrategy extends GroupReplicationStrategy
    {
        int groupId;
        int groupSize;

        public BalancedGroupReplicationStrategy(int replicas, int groupSize)
        {
            super(replicas);
            groupId = 0;
            this.groupSize = groupSize;
        }

        public void addUnit(Unit n)
        {
            groupMap.put(n, groupId++ / groupSize);
        }
    }

    static class UnbalancedGroupReplicationStrategy extends GroupReplicationStrategy
    {
        int groupId;
        int nextSize;
        int num;
        int minGroupSize;
        int maxGroupSize;
        Random rand;

        public UnbalancedGroupReplicationStrategy(int replicas, int minGroupSize, int maxGroupSize, Random rand)
        {
            super(replicas);
            groupId = -1;
            nextSize = 0;
            num = 0;
            this.maxGroupSize = maxGroupSize;
            this.minGroupSize = minGroupSize;
            this.rand = rand;
        }

        public void addUnit(Unit n)
        {
            if (++num > nextSize)
            {
                nextSize = minGroupSize + rand.nextInt(maxGroupSize - minGroupSize + 1);
                ++groupId;
                num = 0;
            }
            groupMap.put(n, groupId);
        }

        public double spreadExpectation()
        {
            return 2;
        }
    }

    static Map<Unit, Double> evaluateReplicatedOwnership(ReplicationAwareTokenAllocator<Unit> t)
    {
        Map<Unit, Double> ownership = Maps.newHashMap();
        Iterator<Token> it = t.sortedTokens.keySet().iterator();
        if (!it.hasNext())
            return ownership;

        Token current = it.next();
        while (it.hasNext())
        {
            Token next = it.next();
            addOwnership(t, current, next, ownership);
            current = next;
        }
        addOwnership(t, current, t.sortedTokens.firstKey(), ownership);

        return ownership;
    }

    private static void addOwnership(ReplicationAwareTokenAllocator<Unit> t, Token current, Token next, Map<Unit, Double> ownership)
    {
        TestReplicationStrategy ts = (TestReplicationStrategy) t.strategy;
        double size = current.size(next);
        Token representative = t.partitioner.midpoint(current, next);
        for (Unit n : ts.getReplicas(representative, t.sortedTokens))
        {
            Double v = ownership.get(n);
            ownership.put(n, v != null ? v + size : size);
        }
    }

    private static double replicatedTokenOwnership(Token token, NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy)
    {
        TestReplicationStrategy ts = (TestReplicationStrategy) strategy;
        Token next = sortedTokens.higherKey(token);
        if (next == null)
            next = sortedTokens.firstKey();
        return ts.replicationStart(token, sortedTokens.get(token), sortedTokens).size(next);
    }

    protected void testExistingCluster(IPartitioner partitioner, int maxVNodeCount)
    {
        for (int rf = 1; rf <= 5; ++rf)
        {
            for (int perUnitCount = 1; perUnitCount <= maxVNodeCount; perUnitCount *= 4)
            {
                testExistingCluster(perUnitCount, fixedTokenCount, new SimpleReplicationStrategy(rf), partitioner);
                testExistingCluster(perUnitCount, varyingTokenCount, new SimpleReplicationStrategy(rf), partitioner);
                if (rf == 1) continue;  // Replication strategy doesn't matter for RF = 1.
                for (int groupSize = 4; groupSize <= 64 && groupSize * rf * 4 < TARGET_CLUSTER_SIZE; groupSize *= 4)
                {
                    testExistingCluster(perUnitCount, fixedTokenCount,
                                        new BalancedGroupReplicationStrategy(rf, groupSize), partitioner);
                    testExistingCluster(perUnitCount, varyingTokenCount,
                                        new UnbalancedGroupReplicationStrategy(rf, groupSize / 2, groupSize * 2, seededRand),
                                        partitioner);
                }
                testExistingCluster(perUnitCount, fixedTokenCount,
                                    new FixedGroupCountReplicationStrategy(rf, rf * 2), partitioner);
            }
        }
    }

    private void testExistingCluster(int perUnitCount, TokenCount tc, TestReplicationStrategy rs, IPartitioner partitioner)
    {
        System.out.println("Testing existing cluster, target " + perUnitCount + " vnodes, replication " + rs);
        final int targetClusterSize = TARGET_CLUSTER_SIZE;
        NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();

        random(tokenMap, rs, targetClusterSize / 2, tc, perUnitCount, partitioner);

        ReplicationAwareTokenAllocator<Unit> t = new ReplicationAwareTokenAllocator<>(tokenMap, rs, partitioner);
        grow(t, targetClusterSize * 9 / 10, tc, perUnitCount, false);
        grow(t, targetClusterSize, tc, perUnitCount, true);
        loseAndReplace(t, targetClusterSize / 10, tc, perUnitCount, partitioner);
        System.out.println();
    }

    protected void testNewCluster(IPartitioner partitioner, int maxVNodeCount)
    {
        // This test is flaky because the selection of the tokens for the first RF nodes (which is random, with an
        // uncontrolled seed) can sometimes cause a pathological situation where the algorithm will find a (close to)
        // ideal distribution of tokens for some number of nodes, which in turn will inevitably cause it to go into a
        // bad (unacceptable to the test criteria) distribution after adding one more node.

        // This should happen very rarely, unless something is broken in the token allocation code.

        for (int rf = 2; rf <= 5; ++rf)
        {
            for (int perUnitCount = 1; perUnitCount <= maxVNodeCount; perUnitCount *= 4)
            {
                testNewCluster(perUnitCount, fixedTokenCount, new SimpleReplicationStrategy(rf), partitioner);
                testNewCluster(perUnitCount, varyingTokenCount, new SimpleReplicationStrategy(rf), partitioner);
                if (rf == 1) continue;  // Replication strategy doesn't matter for RF = 1.
                for (int groupSize = 4; groupSize <= 64 && groupSize * rf * 8 < TARGET_CLUSTER_SIZE; groupSize *= 4)
                {
                    testNewCluster(perUnitCount, fixedTokenCount,
                                   new BalancedGroupReplicationStrategy(rf, groupSize), partitioner);
                    testNewCluster(perUnitCount, varyingTokenCount,
                                   new UnbalancedGroupReplicationStrategy(rf, groupSize / 2, groupSize * 2, seededRand),
                                   partitioner);
                }
                testNewCluster(perUnitCount, fixedTokenCount,
                               new FixedGroupCountReplicationStrategy(rf, rf * 2), partitioner);
            }
        }
    }

    private void testNewCluster(int perUnitCount, TokenCount tc, TestReplicationStrategy rs, IPartitioner partitioner)
    {
        System.out.println("Testing new cluster, target " + perUnitCount + " vnodes, replication " + rs);
        final int targetClusterSize = TARGET_CLUSTER_SIZE;
        NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();

        ReplicationAwareTokenAllocator<Unit> t = new ReplicationAwareTokenAllocator<>(tokenMap, rs, partitioner);
        grow(t, targetClusterSize * 2 / 5, tc, perUnitCount, false);
        grow(t, targetClusterSize, tc, perUnitCount, true);
        loseAndReplace(t, targetClusterSize / 5, tc, perUnitCount, partitioner);
        System.out.println();
    }

    private void loseAndReplace(ReplicationAwareTokenAllocator<Unit> t, int howMany,
                                TokenCount tc, int perUnitCount, IPartitioner partitioner)
    {
        int fullCount = t.unitCount();
        System.out.format("Losing %d units. ", howMany);
        for (int i = 0; i < howMany; ++i)
        {
            Unit u = t.unitFor(partitioner.getRandomToken(seededRand));
            t.removeUnit(u);
            ((TestReplicationStrategy) t.strategy).removeUnit(u);
        }
        // Grow half without verifying.
        grow(t, (t.unitCount() + fullCount * 3) / 4, tc, perUnitCount, false);
        // Metrics should be back to normal by now. Check that they remain so.
        grow(t, fullCount, tc, perUnitCount, true);
    }

    public void grow(ReplicationAwareTokenAllocator<Unit> t, int targetClusterSize, TokenCount tc, int perUnitCount, boolean verifyMetrics)
    {
        int size = t.unitCount();
        Summary su = new Summary();
        Summary st = new Summary();
        Random rand = new Random(targetClusterSize + perUnitCount);
        TestReplicationStrategy strategy = (TestReplicationStrategy) t.strategy;
        if (size < targetClusterSize)
        {
            System.out.format("Adding %d unit(s) using %s...", targetClusterSize - size, t.toString());
            long time = currentTimeMillis();
            while (size < targetClusterSize)
            {
                int tokens = tc.tokenCount(perUnitCount, rand);
                Unit unit = new Unit();
                strategy.addUnit(unit);
                t.addUnit(unit, tokens);
                ++size;
                if (verifyMetrics)
                    updateSummary(t, su, st, false);
            }
            System.out.format(" Done in %.3fs\n", (currentTimeMillis() - time) / 1000.0);
            if (verifyMetrics)
            {
                updateSummary(t, su, st, true);
                double maxExpected = 1.0 + tc.spreadExpectation() * strategy.spreadExpectation() / (perUnitCount * t.replicas);
                if (su.max > maxExpected)
                {
                    Assert.fail(String.format("Expected max unit size below %.4f, was %.4f", maxExpected, su.max));
                }
                // We can't verify lower side range as small loads can't always be fixed.
            }
        }
    }

    private void updateSummary(ReplicationAwareTokenAllocator<Unit> t, Summary su, Summary st, boolean print)
    {
        int size = t.sortedTokens.size();
        double inverseAverage = 1.0 * size / t.strategy.replicas();

        Map<Unit, Double> ownership = evaluateReplicatedOwnership(t);
        SummaryStatistics unitStat = new SummaryStatistics();
        for (Map.Entry<Unit, Double> en : ownership.entrySet())
            unitStat.addValue(en.getValue() * inverseAverage / t.unitToTokens.get(en.getKey()).size());
        su.update(unitStat, t.unitCount());

        SummaryStatistics tokenStat = new SummaryStatistics();
        for (Token tok : t.sortedTokens.keySet())
            tokenStat.addValue(replicatedTokenOwnership(tok, t.sortedTokens, t.strategy) * inverseAverage);
        st.update(tokenStat, t.unitCount());

        if (print)
        {
            System.out.format("Size %d(%d)   \tunit %s  token %s   %s\n",
                              t.unitCount(), size,
                              mms(unitStat),
                              mms(tokenStat),
                              t.strategy);
            System.out.format("Worst intermediate unit\t%s  token %s\n", su, st);
        }
    }
}
