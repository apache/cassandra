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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Random;

import com.google.common.collect.Maps;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;

public class NoReplicationTokenAllocatorTest extends TokenAllocatorTestBase
{

    @Test
    public void testNewClusterWithMurmur3Partitioner()
    {
        testNewCluster(new Murmur3Partitioner());
    }

    @Test
    public void testNewClusterWithRandomPartitioner()
    {
        testNewCluster(new RandomPartitioner());
    }

    private void testNewCluster(IPartitioner partitioner)
    {
        for (int perUnitCount = 1; perUnitCount <= MAX_VNODE_COUNT; perUnitCount *= 4)
        {
            testNewCluster(perUnitCount, fixedTokenCount, new NoReplicationStrategy(), partitioner);
            testNewCluster(perUnitCount, fixedTokenCount, new ZeroReplicationStrategy(), partitioner);
        }
    }

    public void testNewCluster(int perUnitCount, TokenCount tc, NoReplicationStrategy rs, IPartitioner partitioner)
    {
        System.out.println("Testing new cluster, target " + perUnitCount + " vnodes, replication " + rs);
        final int targetClusterSize = TARGET_CLUSTER_SIZE;
        NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();

        NoReplicationTokenAllocator<Unit> t = new NoReplicationTokenAllocator<Unit>(tokenMap, rs, partitioner);
        grow(t, targetClusterSize * 2 / 5, tc, perUnitCount, false);
        grow(t, targetClusterSize, tc, perUnitCount, true);
        System.out.println();
    }

    @Test
    public void testExistingClusterWithMurmur3Partitioner()
    {
        testExistingCluster(new Murmur3Partitioner());
    }

    @Test
    public void testExistingClusterWithRandomPartitioner()
    {
        testExistingCluster(new RandomPartitioner());
    }

    private void testExistingCluster(IPartitioner partitioner)
    {
        for (int perUnitCount = 1; perUnitCount <= MAX_VNODE_COUNT; perUnitCount *= 4)
        {
            testExistingCluster(perUnitCount, fixedTokenCount, new NoReplicationStrategy(), partitioner);
            testExistingCluster(perUnitCount, fixedTokenCount, new ZeroReplicationStrategy(), partitioner);
        }
    }

    public NoReplicationTokenAllocator<Unit> randomWithTokenAllocator(NavigableMap<Token, Unit> map, NoReplicationStrategy rs,
                                                                      int unitCount, TokenCount tc, int perUnitCount,
                                                                      IPartitioner partitioner)
    {
        super.random(map, rs, unitCount, tc, perUnitCount, partitioner);
        NoReplicationTokenAllocator<Unit> t = new NoReplicationTokenAllocator<Unit>(map, rs, partitioner);
        t.createTokenInfos();
        return t;
    }

    public void testExistingCluster(int perUnitCount, TokenCount tc, NoReplicationStrategy rs, IPartitioner partitioner)
    {
        System.out.println("Testing existing cluster, target " + perUnitCount + " vnodes, replication " + rs);
        final int targetClusterSize = TARGET_CLUSTER_SIZE;
        NavigableMap<Token, Unit> tokenMap = Maps.newTreeMap();
        NoReplicationTokenAllocator<Unit> t = randomWithTokenAllocator(tokenMap, rs, targetClusterSize / 2, tc, perUnitCount, partitioner);
        updateSummaryBeforeGrow(t);

        grow(t, targetClusterSize * 9 / 10, tc, perUnitCount, false);
        grow(t, targetClusterSize, tc, perUnitCount, true);
        loseAndReplace(t, targetClusterSize / 10, tc, perUnitCount, partitioner);
        System.out.println();
    }

    private void loseAndReplace(NoReplicationTokenAllocator<Unit> t, int howMany,
                                TokenCount tc, int perUnitCount, IPartitioner partitioner)
    {
        int fullCount = t.sortedUnits.size();
        System.out.format("Losing %d units. ", howMany);
        for (int i = 0; i < howMany; ++i)
        {
            Unit u = t.unitFor(partitioner.getRandomToken(seededRand));
            t.removeUnit(u);
        }
        // Grow half without verifying.
        grow(t, (t.sortedUnits.size() + fullCount * 3) / 4, tc, perUnitCount, false);
        // Metrics should be back to normal by now. Check that they remain so.
        grow(t, fullCount, tc, perUnitCount, true);
    }

    private void updateSummaryBeforeGrow(NoReplicationTokenAllocator<Unit> t)
    {
        Summary su = new Summary();
        Summary st = new Summary();
        System.out.println("Before growing cluster: ");
        updateSummary(t, su, st, true);
    }

    private void grow(NoReplicationTokenAllocator<Unit> t, int targetClusterSize, TokenCount tc, int perUnitCount, boolean verifyMetrics)
    {
        int size = t.sortedUnits.size();
        Summary su = new Summary();
        Summary st = new Summary();
        Random rand = new Random(targetClusterSize + perUnitCount);
        TestReplicationStrategy strategy = (TestReplicationStrategy) t.strategy;
        if (size < targetClusterSize)
        {
            System.out.format("Adding %d unit(s) using %s...", targetClusterSize - size, t.toString());
            long time = System.currentTimeMillis();

            while (size < targetClusterSize)
            {
                int num_tokens = tc.tokenCount(perUnitCount, rand);
                Unit unit = new Unit();
                t.addUnit(unit, num_tokens);
                ++size;
                if (verifyMetrics)
                    updateSummary(t, su, st, false);
            }
            System.out.format(" Done in %.3fs\n", (System.currentTimeMillis() - time) / 1000.0);

            if (verifyMetrics)
            {
                updateSummary(t, su, st, true);
                double maxExpected = 1.0 + tc.spreadExpectation() * strategy.spreadExpectation() / perUnitCount;
                if (su.max > maxExpected)
                {
                    Assert.fail(String.format("Expected max unit size below %.4f, was %.4f", maxExpected, su.max));
                }
            }
        }
    }

    private void updateSummary(NoReplicationTokenAllocator<Unit> t, Summary su, Summary st, boolean print)
    {
        int size = t.sortedTokens.size();

        SummaryStatistics unitStat = new SummaryStatistics();
        for (TokenAllocatorBase.Weighted<TokenAllocatorBase.UnitInfo> wu : t.sortedUnits)
        {
            unitStat.addValue(wu.weight * size / t.tokensInUnits.get(wu.value.unit).size());
        }
        su.update(unitStat);

        SummaryStatistics tokenStat = new SummaryStatistics();
        for (PriorityQueue<TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo>> tokens : t.tokensInUnits.values())
        {
            for (TokenAllocatorBase.Weighted<TokenAllocatorBase.TokenInfo> token : tokens)
            {
                tokenStat.addValue(token.weight);
            }
        }
        st.update(tokenStat);

        if (print)
        {
            System.out.format("Size %d(%d)   \tunit %s  token %s   %s\n",
                              t.sortedUnits.size(), size,
                              mms(unitStat),
                              mms(tokenStat),
                              t.strategy);
            System.out.format("Worst intermediate unit\t%s  token %s\n", su, st);
        }
    }

    static class NoReplicationStrategy implements TestReplicationStrategy
    {
        public List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens)
        {
            return Collections.singletonList(sortedTokens.ceilingEntry(token).getValue());
        }

        public Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens)
        {
            return sortedTokens.lowerKey(token);
        }

        public String toString()
        {
            return "No replication";
        }

        public void addUnit(Unit n)
        {
        }

        public void removeUnit(Unit n)
        {
        }

        public int replicas()
        {
            return 1;
        }

        public Object getGroup(Unit unit)
        {
            return unit;
        }

        public double spreadExpectation()
        {
            return 1;
        }
    }

    static class ZeroReplicationStrategy extends NoReplicationStrategy
    {
        public int replicas()
        {
            return 0;
        }
    }
}
