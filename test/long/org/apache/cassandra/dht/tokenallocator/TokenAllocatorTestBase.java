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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

/**
 * Base class for {@link NoReplicationTokenAllocatorTest} and {@link AbstractReplicationAwareTokenAllocatorTest},
 */
abstract class TokenAllocatorTestBase
{
    protected static final int TARGET_CLUSTER_SIZE = 250;
    protected static final int MAX_VNODE_COUNT = 64;

    interface TestReplicationStrategy extends ReplicationStrategy<Unit>
    {
        void addUnit(Unit n);

        void removeUnit(Unit n);

        /**
         * Returns a list of all replica units for given token.
         */
        List<Unit> getReplicas(Token token, NavigableMap<Token, Unit> sortedTokens);

        /**
         * Returns the start of the token span that is replicated in this token.
         * Note: Though this is not trivial to see, the replicated span is always contiguous. A token in the same
         * group acts as a barrier; if one is not found the token replicates everything up to the replica'th distinct
         * group seen in front of it.
         */
        Token replicationStart(Token token, Unit unit, NavigableMap<Token, Unit> sortedTokens);

        /**
         * Multiplier for the acceptable disbalance in the cluster. With some strategies it is harder to achieve good
         * results.
         */
        double spreadExpectation();
    }

    interface TokenCount
    {
        int tokenCount(int perUnitCount, Random rand);

        double spreadExpectation();
    }

    TokenCount fixedTokenCount = new TokenCount()
    {
        public int tokenCount(int perUnitCount, Random rand)
        {
            return perUnitCount;
        }

        public double spreadExpectation()
        {
            return 4;  // High tolerance to avoid flakiness.
        }
    };

    TokenCount varyingTokenCount = new TokenCount()
    {
        public int tokenCount(int perUnitCount, Random rand)
        {
            if (perUnitCount == 1) return 1;
            // 25 to 175%
            return rand.nextInt(perUnitCount * 3 / 2) + (perUnitCount + 3) / 4;
        }

        public double spreadExpectation()
        {
            return 8;  // High tolerance to avoid flakiness.
        }
    };

    Random seededRand = new Random(2);

    public void random(Map<Token, Unit> map, TestReplicationStrategy rs,
                              int unitCount, TokenCount tc, int perUnitCount, IPartitioner partitioner)
    {
        System.out.format("\nRandom generation of %d units with %d tokens each\n", unitCount, perUnitCount);
        Random rand = seededRand;
        for (int i = 0; i < unitCount; i++)
        {
            Unit unit = new Unit();
            rs.addUnit(unit);
            int tokens = tc.tokenCount(perUnitCount, rand);
            for (int j = 0; j < tokens; j++)
            {
                map.put(partitioner.getRandomToken(rand), unit);
            }
        }
    }

    public String mms(SummaryStatistics s)
    {
        return String.format("max %.2f min %.2f stddev %.4f", s.getMax(), s.getMin(), s.getStandardDeviation());
    }

    class Summary
    {
        double min = 1;
        double max = 1;
        double stddev = 0;

        void update(SummaryStatistics stat)
        {
            min = Math.min(min, stat.getMin());
            max = Math.max(max, stat.getMax());
            stddev = Math.max(stddev, stat.getStandardDeviation());
        }

        public String toString()
        {
            return String.format("max %.2f min %.2f stddev %.4f", max, min, stddev);
        }
    }

    int nextUnitId = 0;

    final class Unit implements Comparable<Unit>
    {
        int unitId = nextUnitId++;

        public String toString()
        {
            return Integer.toString(unitId);
        }

        @Override
        public int compareTo(Unit o)
        {
            return Integer.compare(unitId, o.unitId);
        }
    }
}
