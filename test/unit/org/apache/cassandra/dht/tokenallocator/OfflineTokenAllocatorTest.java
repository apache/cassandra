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

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocator.allocate;

public class OfflineTokenAllocatorTest
{
    private static final Logger logger = LoggerFactory.getLogger(OfflineTokenAllocatorTest.class);
    private static final OutputHandler FAIL_ON_WARN_OUTPUT = new SystemOutputImpl();

    @Before
    public void setup()
    {
        Util.initDatabaseDescriptor();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedPartitioner()
    {
        List<OfflineTokenAllocator.FakeNode> nodes = allocate(3, 4, new int[]{1,1,1}, FAIL_ON_WARN_OUTPUT, ByteOrderedPartitioner.instance);
        Assert.assertEquals(3, nodes.size());
    }

    /**
     * Cycle through a matrix of valid ranges.
     */
    @Test
    public void testTokenGenerations()
    {
        for (int numTokens = 1; numTokens <= 16 ; ++numTokens)
        {
            for (int rf = 1; rf <=5; ++rf)
            {
                int nodeCount = 32;
                for (int racks = 1; racks <= 10; ++racks)
                {
                    int[] nodeToRack = makeRackCountArray(nodeCount, racks);
                    for (IPartitioner partitioner : new IPartitioner[] { Murmur3Partitioner.instance, RandomPartitioner.instance })
                    {
                        logger.info("Testing offline token allocator for numTokens={}, rf={}, racks={}, nodeToRack={}, partitioner={}",
                                    numTokens, rf, racks, nodeToRack, partitioner);
                        assertTokensAndNodeCount(numTokens, nodeCount, allocate(rf,
                                                                                numTokens,
                                                                                nodeToRack,
                                                                                new SystemOutputImpl(rf, racks),
                                                                                partitioner));
                    }
                }
            }
        }
    }

    private void assertTokensAndNodeCount(int numTokens, int nodeCount, List<OfflineTokenAllocator.FakeNode> nodes)
    {
        assertEquals(nodeCount, nodes.size());
        Collection<Token> allTokens = Lists.newArrayList();
        for (OfflineTokenAllocator.FakeNode node : nodes)
        {
            Assertions.assertThat(node.tokens()).hasSize(numTokens);
            Assertions.assertThat(allTokens).doesNotContainAnyElementsOf(node.tokens());
            allTokens.addAll(node.tokens());
        }
    }

    private static int[] makeRackCountArray(int nodes, int racks)
    {
        assert nodes > 0;
        assert racks > 0;
        // Distribute nodes among the racks in round-robin fashion in the order the user is supposed to start them.
        int[] rackCounts = new int[racks];
        int rack = 0;
        for (int node = 0; node < nodes; node++)
        {
            rackCounts[rack]++;
            if (++rack == racks)
                rack = 0;
        }
        return rackCounts;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTokenGenerator_more_rf_than_racks()
    {
        OfflineTokenAllocator.allocate(3, 16, new int[]{1, 1}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testTokenGenerator_more_rf_than_nodes()
    {
        OfflineTokenAllocator.allocate(3, 16, new int[]{2}, FAIL_ON_WARN_OUTPUT, Murmur3Partitioner.instance);
    }

    @Test
    public void testTokenGenerator_single_rack_or_single_rf()
    {
        int numTokens = 16;
        // Simple cases, single rack or single replication.
        assertTokensAndNodeCount(numTokens, 1, allocate(1,
                                                        numTokens,
                                                        new int[]{1},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
        assertTokensAndNodeCount(numTokens, 2, allocate(1,
                                                        numTokens,
                                                        new int[]{1, 1},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
        assertTokensAndNodeCount(numTokens, 2, allocate(1,
                                                        numTokens,
                                                        new int[]{2},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
    }

    @Test
    public void testTokenGenerator_unbalanced_racks()
    {
        int numTokens = 16;
        // Simple cases, single rack or single replication.
        assertTokensAndNodeCount(numTokens, 6, allocate(1,
                                                        numTokens,
                                                        new int[]{5, 1},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
        assertTokensAndNodeCount(numTokens, 7, allocate(1,
                                                        numTokens,
                                                        new int[]{5, 1, 1},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
        assertTokensAndNodeCount(numTokens, 6, allocate(3,
                                                        numTokens,
                                                        new int[]{5, 1},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
        assertTokensAndNodeCount(numTokens, 7, allocate(3,
                                                        numTokens,
                                                        new int[]{5, 1, 1},
                                                        FAIL_ON_WARN_OUTPUT,
                                                        Murmur3Partitioner.instance));
    }

    private static class SystemOutputImpl extends OutputHandler.SystemOutput
    {
        private final int rf;
        private final int racks;

        private SystemOutputImpl()
        {
            super(true, true);
            rf = racks = 1;
        }

        private SystemOutputImpl(int rf, int racks)
        {
            super(true, true);
            this.rf = rf;
            this.racks = racks;
        }

        @Override
        public void warn(String msg)
        {
            // We can only guarantee that ownership stdev won't increase above the warn threshold for racks==1 or racks==rf
            if (racks == 1 || racks == rf)
                fail(msg);
            else
                super.warn(msg);
        }

        @Override
        public void warn(String msg, Throwable th)
        {
            // We can only guarantee that ownership stdev won't increase above the warn threshold for racks==1 or racks==rf
            if (racks == 1 || racks == rf)
                fail(msg);
            else
                super.warn(msg, th);
        }
    }
}
