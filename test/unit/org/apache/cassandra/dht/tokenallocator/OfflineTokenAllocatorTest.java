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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.tools.Util;

import static org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocator.allocate;
import static org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocatorTestUtils.FAIL_ON_WARN_OUTPUT;
import static org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocatorTestUtils.assertTokensAndNodeCount;

public class OfflineTokenAllocatorTest
{
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
}
