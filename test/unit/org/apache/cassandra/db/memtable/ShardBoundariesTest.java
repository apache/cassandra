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

package org.apache.cassandra.db.memtable;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ShardBoundariesTest
{
    private ShardBoundaries boundaries;

    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        boundaries = new ShardBoundaries(new Token[]{ token(10), token(20), token(30) }, Epoch.EMPTY);
    }

    private static Murmur3Partitioner.LongToken token(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    private static DecoratedKey key(long tokenValue)
    {
        return new BufferDecoratedKey(token(tokenValue), ByteBufferUtil.bytes(tokenValue));
    }

    @Test
    public void testGetShardForToken()
    {
        assertEquals(0, boundaries.getShardForToken(token(5)));
        assertEquals(0, boundaries.getShardForToken(token(10)));
        assertEquals(1, boundaries.getShardForToken(token(15)));
        assertEquals(1, boundaries.getShardForToken(token(20)));
        assertEquals(2, boundaries.getShardForToken(token(30)));
        assertEquals(3, boundaries.getShardForToken(token(35)));
    }

    @Test
    public void testGetShardsForRangeNoBoundaries()
    {
        List<Integer> shards = ShardBoundaries.NONE.getShardsForRange(new Bounds<>(key(1), key(100)));
        assertEquals(List.of(0), shards);
    }

    @Test
    public void testGetShardsForRangeWholeRing()
    {
        PartitionPosition minimum = Murmur3Partitioner.instance.getMinimumToken().minKeyBound();
        List<Integer> shards = boundaries.getShardsForRange(new Range<>(minimum, minimum));
        assertEquals(List.of(0, 1, 2, 3), shards);
    }

    @Test
    public void testGetShardsForRangeSingleShard()
    {
        List<Integer> shards = boundaries.getShardsForRange(new Bounds<>(key(12), key(15)));
        assertEquals(List.of(1), shards);
    }

    @Test
    public void testGetShardsForRangeMultipleContiguousShards()
    {
        List<Integer> shards = boundaries.getShardsForRange(new Bounds<>(key(5), key(25)));
        assertEquals(List.of(0, 1, 2), shards);
    }

    @Test
    public void testGetShardsForRangeOpenEndedRight()
    {
        PartitionPosition minimum = Murmur3Partitioner.instance.getMinimumToken().minKeyBound();
        List<Integer> shards = boundaries.getShardsForRange(new Bounds<>(key(15), minimum));
        assertEquals(List.of(1, 2, 3), shards);
    }

    @Test
    public void testGetShardsForRangeWrappingRange()
    {
        List<Integer> shards = boundaries.getShardsForRange(new Range<>(key(25), key(5)));
        assertEquals(List.of(0, 2, 3), shards);
    }
}
