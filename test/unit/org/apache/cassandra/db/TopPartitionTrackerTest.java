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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopPartitionTrackerTest extends CQLTester
{
    @Test
    public void testSizeLimit()
    {
        DatabaseDescriptor.setMaxTopSizePartitionCount(5);
        DatabaseDescriptor.setMinTrackedPartitionSize(12);

        Collection<Range<Token>> fullRange = Collections.singleton(r(0, 0));
        TopPartitionTracker tpt = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(fullRange);
        for (int i = 5; i < 15; i++)
            collector.trackSize(dk(i), i);
        tpt.merge(collector);
        assertEquals(3, tpt.topSizes().top.size());
        assertTrue(tpt.topSizes().top.stream().allMatch(tp -> tp.value >= 12));

        Collection<Range<Token>> keyRange = rangesFor(7);
        collector = new TopPartitionTracker.Collector(keyRange);
        collector.trackSize(dk(7), 7);
        tpt.merge(collector);
        assertEquals(3, tpt.topSizes().top.size());
        assertTrue(tpt.topSizes().top.stream().allMatch(tp -> tp.value >= 12));
        assertFalse(tpt.topSizes().top.contains(tp(7, 7)));

        collector = new TopPartitionTracker.Collector(keyRange);
        collector.trackSize(dk(7), 100);
        tpt.merge(collector);
        assertEquals(4, tpt.topSizes().top.size());
        assertTrue(tpt.topSizes().top.stream().allMatch(tp -> tp.value >= 12));
        assertTrue(tpt.topSizes().top.contains(tp(7, 100)));
    }

    @Test
    public void testCountLimit()
    {
        DatabaseDescriptor.setMinTrackedPartitionSize(0);
        DatabaseDescriptor.setMaxTopSizePartitionCount(5);
        Collection<Range<Token>> fullRange = Collections.singleton(r(0, 0));
        TopPartitionTracker tpt = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(fullRange);
        for (int i = 5; i < 15; i++)
            collector.trackSize(dk(i), i);
        tpt.merge(collector);
        assertEquals(5, tpt.topSizes().top.size());

        collector = new TopPartitionTracker.Collector(fullRange);
        for (int i = 5; i < 15; i++)
            collector.trackSize(dk(i), i + 1);
        tpt.merge(collector);
        assertEquals(5, tpt.topSizes().top.size());

        collector = new TopPartitionTracker.Collector(rangesFor(15));
        collector.trackSize(dk(15), 14);
        tpt.merge(collector);
        assertEquals(5, tpt.topSizes().top.size());
    }

    @Test
    public void testSubRangeMerge()
    {
        DatabaseDescriptor.setMinTrackedPartitionSize(0);
        DatabaseDescriptor.setMaxTopSizePartitionCount(10);
        Collection<Range<Token>> fullRange = Collections.singleton(r(0, 0));
        TopPartitionTracker tpt = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(fullRange);
        for (int i = 0; i < 10; i++)
            collector.trackSize(dk(i), 10);
        tpt.merge(collector);
        assertEquals(10, tpt.topSizes().top.size());
        collector = new TopPartitionTracker.Collector(rangesFor(0,1,2,3,4));
        for (int i = 0; i < 5; i++)
            collector.trackSize(dk(i), 8);
        tpt.merge(collector);

        assertEquals(10, tpt.topSizes().top.size());
        for (TopPartitionTracker.TopPartition tp : tpt.topSizes().top)
        {
            long key = ByteBufferUtil.toLong(tp.key.getKey());
            if (key < 5)
                assertEquals(8, tp.value);
            else
                assertEquals(10, tp.value);
        }
    }

    @Test
    public void testSaveLoad()
    {
        DatabaseDescriptor.setMinTrackedPartitionSize(0);
        DatabaseDescriptor.setMinTrackedPartitionTombstoneCount(0);
        DatabaseDescriptor.setMaxTopSizePartitionCount(10);
        DatabaseDescriptor.setMaxTopTombstonePartitionCount(10);
        Collection<Range<Token>> fullRange = Collections.singleton(r(0, 0));
        TopPartitionTracker tpt = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(fullRange);
        for (int i = 0; i < 10; i++)
        {
            collector.trackSize(dk(i), 10);
            collector.trackTombstones(dk(i + 10), 100);
        }
        tpt.merge(collector);
        assertEquals(10, tpt.topSizes().top.size());
        assertEquals(10, tpt.topTombstones().top.size());
        tpt.save();
        TopPartitionTracker tptLoaded = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        assertEquals(tpt.topSizes().top, tptLoaded.topSizes().top);
        assertEquals(tpt.topTombstones().top, tptLoaded.topTombstones().top);

        DatabaseDescriptor.setMaxTopSizePartitionCount(5);
        DatabaseDescriptor.setMaxTopTombstonePartitionCount(5);

        tptLoaded = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        assertEquals(5, tptLoaded.topSizes().top.size());
        assertEquals(5, tptLoaded.topTombstones().top.size());

        Iterator<TopPartitionTracker.TopPartition> oldIter = tpt.topSizes().top.iterator();
        Iterator<TopPartitionTracker.TopPartition> loadedIter = tptLoaded.topSizes().top.iterator();
        while (loadedIter.hasNext())
        {
            TopPartitionTracker.TopPartition old = oldIter.next();
            TopPartitionTracker.TopPartition loaded = loadedIter.next();
            assertEquals(old.key, loaded.key);
            assertEquals(old.value, loaded.value);
        }

        oldIter = tpt.topTombstones().top.iterator();
        loadedIter = tptLoaded.topTombstones().top.iterator();
        while (loadedIter.hasNext())
        {
            TopPartitionTracker.TopPartition old = oldIter.next();
            TopPartitionTracker.TopPartition loaded = loadedIter.next();
            assertEquals(old.key, loaded.key);
            assertEquals(old.value, loaded.value);
        }

    }

    @Test
    public void randomTest()
    {
        DatabaseDescriptor.setMinTrackedPartitionSize(0);
        DatabaseDescriptor.setMaxTopSizePartitionCount(1000);
        long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        List<DecoratedKey> keys = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++)
            keys.add(dk(i));

        Collection<Range<Token>> fullRange = Collections.singleton(r(0, 0));
        List<Pair<DecoratedKey, Long>> expected = new ArrayList<>();
        TopPartitionTracker tpt = new TopPartitionTracker(Murmur3Partitioner.instance, "something", "else");
        TopPartitionTracker.Collector collector = new TopPartitionTracker.Collector(fullRange);
        for (int i = 0; i < keys.size(); i++)
        {
            DecoratedKey key = keys.get(i);
            long value = Math.abs(r.nextLong() % 20000);
            expected.add(Pair.create(key, value));
            collector.trackSize(key, value);
        }
        tpt.merge(collector);
        expected.sort((o1, o2) -> {
            int cmp = -o1.right.compareTo(o2.right);
            if (cmp != 0)
                return cmp;
            return o1.left.compareTo(o2.left);
        });
        Iterator<Pair<DecoratedKey, Long>> expectedTop = expected.subList(0,1000).iterator();
        Iterator<TopPartitionTracker.TopPartition> trackedTop = tpt.topSizes().top.iterator();

        while (expectedTop.hasNext())
        {
            Pair<DecoratedKey, Long> ex = expectedTop.next();
            TopPartitionTracker.TopPartition tracked = trackedTop.next();
            assertEquals("seed "+seed, ex.left, tracked.key);
            assertEquals("seed "+seed, (long)ex.right, tracked.value);
        }
    }


    private static TopPartitionTracker.TopPartition tp(int i, long c)
    {
        return new TopPartitionTracker.TopPartition(dk(i), c);
    }
    private static DecoratedKey dk(long i)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(i));
    }
    private static Range<Token> r(long start, long end)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(start), new Murmur3Partitioner.LongToken(end));
    }
    private static long tokenValue(long key)
    {
        return (long) dk(key).getToken().getTokenValue();
    }
    private static Collection<Range<Token>> rangesFor(long ... keys)
    {
        List<Range<Token>> ranges = new ArrayList<>(keys.length);
        for (long key : keys)
            ranges.add(r(tokenValue(key) - 1, tokenValue(key)));
        return ranges;
    }
}
