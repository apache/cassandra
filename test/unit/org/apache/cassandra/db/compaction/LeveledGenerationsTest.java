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

package org.apache.cassandra.db.compaction;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class LeveledGenerationsTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testWrappingIterable()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();

        LeveledGenerations gens = new LeveledGenerations();

        for (int i = 0; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i, 5, true, i, i, 2, cfs);
            gens.addAll(Collections.singleton(sstable));
        }
        int gen = 10;
        assertIter(gens.wrappingIterator(2, sst(++gen, cfs, 5, 5)),
                   6, 5, 10);
        assertIter(gens.wrappingIterator(2, null),
                   0, 9, 10);
        assertIter(gens.wrappingIterator(2, sst(++gen, cfs, -10, 0)),
                   1, 0, 10);
        assertIter(gens.wrappingIterator(2, sst(++gen, cfs, 5, 9)),
                   0, 9, 10);
        assertIter(gens.wrappingIterator(2, sst(++gen, cfs, 0, 1000)),
                   0, 9, 10);

        gens.addAll(Collections.singleton(MockSchema.sstable(100, 5, true, 5, 10, 3, cfs)));
        assertIter(gens.wrappingIterator(3, sst(++gen, cfs, -10, 0)),
                   5, 5, 1);
        assertIter(gens.wrappingIterator(3, sst(++gen, cfs, 0, 100)),
                   5, 5, 1);

        gens.addAll(Collections.singleton(MockSchema.sstable(200, 5, true, 5, 10, 4, cfs)));
        gens.addAll(Collections.singleton(MockSchema.sstable(201, 5, true, 40, 50, 4, cfs)));
        assertIter(gens.wrappingIterator(4, sst(++gen, cfs, 0, 0)),
                   5, 40, 2);
        assertIter(gens.wrappingIterator(4, sst(++gen, cfs, 0, 5)),
                   40, 5, 2);
        assertIter(gens.wrappingIterator(4, sst(++gen, cfs, 7, 8)),
                   40, 5, 2);
        assertIter(gens.wrappingIterator(4, sst(++gen, cfs, 39, 39)),
                   40, 5, 2);
        assertIter(gens.wrappingIterator(4, sst(++gen, cfs, 40, 40)),
                   5, 40, 2);
        assertIter(gens.wrappingIterator(4, sst(++gen, cfs, 100, 1000)),
                   5, 40, 2);
    }

    @Test
    public void testWrappingIterableWiderSSTables()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledGenerations generations = new LeveledGenerations();
        int gen = 0;
        generations.addAll(Lists.newArrayList(
            sst(++gen, cfs, 0, 50),
            sst(++gen, cfs, 51, 100),
            sst(++gen, cfs, 150, 200)));

        assertIter(generations.wrappingIterator(2, sst(++gen, cfs, -100, -50)),
                   0, 150, 3);

        assertIter(generations.wrappingIterator(2, sst(++gen, cfs, 0, 40)),
                   51, 0, 3);
        assertIter(generations.wrappingIterator(2, sst(++gen, cfs, 0, 50)),
                   51, 0, 3);
        assertIter(generations.wrappingIterator(2, sst(++gen, cfs, 0, 51)),
                   150, 51, 3);

        assertIter(generations.wrappingIterator(2, sst(++gen, cfs, 100, 149)),
                   150, 51, 3);
        assertIter(generations.wrappingIterator(2, sst(++gen, cfs, 100, 300)),
                   0, 150, 3);

    }

    @Test
    public void testEmptyLevel()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledGenerations generations = new LeveledGenerations();
        assertFalse(generations.wrappingIterator(3, sst(0, cfs, 0, 10)).hasNext());
        assertFalse(generations.wrappingIterator(3, null).hasNext());
    }

    @Test
    public void testFillLevels()
    {
        LeveledGenerations generations = new LeveledGenerations();

        ColumnFamilyStore cfs = MockSchema.newCFS();
        for (int i = 0; i < LeveledGenerations.MAX_LEVEL_COUNT; i++)
            generations.addAll(Collections.singleton(MockSchema.sstableWithLevel(i, i, i, i, cfs)));

        for (int i = 0; i < generations.levelCount(); i++)
            assertEquals(i, generations.get(i).iterator().next().getSSTableLevel());

        assertEquals(9, generations.levelCount());

        try
        {
            generations.get(9);
            fail("don't have 9 generations");
        }
        catch (ArrayIndexOutOfBoundsException e)
        {}
        try
        {
            generations.get(-1);
            fail("don't have -1 generations");
        }
        catch (ArrayIndexOutOfBoundsException e)
        {}
    }

    @Test
    public void testGetOverlapping()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledGenerations gens = new LeveledGenerations();

        // Populate Level 2 with disjoint sstables: [0, 10], [20, 30], [40, 50], [60, 70], [80, 90]
        SSTableReader s1 = MockSchema.sstable(1, 5, true, 0, 10, 2, cfs);
        SSTableReader s2 = MockSchema.sstable(2, 5, true, 20, 30, 2, cfs);
        SSTableReader s3 = MockSchema.sstable(3, 5, true, 40, 50, 2, cfs);
        SSTableReader s4 = MockSchema.sstable(4, 5, true, 60, 70, 2, cfs);
        SSTableReader s5 = MockSchema.sstable(5, 5, true, 80, 90, 2, cfs);
        gens.addAll(Lists.newArrayList(s1, s2, s3, s4, s5));

        // 1. Overlap across multiple tables [25, 65]: should find s2 [20, 30], s3 [40, 50], s4 [60, 70]
        SSTableReader targetMulti = MockSchema.sstable(10, 5, true, 25, 65, 1, cfs);
        Set<SSTableReader> overlapping = gens.getOverlapping(2, targetMulti);
        assertEquals(3, overlapping.size());
        assertTrue(overlapping.contains(s2));
        assertTrue(overlapping.contains(s3));
        assertTrue(overlapping.contains(s4));
        assertFalse(overlapping.contains(s1));
        assertFalse(overlapping.contains(s5));

        // 2. Exact match with s3 [40, 50]: should find only s3
        SSTableReader targetExact = MockSchema.sstable(11, 5, true, 40, 50, 1, cfs);
        overlapping = gens.getOverlapping(2, targetExact);
        assertEquals(Collections.singleton(s3), overlapping);

        // 3. In gap between s2 and s3 [32, 38]: should find 0
        SSTableReader targetGap = MockSchema.sstable(12, 5, true, 32, 38, 1, cfs);
        overlapping = gens.getOverlapping(2, targetGap);
        assertTrue(overlapping.isEmpty());

        // 4. Completely before all tables [-50, -10]: should find 0
        SSTableReader targetBefore = MockSchema.sstable(13, 5, true, -50, -10, 1, cfs);
        overlapping = gens.getOverlapping(2, targetBefore);
        assertTrue(overlapping.isEmpty());

        // 5. Completely after all tables [100, 150]: should find 0
        SSTableReader targetAfter = MockSchema.sstable(14, 5, true, 100, 150, 1, cfs);
        overlapping = gens.getOverlapping(2, targetAfter);
        assertTrue(overlapping.isEmpty());

        // 6. Overlap with predecessor floor only [-5, 5]: should find s1 [0, 10]
        SSTableReader targetFloor = MockSchema.sstable(15, 5, true, -5, 5, 1, cfs);
        overlapping = gens.getOverlapping(2, targetFloor);
        assertEquals(Collections.singleton(s1), overlapping);

        // 7. Overlap with successor only [85, 95]: should find s5 [80, 90]
        SSTableReader targetSuccessor = MockSchema.sstable(16, 5, true, 85, 95, 1, cfs);
        overlapping = gens.getOverlapping(2, targetSuccessor);
        assertEquals(Collections.singleton(s5), overlapping);

        // 8. Empty level: should return empty set
        overlapping = gens.getOverlapping(3, targetExact);
        assertTrue(overlapping.isEmpty());
    }

    private void assertIter(Iterator<SSTableReader> iter, long first, long last, int expectedCount)
    {
        List<SSTableReader> drained = Lists.newArrayList(iter);
        assertEquals(expectedCount, drained.size());
        assertEquals(dk(first).getToken(), first(drained).getFirst().getToken());
        assertEquals(dk(last).getToken(), last(drained).getFirst().getToken()); // we sort by first token, so this is the first token of the last sstable in iter
    }

    private SSTableReader last(Iterable<SSTableReader> iter)
    {
        return Iterables.getLast(iter);
    }
    private SSTableReader first(Iterable<SSTableReader> iter)
    {
        SSTableReader first = Iterables.getFirst(iter, null);
        if (first == null)
            throw new RuntimeException();
        return first;
    }

    private DecoratedKey dk(long x)
    {
        return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(x), ByteBufferUtil.bytes(x));
    }
    private SSTableReader sst(int gen, ColumnFamilyStore cfs, long first, long last)
    {
        return MockSchema.sstable(gen, 5, true, first, last, 2, cfs);
    }

    private void print(SSTableReader sstable)
    {
        System.out.println(String.format("%d %s %s %d", sstable.descriptor.id, sstable.getFirst(), sstable.getLast(), sstable.getSSTableLevel()));
    }
}
