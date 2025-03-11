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
package org.apache.cassandra.service.tracking;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.MutationId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.cassandra.db.MutationId.offset;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SequenceIdsTest
{
    private static class TestConsumer implements SequenceIds.RangeConsumer
    {
        static class OffsetRange
        {
            final int start;
            final int end;

            public OffsetRange(int start, int end)
            {
                this.start = start;
                this.end = end;
            }

            @Override
            public boolean equals(Object o)
            {
                if (o == null || getClass() != o.getClass()) return false;
                OffsetRange range = (OffsetRange) o;
                return start == range.start && end == range.end;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(start, end);
            }

            @Override
            public String toString()
            {
                return String.format("<%s,%s>", offset(start), offset(end));
            }
        }

        final List<OffsetRange> ranges = new ArrayList<>();

        @Override
        public void consume(long start, long end)
        {
            consumerOffsets(offset(start), offset(end));
        }

        public void consumerOffsets(int start, int end)
        {
            ranges.add(new OffsetRange(start, end));
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            TestConsumer that = (TestConsumer) o;
            return Objects.equals(ranges, that.ranges);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(ranges);
        }

        @Override
        public String toString()
        {
            return ranges.toString();
        }

        public TestConsumer assertOffsetsConsumed(int... expected)
        {
            Assert.assertTrue(expected.length % 2 == 0);
            TestConsumer expectedConsumer = new TestConsumer();
            for (int i = 0; i < expected.length; i+=2)
                expectedConsumer.consumerOffsets(expected[i], expected[i+1]);

            Assert.assertEquals(expectedConsumer, this);
            return this;
        }

        public TestConsumer assertConsumed(long... expected)
        {
            int[] offsets = new int[expected.length];
            for (int i = 0; i < expected.length; i++)
                offsets[i] = offset(expected[i]);

            return assertOffsetsConsumed(offsets);
        }

        void clear()
        {
            ranges.clear();
        }

        boolean isEmpty()
        {
            return ranges.isEmpty();
        }
    }

    private static SequenceIds sequenceIds(int... bounds)
    {
        Assert.assertTrue(bounds.length % 2 == 0);
        SequenceIds ids = new SequenceIds();
        int keys = 0;
        int last = 0;
        for (int i=0; i<bounds.length; i+=2)
        {
            int start = bounds[i];
            int end = bounds[i + 1];
            keys += end - start + 1;
            Assert.assertTrue(start <= end);
            if (i > 0)
                Assert.assertTrue(start > last + 1);

            ids.add(sequenceId(start), sequenceId(end));
            last = end;
        }

        Assert.assertEquals(bounds.length/2, ids.rangeCount());
        Assert.assertEquals(keys, ids.idCount());
        return ids;
    }


    @Test
    public void testEmptyAndAddExisting()
    {
        SequenceIds ids = new SequenceIds();
        assertEquals(0, ids.rangeCount());
        assertEquals(0, ids.idCount());

        long id10 = id(10);

        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        assertFalse(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());
    }

    @Test
    public void testAppend()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should extend
        long id11 = id(11);
        assertTrue(ids.add(id11));
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should append
        long id13 = id(13);
        assertTrue(ids.add(id13));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());
    }

    @Test
    public void testPrepend()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should extend
        long id9 = id(9);
        assertTrue(ids.add(id9));
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should prepend
        long id7 = id(7);
        assertTrue(ids.add(id7));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());
    }

    @Test
    public void testClosesGaps()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should prepend
        long id6 = id(6);
        assertTrue(ids.add(id6));
        assertEquals(2, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should extend left range
        long id7 = id(7);
        assertTrue(ids.add(id7));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());

        // should extend right range
        long id9 = id(9);
        assertTrue(ids.add(id9));
        assertEquals(2, ids.rangeCount());
        assertEquals(4, ids.idCount());

        // should close the gap and collapse all into one range
        long id8 = id(8);
        assertTrue(ids.add(id8));
        assertEquals(1, ids.rangeCount());
        assertEquals(5, ids.idCount());
    }

    @Test
    public void testCreatesMoreGaps()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should prepend
        long id6 = id(6);
        assertTrue(ids.add(id6));
        assertEquals(2, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should insert in the middle
        long id8 = id(8);
        assertTrue(ids.add(id8));
        assertEquals(3, ids.rangeCount());
        assertEquals(3, ids.idCount());
    }

    @Test
    public void testRangeAppend()
    {
        SequenceIds ids = new SequenceIds();
        ids.add(id(5), id(7));
        TestConsumer consumer = new TestConsumer();

        // add overlapping range 1
        assertTrue(ids.add(id(6), id(8), consumer));
        assertEquals(1, ids.rangeCount());
        assertEquals(4, ids.idCount());
        consumer.assertOffsetsConsumed(8, 8).clear();

        // add overlapping range 2
        assertTrue(ids.add(id(8), id(9), consumer));
        assertEquals(1, ids.rangeCount());
        assertEquals(5, ids.idCount());
        consumer.assertOffsetsConsumed(9, 9).clear();

        // add adjacent range
        assertTrue(ids.add(id(10), id(12), consumer));
        assertEquals(1, ids.rangeCount());
        assertEquals(8, ids.idCount());
        consumer.assertOffsetsConsumed(10, 12).clear();

        // add disjoint range
        assertTrue(ids.add(id(14), id(16), consumer));
        assertEquals(2, ids.rangeCount());
        assertEquals(11, ids.idCount());
        consumer.assertOffsetsConsumed(14, 16).clear();

    }

    @Test
    public void testRangePrepend()
    {
        SequenceIds ids = new SequenceIds();
        ids.add(id(10), id(12));
        TestConsumer consumer = new TestConsumer();

        assertEquals(1, ids.rangeCount());
        assertEquals(3, ids.idCount());

        // add overlapping range 1
        assertTrue(ids.add(id(9), id(11), consumer));
        assertEquals(1, ids.rangeCount());
        assertEquals(4, ids.idCount());
        consumer.assertOffsetsConsumed(9, 9).clear();

        // add overlapping range 2
        assertTrue(ids.add(id(8), id(9), consumer));
        assertEquals(1, ids.rangeCount());
        assertEquals(5, ids.idCount());
        consumer.assertOffsetsConsumed(8, 8).clear();

        // add adjacent range
        assertTrue(ids.add(id(6), id(7), consumer));
        assertEquals(1, ids.rangeCount());
        assertEquals(7, ids.idCount());
        consumer.assertOffsetsConsumed(6, 7).clear();

        // add disjoint range
        assertTrue(ids.add(id(0), id(3), consumer));
        assertEquals(2, ids.rangeCount());
        assertEquals(11, ids.idCount());
        consumer.assertOffsetsConsumed(0, 3).clear();
    }

    @Test
    public void testRangeAddition()
    {
        SequenceIds ids = new SequenceIds();
        ids.add(id(5), id(7));

        assertEquals(1, ids.rangeCount());
        assertEquals(3, ids.idCount());

    }

    /**
     * adding ranges fully contained in existing ranges should noop
     */
    @Test
    public void testRangeInclusion()
    {
        SequenceIds ids = new SequenceIds();
        TestConsumer consumer = new TestConsumer();
        ids.add(id(0), id(3));
        ids.add(id(7), id(10));
        ids.add(id(15), id(17));

        assertEquals(3, ids.rangeCount());
        assertEquals(11, ids.idCount());

        // fully contained in first
        assertFalse(ids.add(id(0), id(2), consumer));
        assertFalse(ids.add(id(1), id(2), consumer));
        assertFalse(ids.add(id(1), id(3), consumer));
        assertFalse(ids.add(id(0), id(3), consumer));


        // fully contained in second
        assertFalse(ids.add(id(7), id(9), consumer));
        assertFalse(ids.add(id(8), id(9), consumer));
        assertFalse(ids.add(id(8), id(10), consumer));
        assertFalse(ids.add(id(7), id(10), consumer));

        // fully contained in third
        assertFalse(ids.add(id(16), id(16), consumer));
        assertFalse(ids.add(id(16), id(17), consumer));
        assertFalse(ids.add(id(15), id(16), consumer));
        assertFalse(ids.add(id(15), id(17), consumer));

        // nothing should have changed
        assertEquals(3, ids.rangeCount());
        assertEquals(11, ids.idCount());
        assertTrue(consumer.isEmpty());
    }

    @Test
    public void testRangeInsert()
    {
        Supplier<SequenceIds> sequenceIds = () -> {
            SequenceIds ids0 = new SequenceIds();
            ids0.add(id(0), id(3));
            ids0.add(id(7), id(10));
            ids0.add(id(15), id(17));

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.idCount());
            return ids0;
        };

        // disjoint insert
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(12), id(13), consumer));
            assertEquals(4, ids.rangeCount());
            assertEquals(13, ids.idCount());
            consumer.assertOffsetsConsumed(12, 13).clear();
        }

        // left adjacent insert
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(5), id(6), consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.idCount());
            consumer.assertOffsetsConsumed(5, 6).clear();
        }

        // right adjacent insert
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(11), id(12), consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.idCount());
            consumer.assertOffsetsConsumed(11, 12).clear();
        }

        // both adjacent insert
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(11), id(14), consumer));
            assertEquals(2, ids.rangeCount());
            assertEquals(15, ids.idCount());
            consumer.assertOffsetsConsumed(11, 14).clear();
        }
    }


    @Test
    public void testRangeMerging()
    {
        Supplier<SequenceIds> sequenceIds = () -> {
            SequenceIds ids0 = new SequenceIds();
            ids0.add(id(0), id(3));
            ids0.add(id(7), id(10));
            ids0.add(id(15), id(17));

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.idCount());
            return ids0;
        };

        // left merge
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(5), id(8), consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.idCount());
            consumer.assertOffsetsConsumed(5, 6).clear();
        }

        // right merge
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(8), id(12), consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.idCount());
            consumer.assertOffsetsConsumed(11, 12).clear();
        }

        // right and left merge
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(6), id(11), consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.idCount());
            consumer.assertOffsetsConsumed(6, 6, 11, 11).clear();
        }

        // 2 range merge
        {
            SequenceIds ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(id(2), id(8), consumer));
            assertEquals(2, ids.rangeCount());
            assertEquals(14, ids.idCount());
            consumer.assertOffsetsConsumed(4, 6).clear();
        }
    }

    @Test
    public void appendTest()
    {
        SequenceIds ids = new SequenceIds();
        ids.append(id(5));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        ids.append(id(6));
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.idCount());

        ids.append(id(8));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());

        // insert before tail
        try
        {
            ids.append(id(8));
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // expected
            assertEquals(2, ids.rangeCount());
            assertEquals(3, ids.idCount());
        }

        // insert before tail
        try
        {
            ids.append(id(7));
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // expected
            assertEquals(2, ids.rangeCount());
            assertEquals(3, ids.idCount());
        }
    }

    @Test
    public void unionTest()
    {
        // left union
        Assert.assertEquals(sequenceIds(0, 3, 6, 10, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(6, 9)));

        // left adjacent union
        Assert.assertEquals(sequenceIds(0, 3, 5, 10, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(5, 6)));

        // right union
        Assert.assertEquals(sequenceIds(0, 3, 7, 11, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(9, 11)));

        // right adjacent
        Assert.assertEquals(sequenceIds(0, 3, 7, 12, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(11, 12)));

        // superset union
        Assert.assertEquals(sequenceIds(0, 3, 5, 12, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(5, 12)));

        // join union
        Assert.assertEquals(sequenceIds(0, 10, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(2, 8)));

        // disjoint
        Assert.assertEquals(sequenceIds(0, 10, 12, 13, 15, 17),
                            SequenceIds.union(sequenceIds(0, 3, 7, 10, 15, 17),
                                              sequenceIds(2, 8, 12, 13)));

    }

    @Test
    public void differenceTest()
    {
        // noop
        Assert.assertEquals(sequenceIds(0, 3, 7, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(5, 5)));
        Assert.assertEquals(sequenceIds(0, 3, 7, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(5, 5, 20, 21)));

        // noop before adjacent
        Assert.assertEquals(sequenceIds(0, 3, 7, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(5, 6, 20, 21)));

        Assert.assertEquals(sequenceIds(0, 3, 7, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(5, 6)));

        // noop after adjacent
        Assert.assertEquals(sequenceIds(0, 3, 7, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(4, 5, 20, 21)));

        Assert.assertEquals(sequenceIds(0, 3, 7, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(4, 5)));

        // before
        Assert.assertEquals(sequenceIds(0, 3, 9, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(6, 8, 20, 21)));

        Assert.assertEquals(sequenceIds(0, 3, 9, 10, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(6, 8)));


        // after
        Assert.assertEquals(sequenceIds(0, 3, 7, 8, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(9, 11, 20, 21)));

        Assert.assertEquals(sequenceIds(0, 3, 7, 8, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(9, 11)));

        // both sides
        Assert.assertEquals(sequenceIds(0, 3, 8, 9, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(6, 7, 10, 12, 20, 21)));

        Assert.assertEquals(sequenceIds(0, 3, 8, 9, 15, 17),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 10, 15, 17),
                                                   sequenceIds(6, 7, 10, 12)));

        // multi-split
        Assert.assertEquals(sequenceIds(0, 3, 7, 8, 11, 11, 14, 15, 20, 22),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 15, 20, 22),
                                                   sequenceIds(9, 10, 12, 13, 30, 31)));

        Assert.assertEquals(sequenceIds(0, 3, 7, 8, 11, 11, 14, 15, 20, 22),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 15, 20, 22),
                                                   sequenceIds(9, 10, 12, 13)));

        // multi-split w/ edges
        Assert.assertEquals(sequenceIds(0, 3, 8, 9, 11, 13, 20, 22),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 15, 20, 22),
                                                   sequenceIds(6, 7, 10, 10, 14, 16, 30, 31)));

        Assert.assertEquals(sequenceIds(0, 3, 8, 9, 11, 13, 20, 22),
                            SequenceIds.difference(sequenceIds(0, 3, 7, 15, 20, 22),
                                                   sequenceIds(6, 7, 10, 10, 14, 16)));

    }

    private static long id(int offset)
    {
        return MutationId.sequenceId(offset, (int) (currentTimeMillis() / 1000));
    }

    private static long sequenceId(int offset)
    {
        return MutationId.sequenceId(offset, 0);
    }
}
