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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OffsetsTest
{
    private static final CoordinatorLogId LOG_ID = new CoordinatorLogId(0, 0);
    private static class TestConsumer implements Offsets.RangeConsumer
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
                return String.format("<%s,%s>", start, end);
            }
        }

        final List<OffsetRange> ranges = new ArrayList<>();

        @Override
        public void consume(CoordinatorLogId logId, int start, int end)
        {
            consumerOffsets(start, end);
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

        public TestConsumer assertConsumed(int... expected)
        {
            int[] offsets = new int[expected.length];
            System.arraycopy(expected, 0, offsets, 0, expected.length);
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

    private static Offsets offsets(int... bounds)
    {
        Assert.assertTrue(bounds.length % 2 == 0);
        Offsets ids = new Offsets(LOG_ID);
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

            ids.add(start, end);
            last = end;
        }

        Assert.assertEquals(bounds.length/2, ids.rangeCount());
        Assert.assertEquals(keys, ids.offsetCount());
        return ids;
    }


    @Test
    public void testEmptyAndAddExisting()
    {
        Offsets offsets = new Offsets(LOG_ID);
        assertEquals(0, offsets.rangeCount());
        assertEquals(0, offsets.offsetCount());

        assertTrue(offsets.add(10));
        assertEquals(1, offsets.rangeCount());
        assertEquals(1, offsets.offsetCount());

        assertFalse(offsets.add(10));
        assertEquals(1, offsets.rangeCount());
        assertEquals(1, offsets.offsetCount());
    }

    @Test
    public void testAppend()
    {
        Offsets offsets = new Offsets(LOG_ID);

        assertTrue(offsets.add(10));
        assertEquals(1, offsets.rangeCount());
        assertEquals(1, offsets.offsetCount());

        // should extend
        assertTrue(offsets.add(11));
        assertEquals(1, offsets.rangeCount());
        assertEquals(2, offsets.offsetCount());

        // should append
        assertTrue(offsets.add(13));
        assertEquals(2, offsets.rangeCount());
        assertEquals(3, offsets.offsetCount());
    }

    @Test
    public void testPrepend()
    {
        Offsets offsets = new Offsets(LOG_ID);

        assertTrue(offsets.add(10));
        assertEquals(1, offsets.rangeCount());
        assertEquals(1, offsets.offsetCount());

        // should extend
        assertTrue(offsets.add(9));
        assertEquals(1, offsets.rangeCount());
        assertEquals(2, offsets.offsetCount());

        // should prepend
        assertTrue(offsets.add(7));
        assertEquals(2, offsets.rangeCount());
        assertEquals(3, offsets.offsetCount());
    }

    @Test
    public void testClosesGaps()
    {
        Offsets offsets = new Offsets(LOG_ID);

        assertTrue(offsets.add(10));
        assertEquals(1, offsets.rangeCount());
        assertEquals(1, offsets.offsetCount());

        // should prepend
        assertTrue(offsets.add(6));
        assertEquals(2, offsets.rangeCount());
        assertEquals(2, offsets.offsetCount());

        // should extend left range
        assertTrue(offsets.add(7));
        assertEquals(2, offsets.rangeCount());
        assertEquals(3, offsets.offsetCount());

        // should extend right range
        assertTrue(offsets.add(9));
        assertEquals(2, offsets.rangeCount());
        assertEquals(4, offsets.offsetCount());

        // should close the gap and collapse all into one range
        assertTrue(offsets.add(8));
        assertEquals(1, offsets.rangeCount());
        assertEquals(5, offsets.offsetCount());
    }

    @Test
    public void testCreatesMoreGaps()
    {
        Offsets offsets = new Offsets(LOG_ID);

        assertTrue(offsets.add(10));
        assertEquals(1, offsets.rangeCount());
        assertEquals(1, offsets.offsetCount());

        // should prepend
        assertTrue(offsets.add(6));
        assertEquals(2, offsets.rangeCount());
        assertEquals(2, offsets.offsetCount());

        // should insert in the middle
        assertTrue(offsets.add(8));
        assertEquals(3, offsets.rangeCount());
        assertEquals(3, offsets.offsetCount());
    }

    @Test
    public void testRangeAppend()
    {
        Offsets offsets = new Offsets(LOG_ID);
        offsets.add(5, 7);
        TestConsumer consumer = new TestConsumer();

        // add overlapping range 1
        assertTrue(offsets.add(6, 8, consumer));
        assertEquals(1, offsets.rangeCount());
        assertEquals(4, offsets.offsetCount());
        consumer.assertOffsetsConsumed(8, 8).clear();

        // add overlapping range 2
        assertTrue(offsets.add(8, 9, consumer));
        assertEquals(1, offsets.rangeCount());
        assertEquals(5, offsets.offsetCount());
        consumer.assertOffsetsConsumed(9, 9).clear();

        // add adjacent range
        assertTrue(offsets.add(10, 12, consumer));
        assertEquals(1, offsets.rangeCount());
        assertEquals(8, offsets.offsetCount());
        consumer.assertOffsetsConsumed(10, 12).clear();

        // add disjoint range
        assertTrue(offsets.add(14, 16, consumer));
        assertEquals(2, offsets.rangeCount());
        assertEquals(11, offsets.offsetCount());
        consumer.assertOffsetsConsumed(14, 16).clear();

    }

    @Test
    public void testRangePrepend()
    {
        Offsets offsets = new Offsets(LOG_ID);
        offsets.add(10, 12);
        TestConsumer consumer = new TestConsumer();

        assertEquals(1, offsets.rangeCount());
        assertEquals(3, offsets.offsetCount());

        // add overlapping range 1
        assertTrue(offsets.add(9, 11, consumer));
        assertEquals(1, offsets.rangeCount());
        assertEquals(4, offsets.offsetCount());
        consumer.assertOffsetsConsumed(9, 9).clear();

        // add overlapping range 2
        assertTrue(offsets.add(8, 9, consumer));
        assertEquals(1, offsets.rangeCount());
        assertEquals(5, offsets.offsetCount());
        consumer.assertOffsetsConsumed(8, 8).clear();

        // add adjacent range
        assertTrue(offsets.add(6, 7, consumer));
        assertEquals(1, offsets.rangeCount());
        assertEquals(7, offsets.offsetCount());
        consumer.assertOffsetsConsumed(6, 7).clear();

        // add disjoint range
        assertTrue(offsets.add(0, 3, consumer));
        assertEquals(2, offsets.rangeCount());
        assertEquals(11, offsets.offsetCount());
        consumer.assertOffsetsConsumed(0, 3).clear();
    }

    @Test
    public void testRangeAddition()
    {
        Offsets offsets = new Offsets(LOG_ID);
        offsets.add(5, 7);

        assertEquals(1, offsets.rangeCount());
        assertEquals(3, offsets.offsetCount());
    }

    /**
     * adding ranges fully contained in existing ranges should noop
     */
    @Test
    public void testRangeInclusion()
    {
        Offsets offsets = new Offsets(LOG_ID);
        TestConsumer consumer = new TestConsumer();
        offsets.add(0, 3);
        offsets.add(7, 10);
        offsets.add(15, 17);

        assertEquals(3, offsets.rangeCount());
        assertEquals(11, offsets.offsetCount());

        // fully contained in first
        assertFalse(offsets.add(0, 2, consumer));
        assertFalse(offsets.add(1, 2, consumer));
        assertFalse(offsets.add(1, 3, consumer));
        assertFalse(offsets.add(0, 3, consumer));


        // fully contained in second
        assertFalse(offsets.add(7, 9, consumer));
        assertFalse(offsets.add(8, 9, consumer));
        assertFalse(offsets.add(8, 10, consumer));
        assertFalse(offsets.add(7, 10, consumer));

        // fully contained in third
        assertFalse(offsets.add(16, 16, consumer));
        assertFalse(offsets.add(16, 17, consumer));
        assertFalse(offsets.add(15, 16, consumer));
        assertFalse(offsets.add(15, 17, consumer));

        // nothing should have changed
        assertEquals(3, offsets.rangeCount());
        assertEquals(11, offsets.offsetCount());
        assertTrue(consumer.isEmpty());
    }

    @Test
    public void testRangeInsert()
    {
        Supplier<Offsets> sequenceIds = () -> {
            Offsets ids0 = new Offsets(LOG_ID);
            ids0.add(0, 3);
            ids0.add(7, 10);
            ids0.add(15, 17);

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.offsetCount());
            return ids0;
        };

        // disjoint insert
        {
            Offsets ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(12, 13, consumer));
            assertEquals(4, ids.rangeCount());
            assertEquals(13, ids.offsetCount());
            consumer.assertOffsetsConsumed(12, 13).clear();
        }

        // left adjacent insert
        {
            Offsets offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(5, 6, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(5, 6).clear();
        }

        // right adjacent insert
        {
            Offsets ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(11, 12, consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.offsetCount());
            consumer.assertOffsetsConsumed(11, 12).clear();
        }

        // both adjacent insert
        {
            Offsets offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(11, 14, consumer));
            assertEquals(2, offsets.rangeCount());
            assertEquals(15, offsets.offsetCount());
            consumer.assertOffsetsConsumed(11, 14).clear();
        }
    }


    @Test
    public void testRangeMerging()
    {
        Supplier<Offsets> sequenceIds = () -> {
            Offsets ids0 = new Offsets(LOG_ID);
            ids0.add(0, 3);
            ids0.add(7, 10);
            ids0.add(15, 17);

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.offsetCount());
            return ids0;
        };

        // left merge
        {
            Offsets offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(5, 8, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(5, 6).clear();
        }

        // right merge
        {
            Offsets offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(8, 12, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(11, 12).clear();
        }

        // right and left merge
        {
            Offsets offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(6, 11, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(6, 6, 11, 11).clear();
        }

        // 2 range merge
        {
            Offsets ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(2, 8, consumer));
            assertEquals(2, ids.rangeCount());
            assertEquals(14, ids.offsetCount());
            consumer.assertOffsetsConsumed(4, 6).clear();
        }
    }

    @Test
    public void appendTest()
    {
        Offsets ids = new Offsets(LOG_ID);
        ids.append(5);
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.offsetCount());

        ids.append(6);
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.offsetCount());

        ids.append(8);
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.offsetCount());

        // insert before tail
        try
        {
            ids.append(8);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // expected
            assertEquals(2, ids.rangeCount());
            assertEquals(3, ids.offsetCount());
        }

        // insert before tail
        try
        {
            ids.append(7);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // expected
            assertEquals(2, ids.rangeCount());
            assertEquals(3, ids.offsetCount());
        }
    }

    private static void testUnion(Offsets expected, Offsets a, Offsets b)
    {
        Assert.assertEquals(expected, Offsets.union(a, b));
        Assert.assertEquals(expected, new Offsets(Offsets.union(a.rangeIterator(), b.rangeIterator())));
        Assert.assertEquals(expected, Offsets.union(b, a));
        Assert.assertEquals(expected, new Offsets(Offsets.union(b.rangeIterator(), a.rangeIterator())));
    }

    @Test
    public void unionTest()
    {
        // empty
        testUnion(offsets(1, 1, 5, 6),
                  offsets(1, 1, 5, 6),
                  offsets());

        // left union
        testUnion(offsets(0, 3, 6, 10, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(6, 9));

        // left adjacent union
        testUnion(offsets(0, 3, 5, 10, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(5, 6));

        // right union
        testUnion(offsets(0, 3, 7, 11, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(9, 11));

        // right adjacent
        testUnion(offsets(0, 3, 7, 12, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(11, 12));

        // superset union
        testUnion(offsets(0, 3, 5, 12, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(5, 12));

        // join union
        testUnion(offsets(0, 10, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(2, 8));

        // disjoint
        testUnion(offsets(0, 10, 12, 13, 15, 17),
                  offsets(0, 3, 7, 10, 15, 17),
                  offsets(2, 8, 12, 13));

    }

    private static void testDifference(Offsets expected, Offsets a, Offsets b)
    {
        Offsets bPlus = b.copy();
        bPlus.add(50, 55);

        // check copy-remaining
        Assert.assertEquals(expected, Offsets.difference(a, b));
        Assert.assertEquals(expected, new Offsets(Offsets.difference(a.rangeIterator(), b.rangeIterator())));

        // check discarded tail
        Assert.assertEquals(expected, Offsets.difference(a, bPlus));
        Assert.assertEquals(expected, new Offsets(Offsets.difference(a.rangeIterator(), bPlus.rangeIterator())));
    }

    @Test
    public void differenceTest()
    {
        // empty input
        testDifference(offsets(1, 1),
                       offsets(1, 1),
                       offsets());

        testDifference(offsets(),
                       offsets(),
                       offsets(1, 1));

        // empty result
        testDifference(offsets(),
                       offsets(1, 1),
                       offsets(1, 1));


        // noop
        testDifference(offsets(0, 3, 7, 10, 15, 17),
                       offsets(0, 3, 7, 10, 15, 17),
                       offsets(5, 5));

        // noop before adjacent
        testDifference(offsets(0, 3, 7, 10, 15, 17),
                       offsets(0, 3, 7, 10, 15, 17),
                       offsets(5, 6));

        // noop after adjacent
        testDifference(offsets(0, 3, 7, 10, 15, 17),
                       offsets(0, 3, 7, 10, 15, 17),
                       offsets(4, 5));

        // before
        testDifference(offsets(0, 3, 9, 10, 15, 17),
                       offsets(0, 3, 7, 10, 15, 17),
                       offsets(6, 8));


        // after
        testDifference(offsets(0, 3, 7, 8, 15, 17),
                       offsets(0, 3, 7, 10, 15, 17),
                       offsets(9, 11));

        // both sides
        testDifference(offsets(0, 3, 8, 9, 15, 17),
                       offsets(0, 3, 7, 10, 15, 17),
                       offsets(6, 7, 10, 12));

        // multi-split
        testDifference(offsets(0, 3, 7, 8, 11, 11, 14, 15, 20, 22),
                       offsets(0, 3, 7, 15, 20, 22),
                       offsets(9, 10, 12, 13));

        // multi-split w/ edges
        testDifference(offsets(0, 3, 8, 9, 11, 13, 20, 22),
                       offsets(0, 3, 7, 15, 20, 22),
                       offsets(6, 7, 10, 10, 14, 16));

    }

    private static void testIntersection(Offsets expected, Offsets a, Offsets b)
    {
        Offsets aPlus = a.copy();
        aPlus.add(50, 55);
        Offsets bPlus = b.copy();
        bPlus.add(50, 55);

        Assert.assertEquals(expected, Offsets.intersection(a, b));
        Assert.assertEquals(expected, Offsets.intersection(aPlus, b));
        Assert.assertEquals(expected, Offsets.intersection(a, bPlus));
        Assert.assertEquals(expected, Offsets.intersection(b, a));
        Assert.assertEquals(expected, Offsets.intersection(bPlus, a));
        Assert.assertEquals(expected, Offsets.intersection(b, aPlus));
    }

    @Test
    public void intersectionTest()
    {
        // emtpy input
        testIntersection(offsets(),
                         offsets(0, 3, 7, 10, 15, 17),
                         offsets());

        // disjoint test
        testIntersection(offsets(),
                         offsets(0, 3, 7, 10, 15, 17),
                         offsets(4, 6, 11, 14));

        // left intersect test
        testIntersection(offsets(7, 9),
                         offsets(0, 3, 7, 10, 15, 17),
                         offsets(6, 9));


        // right intersect test
        testIntersection(offsets(8, 10),
                         offsets(0, 3, 7, 10, 15, 17),
                         offsets(8, 11));

        // superset test
        testIntersection(offsets(7, 10),
                         offsets(0, 3, 7, 10, 15, 17),
                         offsets(6, 11));

        // multi-intersect test
        testIntersection(offsets(8, 9, 11, 13, 15, 16),
                         offsets(0, 3, 7, 17, 25, 30),
                         offsets(8, 9, 11, 13, 15, 16));

        // multi-intersect test w/ ends
        testIntersection(offsets(7, 9, 11, 13, 16, 17),
                         offsets(0, 3, 7, 17, 25, 30),
                         offsets(6, 9, 11, 13, 16, 18));
    }

    @Test
    public void serializerTest() throws IOException
    {

        DataOutputBuffer buffer = new DataOutputBuffer();
        IVersionedSerializers.testSerde(buffer, Offsets.serializer, offsets(0, 3, 7, 10, 15, 17), MessagingService.current_version);
    }
}
