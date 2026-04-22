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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.filesystem.UnversionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;

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
            assertEquals(0, expected.length % 2);
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

    private static Offsets.Mutable offsets(int... bounds)
    {
        assertEquals(0, bounds.length % 2);
        Offsets.Mutable ids = new Offsets.Mutable(LOG_ID);
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

    private static void assertOffsetsEqual(Offsets expected, Offsets actual)
    {
        if (!Offsets.contentsEqual(expected, actual))
            throw new AssertionError("expected: " + expected.toString() + " but was: " + actual.toString());
    }


    @Test
    public void testEmptyAndAddExisting()
    {
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);
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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);

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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);

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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);

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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);

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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);
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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);
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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);
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
        Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID);
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
        Supplier<Offsets.Mutable> sequenceIds = () -> {
            Offsets.Mutable ids0 = new Offsets.Mutable(LOG_ID);
            ids0.add(0, 3);
            ids0.add(7, 10);
            ids0.add(15, 17);

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.offsetCount());
            return ids0;
        };

        // disjoint insert
        {
            Offsets.Mutable ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(12, 13, consumer));
            assertEquals(4, ids.rangeCount());
            assertEquals(13, ids.offsetCount());
            consumer.assertOffsetsConsumed(12, 13).clear();
        }

        // left adjacent insert
        {
            Offsets.Mutable offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(5, 6, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(5, 6).clear();
        }

        // right adjacent insert
        {
            Offsets.Mutable ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(11, 12, consumer));
            assertEquals(3, ids.rangeCount());
            assertEquals(13, ids.offsetCount());
            consumer.assertOffsetsConsumed(11, 12).clear();
        }

        // both adjacent insert
        {
            Offsets.Mutable offsets = sequenceIds.get();
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
        Supplier<Offsets.Mutable> sequenceIds = () -> {
            Offsets.Mutable ids0 = new Offsets.Mutable(LOG_ID);
            ids0.add(0, 3);
            ids0.add(7, 10);
            ids0.add(15, 17);

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.offsetCount());
            return ids0;
        };

        // left merge
        {
            Offsets.Mutable offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(5, 8, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(5, 6).clear();
        }

        // right merge
        {
            Offsets.Mutable offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(8, 12, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(11, 12).clear();
        }

        // right and left merge
        {
            Offsets.Mutable offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(6, 11, consumer));
            assertEquals(3, offsets.rangeCount());
            assertEquals(13, offsets.offsetCount());
            consumer.assertOffsetsConsumed(6, 6, 11, 11).clear();
        }

        // 2 range merge
        {
            Offsets.Mutable ids = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(ids.add(2, 8, consumer));
            assertEquals(2, ids.rangeCount());
            assertEquals(14, ids.offsetCount());
            consumer.assertOffsetsConsumed(4, 6).clear();
        }
    }

    @Test
    public void testMultiMerge()
    {
        Supplier<Offsets.Mutable> sequenceIds = () -> {
            Offsets.Mutable ids0 = new Offsets.Mutable(LOG_ID);
            ids0.add(0, 3);
            ids0.add(7, 10);
            ids0.add(15, 17);

            assertEquals(3, ids0.rangeCount());
            assertEquals(11, ids0.offsetCount());
            return ids0;
        };

        {
            // extends on the end
            Offsets.Mutable offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(0, 12, consumer));
            consumer.assertOffsetsConsumed(4, 6, 11, 12).clear();
            assertEquals(offsets(0, 12, 15, 17), offsets);
        }

        {
            // extends on the start
            Offsets.Mutable offsets = sequenceIds.get();
            TestConsumer consumer = new TestConsumer();

            assertTrue(offsets.add(5, 17, consumer));
            consumer.assertOffsetsConsumed(5, 6, 11, 14).clear();
            assertEquals(offsets(0, 3, 5, 17), offsets);
        }
    }

    @Test
    public void addTest()
    {
        Offsets.Mutable ids = new Offsets.Mutable(LOG_ID);
        ids.add(5);
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.offsetCount());

        ids.add(6);
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.offsetCount());

        ids.add(8);
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.offsetCount());
    }

    private static void testUnion(Offsets expected, Offsets a, Offsets b)
    {
        assertOffsetsEqual(expected, Offsets.Mutable.union(a, b));
        assertOffsetsEqual(expected, Offsets.Immutable.union(a, b));
        assertOffsetsEqual(expected, new Offsets.Mutable(Offsets.union(a.rangeIterator(), b.rangeIterator())));
        assertOffsetsEqual(expected, Offsets.Mutable.union(b, a));
        assertOffsetsEqual(expected, Offsets.Immutable.union(b, a));
        assertOffsetsEqual(expected, new Offsets.Mutable(Offsets.union(b.rangeIterator(), a.rangeIterator())));
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
        Offsets.Mutable bPlus = Offsets.Mutable.copy(b);
        bPlus.add(50, 55);

        // check copy-remaining
        assertOffsetsEqual(expected, Offsets.Mutable.difference(a, b));
        assertOffsetsEqual(expected, Offsets.Immutable.difference(a, b));
        assertOffsetsEqual(expected, new Offsets.Mutable(Offsets.difference(a.rangeIterator(), b.rangeIterator())));

        // check discarded tail
        assertOffsetsEqual(expected, Offsets.Mutable.difference(a, bPlus));
        assertOffsetsEqual(expected, Offsets.Immutable.difference(a, bPlus));
        assertOffsetsEqual(expected, new Offsets.Mutable(Offsets.difference(a.rangeIterator(), bPlus.rangeIterator())));
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
        Offsets.Mutable aPlus = Offsets.Mutable.copy(a);
        aPlus.add(50, 55);
        Offsets.Mutable bPlus = Offsets.Mutable.copy(b);
        bPlus.add(50, 55);

        assertOffsetsEqual(expected, Offsets.Mutable.intersection(a, b));
        assertOffsetsEqual(expected, Offsets.Immutable.intersection(a, b));
        assertOffsetsEqual(expected, Offsets.Mutable.intersection(aPlus, b));
        assertOffsetsEqual(expected, Offsets.Immutable.intersection(aPlus, b));
        assertOffsetsEqual(expected, Offsets.Mutable.intersection(a, bPlus));
        assertOffsetsEqual(expected, Offsets.Immutable.intersection(a, bPlus));
        assertOffsetsEqual(expected, Offsets.Mutable.intersection(b, a));
        assertOffsetsEqual(expected, Offsets.Immutable.intersection(b, a));
        assertOffsetsEqual(expected, Offsets.Mutable.intersection(bPlus, a));
        assertOffsetsEqual(expected, Offsets.Immutable.intersection(bPlus, a));
        assertOffsetsEqual(expected, Offsets.Mutable.intersection(b, aPlus));
        assertOffsetsEqual(expected, Offsets.Immutable.intersection(b, aPlus));
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
        UnversionedSerializers.testSerde(buffer, Offsets.serializer, Offsets.Immutable.copy(offsets(0, 3, 7, 10, 15, 17)));
    }

    private static List<ShortMutationId> ids(int... offsets)
    {
        List<ShortMutationId> ids = new ArrayList<>(offsets.length);
        for (int offset : offsets)
            ids.add(new ShortMutationId(LOG_ID, offset));
        return ids;
    }

    private static List<ShortMutationId> ids(Offsets offsets)
    {
        final List<ShortMutationId> ids = new ArrayList<>();
        for (ShortMutationId id : offsets)
            ids.add(id);
        return ids;
    }

    @Test
    public void iteratorTest()
    {
        Assert.assertEquals(ids(), ids(offsets()));
        Assert.assertEquals(ids(1, 2, 3), ids(offsets(1, 3)));
        Assert.assertEquals(ids(1, 2, 3, 5, 6, 7), ids(offsets(1, 3, 5, 7)));
    }

    @Test
    public void testRemoveFromEmpty()
    {
        testRemove(offsets(), 5, offsets());
    }

    @Test
    public void testRemoveFromSingleRange()
    {
        // before all ranges
        testRemove(offsets(5, 7), 4, offsets(5, 7));
        // after all ranges
        testRemove(offsets(5, 7), 8, offsets(5, 7));
        // start of range
        testRemove(offsets(5, 7), 5, offsets(6, 7));
        // end of range
        testRemove(offsets(5, 7), 7, offsets(5, 6));
        // middle of range
        testRemove(offsets(5, 7), 6, offsets(5, 5, 7, 7));
        // single element range
        testRemove(offsets(5, 5), 5, offsets());
    }

    @Test
    public void testRemoveGeneric()
    {
        // before all ranges
        testRemove(offsets(5, 7, 9, 9, 11, 13), 4, offsets(5, 7, 9, 9, 11, 13));
        // after all ranges
        testRemove(offsets(5, 7, 9, 9, 11, 13), 14, offsets(5, 7, 9, 9, 11, 13));
        // between two ranges
        testRemove(offsets(5, 7, 9, 9, 11, 13), 8, offsets(5, 7, 9, 9, 11, 13));
        // start of a range
        testRemove(offsets(5, 7, 9, 9, 11, 13), 11, offsets(5, 7, 9, 9, 12, 13));
        // end of a range
        testRemove(offsets(5, 7, 9, 9, 11, 13), 7, offsets(5, 6, 9, 9, 11, 13));
        // middle of a range
        testRemove(offsets(5, 7, 9, 9, 11, 13), 12, offsets(5, 7, 9, 9, 11, 11, 13, 13));
        // single element range
        testRemove(offsets(5, 7, 9, 9, 11, 13), 9, offsets(5, 7, 11, 13));

        // single element ranges at different positions
        testRemove(offsets(5, 5, 10, 12), 5, offsets(10, 12));
        testRemove(offsets(1, 3, 5, 5, 7, 9), 5, offsets(1, 3, 7, 9));

        // range splitting at boundaries
        testRemove(offsets(5, 10), 7, offsets(5, 6, 8, 10));
        testRemove(offsets(5, 10, 15, 17), 7, offsets(5, 6, 8, 10, 15, 17));
    }

    private void testRemove(Offsets.Mutable from, int toRemove, Offsets.Mutable expectedAfter)
    {
        from.remove(toRemove);
        assertOffsetsEqual(expectedAfter, from);
    }

    @Test
    public void testRemoveWithExactSizedArray()
    {
        {
            Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID, new int[]{ 10, 11 }, 2);
            offsets.remove(10);
            assertOffsetsEqual(offsets(11, 11), offsets);
        }

        {
            Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID, new int[]{ 10, 11 }, 2);
            offsets.remove(11);
            assertOffsetsEqual(offsets(10, 10), offsets);
        }

        {
            Offsets.Mutable offsets = new Offsets.Mutable(LOG_ID, new int[]{ 11, 11 }, 2);
            offsets.remove(11);
            assertOffsetsEqual(offsets(), offsets);
        }
    }

    @Test
    public void asListFromListRoundTripTest()
    {
        for (Offsets.Mutable offsets : new Offsets.Mutable[] { offsets(), offsets(1, 2), offsets(1, 3, 7, 9) })
            assertOffsetsEqual(offsets, Offsets.fromList(LOG_ID, offsets.asList()));
    }

    @Test
    public void testContainsRange()
    {
        {
            Offsets.Mutable offsets = offsets();
            assertFalse(offsets.containsRange(0, 1));
        }

        {
            Offsets.Mutable offsets = offsets(2, 4);

            assertTrue(offsets.containsRange(2, 4));
            assertTrue(offsets.containsRange(3, 4));
            assertTrue(offsets.containsRange(2, 3));

            assertFalse(offsets.containsRange(1, 1));
            assertFalse(offsets.containsRange(1, 2));
            assertFalse(offsets.containsRange(1, 3));
            assertFalse(offsets.containsRange(1, 4));

            assertFalse(offsets.containsRange(2, 5));
            assertFalse(offsets.containsRange(3, 5));
            assertFalse(offsets.containsRange(4, 5));
            assertFalse(offsets.containsRange(5, 5));
        }

        {
            Offsets.Mutable offsets = offsets(2, 4, 6, 8);

            assertTrue(offsets.containsRange(2, 4));
            assertTrue(offsets.containsRange(6, 8));

            assertFalse(offsets.containsRange(0, 2));
            assertFalse(offsets.containsRange(3, 5));
            assertFalse(offsets.containsRange(4, 6));
            assertFalse(offsets.containsRange(5, 7));
            assertFalse(offsets.containsRange(7, 9));
            assertFalse(offsets.containsRange(9, 9));
        }
    }
}
