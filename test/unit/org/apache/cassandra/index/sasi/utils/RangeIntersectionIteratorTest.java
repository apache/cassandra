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
package org.apache.cassandra.index.sasi.utils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator.Strategy;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator.LookupIntersectionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator.BounceIntersectionIterator;
import org.apache.cassandra.io.util.FileUtils;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.index.sasi.utils.LongIterator.convert;

public class RangeIntersectionIteratorTest
{
    @Test
    public void testNoOverlappingValues()
    {
        for (Strategy strategy : Strategy.values())
            testNoOverlappingValues(strategy);
    }

    private void testNoOverlappingValues(Strategy strategy)
    {
        RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(), convert(builder.build()));

        builder = RangeIntersectionIterator.builder(strategy);
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 6L }));

        RangeIterator<Long, Token> range = builder.build();

        Assert.assertNotNull(range);
        Assert.assertFalse(range.hasNext());

        builder = RangeIntersectionIterator.builder(strategy);
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 0L, 10L, 12L }));

        range = builder.build();

        Assert.assertNotNull(range);
        Assert.assertFalse(range.hasNext());
    }

    @Test
    public void testOverlappingValues()
    {
        for (Strategy strategy : Strategy.values())
            testOverlappingValues(strategy);
    }

    private void testOverlappingValues(Strategy strategy)
    {
        RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(4L, 6L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        for (Strategy strategy : Strategy.values())
            testSingleIterator(strategy);
    }

    private void testSingleIterator(Strategy strategy)
    {
        RangeIntersectionIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testSkipTo()
    {
        for (Strategy strategy : Strategy.values())
            testSkipTo(strategy);
    }

    private void testSkipTo(Strategy strategy)
    {
        RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L, 9L, 10L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L, 7L, 10L, 12L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 7L, 9L, 10L }));

        RangeIterator<Long, Token> range = builder.build();
        Assert.assertNotNull(range);

        // first let's skipTo something before range
        Assert.assertEquals(4L, (long) range.skipTo(3L).get());
        Assert.assertEquals(4L, (long) range.getCurrent());

        // now let's skip right to the send value
        Assert.assertEquals(6L, (long) range.skipTo(5L).get());
        Assert.assertEquals(6L, (long) range.getCurrent());

        // now right to the element
        Assert.assertEquals(7L, (long) range.skipTo(7L).get());
        Assert.assertEquals(7L, (long) range.getCurrent());
        Assert.assertEquals(7L, (long) range.next().get());

        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10L, (long) range.getCurrent());

        // now right after the last element
        Assert.assertNull(range.skipTo(11L));
        Assert.assertFalse(range.hasNext());
    }

    @Test
    public void testMinMaxAndCount()
    {
        for (Strategy strategy : Strategy.values())
            testMinMaxAndCount(strategy);
    }

    private void testMinMaxAndCount(Strategy strategy)
    {
        RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);

        builder.add(new LongIterator(new long[]{1L, 2L, 9L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 9L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        Assert.assertEquals(9L, (long) builder.getMaximum());
        Assert.assertEquals(9L, builder.getTokenCount());

        RangeIterator<Long, Token> tokens = builder.build();

        Assert.assertNotNull(tokens);
        Assert.assertEquals(7L, (long) tokens.getMinimum());
        Assert.assertEquals(9L, (long) tokens.getMaximum());
        Assert.assertEquals(9L, tokens.getCount());

        Assert.assertEquals(convert(9L), convert(builder.build()));
    }

    @Test
    public void testBuilder()
    {
        for (Strategy strategy : Strategy.values())
            testBuilder(strategy);
    }

    private void testBuilder(Strategy strategy)
    {
        RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        Assert.assertEquals(0L, builder.getTokenCount());
        Assert.assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        Assert.assertEquals(6L, (long) builder.getMinimum());
        Assert.assertEquals(6L, (long) builder.getMaximum());
        Assert.assertEquals(9L, builder.getTokenCount());
        Assert.assertEquals(3L, builder.rangeCount());
        Assert.assertFalse(builder.statistics.isDisjoint());

        Assert.assertEquals(1L, (long) builder.ranges.poll().getMinimum());
        Assert.assertEquals(4L, (long) builder.ranges.poll().getMinimum());
        Assert.assertEquals(6L, (long) builder.ranges.poll().getMinimum());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        Assert.assertEquals(convert(6L), convert(builder.build()));

        builder = RangeIntersectionIterator.builder(strategy);
        builder.add(new LongIterator(new long[]{ 1L, 5L, 6L }));
        builder.add(new LongIterator(new long[]{ 3L, 5L, 6L }));

        RangeIterator<Long, Token> tokens = builder.build();

        Assert.assertEquals(convert(5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        RangeIterator emptyTokens = RangeIntersectionIterator.builder(strategy).build();
        Assert.assertEquals(0, emptyTokens.getCount());

        builder = RangeIntersectionIterator.builder(strategy);
        Assert.assertEquals(0L, builder.add((RangeIterator<Long, Token>) null).rangeCount());
        Assert.assertEquals(0L, builder.add((List<RangeIterator<Long, Token>>) null).getTokenCount());
        Assert.assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        RangeIterator<Long, Token> single = new LongIterator(new long[] { 1L, 2L, 3L });
        RangeIterator<Long, Token> range = RangeIntersectionIterator.<Long, Token>builder().add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        Assert.assertEquals(range, single);

        // Make a difference between empty and null ranges.
        builder = RangeIntersectionIterator.builder(strategy);
        builder.add(new LongIterator(new long[] {}));
        Assert.assertEquals(0L, builder.rangeCount());
        builder.add(single);
        Assert.assertEquals(1L, builder.rangeCount());
        range = builder.build();
        Assert.assertEquals(0, range.getCount());

        // disjoint case
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));

        Assert.assertTrue(builder.statistics.isDisjoint());

        RangeIterator<Long, Token> disjointIntersection = builder.build();
        Assert.assertNotNull(disjointIntersection);
        Assert.assertFalse(disjointIntersection.hasNext());

    }

    @Test
    public void emptyRangeTest() {
        RangeIterator.Builder<Long, Token> builder;

        // empty, then non-empty
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());

        // non-empty, then empty
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));

        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // empty, then non-empty then empty again
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // non-empty, empty, then non-empty again
        builder = RangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        assertEmpty(builder.build());

        builder = RangeIntersectionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());
    }

    public static void assertEmpty(RangeIterator<Long, Token> range)
    {
        Assert.assertNull(range.getMinimum());
        Assert.assertNull(range.getMaximum());
        Assert.assertFalse(range.hasNext());
        Assert.assertEquals(0, range.getCount());
    }

    @Test
    public void testClose() throws IOException
    {
        for (Strategy strategy : Strategy.values())
            testClose(strategy);
    }

    private void testClose(Strategy strategy) throws IOException
    {
        RangeIterator<Long, Token> tokens = RangeIntersectionIterator.<Long, Token>builder(strategy)
                                            .add(new LongIterator(new long[] { 1L, 2L, 3L }))
                                            .build();

        Assert.assertNotNull(tokens);
        tokens.close();
    }

    @Test
    public void testIsOverlapping()
    {
        RangeIterator<Long, Token> rangeA, rangeB;

        rangeA = new LongIterator(new long[] { 1L, 5L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 6L });
        Assert.assertTrue(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 1L, 4L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertFalse(RangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 6L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 4L });
        Assert.assertFalse(RangeIterator.isOverlapping(rangeA, rangeB));
    }

    @Test
    public void testIntersectionOfRandomRanges()
    {
        for (Strategy strategy : Strategy.values())
            testIntersectionOfRandomRanges(strategy);
    }

    private void testIntersectionOfRandomRanges(Strategy strategy)
    {
        for (int attempt = 0; attempt < 16; attempt++)
        {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            final int maxRanges = random.nextInt(2, 16);

            // generate randomize ranges
            long[][] ranges = new long[maxRanges][];
            for (int i = 0; i < ranges.length; i++)
            {
                int rangeSize = random.nextInt(16, 512);
                LongSet range = new LongHashSet(rangeSize);

                for (int j = 0; j < rangeSize; j++)
                    range.add(random.nextLong(0, 100));

                ranges[i] = range.toArray();
                Arrays.sort(ranges[i]);
            }

            List<Long> expected = new ArrayList<>();
            // determine unique tokens which intersect every range
            for (long token : ranges[0])
            {
                boolean intersectsAll = true;
                for (int i = 1; i < ranges.length; i++)
                {
                    if (Arrays.binarySearch(ranges[i], token) < 0)
                    {
                        intersectsAll = false;
                        break;
                    }
                }

                if (intersectsAll)
                    expected.add(token);
            }

            RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder(strategy);
            for (long[] range : ranges)
                builder.add(new LongIterator(range));

            Assert.assertEquals(expected, convert(builder.build()));
        }
    }

    @Test
    public void testIteratorPeeking()
    {
        RangeIterator.Builder<Long, Token> builder = RangeIntersectionIterator.builder();

        // iterator with only one element
        builder.add(new LongIterator(new long[] { 10L }));

        // iterator with 150 elements (lookup is going to be advantageous over bound in this case)
        long[] tokens = new long[150];
        for (int i = 0; i < tokens.length; i++)
            tokens[i] = i;

        builder.add(new LongIterator(tokens));

        RangeIterator<Long, Token> intersection = builder.build();

        Assert.assertNotNull(intersection);
        Assert.assertEquals(LookupIntersectionIterator.class, intersection.getClass());

        Assert.assertTrue(intersection.hasNext());
        Assert.assertEquals(convert(10L), convert(intersection));

        builder = RangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 3L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 1L, 2L, 5L, 6L }));

        intersection = builder.build();

        // in the situation when there is a similar number of elements inside ranges
        // ping-pong (bounce) intersection is preferred as it covers gaps quicker then linear scan + lookup.
        Assert.assertNotNull(intersection);
        Assert.assertEquals(BounceIntersectionIterator.class, intersection.getClass());

        Assert.assertTrue(intersection.hasNext());
        Assert.assertEquals(convert(1L, 5L), convert(intersection));
    }
}
