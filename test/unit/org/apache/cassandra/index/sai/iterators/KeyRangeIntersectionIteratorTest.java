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
package org.apache.cassandra.index.sai.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KeyRangeIntersectionIteratorTest extends AbstractKeyRangeIteratorTester
{
    @Test
    public void testNoOverlappingValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        assertEquals(convert(), convert(builder.build()));

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 6L }));

        KeyRangeIterator range = builder.build();

        assertNotNull(range);
        assertFalse(range.hasNext());

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 0L, 10L, 12L }));

        range = builder.build();

        assertNotNull(range);
        assertFalse(range.hasNext());
    }

    @Test
    public void testOverlappingValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        assertEquals(convert(4L, 6L), convert(builder.build()));
    }

    @Test
    public void testSameValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));

        assertEquals(convert(1L, 2L, 3L, 4L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testSkipTo()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L, 9L, 10L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L, 7L, 10L, 12L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 7L, 9L, 10L }));

        KeyRangeIterator range = builder.build();
        assertNotNull(range);

        // first let's skipTo something before range
        assertEquals(4L, range.skipTo(LongIterator.fromToken(3L)).token().getLongValue());
        assertEquals(4L, range.getCurrent().token().getLongValue());

        // now let's skip right to the send value
        assertEquals(6L, range.skipTo(LongIterator.fromToken(5L)).token().getLongValue());
        assertEquals(6L, range.getCurrent().token().getLongValue());

        // now right to the element
        assertEquals(7L, range.skipTo(LongIterator.fromToken(7L)).token().getLongValue());
        assertEquals(7L, range.getCurrent().token().getLongValue());
        assertEquals(7L, range.next().token().getLongValue());

        assertTrue(range.hasNext());
        assertEquals(10L, range.getCurrent().token().getLongValue());

        // now right after the last element
        assertNull(range.skipTo(LongIterator.fromToken(11L)));
        assertFalse(range.hasNext());
    }

    @Test
    public void testMinMaxAndCount()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        builder.add(new LongIterator(new long[]{1L, 2L, 9L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 9L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(3L, builder.getCount());

        KeyRangeIterator tokens = builder.build();

        assertNotNull(tokens);
        assertEquals(7L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(3L, tokens.getCount());

        assertEquals(convert(9L), convert(builder.build()));
    }

    @Test
    public void testBuilder()
    {
        KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);

        assertNull(builder.getMinimum());
        assertNull(builder.getMaximum());
        assertEquals(0L, builder.getCount());
        assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(6L, builder.getMinimum().token().getLongValue());
        assertEquals(6L, builder.getMaximum().token().getLongValue());
        assertEquals(3L, builder.getCount());
        assertEquals(3L, builder.rangeCount());
        assertFalse(builder.isDisjoint());

        assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        assertEquals(6L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(convert(6L), convert(builder.build()));

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(new LongIterator(new long[]{ 1L, 5L, 6L }));
        builder.add(new LongIterator(new long[]{ 3L, 5L, 6L }));

        KeyRangeIterator tokens = builder.build();

        assertEquals(convert(5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        KeyRangeIterator emptyTokens = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE).build();
        assertEquals(0, emptyTokens.getCount());

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        assertEquals(0L, builder.add((KeyRangeIterator) null).rangeCount());
        assertEquals(0L, builder.add((List<KeyRangeIterator>) null).getCount());
        assertEquals(0L, builder.add(LongIterator.newEmptyIterator()).rangeCount());

        KeyRangeIterator single = new LongIterator(new long[] { 1L, 2L, 3L });
        KeyRangeIterator range = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE).add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        assertEquals(range, single);

        // Make a difference between empty and null ranges.
        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(LongIterator.newEmptyIterator());
        assertEquals(0L, builder.rangeCount());
        builder.add(single);
        assertEquals(1L, builder.rangeCount());
        range = builder.build();
        assertEquals(0, range.getCount());

        // disjoint case
        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));

        assertTrue(builder.isDisjoint());

        KeyRangeIterator disjointIntersection = builder.build();
        assertNotNull(disjointIntersection);
        assertFalse(disjointIntersection.hasNext());

    }

    @Test
    public void emptyRangeTest()
    {
        KeyRangeIterator.Builder builder;

        // empty, then non-empty
        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(LongIterator.newEmptyIterator());
        builder.add(new LongIterator(new long[] {10}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(LongIterator.newEmptyIterator());
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());

        // non-empty, then empty
        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(new LongIterator(new long[] {10}));
        builder.add(LongIterator.newEmptyIterator());
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));

        builder.add(LongIterator.newEmptyIterator());
        assertEmpty(builder.build());

        // empty, then non-empty then empty again
        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(LongIterator.newEmptyIterator());
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(LongIterator.newEmptyIterator());
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(LongIterator.newEmptyIterator());
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(LongIterator.newEmptyIterator());
        assertEmpty(builder.build());

        // non-empty, empty, then non-empty again
        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(LongIterator.newEmptyIterator());
        builder.add(new LongIterator(new long[] {0, 10}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(LongIterator.newEmptyIterator());
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());
    }

    public static void assertEmpty(KeyRangeIterator range)
    {
        assertNull(range.getMinimum());
        assertNull(range.getMaximum());
        assertFalse(range.hasNext());
        assertEquals(0, range.getCount());
    }

    @Test
    public void testClose() throws IOException
    {
        KeyRangeIterator tokens = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE)
                                                              .add(new LongIterator(new long[] { 1L, 2L, 3L }))
                                                              .build();

        assertNotNull(tokens);
        tokens.close();
    }

    @Test
    public void testIsOverlapping()
    {
        KeyRangeIterator rangeA, rangeB;

        rangeA = new LongIterator(new long[] { 1L, 5L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        assertFalse(KeyRangeIntersectionIterator.isDisjoint(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 6L });
        assertFalse(KeyRangeIntersectionIterator.isDisjoint(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        assertFalse(KeyRangeIntersectionIterator.isDisjoint(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 1L, 4L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        assertTrue(KeyRangeIntersectionIterator.isDisjoint(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 6L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 4L });
        assertTrue(KeyRangeIntersectionIterator.isDisjoint(rangeA, rangeB));
    }

    @Test
    public void testIntersectionOfRandomRanges()
    {
        for (int attempt = 0; attempt < 16; attempt++)
        {
            final int maxRanges = nextInt(2, 16);

            // generate randomize ranges
            long[][] ranges = new long[maxRanges][];
            for (int i = 0; i < ranges.length; i++)
            {
                int rangeSize = nextInt(16, 512);
                LongSet range = new LongHashSet(rangeSize);

                for (int j = 0; j < rangeSize; j++)
                    range.add(nextLong(0, 100));

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

            KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE);
            for (long[] range : ranges)
                builder.add(new LongIterator(range));

            assertEquals(expected, convert(builder.build()));
        }
    }

    // SAI specific tests
    @Test
    public void testSelectiveIntersection()
    {
        KeyRangeIterator intersection = buildSelectiveIntersection(2,
                                                                   arr(1L, 4L, 6L, 7L),
                                                                   arr(1L, 4L, 5L, 6L),
                                                                   arr(4L, 6L, 8L, 9L, 10L)); // skipped

        assertEquals(convert(1L, 4L, 6L), convert(intersection));

        intersection = buildSelectiveIntersection(1,
                                                  arr(2L, 4L, 6L),
                                                  arr(1L, 4L, 5L, 6L),       // skipped
                                                  arr(4L, 6L, 8L, 9L, 10L)); // skipped

        assertEquals(convert(2L, 4L, 6L), convert(intersection));
    }

    @Test
    public void testIntersectionOfUnionsOnError()
    {
        // intersection of two unions
        KeyRangeIterator unionA = buildOnErrorB(this::buildUnion, arr(1L, 2L, 3L), arr(5L, 6L, 7L));
        KeyRangeIterator unionB = buildUnion(arr(2L, 4L, 6L), arr(5L, 7L, 9L));
        assertOnError(buildIntersection(unionA, unionB));

        // intersection of union and intersection
        KeyRangeIterator unionC = buildOnErrorB(this::buildUnion, arr(2L, 4L, 6L), arr(5L, 6L, 9L));
        KeyRangeIterator intersectionA = buildIntersection(arr(3L, 4L, 6L, 9L), arr(2L, 3L, 6L, 9L));
        assertOnError(buildIntersection(unionC, intersectionA));
    }

    @Test
    public void testIntersectionOfIntersectionsOnError()
    {
        KeyRangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L, 6L), arr(2L, 3L, 6L));
        KeyRangeIterator intersectionB = buildOnErrorA(this::buildIntersection, arr(2L, 4L, 6L), arr(5L, 6L, 7L, 9L));
        assertOnError(buildIntersection(intersectionA, intersectionB));

        intersectionA = buildOnErrorB(this::buildIntersection, arr(1L, 2L, 3L, 4L, 5L), arr(2L, 3L, 4L));
        intersectionB = buildIntersection(arr(1L, 2L, 3L, 4L, 6L), arr(2L, 3L, 4L, 7L, 9L));
        assertOnError(buildIntersection(intersectionA, intersectionB));

        intersectionA = buildOnError(this::buildIntersection, arr(1L, 2L, 3L, 5L), arr(3L, 4L));
        intersectionB = buildIntersection(arr(1L, 2L, 3L, 4L, 6L), arr(2L, 3L, 4L, 7L, 9L));
        assertOnError(buildIntersection(intersectionA, intersectionB));
    }
}
