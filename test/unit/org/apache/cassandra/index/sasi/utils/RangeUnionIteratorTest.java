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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.io.util.FileUtils;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.index.sasi.utils.LongIterator.convert;

public class RangeUnionIteratorTest
{
    @Test
    public void testNoOverlappingValues()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testOverlappingValues()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        List<Long> values = convert(builder.build());

        Assert.assertEquals(values.toString(), convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), values);
    }

    @Test
    public void testNoOverlappingRanges()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), convert(builder.build()));
    }

    @Test
    public void testTwoIteratorsWithSingleValues()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L }));
        builder.add(new LongIterator(new long[] { 1L }));

        Assert.assertEquals(convert(1L), convert(builder.build()));
    }

    @Test
    public void testDifferentSizeIterators()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L, 12L, 13L }));
        builder.add(new LongIterator(new long[] { 1L, 7L, 14L, 15 }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 12L, 13L, 14L, 15L), convert(builder.build()));
    }

    @Test
    public void testRandomSequences()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        long[][] values = new long[random.nextInt(1, 20)][];
        int numTests = random.nextInt(10, 20);

        for (int tests = 0; tests < numTests; tests++)
        {
            RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
            int totalCount = 0;

            for (int i = 0; i < values.length; i++)
            {
                long[] part = new long[random.nextInt(1, 500)];
                for (int j = 0; j < part.length; j++)
                    part[j] = random.nextLong();

                // all of the parts have to be sorted to mimic SSTable
                Arrays.sort(part);

                values[i] = part;
                builder.add(new LongIterator(part));
                totalCount += part.length;
            }

            long[] totalOrdering = new long[totalCount];
            int index = 0;

            for (long[] part : values)
            {
                for (long value : part)
                    totalOrdering[index++] = value;
            }

            Arrays.sort(totalOrdering);

            int count = 0;
            RangeIterator<Long, Token> tokens = builder.build();

            Assert.assertNotNull(tokens);
            while (tokens.hasNext())
                Assert.assertEquals(totalOrdering[count++], (long) tokens.next().get());

            Assert.assertEquals(totalCount, count);
        }
    }

    @Test
    public void testMinMaxAndCount()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(9L, (long) builder.getMaximum());
        Assert.assertEquals(9L, builder.getTokenCount());

        RangeIterator<Long, Token> tokens = builder.build();

        Assert.assertNotNull(tokens);
        Assert.assertEquals(1L, (long) tokens.getMinimum());
        Assert.assertEquals(9L, (long) tokens.getMaximum());
        Assert.assertEquals(9L, tokens.getCount());

        for (long i = 1; i < 10; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            Assert.assertEquals(i, (long) tokens.next().get());
        }

        Assert.assertFalse(tokens.hasNext());
        Assert.assertEquals(1L, (long) tokens.getMinimum());
    }

    @Test
    public void testBuilder()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        Assert.assertEquals(0L, builder.getTokenCount());
        Assert.assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(1L, (long) builder.getMinimum());
        Assert.assertEquals(9L, (long) builder.getMaximum());
        Assert.assertEquals(9L, builder.getTokenCount());
        Assert.assertEquals(3L, builder.rangeCount());
        Assert.assertFalse(builder.statistics.isDisjoint());

        Assert.assertEquals(1L, (long) builder.ranges.poll().getMinimum());
        Assert.assertEquals(4L, (long) builder.ranges.poll().getMinimum());
        Assert.assertEquals(7L, (long) builder.ranges.poll().getMinimum());

        RangeIterator<Long, Token> tokens = RangeUnionIterator.build(new ArrayList<RangeIterator<Long, Token>>()
        {{
            add(new LongIterator(new long[]{1L, 2L, 4L}));
            add(new LongIterator(new long[]{3L, 5L, 6L}));
        }});

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        RangeIterator emptyTokens = RangeUnionIterator.builder().build();
        Assert.assertEquals(0, emptyTokens.getCount());

        builder = RangeUnionIterator.builder();
        Assert.assertEquals(0L, builder.add((RangeIterator<Long, Token>) null).rangeCount());
        Assert.assertEquals(0L, builder.add((List<RangeIterator<Long, Token>>) null).getTokenCount());
        Assert.assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        RangeIterator<Long, Token> single = new LongIterator(new long[] { 1L, 2L, 3L });
        RangeIterator<Long, Token> range = RangeIntersectionIterator.<Long, Token>builder().add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        Assert.assertEquals(range, single);
    }

    @Test
    public void testSkipTo()
    {
        RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 3L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 6L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        RangeIterator<Long, Token> tokens = builder.build();
        Assert.assertNotNull(tokens);

        tokens.skipTo(5L);
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(5L, (long) tokens.next().get());

        tokens.skipTo(7L);
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(7L, (long) tokens.next().get());

        tokens.skipTo(10L);
        Assert.assertFalse(tokens.hasNext());
        Assert.assertEquals(1L, (long) tokens.getMinimum());
        Assert.assertEquals(9L, (long) tokens.getMaximum());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        RangeUnionIterator.Builder<Long, Token> builderA = RangeUnionIterator.builder();

        builderA.add(new LongIterator(new long[] { 1L, 3L, 5L }));
        builderA.add(new LongIterator(new long[] { 8L, 10L, 12L }));

        RangeUnionIterator.Builder<Long, Token> builderB = RangeUnionIterator.builder();

        builderB.add(new LongIterator(new long[] { 7L, 9L, 11L }));
        builderB.add(new LongIterator(new long[] { 2L, 4L, 6L }));

        RangeIterator<Long, Token> union = RangeUnionIterator.build(Arrays.asList(builderA.build(), builderB.build()));
        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), convert(union));
    }

    @Test
    public void testRangeIterator()
    {
        LongIterator tokens = new LongIterator(new long[] { 0L, 1L, 2L, 3L });

        Assert.assertEquals(0L, (long) tokens.getMinimum());
        Assert.assertEquals(3L, (long) tokens.getMaximum());

        for (int i = 0; i <= 3; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            Assert.assertEquals(i, (long) tokens.getCurrent());
            Assert.assertEquals(i, (long) tokens.next().get());
        }

        tokens = new LongIterator(new long[] { 0L, 1L, 3L, 5L });

        Assert.assertEquals(3L, (long) tokens.skipTo(2L).get());
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(3L, (long) tokens.getCurrent());
        Assert.assertEquals(3L, (long) tokens.next().get());

        Assert.assertEquals(5L, (long) tokens.skipTo(5L).get());
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(5L, (long) tokens.getCurrent());
        Assert.assertEquals(5L, (long) tokens.next().get());

        LongIterator empty = new LongIterator(new long[0]);

        Assert.assertNull(empty.skipTo(3L));
        Assert.assertFalse(empty.hasNext());
    }

    @Test
    public void emptyRangeTest() {
        RangeIterator.Builder<Long, Token> builder;
        RangeIterator<Long, Token> range;
        // empty, then non-empty
        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        Assert.assertEquals(Long.valueOf(10), range.getMinimum());
        Assert.assertEquals(Long.valueOf(19), range.getMaximum());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getCount());

        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        range = builder.build();
        Assert.assertEquals(Long.valueOf(10), range.getMinimum());
        Assert.assertEquals(Long.valueOf(10), range.getMaximum());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(1, range.getCount());

        // non-empty, then empty
        builder = RangeUnionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(Long.valueOf(10), range.getMinimum());
        Assert.assertEquals(Long.valueOf(19), range.getMaximum());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getCount());

        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(Long.valueOf(10), range.getMinimum());
        Assert.assertEquals(Long.valueOf(10), range.getMaximum());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(1, range.getCount());

        // empty, then non-empty then empty again
        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(Long.valueOf(10), range.getMinimum());
        Assert.assertEquals(Long.valueOf(19), range.getMaximum());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getCount());

        // non-empty, empty, then non-empty again
        builder = RangeUnionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        Assert.assertEquals(Long.valueOf(10), range.getMinimum());
        Assert.assertEquals(Long.valueOf(19), range.getMaximum());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getCount());
    }
}
