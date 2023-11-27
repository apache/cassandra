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
package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sai.utils.LongIterator.convert;

public class RangeUnionIteratorTest extends AbstractRangeIteratorTest
{
    @Test
    public void testNoOverlappingValues()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testOverlappingValues()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        List<Long> values = convert(builder.build());

        Assert.assertEquals(values.toString(), convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), values);
    }

    @Test
    public void testNoOverlappingRanges()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), convert(builder.build()));
    }

    @Test
    public void testTwoIteratorsWithSingleValues()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L }));
        builder.add(new LongIterator(new long[] { 1L }));

        Assert.assertEquals(convert(1L), convert(builder.build()));
    }

    @Test
    public void testDifferentSizeIterators()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L, 12L, 13L }));
        builder.add(new LongIterator(new long[] { 1L, 7L, 14L, 15 }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 12L, 13L, 14L, 15L), convert(builder.build()));
    }

    @Test
    public void testRandomSequences()
    {
        for (int testIteration = 0; testIteration < 16; testIteration++)
        {
            var p = createRandom(nextInt(1, 20));
            validateWithSkipping(p.left, p.right);
        }
    }

    static Pair<RangeIterator, long[]> createRandom(int nRanges)
    {
        long[][] values = new long[nRanges][];
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        var allValues = new HashSet<Long>();
        // add a random number of random values
        for (int i = 0; i < values.length; i++)
        {
            int partLength = nextInt(1, 500);
            var part = new HashSet<Long>(partLength);
            for (int j = 0; j < partLength; j++)
            {
                long m = nextLong(0, 1024);
                part.add(m);
                allValues.add(m);
            }

            // all of the parts have to be sorted to mimic SSTable
            builder.add(new LongIterator(part.stream().mapToLong(Long::longValue).sorted().toArray()));
        }

        long[] totalOrdering = allValues.stream().mapToLong(Long::longValue).sorted().toArray();
        RangeIterator tokens = builder.build();
        return Pair.create(tokens, totalOrdering);
    }

    @Test
    public void testMinMaxAndCount()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(9L, builder.getMaximum().token().getLongValue());
        Assert.assertEquals(9L, builder.getTokenCount());

        RangeIterator tokens = builder.build();

        Assert.assertNotNull(tokens);
        Assert.assertEquals(1L, tokens.getMinimum().token().getLongValue());
        Assert.assertEquals(9L, tokens.getMaximum().token().getLongValue());
        Assert.assertEquals(9L, tokens.getMaxKeys());

        for (long i = 1; i < 10; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            Assert.assertEquals(i, tokens.next().token().getLongValue());
        }

        Assert.assertFalse(tokens.hasNext());
        Assert.assertEquals(1L, tokens.getMinimum().token().getLongValue());
    }

    @Test
    public void testBuilder()
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        Assert.assertEquals(0L, builder.getTokenCount());
        Assert.assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(1L, builder.getMinimum().token().getLongValue());
        Assert.assertEquals(9L, builder.getMaximum().token().getLongValue());
        Assert.assertEquals(9L, builder.getTokenCount());
        Assert.assertEquals(3L, builder.rangeCount());
        Assert.assertFalse(builder.statistics.isDisjoint());

        Assert.assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        Assert.assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        Assert.assertEquals(7L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        RangeIterator tokens = RangeUnionIterator.build(new ArrayList<RangeIterator>()
        {{
            add(new LongIterator(new long[]{1L, 2L, 4L}));
            add(new LongIterator(new long[]{3L, 5L, 6L}));
        }});

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        var emptyTokens = RangeUnionIterator.builder().build();
        Assert.assertEquals(0, emptyTokens.getMaxKeys());

        builder = RangeUnionIterator.builder();
        Assert.assertEquals(0L, builder.add((RangeIterator) null).rangeCount());
        Assert.assertEquals(0L, builder.add((List<RangeIterator>) null).getTokenCount());
        Assert.assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        var single = new LongIterator(new long[] { 1L, 2L, 3L });
        var range = RangeIntersectionIterator.<PrimaryKey>builder().add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        Assert.assertEquals(range, single);
    }

    @Test
    public void testSkipTo()
    {
        var builder = RangeUnionIterator.<PrimaryKey>builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 3L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 6L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        RangeIterator tokens = builder.build();
        Assert.assertNotNull(tokens);

        tokens.skipTo(LongIterator.fromToken(5L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(5L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(7L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(7L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(10L));
        Assert.assertFalse(tokens.hasNext());
        Assert.assertEquals(1L, tokens.getMinimum().token().getLongValue());
        Assert.assertEquals(9L, tokens.getMaximum().token().getLongValue());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        RangeUnionIterator.Builder builderA = RangeUnionIterator.builder();

        builderA.add(new LongIterator(new long[] { 1L, 3L, 5L }));
        builderA.add(new LongIterator(new long[] { 8L, 10L, 12L }));

        RangeUnionIterator.Builder builderB = RangeUnionIterator.builder();

        builderB.add(new LongIterator(new long[] { 7L, 9L, 11L }));
        builderB.add(new LongIterator(new long[] { 2L, 4L, 6L }));

        RangeIterator union = RangeUnionIterator.build(Arrays.asList(builderA.build(), builderB.build()));
        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), convert(union));
    }

    @Test
    public void testRangeIterator()
    {
        LongIterator tokens = new LongIterator(new long[] { 0L, 1L, 2L, 3L });

        Assert.assertEquals(0L, tokens.getMinimum().token().getLongValue());
        Assert.assertEquals(3L, tokens.getMaximum().token().getLongValue());

        for (int i = 0; i <= 3; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            Assert.assertEquals(i, tokens.peek().token().getLongValue());
            Assert.assertEquals(i, tokens.next().token().getLongValue());
        }

        tokens = new LongIterator(new long[] { 0L, 1L, 3L, 5L });

        tokens.skipTo(LongIterator.fromToken(2L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(3L, tokens.peek().token().getLongValue());
        Assert.assertEquals(3L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(5L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(5L, tokens.peek().token().getLongValue());
        Assert.assertEquals(5L, tokens.next().token().getLongValue());

        LongIterator empty = new LongIterator(new long[0]);

        empty.skipTo(LongIterator.fromToken(3L));
        Assert.assertFalse(empty.hasNext());
    }

    @Test
    public void emptyRangeTest() {
        RangeIterator.Builder builder;
        RangeIterator range;
        // empty, then non-empty
        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());

        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(1, range.getMaxKeys());

        // non-empty, then empty
        builder = RangeUnionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(10, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());

        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(1, range.getMaxKeys());

        // empty, then non-empty then empty again
        builder = RangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());

        // non-empty, empty, then non-empty again
        builder = RangeUnionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());
    }

    // SAI specific tests
    @Test
    public void testUnionOfIntersection()
    {
        // union of two non-intersected intersections
        RangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(4L, 5L, 6L));
        RangeIterator intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(9L, 10L, 11L));

        RangeIterator union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(), convert(union));

        // union of two intersected intersections
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(7L, 8L, 9L));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L, 7L, 8L), convert(union));
        assertEquals(RangeUnionIterator.class, union.getClass());

        // union of one intersected intersection and one non-intersected intersection
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L ));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(10L ));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L), convert(union));
    }

    @Test
    public void testUnionOfRandom()
    {
        for (int testIteration = 0; testIteration < 16; testIteration++)
        {
            var allValues = new HashSet<Long>();
            var builder = RangeUnionIterator.builder();
            for (int i = 0; i < nextInt(2, 3); i++)
            {
                var p = createRandomIterator();
                builder.add(p.left);
                allValues.addAll(Arrays.stream(p.right).boxed().collect(Collectors.toList()));
            }
            long[] totalOrdered = allValues.stream().mapToLong(Long::longValue).sorted().toArray();
            validateWithSkipping(builder.build(), totalOrdered);
        }
    }
}
