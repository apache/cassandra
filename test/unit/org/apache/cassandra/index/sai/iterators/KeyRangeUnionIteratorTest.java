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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class KeyRangeUnionIteratorTest extends AbstractKeyRangeIteratorTester
{
    @Test
    public void testNoOverlappingValues()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testOverlappingValues()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        List<Long> values = convert(builder.build());

        assertEquals(values.toString(), convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), values);
    }

    @Test
    public void testNoOverlappingRanges()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), convert(builder.build()));
    }

    @Test
    public void testTwoIteratorsWithSingleValues()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 1L }));
        builder.add(new LongIterator(new long[] { 1L }));

        assertEquals(convert(1L), convert(builder.build()));
    }

    @Test
    public void testDifferentSizeIterators()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L, 12L, 13L }));
        builder.add(new LongIterator(new long[] { 1L, 7L, 14L, 15 }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 8L, 9L, 10L }));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 12L, 13L, 14L, 15L), convert(builder.build()));
    }

    @Test
    public void testRandomSequences()
    {
        long[][] values = new long[getRandom().nextIntBetween(1, 20)][];
        int numTests = getRandom().nextIntBetween(10, 20);

        for (int tests = 0; tests < numTests; tests++)
        {
            KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);
            int totalCount = 0;

            for (int i = 0; i < values.length; i++)
            {
                long[] part = new long[getRandom().nextIntBetween(1, 500)];
                for (int j = 0; j < part.length; j++)
                    part[j] = getRandom().nextLong();

                // all the parts have to be sorted to mimic SSTable
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
            KeyRangeIterator tokens = builder.build();

            Assert.assertNotNull(tokens);
            while (tokens.hasNext())
                assertEquals(totalOrdering[count++], tokens.next().token().getLongValue());

            assertEquals(totalCount, count);
        }
    }

    @Test
    public void testMinMaxAndCount()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getCount());

        KeyRangeIterator tokens = builder.build();

        Assert.assertNotNull(tokens);
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(9L, tokens.getCount());

        for (long i = 1; i < 10; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            assertEquals(i, tokens.next().token().getLongValue());
        }

        Assert.assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
    }

    @Test
    public void testBuilder()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        assertEquals(0L, builder.getCount());
        assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        assertEquals(1L, builder.getMinimum().token().getLongValue());
        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getCount());
        assertEquals(3L, builder.rangeCount());

        assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        assertEquals(7L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        KeyRangeIterator tokens = KeyRangeUnionIterator.build(new ArrayList<KeyRangeIterator>()
        {{
            add(new LongIterator(new long[]{1L, 2L, 4L}));
            add(new LongIterator(new long[]{3L, 5L, 6L}));
        }});

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        KeyRangeIterator emptyTokens = KeyRangeUnionIterator.builder(16).build();
        assertEquals(0, emptyTokens.getCount());

        builder = KeyRangeUnionIterator.builder(16);
        assertEquals(0L, builder.add((KeyRangeIterator) null).rangeCount());
        assertEquals(0L, builder.add((List<KeyRangeIterator>) null).getCount());
        assertEquals(0L, builder.add(LongIterator.newEmptyIterator()).rangeCount());

        KeyRangeIterator single = new LongIterator(new long[] { 1L, 2L, 3L });
        KeyRangeIterator range = KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE).add(single).build();

        // because build should return first element if it's only one instead of building yet another iterator
        assertEquals(range, single);
    }

    @Test
    public void testSkipTo()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder(16);

        builder.add(new LongIterator(new long[]{1L, 2L, 3L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 6L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        KeyRangeIterator tokens = builder.build();
        Assert.assertNotNull(tokens);

        tokens.skipTo(LongIterator.fromToken(5L));
        Assert.assertTrue(tokens.hasNext());
        assertEquals(5L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(7L));
        Assert.assertTrue(tokens.hasNext());
        assertEquals(7L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.fromToken(10L));
        Assert.assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        KeyRangeUnionIterator.Builder builderA = KeyRangeUnionIterator.builder(16);

        builderA.add(new LongIterator(new long[] { 1L, 3L, 5L }));
        builderA.add(new LongIterator(new long[] { 8L, 10L, 12L }));

        KeyRangeUnionIterator.Builder builderB = KeyRangeUnionIterator.builder(16);

        builderB.add(new LongIterator(new long[] { 7L, 9L, 11L }));
        builderB.add(new LongIterator(new long[] { 2L, 4L, 6L }));

        KeyRangeIterator union = KeyRangeUnionIterator.build(Arrays.asList(builderA.build(), builderB.build()));
        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), convert(union));
    }

    @Test
    public void testRangeIterator()
    {
        try (LongIterator tokens = new LongIterator(new long[] { 0L, 1L, 2L, 3L }))
        {
            assertEquals(0L, tokens.getMinimum().token().getLongValue());
            assertEquals(3L, tokens.getMaximum().token().getLongValue());

            for (int i = 0; i <= 3; i++)
            {
                Assert.assertTrue(tokens.hasNext());
                assertEquals(i, tokens.getCurrent().token().getLongValue());
                assertEquals(i, tokens.next().token().getLongValue());
            }
        }

        try (LongIterator tokens = new LongIterator(new long[] { 0L, 1L, 3L, 5L }))
        {
            assertEquals(3L, tokens.skipTo(LongIterator.fromToken(2L)).token().getLongValue());
            Assert.assertTrue(tokens.hasNext());
            assertEquals(3L, tokens.getCurrent().token().getLongValue());
            assertEquals(3L, tokens.next().token().getLongValue());

            assertEquals(5L, tokens.skipTo(LongIterator.fromToken(5L)).token().getLongValue());
            Assert.assertTrue(tokens.hasNext());
            assertEquals(5L, tokens.getCurrent().token().getLongValue());
            assertEquals(5L, tokens.next().token().getLongValue());
        }

        try (LongIterator empty = LongIterator.newEmptyIterator())
        {
            Assert.assertNull(empty.skipTo(LongIterator.fromToken(3L)));
            Assert.assertFalse(empty.hasNext());
        }
    }

    @Test
    public void emptyRangeTest()
    {
        KeyRangeIterator.Builder builder;
        KeyRangeIterator range;
        // empty, then non-empty
        builder = KeyRangeUnionIterator.builder(16);
        builder.add(LongIterator.newEmptyIterator());
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());

        builder = KeyRangeUnionIterator.builder(16);
        builder.add(LongIterator.newEmptyIterator());
        builder.add(new LongIterator(new long[] {10}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(1, range.getCount());

        // non-empty, then empty
        builder = KeyRangeUnionIterator.builder(16);
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(LongIterator.newEmptyIterator());
        range = builder.build();
        assertEquals(10, range.getMinimum().token().getLongValue());
        assertEquals(19, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());

        builder = KeyRangeUnionIterator.builder(16);
        builder.add(new LongIterator(new long[] {10}));
        builder.add(LongIterator.newEmptyIterator());
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(1, range.getCount());

        // empty, then non-empty then empty again
        builder = KeyRangeUnionIterator.builder(16);
        builder.add(LongIterator.newEmptyIterator());
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(LongIterator.newEmptyIterator());
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());

        // non-empty, empty, then non-empty again
        builder = KeyRangeUnionIterator.builder(16);
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(LongIterator.newEmptyIterator());
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        assertEquals(10, range.getCount());
    }

    // SAI specific tests
    @Test
    public void testUnionOfIntersection()
    {
        // union of two non-intersected intersections
        KeyRangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(4L, 5L, 6L));
        KeyRangeIterator intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(9L, 10L, 11L));

        KeyRangeIterator union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(), convert(union));

        // union of two intersected intersections
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(7L, 8L, 9L));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L, 7L, 8L), convert(union));
        assertSame(KeyRangeUnionIterator.class, union.getClass());

        // union of one intersected intersection and one non-intersected intersection
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L ));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(10L ));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L), convert(union));
    }

    @Test
    public void testUnionOnError()
    {
        assertOnError(buildOnError(this::buildUnion, arr(1L, 3L, 4L ), arr(7L, 8L)));
        assertOnError(buildOnErrorA(this::buildUnion, arr(1L, 3L, 4L ), arr(4L, 5L)));
        assertOnError(buildOnErrorB(this::buildUnion, arr(1L), arr(2)));
    }

    @Test
    public void testUnionOfIntersectionsOnError()
    {
        KeyRangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L, 6L), arr(2L, 3L, 6L));
        KeyRangeIterator intersectionB = buildOnErrorA(this::buildIntersection, arr(2L, 4L, 6L), arr(5L, 6L, 7L, 9L));
        assertOnError(buildUnion(intersectionA, intersectionB));

        intersectionA = buildOnErrorB(this::buildIntersection, arr(1L, 2L, 3L, 4L, 5L), arr(2L, 3L, 5L));
        intersectionB = buildIntersection(arr(2L, 4L, 5L), arr(5L, 6L, 7L));
        assertOnError(buildUnion(intersectionA, intersectionB));
    }

    @Test
    public void testUnionOfUnionsOnError()
    {
        KeyRangeIterator unionA = buildUnion(arr(1L, 2L, 3L, 6L), arr(6L, 7L, 8L));
        KeyRangeIterator unionB = buildOnErrorA(this::buildUnion, arr(2L, 4L, 6L), arr (6L, 7L, 9L));
        assertOnError(buildUnion(unionA, unionB));

        unionA = buildOnErrorB(this::buildUnion, arr(1L, 2L, 3L), arr(3L, 7L, 8L));
        unionB = buildUnion(arr(2L, 4L, 5L), arr (5L, 7L, 9L));
        assertOnError(buildUnion(unionA, unionB));
    }

    @Test
    public void testUnionOfMergingOnError()
    {
        KeyRangeIterator mergingA = buildConcat(arr(1L, 2L, 3L, 6L), arr(6L, 7L, 8L));
        KeyRangeIterator mergingB = buildOnErrorA(this::buildConcat, arr(2L, 4L, 6L), arr (6L, 7L, 9L));
        assertOnError(buildUnion(mergingA, mergingB));

        mergingA = buildOnErrorB(this::buildConcat, arr(1L, 2L, 3L), arr(3L, 7L, 8L));
        mergingB = buildConcat(arr(2L, 4L, 5L), arr (5L, 7L, 9L));
        assertOnError(buildUnion(mergingA, mergingB));
    }
}
