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

import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;

import static org.apache.cassandra.index.sai.utils.LongIterator.convert;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.CONCAT;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.INTERSECTION;
import static org.apache.cassandra.index.sai.utils.RangeIterator.Builder.IteratorType.UNION;

public class RangeConcatIteratorTest extends AbstractRangeIteratorTest
{
    @Test
    public void testValidation()
    {
        try
        {
            buildConcat(build(1L, 4L), build(2L, 3L));
            fail("Flows for a merging concatenation must not contain one another.");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        try
        {
            buildConcat(build(1L, 4L), build(2L, 5L));
            fail("Minimum for flow must not be included in exclusive range of previous flow.");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        // allow min boundary included
        RangeIterator concat = buildConcat(build(1L, 4L), build(4L, 5L));
        assertEquals(convert(1L, 4L, 4L, 5L), convert(concat));

        try
        {
            buildConcat(build(1L, 4L), build(0L, 3L));
            fail("Maximum for flow must not be included in exclusive range of previous flow.");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        try
        {
            buildConcat(build(2L, 4L), build(0L, 1L));
            fail("Flows for merging concatenation must be sorted.");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        // with empty flow
        concat = buildConcat(build(), build(0L, 1L));
        assertEquals(convert(0L, 1L), convert(concat));

        concat = buildConcat(build(0L, 1L), build());
        assertEquals(convert(0L, 1L), convert(concat));

        concat = buildConcat(build(), build(0L, 1L), build());
        assertEquals(convert(0L, 1L), convert(concat));

        concat = buildConcat(build(), build(0L, 1L),
                              build(), build(2L, 3L));
        assertEquals(convert(0L, 1L, 2L, 3L), convert(concat));
    }

    @Test
    public void testSingleIterator()
    {
        RangeIterator origin = build(1L, 2L, 4L, 9L );
        RangeIterator concat = buildConcat(origin);
        assertSame(origin, concat);
        assertEquals(convert(1L, 2L, 4L, 9L), convert(concat));
    }

    @Test
    public void testNoOverlappingSortedRanges()
    {
        RangeIterator concat = buildConcat(build(1L, 2L, 3L),
                                           build(4L, 5L),
                                           build(7L, 8L, 9L, 10L));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 7L, 8L, 9L, 10L), convert(concat));
    }

    @Test
    public void testMinMaxAndCount()
    {
        RangeIterator.Builder builder = getConcatBuilder();

        builder.add(build(1L, 2L, 3L));
        builder.add(build(4L, 5L, 6L));
        builder.add(build(7L, 8L, 9L));

        assertEquals(9L, (long) builder.getMaximum());
        assertEquals(9L, builder.getTokenCount());

        RangeIterator tokens = builder.build();

        assertNotNull(tokens);
        assertEquals(1L, (long)  tokens.getMinimum());
        assertEquals(9L, (long)  tokens.getMaximum());
        assertEquals(9L, tokens.getCount());

        for (long i = 1; i < 10; i++)
        {
            assertTrue(tokens.hasNext());
            assertEquals(i, tokens.next().getLong());
        }

        assertFalse(tokens.hasNext());
        assertEquals(1L, (long) tokens.getMinimum());
    }

    @Test
    public void testSkipTo()
    {
        // flow is single use..
        Supplier<RangeIterator> init = () ->  buildConcat(build(1L, 2L, 3L),
                                                          build( 4L, 5L, 6L),
                                                          build( 7L, 8L, 9L));

        RangeIterator tokens;

        tokens = init.get();
        tokens.skipTo(5L);
        assertTrue(tokens.hasNext());
        assertEquals(5L, tokens.next().getLong());

        tokens = init.get();
        tokens.skipTo(7L);
        assertTrue(tokens.hasNext());
        assertEquals(7L, tokens.next().getLong());

        tokens = init.get();
        tokens.skipTo(2L);
        tokens.skipTo(5L);
        tokens.skipTo(10L);
        assertFalse(tokens.hasNext());
        assertEquals(1L, (long) tokens.getMinimum());
        assertEquals(9L, (long) tokens.getMaximum());
    }

    @Test
    public void testSkipToWithGaps()
    {
        // flow is single use..
        Supplier<RangeIterator> init = () ->  buildConcat(build(1L, 2L, 3L), build(4L, 6L), build(8L, 9L));

        RangeIterator tokens;

        tokens = init.get();
        tokens.skipTo(5L);
        assertTrue(tokens.hasNext());
        assertEquals(6L, tokens.next().getLong());

        tokens = init.get();
        tokens.skipTo(7L);
        assertTrue(tokens.hasNext());
        assertEquals(8L, tokens.next().getLong());

        tokens = init.get();
        tokens.skipTo(2L);
        tokens.skipTo(5L);
        tokens.skipTo(10L);
        assertFalse(tokens.hasNext());
        assertEquals(1L, (long) tokens.getMinimum());
        assertEquals(9L, (long) tokens.getMaximum());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        RangeIterator concatA = buildConcat(build(1L, 3L, 5L), build(8L, 10L, 12L));
        RangeIterator concatB = buildConcat(build(7L, 9L, 11L), build(12L, 14L, 16L));

        assertEquals(convert(1L, 3L, 5L, 7L, 8L, 9L, 10L, 11L, 12L, 14L, 16L), convert(buildUnion(concatA, concatB)));
    }

    @Test
    public void testEmptyThenManyNonEmpty()
    {
        final RangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));

        RangeIterator range = builder.build();

        assertEquals(10L, (long) range.getMinimum());
        assertEquals(19L, (long) range.getMaximum());
        assertTrue(range.hasNext());
        assertEquals(10, range.getCount());
    }

    @Test
    public void testEmptyThenSingleNonEmpty()
    {
        RangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        builder.add(build(10));

        RangeIterator range = builder.build();
        assertEquals(10L, (long) range.getMinimum());
        assertEquals(10L, (long) range.getMaximum());
        assertTrue(range.hasNext());
        assertEquals(1, range.getCount());
    }

    @Test
    public void testManyNonEmptyThenEmpty()
    {
        final RangeIterator.Builder builder = getConcatBuilder();

        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));
        builder.add(build());

        RangeIterator range = builder.build();
        assertEquals(10L, (long) range.getMinimum());
        assertEquals(19L, (long) range.getMaximum());
        assertTrue(range.hasNext());
        assertEquals(10, range.getCount());
    }

    @Test
    public void testSingleNonEmptyThenEmpty()
    {
        RangeIterator.Builder builder = getConcatBuilder();

        builder.add(build(10));
        builder.add(build());

        RangeIterator range = builder.build();
        assertEquals(10L, (long) range.getMinimum());
        assertEquals(10L, (long) range.getMaximum());
        assertTrue(range.hasNext());
        assertEquals(1, range.getCount());
    }

    @Test
    public void testEmptyNonEmptyEmpty()
    {
        final RangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));
        builder.add(build());

        RangeIterator range = builder.build();
        assertEquals(10L, (long) range.getMinimum());
        assertEquals(19L, (long) range.getMaximum());
        assertTrue(range.hasNext());
        assertEquals(10, range.getCount());
    }

    @Test
    public void testNonEmptyEmptyNonEmpty()
    {
        final RangeIterator.Builder builder = getConcatBuilder();

        IntStream.range(10, 15).forEach(value -> builder.add(build(value)));
        builder.add(build());
        IntStream.range(15, 20).forEach(value -> builder.add(build(value)));

        RangeIterator range = builder.build();
        assertEquals(10L, (long) range.getMinimum());
        assertEquals(19L, (long) range.getMaximum());
        assertTrue(range.hasNext());
        assertEquals(10, range.getCount());
    }

    @Test
    public void testConcatOfIntersection()
    {
        // concat of two non-intersected intersections
        RangeIterator intersectionA = buildIntersection(build(1L, 2L, 3L), build(4L, 5L, 6L));
        RangeIterator intersectionB = buildIntersection(build(6L, 7L, 8L), build(9L, 10L, 11L));
        assertEquals(convert(), convert(buildConcat(intersectionA, intersectionB)));

        // concat of two intersected intersections
        intersectionA = buildIntersection(build( 1L, 2L, 3L), build( 2L, 3L, 4L));
        intersectionB = buildIntersection(build( 6L, 7L, 8L), build( 7L, 8L, 9L));
        assertEquals(convert(2L, 3L, 7L, 8L), convert(buildConcat(intersectionA, intersectionB)));

        // concat of one intersected intersection and one non-intersected intersection
        intersectionA = buildIntersection(build( 1L, 2L, 3L), build( 2L, 3L, 4L));
        intersectionB = buildIntersection(build( 6L, 7L, 8L), build( 10L));
        assertEquals(convert(2L, 3L), convert(buildConcat(intersectionA, intersectionB)));

        // concat of one non-intersected intersection and one intersected intersection
        intersectionA = buildIntersection(build( 6L, 7L, 8L), build( 10L));
        intersectionB = buildIntersection(build( 1L, 2L, 3L), build( 2L, 3L, 4L));
        assertEquals(convert(2L, 3L), convert(buildConcat(intersectionA, intersectionB)));
    }


    @Test
    public void testIntersectionOfConcat()
    {
        RangeIterator rangeA = build(1L, 2L, 3L);
        RangeIterator rangeB = build(4L, 5L, 6L);
        RangeIterator rangeC = build(7L);
        RangeIterator rangeD = build(8L);
        RangeIterator rangeE = build(9L);
        RangeIterator concatA = buildConcat(rangeA, rangeB, rangeC, rangeD, rangeE);

        rangeA = build( 1L, 3L);
        rangeB = build( 5L, 7L, 9L);
        RangeIterator concatB = buildConcat(rangeA, rangeB);

        assertEquals(convert(1L, 3L, 5L, 7L, 9L), convert(buildIntersection(concatA, concatB)));
    }

    @Test
    public void testConcatOnError()
    {
        assertOnError(buildOnErrorA(CONCAT, arr(1L, 2L, 3L), arr(4L, 5L, 6L)));
        assertOnError(buildOnErrorB(CONCAT, arr( 1L, 2L, 3L), arr(4L)));
    }

    @Test
    public void testConcatOfUnionsOnError()
    {
        RangeIterator unionA = buildUnion(arr(1L, 2L, 3L), arr(4L));
        RangeIterator unionB = buildOnErrorB(UNION, arr(6L), arr(8L, 9L));
        assertOnError(buildConcat(unionA, unionB));

        unionA = buildOnErrorA(UNION, arr( 1L, 2L, 3L), arr( 4L));
        unionB = buildUnion(arr( 5L), arr( 5L, 6L));
        assertOnError(buildConcat(unionA, unionB));
    }

    @Test
    public void testConcatOfIntersectionsOnError()
    {
        RangeIterator intersectionA = buildOnErrorA(INTERSECTION, arr(1L, 2L, 3L), arr(2L, 3L, 4L));
        RangeIterator intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(7L, 8L, 9L));
        assertOnError(buildConcat(intersectionA, intersectionB));

        intersectionA = buildIntersection(arr( 1L, 2L, 3L), arr( 2L, 3L, 4L));
        intersectionB = buildOnErrorB(INTERSECTION, arr( 6L, 7L, 8L, 9L, 10L), arr(  7L, 8L, 9L));
        assertOnError(buildConcat(intersectionA, intersectionB));
    }

    @Test
    public void testDuplicatedElementsInTheSameFlow()
    {
        // In real case, we should not have duplicated elements from the same PostingListRangeIterator
        RangeIterator rangeA = build(1L, 2L, 3L, 3L, 4L, 4L);
        RangeIterator rangeB = build(6L, 6L, 7L, 7L);
        RangeIterator rangeC = build(8L, 8L);
        RangeIterator concatA = buildConcat(rangeA, rangeB, rangeC);

        assertEquals(convert(1L, 2L, 3L, 3L, 4L, 4L, 6L, 6L, 7L, 7L, 8L, 8L), convert(concatA));
    }

    @Test
    public void testOverlappingBoundaries()
    {
        RangeIterator rangeA = build(1L, 2L, 3L);
        RangeIterator rangeB = build(3L, 4L, 6L, 7L);
        RangeIterator rangeC = build(7L, 8L);
        RangeIterator rangeD = build(8L, 9L);
        RangeIterator rangeE = build(9L);
        RangeIterator rangeF = build(9L);
        RangeIterator rangeG = build(9L, 10L);
        RangeIterator concatA = buildConcat(rangeA, rangeB, rangeC, rangeD, rangeE, rangeF, rangeG);

        assertEquals(convert(1L, 2L, 3L, 3L, 4L, 6L, 7L, 7L, 8L, 8L, 9L, 9L, 9L, 9L, 10L), convert(concatA));
    }

    @Test
    public void testDuplicatedElementsAndOverlappingBoundaries()
    {
        RangeIterator rangeA = build(1L, 2L, 2L, 3L);
        RangeIterator rangeB = build(3L, 4L, 4L, 6L, 6L, 7L);
        assertEquals(convert(1L, 2L, 2L, 3L, 3L, 4L, 4L, 6L, 6L, 7L), convert(buildConcat(rangeA, rangeB)));

        rangeA = build(1L, 2L, 2L, 3L);
        rangeB = build(3L);
        RangeIterator rangeC = build(3L, 4L, 4L, 6L, 6L, 7L);
        RangeIterator rangeD = build(7L, 7L, 8L);
        RangeIterator rangeE = build(8L, 9L, 9L);
        RangeIterator rangeF = build(9L, 10L);
        RangeIterator concatA = buildConcat(rangeA, rangeB, rangeC, rangeD, rangeE, rangeF);

        assertEquals(convert(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 6L, 6L, 7L, 7L, 7L, 8L, 8L, 9L, 9L, 9L, 10L), convert(concatA));
    }

    @Test
    public void testDuplicateElementsAtBoundary()
    {
        // Duplicate on the right:
        RangeIterator rangeA = build(1L, 2L, 3L);
        RangeIterator rangeB = build(3L, 3L, 4L, 5L);
        assertEquals(convert(1L, 2L, 3L, 3L, 3L, 4L, 5L), convert(buildConcat(rangeA, rangeB)));

        // Duplicate on the left:
        rangeA = build(1L, 2L, 3L, 3L);
        rangeB = build(3L, 4L, 5L);
        assertEquals(convert(1L, 2L, 3L, 3L, 3L, 4L, 5L), convert(buildConcat(rangeA, rangeB)));

        // Duplicates on both sides:
        rangeA = build(1L, 2L, 3L, 3L);
        rangeB = build(3L, 3L, 4L, 5L);
        assertEquals(convert(1L, 2L, 3L, 3L, 3L, 3L, 4L, 5L), convert(buildConcat(rangeA, rangeB)));
    }

    private RangeIterator.Builder getConcatBuilder()
    {
        return RangeConcatIterator.builder();
    }
}
