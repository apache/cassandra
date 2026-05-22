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
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyRangeConcatIteratorTest extends AbstractKeyRangeIteratorTest
{
    @Test
    public void testValidation()
    {
        // Iterators being merged via concatanation must not include each other
        assertThatThrownBy(() -> buildConcat(build(1L, 4L), build(2L, 3L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 2));

        // Iterators being merged via concatanation must not overlap
        assertThatThrownBy(() -> buildConcat(build(1L, 4L), build(2L, 5L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 2));

        // allow min boundary included
        KeyRangeIterator concat = buildConcat(build(1L, 4L), build(4L, 5L));
        assertEquals(convert(1L, 4L, 4L, 5L), convert(concat));

        assertThatThrownBy(() -> buildConcat(build(1L, 4L), build(0L, 3L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 0));

        // Iterators being merged via concatanation must be sorted
        assertThatThrownBy(() -> buildConcat(build(2L, 4L), build(0L, 1L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 0));

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
        KeyRangeIterator origin = build(1L, 2L, 4L, 9L );
        KeyRangeIterator concat = buildConcat(origin);
        assertSame(origin, concat);
        assertEquals(convert(1L, 2L, 4L, 9L), convert(concat));
    }

    @Test
    public void testNoOverlappingSortedRanges()
    {
        KeyRangeIterator concat = buildConcat(build(1L, 2L, 3L),
                                              build(4L, 5L),
                                              build(7L, 8L, 9L, 10L));

        assertEquals(convert(1L, 2L, 3L, 4L, 5L, 7L, 8L, 9L, 10L), convert(concat));
    }

    @Test
    public void testMinMaxAndCount()
    {
        KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build(1L, 2L, 3L));
        builder.add(build(4L, 5L, 6L));
        builder.add(build(7L, 8L, 9L));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(9L, builder.getTokenCount());

        KeyRangeIterator tokens = builder.build();

        assertNotNull(tokens);
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(9L, tokens.getMaxKeys());

        for (long i = 1; i < 10; i++)
        {
            assertTrue(tokens.hasNext());
            assertEquals(i, tokens.next().token().getLongValue());
        }

        assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
    }

    @Test
    public void testSkipTo()
    {
        // flow is single use..
        Supplier<KeyRangeIterator> init = () ->  buildConcat(build(1L, 2L, 3L),
                                                             build( 4L, 5L, 6L),
                                                             build( 7L, 8L, 9L));

        KeyRangeIterator tokens;

        tokens = init.get();
        tokens.skipTo(LongIterator.makeKey(5));
        assertTrue(tokens.hasNext());
        assertEquals(5L, tokens.next().token().getLongValue());

        tokens = init.get();
        tokens.skipTo(LongIterator.makeKey(7L));
        assertTrue(tokens.hasNext());
        assertEquals(7L, tokens.next().token().getLongValue());

        tokens = init.get();
        tokens.skipTo(LongIterator.makeKey(2L));
        tokens.skipTo(LongIterator.makeKey(5L));
        tokens.skipTo(LongIterator.makeKey(10L));
        assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
    }

    @Test
    public void testSkipToWithGaps()
    {
        // flow is single use..
        Supplier<KeyRangeIterator> init = () ->  buildConcat(build(1L, 2L, 3L), build(4L, 6L), build(8L, 9L));

        KeyRangeIterator tokens;

        tokens = init.get();
        tokens.skipTo(LongIterator.makeKey(5L));
        assertTrue(tokens.hasNext());
        assertEquals(6L, tokens.next().token().getLongValue());

        tokens = init.get();
        tokens.skipTo(LongIterator.makeKey(7L));
        assertTrue(tokens.hasNext());
        assertEquals(8L, tokens.next().token().getLongValue());

        tokens = init.get();
        tokens.skipTo(LongIterator.makeKey(2L));
        tokens.skipTo(LongIterator.makeKey(5L));
        tokens.skipTo(LongIterator.makeKey(10L));
        assertFalse(tokens.hasNext());
        assertEquals(1L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        KeyRangeIterator concatA = buildConcat(build(1L, 3L, 5L), build(8L, 10L, 12L));
        KeyRangeIterator concatB = buildConcat(build(7L, 9L, 11L), build(12L, 14L, 16L));

        assertEquals(convert(1L, 3L, 5L, 7L, 8L, 9L, 10L, 11L, 12L, 14L, 16L), convert(buildUnion(concatA, concatB)));
    }

    @Test
    public void testEmptyThenManyNonEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));

        KeyRangeIterator range = builder.build();

        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        assertTrue(range.hasNext());
        assertEquals(10, range.getMaxKeys());
    }

    @Test
    public void testEmptyThenSingleNonEmpty()
    {
        KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        builder.add(build(10));

        KeyRangeIterator range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(10L, range.getMaximum().token().getLongValue());
        assertTrue(range.hasNext());
        assertEquals(1, range.getMaxKeys());
    }

    @Test
    public void testManyNonEmptyThenEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));
        builder.add(build());

        KeyRangeIterator range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        assertTrue(range.hasNext());
        assertEquals(10, range.getMaxKeys());
    }

    @Test
    public void testSingleNonEmptyThenEmpty()
    {
        KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build(10));
        builder.add(build());

        KeyRangeIterator range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(10L, range.getMaximum().token().getLongValue());
        assertTrue(range.hasNext());
        assertEquals(1, range.getMaxKeys());
    }

    @Test
    public void testEmptyNonEmptyEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));
        builder.add(build());

        KeyRangeIterator range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        assertTrue(range.hasNext());
        assertEquals(10, range.getMaxKeys());
    }

    @Test
    public void testNonEmptyEmptyNonEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        IntStream.range(10, 15).forEach(value -> builder.add(build(value)));
        builder.add(build());
        IntStream.range(15, 20).forEach(value -> builder.add(build(value)));

        KeyRangeIterator range = builder.build();
        assertEquals(10L, range.getMinimum().token().getLongValue());
        assertEquals(19L, range.getMaximum().token().getLongValue());
        assertTrue(range.hasNext());
        assertEquals(10, range.getMaxKeys());
    }

    @Test
    public void testConcatOfIntersection()
    {
        // concat of two non-intersected intersections
        KeyRangeIterator intersectionA = buildIntersection(build(1L, 2L, 3L), build(4L, 5L, 6L));
        KeyRangeIterator intersectionB = buildIntersection(build(6L, 7L, 8L), build(9L, 10L, 11L));
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
        KeyRangeIterator rangeA = build(1L, 2L, 3L);
        KeyRangeIterator rangeB = build(4L, 5L, 6L);
        KeyRangeIterator rangeC = build(7L);
        KeyRangeIterator rangeD = build(8L);
        KeyRangeIterator rangeE = build(9L);
        KeyRangeIterator concatA = buildConcat(rangeA, rangeB, rangeC, rangeD, rangeE);

        rangeA = build( 1L, 3L);
        rangeB = build( 5L, 7L, 9L);
        KeyRangeIterator concatB = buildConcat(rangeA, rangeB);

        assertEquals(convert(1L, 3L, 5L, 7L, 9L), convert(buildIntersection(concatA, concatB)));
    }

    @Test
    public void testDuplicatedElementsInTheSameFlow()
    {
        // In real case, we should not have duplicated elements from the same PostingListKeyRangeIterator
        KeyRangeIterator rangeA = build(1L, 2L, 3L, 3L, 4L, 4L);
        KeyRangeIterator rangeB = build(6L, 6L, 7L, 7L);
        KeyRangeIterator rangeC = build(8L, 8L);
        KeyRangeIterator concatA = buildConcat(rangeA, rangeB, rangeC);

        assertEquals(convert(1L, 2L, 3L, 3L, 4L, 4L, 6L, 6L, 7L, 7L, 8L, 8L), convert(concatA));
    }

    @Test
    public void testOverlappingBoundaries()
    {
        KeyRangeIterator rangeA = build(1L, 2L, 3L);
        KeyRangeIterator rangeB = build(3L, 4L, 6L, 7L);
        KeyRangeIterator rangeC = build(7L, 8L);
        KeyRangeIterator rangeD = build(8L, 9L);
        KeyRangeIterator rangeE = build(9L);
        KeyRangeIterator rangeF = build(9L);
        KeyRangeIterator rangeG = build(9L, 10L);
        KeyRangeIterator concatA = buildConcat(rangeA, rangeB, rangeC, rangeD, rangeE, rangeF, rangeG);

        assertEquals(convert(1L, 2L, 3L, 3L, 4L, 6L, 7L, 7L, 8L, 8L, 9L, 9L, 9L, 9L, 10L), convert(concatA));
    }

    @Test
    public void testDuplicatedElementsAndOverlappingBoundaries()
    {
        KeyRangeIterator rangeA = build(1L, 2L, 2L, 3L);
        KeyRangeIterator rangeB = build(3L, 4L, 4L, 6L, 6L, 7L);
        assertEquals(convert(1L, 2L, 2L, 3L, 3L, 4L, 4L, 6L, 6L, 7L), convert(buildConcat(rangeA, rangeB)));

        rangeA = build(1L, 2L, 2L, 3L);
        rangeB = build(3L);
        KeyRangeIterator rangeC = build(3L, 4L, 4L, 6L, 6L, 7L);
        KeyRangeIterator rangeD = build(7L, 7L, 8L);
        KeyRangeIterator rangeE = build(8L, 9L, 9L);
        KeyRangeIterator rangeF = build(9L, 10L);
        KeyRangeIterator concatA = buildConcat(rangeA, rangeB, rangeC, rangeD, rangeE, rangeF);

        assertEquals(convert(1L, 2L, 2L, 3L, 3L, 3L, 4L, 4L, 6L, 6L, 7L, 7L, 7L, 8L, 8L, 9L, 9L, 9L, 10L), convert(concatA));
    }

    @Test
    public void testDuplicateElementsAtBoundary()
    {
        // Duplicate on the right:
        KeyRangeIterator rangeA = build(1L, 2L, 3L);
        KeyRangeIterator rangeB = build(3L, 3L, 4L, 5L);
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

    @Test
    public void testRandom()
    {
        for (int testIteration = 0; testIteration < 16; testIteration++)
        {
            var p = createRandom();
            validateWithSkipping(p.left, p.right);
        }
    }

    static Pair<KeyRangeIterator, long[]> createRandom()
    {
        var ranges = new ArrayList<KeyRangeIterator>();
        var current = new ArrayList<Long>();
        var allValues = new ArrayList<Long>();
        int maxValue = 1024;
        for (int i = 0; i < maxValue; i++)
        {
            allValues.add((long) i);
            current.add((long) i);
            if (randomDouble() < 0.05)
            {
                ranges.add(build(current.stream().mapToLong(Long::longValue).toArray()));
                current.clear();
            }
            if (randomDouble() < 0.1)
                i += nextInt(5);
        }
        ranges.add(build(current.stream().mapToLong(Long::longValue).toArray()));

        long[] totalOrdered = allValues.stream().mapToLong(Long::longValue).toArray();
        KeyRangeIterator it = buildConcat(ranges.toArray(KeyRangeIterator[]::new));
        assertEquals(totalOrdered.length, it.getMaxKeys());
        return Pair.create(it, totalOrdered);
    }

    private KeyRangeIterator.Builder getConcatBuilder()
    {
        return KeyRangeConcatIterator.builder();
    }

    private String createErrorMessage(int max, int min)
    {
        return String.format(KeyRangeConcatIterator.MUST_BE_SORTED_ERROR,
                             LongIterator.makeKey(max),
                             LongIterator.makeKey(min));
    }
}
