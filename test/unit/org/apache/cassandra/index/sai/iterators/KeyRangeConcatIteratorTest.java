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

import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class KeyRangeConcatIteratorTest extends AbstractKeyRangeIteratorTester
{
    PrimaryKey.Factory primaryKeyFactory = new PrimaryKey.Factory(Murmur3Partitioner.instance, null);
    @Test
    public void testValidation()
    {
        // Iterators being merged via concatanation must not include each other
        assertThatThrownBy(() -> buildConcat(build(1L, 4L), build(2L, 3L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 2));

        // Iterators being merged via concatanation must not overlap
        assertThatThrownBy(() -> buildConcat(build(1L, 4L), build(2L, 5L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 2));

        assertThatThrownBy(() -> buildConcat(build(1L, 4L), build(0L, 3L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 0));

        // Iterators being merged via concatanation must be sorted
        assertThatThrownBy(() -> buildConcat(build(2L, 4L), build(0L, 1L))).isInstanceOf(IllegalArgumentException.class)
                                                                           .hasMessage(createErrorMessage(4, 0));

        // allow min boundary included
        KeyRangeIterator concat = buildConcat(build(1L, 4L), build(4L, 5L));
        assertEquals(convert(1L, 4L, 4L, 5L), convert(concat));

        // with empty iterator
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
        assertEquals(9L, builder.getCount());

        KeyRangeIterator keyIterator = builder.build();

        assertNotNull(keyIterator);
        assertEquals(1L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(9L, keyIterator.getMaximum().token().getLongValue());
        assertEquals(9L, keyIterator.getCount());

        for (long i = 1; i < 10; i++)
        {
            assertTrue(keyIterator.hasNext());
            assertEquals(i, keyIterator.next().token().getLongValue());
        }

        assertFalse(keyIterator.hasNext());
        assertEquals(1L, keyIterator.getMinimum().token().getLongValue());
    }

    @Test
    public void testSkipTo()
    {
        Supplier<KeyRangeIterator> init = () ->  buildConcat(build(1L, 2L, 3L),
                                                             build( 4L, 5L, 6L),
                                                             build( 7L, 8L, 9L));

        KeyRangeIterator keyIterator;

        keyIterator = init.get();
        keyIterator.skipTo(LongIterator.fromToken(5));
        assertTrue(keyIterator.hasNext());
        assertEquals(5L, keyIterator.next().token().getLongValue());

        keyIterator = init.get();
        keyIterator.skipTo(LongIterator.fromToken(7L));
        assertTrue(keyIterator.hasNext());
        assertEquals(7L, keyIterator.next().token().getLongValue());

        keyIterator = init.get();
        keyIterator.skipTo(LongIterator.fromToken(2L));
        keyIterator.skipTo(LongIterator.fromToken(5L));
        keyIterator.skipTo(LongIterator.fromToken(10L));
        assertFalse(keyIterator.hasNext());
        assertEquals(1L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(9L, keyIterator.getMaximum().token().getLongValue());
    }

    @Test
    public void testSkipToWithGaps()
    {
        Supplier<KeyRangeIterator> init = () ->  buildConcat(build(1L, 2L, 3L), build(4L, 6L), build(8L, 9L));

        KeyRangeIterator keyIterator;

        keyIterator = init.get();
        keyIterator.skipTo(LongIterator.fromToken(5L));
        assertTrue(keyIterator.hasNext());
        assertEquals(6L, keyIterator.next().token().getLongValue());

        keyIterator = init.get();
        keyIterator.skipTo(LongIterator.fromToken(7L));
        assertTrue(keyIterator.hasNext());
        assertEquals(8L, keyIterator.next().token().getLongValue());

        keyIterator = init.get();
        keyIterator.skipTo(LongIterator.fromToken(2L));
        keyIterator.skipTo(LongIterator.fromToken(5L));
        keyIterator.skipTo(LongIterator.fromToken(10L));
        assertFalse(keyIterator.hasNext());
        assertEquals(1L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(9L, keyIterator.getMaximum().token().getLongValue());
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

        KeyRangeIterator keyIterator = builder.build();
        assertEquals(10L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(19L, keyIterator.getMaximum().token().getLongValue());
        assertTrue(keyIterator.hasNext());
        assertEquals(10, keyIterator.getCount());
    }

    @Test
    public void testEmptyThenSingleNonEmpty()
    {
        KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        builder.add(build(10));

        KeyRangeIterator keyIterator = builder.build();
        assertEquals(10L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(10L, keyIterator.getMaximum().token().getLongValue());
        assertTrue(keyIterator.hasNext());
        assertEquals(1, keyIterator.getCount());
    }

    @Test
    public void testManyNonEmptyThenEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));
        builder.add(build());

        KeyRangeIterator keyIterator = builder.build();
        assertEquals(10L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(19L, keyIterator.getMaximum().token().getLongValue());
        assertTrue(keyIterator.hasNext());
        assertEquals(10, keyIterator.getCount());
    }

    @Test
    public void testSingleNonEmptyThenEmpty()
    {
        KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build(10));
        builder.add(build());

        KeyRangeIterator keyIterator = builder.build();
        assertEquals(10L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(10L, keyIterator.getMaximum().token().getLongValue());
        assertTrue(keyIterator.hasNext());
        assertEquals(1, keyIterator.getCount());
    }

    @Test
    public void testEmptyNonEmptyEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        builder.add(build());
        IntStream.range(10, 20).forEach(value -> builder.add(build(value)));
        builder.add(build());

        KeyRangeIterator keyIterator = builder.build();
        assertEquals(10L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(19L, keyIterator.getMaximum().token().getLongValue());
        assertTrue(keyIterator.hasNext());
        assertEquals(10, keyIterator.getCount());
    }

    @Test
    public void testNonEmptyEmptyNonEmpty()
    {
        final KeyRangeIterator.Builder builder = getConcatBuilder();

        IntStream.range(10, 15).forEach(value -> builder.add(build(value)));
        builder.add(build());
        IntStream.range(15, 20).forEach(value -> builder.add(build(value)));

        KeyRangeIterator keyIterator = builder.build();
        assertEquals(10L, keyIterator.getMinimum().token().getLongValue());
        assertEquals(19L, keyIterator.getMaximum().token().getLongValue());
        assertTrue(keyIterator.hasNext());
        assertEquals(10, keyIterator.getCount());
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
    public void testConcatOnError()
    {
        assertOnError(buildOnErrorA(this::buildConcat, arr(1L, 2L, 3L), arr(4L, 5L, 6L)));
        assertOnError(buildOnErrorB(this::buildConcat, arr( 1L, 2L, 3L), arr(4L)));
    }

    @Test
    public void testConcatOfUnionsOnError()
    {
        KeyRangeIterator unionA = buildUnion(arr(1L, 2L, 3L), arr(4L));
        KeyRangeIterator unionB = buildOnErrorB(this::buildUnion, arr(6L), arr(8L, 9L));
        assertOnError(buildConcat(unionA, unionB));

        unionA = buildOnErrorA(this::buildUnion, arr( 1L, 2L, 3L), arr( 4L));
        unionB = buildUnion(arr( 5L), arr( 5L, 6L));
        assertOnError(buildConcat(unionA, unionB));
    }

    @Test
    public void testConcatOfIntersectionsOnError()
    {
        KeyRangeIterator intersectionA = buildOnErrorA(this::buildIntersection, arr(1L, 2L, 3L), arr(2L, 3L, 4L));
        KeyRangeIterator intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(7L, 8L, 9L));
        assertOnError(buildConcat(intersectionA, intersectionB));

        intersectionA = buildIntersection(arr( 1L, 2L, 3L), arr( 2L, 3L, 4L));
        intersectionB = buildOnErrorB(this::buildIntersection, arr( 6L, 7L, 8L, 9L, 10L), arr(  7L, 8L, 9L));
        assertOnError(buildConcat(intersectionA, intersectionB));
    }

    @Test
    public void testDuplicatedElementsInTheSameIterator()
    {
        // In real case, we should not have duplicated elements from the same PostingListRangeIterator
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

    private KeyRangeIterator.Builder getConcatBuilder()
    {
        return KeyRangeConcatIterator.builder(16);
    }

    private String createErrorMessage(int max, int min)
    {
        return String.format(KeyRangeConcatIterator.MUST_BE_SORTED_ERROR,
                             primaryKeyFactory.create(new Murmur3Partitioner.LongToken(max)),
                             primaryKeyFactory.create(new Murmur3Partitioner.LongToken(min)));
    }
}
