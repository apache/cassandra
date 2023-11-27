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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;

import org.apache.cassandra.utils.Pair;

public class AbstractRangeIteratorTest extends SaiRandomizedTest
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    final RangeIterator buildIntersection(RangeIterator... ranges)
    {
        return RangeIntersectionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildSelectiveIntersection(int limit, RangeIterator... ranges)
    {
        return RangeIntersectionIterator.<PrimaryKey>builder(limit).add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final RangeIterator buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    static RangeIterator buildUnion(RangeIterator... ranges)
    {
        return RangeUnionIterator.<PrimaryKey>builder().add(Arrays.asList(ranges)).build();
    }

    static RangeIterator buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    static RangeIterator buildConcat(RangeIterator... ranges)
    {
        return RangeConcatIterator.builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    static RangeIterator buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private static RangeIterator[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(AbstractRangeIteratorTest::build).toArray(RangeIterator[]::new);
    }

    protected static LongIterator build(long... tokens)
    {
        return new LongIterator(tokens);
    }

    protected RangeIterator build(RangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        RangeIterator rangeA = new LongIterator(tokensA);
        RangeIterator rangeB = new LongIterator(tokensB);

        switch (type)
        {
            case INTERSECTION:
                return buildIntersection(rangeA, rangeB);
            case UNION:
                return buildUnion(rangeA, rangeB);
            case CONCAT:
                return buildConcat(rangeA, rangeB);
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    static void validateWithSkipping(RangeIterator ri, long[] totalOrdering)
    {
        int count = 0;
        while (ri.hasNext())
        {
            // make sure hasNext plays nice with skipTo
            if (randomBoolean())
                ri.hasNext();

            // skipping to the same element should also be a no-op
            if (randomBoolean())
                ri.skipTo(LongIterator.fromToken(totalOrdering[count]));

            // skip a few elements
            if (nextDouble() < 0.1)
            {
                int n = nextInt(1, 3);
                if (count + n < totalOrdering.length)
                {
                    count += n;
                    ri.skipTo(LongIterator.fromToken(totalOrdering[count]));
                }
            }
            Assert.assertEquals(totalOrdering[count++], ri.next().token().getLongValue());
        }
        Assert.assertEquals(totalOrdering.length, count);
    }

    static Set<Long> toSet(long[] tokens)
    {
        return Arrays.stream(tokens).boxed().collect(Collectors.toSet());
    }

    /**
     * @return a random {Concat,Intersection, Union} iterator, and a long[] of the elements in the iterator.
     *         elements will range from 0..1024.
     */
    static Pair<RangeIterator, long[]> createRandomIterator()
    {
        var n = randomIntBetween(0, 3);
        switch (n)
        {
            case 0:
                return RangeConcatIteratorTest.createRandom();
            case 1:
                return RangeIntersectionIteratorTest.createRandom(nextInt(1, 16));
            case 2:
                return RangeUnionIteratorTest.createRandom(nextInt(1, 16));
            default:
                throw new AssertionError();
        }
    }
}
