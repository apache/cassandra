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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AbstractRangeIteratorTest extends NdiRandomizedTest
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    void assertOnError(RangeIterator range)
    {
        assertThatThrownBy(() -> LongIterator.convert(range)).isInstanceOf(RuntimeException.class);
    }

    final RangeIterator buildIntersection(RangeIterator... ranges)
    {
        return RangeIntersectionIterator.builder().add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildSelectiveIntersection(int limit, RangeIterator... ranges)
    {
        return RangeIntersectionIterator.selectiveBuilder(limit).add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final RangeIterator buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    final RangeIterator buildUnion(RangeIterator... ranges)
    {
        return RangeUnionIterator.builder().add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    final RangeIterator buildConcat(RangeIterator... ranges)
    {
        return RangeConcatIterator.builder().add(Arrays.asList(ranges)).build();
    }

    final RangeIterator buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private RangeIterator[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(this::build).toArray(RangeIterator[]::new);
    }

    protected LongIterator build(long... tokens)
    {
        return build(tokens, false);
    }

    protected LongIterator build(long[] tokensA, boolean onErrorA)
    {
        LongIterator rangeA = new LongIterator(tokensA);

        if (onErrorA)
            rangeA.throwsException();

        return rangeA;
    }

    protected RangeIterator buildOnError(RangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        return build(type, tokensA, true, tokensB, true);
    }

    protected RangeIterator buildOnErrorA(RangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        return build(type, tokensA, true, tokensB, false);
    }

    protected RangeIterator buildOnErrorB(RangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        return build(type, tokensA, false, tokensB, true);
    }

    protected RangeIterator build(RangeIterator.Builder.IteratorType type, long[] tokensA, long[] tokensB)
    {
        return build(type, tokensA, false, tokensB, false);
    }

    protected RangeIterator build(RangeIterator.Builder.IteratorType type, long[] tokensA, boolean onErrorA, long[] tokensB, boolean onErrorB)
    {
        RangeIterator rangeA = build(tokensA, onErrorA);
        RangeIterator rangeB = build(tokensB, onErrorB);

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
}
