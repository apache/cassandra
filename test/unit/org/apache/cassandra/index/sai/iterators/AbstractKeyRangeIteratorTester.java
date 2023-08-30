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

import java.util.Arrays;
import java.util.function.BiFunction;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AbstractKeyRangeIteratorTester extends SAIRandomizedTester
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    void assertOnError(KeyRangeIterator<PrimaryKey> range)
    {
        assertThatThrownBy(() -> LongIterator.convert(range)).isInstanceOf(RuntimeException.class);
    }

    final KeyRangeIterator<PrimaryKey> buildIntersection(KeyRangeIterator<PrimaryKey>... ranges)
    {
        return KeyRangeIntersectionIterator.<PrimaryKey>builder(16, Integer.MAX_VALUE).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator<PrimaryKey> buildSelectiveIntersection(int limit, KeyRangeIterator<PrimaryKey>... ranges)
    {
        return KeyRangeIntersectionIterator.<PrimaryKey>builder(16, limit).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator<PrimaryKey> buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final KeyRangeIterator<PrimaryKey> buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    final KeyRangeIterator<PrimaryKey> buildUnion(KeyRangeIterator<PrimaryKey>... ranges)
    {
        return KeyRangeUnionIterator.<PrimaryKey>builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator<PrimaryKey> buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    @SafeVarargs
    final KeyRangeIterator<PrimaryKey> buildConcat(KeyRangeIterator<PrimaryKey>... ranges)
    {
        return KeyRangeConcatIterator.<PrimaryKey>builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator<PrimaryKey> buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private KeyRangeIterator<PrimaryKey>[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(this::build).toArray(KeyRangeIterator[]::new);
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

    protected KeyRangeIterator<PrimaryKey> buildOnError(BiFunction<KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>> builder, long[] tokensA, long[] tokensB)
    {
        return build(builder, tokensA, true, tokensB, true);
    }

    protected KeyRangeIterator<PrimaryKey> buildOnErrorA(BiFunction<KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>> builder, long[] tokensA, long[] tokensB)
    {
        return build(builder, tokensA, true, tokensB, false);
    }

    protected KeyRangeIterator<PrimaryKey> buildOnErrorB(BiFunction<KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>> builder, long[] tokensA, long[] tokensB)
    {
        return build(builder, tokensA, false, tokensB, true);
    }

    protected KeyRangeIterator<PrimaryKey> build(BiFunction<KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>, KeyRangeIterator<PrimaryKey>> builder,
                                     long[] tokensA,
                                     boolean onErrorA,
                                     long[] tokensB,
                                     boolean onErrorB)
    {
        return builder.apply(build(tokensA, onErrorA), build(tokensB, onErrorB));
    }
}
