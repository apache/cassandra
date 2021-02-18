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
package org.apache.cassandra.index.sai.disk;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.RangeIterator;

public class KDTreeIndexSearcherTest extends NdiRandomizedTest
{
    private static final short EQ_TEST_LOWER_BOUND_INCLUSIVE = 0;
    private static final short EQ_TEST_UPPER_BOUND_EXCLUSIVE = 3;

    private static final short RANGE_TEST_LOWER_BOUND_INCLUSIVE = 0;
    private static final short RANGE_TEST_UPPER_BOUND_EXCLUSIVE = 10;

    @Test
    public void testRangeQueriesAgainstInt32Index() throws Exception
    {
        doTestRangeQueriesAgainstInt32Index();
    }

    private void doTestRangeQueriesAgainstInt32Index() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildInt32Searcher(newIndexComponents(), 0, 10);
        testRangeQueries(indexSearcher, Int32Type.instance, Int32Type.instance, Integer::valueOf);
    }

    @Test
    public void testEqQueriesAgainstInt32Index() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildInt32Searcher(newIndexComponents(),
                                                                            EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, Int32Type.instance, Int32Type.instance, Integer::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstLongIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildLongSearcher(newIndexComponents(), 0, 10);
        testRangeQueries(indexSearcher, LongType.instance, Int32Type.instance, Long::valueOf);
    }

    @Test
    public void testEqQueriesAgainstLongIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildLongSearcher(newIndexComponents(),
                                                                           EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, LongType.instance, Int32Type.instance, Long::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstShortIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildShortSearcher(newIndexComponents(), (short) 0, (short) 10);
        testRangeQueries(indexSearcher, ShortType.instance, Int32Type.instance, Function.identity());
    }

    @Test
    public void testEqQueriesAgainstShortIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildShortSearcher(newIndexComponents(),
                                                                            EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, ShortType.instance, Int32Type.instance, Function.identity());
    }

    @Test
    public void testRangeQueriesAgainstDecimalIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildDecimalSearcher(newIndexComponents(),
                                                                              BigDecimal.ZERO, BigDecimal.valueOf(10L));
        testRangeQueries(indexSearcher, DecimalType.instance, DecimalType.instance, BigDecimal::valueOf,
                         getLongsOnInterval(21L, 70L));
    }

    private List<Long> getLongsOnInterval(long lowerInclusive, long upperInclusive)
    {
        return LongStream.range(lowerInclusive, upperInclusive + 1L).boxed().collect(Collectors.toList());
    }

    @Test
    public void testEqQueriesAgainstDecimalIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildDecimalSearcher(newIndexComponents(),
                                                                              BigDecimal.valueOf(EQ_TEST_LOWER_BOUND_INCLUSIVE), BigDecimal.valueOf(EQ_TEST_UPPER_BOUND_EXCLUSIVE));
        testEqQueries(indexSearcher, DecimalType.instance, DecimalType.instance, BigDecimal::valueOf);
    }


    @Test
    public void testEqQueriesAgainstBigIntegerIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildBigIntegerSearcher(newIndexComponents(),
                                                                                 BigInteger.valueOf(EQ_TEST_LOWER_BOUND_INCLUSIVE), BigInteger.valueOf(EQ_TEST_UPPER_BOUND_EXCLUSIVE));
        testEqQueries(indexSearcher, IntegerType.instance, IntegerType.instance, BigInteger::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstBigIntegerIndex() throws Exception
    {
        IndexSearcher indexSearcher = KDTreeIndexBuilder.buildBigIntegerSearcher(newIndexComponents(),
                                                                                 BigInteger.ZERO, BigInteger.valueOf(10L));
        testRangeQueries(indexSearcher, IntegerType.instance, IntegerType.instance, BigInteger::valueOf);
    }


    @Test
    public void testUnsupportedOperator() throws Exception
    {
        final IndexSearcher indexSearcher = KDTreeIndexBuilder.buildShortSearcher(newIndexComponents(), (short) 0, (short) 3);
        try
        {
            indexSearcher.search(new Expression(SAITester.createColumnContext("meh", ShortType.instance))
            {{
                operation = Op.NOT_EQ;
                lower = upper = new Bound(ShortType.instance.decompose((short) 0), Int32Type.instance, true);
            }}, SSTableQueryContext.forTest(), false);

            fail("Expect IllegalArgumentException thrown, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private <T extends Number> void testEqQueries(final IndexSearcher indexSearcher,
                                                  final NumberType<T> rawType, final NumberType<?> encodedType,
                                                  final Function<Short, T> rawValueProducer) throws Exception
    {
        try (RangeIterator results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.EQ;
            lower = upper = new Bound(rawType.decompose(rawValueProducer.apply(EQ_TEST_LOWER_BOUND_INCLUSIVE)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false))
        {
            assertEquals(results.getMinimum(), results.getCurrent());
            assertTrue(results.hasNext());

            assertEquals(0L, results.next().getLong());
        }

        try (RangeIterator results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.EQ;
            lower = upper = new Bound(rawType.decompose(rawValueProducer.apply(EQ_TEST_UPPER_BOUND_EXCLUSIVE)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false))
        {
            assertFalse(results.hasNext());
            indexSearcher.close();
        }
    }

    private <T extends Number> void testRangeQueries(final IndexSearcher indexSearcher,
                                                     final NumberType<T> rawType, final NumberType<?> encodedType,
                                                     final Function<Short, T> rawValueProducer) throws Exception
    {
        List<Long> expectedTokenList = getLongsOnInterval(3L, 7L);
        testRangeQueries(indexSearcher, rawType, encodedType, rawValueProducer, expectedTokenList);
    }


    private <T extends Number> void testRangeQueries(final IndexSearcher indexSearcher,
                                                     final NumberType<T> rawType, final NumberType<?> encodedType,
                                                     final Function<Short, T> rawValueProducer, List<Long> expectedTokenList) throws Exception
    {
        try (RangeIterator results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.RANGE;

            lower = new Bound(rawType.decompose(rawValueProducer.apply((short)2)), encodedType, false);
            upper = new Bound(rawType.decompose(rawValueProducer.apply((short)7)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false))
        {
            assertEquals(results.getMinimum(), results.getCurrent());
            assertTrue(results.hasNext());

            List<Long> actualTokenList = Lists.newArrayList(Iterators.transform(results, Token::get));
            assertEquals(expectedTokenList, actualTokenList);
        }

        try (RangeIterator results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.RANGE;
            lower = new Bound(rawType.decompose(rawValueProducer.apply(RANGE_TEST_UPPER_BOUND_EXCLUSIVE)), encodedType, true);
        }}, SSTableQueryContext.forTest(), false))
        {
            assertFalse(results.hasNext());
        }

        try (RangeIterator results = indexSearcher.search(new Expression(SAITester.createColumnContext("meh", rawType))
        {{
            operation = Op.RANGE;
            upper = new Bound(rawType.decompose(rawValueProducer.apply(RANGE_TEST_LOWER_BOUND_INCLUSIVE)), encodedType, false);
        }}, SSTableQueryContext.forTest(), false))
        {
            assertFalse(results.hasNext());
            indexSearcher.close();
        }
    }
}
