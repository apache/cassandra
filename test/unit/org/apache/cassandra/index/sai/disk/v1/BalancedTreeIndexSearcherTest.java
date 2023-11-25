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
package org.apache.cassandra.index.sai.disk.v1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeIndexBuilder;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class BalancedTreeIndexSearcherTest extends SAIRandomizedTester
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
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildInt32Searcher(newIndexDescriptor(), 0, 10);
        testRangeQueries(indexSearcher, Int32Type.instance, Int32Type.instance, Integer::valueOf);
    }

    @Test
    public void testEqQueriesAgainstInt32Index() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildInt32Searcher(newIndexDescriptor(),
                                                                                              EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, Int32Type.instance, Int32Type.instance, Integer::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstLongIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildLongSearcher(newIndexDescriptor(), 0, 10);
        testRangeQueries(indexSearcher, LongType.instance, Int32Type.instance, Long::valueOf);
    }

    @Test
    public void testEqQueriesAgainstLongIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildLongSearcher(newIndexDescriptor(),
                                                                                             EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, LongType.instance, Int32Type.instance, Long::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstShortIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildShortSearcher(newIndexDescriptor(), (short) 0, (short) 10);
        testRangeQueries(indexSearcher, ShortType.instance, Int32Type.instance, Function.identity());
    }

    @Test
    public void testEqQueriesAgainstShortIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildShortSearcher(newIndexDescriptor(),
                                                                                              EQ_TEST_LOWER_BOUND_INCLUSIVE, EQ_TEST_UPPER_BOUND_EXCLUSIVE);
        testEqQueries(indexSearcher, ShortType.instance, Int32Type.instance, Function.identity());
    }

    @Test
    public void testRangeQueriesAgainstDecimalIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildDecimalSearcher(newIndexDescriptor(),
                                                                                                BigDecimal.ZERO, BigDecimal.valueOf(10L));
        testRangeQueries(indexSearcher, DecimalType.instance, DecimalType.instance, BigDecimal::valueOf,
                         getLongsOnInterval(20L, 70L));
    }

    private List<Long> getLongsOnInterval(long lowerInclusive, long upperInclusive)
    {
        return LongStream.range(lowerInclusive, upperInclusive + 1L).boxed().collect(Collectors.toList());
    }

    @Test
    public void testEqQueriesAgainstDecimalIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildDecimalSearcher(newIndexDescriptor(),
                                                                                                BigDecimal.valueOf(EQ_TEST_LOWER_BOUND_INCLUSIVE), BigDecimal.valueOf(EQ_TEST_UPPER_BOUND_EXCLUSIVE));
        testEqQueries(indexSearcher, DecimalType.instance, DecimalType.instance, BigDecimal::valueOf);
    }


    @Test
    public void testEqQueriesAgainstBigIntegerIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildBigIntegerSearcher(newIndexDescriptor(),
                                                                                                   BigInteger.valueOf(EQ_TEST_LOWER_BOUND_INCLUSIVE), BigInteger.valueOf(EQ_TEST_UPPER_BOUND_EXCLUSIVE));
        testEqQueries(indexSearcher, IntegerType.instance, IntegerType.instance, BigInteger::valueOf);
    }

    @Test
    public void testRangeQueriesAgainstBigIntegerIndex() throws Exception
    {
        IndexSegmentSearcher indexSearcher = BlockBalancedTreeIndexBuilder.buildBigIntegerSearcher(newIndexDescriptor(),
                                                                                                   BigInteger.ZERO, BigInteger.valueOf(10L));
        testRangeQueries(indexSearcher, IntegerType.instance, IntegerType.instance, BigInteger::valueOf);
    }

    private <T extends Number> void testEqQueries(final IndexSegmentSearcher indexSearcher,
                                                  final NumberType<T> rawType, final NumberType<?> encodedType,
                                                  final Function<Short, T> rawValueProducer) throws Exception
    {
        try (KeyRangeIterator results = indexSearcher.search(Expression.create(SAITester.createIndexTermType(rawType))
                                                                       .add(Operator.EQ, rawType.decompose(rawValueProducer.apply(EQ_TEST_LOWER_BOUND_INCLUSIVE)))
                                                             , null, mock(QueryContext.class)))
        {
            assertEquals(results.getMinimum(), results.getCurrent());
            assertTrue(results.hasNext());

            assertEquals(0L, results.next().token().getLongValue());
        }

        try (KeyRangeIterator results = indexSearcher.search(Expression.create(SAITester.createIndexTermType(rawType))
                                                                       .add(Operator.EQ, rawType.decompose(rawValueProducer.apply(EQ_TEST_UPPER_BOUND_EXCLUSIVE))),
                                                             null, mock(QueryContext.class)))
        {
            assertFalse(results.hasNext());
            indexSearcher.close();
        }
    }

    private <T extends Number> void testRangeQueries(final IndexSegmentSearcher indexSearcher,
                                                     final NumberType<T> rawType, final NumberType<?> encodedType,
                                                     final Function<Short, T> rawValueProducer) throws Exception
    {
        List<Long> expectedTokenList = getLongsOnInterval(2L, 7L);
        testRangeQueries(indexSearcher, rawType, encodedType, rawValueProducer, expectedTokenList);
    }


    private <T extends Number> void testRangeQueries(final IndexSegmentSearcher indexSearcher,
                                                     final NumberType<T> rawType, final NumberType<?> encodedType,
                                                     final Function<Short, T> rawValueProducer, List<Long> expectedTokenList) throws Exception
    {
        try (KeyRangeIterator results = indexSearcher.search(Expression.create(SAITester.createIndexTermType(rawType))
                                                                       .add(Operator.GTE, rawType.decompose(rawValueProducer.apply((short)2)))
                                                                       .add(Operator.LTE, rawType.decompose(rawValueProducer.apply((short)7))),
                                                             null, mock(QueryContext.class)))
        {
            assertEquals(results.getMinimum(), results.getCurrent());
            assertTrue(results.hasNext());

            List<Long> actualTokenList = Lists.newArrayList(Iterators.transform(results, key -> key.token().getLongValue()));
            assertEquals(expectedTokenList, actualTokenList);
        }

        try (KeyRangeIterator results = indexSearcher.search(new Expression.IndexedExpression(SAITester.createMockIndex("meh", rawType))
        {{
            operator = IndexOperator.RANGE;
            lower = new Bound(rawType.decompose(rawValueProducer.apply(RANGE_TEST_UPPER_BOUND_EXCLUSIVE)), getIndexTermType(), true);
        }}, null, mock(QueryContext.class)))
        {
            assertFalse(results.hasNext());
        }

        try (KeyRangeIterator results = indexSearcher.search(new Expression.IndexedExpression(SAITester.createMockIndex("meh", rawType))
        {{
            operator = IndexOperator.RANGE;
            upper = new Bound(rawType.decompose(rawValueProducer.apply(RANGE_TEST_LOWER_BOUND_INCLUSIVE)), getIndexTermType(), false);
        }}, null, mock(QueryContext.class)))
        {
            assertFalse(results.hasNext());
            indexSearcher.close();
        }
    }
}
