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

package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.AbstractReadCommandBuilder.MultiPartitionBuilder;
import org.apache.cassandra.db.AbstractReadCommandBuilder.PartitionRangeBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Row;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class AggregationPagerTest extends QueryPagerTests
{
    @Parameterized.Parameter
    public PageSize subPageSize;

    @Parameterized.Parameters(name = "subPageSize={0}")
    public static Collection options()
    {
        return Arrays.asList(new Object[][]{
        { PageSize.NONE },
        { PageSize.inRows(2) },
        { PageSize.inRows(3) },
        { PageSize.inRows(4) },
        { PageSize.inRows(10) },
        { PageSize.inRows(15) },
        });
    }

    final int expectedPerGroupCnt = 10;

    private final SinglePartitionPagerTest singlePartitionPagerTest = new SinglePartitionPagerTest()
    {
        @Override
        QueryPager checkNextPage(QueryPager pager, ReadQuery command, boolean testPagingState, PageSize pageSize, int expectedRows, Consumer<List<FilteredPartition>> assertion)
        {
            return super.checkNextPage(pager, command, testPagingState, pageSize, expectedRows * expectedPerGroupCnt, assertion);
        }

        @Override
        protected void checkAllPages(ReadCommand cmd, boolean testPagingState, PageSize pageSize, int expectedPages, int expectedRows, String firstPartitionKey, String firstName, String lastPartitionKey, String lastName)
        {
            super.checkAllPages(cmd, testPagingState, pageSize, expectedPages, expectedRows * expectedPerGroupCnt, firstPartitionKey, firstName, lastPartitionKey, lastName);
        }

        @Override
        void assertRow(FilteredPartition p, String key, String... names)
        {
            AggregationPagerTest.this.assertRow(p, key, names);
        }
    };

    private final PartitionRangePagerTest partitionRangePagerTest = new PartitionRangePagerTest()
    {
        @Override
        QueryPager checkNextPage(QueryPager pager, ReadQuery command, boolean testPagingState, PageSize pageSize, int expectedRows, Consumer<List<FilteredPartition>> assertion)
        {
            return super.checkNextPage(pager, command, testPagingState, pageSize, expectedRows * expectedPerGroupCnt, assertion);
        }

        @Override
        protected void checkAllPages(ReadCommand cmd, boolean testPagingState, PageSize pageSize, int expectedPages, int expectedRows, String firstPartitionKey, String firstName, String lastPartitionKey, String lastName)
        {
            super.checkAllPages(cmd, testPagingState, pageSize, expectedPages, expectedRows * expectedPerGroupCnt, firstPartitionKey, firstName, lastPartitionKey, lastName);
        }

        @Override
        void assertRow(FilteredPartition p, String key, String... names)
        {
            AggregationPagerTest.this.assertRow(p, key, names);
        }

        @Override
        protected PartitionRangeBuilder makeBuilder(int limit, int perPartitionLimit, PageSize pageSize, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl)
        {
            PartitionRangeBuilder builder = super.makeBuilder(limit, perPartitionLimit, pageSize, cfs, startKeyInc, endKeyExcl);
            builder.withAggregationSpecification(AggregationSpecification.aggregatePkPrefixFactory(cfs.metadata().comparator, 1).newInstance(QueryOptions.DEFAULT));
            builder.withSubPageSize(getSubPageSize());
            return builder;
        }
    };

    private final MultiPartitionPagerTest multiPartitionPagerTest = new MultiPartitionPagerTest()
    {
        @Override
        QueryPager checkNextPage(QueryPager pager, ReadQuery command, boolean testPagingState, PageSize pageSize, int expectedRows, Consumer<List<FilteredPartition>> assertion)
        {
            return super.checkNextPage(pager, command, testPagingState, pageSize, expectedRows * expectedPerGroupCnt, assertion);
        }

        @Override
        protected void checkAllPages(ReadCommand cmd, boolean testPagingState, PageSize pageSize, int expectedPages, int expectedRows, String firstPartitionKey, String firstName, String lastPartitionKey, String lastName)
        {
            super.checkAllPages(cmd, testPagingState, pageSize, expectedPages, expectedRows * expectedPerGroupCnt, firstPartitionKey, firstName, lastPartitionKey, lastName);
        }

        @Override
        void assertRow(FilteredPartition p, String key, String... names)
        {
            AggregationPagerTest.this.assertRow(p, key, names);
        }

        @Override
        protected MultiPartitionBuilder makeBuilder(int limit, int perPartitionLimit, PageSize pageSize, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl)
        {
            MultiPartitionBuilder builder = super.makeBuilder(limit, perPartitionLimit, pageSize, cfs, startKeyInc, endKeyExcl);
            builder.withAggregationSpecification(AggregationSpecification.aggregatePkPrefixFactory(cfs.metadata().comparator, 1).newInstance(QueryOptions.DEFAULT));
            builder.withSubPageSize(getSubPageSize());
            return builder;
        }
    };

    @Override
    void assertRow(FilteredPartition p, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRowsGroup(p, key, bbs);
    }

    void assertRowsGroup(FilteredPartition returnedPartition, String key, ByteBuffer... names)
    {
        assertEquals(key, string(returnedPartition.partitionKey().getKey()));
        assertFalse(returnedPartition.isEmpty());
        int i = 0;
        int cnt = 0;
        ByteBuffer prev = null;
        for (Row row : Util.once(returnedPartition.iterator()))
        {
            logger.info("Found row {}:{}", key, row.toString(returnedPartition.metadata()));

            if (prev == null)
                prev = row.clustering().bufferAt(0);

            if (Objects.equals(prev, row.clustering().bufferAt(0)))
            {
                cnt++;
                continue;
            }

            assertEquals(String.format("Group size for pk=%s, name=%s: %d != %d", string(returnedPartition.partitionKey().getKey()), string(prev), cnt, expectedPerGroupCnt), expectedPerGroupCnt, cnt);
            assert i < names.length : "Found more rows than expected (" + (i + 1) + ") in partition " + key;
            ByteBuffer expected = names[i++];
            assertEquals(String.format("column %d doesn't match %s vs %s", i, string(expected), string(prev)), expected, prev);
            prev = row.clustering().bufferAt(0);
            cnt = 1;
        }
    }

    private ReadCommand makeSliceQuery(int limit, int perPartitionLimit, boolean isReversed)
    {
        ColumnFamilyStore cfs = cfs(KEYSPACE1, CF_WITH_TWO_CLUSTERINGS);
        return sliceQuery(limit, perPartitionLimit, PageSize.NONE, cfs, "k0", "c1", "c9", isReversed).withAggregationSpecification(AggregationSpecification.aggregatePkPrefixFactory(cfs.metadata().comparator, 1).newInstance(QueryOptions.DEFAULT)).withSubPageSize(subPageSize)
                                                                                                     .build();
    }

    @Test
    public void singlePartitionSliceQueryTest()
    {
        ReadCommand cmd = makeSliceQuery(NO_LIMIT, NO_LIMIT, false);
        singlePartitionPagerTest.queryTest(false, cmd);
        singlePartitionPagerTest.queryTest(true, cmd);
    }

    @Test
    public void singlePartitionSliceQueryWithLimitsTest()
    {
        singlePartitionPagerTest.queryWithLimitsTest(false, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false));
        singlePartitionPagerTest.queryWithLimitsTest(true, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false));
    }

    @Test
    public void singlePartitionSliceQueryWithPagingInRowsTest()
    {
        singlePartitionPagerTest.queryWithPagingTest(true, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false), this::pageSizeInRows);
        singlePartitionPagerTest.queryWithPagingTest(false, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false), this::pageSizeInRows);
    }

    @Test
    public void singlePartitionReversedSliceQueryTest()
    {
        ReadCommand cmd = makeSliceQuery(NO_LIMIT, NO_LIMIT, true);
        singlePartitionPagerTest.reversedQueryTest(false, cmd);
        singlePartitionPagerTest.reversedQueryTest(true, cmd);
    }

    private ReadQuery makePartitionsRangeSliceQuery(PartitionsPagerTests test, int limit, int perPartitionLimit)
    {
        return test.makePartitionsSliceQuery(limit, perPartitionLimit, cfs(KEYSPACE1, CF_WITH_TWO_CLUSTERINGS),
                                             tokenOrderedKeys.get(1), tokenOrderedKeys.get(5),
                                             "c1", "c8").build();
    }

    @Test
    public void partitionRangeSliceQueryWithPagingInRowsTest()
    {
        partitionRangePagerTest.partitionsQueryWithPagingInRowsTest((limit, perPartitionLimit) -> makePartitionsRangeSliceQuery(partitionRangePagerTest, limit, perPartitionLimit), false, this::pageSizeInRows);
        partitionRangePagerTest.partitionsQueryWithPagingInRowsTest((limit, perPartitionLimit) -> makePartitionsRangeSliceQuery(partitionRangePagerTest, limit, perPartitionLimit), true, this::pageSizeInRows);
    }

    @Test
    public void partitionRangeSliceQueryWithLimitsTest()
    {
        partitionRangePagerTest.partitionsQueryWithLimitsTest((limit, perPartitionLimit) -> makePartitionsRangeSliceQuery(partitionRangePagerTest, limit, perPartitionLimit), false);
        partitionRangePagerTest.partitionsQueryWithLimitsTest((limit, perPartitionLimit) -> makePartitionsRangeSliceQuery(partitionRangePagerTest, limit, perPartitionLimit), true);
    }

    private ReadQuery makeMultiPartitionSliceQuery(MultiPartitionPagerTest test, int limit, int perPartitionLimit)
    {
        return test.makePartitionsSliceQuery(limit, perPartitionLimit, cfs(KEYSPACE1, CF_WITH_TWO_CLUSTERINGS),
                                             tokenOrderedKeys.get(1), tokenOrderedKeys.get(5),
                                             "c1", "c8").build();
    }

    @Test
    public void multiPartitionSliceQueryWithPagingInRowsTest()
    {
        multiPartitionPagerTest.partitionsQueryWithPagingInRowsTest((limit, perPartitionLimit) -> makeMultiPartitionSliceQuery(multiPartitionPagerTest, limit, perPartitionLimit), false, this::pageSizeInRows);
        multiPartitionPagerTest.partitionsQueryWithPagingInRowsTest((limit, perPartitionLimit) -> makeMultiPartitionSliceQuery(multiPartitionPagerTest, limit, perPartitionLimit), true, this::pageSizeInRows);
    }

    @Test
    public void multiPartitionSliceQueryWithLimitsTest()
    {
        multiPartitionPagerTest.partitionsQueryWithLimitsTest((limit, perPartitionLimit) -> makeMultiPartitionSliceQuery(multiPartitionPagerTest, limit, perPartitionLimit), false);
        multiPartitionPagerTest.partitionsQueryWithLimitsTest((limit, perPartitionLimit) -> makeMultiPartitionSliceQuery(multiPartitionPagerTest, limit, perPartitionLimit), true);
    }
}
