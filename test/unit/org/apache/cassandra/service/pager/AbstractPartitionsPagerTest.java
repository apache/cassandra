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

import java.util.function.BiFunction;
import java.util.function.IntFunction;

import org.junit.Test;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadQuery;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPartitionsPagerTest extends QueryPagerTest
{
    protected abstract ReadQuery makePartitionsSliceQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String startClustInc, String endClustExcl);

    protected abstract ReadQuery makePartitionsNamesQuery(int limit, int perPartitionLimit, ColumnFamilyStore cfs, String startKeyInc, String endKeyExcl, String... clusts);


    private ReadQuery makePartitionsSliceQuery(int limit, int perPartitionLimit)
    {
        return makePartitionsSliceQuery(limit, perPartitionLimit, cfs(KEYSPACE1, CF_STANDARD),
                                        tokenOrderedKeys.get(1), tokenOrderedKeys.get(5),
                                        "c1", "c8");
    }

    private ReadQuery makePartitionsNamesQuery(int limit, int perPartitionLimit)
    {
        return makePartitionsNamesQuery(limit, perPartitionLimit, cfs(KEYSPACE1, CF_STANDARD),
                                        tokenOrderedKeys.get(1), tokenOrderedKeys.get(5),
                                        "c1", "c2", "c3", "c4", "c5", "c6", "c7");
    }

    @Test
    public void partitionsSliceQueryWithPagingInBytesTest()
    {
        partitionsQueryWithPagingInRowsTest(this::makePartitionsSliceQuery, false, this::pageSizeInBytes);
        partitionsQueryWithPagingInRowsTest(this::makePartitionsSliceQuery, true, this::pageSizeInBytes);
    }

    @Test
    public void partitionsNamesQueryWithPagingInBytesTest()
    {
        partitionsQueryWithPagingInRowsTest(this::makePartitionsNamesQuery, false, this::pageSizeInBytes);
        partitionsQueryWithPagingInRowsTest(this::makePartitionsNamesQuery, true, this::pageSizeInBytes);
    }

    @Test
    public void partitionsSliceQueryWithPagingInRowsTest()
    {
        partitionsQueryWithPagingInRowsTest(this::makePartitionsSliceQuery, false, this::pageSizeInRows);
        partitionsQueryWithPagingInRowsTest(this::makePartitionsSliceQuery, true, this::pageSizeInRows);
    }

    @Test
    public void partitionsNamesQueryWithPagingInRowsTest()
    {
        partitionsQueryWithPagingInRowsTest(this::makePartitionsNamesQuery, false, this::pageSizeInRows);
        partitionsQueryWithPagingInRowsTest(this::makePartitionsNamesQuery, true, this::pageSizeInRows);
    }

    public void partitionsQueryWithPagingInRowsTest(BiFunction<Integer, Integer, ReadQuery> querySupplier, boolean testPagingState, IntFunction<PageSize> pageSizeSupplier)
    {
        int limit = NO_LIMIT;
        int perPartitionLimit = NO_LIMIT;
        ReadQuery query = querySupplier.apply(limit, perPartitionLimit);
        QueryPager pager = checkNextPage(null, query, testPagingState, pageSizeSupplier.apply(5), 5, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3", "c4", "c5");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(4), 4, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c6", "c7");
            assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(6), 6, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(2), "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(1), tokenOrderedKeys.get(3), "c1");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(5), 5, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(3), "c2", "c3", "c4", "c5", "c6");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(5), 5, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(3), "c7");
            assertRow(partitions.get(1), tokenOrderedKeys.get(4), "c1", "c2", "c3", "c4");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(5), 3, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(4), "c5", "c6", "c7");
        });
        assertTrue(pager.isExhausted());

        // Test with page size < limit

        limit = 16;
        perPartitionLimit = 5;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, pageSizeSupplier.apply(3), 3, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(2), 2, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c4", "c5");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(7), 7, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(2), "c1", "c2", "c3", "c4", "c5");
            assertRow(partitions.get(1), tokenOrderedKeys.get(3), "c1", "c2");
        });
        pager = checkNextPage(pager, query, testPagingState, pageSizeSupplier.apply(100), 4, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(3), "c3", "c4", "c5");
            assertRow(partitions.get(1), tokenOrderedKeys.get(4), "c1");
        });
        assertTrue(pager.isExhausted());
    }

    @Test
    public void partitionsSliceQueryWithLimitsTest()
    {
        partitionsQueryWithLimitsTest(this::makePartitionsSliceQuery, false);
        partitionsQueryWithLimitsTest(this::makePartitionsSliceQuery, true);
    }

    @Test
    public void partitionsNamesQueryWithLimitsTest()
    {
        partitionsQueryWithLimitsTest(this::makePartitionsNamesQuery, false);
        partitionsQueryWithLimitsTest(this::makePartitionsNamesQuery, true);
    }

    public void partitionsQueryWithLimitsTest(BiFunction<Integer, Integer, ReadQuery> querySupplier, boolean testPagingState)
    {
        // Test with limit < partition size

        int limit = 3;
        int perPartitionLimit = NO_LIMIT;
        ReadQuery query = querySupplier.apply(limit, perPartitionLimit);
        QueryPager pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 3, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3");
        });
        assertTrue(pager.isExhausted());

        // Test with limit == partition size

        limit = 7;
        perPartitionLimit = NO_LIMIT;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 7, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
        });
        assertTrue(pager.isExhausted());

        // Test with limit > partition size

        limit = 11;
        perPartitionLimit = NO_LIMIT;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 11, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2", "c3", "c4");
        });
        assertTrue(pager.isExhausted());

        // Test with perPartitionLimit < partition size

        limit = NO_LIMIT;
        perPartitionLimit = 3;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 12, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3");
            assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2", "c3");
            assertRow(partitions.get(2), tokenOrderedKeys.get(3), "c1", "c2", "c3");
            assertRow(partitions.get(3), tokenOrderedKeys.get(4), "c1", "c2", "c3");
        });
        assertTrue(pager.isExhausted());

        // Test with perPartitionLimit == partition size

        limit = NO_LIMIT;
        perPartitionLimit = 7;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 28, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(2), tokenOrderedKeys.get(3), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(3), tokenOrderedKeys.get(4), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
        });
        assertTrue(pager.isExhausted());

        // Test with perPartitionLimit > partition size

        limit = NO_LIMIT;
        perPartitionLimit = 11;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 28, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(2), tokenOrderedKeys.get(3), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
            assertRow(partitions.get(3), tokenOrderedKeys.get(4), "c1", "c2", "c3", "c4", "c5", "c6", "c7");
        });
        assertTrue(pager.isExhausted());

        // Test with limit < perPartitionLimit < partition size

        limit = 3;
        perPartitionLimit = 4;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 3, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3");
        });
        assertTrue(pager.isExhausted());

        // Test with perPartitionLimit < limit < partition size

        limit = 4;
        perPartitionLimit = 3;
        query = querySupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, query, testPagingState, PageSize.NONE, 4, partitions -> {
            assertRow(partitions.get(0), tokenOrderedKeys.get(1), "c1", "c2", "c3");
            assertRow(partitions.get(1), tokenOrderedKeys.get(2), "c1");
        });
        assertTrue(pager.isExhausted());
    }

}
