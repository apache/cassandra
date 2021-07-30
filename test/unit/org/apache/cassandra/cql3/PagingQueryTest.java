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

package org.apache.cassandra.cql3;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.OperationExecutionException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PagingQueryTest extends CQLTester
{
    final int ROW_SIZE = 49; // size of internal representation

    @Parameterized.Parameters(name = "aggregation_sub_page_size={0}")
    public static Collection<Object[]> generateParameters()
    {
        return Arrays.asList(new Object[]{ PageSize.inBytes(1024) }, new Object[]{ PageSize.NONE });
    }

    public PagingQueryTest(PageSize subPageSize)
    {
        DatabaseDescriptor.setAggregationSubPageSize(subPageSize);
    }

    @Test
    public void pagingOnRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    " k1 int," +
                    " c1 int," +
                    " c2 int," +
                    " v1 text," +
                    " v2 text," +
                    " v3 text," +
                    " v4 text," +
                    "PRIMARY KEY (k1, c1, c2))");

        for (int c1 = 0; c1 < 100; c1++)
        {
            for (int c2 = 0; c2 < 100; c2++)
            {
                execute("INSERT INTO %s (k1, c1, c2, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?, ?, ?)", 1, c1, c2,
                        Integer.toString(c1), Integer.toString(c2), someText(), someText());
            }

            if (c1 % 30 == 0)
                flush();
        }

        flush();

        Session session = sessionNet();
        SimpleStatement stmt = new SimpleStatement("SELECT c1, c2, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE k1 = 1");
        stmt.setFetchSize(3);
        ResultSet rs = session.execute(stmt);
        Iterator<Row> iter = rs.iterator();
        for (int c1 = 0; c1 < 100; c1++)
        {
            for (int c2 = 0; c2 < 100; c2++)
            {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                String msg = "On " + c1 + ',' + c2;
                assertEquals(msg, c1, row.getInt(0));
                assertEquals(msg, c2, row.getInt(1));
                assertEquals(msg, Integer.toString(c1), row.getString(2));
                assertEquals(msg, Integer.toString(c2), row.getString(3));
            }
        }
        assertFalse(iter.hasNext());

        for (int c1 = 0; c1 < 100; c1++)
        {
            stmt = new SimpleStatement("SELECT c1, c2, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE k1 = 1 AND c1 = ?", c1);
            stmt.setFetchSize(3);
            rs = session.execute(stmt);
            iter = rs.iterator();
            for (int c2 = 0; c2 < 100; c2++)
            {
                assertTrue(iter.hasNext());
                Row row = iter.next();
                String msg = "Within " + c1 + " on " + c2;
                assertEquals(msg, c1, row.getInt(0));
                assertEquals(msg, c2, row.getInt(1));
                assertEquals(msg, Integer.toString(c1), row.getString(2));
                assertEquals(msg, Integer.toString(c2), row.getString(3));
            }
            assertFalse(iter.hasNext());
        }
    }

    // new paging-in-bytes tests

    /**
     * Returns a lambda that creates a pager for the query
     */
    private Supplier<Pair<QueryPager, SelectStatement>> getPager(String query, Object... args)
    {
        return () -> {
            QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(String.format(query, args));
            SelectStatement select = (SelectStatement) prepared.statement;
            ReadQuery readQuery = select.getQuery(QueryState.forInternalCalls(), QueryProcessor.makeInternalOptions(prepared.statement, EMPTY_OBJECT_ARRAY), FBUtilities.nowInSeconds());
            QueryPager pager = select.getPager(readQuery, QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, Collections.emptyList()));
            return Pair.create(pager, select);
        };
    }

    /**
     * Inovke the test and check for the expected number of rows
     */
    private void assertResults(Supplier<Pair<QueryPager, SelectStatement>> pagerSupplier, int expectedCount)
    {
        Pair<QueryPager, SelectStatement> pagerAndStmt = pagerSupplier.get();
        QueryPager pager = pagerAndStmt.left;
        SelectStatement select = pagerAndStmt.right;

        List<List<ByteBuffer>> rows;

        int nowInSec = FBUtilities.nowInSeconds();
        assertThat(pager.isExhausted()).isFalse();
        try (ReadExecutionController executionController = pager.executionController();
             PartitionIterator iter = pager.fetchPageInternal(PageSize.NONE, executionController))
        {
            rows = select.process(iter, nowInSec).rows;
        }

        assertThat(rows.size()).isEqualTo(expectedCount);
        assertThat(pager.isExhausted()).isTrue();
    }

    /**
     * Invoke the tests with the provided page size. Firstly we just request the page size in rows as provided by the parameter.
     * In the second test we convert (by multiplying) the requested number of rows on page to the number of bytes (assuming certain row size).
     */
    private void assertResults(Supplier<Pair<QueryPager, SelectStatement>> pagerSupplier, int requestedPageSizeInRows, int expectedCountOnFirstPage, int expectedCount)
    {
        assertResults(pagerSupplier, PageSize.inRows(requestedPageSizeInRows), expectedCountOnFirstPage, expectedCount);
        assertResults(pagerSupplier, PageSize.inBytes(requestedPageSizeInRows * ROW_SIZE), expectedCountOnFirstPage, expectedCount);
    }

    /**
     * Invoke the tests with the provided page size. Firstly we just request the page size in rows as provided by the parameter.
     * In the second test we convert (by multiplying) the requested number of rows on page to the number of bytes (assuming certain row size).
     */
    private void assertResults(Supplier<Pair<QueryPager, SelectStatement>> pagerSupplier, int requestedPageSizeInRows, int expectedCountOnFirstPage, int expectedCount, int expectedValue)
    {
        List<List<ByteBuffer>> rows = assertResults(pagerSupplier, PageSize.inRows(requestedPageSizeInRows), expectedCountOnFirstPage, expectedCount);
        assertThat(ByteBufferUtil.toLong(rows.get(0).get(0))).isEqualTo((long) expectedValue);
        assertResults(pagerSupplier, PageSize.inBytes(requestedPageSizeInRows * ROW_SIZE), expectedCountOnFirstPage, expectedCount);
    }

    /**
     * Invoke the test with the provided page size. Expect the exact number of rows on the first page and exact number of rows in total (all pages).
     */
    private List<List<ByteBuffer>> assertResults(Supplier<Pair<QueryPager, SelectStatement>> pagerSupplier, PageSize requestedPageSize, int expectedCountOnFirstPage, int expectedCount)
    {
        Pair<QueryPager, SelectStatement> pagerAndStmt = pagerSupplier.get();
        QueryPager pager = pagerAndStmt.left;
        SelectStatement select = pagerAndStmt.right;

        List<List<ByteBuffer>> rows = null;

        int nowInSec = FBUtilities.nowInSeconds();

        int countOnFirstPage = -1;
        int count = 0;

        logger.info("Assertion on query {} with requested page size {} - expected count on first page = {}, expected count total = {}:", select.toString(), requestedPageSize, expectedCountOnFirstPage, expectedCount);

        try
        {
            while (!pager.isExhausted())
            {
                try (ReadExecutionController executionController = pager.executionController();
                     PartitionIterator iter = pager.fetchPageInternal(requestedPageSize, executionController))
                {
                    rows = select.process(iter, nowInSec).rows;
                    logger.info("Got page of {} rows with size: {}", rows.size(), rows.stream().mapToInt(cols -> cols.stream().mapToInt(Buffer::remaining).sum()).sum());
                }

                if (countOnFirstPage < 0)
                    countOnFirstPage = rows.size();
                count += rows.size();
            }

            assertThat(countOnFirstPage).isEqualTo(expectedCountOnFirstPage);
            assertThat(count).isEqualTo(expectedCount);
            assertThat(pager.isExhausted()).isTrue();
        }
        catch (OperationExecutionException ex)
        {
            if (pager instanceof AggregationQueryPager && requestedPageSize.getUnit() == PageSize.PageUnit.BYTES)
                return null;
        }

        if (pager instanceof AggregationQueryPager && requestedPageSize.getUnit() == PageSize.PageUnit.BYTES)
            fail("Expected " + OperationExecutionException.class.getSimpleName() + " to be thrown when paging is in bytes");

        return rows;
    }

    private void testPagingCases(String query, int selPartitions, int selClusterings, int genPartitions, int genClusterings) throws Throwable
    {
        testPagingCases(query, selPartitions, selClusterings, genPartitions, genClusterings, 1);
    }


    private void testPagingCases(String query, int selPartitions, int selClusterings, int genPartitions, int genClusterings, int genClusterings2) throws Throwable
    {
        String table = generateData(genPartitions, genClusterings, genClusterings2);

        flush(true);
        Supplier<Pair<QueryPager, SelectStatement>> pagerSupplier;
        query = String.format(query, KEYSPACE + '.' + table);
        int selected = selPartitions * selClusterings;

        // when there is a page size
        pagerSupplier = getPager("%s ALLOW FILTERING", query);
        assertResults(pagerSupplier, selected / 3, selected / 3, selected);

        // when there is a query limit
        pagerSupplier = getPager("%s LIMIT %d ALLOW FILTERING", query, selected / 3);
        assertResults(pagerSupplier, selected / 3);


        // when there is a per partition limit
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d ALLOW FILTERING", query, selClusterings / 2);
        assertResults(pagerSupplier, selPartitions * (selClusterings / 2));


        // when there is a page size and a query limit:

        // - where query limit is == page size
        pagerSupplier = getPager("%s LIMIT %d ALLOW FILTERING", query, selected / 2);
        assertResults(pagerSupplier, selected / 2, selected / 2, selected / 2);

        // - where query limit is < page size
        pagerSupplier = getPager("%s LIMIT %d ALLOW FILTERING", query, selected / 3);
        assertResults(pagerSupplier, selected / 2, selected / 3, selected / 3);

        // - where query limit is > page size
        pagerSupplier = getPager("%s LIMIT %d ALLOW FILTERING", query, selected / 2);
        assertResults(pagerSupplier, selected / 3, selected / 3, selected / 2);


        // when there is a per partition limit and a query limit:

        // - where query limit is < per partition limit
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 2, selClusterings / 3);
        assertResults(pagerSupplier, selClusterings / 3);

        // - where query limit is > per partition limit (case for single partition and multiple partitions)
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 3, selClusterings / 2);
        if (selPartitions == 1)
            assertResults(pagerSupplier, selClusterings / 3);
        else
            assertResults(pagerSupplier, selClusterings / 2);

        // - where query limit is == per partition limit
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 2, selClusterings / 2);
        assertResults(pagerSupplier, selClusterings / 2);

        // when there is a page size and a per partition limit,

        // - where page size is < per partition limit
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d ALLOW FILTERING", query, selClusterings / 2);
        assertResults(pagerSupplier, selClusterings / 3, selClusterings / 3, selPartitions * (selClusterings / 2));

        // - where page size is == per partition limit
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d ALLOW FILTERING", query, selClusterings / 2);
        assertResults(pagerSupplier, selClusterings / 2, selClusterings / 2, selPartitions * (selClusterings / 2));

        // - where page size is > per partition limit (case for single partition and mulitple partitions)
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d ALLOW FILTERING", query, selClusterings / 3);
        if (selPartitions == 1)
            assertResults(pagerSupplier, selClusterings / 2, selClusterings / 3, selClusterings / 3);
        else
            assertResults(pagerSupplier, selClusterings / 2, selClusterings / 2, selPartitions * (selClusterings / 3));


        // when there is a page size, a per partition limit and a query limit

        // - where per partition limit == query limit == page size
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 2, selClusterings / 2);
        assertResults(pagerSupplier, selClusterings / 2, selClusterings / 2, selClusterings / 2);

        // - where per partition limit > query limit > page size
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 2, selClusterings / 3);
        assertResults(pagerSupplier, selClusterings / 4, selClusterings / 4, selClusterings / 3);

        // - where per partition limit > page size > query limit
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 2, selClusterings / 4);
        assertResults(pagerSupplier, selClusterings / 3, selClusterings / 4, selClusterings / 4);

        // - where per query limit > per partition limit > page size (case for single partition and mulitple partitions)
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 3, selClusterings / 2);
        if (selPartitions == 1)
            assertResults(pagerSupplier, selClusterings / 4, selClusterings / 4, selClusterings / 3);
        else
            assertResults(pagerSupplier, selClusterings / 4, selClusterings / 4, selClusterings / 2);

        // - where per query limit > page size > per partition limit (case for single partition and mulitple partitions)
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 4, selClusterings / 2);
        if (selPartitions == 1)
            assertResults(pagerSupplier, selClusterings / 3, selClusterings / 4, selClusterings / 4);
        else
            assertResults(pagerSupplier, selClusterings / 3, selClusterings / 3, selClusterings / 2);

        // - where page size > per partition limit > query limit (case for single partition and mulitple partitions)
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 3, selClusterings / 4);
        if (selPartitions == 1)
            assertResults(pagerSupplier, selClusterings / 2, selClusterings / 4, selClusterings / 4);
        else
            assertResults(pagerSupplier, selClusterings / 2, selClusterings / 4, selClusterings / 4);

        // - where page size > query limit > per partition limit (case for single partition and mulitple partitions)
        pagerSupplier = getPager("%s PER PARTITION LIMIT %d LIMIT %d ALLOW FILTERING", query, selClusterings / 4, selClusterings / 3);
        if (selPartitions == 1)
            assertResults(pagerSupplier, selClusterings / 2, selClusterings / 4, selClusterings / 4);
        else
            assertResults(pagerSupplier, selClusterings / 2, selClusterings / 3, selClusterings / 3);
    }


    private void testPagingCasesWithAggregateEverything(String query, int genPartitions, int genClusterings, int genClusterings2, int expectedResult) throws Throwable
    {
        String table = generateData(genPartitions, genClusterings, genClusterings2);

        flush(true);
        Supplier<Pair<QueryPager, SelectStatement>> pagerSupplier;
        query = String.format(query, KEYSPACE + '.' + table);

        // when there is a page size
        pagerSupplier = getPager("%s ALLOW FILTERING", query);
        assertResults(pagerSupplier, 1, 1, 1, expectedResult);
    }

    private String generateData(int genPartitions, int genClusterings, int genClusterings2) throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k INT, c INT, c2 INT, v INT, PRIMARY KEY (k, c, c2))");
        for (int k = 0; k < genPartitions; k++)
        {
            for (int c = 0; c < genClusterings; c++)
            {
                for (int c2 = 0; c2 < genClusterings2; c2++)
                {
                    execute("INSERT INTO %s (k, c, c2, v) VALUES (?, ?, ?, ?)", k, c, c2, 1);
                    if ((k * genClusterings + c) % (3 * (genClusterings + genPartitions) / 2) == 0)
                        flush(true);
                }
            }
        }
        return table;
    }


    @Test
    public void testLimitsOnFullScanQuery() throws Throwable
    {
        testPagingCases("SELECT * FROM %s", 10, 10, 10, 10);
    }

    @Test
    public void testLimitsOnSliceSelection() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE c > 2 AND c <= 7", 10, 5, 10, 10);
    }

    @Test
    public void testLimitsOnClusteringsSelection() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE c IN (2, 4, 7, 8)", 10, 4, 10, 10);
    }

    @Test
    public void testLimitsOnSliceAndKeyRangeSelection() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE c > 2 AND c <= 7 AND TOKEN(k) > TOKEN(0)", 6, 5, 10, 10);
    }

    @Test
    public void testLimitsInSinglePartition() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE k = 5", 1, 100, 10, 100);
    }

    @Test
    public void testLimitsInMultiplePartitions() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE k IN (5, 7, 9)", 3, 100, 10, 100);
    }

    @Test
    public void testLimitsOnSliceInSinglePartition() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE c > 20 AND c <= 70 AND k = 5", 1, 50, 10, 100);
    }

    @Test
    public void testLimitsOnClusteringsInSinglePartitionSelection() throws Throwable
    {
        testPagingCases("SELECT * FROM %s WHERE c IN (2, 4, 7, 8) AND k = 5", 1, 4, 10, 10);
    }

    @Test
    public void testLimitsOnFullScanQueryWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s GROUP BY k, c", 10, 10, 10, 10, 10);
    }

    @Test
    public void testLimitsOnSliceSelectionWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s WHERE c > 2 AND c <= 7 GROUP BY k, c", 10, 5, 10, 10, 10);
    }

    @Test
    public void testLimitsOnClusteringsSelectionWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s WHERE c IN (2, 4, 7, 8) GROUP BY k, c", 10, 4, 10, 10, 10);
    }

    @Test
    public void testLimitsOnSliceAndKeyRangeSelectionWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s WHERE c > 2 AND c <= 7 AND TOKEN(k) > TOKEN(0) GROUP BY k, c", 6, 5, 10, 10, 10);
    }

    @Test
    public void testLimitsInSinglePartitionWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s WHERE k = 5 GROUP BY k, c", 1, 100, 10, 100, 10);
    }

    @Test
    public void testLimitsInMultiplePartitionsWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s WHERE k IN (5, 7, 9) GROUP BY k, c", 3, 100, 10, 100, 10);
    }

    @Test
    public void testLimitsOnSliceInSinglePartitionWithGrouping() throws Throwable
    {
        testPagingCases("SELECT k, c, SUM(v) FROM %s WHERE c > 20 AND c <= 70 AND k = 5 GROUP BY k, c", 1, 50, 10, 100, 10);
    }


    @Test
    public void testLimitsOnFullScanQueryWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s", 3, 3, 3, 27);
    }

    @Test
    public void testLimitsOnSliceSelectionWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s WHERE c > 2 AND c <= 7", 10, 10, 10, 500);
    }

    @Test
    public void testLimitsOnClusteringsSelectionWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s WHERE c IN (2, 4, 7, 8)", 10, 10, 10, 400);
    }

    @Test
    public void testLimitsOnSliceAndKeyRangeSelectionWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s WHERE c > 2 AND c <= 7 AND TOKEN(k) > TOKEN(0)", 10, 10, 10, 300);
    }

    @Test
    public void testLimitsInSinglePartitionWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s WHERE k = 5", 10, 10, 10, 100);
    }

    @Test
    public void testLimitsInMultiplePartitionsWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s WHERE k IN (5, 7, 9)", 10, 10, 10, 300);
    }

    @Test
    public void testLimitsOnSliceInSinglePartitionWithAggregateEverything() throws Throwable
    {
        testPagingCasesWithAggregateEverything("SELECT COUNT(*) FROM %s WHERE c > 2 AND c <= 7 AND k = 5", 10, 10, 10, 50);
    }


    private static String someText()
    {
        char[] arr = new char[1024];
        for (int i = 0; i < arr.length; i++)
            arr[i] = (char) (32 + ThreadLocalRandom.current().nextInt(95));
        return new String(arr);
    }
}
