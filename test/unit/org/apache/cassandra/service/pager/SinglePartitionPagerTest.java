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

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SinglePartitionPagerTest extends QueryPagerTest
{
    protected ReadCommand makeSliceQuery(int limit, int perPartitionLimit, boolean isReversed)
    {
        return sliceQuery(limit, perPartitionLimit, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD), "k0", "c1", "c9", isReversed).build();
    }

    protected ReadCommand makeNamesQuery(int limit, int perPartitionLimit)
    {
        return namesQuery(limit, perPartitionLimit, PageSize.NONE, cfs(KEYSPACE1, CF_STANDARD),
                          "k0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8").build();
    }

    @Test
    public void sliceQueryTest()
    {
        ReadCommand cmd = makeSliceQuery(NO_LIMIT, NO_LIMIT, false);
        queryTest(false, cmd);
        queryTest(true, cmd);
    }

    @Test
    public void namesQueryTest()
    {
        ReadCommand cmd = makeNamesQuery(NO_LIMIT, NO_LIMIT);
        queryTest(false, cmd);
        queryTest(true, cmd);
    }

    public void queryTest(boolean testPagingState, ReadCommand cmd)
    {
        QueryPager pager = null;
        pager = checkNextPage(pager, cmd, testPagingState, PageSize.inRows(3), 3, p -> assertRow(p.get(0), "k0", "c1", "c2", "c3"));
        pager = checkNextPage(pager, cmd, testPagingState, PageSize.inRows(3), 3, p -> assertRow(p.get(0), "k0", "c4", "c5", "c6"));
        pager = checkNextPage(pager, cmd, testPagingState, PageSize.inRows(3), 2, p -> assertRow(p.get(0), "k0", "c7", "c8"));
        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryWithLimitsTest()
    {
        queryWithLimitsTest(false, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false));
        queryWithLimitsTest(true, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false));
    }

    @Test
    public void namesQueryWithLimitsTest()
    {
        queryWithLimitsTest(false, this::makeNamesQuery);
        queryWithLimitsTest(true, this::makeNamesQuery);
    }

    public void queryWithLimitsTest(boolean testPagingState, BiFunction<Integer, Integer, ReadCommand> cmdSupplier)
    {
        ReadCommand cmd;
        QueryPager pager;

        // Test with count < partitionCount

        int limit = 1;
        int perPartitionLimit = 2;
        cmd = cmdSupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, cmd, testPagingState, PageSize.NONE, 1, p -> assertRow(p.get(0), "k0", "c1"));
        assertTrue(pager.isExhausted());

        // Test with count > partitionCount

        limit = 2;
        perPartitionLimit = 1;
        cmd = cmdSupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, cmd, testPagingState, PageSize.NONE, 1, p -> assertRow(p.get(0), "k0", "c1"));
        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryWithPagingInRowsTest()
    {
        queryWithPagingTest(false, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false), this::pageSizeInRows);
        queryWithPagingTest(true, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false), this::pageSizeInRows);
    }

    @Test
    public void namesQueryWithPagingInRowsTest()
    {
        queryWithPagingTest(false, this::makeNamesQuery, this::pageSizeInRows);
        queryWithPagingTest(true, this::makeNamesQuery, this::pageSizeInRows);
    }

    @Test
    public void sliceQueryWithPagingInBytesTest()
    {
        queryWithPagingTest(false, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false), this::pageSizeInBytes);
        queryWithPagingTest(true, (limit, perPartitionLimit) -> makeSliceQuery(limit, perPartitionLimit, false), this::pageSizeInBytes);
    }

    @Test
    public void namesQueryWithPagingInBytesTest()
    {
        queryWithPagingTest(false, this::makeNamesQuery, this::pageSizeInBytes);
        queryWithPagingTest(true, this::makeNamesQuery, this::pageSizeInBytes);
    }

    public void queryWithPagingTest(boolean testPagingState, BiFunction<Integer, Integer, ReadCommand> cmdSupplier, IntFunction<PageSize> pageSizeSupplier)
    {
        ReadCommand cmd;
        QueryPager pager;

        int limit = 10;
        int perPartitionLimit = 5;
        cmd = cmdSupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, cmd, testPagingState, pageSizeSupplier.apply(3), 3, p -> assertRow(p.get(0), "k0", "c1", "c2", "c3"));
        pager = checkNextPage(pager, cmd, testPagingState, pageSizeSupplier.apply(3), 2, p -> assertRow(p.get(0), "k0", "c4", "c5"));
        assertTrue(pager.isExhausted());

        limit = 5;
        perPartitionLimit = 10;
        cmd = cmdSupplier.apply(limit, perPartitionLimit);
        pager = checkNextPage(null, cmd, testPagingState, pageSizeSupplier.apply(3), 3, p -> assertRow(p.get(0), "k0", "c1", "c2", "c3"));
        pager = checkNextPage(pager, cmd, testPagingState, pageSizeSupplier.apply(3), 2, p -> assertRow(p.get(0), "k0", "c4", "c5"));
        assertTrue(pager.isExhausted());
    }

    @Test
    public void reversedSliceQueryTest()
    {
        ReadCommand cmd = makeSliceQuery(NO_LIMIT, NO_LIMIT, true);
        reversedQueryTest(false, cmd);
        reversedQueryTest(true, cmd);
    }

    public void reversedQueryTest(boolean testPagingState, ReadCommand cmd)
    {
        QueryPager pager;

        pager = checkNextPage(null, cmd, testPagingState, PageSize.inRows(3), 3, p -> assertRow(p.get(0), "k0", "c6", "c7", "c8"));
        pager = checkNextPage(pager, cmd, testPagingState, PageSize.inRows(3), 3, p -> assertRow(p.get(0), "k0", "c3", "c4", "c5"));
        pager = checkNextPage(pager, cmd, testPagingState, PageSize.inRows(3), 2, p -> assertRow(p.get(0), "k0", "c1", "c2"));
        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryWithTombstoneTest()
    {
        sliceQueryWithTombstoneTest(false);
        sliceQueryWithTombstoneTest(true);
    }

    public void sliceQueryWithTombstoneTest(boolean testPagingState)
    {
        // Testing for the bug of #6748
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);

        // Insert rows but with a tombstone as last cell
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, v) VALUES ('k%d', 'c%d', null)", cfs.keyspace.getName(), cfs.name, 0, i));

        ReadCommand cmd = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, Util.dk("k0"), Slice.ALL);
        QueryPager pager = null;
        for (int i = 0; i < 5; i++)
        {
            String c = "c" + i;
            // The only live cell we should have each time is the row marker
            pager = checkNextPage(pager, cmd, testPagingState, PageSize.inRows(1), 1, p -> assertRow(p.get(0), "k0", c));
        }
    }

    @Test
    public void pagingReversedQueriesWithStaticColumnsTest()
    {
        // There was a bug in paging for reverse queries when the schema includes static columns in
        // 2.1 & 2.2. This was never a problem in 3.0, so this test just guards against regressions
        // see CASSANDRA-13222

        // insert some rows into a single partition
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, st, v1, v2) VALUES ('k0', '%3$s', %3$s, %3$s, %3$s)",
                                          KEYSPACE_CQL, PER_TEST_CF_CQL_WITH_STATIC, i));

        // query the table in reverse with page size = 1 & check that the returned rows contain the correct cells
        TableMetadata table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(PER_TEST_CF_CQL_WITH_STATIC).metadata();
        queryAndVerifyCells(table, true, "k0");
    }

    @Test
    public void toStringTest()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tab")
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addColumn(ColumnMetadata.regularColumn("ks", "tab", "v", Int32Type.instance))
                                              .build();

        DataLimits limits = DataLimits.cqlLimits(31, 29);

        Clustering clustering = Clustering.make(bytes(11));
        Row row = mock(Row.class);
        when(row.clustering()).thenReturn(clustering);
        when(row.isRow()).thenReturn(true);

        PagingState state = new PagingState(ByteBufferUtil.bytes(1), PagingState.RowMark.create(metadata, row, ProtocolVersion.CURRENT), 19, 17);

        SinglePartitionReadQuery singlePartitionReadQuery = mock(SinglePartitionReadQuery.class);
        when(singlePartitionReadQuery.metadata()).thenReturn(metadata);
        when(singlePartitionReadQuery.limits()).thenReturn(limits);
        when(singlePartitionReadQuery.partitionKey()).thenReturn(metadata.partitioner.decorateKey(ByteBufferUtil.bytes(1)));
        QueryPager singlePartitionPager = new SinglePartitionPager(singlePartitionReadQuery, state, ProtocolVersion.CURRENT);
        Assertions.assertThat(singlePartitionPager.toString())
                  .contains(limits.toString())
                  .contains("remaining=19")
                  .contains("remainingInPartition=17")
                  .contains("lastReturned=c=11")
                  .contains("lastCounter=null")
                  .contains("lastKey=DecoratedKey(00000001, 00000001)")
                  .contains("exhausted=false");
    }
}
