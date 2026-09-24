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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CASSANDRA-11745: static data must fill byte pages even alongside regular rows or in grouped subpages.
 */
public class BytePagingStaticColumnsTest extends CQLTester
{
    private static final int PARTITIONS = 3;
    private static final PageSize PAGE_SIZE = PageSize.inBytes(128);
    private static final String STATIC_VALUE = "s".repeat(1024);

    @Test
    public void staticPayloadCountsWithRegularRows()
    {
        // HARNESS: local range and IN pagers exercise the real counters without requiring replicas.
        createStaticPartitions();
        String[] queries = { "SELECT * FROM %s", "SELECT * FROM %s WHERE pk IN (0, 1, 2)" };

        // Static-only partitions already consume the byte budget.
        for (String query : queries)
            assertSinglePartitionPages(query, PAGE_SIZE, false);

        // TRIGGER: adding a small regular row must not make the static payload disappear from the budget.
        for (int pk = 0; pk < PARTITIONS; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, 0, ?)", pk, pk);

        for (String query : queries)
            assertSinglePartitionPages(query, PageSize.inRows(1), true);

        // ORACLE: each 1 KiB static value fills a 128-byte page; both static and regular data must survive paging.
        for (String query : queries)
            assertSinglePartitionPages(query, PAGE_SIZE, true);
    }

    @Test
    public void groupedStaticOnlySubpagesStopAtByteLimit()
    {
        // HARNESS: use the raw pager that supplies internal subpages to AggregationQueryPager.
        createStaticPartitions();
        String query = "SELECT * FROM %s WHERE pk IN (0, 1, 2)";
        assertSinglePartitionPages(query, PAGE_SIZE, false);

        // TRIGGER: every child pager is exhausted after its static row, while further partitions remain.
        // ORACLE: GROUP BY must stop the subpage after one oversized static row and resume the remaining groups.
        assertSinglePartitionPages(query + " GROUP BY pk", PAGE_SIZE, false);
    }

    private void createStaticPartitions()
    {
        createTable("CREATE TABLE %s (pk int, ck int, s text static, v int, PRIMARY KEY (pk, ck))");
        for (int pk = 0; pk < PARTITIONS; pk++)
            execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, STATIC_VALUE);
    }

    private void assertSinglePartitionPages(String cql, PageSize pageSize, boolean hasRegularRows)
    {
        String queryText = String.format(cql, KEYSPACE + '.' + currentTable());
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(queryText).prepare(ClientState.forInternalCalls());
        ReadQuery query = statement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        QueryPager pager = query.getPager(null, ProtocolVersion.CURRENT);
        ColumnMetadata staticColumn = getCurrentColumnFamilyStore().metadata().getColumn(ByteBufferUtil.bytes("s"));
        ColumnMetadata valueColumn = getCurrentColumnFamilyStore().metadata().getColumn(ByteBufferUtil.bytes("v"));
        List<Integer> returned = new ArrayList<>();

        for (int page = 0; !pager.isExhausted(); page++)
        {
            assertThat(page).as("pager must finish after three partitions and at most one empty final page")
                            .isLessThanOrEqualTo(PARTITIONS);
            int before = returned.size();
            try (ReadExecutionController controller = pager.executionController();
                 PartitionIterator partitions = pager.fetchPageInternal(pageSize, controller))
            {
                while (partitions.hasNext())
                {
                    try (RowIterator partition = partitions.next())
                    {
                        int pk = Int32Type.instance.compose(partition.partitionKey().getKey());
                        returned.add(pk);
                        assertThat(partition.staticRow().getCell(staticColumn)).isNotNull();
                        assertThat(UTF8Type.instance.compose(partition.staticRow().getCell(staticColumn).buffer()))
                            .isEqualTo(STATIC_VALUE);

                        int rows = 0;
                        while (partition.hasNext())
                        {
                            Row row = partition.next();
                            assertThat(Int32Type.instance.compose(row.clustering().bufferAt(0))).isZero();
                            assertThat(row.getCell(valueColumn)).isNotNull();
                            assertThat(Int32Type.instance.compose(row.getCell(valueColumn).buffer())).isEqualTo(pk);
                            rows++;
                        }
                        assertThat(rows).as("regular rows in partition %s", pk).isEqualTo(hasRegularRows ? 1 : 0);
                    }
                }
            }

            int partitionsInPage = returned.size() - before;
            if (partitionsInPage == 0)
                assertThat(pager.isExhausted()).as("empty page must exhaust the pager").isTrue();
            else
                assertThat(partitionsInPage).as("partitions in page %s of %s with page size %s", page, queryText, pageSize)
                                            .isEqualTo(1);
        }
        assertThat(returned).as("every partition must be returned exactly once").containsExactlyInAnyOrder(0, 1, 2);
    }
}
