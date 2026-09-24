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
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Byte paging must not report a pager as exhausted while live rows remain.
 * <p>
 * A row written by {@code INSERT ... USING TTL} whose other cell was written without a TTL stays live after the row
 * marker expires. The replica-side counter ({@code ReadCommand.executeLocally}) sizes such a row before purge, when
 * the expired marker still counts as an expiring {@code LivenessInfo}. The pager counts the same row after
 * {@code Filter} has purged the marker to {@code LivenessInfo.EMPTY}, which is smaller. The replica therefore stops
 * at the byte limit while the pager sees fewer bytes than the limit, concludes that the data is exhausted, and ends
 * the query early.
 */
public class BytePagingExpiredLivenessTest extends CQLTester
{
    private static final int ROWS = 100;
    private static final int TTL = 60;

    // Read after the row markers have expired but well within gc_grace_seconds, so the replica does not purge them.
    private static final long READ_NOW_IN_SEC = FBUtilities.nowInSeconds() + 3600;

    @Test
    public void testExpiredRowLivenessDoesNotExhaustPager()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        for (int ck = 0; ck < ROWS; ck++)
        {
            execute("INSERT INTO %s (pk, ck, a) VALUES (0, ?, 0) USING TTL " + TTL, ck);
            execute("UPDATE %s SET b = ? WHERE pk = 0 AND ck = ?", ck, ck);
        }

        assertPagesThroughAllRows();
    }

    @Test
    public void testLiveRowLivenessPagesThroughAllRows()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        for (int ck = 0; ck < ROWS; ck++)
        {
            execute("INSERT INTO %s (pk, ck, a) VALUES (0, ?, 0)", ck);
            execute("UPDATE %s SET b = ? WHERE pk = 0 AND ck = ?", ck, ck);
        }

        assertPagesThroughAllRows();
    }

    private void assertPagesThroughAllRows()
    {
        ReadQuery query = readQuery();

        List<Row> unpaged = readUnpaged(query);
        assertThat(unpaged).as("every row must still be live through its non-TTL cell").hasSize(ROWS);

        // A page of exactly N pager-side rows. When the replica sizes rows larger than the pager does, the replica
        // stops before N rows, and the pager counts less than the page size for the rows it got.
        int pagerRowSize = unpaged.get(0).liveDataSize(READ_NOW_IN_SEC);
        PageSize pageSize = PageSize.inBytes(10 * pagerRowSize);

        QueryPager pager = query.getPager(null, ProtocolVersion.CURRENT);
        List<Integer> returned = new ArrayList<>();
        while (!pager.isExhausted())
        {
            int before = returned.size();
            returned.addAll(fetchPage(pager, pageSize));

            if (pager.isExhausted() && returned.size() < ROWS)
            {
                throw new AssertionError(String.format("Pager reported exhausted after a page of %d rows, having returned %d of %d live rows " +
                                                       "(%d bytes per row on the pager side, page size %s). Rows ck > %d were never returned.",
                                                       returned.size() - before, returned.size(), ROWS, pagerRowSize, pageSize,
                                                       returned.isEmpty() ? -1 : returned.get(returned.size() - 1)));
            }
        }

        List<Integer> expected = new ArrayList<>();
        for (int ck = 0; ck < ROWS; ck++)
            expected.add(ck);
        assertThat(returned).isEqualTo(expected);
    }

    private ReadQuery readQuery()
    {
        String cql = String.format("SELECT * FROM %s.%s WHERE pk = 0", KEYSPACE, currentTable());
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(cql).prepare(ClientState.forInternalCalls());
        return statement.getQuery(QueryOptions.DEFAULT, READ_NOW_IN_SEC);
    }

    private static List<Row> readUnpaged(ReadQuery query)
    {
        List<Row> rows = new ArrayList<>();
        try (ReadExecutionController controller = query.executionController();
             PartitionIterator partitions = query.executeInternal(controller))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        rows.add(partition.next());
                }
            }
        }
        return rows;
    }

    private static List<Integer> fetchPage(QueryPager pager, PageSize pageSize)
    {
        List<Integer> clusterings = new ArrayList<>();
        try (ReadExecutionController controller = pager.executionController();
             PartitionIterator partitions = pager.fetchPageInternal(pageSize, controller))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                        clusterings.add(Int32Type.instance.compose(partition.next().clustering().bufferAt(0)));
                }
            }
        }
        return clusterings;
    }
}
