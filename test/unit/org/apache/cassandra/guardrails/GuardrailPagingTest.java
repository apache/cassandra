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

package org.apache.cassandra.guardrails;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GuardrailPagingTest extends GuardrailTester
{
    private static final String PARTITION_RANGE_QUERY = "SELECT * FROM %s.%s";
    private static final String SINGLE_PARTITION_QUERY = "SELECT * FROM %s.%s WHERE k = 5";
    private static final String MULTI_PARTITION_QUERY = "SELECT * FROM %s.%s WHERE k IN (1, 3, 5)";

    private static int defaultPageSizeThreshold;
    private static final int pageSizeThresholdInKB = 5;

    private static final int partitionCount = 10;
    private static final int rowsPerPartition = 100;

    @Parameterized.Parameters(name = "q={0},size={1}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(new Object[]{ PARTITION_RANGE_QUERY, partitionCount * rowsPerPartition },
                             new Object[]{ SINGLE_PARTITION_QUERY, rowsPerPartition },
                             new Object[]{ MULTI_PARTITION_QUERY, 3 * rowsPerPartition });
    }

    @Parameterized.Parameter(0)
    public String query;

    @Parameterized.Parameter(1)
    public int limit;

    @BeforeClass
    public static void setup()
    {
        defaultPageSizeThreshold = DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb;
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb = defaultPageSizeThreshold;
    }

    @Before
    public void setUp() throws Throwable
    {
        DatabaseDescriptor.getGuardrailsConfig().page_size_failure_threshold_in_kb = pageSizeThresholdInKB;

        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");

        for (int i = 0; i < partitionCount; i++)
            for (int j = 0; j < rowsPerPartition; j++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, "long long test message bla bla bla bla bla bla bla bla bla bla bla");
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.page_size_failure_threshold_in_kb = v.intValue(),
                                                 "page_size_failure_threshold_in_kb");
    }

    private ResultMessage.Rows selectWithPaging(String query, PageSize pageSize, ClientState clientState) throws InvalidRequestException
    {
        QueryOptions options = QueryOptions.create(ConsistencyLevel.LOCAL_QUORUM,
                                                   Collections.emptyList(),
                                                   false,
                                                   pageSize,
                                                   null,
                                                   ConsistencyLevel.LOCAL_SERIAL,
                                                   ProtocolVersion.CURRENT,
                                                   KEYSPACE);

        clientState.setKeyspace(KEYSPACE);
        QueryState queryState = new QueryState(clientState);

        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(String.format(query, KEYSPACE, currentTable()));
        return (ResultMessage.Rows) prepared.statement.execute(queryState, options, System.nanoTime());
    }

    private ResultMessage.Rows testQueryWithPagedByRows(String query, PageSize pageSize, int rowLimit) throws Throwable
    {
        ResultMessage.Rows result = selectWithPaging(query, pageSize, ClientState.forExternalCalls(AuthenticatedUser.ANONYMOUS_USER));
        Assertions.assertThat(result.result.rows.size()).isLessThan(rowLimit);
        return result;
    }

    /**
     * Test that the number of returned rows per page is silently limited to fit into the guardrail hard limit
     */
    @Test
    public void testPartitionQueryWithPagedByRows() throws Throwable
    {
        // ask for more rows per page than can fit with the current guardrail
        testQueryWithPagedByRows(query, PageSize.inRows(limit), limit);
    }

    /**
     * Test that a query throws with page size that is bigger than the guardrail hard limit
     */
    @Test(expected = InvalidRequestException.class)
    public void testQueryWithLargeBytePagesThrows() throws Throwable
    {
        testQueryWithPagedByRows(query, PageSize.inBytes(10 * 1024), limit);
    }

    /**
     * Test that a query does not throw with page size that is smaller than the guardrail hard limit
     */
    @Test
    public void testQueryWithSmallBytePagesWorks() throws Throwable
    {
        int maxPageSize = 2 * 1024;
        ResultMessage.Rows result = testQueryWithPagedByRows(query, PageSize.inBytes(maxPageSize), limit);
        // technically incorrect as we compare a size of encoded message to be sent to a client to the page size,
        // but we can't know the page at this point.
        assertTrue(ResultMessage.codec.encodedSize(result, ProtocolVersion.CURRENT) < maxPageSize);
    }

    /**
     * Test that superusers and internal queries are excluded from the guardrail.
     */
    @Test
    public void testExcludedUsers()
    {
        selectWithPaging(query, PageSize.inBytes(10 * 1024), ClientState.forInternalCalls());
        selectWithPaging(query, PageSize.inBytes(10 * 1024), ClientState.forExternalCalls(new AuthenticatedUser("cassandra")));
    }
}
