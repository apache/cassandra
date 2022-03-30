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

package org.apache.cassandra.db.guardrails;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;

/**
 * Tests the guardrail for the page size, {@link Guardrails#pageSize}.
 */
public class GuardrailPageSizeTest extends ThresholdTester
{
    private static final int PAGE_SIZE_WARN_THRESHOLD = 5;
    private static final int PAGE_SIZE_FAIL_THRESHOLD = 10;

    public GuardrailPageSizeTest()
    {
        super(PAGE_SIZE_WARN_THRESHOLD,
              PAGE_SIZE_FAIL_THRESHOLD,
              Guardrails.pageSize,
              Guardrails::setPageSizeThreshold,
              Guardrails::getPageSizeWarnThreshold,
              Guardrails::getPageSizeFailThreshold);
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @Test
    public void testSelectStatementAgainstPageSize() throws Throwable
    {
        // regular query
        String query = "SELECT * FROM %s";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_SIZE_WARN_THRESHOLD);
        assertPagingWarns(query, 6);
        assertPagingWarns(query, PAGE_SIZE_FAIL_THRESHOLD);
        assertPagingFails(query, 11);

        // aggregation query
        query = "SELECT COUNT(*) FROM %s WHERE k=0";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_SIZE_WARN_THRESHOLD);
        assertPagingWarns(query, 6);
        assertPagingWarns(query, PAGE_SIZE_FAIL_THRESHOLD);
        assertPagingFails(query, 11);

        // query with limit over thresholds
        query = "SELECT * FROM %s LIMIT 100";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_SIZE_WARN_THRESHOLD);
        assertPagingWarns(query, 6);
        assertPagingWarns(query, PAGE_SIZE_FAIL_THRESHOLD);
        assertPagingFails(query, 11);

        // query with limit under thresholds
        query = "SELECT * FROM %s LIMIT 1";
        assertPagingValid(query, 3);
        assertPagingValid(query, PAGE_SIZE_WARN_THRESHOLD);
        assertPagingValid(query, 6);
        assertPagingValid(query, PAGE_SIZE_FAIL_THRESHOLD);
        assertPagingValid(query, 11);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        assertPagingIgnored("SELECT * FROM %s", PAGE_SIZE_WARN_THRESHOLD + 1);
        assertPagingIgnored("SELECT * FROM %s", PAGE_SIZE_FAIL_THRESHOLD + 1);
    }

    private void assertPagingValid(String query, int pageSize) throws Throwable
    {
        assertValid(() -> executeWithPaging(userClientState, query, pageSize));
    }

    private void assertPagingIgnored(String query, int pageSize) throws Throwable
    {
        assertValid(() -> executeWithPaging(superClientState, query, pageSize));
        assertValid(() -> executeWithPaging(systemClientState, query, pageSize));
    }

    private void assertPagingWarns(String query, int pageSize) throws Throwable
    {
        assertWarns(() -> executeWithPaging(userClientState, query, pageSize),
                    format("Query for table %s with page size %s exceeds warning threshold of %s.",
                           currentTable(), pageSize, PAGE_SIZE_WARN_THRESHOLD));
    }

    private void assertPagingFails(String query, int pageSize) throws Throwable
    {
        assertFails(() -> executeWithPaging(userClientState, query, pageSize),
                    format("Aborting query for table %s, page size %s exceeds fail threshold of %s.",
                           currentTable(), pageSize, PAGE_SIZE_FAIL_THRESHOLD));
    }

    private void executeWithPaging(ClientState state, String query, int pageSize)
    {
        QueryState queryState = new QueryState(state);

        String formattedQuery = formatQuery(query);
        CQLStatement statement = QueryProcessor.parseStatement(formattedQuery, queryState.getClientState());
        statement.validate(state);

        QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE,
                                                   Collections.emptyList(),
                                                   false,
                                                   pageSize,
                                                   null,
                                                   null,
                                                   ProtocolVersion.CURRENT,
                                                   KEYSPACE);

        statement.executeLocally(queryState, options);
    }
}
