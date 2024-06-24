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

package org.apache.cassandra.cql3.validation.miscellaneous;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

public class PreparedStatementsCacheTest
{
    private static final String KEYSPACE = "utest";
    private static final int MAX_CAPACITY = 3;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setUseWeightBasedPreparedStatementsCache(false);
        DatabaseDescriptor.setPreparedStatementsCacheMaxCapacity(MAX_CAPACITY);
        SchemaLoader.loadSchema();
        execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        execute(String.format("CREATE TABLE " + KEYSPACE + ".test_cachecap (a int primary key, b int)"));
    }

    @Test
    public void testUseMaxCapacityPreparedStatementCache() throws Throwable
    {
        // nothing evicted
        Assert.assertEquals(0, QueryProcessor.metrics.preparedStatementsEvicted.getCount());

        for (int i = 0; i < MAX_CAPACITY + 5; i++)
        {
            QueryProcessor.instance.prepare(String.format("SELECT b FROM %s.test_cachecap where a = %s", KEYSPACE, i), ClientState.forInternalCalls());
        }

        Assert.assertEquals(MAX_CAPACITY, QueryProcessor.instance.getPreparedStatements().size());

        // should have 5 evicted
        Assert.assertEquals(5, QueryProcessor.metrics.preparedStatementsEvicted.getCount());
    }

    private static void execute(String query)
    {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(ClientState.forInternalCalls());
        CQLStatement statement = QueryProcessor.parseStatement(query, queryState.getClientState());
        statement.validate(state);
        QueryOptions options = QueryOptions.forInternalCalls(Collections.<ByteBuffer>emptyList());
        statement.executeLocally(queryState, options);
    }
}
