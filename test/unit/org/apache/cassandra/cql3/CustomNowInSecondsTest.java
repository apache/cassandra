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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

public class CustomNowInSecondsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();

        prepareServer();
        requireNetwork();
    }

    @Test
    public void testSelectQuery()
    {
        testSelectQuery(false);
        testSelectQuery(true);
    }

    private void testSelectQuery(boolean prepared)
    {
        int day = 86400;

        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int primary key, val int)");

        // insert a row with TTL = 1 day.
        executeModify(format("INSERT INTO %s.%s (id, val) VALUES (0, 0) USING TTL %d", ks, tbl, day), Long.MIN_VALUE, prepared);

        long now = FBUtilities.nowInSeconds();

        // execute a SELECT query without overriding nowInSeconds - make sure we observe one row.
        assertEquals(1, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), Integer.MIN_VALUE, prepared).size());

        // execute a SELECT query with nowInSeconds set to [now + 1 day + 1], when the row should have expired.
        assertEquals(0, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + day + 1, prepared).size());
    }

    @Test
    public void testSelectQueryOverflowingIntTimestamps()
    {
        testSelectQueryOverflowingIntTimestamps(false);
        testSelectQueryOverflowingIntTimestamps(true);
    }

    private void testSelectQueryOverflowingIntTimestamps(boolean prepared)
    {
        ExpirationDateOverflowPolicy origPolicy = ExpirationDateOverflowHandling.policy;
        ExpirationDateOverflowHandling.policy = ExpirationDateOverflowPolicy.REJECT;

        int ttl = Attributes.MAX_TTL - 10; // Give it a TTL that should overflow 'int' timestamps

        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int primary key, val int)");

        // insert a row with an int overflowing timestamp. Behavior will depend on the used sstable version
        String query = format("INSERT INTO %s.%s (id, val) VALUES (0, 0) USING TTL %d", ks, tbl, ttl);

        if (Cell.getVersionedMaxDeletiontionTime() == Cell.MAX_DELETION_TIME_2038_LEGACY_CAP)
        {
            Assertions.assertThatThrownBy(() -> executeModify(query, Long.MIN_VALUE, prepared))
                      .isInstanceOf(InvalidRequestException.class)
                      .hasMessageContaining("exceeds maximum supported expiration date");
        }
        else
        {
            executeModify(query, Long.MIN_VALUE, prepared);

            long now = FBUtilities.nowInSeconds();

            // execute a SELECT query without overriding nowInSeconds - make sure we observe one row.
            assertEquals(1, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), Long.MIN_VALUE, prepared).size());

            // execute a SELECT query with nowInSeconds set to [now + ttl + 1], when the row should have expired.
            assertEquals(0, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + ttl + 1, prepared).size());
        }

        ExpirationDateOverflowHandling.policy = origPolicy;
    }

    @Test
    public void testModifyQuery()
    {
        testModifyQuery(false);
        testModifyQuery(true);
    }

    private void testModifyQuery(boolean prepared)
    {
        long now = FBUtilities.nowInSeconds();
        int day = 86400;

        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int primary key, val int)");

        // execute an INSERT query with now set to [now + 1 day], with ttl = 1, making its effective ttl = 1 day + 1.
        executeModify(format("INSERT INTO %s.%s (id, val) VALUES (0, 0) USING TTL %d", ks, tbl, 1), now + day, prepared);

        // verify that despite TTL having passed (if not for nowInSeconds override) the row is still there.
        assertEquals(1, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + 1, prepared).size());

        // jump in time by one day, make sure the row expired
        assertEquals(0, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + day + 1, prepared).size());
    }

    @Test
    public void testBatchQuery()
    {
        testBatchQuery(false);
        testBatchQuery(true);
    }

    private void testBatchQuery(boolean prepared)
    {
        long now = FBUtilities.nowInSeconds();
        int day = 86400;

        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int primary key, val int)");

        // execute an BATCH query with now set to [now + 1 day], with ttl = 1, making its effective ttl = 1 day + 1.
        String batch = format("BEGIN BATCH " +
                              "INSERT INTO %s.%s (id, val) VALUES (0, 0) USING TTL %d; " +
                              "INSERT INTO %s.%s (id, val) VALUES (1, 1) USING TTL %d; " +
                              "APPLY BATCH;",
                              ks, tbl, 1,
                              ks, tbl, 1);
        executeModify(batch, now + day, prepared);

        // verify that despite TTL having passed at now + 1 the rows are still there.
        assertEquals(2, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + 1, prepared).size());

        // jump in time by one day, make sure the row expired.
        assertEquals(0, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + day + 1, prepared).size());
    }

    @Test
    public void testBatchMessage()
    {
        // test BatchMessage path

        long now = FBUtilities.nowInSeconds();
        int day = 86400;

        String ks = createKeyspace("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        String tbl = createTable(ks, "CREATE TABLE %s (id int primary key, val int)");

        List<String> queries = ImmutableList.of(
            format("INSERT INTO %s.%s (id, val) VALUES (0, 0) USING TTL %d;", ks, tbl, 1),
            format("INSERT INTO %s.%s (id, val) VALUES (1, 1) USING TTL %d;", ks, tbl, 1)
        );

        ClientState cs = ClientState.forInternalCalls();
        QueryState qs = new QueryState(cs);

        List<ModificationStatement> statements = new ArrayList<>(queries.size());
        for (String query : queries)
            statements.add((ModificationStatement) QueryProcessor.parseStatement(query, cs));

        BatchStatement batch =
            new BatchStatement(BatchStatement.Type.UNLOGGED, VariableSpecifications.empty(), statements, Attributes.none());

        // execute an BATCH message with now set to [now + 1 day], with ttl = 1, making its effective ttl = 1 day + 1.
        QueryProcessor.instance.processBatch(batch, qs, batchQueryOptions(now + day), emptyMap(), nanoTime());

        // verify that despite TTL having passed at now + 1 the rows are still there.
        assertEquals(2, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + 1, false).size());

        // jump in time by one day, make sure the row expired.
        assertEquals(0, executeSelect(format("SELECT * FROM %s.%s", ks, tbl), now + day + 1, false).size());
    }

    private static ResultSet executeSelect(String query, long nowInSeconds, boolean prepared)
    {
        ResultMessage message = execute(query, nowInSeconds, prepared);
        return ((ResultMessage.Rows) message).result;
    }

    private static void executeModify(String query, long nowInSeconds, boolean prepared)
    {
        execute(query, nowInSeconds, prepared);
    }

    // prepared = false tests QueryMessage path, prepared = true tests ExecuteMessage path
    private static ResultMessage execute(String query, long nowInSeconds, boolean prepared)
    {
        ClientState cs = ClientState.forInternalCalls();
        QueryState qs = new QueryState(cs);

        if (prepared)
        {
            CQLStatement statement = QueryProcessor.parseStatement(query, cs);
            return QueryProcessor.instance.processPrepared(statement, qs, queryOptions(nowInSeconds), emptyMap(), nanoTime());
        }
        else
        {
            CQLStatement statement = QueryProcessor.instance.parse(query, qs, queryOptions(nowInSeconds));
            return QueryProcessor.instance.process(statement, qs, queryOptions(nowInSeconds), emptyMap(), nanoTime());
        }
    }

    private static QueryOptions queryOptions(long nowInSeconds)
    {
        return QueryOptions.create(ConsistencyLevel.ONE,
                                   Collections.emptyList(),
                                   false,
                                   Integer.MAX_VALUE,
                                   null,
                                   null,
                                   ProtocolVersion.CURRENT,
                                   null,
                                   Long.MIN_VALUE,
                                   nowInSeconds);
    }

    private static BatchQueryOptions batchQueryOptions(long nowInSeconds)
    {
        return BatchQueryOptions.withoutPerStatementVariables(queryOptions(nowInSeconds));
    }
}
