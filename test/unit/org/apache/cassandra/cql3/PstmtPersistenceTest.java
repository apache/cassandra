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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;

import static org.junit.Assert.*;

public class PstmtPersistenceTest extends CQLTester
{
    @Before
    public void setUp()
    {
        QueryProcessor.clearPreparedStatements(false);
    }
 
    @Test
    public void testCachedPreparedStatements() throws Throwable
    {
        // need this for pstmt execution/validation tests
        requireNetwork();

        assertEquals(0, numberOfStatementsOnDisk());

        execute("CREATE KEYSPACE IF NOT EXISTS foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        execute("CREATE TABLE foo.bar (key text PRIMARY KEY, val int)");

        ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234));

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");

        List<MD5Digest> stmtIds = new ArrayList<>();
        // #0
        stmtIds.add(prepareStatement("SELECT * FROM %s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspace.TABLES, clientState));
        // #1
        stmtIds.add(prepareStatement("SELECT * FROM %s WHERE pk = ?", clientState));
        // #2
        stmtIds.add(prepareStatement("SELECT * FROM %s WHERE key = ?", "foo", "bar", clientState));
        clientState.setKeyspace("foo");
        // #3
        stmtIds.add(prepareStatement("SELECT * FROM %s WHERE pk = ?", clientState));
        // #4
        stmtIds.add(prepareStatement("SELECT * FROM %S WHERE key = ?", "foo", "bar", clientState));

        assertEquals(5, stmtIds.size());
        assertEquals(5, QueryProcessor.preparedStatementsCount());

        Assert.assertEquals(5, numberOfStatementsOnDisk());

        QueryHandler handler = ClientState.getCQLQueryHandler();
        validatePstmts(stmtIds, handler);

        // clear prepared statements cache
        QueryProcessor.clearPreparedStatements(true);
        Assert.assertEquals(0, QueryProcessor.preparedStatementsCount());
        for (MD5Digest stmtId : stmtIds)
            Assert.assertNull(handler.getPrepared(stmtId));

        // load prepared statements and validate that these still execute fine
        QueryProcessor.preloadPreparedStatement();
        validatePstmts(stmtIds, handler);


        // validate that the prepared statements are in the system table
        String queryAll = "SELECT * FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS;
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(queryAll))
        {
            MD5Digest digest = MD5Digest.wrap(ByteBufferUtil.getArray(row.getBytes("prepared_id")));
            ParsedStatement.Prepared prepared = QueryProcessor.instance.getPrepared(digest);
            Assert.assertNotNull(prepared);
        }

        // add anther prepared statement and sync it to table
        prepareStatement("SELECT * FROM %s WHERE key = ?", "foo", "bar", clientState);
        assertEquals(6, numberOfStatementsInMemory());
        assertEquals(6, numberOfStatementsOnDisk());

        // drop a keyspace (prepared statements are removed - syncPreparedStatements() remove should the rows, too)
        execute("DROP KEYSPACE foo");
        assertEquals(3, numberOfStatementsInMemory());
        assertEquals(3, numberOfStatementsOnDisk());
    }

    private void validatePstmts(List<MD5Digest> stmtIds, QueryHandler handler)
    {
        assertEquals(5, QueryProcessor.preparedStatementsCount());
        QueryOptions optionsStr = QueryOptions.forInternalCalls(Collections.singletonList(UTF8Type.instance.fromString("foobar")));
        QueryOptions optionsInt = QueryOptions.forInternalCalls(Collections.singletonList(Int32Type.instance.decompose(42)));
        validatePstmt(handler, stmtIds.get(0), optionsStr);
        validatePstmt(handler, stmtIds.get(1), optionsInt);
        validatePstmt(handler, stmtIds.get(2), optionsStr);
        validatePstmt(handler, stmtIds.get(3), optionsInt);
        validatePstmt(handler, stmtIds.get(4), optionsStr);
    }

    private static void validatePstmt(QueryHandler handler, MD5Digest stmtId, QueryOptions options)
    {
        ParsedStatement.Prepared prepared = handler.getPrepared(stmtId);
        assertNotNull(prepared);
        handler.processPrepared(prepared.statement, QueryState.forInternalCalls(), options, Collections.emptyMap(), System.nanoTime());
    }

    @Test
    public void testPstmtInvalidation() throws Throwable
    {
        ClientState clientState = ClientState.forInternalCalls();

        createTable("CREATE TABLE %s (key int primary key, val int)");

        for (int cnt = 1; cnt < 10000; cnt++)
        {
            prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt, clientState);

            if (numberOfEvictedStatements() > 0)
            {
                assertEquals("Number of statements in table and in cache don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk());

                // prepare a more statements to trigger more evictions
                for (int cnt2 = 1; cnt2 < 10; cnt2++)
                    prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt2, clientState);

                // each new prepared statement should have caused an eviction
                assertEquals("eviction count didn't increase by the expected number", numberOfEvictedStatements(), 10);
                assertEquals("Number of statements in table and in cache don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk());

                return;
            }
        }

        fail("Prepared statement eviction does not work");
    }

    private long numberOfStatementsOnDisk() throws Throwable
    {
        UntypedResultSet.Row row = execute("SELECT COUNT(*) FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS).one();
        return row.getLong("count");
    }

    private long numberOfStatementsInMemory()
    {
        return QueryProcessor.preparedStatementsCount();
    }

    private long numberOfEvictedStatements()
    {
        return QueryProcessor.metrics.preparedStatementsEvicted.getCount();
    }

    private MD5Digest prepareStatement(String stmt, ClientState clientState)
    {
        return prepareStatement(stmt, keyspace(), currentTable(), clientState);
    }

    private MD5Digest prepareStatement(String stmt, String keyspace, String table, ClientState clientState)
    {
        return QueryProcessor.prepare(String.format(stmt, keyspace + "." + table), clientState, false).statementId;
    }
}
