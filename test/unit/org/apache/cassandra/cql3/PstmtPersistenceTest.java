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

public class PstmtPersistenceTest extends CQLTester
{
    @Test
    public void testCachedPreparedStatements() throws Throwable
    {
        // need this for pstmt execution/validation tests
        requireNetwork();

        int rows = QueryProcessor.executeOnceInternal("SELECT * FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS).size();
        Assert.assertEquals(0, rows);

        execute("CREATE KEYSPACE IF NOT EXISTS foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        execute("CREATE TABLE foo.bar (key text PRIMARY KEY, val int)");

        ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234));

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");

        List<MD5Digest> stmtIds = new ArrayList<>();
        // #0
        stmtIds.add(QueryProcessor.prepare("SELECT * FROM " + SchemaConstants.SCHEMA_KEYSPACE_NAME + '.' + SchemaKeyspace.TABLES + " WHERE keyspace_name = ?", clientState, false).statementId);
        // #1
        stmtIds.add(QueryProcessor.prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE pk = ?", clientState, false).statementId);
        // #2
        stmtIds.add(QueryProcessor.prepare("SELECT * FROM foo.bar WHERE key = ?", clientState, false).statementId);
        clientState.setKeyspace("foo");
        // #3
        stmtIds.add(QueryProcessor.prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE pk = ?", clientState, false).statementId);
        // #4
        stmtIds.add(QueryProcessor.prepare("SELECT * FROM foo.bar WHERE key = ?", clientState, false).statementId);

        Assert.assertEquals(5, stmtIds.size());
        Assert.assertEquals(5, QueryProcessor.preparedStatementsCount());

        String queryAll = "SELECT * FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS;

        rows = QueryProcessor.executeOnceInternal(queryAll).size();
        Assert.assertEquals(5, rows);

        QueryHandler handler = ClientState.getCQLQueryHandler();
        validatePstmts(stmtIds, handler);

        // clear prepared statements cache
        QueryProcessor.clearPrepraredStatements();
        Assert.assertEquals(0, QueryProcessor.preparedStatementsCount());
        for (MD5Digest stmtId : stmtIds)
            Assert.assertNull(handler.getPrepared(stmtId));

        // load prepared statements and validate that these still execute fine
        QueryProcessor.preloadPreparedStatement();
        validatePstmts(stmtIds, handler);

        // validate that the prepared statements are in the system table
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(queryAll))
        {
            MD5Digest digest = MD5Digest.wrap(ByteBufferUtil.getArray(row.getBytes("prepared_id")));
            ParsedStatement.Prepared prepared = QueryProcessor.instance.getPrepared(digest);
            Assert.assertNotNull(prepared);
        }

        // add anther prepared statement and sync it to table
        QueryProcessor.prepare("SELECT * FROM bar WHERE key = ?", clientState, false);
        Assert.assertEquals(6, QueryProcessor.preparedStatementsCount());
        rows = QueryProcessor.executeOnceInternal(queryAll).size();
        Assert.assertEquals(6, rows);

        // drop a keyspace (prepared statements are removed - syncPreparedStatements() remove should the rows, too)
        execute("DROP KEYSPACE foo");
        Assert.assertEquals(3, QueryProcessor.preparedStatementsCount());
        rows = QueryProcessor.executeOnceInternal(queryAll).size();
        Assert.assertEquals(3, rows);

    }

    private void validatePstmts(List<MD5Digest> stmtIds, QueryHandler handler)
    {
        Assert.assertEquals(5, QueryProcessor.preparedStatementsCount());
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
        Assert.assertNotNull(prepared);
        handler.processPrepared(prepared.statement, QueryState.forInternalCalls(), options, Collections.emptyMap(), System.nanoTime());
    }
}
