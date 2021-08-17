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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.google.common.collect.Lists.newArrayList;

public class QueryEventsTest extends CQLTester
{
    @Test
    public void queryTest() throws Throwable
    {
        createTable("create table %s (id int primary key, v int)");
        MockListener listener = new MockListener(getCurrentColumnFamilyStore());
        QueryEvents.instance.registerListener(listener);
        String query = formatQuery("insert into %s (id, v) values (1, 1)");
        executeNet(query);
        listener.verify("querySuccess", 1);
        assertEquals(query, listener.query);
        assertTrue(listener.statement instanceof ModificationStatement);

        query = formatQuery("select * from %s where id=1");
        executeNet(query);
        listener.verify("querySuccess", 2);
        assertEquals(query, listener.query);
        assertTrue(listener.statement instanceof SelectStatement);

        query = formatQuery("select xyz from %s where id=1");
        Exception expectedException = null;
        try
        {
            executeNet(query);
            fail("Query should fail");
        }
        catch (Exception e)
        {
            expectedException = e;
        }
        listener.verify(newArrayList("querySuccess", "queryFailure"), newArrayList(2, 1));
        assertEquals(query, listener.query);
        assertNotNull(listener.e);
        assertNotNull(expectedException);
    }

    @Test
    public void prepareExecuteTest()
    {
        createTable("create table %s (id int primary key, v int)");
        MockListener listener = new MockListener(getCurrentColumnFamilyStore());
        QueryEvents.instance.registerListener(listener);
        Session session = sessionNet();
        String query = formatQuery("select * from %s where id = 1");
        PreparedStatement ps = session.prepare(query);
        listener.verify("prepareSuccess", 1);
        assertEquals(query, listener.query);
        assertTrue(listener.statement instanceof SelectStatement);
        Statement s = ps.bind();
        session.execute(s);
        listener.verify(newArrayList("prepareSuccess", "executeSuccess"), newArrayList(1, 1));

        QueryProcessor.clearPreparedStatements(false);
        s = ps.bind();
        session.execute(s); // this re-prepares the query!!
        listener.verify(newArrayList("prepareSuccess", "executeSuccess", "executeFailure"), newArrayList(2, 2, 1));

        query = formatQuery("select abcdef from %s where id = 1");
        Exception expectedException = null;
        try
        {
            session.prepare(query);
            fail("should fail");
        }
        catch (Exception e)
        {
            expectedException = e;
        }
        listener.verify(newArrayList("prepareSuccess", "prepareFailure", "executeSuccess", "executeFailure"), newArrayList(2, 1, 2, 1));
        assertNotNull(listener.e);
        assertNotNull(expectedException);
    }

    @Test
    public void batchTest()
    {
        createTable("create table %s (id int primary key, v int)");
        BatchMockListener listener = new BatchMockListener(getCurrentColumnFamilyStore());
        QueryEvents.instance.registerListener(listener);
        Session session = sessionNet();
        com.datastax.driver.core.BatchStatement batch = new com.datastax.driver.core.BatchStatement(com.datastax.driver.core.BatchStatement.Type.UNLOGGED);
        String q1 = formatQuery("insert into %s (id, v) values (?, ?)");
        PreparedStatement ps = session.prepare(q1);
        batch.add(ps.bind(1,1));
        batch.add(ps.bind(2,2));
        String q2 = formatQuery("insert into %s (id, v) values (1,1)");
        batch.add(new SimpleStatement(formatQuery("insert into %s (id, v) values (1,1)")));
        session.execute(batch);

        listener.verify(newArrayList("prepareSuccess", "batchSuccess"), newArrayList(1, 1));
        assertEquals(3, listener.queries.size());
        assertEquals(BatchStatement.Type.UNLOGGED, listener.batchType);
        assertEquals(newArrayList(q1, q1, q2), listener.queries);
        assertEquals(newArrayList(newArrayList(ByteBufferUtil.bytes(1), ByteBufferUtil.bytes(1)),
                                  newArrayList(ByteBufferUtil.bytes(2), ByteBufferUtil.bytes(2)),
                                  newArrayList(newArrayList())), listener.values);

        batch.add(new SimpleStatement("insert into abc.def (id, v) values (1,1)"));
        try
        {
            session.execute(batch);
            fail("Batch should fail");
        }
        catch (Exception e)
        {
            // ok
        }
        listener.verify(newArrayList("prepareSuccess", "batchSuccess", "batchFailure"), newArrayList(1, 1, 1));
        assertEquals(3, listener.queries.size());
        assertEquals(BatchStatement.Type.UNLOGGED, listener.batchType);
        assertEquals(newArrayList(q1, q1, q2), listener.queries);
        assertEquals(newArrayList(newArrayList(ByteBufferUtil.bytes(1), ByteBufferUtil.bytes(1)),
                                  newArrayList(ByteBufferUtil.bytes(2), ByteBufferUtil.bytes(2)),
                                  newArrayList(newArrayList()),
                                  newArrayList(newArrayList())), listener.values);
    }

    @Test
    public void errorInListenerTest() throws Throwable
    {
        createTable("create table %s (id int primary key, v int)");
        QueryEvents.Listener listener = new QueryEvents.Listener()
        {
            @Override
            public void querySuccess(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryTime, Message.Response response)
            {
                throw new AssertionError("whoo");
            }

            @Override
            public void queryFailure(@Nullable CQLStatement statement, String query, QueryOptions options, QueryState state, Exception cause)
            {
                throw new AssertionError("whee");
            }
        };

        QueryEvents.instance.registerListener(listener);

        executeNet("insert into %s (id, v) values (2,2)");
        ResultSet rs = executeNet("select * from %s");
        assertEquals(2, rs.one().getInt("id"));

        // record the exception without the throwing listener:
        QueryEvents.instance.unregisterListener(listener);
        Exception expected = null;
        try
        {
            executeNet("select blabla from %s");
            fail("Query should throw");
        }
        catch (Exception e)
        {
            expected = e;
        }

        QueryEvents.instance.registerListener(listener);
        // and with the listener:
        try
        {
            executeNet("select blabla from %s");
            fail("Query should throw");
        }
        catch (Exception e)
        {
            // make sure we throw the same exception even if the listener throws;
            assertSame(expected.getClass(), e.getClass());
            assertEquals(expected.getMessage(), e.getMessage());
        }


    }

    private static class MockListener implements QueryEvents.Listener
    {
        private final String tableName;
        private Map<String, Integer> callCounts = new HashMap<>();
        private String query;
        private CQLStatement statement;
        private long start = System.currentTimeMillis();
        long queryTime;
        private Exception e;


        MockListener(ColumnFamilyStore currentColumnFamilyStore)
        {
            tableName = currentColumnFamilyStore.getTableName();
        }

        public void querySuccess(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryTime, Message.Response response)
        {
            if (query.contains(tableName))
            {
                inc("querySuccess");
                assertNotNull(query);
                this.query = query;
                assertNotNull(statement);
                this.statement = statement;
                this.queryTime = queryTime;
            }
        }

        public void queryFailure(@Nullable CQLStatement statement, String query, QueryOptions options, QueryState state, Exception cause)
        {

            if (query.contains(tableName))
            {
                inc("queryFailure");
                assertNotNull(query);
                this.query = query;
                this.statement = statement;
                e = cause;
            }
        }

        public void executeSuccess(CQLStatement statement, String query, QueryOptions options, QueryState state, long queryTime, Message.Response response)
        {
            if (query.contains(tableName))
                inc("executeSuccess");
        }
        public void executeFailure(@Nullable CQLStatement statement, @Nullable String query, QueryOptions options, QueryState state, Exception cause)
        {
            inc("executeFailure");
            e = cause;
        }


        public void prepareSuccess(CQLStatement statement, String query, QueryState state, long queryTime, ResultMessage.Prepared response)
        {
            if (query.contains(tableName))
            {
                inc("prepareSuccess");
                assertNotNull(query);
                this.query = query;
                this.statement = statement;
                this.queryTime = queryTime;
            }
        }
        public void prepareFailure(@Nullable CQLStatement statement, String query, QueryState state, Exception cause)
        {
            if (query.contains(tableName))
            {
                inc("prepareFailure");
                assertNotNull(query);
                assertNull(statement);
                this.query = query;
            }
        }

        void inc(String key)
        {
            callCounts.compute(key, (k, v) -> v == null ? 1 : v + 1);
        }

        private void verify(String key, int expected)
        {
            verify(Collections.singletonList(key), Collections.singletonList(expected));
        }

        void verify(List<String> keys, List<Integer> expected)
        {
            assertEquals("key count not equal: "+keys+" : "+callCounts, keys.size(), callCounts.size());
            assertTrue(queryTime >= start && queryTime <= System.currentTimeMillis());
            for (int i = 0; i < keys.size(); i++)
            {
                assertEquals("expected count mismatch for "+keys.get(i), (int)expected.get(i), (int) callCounts.get(keys.get(i)));
            }
        }
    }

    private static class BatchMockListener extends MockListener
    {
        private List<String> queries;
        private List<List<ByteBuffer>> values;
        private BatchStatement.Type batchType;

        BatchMockListener(ColumnFamilyStore currentColumnFamilyStore)
        {
            super(currentColumnFamilyStore);
        }

        public void batchSuccess(BatchStatement.Type batchType, List<? extends CQLStatement> statements, List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState state, long queryTime, Message.Response response)
        {
            inc("batchSuccess");
            this.queries = queries;
            this.values = values;
            this.batchType = batchType;
            this.queryTime = queryTime;
        }

        public void batchFailure(BatchStatement.Type batchType, List<? extends CQLStatement> statements, List<String> queries, List<List<ByteBuffer>> values, QueryOptions options, QueryState state, Exception cause)
        {
            inc("batchFailure");
            this.queries = queries;
            this.values = values;
            this.batchType = batchType;
        }

    }
}
