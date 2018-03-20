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
package org.apache.cassandra.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

public class InMemoryVirtualTableTest extends CQLTester
{
    private static TableMetadata metadata;
    private static VirtualTableTestImpl table;
    private static KeyspaceMetadata keyspace;

    @BeforeClass
    public static void setup()
    {
        String cql = "CREATE TABLE USING 'org.apache.cassandra.db.VirtualTableTestImpl' system_info.vtable";
        TableMetadata.Builder b = CreateTableStatement.parse(cql, SchemaConstants.INFO_KEYSPACE_NAME);
        metadata = b.build();
        assertTrue(metadata.isVirtual());
        table = new VirtualTableTestImpl(metadata);
        keyspace = KeyspaceMetadata.create("test" + System.currentTimeMillis(), KeyspaceParams.local(),
                Tables.of(metadata));
        Schema.instance.load(keyspace);
        Schema.instance.putVirtualTable(metadata, table);
    }

    private void createReplTable() throws Throwable
    {
        createTable("CREATE TABLE %s (p1 int, p2 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY ((p1, p2), c1, c2))");

        for (int p1 = 0; p1 < 3; p1++)
        {
            for (int p2 = 0; p2 < 7; p2++)
            {
                for (int c1 = 0; c1 < 7; c1++)
                {
                    for (int c2 = 0; c2 < 3; c2++)
                    {
                        execute("INSERT INTO %s (p1, p2, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?, ?)",
                                p1, p2, c1, c2,
                                p1 + p2 * c1 + c2,
                                p1 * p2 + c1 * c2);
                    }
                }
            }
        }
    }

    @Test
    public void compareNonVirtualTable() throws Throwable
    {
        createReplTable();
        Comparator<DecoratedKey> comp = table.comp;
        try
        {
            // this can mess up paging a bit but required to match normal table
            table.comp = Ordering.natural();

            compareRepl("SELECT * FROM {0}");
            compareRepl("SELECT * FROM {0} PER PARTITION LIMIT 3");
            compareRepl("SELECT v2 FROM {0} LIMIT 10");
            compareRepl("SELECT count(*) FROM {0}");
            compareRepl("SELECT p1, p2, sum(v1) FROM {0} GROUP BY p1, p2");
            compareRepl("SELECT DISTINCT p1, p2 FROM {0}");
        }
        finally
        {
            table.comp = comp;
        }
    }

    @Test
    public void testInsert()
    {
        String query = "INSERT INTO " + keyspace.name + ".vtable (p1, p2, c1, c2, v1, v2) VALUES (1,2,3,4,5,6)";
        CQLStatement statement = QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
        statement.execute(QueryState.forInternalCalls(), QueryOptions.DEFAULT, 1);
        assertEquals(1, table.inserts.size());
        Pair<DecoratedKey, Row> insert = table.inserts.remove(0);
        assertEquals("1:2", metadata.partitionKeyType.getString(insert.left.getKey()));
        assertEquals("Row: c1=3, c2=4 | v1=5, v2=6", insert.right.toString(metadata));

        query = "UPDATE " + keyspace.name
                + ".vtable USING TTL 100 SET v1=1, v2=2 WHERE p1=2 AND p2=3 AND c1=1 AND c2=2";
        statement = QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
        statement.execute(QueryState.forInternalCalls(), QueryOptions.DEFAULT, 1);
        assertEquals(1, table.inserts.size());
        insert = table.inserts.remove(0);
        assertEquals("2:3", metadata.partitionKeyType.getString(insert.left.getKey()));
        assertEquals("Row: c1=1, c2=2 | v1=1, v2=2", insert.right.toString(metadata));
        ColumnMetadata v1 = metadata.getColumn(ColumnIdentifier.getInterned("v1", true));
        assertEquals(true, insert.right.getCell(v1).isExpiring());
    }

    /**
     * Test fetching entire set in single query
     */
    @Test
    public void testFetchAll() throws Throwable
    {
        SelectStatement statement = getSelect("SELECT * FROM " + keyspace.name + ".vtable;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        checkResult(rs.iterator());
    }

    @Test
    public void testFetchAllOrdered() throws Throwable
    {
        SelectStatement statement = getSelect(
                "SELECT * FROM " + keyspace.name + ".vtable WHERE p1 = 1 AND p2 = 1 ORDER BY c1 DESC;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        Iterator<UntypedResultSet.Row> i = rs.iterator();

        for (int c1 = 6; c1 >= 0; c1--)
        {
            for (int c2 = 0; c2 < 3; c2++)
            {
                assertTrue(i.hasNext());
                UntypedResultSet.Row row = i.next();
                assertEquals(1, row.getInt("p1"));
                assertEquals(1, row.getInt("p2"));
                assertEquals(c1, row.getInt("c1"));
                assertEquals(c2, row.getInt("c2"));
            }
        }
        statement = getSelect("SELECT * FROM " + keyspace.name + ".vtable WHERE p1 = 1 AND p2 = 1 ORDER BY c1 ASC;");
        rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        i = rs.iterator();

        for (int c1 = 0; c1 < 7; c1++)
        {
            for (int c2 = 0; c2 < 3; c2++)
            {
                assertTrue(i.hasNext());
                UntypedResultSet.Row row = i.next();
                assertEquals(1, row.getInt("p1"));
                assertEquals(1, row.getInt("p2"));
                assertEquals(c1, row.getInt("c1"));
                assertEquals(c2, row.getInt("c2"));
            }
        }
        statement = getSelect(
                "SELECT * FROM " + keyspace.name + ".vtable WHERE p1 = 1 AND p2 = 1 ORDER BY c1 desc, c2 desc;");
        rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        i = rs.iterator();

        for (int c1 = 6; c1 >= 0; c1--)
        {
            for (int c2 = 2; c2 >= 0; c2--)
            {
                assertTrue(i.hasNext());
                UntypedResultSet.Row row = i.next();
                assertEquals(1, row.getInt("p1"));
                assertEquals(1, row.getInt("p2"));
                assertEquals(c1, row.getInt("c1"));
                assertEquals(c2, row.getInt("c2"));
            }
        }

        statement = getSelect(
                "SELECT * FROM " + keyspace.name + ".vtable WHERE p1 = 1 AND p2 = 1 ORDER BY c1 asc, c2 asc;");
        rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        i = rs.iterator();

        for (int c1 = 0; c1 < 7; c1++)
        {
            for (int c2 = 0; c2 < 3; c2++)
            {
                assertTrue(i.hasNext());
                UntypedResultSet.Row row = i.next();
                assertEquals(1, row.getInt("p1"));
                assertEquals(1, row.getInt("p2"));
                assertEquals(c1, row.getInt("c1"));
                assertEquals(c2, row.getInt("c2"));
            }
        }
    }

    /**
     * Test internal paging like done with count or other aggregation functions
     */
    @Test
    public void testAggregatePaging()
    {
        List<UntypedResultSet.Row> results = Lists.newArrayList();
        SelectStatement statement = getSelect("SELECT * FROM " + keyspace.name + ".vtable;");
        ReadQuery q = statement.getQuery(makeQueryOpts(3, null), FBUtilities.nowInSeconds());
        QueryPager pager = q.getPager(null, ProtocolVersion.CURRENT);
        UntypedResultSet rs = UntypedResultSet.create(statement, pager, 7);
        for (UntypedResultSet.Row r : rs)
        {
            results.add(r);
        }
        checkResult(results.iterator());
    }

    @Test
    public void testResultBuilderFilter()
    {
        SelectStatement statement = getSelect(
                "SELECT * FROM " + keyspace.name + ".vtable WHERE p1=0 AND p2=0 AND c1=0 AND c2=0;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        Iterator<UntypedResultSet.Row> i = rs.iterator();
        assertTrue(i.hasNext());
        UntypedResultSet.Row row = i.next();
        assertEquals(0, row.getInt("p1"));
        assertEquals(0, row.getInt("p2"));
        assertEquals(0, row.getInt("c1"));
        assertEquals(0, row.getInt("c2"));
        assertTrue(!i.hasNext());
    }

    @Test
    public void testAllowFiltering()
    {
        SelectStatement statement = getSelect("SELECT * FROM " + keyspace.name + ".vtable WHERE c1=0 AND c2=0;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        assertEquals(21, rs.size());
    }

    @Test
    public void testColumnFilter()
    {
        SelectStatement statement = getSelect("SELECT v2 FROM " + keyspace.name + ".vtable;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        UntypedResultSet.Row row = rs.iterator().next();
        assertEquals(1, row.getColumns().size());
        assertEquals("v2", row.getColumns().get(0).name.toCQLString());
    }

    @Test
    public void testAggregate()
    {
        SelectStatement statement = getSelect("SELECT count(*) FROM " + keyspace.name + ".vtable;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        UntypedResultSet.Row row = rs.iterator().next();
        assertEquals(1, row.getColumns().size());
        assertEquals(3 * 7 * 7 * 3, row.getLong("count"));
    }

    @Test
    public void testDistinct()
    {
        // tests PER PARTITION and DISTINCT column filtering
        SelectStatement statement = getSelect("SELECT DISTINCT p1, p2 FROM " + keyspace.name + ".vtable;");
        UntypedResultSet rs = UntypedResultSet
                .create(statement.executeInternal(QueryState.forInternalCalls(), QueryOptions.DEFAULT).result);
        Iterator<UntypedResultSet.Row> i = rs.iterator();

        for (int p1 = 0; p1 < 3; p1++)
        {
            for (int p2 = 0; p2 < 7; p2++)
            {
                assertTrue(i.hasNext());
                UntypedResultSet.Row row = i.next();
                assertEquals(p1, row.getInt("p1"));
                assertEquals(p2, row.getInt("p2"));
                assertEquals(2, row.getColumns().size());
            }
        }
    }

    /**
     * Test making query repeatedly passing previous state like app side paging
     */
    @Test
    public void testPagingState()
    {
        List<UntypedResultSet.Row> results = Lists.newArrayList();
        PagingState state = null;
        Rows rows = null;
        do
        {
            SelectStatement statement = getSelect("SELECT * FROM " + keyspace.name + ".vtable;");
            rows = statement.execute(QueryState.forInternalCalls(), makeQueryOpts(100, state),
                    FBUtilities.nowInSeconds());
            UntypedResultSet rs = UntypedResultSet.create(rows.result);
            for (UntypedResultSet.Row r : rs)
            {
                results.add(r);
            }
            state = rows.result.metadata.getPagingState();
        } while (state != null && state.remaining > 0);
        checkResult(results.iterator());
    }

    @Test
    public void testNetPaging() throws Throwable
    {
        for (int i = 0; i < 10; i++)
        {
            ResultSet netRs = executeNetWithPaging("SELECT * FROM " + keyspace.name + ".vtable;",
                    (int) Math.random() * 1000);
            checkResult(netRs);
            netRs = executeNetWithPaging("SELECT * FROM " + keyspace.name + ".vtable PER PARTITION LIMIT 3;",
                    (int) Math.random() * 1000);
            Iterator<com.datastax.driver.core.Row> rows = netRs.iterator();
            for (int p1 = 0; p1 < 3; p1++)
            {
                for (int p2 = 0; p2 < 7; p2++)
                {
                    for (int c1 = 0; c1 < 1; c1++)
                    {
                        for (int c2 = 0; c2 < 3; c2++)
                        {
                            assertTrue(rows.hasNext());
                            com.datastax.driver.core.Row row = rows.next();
                            assertEquals(p1, row.getInt("p1"));
                            assertEquals(p2, row.getInt("p2"));
                            assertEquals(c1, row.getInt("c1"));
                            assertEquals(c2, row.getInt("c2"));
                        }
                    }
                }
            }
            netRs = executeNetWithPaging("SELECT * FROM " + keyspace.name + ".vtable LIMIT 3;",
                    (int) Math.random() * 1000);
            assertEquals(netRs.all().size(), 3);
        }
    }

    private void compareRepl(String q) throws Throwable
    {
        compareRepl(MessageFormat.format(q, "%s"), MessageFormat.format(q, keyspace.name + ".vtable"));
    }

    private void compareRepl(String real, String virtual) throws Throwable
    {
        UntypedResultSet rs = execute(real);
        ResultSet netRs = executeNetWithPaging(virtual, 1000);
        Iterator<UntypedResultSet.Row> rows = rs.iterator();
        for (com.datastax.driver.core.Row row : netRs)
        {
            UntypedResultSet.Row r = rows.next();
            for (Definition d : row.getColumnDefinitions().asList())
            {
                assertEquals(r.getBlob(d.getName()), row.getBytesUnsafe(d.getName()));
            }
        }
    }

    private void checkResult(ResultSet rs)
    {
        Iterator<com.datastax.driver.core.Row> rows = rs.iterator();
        for (int p1 = 0; p1 < 3; p1++)
        {
            for (int p2 = 0; p2 < 7; p2++)
            {
                for (int c1 = 0; c1 < 7; c1++)
                {
                    for (int c2 = 0; c2 < 3; c2++)
                    {
                        assertTrue(rows.hasNext());
                        com.datastax.driver.core.Row row = rows.next();
                        assertEquals(p1, row.getInt("p1"));
                        assertEquals(p2, row.getInt("p2"));
                        assertEquals(c1, row.getInt("c1"));
                        assertEquals(c2, row.getInt("c2"));
                    }
                }
            }
        }
    }

    private void checkResult(Iterator<UntypedResultSet.Row> i)
    {
        for (int p1 = 0; p1 < 3; p1++)
        {
            for (int p2 = 0; p2 < 7; p2++)
            {
                for (int c1 = 0; c1 < 7; c1++)
                {
                    for (int c2 = 0; c2 < 3; c2++)
                    {
                        assertTrue(i.hasNext());
                        UntypedResultSet.Row row = i.next();
                        assertEquals(p1, row.getInt("p1"));
                        assertEquals(p2, row.getInt("p2"));
                        assertEquals(c1, row.getInt("c1"));
                        assertEquals(c2, row.getInt("c2"));
                    }
                }
            }
        }
    }

    private SelectStatement getSelect(String query) throws RequestValidationException
    {
        CQLStatement statement = QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
        assertTrue(statement instanceof SelectStatement);
        return (SelectStatement) statement;
    }

    private QueryOptions makeQueryOpts(int size, PagingState state)
    {
        return QueryOptions.create(ConsistencyLevel.ALL, Collections.<ByteBuffer> emptyList(), false, size, state,
                ConsistencyLevel.ALL, ProtocolVersion.CURRENT, keyspace.name);
    }
}
