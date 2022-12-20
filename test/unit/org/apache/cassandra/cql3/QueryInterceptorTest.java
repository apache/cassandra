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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;

public class QueryInterceptorTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Throwable
    {
        requireNetwork();
    }

    @After
    public void cleanInterceptors() throws Throwable
    {
        QueryProcessor.instance.clearInterceptors();
    }

    @Test
    public void returnsNoRows() throws Throwable
    {
        createTable("create table %s (id int primary key, v int)");
        execute("insert into %s (id, v) values (0, 0)");
        execute("insert into %s (id, v) values (1, 1)");

        assertRows(execute("select * from %s where id = 1"), row(1, 1));

        QueryProcessor.instance.registerInterceptor(new QueryInterceptor()
        {
            @Nullable
            @Override
            public ResultMessage interceptStatement(CQLStatement statement, QueryState queryState, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime)
            {
                if (statement instanceof SelectStatement)
                {
                    SelectStatement selectStatement = (SelectStatement)statement;
                    if (selectStatement.table.keyspace.equals(keyspace()) && selectStatement.table.name.equals(currentTable()))
                    {
                        return generateResults();
                    }
                }
                return null;
            }
        });

        assertEquals(0, executeNet("select * from %s where id = 1").all().size());
    }

    @Test
    public void altersExistingRows() throws Throwable
    {
        createTable("create table %s (id int primary key, v int)");
        execute("insert into %s (id, v) values (0, 0)");
        execute("insert into %s (id, v) values (1, 1)");

        assertRows(execute("select * from %s where id = 1"), row(1, 1));

        QueryProcessor.instance.registerInterceptor(new QueryInterceptor()
        {
            @Nullable
            @Override
            public ResultMessage interceptStatement(CQLStatement statement, QueryState queryState, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime)
            {
                if (statement instanceof SelectStatement)
                {
                    SelectStatement selectStatement = (SelectStatement)statement;
                    if (selectStatement.table.keyspace.equals(keyspace()) && selectStatement.table.name.equals(currentTable()))
                    {
                        return generateResults(row(1, 2));
                    }
                }
                return null;
            }
        });

        List<Row> rows = executeNet("select * from %s where id = 1").all();

        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(2, rows.get(0).getInt(1));
    }

    @Test
    public void addsAdditionalRows() throws Throwable
    {
        createTable("create table %s (id int primary key, v int)");
        execute("insert into %s (id, v) values (0, 0)");
        execute("insert into %s (id, v) values (1, 1)");

        assertRows(execute("select * from %s where id = 1"), row(1, 1));

        QueryProcessor.instance.registerInterceptor(new QueryInterceptor()
        {
            @Nullable
            @Override
            public ResultMessage interceptStatement(CQLStatement statement, QueryState queryState, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime)
            {
                if (statement instanceof SelectStatement)
                {
                    SelectStatement selectStatement = (SelectStatement)statement;
                    if (selectStatement.table.keyspace.equals(keyspace()) && selectStatement.table.name.equals(currentTable()))
                    {
                        return generateResults(row(1, 1), row(1, 2));
                    }
                }
                return null;
            }
        });

        List<Row> rows = executeNet("select * from %s where id = 1").all();

        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(1, rows.get(0).getInt(1));
        assertEquals(1, rows.get(1).getInt(0));
        assertEquals(2, rows.get(1).getInt(1));
    }

    @Test
    public void testInterceptBatchStatement() throws Throwable
    {
        createTable("create table %s (id int primary key, v int)");

        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(new SimpleStatement(String.format("insert into %s.%s (id, v) values (0, 0)", keyspace(), currentTable())));
        batch.add(new SimpleStatement(String.format("insert into %s.%s (id, v) values (1, 1)", keyspace(), currentTable())));
        executeNet(batch);

        assertRows(execute("select count(*) from %s"), row(2L));

        // skip batch execution
        QueryProcessor.instance.registerInterceptor(new QueryInterceptor()
        {
            @Override
            public ResultMessage interceptBatchStatement(org.apache.cassandra.cql3.statements.BatchStatement batch,
                                                         QueryState state,
                                                         BatchQueryOptions options,
                                                         Map<String, ByteBuffer> customPayload,
                                                         long queryStartNanoTime)
            {
                return new ResultMessage.Void();
            }
        });

        batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(new SimpleStatement(String.format("insert into %s.%s (id, v) values (3, 0)", keyspace(), currentTable())));
        batch.add(new SimpleStatement(String.format("insert into %s.%s (id, v) values (4, 1)", keyspace(), currentTable())));
        executeNet(batch);

        // verify second batch is not inserted
        assertRows(execute("select count(*) from %s"), row(2L));

        // clear interceptor and inject default interceptor
        QueryProcessor.instance.clearInterceptors();
        QueryProcessor.instance.registerInterceptor(new QueryInterceptor() {});

        batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(new SimpleStatement(String.format("insert into %s.%s (id, v) values (5, 0)", keyspace(), currentTable())));
        batch.add(new SimpleStatement(String.format("insert into %s.%s (id, v) values (6, 1)", keyspace(), currentTable())));
        executeNet(batch);

        // verify third batch is inserted
        assertRows(execute("select count(*) from %s"), row(4L));
    }

    private ResultMessage generateResults(Object[]... rows)
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata table = cfs.metadata();
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(new ArrayList<>(table.columns()));
        ResultSet resultSet = new ResultSet(resultMetadata);

        for (int index = 0; index < rows.length; index++)
        {
            Object[] row = rows[index];
            resultSet.addRow(Arrays.asList(Int32Type.instance.decompose((Integer)row[0]), Int32Type.instance.decompose((Integer)row[1])));
        }
        return new ResultMessage.Rows(resultSet);
    }
}
