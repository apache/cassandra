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

package org.apache.cassandra.cql3.restrictions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryInterceptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ExternalRestrictionTest extends CQLTester
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
    public void testExternalExpressionFiltering() throws Throwable
    {
        // This test tests the functionality of the ExternalExpression
        // An ExternalExpression is added to the SelectStatement to only
        // allow one row through rather that the 2 rows that should be returned
        createTable("create table %s (pk int, ck int, v int, primary key(pk, ck))");
        execute("insert into %s (pk, ck, v) values (0, 0, 0)");
        execute("insert into %s (pk, ck, v) values (1, 1, 1)");
        execute("insert into %s (pk, ck, v) values (1, 2, 2)");

        assertRowsIgnoringOrder(execute("select * from %s where pk = 1"), row(1, 1, 1), row(1, 2, 2));

        QueryProcessor.instance.registerInterceptor(new QueryInterceptor()
        {
            @Nullable
            @Override
            public ResultMessage interceptStatement(CQLStatement statement, QueryState queryState, QueryOptions options, long queryStartNanoTime)
            {
                if (statement instanceof SelectStatement)
                {
                    SelectStatement selectStatement = (SelectStatement)statement;
                    if (selectStatement.table.keyspace.equals(keyspace()) && selectStatement.table.name.equals(currentTable()))
                    {
                        ColumnMetadata column = selectStatement.table.getColumn(ColumnIdentifier.getInterned("v", false));
                        ByteBuffer value = Int32Type.instance.decompose(1);
                        statement = selectStatement.addIndexRestrictions(Collections.singleton(new TestExternalRestriction(column, value)));
                        return statement.execute(queryState, options, queryStartNanoTime);
                    }
                }
                return null;
            }
        });

        List<com.datastax.driver.core.Row> rows = executeNet("select * from %s where pk = 1").all();

        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).getInt(2));
    }

    static class TestExternalRestriction implements ExternalRestriction
    {
        ColumnMetadata column;
        ByteBuffer value;

        TestExternalRestriction(ColumnMetadata column, ByteBuffer value)
        {
            this.column = column;
            this.value = value;
        }

        @Override
        public void addToRowFilter(RowFilter.Builder filter, TableMetadata table, QueryOptions options)
        {
            filter.addUserExpression(new TestFilterExpression(column, value));
        }

        @Override
        public boolean needsFiltering(Index.Group indexGroup)
        {
            return false;
        }
    }

    static class TestFilterExpression extends RowFilter.UserExpression
    {
        TestFilterExpression(ColumnMetadata column, ByteBuffer value)
        {
            super(column, Operator.EQ, value);
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            return ByteBufferUtil.compareUnsigned(value, row.getCell(column).buffer()) == 0;
        }

        @Override
        protected void serialize(DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long serializedSize(int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
