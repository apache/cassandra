/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MD5Digest;

import static org.junit.Assert.fail;
import static org.apache.cassandra.cql3.QueryProcessor.process;

public class IndexedValuesValidationTest
{
    static ClientState clientState;
    static String keyspace = "indexed_value_validation_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        clientState = ClientState.forInternalCalls();
    }

    // CASSANDRA-8280/8081
    // reject updates with indexed values where value > 64k
    @Test
    public void testIndexOnCompositeValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.composite_index_table (a int, b int, c blob, PRIMARY KEY (a))");
        executeSchemaChange("CREATE INDEX ON %s.composite_index_table(c)");
        performInsertWithIndexedValueOver64k("INSERT INTO %s.composite_index_table (a, b, c) VALUES (0, 0, ?)");
    }

    @Test
    public void testIndexOnClusteringValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.ck_index_table (a int, b blob, c int, PRIMARY KEY (a, b))");
        executeSchemaChange("CREATE INDEX ON %s.ck_index_table(b)");
        performInsertWithIndexedValueOver64k("INSERT INTO %s.ck_index_table (a, b, c) VALUES (0, ?, 0)");
    }

    @Test
    public void testIndexOnPartitionKeyOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.pk_index_table (a blob, b int, c int, PRIMARY KEY ((a, b)))");
        executeSchemaChange("CREATE INDEX ON %s.pk_index_table(a)");
        performInsertWithIndexedValueOver64k("INSERT INTO %s.pk_index_table (a, b, c) VALUES (?, 0, 0)");
    }

    @Test
    public void testCompactTableWithValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.compact_table (a int, b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE");
        executeSchemaChange("CREATE INDEX ON %s.compact_table(b)");
        performInsertWithIndexedValueOver64k("INSERT INTO %s.compact_table (a, b) VALUES (0, ?)");
    }

    private static void performInsertWithIndexedValueOver64k(String insertCQL) throws Exception
    {
        ByteBuffer buf = ByteBuffer.allocate(1024 * 65);
        buf.clear();
        for (int i=0; i<1024 + 1; i++)
            buf.put((byte)0);

        try
        {
            execute(String.format(insertCQL, keyspace), buf);
            fail("Expected statement to fail validation");
        }
        catch (InvalidRequestException e)
        {
            // as expected
        }
    }

    private static void execute(String query, ByteBuffer value) throws RequestValidationException, RequestExecutionException
    {
        MD5Digest statementId = QueryProcessor.prepare(String.format(query, keyspace), clientState, false).statementId;
        CQLStatement statement = QueryProcessor.instance.getPrepared(statementId);
        statement.executeInternal(QueryState.forInternalCalls(),
                                  new QueryOptions(ConsistencyLevel.ONE, Collections.singletonList(value)));
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        }
        catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }
}

