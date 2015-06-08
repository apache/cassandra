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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.*;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;

import static org.junit.Assert.fail;
import static org.apache.cassandra.cql3.QueryProcessor.process;

public class IndexedValuesValidationTest
{
    private static final int TOO_BIG = 1024 * 65;

    static ClientState clientState;
    static String keyspace = "indexed_value_validation_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        clientState = ClientState.forInternalCalls();
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

    // CASSANDRA-8280/8081
    // reject updates with indexed values where value > 64k
    @Test
    public void testIndexOnCompositeValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.t1(a int, b int, c blob, PRIMARY KEY (a))");
        executeSchemaChange("CREATE INDEX ON %s.t1(c)");
        failInsert("INSERT INTO %s.t1 (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnClusteringColumnInsertPartitionKeyAndClusteringsOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.t2(a blob, b blob, c blob, d int, PRIMARY KEY (a, b, c))");
        executeSchemaChange("CREATE INDEX ON %s.t2(b)");

        // CompositeIndexOnClusteringKey creates index entries composed of the
        // PK plus all of the non-indexed clustering columns from the primary row
        // so we should reject where len(a) + len(c) > 65560 as this will form the
        // total clustering in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(10);
        ByteBuffer c = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 99);

        failInsert("INSERT INTO %s.t2 (a, b, c, d) VALUES (?, ?, ?, 0)", a, b, c);
    }

    @Test
    public void testCompactTableWithValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.t3(a int, b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE");
        executeSchemaChange("CREATE INDEX ON %s.t3(b)");
        failInsert("INSERT INTO %s.t3 (a, b) VALUES (0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnPartitionKeyInsertValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.t6(a int, b int, c blob, PRIMARY KEY ((a, b)))");
        executeSchemaChange("CREATE INDEX ON %s.t6(a)");
        succeedInsert("INSERT INTO %s.t6 (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnClusteringColumnInsertValueOver64k() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.t7(a int, b int, c blob, PRIMARY KEY (a, b))");
        executeSchemaChange("CREATE INDEX ON %s.t7(b)");
        succeedInsert("INSERT INTO %s.t7 (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    public void failInsert(String insertCQL, ByteBuffer...args) throws Throwable
    {
        try
        {
            execute(String.format(insertCQL, keyspace), args);
            fail("Expected statement to fail validation");
        }
        catch (Exception e)
        {
            // as expected
        }
    }

    private static void execute(String query, ByteBuffer...value) throws RequestValidationException, RequestExecutionException
    {
        MD5Digest statementId = QueryProcessor.prepare(String.format(query, keyspace), clientState, false).statementId;
        CQLStatement statement = QueryProcessor.instance.getPrepared(statementId);
        statement.executeInternal(QueryState.forInternalCalls(),
                                  new QueryOptions(ConsistencyLevel.ONE, com.google.common.collect.Lists.newArrayList(
                                                                                                                     value)));
    }

    public void succeedInsert(String insertCQL, ByteBuffer...args) throws Throwable
    {
        execute(String.format(insertCQL,keyspace), args);
    }
}
