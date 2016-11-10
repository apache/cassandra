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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NonNativeTimestampTest extends SchemaLoader
{
    @BeforeClass
    public static void setup() throws Exception
    {
        Schema.instance.clear();
        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }

    @Test
    public void setServerTimestampForNonCqlNativeStatements() throws RequestValidationException, RequestExecutionException
    {
        String createKsCQL = "CREATE KEYSPACE non_native_ts_test" +
                             " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        String createTableCQL = "CREATE TABLE non_native_ts_test.table_0 (k int PRIMARY KEY, v int)";
        String insertCQL = "INSERT INTO non_native_ts_test.table_0 (k, v) values (1, ?)";
        String selectCQL = "SELECT v, writetime(v) AS wt FROM non_native_ts_test.table_0 WHERE k = 1";

        QueryProcessor.instance.process(createKsCQL,
                                        QueryState.forInternalCalls(),
                                        QueryOptions.forInternalCalls(Collections.<ByteBuffer>emptyList()));
        QueryProcessor.instance.process(createTableCQL,
                                        QueryState.forInternalCalls(),
                                        QueryOptions.forInternalCalls(Collections.<ByteBuffer>emptyList()));
        QueryProcessor.instance.process(insertCQL,
                                        QueryState.forInternalCalls(),
                                        QueryOptions.forInternalCalls(ConsistencyLevel.ONE,
                                                                      Arrays.asList(ByteBufferUtil.bytes(2))));
        UntypedResultSet.Row row = QueryProcessor.instance.executeInternal(selectCQL).one();
        assertEquals(2, row.getInt("v"));
        long timestamp1 = row.getLong("wt");
        assertFalse(timestamp1 == -1l);

        // per CASSANDRA-8246 the two updates will have the same (incorrect)
        // timestamp, so reconcilliation is by value and the "older" update wins
        QueryProcessor.instance.process(insertCQL,
                                        QueryState.forInternalCalls(),
                                        QueryOptions.forInternalCalls(ConsistencyLevel.ONE,
                                                                      Arrays.asList(ByteBufferUtil.bytes(1))));
        row = QueryProcessor.executeInternal(selectCQL).one();
        assertEquals(1, row.getInt("v"));
        assertTrue(row.getLong("wt") > timestamp1);
    }
}
