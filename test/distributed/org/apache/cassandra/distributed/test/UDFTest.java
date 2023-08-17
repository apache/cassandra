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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class UDFTest extends TestBaseImpl
{
    @Test
    public void testUDFContextKeyspaceNotNull() throws IOException, ExecutionException, InterruptedException
    {
        /*
        To test the keyspace is not null in the context we call UDFContext.newUDTValue() because it
        relies on the keyspace not being null.
         */
        String[] createStmts = {
        "CREATE TABLE "+KEYSPACE+".current (city text, PRIMARY KEY (city))",
        "CREATE TYPE IF NOT EXISTS "+KEYSPACE+".aggst (lt int, ge int)",
        "CREATE FUNCTION "+KEYSPACE+".udf_not_null ()\n" +
        "CALLED ON NULL INPUT\n" +
        "RETURNS boolean LANGUAGE java AS $$\n" +
        "udfContext.newUDTValue(\"aggst\");\n"+
        "return Boolean.TRUE;\n" +
        "$$;",
        };
        try (Cluster cluster = init(Cluster.create(1, config -> config.set("enable_user_defined_functions", "true"))))
        {
            for (String stmt : createStmts) {
                cluster.schemaChange(stmt);
            }
            cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".current (city) VALUES ('Helsinki')", ConsistencyLevel.ALL);
            assertRows(cluster.coordinator(1).execute( "SELECT "+KEYSPACE+".udf_not_null() AS m FROM "+KEYSPACE+".current", ConsistencyLevel.ALL), row( Boolean.TRUE ));
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            assertRows(cluster.coordinator(1).execute( "SELECT "+KEYSPACE+".udf_not_null() AS m FROM "+KEYSPACE+".current", ConsistencyLevel.ALL), row(Boolean.TRUE));
        }
    }
}