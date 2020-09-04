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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JVMDTestTest extends TestBaseImpl
{
    @Test
    public void insertTimestampTest() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            long now = FBUtilities.timestampMicros();
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int primary key, i int)");
            cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, i) VALUES (1,1)", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, i) VALUES (2,2) USING TIMESTAMP 1000", ConsistencyLevel.ALL);

            Object [][] res = cluster.coordinator(1).execute("SELECT writetime(i) FROM "+KEYSPACE+".tbl WHERE id = 1", ConsistencyLevel.ALL);
            assertEquals(1, res.length);
            assertTrue("ts="+res[0][0], (long)res[0][0] >= now);

            res = cluster.coordinator(1).execute("SELECT writetime(i) FROM "+KEYSPACE+".tbl WHERE id = 2", ConsistencyLevel.ALL);
            assertEquals(1, res.length);
            assertEquals(1000, (long) res[0][0]);
        }
    }
}
