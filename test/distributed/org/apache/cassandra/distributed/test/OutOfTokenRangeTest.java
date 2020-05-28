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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class OutOfTokenRangeTest extends TestBaseImpl
{
    private static ICluster<?> cluster = null;

    @BeforeClass
    public static void setupPartitions() throws IOException
    {
        cluster = init(Cluster.build().withNodes(2).start(), 2);
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck text, v text, PRIMARY KEY (pk, ck))");

        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk,ck,v) VALUES (1, 'a', '1a')", ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk,ck,v) VALUES (1, 'b', '1b')", ConsistencyLevel.ALL);
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk,ck,v) VALUES (1, 'c', '1c')", ConsistencyLevel.ALL);

        // Prove the row is there before adding the index
        assertRows(cluster.coordinator(1).execute("SELECT pk, ck, v FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck = 'a' ALLOW FILTERING", ConsistencyLevel.ALL), row(1, "a", "1a"));

        cluster.schemaChange("CREATE INDEX ckindex ON " + KEYSPACE + ".tbl (ck)");
    }

    @AfterClass
    public static void closeCluster() throws Exception
    {
        if (cluster != null)
        {
            cluster.close();
        }
    }

    @Test
    public void checkSinglePartitionReadCommandTest()
    {
        Object[][] results = cluster.coordinator(1).execute("SELECT pk, ck, v FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
        assertRows(results, row(1, "a", "1a"), row(1, "b", "1b"), row(1, "c", "1c"));
    }

    @Test
    public void checkPartitionRangeReadCommandTest()
    {
        Object[][] results = cluster.coordinator(1).execute("SELECT pk, ck, v FROM " + KEYSPACE + ".tbl WHERE pk = 1 AND ck = 'a'", ConsistencyLevel.ALL);
        assertRows(results, row(1, "a", "1a"));
    }
}
