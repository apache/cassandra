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

package org.apache.cassandra.distributed.test.schema;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class SchemaTest extends TestBaseImpl
{
    @Test
    public void bootstrapTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            System.out.println("ABOUT TO CREATE KEYSPACE");
            // TODO: consistency level will become irrelevant
            cluster.coordinator(2).execute("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};",
                                           ConsistencyLevel.ALL);

            cluster.coordinator(2).execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v int);",
                                           ConsistencyLevel.ALL);

            System.out.println("STARTING READING");
            for (int i = 1; i <= 3; i++)
            {
                cluster.coordinator(i).execute("SELECT * FROM " + KEYSPACE + ".tbl;",
                                               ConsistencyLevel.ALL);
            }
        }
    }
}
