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

import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public class SchemaTest extends TestBaseImpl
{
    private static final Consumer<IInstanceConfig> CONFIG_CONSUMER = config -> {
        config.set("partitioner", ByteOrderedPartitioner.class.getSimpleName());
        config.set("initial_token", Integer.toString(config.num() * 1000));
    };

    @Test
    public void dropColumnMixedMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2, CONFIG_CONSUMER)))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int primary key, v1 int, v2 int, v3 int)");
            Object [][] someExpected = new Object[5][];
            Object [][] allExpected1 = new Object[5][];
            Object [][] allExpected2 = new Object[5][];
            for (int i = 0; i < 5; i++)
            {
                int v1 = i * 10, v2 = i * 100, v3 = i * 1000;
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1, v2, v3) VALUES (?,?,?, ?)" , ConsistencyLevel.ALL, i, v1, v2, v3);
                someExpected[i] = new Object[] {i, v1};
                allExpected1[i] = new Object[] {i, v1, v3};
                allExpected2[i] = new Object[] {i, v1, v2, v3};
            }
            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl DROP v2");
            assertRows(cluster.coordinator(1).execute("SELECT id, v1 FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), someExpected);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), allExpected1);
            assertRows(cluster.coordinator(2).execute("SELECT id, v1 FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), someExpected);
            assertRows(cluster.coordinator(2).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), allExpected2);
        }
    }

    @Test
    public void addColumnMixedMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2, CONFIG_CONSUMER)))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int primary key, v1 int, v2 int)");
            Object [][] someExpected = new Object[5][];
            Object [][] allExpected1 = new Object[5][];
            Object [][] allExpected2 = new Object[5][];
            for (int i = 0; i < 5; i++)
            {
                int v1 = i * 10, v2 = i * 100;
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1, v2) VALUES (?,?,?)" , ConsistencyLevel.ALL, i, v1, v2);
                someExpected[i] = new Object[] {i, v1};
                allExpected1[i] = new Object[] {i, v1, v2, null};
                allExpected2[i] = new Object[] {i, v1, v2};
            }
            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl ADD v3 int");
            assertRows(cluster.coordinator(1).execute("SELECT id, v1 FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), someExpected);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), allExpected1);
            assertRows(cluster.coordinator(2).execute("SELECT id, v1 FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), someExpected);
            assertRows(cluster.coordinator(2).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), allExpected2);
        }
    }

    @Test
    public void addDropColumnMixedMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2, CONFIG_CONSUMER)))
        {
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (id int primary key, v1 int, v2 int)");
            Object [][] someExpected = new Object[5][];
            Object [][] allExpected1 = new Object[5][];
            Object [][] allExpected2 = new Object[5][];
            for (int i = 0; i < 5; i++)
            {
                int v1 = i * 10, v2 = i * 100;
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, v1, v2) VALUES (?,?,?)" , ConsistencyLevel.ALL, i, v1, v2);
                someExpected[i] = new Object[] {i, v1};
                allExpected1[i] = new Object[] {i, v1, v2, null};
                allExpected2[i] = new Object[] {i, v1};
            }
            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            cluster.get(1).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl ADD v3 int");
            cluster.get(2).schemaChangeInternal("ALTER TABLE "+KEYSPACE+".tbl DROP v2");
            assertRows(cluster.coordinator(1).execute("SELECT id, v1 FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), someExpected);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), allExpected1);
            assertRows(cluster.coordinator(2).execute("SELECT id, v1 FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), someExpected);
            assertRows(cluster.coordinator(2).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL), allExpected2);
        }
    }
}
