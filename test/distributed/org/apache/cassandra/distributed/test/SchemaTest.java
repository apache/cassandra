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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class SchemaTest extends TestBaseImpl
{
    @Test
    public void readRepair() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int,  primary key (pk, ck))");
            String name = "aaa";
            cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            selectSilent(cluster, name);

            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
        }
    }

    @Test
    public void readRepairWithCompaction() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v1 int, v2 int,  primary key (pk, ck))");
            String name = "v10";
            cluster.get(1).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2) values (?,1,1,1)", 1);
            selectSilent(cluster, name);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).schemaChangeInternal("ALTER TABLE " + KEYSPACE + ".tbl ADD " + name + " list<int>");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v1, v2, " + name + ") values (?,1,1,1,[1])", 1);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            cluster.get(2).forceCompact(KEYSPACE, "tbl");
        }
    }

    private void selectSilent(Cluster cluster, String name)
    {
        try
        {
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ?"), ConsistencyLevel.ALL, 1);
        }
        catch (Exception e)
        {
            boolean causeIsUnknownColumn = false;
            Throwable cause = e;
            while (cause != null)
            {
                if (cause.getMessage() != null && cause.getMessage().contains("Unknown column "+name+" during deserialization"))
                    causeIsUnknownColumn = true;
                cause = cause.getCause();
            }
            assertTrue(causeIsUnknownColumn);
        }
    }

    @Test
    public void dropColumnMixedMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (id int primary key, v1 int, v2 int, v3 int)"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id, v1, v2, v3) VALUES (?,?,?, ?)") , ConsistencyLevel.ALL, 1, 10, 100, 1000);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id, v1, v2, v3) VALUES (?,?,?, ?)") , ConsistencyLevel.ALL, 2, 20, 200, 2000);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id, v1, v2, v3) VALUES (?,?,?, ?)") , ConsistencyLevel.ALL, 3, 30, 300, 3000);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id, v1, v2, v3) VALUES (?,?,?, ?)") , ConsistencyLevel.ALL, 4, 40, 400, 4000);

            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            cluster.get(1).schemaChangeInternal(withKeyspace("ALTER TABLE %s.tbl DROP v2"));

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT id, v1 FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 10),
                       row(2, 20),
                       row(4, 40),
                       row(3, 30));

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 10, 1000),
                       row(2, 20, 2000),
                       row(4, 40, 4000),
                       row(3, 30, 3000));

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT id, v1 FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 10),
                       row(2, 20),
                       row(4, 40),
                       row(3, 30));

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 10, 100, 1000),
                       row(2, 20, 200, 2000),
                       row(4, 40, 400, 4000),
                       row(3, 30, 300, 3000));
        }
    }

    @Test
    public void dropStaticColumnMixedMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, c int, v1 int, v2 int, s1 int static, s2 int static, PRIMARY KEY(pk, c))"));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, s1, s2) VALUES (?, ?, ?)") , ConsistencyLevel.ALL, 1, 10, 100);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, s1, s2) VALUES (?, ?, ?)") , ConsistencyLevel.ALL, 2, 20, 200);

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, c, v1, v2) VALUES (?, ?, ?, ?)") , ConsistencyLevel.ALL, 1, 1, 10, 100);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, c, v1, v2) VALUES (?, ?, ?, ?)") , ConsistencyLevel.ALL, 1, 2, 20, 200);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, c, v1, v2) VALUES (?, ?, ?, ?)") , ConsistencyLevel.ALL, 1, 3, 30, 300);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, c, v1, v2) VALUES (?, ?, ?, ?)") , ConsistencyLevel.ALL, 2, 1, 10, 100);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, c, v1, v2) VALUES (?, ?, ?, ?)") , ConsistencyLevel.ALL, 2, 2, 20, 200);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, c, v1, v2) VALUES (?, ?, ?, ?)") , ConsistencyLevel.ALL, 2, 3, 30, 300);

            cluster.forEach((instance) -> instance.flush(KEYSPACE));
            cluster.get(1).schemaChangeInternal(withKeyspace("ALTER TABLE %s.tbl DROP s2"));

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT DISTINCT pk, s1 FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 10),
                       row(2, 20));

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, c, s1, v1 FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 1, 10, 10),
                       row(1, 2, 10, 20),
                       row(1, 3, 10, 30),
                       row(2, 1, 20, 10),
                       row(2, 2, 20, 20),
                       row(2, 3, 20, 30));

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk, c, s1 FROM %s.tbl WHERE pk IN (1, 2) AND c = 2"), ConsistencyLevel.ALL),
                       row(1, 2, 10),
                       row(2, 2, 20));

            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 1, 10, 10, 100),
                       row(1, 2, 10, 20, 200),
                       row(1, 3, 10, 30, 300),
                       row(2, 1, 20, 10, 100),
                       row(2, 2, 20, 20, 200),
                       row(2, 3, 20, 30, 300));

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT DISTINCT pk, s1, s2 FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 10, 100),
                       row(2, 20, 200));

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT pk, c, s1, s2, v1 FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 1, 10, 100, 10),
                       row(1, 2, 10, 100, 20),
                       row(1, 3, 10, 100, 30),
                       row(2, 1, 20, 200, 10),
                       row(2, 2, 20, 200, 20),
                       row(2, 3, 20, 200, 30));

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT pk, c, s1, s2 FROM %s.tbl WHERE pk IN (1, 2) AND c = 2"), ConsistencyLevel.ALL),
                       row(1, 2, 10, 100),
                       row(2, 2, 20, 200));

            assertRows(cluster.coordinator(2).execute(withKeyspace("SELECT * FROM %s.tbl"), ConsistencyLevel.ALL),
                       row(1, 1, 10, 100, 10, 100),
                       row(1, 2, 10, 100, 20, 200),
                       row(1, 3, 10, 100, 30, 300),
                       row(2, 1, 20, 200, 10, 100),
                       row(2, 2, 20, 200, 20, 200),
                       row(2, 3, 20, 200, 30, 300));
        }
    }
}
