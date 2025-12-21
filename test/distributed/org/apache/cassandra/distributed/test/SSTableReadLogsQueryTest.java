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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;

import static org.junit.Assert.assertEquals;

public class SSTableReadLogsQueryTest extends TestBaseImpl
{
    @Test
    public void logQueryTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v counter)");

            cluster.get(1).runOnInstance(() -> {
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction();
            });

            for (int i = 0; i <= 100; i++)
            {
                cluster.get(1).executeInternal("UPDATE " + KEYSPACE + ".tbl SET v = v + 1 WHERE pk = 2");
                cluster.get(1).flush(withKeyspace("%s"));
            }

            cluster.get(1).runOnInstance(() -> {
                assertEquals(101, Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTracker().getView().liveSSTables().size());
            });

            String query = "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 2";
            cluster.get(1).executeInternalWithResult(query);

            assertEquals(1, cluster.get(1).logs().watchFor("The following query").getResult().size());
        }
    }

    @Test
    public void setSSTablesPerReadLogThresholdTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v counter)");

            cluster.get(1).runOnInstance(() -> {
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction();
            });

            cluster.get(1).runOnInstance(() -> {
                DatabaseDescriptor.setSSTablesPerReadLogThreshold(25);
            });

            for (int i = 0; i <= 25; i++)
            {
                cluster.get(1).executeInternal("UPDATE " + KEYSPACE + ".tbl SET v = v + 1 WHERE pk = 2");
                cluster.get(1).flush(withKeyspace("%s"));
            }

            cluster.get(1).runOnInstance(() -> {
                assertEquals(26, Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTracker().getView().liveSSTables().size());
            });

            String query = "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 2";
            cluster.get(1).executeInternalWithResult(query);

            assertEquals(1, cluster.get(1).logs().watchFor("The following query").getResult().size());
        }
    }

    @Test
    public void logRangeReadQueryTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v int)");

            cluster.get(1).runOnInstance(() -> {
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction();
            });

            for (int i = 0; i <= 100; i++)
            {
                cluster.get(1).executeInternal(String.format("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (%s, %s)", i, i));
                cluster.get(1).flush(withKeyspace("%s"));
            }

            cluster.get(1).runOnInstance(() -> {
                assertEquals(101, Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getTracker().getView().liveSSTables().size());
            });

            String query = "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk >= 0 AND pk < 51 ALLOW FILTERING";
            cluster.get(1).executeInternalWithResult(query);

            assertEquals(1, cluster.get(1).logs().watchFor("The following query").getResult().size());
        }
    }
}