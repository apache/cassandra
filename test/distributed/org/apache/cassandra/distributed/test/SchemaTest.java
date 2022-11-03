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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;

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
    public void schemaReset() throws Throwable
    {
        int delayUnit = 1000;

        System.setProperty("cassandra.migration_delay_ms", Integer.toString(5 * delayUnit));
        System.setProperty("cassandra.schema_pull_interval_ms", Integer.toString(5 * delayUnit));
        System.setProperty("cassandra.schema_pull_backoff_delay_ms", Integer.toString(delayUnit));

        try (Cluster cluster = init(Cluster.build(2).withConfig(cfg -> cfg.with(Feature.GOSSIP, Feature.NETWORK)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk INT PRIMARY KEY, v TEXT)");

            assertTrue(cluster.get(1).callOnInstance(() -> Schema.instance.getCFMetaData(KEYSPACE, "tbl") != null));
            assertTrue(cluster.get(2).callOnInstance(() -> Schema.instance.getCFMetaData(KEYSPACE, "tbl") != null));

            cluster.get(2).shutdown().get();

            waitForIt(10, 3, () -> cluster.get(1).callOnInstance(() -> Gossiper.instance.getLiveMembers()
                                                                                        .stream()
                                                                                        .allMatch(addr -> addr.equals(FBUtilities.getBroadcastAddress()))));

            // when schema is removed and there is no other node to fetch it from, node 1 should be left with clean schema
            //noinspection Convert2MethodRef
            cluster.get(1).runOnInstance(() -> StorageService.instance.resetLocalSchema());
            assertTrue(cluster.get(1).callOnInstance(() -> Schema.instance.getCFMetaData(KEYSPACE, "tbl") == null));

            // sleep slightly longer than the schema pull interval
            Uninterruptibles.sleepUninterruptibly(6 * delayUnit, TimeUnit.MILLISECONDS);

            // when the other node is started, schema should be back in sync - node 2 should send schema mutations to node 1
            cluster.get(2).startup();

            // sleep slightly longer than the schema pull interval
            Uninterruptibles.sleepUninterruptibly(6 * delayUnit, TimeUnit.MILLISECONDS);

            waitForIt(6, 1, () -> cluster.get(1).callOnInstance(() -> Schema.instance.getCFMetaData(KEYSPACE, "tbl") != null));

            // when schema is removed and there is a node to fetch it from, node 1 should immediately restore the schema
            //noinspection Convert2MethodRef
            cluster.get(2).runOnInstance(() -> StorageService.instance.resetLocalSchema());

            waitForIt(6, 1, () -> cluster.get(2).callOnInstance(() -> Schema.instance.getCFMetaData(KEYSPACE, "tbl") != null));
        }
        finally
        {
            System.clearProperty("cassandra.migration_delay_ms");
            System.clearProperty("cassandra.schema_pull_interval_ms");
            System.clearProperty("cassandra.schema_pull_backoff_delay_ms");
        }
    }

    private void waitForIt(int rounds, int sleepInSeconds, Supplier<Boolean> test)
    {
        for (int i = 0; i < rounds; i++)
        {
            if (test.get())
                return;

            Uninterruptibles.sleepUninterruptibly(sleepInSeconds, TimeUnit.SECONDS);
        }

        throw new RuntimeException("Timeout reached");
    }

}
