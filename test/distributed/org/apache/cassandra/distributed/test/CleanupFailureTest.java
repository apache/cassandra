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
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.ring.BootstrapTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.mortbay.util.IO;

import static java.util.Arrays.asList;
import static org.apache.cassandra.distributed.action.GossipHelper.bootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.decommission;
import static org.apache.cassandra.distributed.action.GossipHelper.pullSchemaFrom;
import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class CleanupFailureTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void before() throws IOException
    {
        CLUSTER = init(Cluster.build()
                              .withNodes(3)
                              .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                              .start());
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testCleanupFailsDuringOngoingDecommission()
    {
        // set up keyspace and table
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        CLUSTER.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("ALTER KEYSPACE system_distributed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");

        // disable autocompaction
        CLUSTER.get(1).nodetoolResult("disableautocompaction", KEYSPACE).asserts().success();

        // populate data and flush
        for(int i=0; i < 20; i++){
            CLUSTER.get(1).coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                    ConsistencyLevel.ALL,
                    1, i, i);
            CLUSTER.get(1).flush(KEYSPACE);
        }

        // assert data has been populated
        Object[][] beforeDecommResponse = CLUSTER.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl ;");
        Assert.assertEquals(20, beforeDecommResponse.length);

        // assert 20 sstables
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            View view = cfs.getTracker().getView();
            Assert.assertEquals(20, view.liveSSTables().size());
        });

        CLUSTER.get(2).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            View view = cfs.getTracker().getView();
            Assert.assertEquals(0, view.liveSSTables().size());
        });

        // kick off decommission
        Thread decommThread = new Thread(() -> CLUSTER.run(decommission(), 1));
        decommThread.start();

        // run cleanup while decomm is ongoing
        while(decommThread.isAlive()){
            Thread t = new Thread(() -> CLUSTER.get(2).nodetool("cleanup"));
            t.start();
            try
            {
                t.join();
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        try
        {
            decommThread.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        // check data still present
        Object[][] afterDecommResponse = CLUSTER.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl ;");
        Assert.assertEquals(20, afterDecommResponse.length);

        CLUSTER.get(2).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            View view = cfs.getTracker().getView();
            Assert.assertEquals(1, view.liveSSTables().size());
        });
    }
}
