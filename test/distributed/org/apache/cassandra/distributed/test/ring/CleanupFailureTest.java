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

package org.apache.cassandra.distributed.test.ring;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.action.GossipHelper.decommission;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class CleanupFailureTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testCleanupFailsDuringOngoingDecommission() throws IOException, InterruptedException
    {
        // set up cluster
        CLUSTER = init(Cluster.build()
                      .withNodes(2)
                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                      .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                      .start());

        // set up keyspace and table
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        CLUSTER.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("ALTER KEYSPACE system_distributed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("ALTER KEYSPACE system_traces WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");

        // populate data
        populate(CLUSTER,0,1);

        Object[][] beforeDecommResponse = CLUSTER.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl ;", ConsistencyLevel.ONE);
        Assert.assertEquals(1, beforeDecommResponse.length);

        // kick off decommission
        Thread decommThread = new Thread(() -> CLUSTER.run(decommission(), 1));
        decommThread.start();

        // run cleanup while decomm is ongoing
        while(decommThread.isAlive())
        {
            Thread t = new Thread(() -> CLUSTER.get(2).nodetool("cleanup"));
            t.start();
            t.join();
            Thread.sleep(1000);
        }

        decommThread.join();

        // check data still present on node2
        Object[][] afterDecommResponse = CLUSTER.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl ;");
        Assert.assertEquals(1, afterDecommResponse.length);
    }
    @Test
    public void testCleanupFailsDuringOngoingBootstrap() throws IOException, InterruptedException
    {
        // set up cluster
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        CLUSTER = init(Cluster.build()
                              .withNodes(originalNodeCount)
                              .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                              .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                              .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                              .start());

        // set up keyspace and table
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        CLUSTER.schemaChange("ALTER KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("ALTER KEYSPACE system_distributed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        CLUSTER.schemaChange("ALTER KEYSPACE system_traces WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");

        // populate data
        populate(CLUSTER,0,10000);
        CLUSTER.get(1).flush(KEYSPACE);

        Object[][] beforeBootstrapResponse = CLUSTER.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl ;", ConsistencyLevel.ONE);
        Assert.assertEquals(10000, beforeBootstrapResponse.length);

        // kick off bootstrap
        Thread bootstrapThread = new Thread(() -> bootstrapAndJoinNode(CLUSTER));
        bootstrapThread.start();

        // run cleanup while bootstrap is ongoing
        while(bootstrapThread.isAlive())
        {
            Thread t = new Thread(() -> CLUSTER.get(2).nodetool("cleanup"));
            t.start();
            t.join();
            Thread.sleep(100);
        }

        bootstrapThread.join();

        // assert data on new node
        Assert.assertEquals(expandedNodeCount, CLUSTER.size());

        Object[][] afterBootstrapResponse = CLUSTER.get(2).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl ;");
        Assert.assertEquals(5006, afterBootstrapResponse.length);
    }
}
