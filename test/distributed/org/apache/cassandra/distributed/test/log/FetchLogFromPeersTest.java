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

package org.apache.cassandra.distributed.test.log;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FetchLogFromPeersTest extends TestBaseImpl
{
    enum ClusterState { COORDINATOR_BEHIND, REPLICA_BEHIND }
    enum Operation { READ, WRITE }

    @Test
    public void testSchema() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key)"));

            for (ClusterState clusterState : ClusterState.values())
                for (Operation operation : Operation.values())
                {
                    System.out.println(String.format("XXX: %s %s", clusterState, operation));
                    setupSchemaBehind(cluster);
                    runQuery(cluster, clusterState, operation);
                }
        }
    }

    public int coordinator(ClusterState clusterState)
    {
        switch (clusterState)
        {
            case COORDINATOR_BEHIND:
                return 2;
            case REPLICA_BEHIND:
                return 3;
        }
        throw new IllegalStateException();
    }

    public void runQuery(Cluster cluster, ClusterState clusterState, Operation operation) throws ExecutionException, InterruptedException
    {
        cluster.get(1).shutdown().get();

        // node2 is behind
        String query;
        switch (operation)
        {
            case READ:
                query = "select * from %s.tbl where id = 5";
                break;
            case WRITE:
                query = "insert into %s.tbl (id) values (5)";
                break;
            default:
                throw new IllegalStateException();
        }
        int coordinator = coordinator(clusterState);
        long mark = cluster.get(2).logs().mark();
        long metricsBefore = cluster.get(2).callOnInstance(() -> TCMMetrics.instance.fetchedPeerLogEntries.getCount());
        if (clusterState == ClusterState.COORDINATOR_BEHIND)
        {
            try
            {
                cluster.coordinator(coordinator).execute(withKeyspace(query), ConsistencyLevel.QUORUM);
                fail("should fail");
            }
            catch (Exception ignored) {}
        }
        cluster.coordinator(coordinator).execute(withKeyspace(query), ConsistencyLevel.QUORUM);
        assertTrue(cluster.get(2).logs().grep(mark, "Fetching log from /127.0.0.3:7012").getResult().size() > 0);
        long metricsAfter = cluster.get(2).callOnInstance(() -> TCMMetrics.instance.fetchedPeerLogEntries.getCount());
        assertTrue(metricsAfter > metricsBefore);

        cluster.get(1).startup();
    }

    public void setupSchemaBehind(Cluster cluster)
    {
        cluster.filters().reset();
        cluster.filters().inbound().from(1).to(2).drop();
        long epochBefore = cluster.get(3).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
        cluster.coordinator(1).execute(withKeyspace("alter table %s.tbl with comment='test " + UUID.randomUUID() + "'"), ConsistencyLevel.ONE);
        cluster.get(3).runOnInstance(() -> {
            try
            {
                ClusterMetadataService.instance().awaitAtLeast(Epoch.create(epochBefore).nextEpoch());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
        cluster.filters().reset();
    }

    @Test
    public void catchupCoordinatorBehindTestPlacements() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));

            cluster.filters().inbound().from(1).to(2).drop();

            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup(cluster);

            cluster.get(1).shutdown().get();

            // node2 is behind, reading from it will cause a failure, but it will then catch up
            try
            {
                cluster.coordinator(2).execute(withKeyspace("insert into %s.tbl (id) values (3)"), ConsistencyLevel.QUORUM);
                fail("writing should fail");
            }
            catch (Exception ignored) {}

            cluster.coordinator(2).execute(withKeyspace("insert into %s.tbl (id) values (3)"), ConsistencyLevel.QUORUM);
        }
    }

    @Test
    public void catchupCoordinatorAheadPlacementsReadTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.filters().inbound().from(1).to(2).drop();

            cluster.get(3).shutdown().get();
            cluster.get(4).nodetoolResult("assassinate", "127.0.0.3").asserts().success();

            cluster.get(1).shutdown().get();
            // node4 is ahead - node2 should catch up and allow the write
            long mark = cluster.get(2).logs().mark();
            cluster.coordinator(4).execute(withKeyspace("select * from %s.tbl where id = 6"), ConsistencyLevel.QUORUM);
            assertTrue(cluster.get(2).logs().grep(mark, "Fetched log from /127.0.0.4:7012").getResult().size() > 0);
        }
    }

    @Test
    public void catchupCoordinatorAheadPlacementsWriteTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key)"));
            cluster.filters().inbound().from(1).to(2).drop();

            cluster.get(3).shutdown().get();
            cluster.get(4).nodetoolResult("assassinate", "127.0.0.3").asserts().success();

            cluster.get(1).shutdown().get();
            // node4 is ahead - node2 should catch up and allow the write
            long mark = cluster.get(2).logs().mark();
            cluster.coordinator(4).execute(withKeyspace("insert into %s.tbl (id) values (6)"), ConsistencyLevel.QUORUM);
            assertTrue(cluster.get(2).logs().grep(mark, "Fetched log from /127.0.0.4:7012").getResult().size() > 0);
        }
    }

    @Test
    public void testCMSCatchupTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)) // needed for addtocms below
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.get(2).nodetoolResult("addtocms").asserts().success();
            cluster.get(3).nodetoolResult("addtocms").asserts().success();
            // isolate node2 from the other CMS members to ensure it's behind
            cluster.filters().inbound().from(1).to(2).drop();
            cluster.filters().inbound().from(3).to(2).drop();
            long mark = cluster.get(4).logs().mark();
            cluster.coordinator(1).execute(withKeyspace("alter table %s.tbl with comment='test 123'"), ConsistencyLevel.ONE);
            cluster.get(4).logs().watchFor(mark, "AlterOptions");
            mark = cluster.get(2).logs().mark();
            cluster.get(1).shutdown().get();
            cluster.get(2).logs().watchFor(mark, "/127.0.0.1:7012 is now DOWN");
            // node2, a CMS member, is now behind and node1 is shut down.
            // Try reading at QUORUM from node4, node2 should detect it's behind and catch up from node4
            mark = cluster.get(2).logs().mark();
            cluster.coordinator(4).execute(withKeyspace("select * from %s.tbl where id = 55"), ConsistencyLevel.QUORUM);
            cluster.get(2).logs().watchFor(mark, "Fetching log from /127.0.0.4:7012");
        }
    }
}
