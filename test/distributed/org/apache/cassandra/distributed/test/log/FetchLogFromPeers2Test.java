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
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.distributed.test.log.FetchLogFromPeersTest.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FetchLogFromPeers2Test extends TestBaseImpl
{
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
                    setupSchemaBehind(cluster);
                    runQuery(cluster, clusterState, operation);
                }
        }
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
            long [] coordinatorBehindMetricsBefore = new long[cluster.size()];
            try
            {
                for (int i = 1; i <= cluster.size(); i++)
                    if (!cluster.get(i).isShutdown())
                        coordinatorBehindMetricsBefore[i - 1] = cluster.get(i).callOnInstance(() -> TCMMetrics.instance.coordinatorBehindSchema.getCount());
                cluster.coordinator(coordinator).execute(withKeyspace(query), ConsistencyLevel.QUORUM);
                fail("should fail");
            }
            catch (Exception ignored) {}

            boolean metricBumped = false;
            for (int i = 1; i <= cluster.size(); i++)
            {
                if (i == coordinator || cluster.get(i).isShutdown())
                    continue;
                long metricAfter = cluster.get(i).callOnInstance(() -> TCMMetrics.instance.coordinatorBehindSchema.getCount());
                if (metricAfter - coordinatorBehindMetricsBefore[i - 1] > 0)
                {
                    metricBumped = true;
                    break;
                }
            }
            assertTrue("Metric CoordinatorBehindSchema should have been bumped for at least one replica", metricBumped);

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
}
