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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.junit.Assert.assertEquals;

public class GetEndpointsTest extends TestBaseImpl
{
    @Test
    public void testGetEndpointsForLocalTable() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .start()))
        {
            for (IInvokableInstance i : cluster)
            {
                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system", "compaction_history", "7d431310-43c9-11ef-bd50-53ff742309a9");
                    assertEquals(1, endpoints.size());
                    assertEquals(FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort(), endpoints.get(0));
                });
            }
        }
    }

    @Test
    public void testGetEndpointsForMetadataTables() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.NETWORK))
                                           .start()))
        {
            for (IInvokableInstance i : cluster)
            {
                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system", "local_metadata_log", "1");
                    assertEquals(1, endpoints.size());
                    assertEquals(FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort(), endpoints.get(0));
                });

                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system_cluster_metadata", "distributed_metadata_log", "1");
                    assertEquals(1, endpoints.size());
                    assertEquals(Replicas.stringify(ClusterMetadata.current().fullCMSMembersAsReplicas(), true), endpoints);
                });
            }
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            for (IInvokableInstance i : cluster)
            {
                i.runOnInstance(() -> {
                    List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort("system_cluster_metadata", "distributed_metadata_log", "1");
                    assertEquals(endpoints.toString(), 3, endpoints.size());
                    assertEquals(Replicas.stringify(ClusterMetadata.current().fullCMSMembersAsReplicas(), true), endpoints);
                });

            }
        }
    }

    @Test
    public void testGetEndpointsByToken() throws IOException
    {
        int nodeCount = 3;
        try (Cluster cluster = init(Cluster.build(nodeCount)
                                           .withConfig(c -> c.with(Feature.NETWORK))
                                           .start()))
        {
            long randomValue = ThreadLocalRandom.current().nextLong();
            long randomRF = ThreadLocalRandom.current().nextInt(1, nodeCount + 1);

            cluster.schemaChange("CREATE KEYSPACE s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':" + randomRF + '}');
            cluster.schemaChange("CREATE TABLE s.t (k text PRIMARY KEY, v int)");
            ICoordinator coordinator = cluster.coordinator(1);

            coordinator.execute("INSERT INTO s.t (k, v) VALUES (?, ?)", QUORUM, String.valueOf(randomValue), 1);

            NodeToolResult resByKey = cluster.get(1).nodetoolResult("getendpoints", "s", "t", String.valueOf(randomValue));
            resByKey.asserts().success();

            // case: keyspace or table not exsit
            cluster.get(1).nodetoolResult("getendpoints", "--token", "s_not_exist", "t", String.valueOf(randomValue)).asserts().failure();
            cluster.get(1).nodetoolResult("getendpoints", "--token", "s", "t_not_exist", String.valueOf(randomValue)).asserts().failure();
            // case: token out of range
            cluster.get(1).nodetoolResult("getendpoints", "--token", "s", "t", String.valueOf(Long.MAX_VALUE) + 1).asserts().failure();

            Object[][] resToken = coordinator.execute("SELECT token(k) as tok FROM s.t where k = '" + randomValue + '\'', QUORUM);
            long token = (Long) resToken[0][0];
            NodeToolResult resByToken = cluster.get(1).nodetoolResult("getendpoints", "--token", "s", "t", String.valueOf(token));
            resByToken.asserts().success();
            // assert by partition key or its corresponding token, we should get the same replica view
            Assert.assertEquals(resByKey.getStdout(), resByToken.getStdout());
        }
    }
}
