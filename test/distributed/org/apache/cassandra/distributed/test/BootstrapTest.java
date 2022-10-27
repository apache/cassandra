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

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.distributed.action.GossipHelper.statusToBootstrap;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class BootstrapTest extends TestBaseImpl
{
    @Test
    public void bootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;
        Cluster.Builder builder = builder().withNodes(originalNodeCount)
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                           .withConfig(config -> config.with(NETWORK, GOSSIP));

        Map<Integer, Long> withBootstrap = null;
        Map<Integer, Long> naturally = null;
        try (Cluster cluster = builder.withNodes(originalNodeCount).start())
        {
            populate(cluster, 0, 1000);

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);

            cluster.bootstrap(config).startup();

            cluster.stream().forEach(instance -> {
                instance.nodetool("cleanup", KEYSPACE, "tbl");
            });

            withBootstrap = count(cluster);
        }

        builder = builder.withNodes(expandedNodeCount)
                         .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                         .withConfig(config -> config.with(NETWORK, GOSSIP));

        try (ICluster cluster = builder.start())
        {
            populate(cluster, 0, 1000);
            naturally = count(cluster);
        }

        for (Map.Entry<Integer, Long> e : withBootstrap.entrySet())
            Assert.assertTrue(e.getValue() >= naturally.get(e.getKey()));
    }

    @Test
    public void readWriteDuringBootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(expandedNodeCount))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance newInstance = cluster.bootstrap(config);
            withProperty("cassandra.join_ring", Boolean.toString(false),
                         () -> newInstance.startup(cluster));

            cluster.forEach(statusToBootstrap(newInstance));

            populate(cluster,0, 100);

            Assert.assertEquals(100, newInstance.executeInternal("SELECT *FROM " + KEYSPACE + ".tbl").length);
        }
    }

    public void populate(ICluster cluster, int from, int to)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 3 + "};");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        for (int i = from; i < to; i++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                           ConsistencyLevel.QUORUM,
                                           i, i, i);
    }

    public Map<Integer, Long> count(ICluster cluster)
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  nodeId -> (Long) cluster.get(nodeId).executeInternal("SELECT count(*) FROM " + KEYSPACE + ".tbl")[0][0]));
    }
}