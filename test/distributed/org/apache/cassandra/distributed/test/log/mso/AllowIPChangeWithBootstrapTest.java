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

package org.apache.cassandra.distributed.test.log.mso;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

public class AllowIPChangeWithBootstrapTest extends TestBaseImpl
{
    /**
     * This test fails a bootstrap after either START_JOIN or MID_JOIN, then changes ips for all instances (including
     * the bootstrapping node), and resumes the bootstrap.
     */
    @Test
    public void testBootstrap() throws Exception
    {
        int NUM_NODES = 6;
        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(NUM_NODES);
        try (Cluster cluster = builder().withNodes(NUM_NODES - 1)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withTokenSupplier((TokenSupplier)i -> i <= NUM_NODES ? ts.tokens(i) : ts.tokens(i - NUM_NODES))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(NUM_NODES * 2, "dc0", "rack0"))
                                        .withInstanceInitializer((cl, i) -> IPChangeWithMSOBase.BBHelper.install(6, BootstrapAndJoin.class, cl, i))
                                        .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            IInstanceConfig bootstrapConfig = cluster.newInstanceConfig();
            bootstrapConfig.set("auto_bootstrap", "true");
            bootstrapConfig.set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, "false");
            IInvokableInstance bootstrapped = cluster.bootstrap(bootstrapConfig, AbstractCluster.CURRENT_VERSION);
            try
            {
                bootstrapped.startup();
                fail();
            }
            catch (Exception ignored)
            {}
            bootstrapped.shutdown();
            cluster.get(1).runOnInstance(() -> {
                assertTrue(ClusterMetadata.current().directory.states.containsValue(NodeState.BOOTSTRAPPING));
            });

            // change all ips and bootstrap node6/12;
            for (int i = 1; i < 6; i++)
            {
                cluster.get(i).shutdown().get();
                IInstanceConfig nodeConfig = cluster.newInstanceConfig();
                nodeConfig.set("data_file_directories", cluster.get(i).config().get("data_file_directories"));
                cluster.bootstrap(nodeConfig, AbstractCluster.CURRENT_VERSION).startup();
            }

            bootstrapConfig = cluster.newInstanceConfig();
            Map<String, String> parameters = new HashMap<>();
            String seedAddress = cluster.get(7).config().getString("listen_address");
            int port = cluster.get(7).config().getInt("storage_port");
            parameters.put("seeds", seedAddress+":"+port);
            bootstrapConfig.set("seed_provider", new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", parameters));
            bootstrapConfig.set("auto_bootstrap", "true");
            bootstrapConfig.set("data_file_directories", cluster.get(6).config().get("data_file_directories"));
            bootstrapped = cluster.bootstrap(bootstrapConfig, AbstractCluster.CURRENT_VERSION);
            long mark = bootstrapped.logs().mark();
            bootstrapped.startup();
            bootstrapped.logs().watchFor(mark, "Committing FINISH_JOIN");
            IPChangeWithMSOBase.assertRecalculatedPlacements(cluster.get(7));
            cluster.get(7).runOnInstance(() -> assertTrue(ClusterMetadata.current().directory.states.values().stream().allMatch(s -> s == NodeState.JOINED)));
        }
    }
}
