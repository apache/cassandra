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

import java.util.Collection;
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
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;

import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

public class AllowIPChangeWithReplaceTest extends TestBaseImpl
{
    @Test
    public void testReplace() throws Exception
    {
        int NUM_NODES = 6;
        int TO_REPLACE = 5;
        TokenSupplier ts = new TokenSupplier()
        {
            TokenSupplier delegate = TokenSupplier.evenlyDistributedTokens(NUM_NODES);
            @Override
            public Collection<String> tokens(int i)
            {
                if (i < NUM_NODES)
                    return delegate.tokens(i);
                if (i == NUM_NODES || i == NUM_NODES * 2) // the replacement
                    return delegate.tokens(TO_REPLACE);
                return delegate.tokens(i - NUM_NODES);
            }
        };

        try (Cluster cluster = builder().withNodes(NUM_NODES - 1)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .withTokenSupplier(ts)
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(NUM_NODES * 2, "dc0", "rack0"))
                                        .withInstanceInitializer((cl, i) -> IPChangeWithMSOBase.BBHelper.install(6, BootstrapAndReplace.class, cl, i))
                                        .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(TO_REPLACE).shutdown();
            IInstanceConfig replacementConfig = cluster.newInstanceConfig();
            replacementConfig.set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, "false");
            replacementConfig.set("auto_bootstrap", "true");
            IInvokableInstance replacement = cluster.bootstrap(replacementConfig, AbstractCluster.CURRENT_VERSION);
            try (WithProperties replacementProps = new WithProperties())
            {
                replacementProps.set(REPLACE_ADDRESS_FIRST_BOOT,
                                     cluster.get(TO_REPLACE).config().broadcastAddress().getAddress().getHostAddress());
                replacement.startup();
                fail();
            }
            catch (Exception ignored)
            {}
            replacement.shutdown();
            cluster.get(1).runOnInstance(() -> {
                assertTrue(ClusterMetadata.current().directory.states.containsValue(NodeState.BOOT_REPLACING));
            });

            for (int i = 1; i < TO_REPLACE; i++)
            {
                cluster.get(i).shutdown().get();
                IInstanceConfig nodeConfig = cluster.newInstanceConfig();
                nodeConfig.set("data_file_directories", cluster.get(i).config().get("data_file_directories"));
                cluster.bootstrap(nodeConfig, AbstractCluster.CURRENT_VERSION).startup();
            }

            replacementConfig = cluster.newInstanceConfig();
            Map<String, String> parameters = new HashMap<>();
            String seedAddress = cluster.get(7).config().getString("listen_address");
            int port = cluster.get(7).config().getInt("storage_port");
            parameters.put("seeds", seedAddress+":"+port);
            replacementConfig.set("seed_provider", new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", parameters));
            replacementConfig.set("auto_bootstrap", "true");
            replacementConfig.set("data_file_directories", cluster.get(6).config().get("data_file_directories"));
            replacement = cluster.bootstrap(replacementConfig, AbstractCluster.CURRENT_VERSION);
            replacement.startup();
            IPChangeWithMSOBase.assertRecalculatedPlacements(cluster.get(7));
            cluster.get(7).runOnInstance(() -> assertTrue(ClusterMetadata.current().directory.states.values().stream().allMatch(s -> s == NodeState.JOINED)));
        }
    }
}
