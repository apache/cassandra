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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.upgrade.ClusterMetadataUpgradeAssassinateTest.checkPlacements;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataUpgradeJoinRingTest extends UpgradeTestBase
{
    @Test
    public void joinRingUpgradeTest() throws Throwable
    {
        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(4);

        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withTokenSupplier(ts::tokens)
        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            IInstanceConfig nodeConfig = cluster.newInstanceConfig();
            IUpgradeableInstance newInstance = cluster.bootstrap(nodeConfig);
            withProperty(CassandraRelevantProperties.JOIN_RING, false, newInstance::startup);
            checkGossipinfo(cluster, false);
        })
        .runAfterClusterUpgrade((cluster) -> {
            checkGossipinfo(cluster, false);
            // node4 not upgraded yet - should be allowed to vote despite being join_ring=false:
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().failure();
            cluster.get(4).shutdown().get();
            cluster.get(4).setVersion(Versions.find().getLatest(v51));
            withProperty(CassandraRelevantProperties.JOIN_RING, false, () -> cluster.get(4).startup());
            checkGossipinfo(cluster, false);
            checkPlacements(cluster.get(1), "127.0.0.4", false);

            // before "cms initialize" - shouldn't be allowed to join
            cluster.get(4).nodetoolResult("join").asserts().failure();
            checkGossipinfo(cluster, false);
            // don't allow non-joined nodes to become initial cms:
            cluster.get(4).nodetoolResult("cms", "initialize").asserts().failure();

            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            checkGossipinfo(cluster, false);
            ((IInvokableInstance)cluster.get(4)).runOnInstance(() -> {
                while (ClusterMetadataService.state() == ClusterMetadataService.State.GOSSIP)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            });
            cluster.get(4).nodetoolResult("join").asserts().success();
            checkGossipinfo(cluster, true);
            checkPlacements(cluster.get(1), "127.0.0.4", true);

        }).run();
    }

    private void checkGossipinfo(UpgradeableCluster cluster, boolean shouldBeJoined)
    {
        Map<String, Map<String, String>> states = ClusterUtils.gossipInfo(cluster.get(1));
        Map<String, String> node4State = states.get("/127.0.0.4");
        assertTrue(node4State != null &&  !node4State.isEmpty());
        assertEquals(!shouldBeJoined, node4State.get("TOKENS").contains("not present"));
        assertEquals(shouldBeJoined, node4State.containsKey("STATUS_WITH_PORT"));
    }

}
