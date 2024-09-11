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

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.NetworkTopology;

public class ClusterMetadataUpgradeChangeIPTest extends UpgradeTestBase
{
    @Test
    public void gossipModeIPChangeTest() throws Throwable
    {
        // all nodes upgraded, bouncing node3 to new ip while in gossip mode
        ipChangeTestHelper(1, 2, 3);
    }

    @Test
    public void upgradeChangeIPTest() throws Throwable
    {
        // changing IP while upgrading node 3
        ipChangeTestHelper(1, 2);
    }

    private void ipChangeTestHelper(int ... toUpgrade) throws Throwable
    {
        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(3);
        new TestCase()
        .nodesToUpgrade(toUpgrade)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .withBuilder(builder -> builder.withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                       .withTokenSupplier((TokenSupplier) i -> i == 4 ? ts.tokens(3) : ts.tokens(i)))
        .nodes(3)
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {})
        .runAfterClusterUpgrade((cluster) -> {
            cluster.get(3).shutdown().get();
            IInstanceConfig nodeConfig = cluster.newInstanceConfig();
            nodeConfig.set("data_file_directories", cluster.get(3).config().get("data_file_directories"));
            IUpgradeableInstance newInstance = cluster.bootstrap(nodeConfig, AbstractCluster.CURRENT_VERSION);
            newInstance.startup();
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();

            cluster.get(2).shutdown().get();
            cluster.get(2).startup();

            cluster.get(2).nodetoolResult("cms", "reconfigure", "3").asserts().success();
        }).run();
    }
}
