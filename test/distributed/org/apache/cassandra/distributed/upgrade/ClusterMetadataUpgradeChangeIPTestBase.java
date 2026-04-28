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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.SimpleSeedProvider;

public class ClusterMetadataUpgradeChangeIPTestBase extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataUpgradeChangeIPTestBase.class);
    static final int NODE_COUNT = 4;

    void ipChangeTestHelper(int ... toUpgrade) throws Throwable
    {
        long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        logger.info("SEED={}", seed);

        TokenSupplier ts = TokenSupplier.evenlyDistributedTokens(NODE_COUNT);

        new UpgradeTestBase.TestCase()
        .nodesToUpgrade(toUpgrade)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .withBuilder(builder -> builder.withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(NODE_COUNT * 2, "dc0", "rack0"))
                                       .withTokenSupplier((TokenSupplier) i -> i > NODE_COUNT ? ts.tokens(i - NODE_COUNT) : ts.tokens(i)))
        .nodes(NODE_COUNT)
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {})
        .runAfterClusterUpgrade((cluster) -> {
            IInstanceConfig.ParameterizedClass seedConf =
                new IInstanceConfig.ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                       Collections.singletonMap("seeds", "127.0.0.1,127.0.0.2,127.0.0.5,127.0.0.6,127.0.0.7"));
            List<Integer> ipChangeOrder = new ArrayList<>(NODE_COUNT);
            for (int i = 1; i <= NODE_COUNT; i++)
            {
                cluster.get(i).config().set("seed_provider", seedConf);
                ipChangeOrder.add(i);
            }
            Collections.shuffle(ipChangeOrder, r);

            for (int i : ipChangeOrder)
            {
                cluster.get(i).shutdown().get();
                IInstanceConfig nodeConfig = cluster.newInstanceConfig();
                nodeConfig.set("seed_provider", seedConf);
                nodeConfig.set("data_file_directories", cluster.get(i).config().get("data_file_directories"));
                IUpgradeableInstance newInstance = cluster.bootstrap(nodeConfig, AbstractCluster.CURRENT_VERSION);
                newInstance.startup();
            }

            cluster.get(randomNode(r)).nodetoolResult("cms", "initialize").asserts().success();
            cluster.get(randomNode(r)).nodetoolResult("cms", "reconfigure", String.valueOf(NODE_COUNT)).asserts().success();
        }).run();
    }

    static int randomNode(Random r)
    {
        return NODE_COUNT + r.nextInt(NODE_COUNT) + 1;
    }
}
