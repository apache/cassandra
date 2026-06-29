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

import com.google.common.collect.ImmutableMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static org.junit.Assert.assertFalse;

public class ClusterMetadataUpgradeDC2InitializeTest extends UpgradeTestBase
{
    public static final Logger logger = LoggerFactory.getLogger(ClusterMetadataUpgradeDC2InitializeTest.class);
    @Test
    public void testCMSInitializeOnDC2NodeAfterUpgrade() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1, 2)
        .withNodeIdTopology(ImmutableMap.of(1, NetworkTopology.dcAndRack("dc1", "rack1"),
                                            2, NetworkTopology.dcAndRack("dc2", "rack2")))
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .upgradesToCurrentFrom(v41)
        .setup((cluster) -> {
        })
        .runBeforeClusterUpgrade(cluster -> {
            cluster.forEach(node -> {
                node.flush("system");
            });
        })
        .runAfterClusterUpgrade((cluster) -> {
            // Run cms initialize on the node in dc2 (node 2) instead of dc1.
            cluster.get(2).nodetoolResult("cms", "initialize").asserts().success();

            cluster.forEach(i -> assertFalse("node " + i.config().num() + " is still in MIGRATING STATE",
                                             ClusterUtils.isMigrating((IInvokableInstance) i)));
        }).run();
    }
}
