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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.test.tcm.CMSPlacementAfterReplacementTest.assertInCMS;

public class CMSPlacementAfterBootstrapTest extends TestBaseImpl
{
    @Test
    public void testBootstrapToCMS() throws IOException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withTokenSupplier(node -> node == 4 ? even.token(1) + 100 : even.token(node))
                                           .start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance toBootstrap = cluster.bootstrap(config);
            toBootstrap.startup(cluster);
            awaitRingJoin(cluster.get(1), toBootstrap);
            awaitRingJoin(toBootstrap, cluster.get(1));
            int joinNodeId = cluster.get(4).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            assertInCMS(cluster, joinNodeId);
        }
    }
}
