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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingState;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitGossipStatus;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getBroadcastAddressHostWithPortString;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokens;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseAssassinatedCase extends TestBaseImpl
{
    protected static final int SEED_NUM = 1;
    protected static final int NODE_TO_REMOVE_NUM = 2;
    protected static final int PEER_NUM = 3;

    abstract void consume(Cluster cluster, IInvokableInstance nodeToRemove);

    protected void afterNodeStatusIsLeft(Cluster cluster, IInvokableInstance nodeToRemove)
    {
    }

    protected String expectedMessage(IInvokableInstance nodeToRemove)
    {
        return "Cannot replace token " + getTokens(nodeToRemove).get(0) + " which does not exist!";
    }

    @Test
    public void test() throws IOException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == 4 || node == 5 ? NODE_TO_REMOVE_NUM : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(SEED_NUM);
            IInvokableInstance nodeToRemove = cluster.get(NODE_TO_REMOVE_NUM);
            IInvokableInstance peer = cluster.get(PEER_NUM);

            setupCluster(cluster);

            consume(cluster, nodeToRemove);

            assertRingState(seed, nodeToRemove, "Normal");

            // assassinate the node
            peer.nodetoolResult("assassinate", getBroadcastAddressHostWithPortString(nodeToRemove))
                .asserts().success();

            // wait until the peer sees this assassination
            awaitGossipStatus(seed, nodeToRemove, "LEFT");

            // Any extra checks to run after the node has been as LEFT
            afterNodeStatusIsLeft(cluster, nodeToRemove);

            // allow replacing nodes with the LEFT state, this should fail since the token isn't in the ring
            assertThatThrownBy(() ->
                               replaceHostAndStart(cluster, nodeToRemove, properties -> {
                                   // since there are downed nodes its possible gossip has the downed node with an old schema, so need
                                   // this property to allow startup
                                   properties.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                                   // since the bootstrap should fail because the token, don't wait "too long" on schema as it doesn't
                                   // matter for this test
                                   properties.set(BOOTSTRAP_SCHEMA_DELAY_MS, 10);
                               }))
            .hasMessage(expectedMessage(nodeToRemove));
        }
    }
}
