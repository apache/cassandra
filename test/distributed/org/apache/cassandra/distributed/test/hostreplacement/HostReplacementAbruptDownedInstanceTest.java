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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingState;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopAbrupt;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.setupCluster;
import static org.apache.cassandra.distributed.test.hostreplacement.HostReplacementTest.validateRows;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class HostReplacementAbruptDownedInstanceTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(HostReplacementAbruptDownedInstanceTest.class);

    /**
     * Can we maybe also test with an abrupt shutdown, that is when the shutdown state is not broadcast and the node to be replaced is on NORMAL state?
     */
    @Test
    public void hostReplaceAbruptShutdown() throws IOException
    {
        int numStartNodes = 3;
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(numStartNodes);
        try (Cluster cluster = Cluster.build(numStartNodes)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withTokenSupplier(node -> even.token(node == (numStartNodes + 1) ? 2 : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(1);
            IInvokableInstance nodeToRemove = cluster.get(2);
            IInvokableInstance peer = cluster.get(3);
            List<IInvokableInstance> peers = Arrays.asList(seed, peer);

            setupCluster(cluster);

            // collect rows/tokens to detect issues later on if the state doesn't match
            SimpleQueryResult expectedState = nodeToRemove.coordinator().executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            stopAbrupt(cluster, nodeToRemove);

            // at this point node 2 should still be NORMAL on all other nodes
            peers.forEach(p -> assertRingState(p, nodeToRemove, "Normal"));

            // node is down, but queries should still work
            //TODO failing, but shouldn't!
//            peers.forEach(p -> validateRows(p.coordinator(), expectedState));

            // now create a new node to replace the other node
            long startNanos = nanoTime();
            IInvokableInstance replacingNode = replaceHostAndStart(cluster, nodeToRemove, properties -> {
                // since node2 was killed abruptly its possible that node2's gossip state has an old schema version
                // if this happens then bootstrap will fail waiting for a schema version it will never see; to avoid
                // this, setting this property to log the warning rather than fail bootstrap
                properties.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
            });
            logger.info("Host replacement of {} with {} took {}", nodeToRemove, replacingNode, Duration.ofNanos(nanoTime() - startNanos));
            peers.forEach(p -> awaitRingJoin(p, replacingNode));

            // make sure all nodes are healthy
            awaitRingHealthy(seed);

            List<IInvokableInstance> expectedRing = Arrays.asList(seed, peer, replacingNode);
            expectedRing.forEach(p -> assertRingIs(p, expectedRing));

            expectedRing.forEach(p -> validateRows(p.coordinator(), expectedState));
        }
    }
}
