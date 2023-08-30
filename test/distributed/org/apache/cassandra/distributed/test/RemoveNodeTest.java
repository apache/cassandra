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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.junit.Assert.assertTrue;

public class RemoveNodeTest extends TestBaseImpl
{
    @Test
    public void testAbort() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                           .withConfig(conf -> conf.set("hinted_handoff_enabled", "false")
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.filters().inbound().verbs(Verb.INITIATE_DATA_MOVEMENTS_REQ.id).drop();
            String nodeId = cluster.get(3).callOnInstance(() -> ClusterMetadata.current().myNodeId().toUUID().toString());
            cluster.get(3).shutdown().get();
            Thread t = new Thread(() -> cluster.get(1).nodetoolResult("removenode", nodeId, "--force"));
            t.start();
            cluster.get(1).logs().watchFor("Committed StartLeave");
            assertTrue(cluster.get(1).nodetoolResult("removenode", "status").getStdout().contains("MID_LEAVE"));

            cluster.get(1).nodetoolResult("removenode", "abort", nodeId).asserts().success();
            cluster.get(2).logs().watchFor("Enacted CancelInProgressSequence");


            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertTrue(metadata.inProgressSequences.isEmpty());
                assertTrue(metadata.directory.peerState(NodeId.fromString(nodeId)) == NodeState.JOINED);
            });

        }
    }

    @Test
    public void removeNodeWithNoStreamingRequired() throws Exception
    {
        // 2 node cluster, keyspaces have RF=2. If we removenode(node2), then no outbound streams are created
        // as all the data is already fully replicated to node1. In this case, ensure that RemoveNodeStreams
        // doesn't hang indefinitely.
        try (Cluster cluster = init(Cluster.build(2)
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                           .withConfig(config -> config.set("default_keyspace_rf", "2")
                                                                       .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            int toRemove = cluster.get(2).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            cluster.get(2).shutdown(false).get();

            cluster.get(1).runOnInstance(() -> {
                NodeId nodeId = new NodeId(toRemove);
                InetAddressAndPort endpoint = ClusterMetadata.current().directory.endpoint(nodeId);
                FailureDetector.instance.forceConviction(endpoint);
                StorageService.instance.removeNode(nodeId, true);
            });

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertTrue(metadata.inProgressSequences.isEmpty());
                assertTrue(metadata.directory.peerState(new NodeId(toRemove)) == NodeState.LEFT);
            });
        }

    }
}
