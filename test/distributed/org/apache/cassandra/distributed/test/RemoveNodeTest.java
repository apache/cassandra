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

import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.SingleNodeSequences;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
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
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance leavingInstance = cluster.get(3);
            IInvokableInstance otherInstance = cluster.get(2);
            String nodeId = leavingInstance.callOnInstance(() -> ClusterMetadata.current().myNodeId().toUUID().toString());
            leavingInstance.shutdown().get();

            // Have the CMS node pause before the mid-leave step is committed, then initiate the removal. Make a note
            // of the _next_ epoch (i.e. when the mid-leave will be enacted), so we can pause before it is enacted
            Callable<Epoch> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareLeave.MidLeave);
            Thread t = new Thread(() -> cluster.get(1).nodetoolResult("removenode", nodeId, "--force"));
            t.start();
            Epoch pauseBeforeEnacting = pending.call().nextEpoch();

            // Check the status of the removal
            String stdout = cluster.get(1).nodetoolResult("removenode", "status").getStdout();
            assertTrue(String.format("\"%s\" does not contain correct node id", stdout), stdout.contains("Removing node " + nodeId));
            assertTrue(String.format("\"%s\" does not contain MID_LEAVE", stdout), stdout.contains("step: MID_LEAVE"));

            // Allow the CMS to proceed with committing the mid-leave step, but make the other node pause before
            // enacting it so we can abort the removal before the final step (FINISH_LEAVE) is committed
            Callable<?> beforeEnacted = pauseBeforeEnacting(otherInstance, pauseBeforeEnacting);
            unpauseCommits(cmsInstance);
            beforeEnacted.call();

            // Now abort the removal. This should succeed in committing a cancellation of the removal sequence before
            // it can be completed, as non-CMS instance is still paused.
            cmsInstance.nodetoolResult("removenode", "abort", nodeId).asserts().success();

            // Resume processing on the non-CMS instance. It will enact the MID_LEAVE step followed by the cancellation
            // of the removal process.
            unpauseEnactment(otherInstance);
            otherInstance.logs().watchFor("Enacted CancelInProgressSequence");

            // Finally, validate that cluster metadata is in the expected state - i.e. the "leaving" node is still joined
            waitForCMSToQuiesce(cluster, cmsInstance, leavingInstance.config().num());
            for (IInvokableInstance instance : new IInvokableInstance[] {cmsInstance, otherInstance})
            {
                instance.runOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    assertTrue(metadata.inProgressSequences.isEmpty());
                    assertTrue(metadata.directory.peerState(NodeId.fromString(nodeId)) == NodeState.JOINED);
                });
            }
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
                SingleNodeSequences.removeNode(nodeId, true);
            });

            cluster.get(1).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                assertTrue(metadata.inProgressSequences.isEmpty());
                assertTrue(metadata.directory.peerState(new NodeId(toRemove)) == NodeState.LEFT);
            });
        }

    }
}
