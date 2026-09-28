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
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamingState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.SingleNodeSequences;

import static org.apache.cassandra.distributed.impl.INodeProvisionStrategy.Strategy.OneNetworkInterface;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoveNodeStreamingTest extends TestBaseImpl
{
    @Test
    public void removeNodeRestoresReplicaCount() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withNodeProvisionStrategy(OneNetworkInterface)
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                           .withConfig(config -> config.set("default_keyspace_rf", "2")
                                                                       .set("hinted_handoff_enabled", false)
                                                                       .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start(), 2))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.restore_replicas (pk int PRIMARY KEY, v int)"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.restore_replicas (pk, v) VALUES (?, ?)"), ConsistencyLevel.ALL, i, i);
            cluster.forEach(instance -> instance.flush(KEYSPACE));

            int toRemove = cluster.get(3).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            cluster.get(3).shutdown(false).get();
            cluster.get(1, 2).forEach(instance -> instance.runOnInstance(() -> StreamManager.instance.clearStates()));

            cluster.get(1).runOnInstance(() -> {
                NodeId nodeId = new NodeId(toRemove);
                FailureDetector.instance.forceConviction(ClusterMetadata.current().directory.endpoint(nodeId));
                SingleNodeSequences.removeNode(nodeId, true);
            });
            waitForCMSToQuiesce(cluster, cluster.get(1), 3);

            int streams = 0;
            for (IInvokableInstance instance : new IInvokableInstance[] {cluster.get(1), cluster.get(2)})
            {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT pk, v FROM %s.restore_replicas"));
                assertEquals(100, rows.length);
                for (Object[] row : rows)
                    assertEquals(row[0], row[1]);

                streams += instance.callOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    assertTrue(metadata.inProgressSequences.isEmpty());
                    assertEquals(NodeState.LEFT, metadata.directory.peerState(new NodeId(toRemove)));
                    int initiated = 0;
                    for (StreamingState stream : StreamManager.instance.getStreamingStates())
                    {
                        assertEquals(StreamOperation.RESTORE_REPLICA_COUNT, stream.operation());
                        if (!stream.follower())
                        {
                            assertEquals(StreamingState.Status.SUCCESS, stream.status());
                            initiated++;
                        }
                    }
                    return initiated;
                });
            }
            assertTrue("Removal must stream data to restore the second replica", streams > 0);
        }
    }
}
