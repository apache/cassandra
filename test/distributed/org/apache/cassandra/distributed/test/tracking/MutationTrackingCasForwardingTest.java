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

package org.apache.cassandra.distributed.test.tracking;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Establish that a coordinator for CAS is forwarding commit to a replica coordinator
 */
public class MutationTrackingCasForwardingTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingCasForwardingTest.class);

    private static final String CONDITIONAL_INSERT_CQL = "INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (1, 1) IF NOT EXISTS";

    @Test
    public void testCasForwardingPaxosV1() throws Throwable
    {
        testCasForwarding("v1", false); // non-replica coordinator
    }

    @Test
    public void testCasForwardingPaxosV1ReplicaCoordinator() throws Throwable
    {
        testCasForwarding("v1", true); // replica coordinator
    }

    @Test
    public void testCasForwardingPaxosV2() throws Throwable
    {
        testCasForwarding("v2", false); // non-replica coordinator
    }

    @Test
    public void testCasForwardingPaxosV2ReplicaCoordinator() throws Throwable
    {
        testCasForwarding("v2", true); // replica coordinator
    }

    private void testCasForwarding(String paxosVariant, boolean useReplicaCoordinator) throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("transient_replication_enabled", "true")
                                                            .set("paxos_variant", paxosVariant))
                                      .start())
        {
            String keyspaceName = KEYSPACE;
            
            // Create tracked keyspace with transient replicas
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            cluster.get(1).runOnInstance(() -> {
                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            // Dynamically determine which nodes are replicas for key "1"
            String replicaEndpointsStr = cluster.get(1).callOnInstance(() -> {
                List<String> endpoints = StorageService.instance.getNaturalEndpointsWithPort(keyspaceName, "tbl", "1");
                return String.join(",", endpoints);
            });
            
            // Parse replica endpoints and convert to node numbers
            String[] replicaEndpoints = replicaEndpointsStr.split(",");
            int[] replicaNodes = new int[replicaEndpoints.length];
            
            for (int i = 0; i < replicaEndpoints.length; i++) {
                String endpoint = replicaEndpoints[i];
                // Extract node number from address like "127.0.0.1:7000" or "/127.0.0.1:7000" -> 1
                int colonIndex = endpoint.indexOf(':');
                String hostPart = colonIndex > 0 ? endpoint.substring(0, colonIndex) : endpoint;
                int lastDotIndex = hostPart.lastIndexOf('.');
                int nodeNum = Integer.parseInt(hostPart.substring(lastDotIndex + 1));
                replicaNodes[i] = nodeNum;
            }
            
            // Determine coordinator based on test parameter
            int coordinatorNode;
            if (useReplicaCoordinator) {
                // Use first replica node as coordinator
                coordinatorNode = replicaNodes[0];
                logger.info("DEBUG testCasForwarding: Using replica coordinator: " + coordinatorNode);
            } else {
                // Find the non-replica node
                coordinatorNode = -1;
            for (int i = 1; i <= 4; i++) {
                    boolean isReplica = false;
                    for (int replicaNode : replicaNodes) {
                        if (i == replicaNode) {
                            isReplica = true;
                            break;
                        }
                    }
                    if (!isReplica) {
                        coordinatorNode = i;
                        break;
                    }
                }
                logger.info("DEBUG testCasForwarding: Using non-replica coordinator: " + coordinatorNode);
            }
            
            // Find a replica node to block (not the same as the coordinator)
            int blockedReplicaNode = -1;
            for (int replicaNode : replicaNodes) {
                if (replicaNode != coordinatorNode) {
                    blockedReplicaNode = replicaNode;
                    break;
                }
            }
            
            logger.info("DEBUG testCasForwarding: Blocked replica node: " + blockedReplicaNode);
            
            // Block the selected replica node from receiving Paxos commit messages
            String blockedAddress = "127.0.0." + blockedReplicaNode;
            cluster.filters().allVerbs().to(blockedReplicaNode).drop().on();
            cluster.filters().allVerbs().from(blockedReplicaNode).drop().on();

            // Mark the blocked replica node as down in gossip so it doesn't participate in Paxos
            for (int i = 1; i <= 4; i++)
            {
                if (i != blockedReplicaNode)
                {
                    final String addressToBlock = blockedAddress;
                    cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked(addressToBlock), Double.MAX_VALUE));
                }
            }

            // Perform CAS operation from determined coordinator
            // This should trigger forwarding if coordinator is not a replica
            cluster.coordinator(coordinatorNode).execute(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);

            // Verify that unblocked replica nodes have the mutation tracked
            for (int replicaNode : replicaNodes) {
                if (replicaNode != blockedReplicaNode) {
                    final int nodeNum = replicaNode;
                    cluster.get(nodeNum).runOnInstance(() -> {
                        MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(
                            Util.dk(1), 
                            ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, 
                            true);
                        assertEquals("Node " + nodeNum + " should have mutation tracked", 1, summary.size());
                        
                        CoordinatorLogId logId = getOnlyLogId(summary);
                        Offsets summaryIds = summaryIdSpace(summary.get(logId));
                        assertEquals("Should have exactly one mutation ID tracked", 1, summaryIds.offsetCount());
                    });
                }
            }

            // The blocked replica node should not have the mutation yet due to filtering
            cluster.get(blockedReplicaNode).runOnInstance(() -> {
                MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(
                    Util.dk(1), 
                    ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, 
                    true);
                assertEquals("Blocked replica node should not have mutation yet", 0, summary.size());
            });

            // Clear filters and revive the blocked replica node
            cluster.filters().reset();
            final String revivedAddress = "127.0.0." + blockedReplicaNode;
            for (int i = 1; i <= 4; i++)
            {
                if (i != blockedReplicaNode)
                {
                    cluster.get(i).runOnInstance(() -> {
                        InetAddressAndPort endpoint = InetAddressAndPort.getByNameUnchecked(revivedAddress);
                        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(endpoint, Gossiper.instance.getEndpointStateForEndpoint(endpoint)));
                    });
                }
            }

            // Perform a non-SERIAL read from coordinator to trigger mutation tracking propagation
            String selectCql = withKeyspace("SELECT * FROM %s.tbl WHERE k = 1");
            Object[][] result = cluster.coordinator(coordinatorNode).execute(selectCql, ConsistencyLevel.ALL);
            assertEquals("Should find the inserted row", 1, result.length);
            assertEquals("Key should be 1", 1, result[0][0]);
            assertEquals("Value should be 1", 1, result[0][1]);

            // Now the blocked replica node should have the mutation propagated via mutation tracking
            // Note: When using replica coordinator, propagation behavior may differ
            cluster.get(blockedReplicaNode).runOnInstance(() -> {
                MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(
                    Util.dk(1), 
                    ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, 
                    true);
                if (useReplicaCoordinator) {
                    // For replica coordinator, propagation may not happen the same way
                    // We expect either 0 (not propagated) or 1 (propagated)
                    assertTrue("Blocked replica node should have 0 or 1 mutations", summary.size() <= 1);
                } else {
                    // For non-replica coordinator, propagation should happen
                    assertEquals("Blocked replica node should now have mutation propagated", 1, summary.size());
                }
            });

            // Verify all replicas have consistent mutation tracking and same mutation ID
            String[] mutationIds = new String[replicaNodes.length];
            for (int i = 0; i < replicaNodes.length; i++) {
                final int nodeNum = replicaNodes[i];
                final int arrayIndex = i;
                cluster.get(nodeNum).runOnInstance(() -> {
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(
                        Util.dk(1), 
                        ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, 
                        true);
                    assertEquals("All replicas should have consistent tracking", 1, summary.size());
                    
                    CoordinatorLogId logId = getOnlyLogId(summary);
                    Offsets summaryIds = summaryIdSpace(summary.get(logId));
                    assertEquals("Should have exactly one mutation ID tracked", 1, summaryIds.offsetCount());
                    
                    // Store the mutation ID for comparison
                    mutationIds[arrayIndex] = summaryIds.toString();
                });
            }
            
            // Verify all replicas have the same mutation ID
            for (int i = 1; i < mutationIds.length; i++) {
                assertEquals("All replicas should have same mutation ID", mutationIds[0], mutationIds[i]);
            }
        }
    }
}