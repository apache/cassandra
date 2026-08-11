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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterators;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.apache.cassandra.distributed.test.tracking.PaxosMigrationTestUtils.assertCasApplied;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Establish that a coordinator for CAS is forwarding commit to a replica coordinator
 */
public class MutationTrackingCasForwardingTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingCasForwardingTest.class);

    private static final String CONDITIONAL_INSERT_CQL = "INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (1, 1) IF NOT EXISTS";

    /** Partition read back by the forwarded SERIAL read tests, and how many rows it holds. */
    private static final int READ_KEY = 1;
    private static final int READ_ROWS = 4;

    /** Forwarded consensus reads seen by the message filter, asserted per query by the read helpers. */
    private final AtomicInteger consensusReadForwards = new AtomicInteger();

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

    @Test
    public void testForwardedSerialReadOrderingPaxosV1() throws Throwable
    {
        testForwardedSerialReadOrdering("v1");
    }

    @Test
    public void testForwardedSerialReadOrderingPaxosV2() throws Throwable
    {
        testForwardedSerialReadOrdering("v2");
    }

    /**
     * A forwarded SERIAL read can return a whole partition, unlike CAS, so a reversed slice has to come
     * back from the replica coordinator in the order it was read in.
     */
    private void testForwardedSerialReadOrdering(String paxosVariant) throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("paxos_variant", paxosVariant))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.ascending (k int, c int, v int, PRIMARY KEY (k, c));"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.descending (k int, c int, v int, PRIMARY KEY (k, c)) " +
                                              "WITH CLUSTERING ORDER BY (c DESC);"));

            int coordinator = nonReplicaCoordinator(cluster, "ascending", READ_KEY);
            logger.info("DEBUG testForwardedSerialReadOrdering: Using non-replica coordinator: " + coordinator);

            for (int c = 1; c <= READ_ROWS; c++)
            {
                cluster.coordinator(coordinator).execute(withKeyspace("INSERT INTO %s.ascending (k, c, v) VALUES (?, ?, ?)"),
                                                         ConsistencyLevel.ALL, READ_KEY, c, c);
                cluster.coordinator(coordinator).execute(withKeyspace("INSERT INTO %s.descending (k, c, v) VALUES (?, ?, ?)"),
                                                         ConsistencyLevel.ALL, READ_KEY, c, c);
            }

            // Count the forwards, so each assertion below can prove the read left the coordinator
            cluster.filters()
                   .verbs(Verb.CONSENSUS_READ_FORWARD_REQ.id)
                   .messagesMatching((from, to, message) -> {
                       consensusReadForwards.incrementAndGet();
                       return false; // count only, deliver as normal
                   })
                   .drop();

            // Controls: rows are stored in the clustering order these read, so the direction cannot show
            assertSerialReadOrder(cluster, coordinator, "ascending", "", 1, 2, 3, 4);
            assertSerialReadOrder(cluster, coordinator, "descending", "", 4, 3, 2, 1);

            // Reversed slices, the reads that come back the wrong way round without the direction
            assertSerialReadOrder(cluster, coordinator, "ascending", " ORDER BY c DESC", 4, 3, 2, 1);
            assertSerialReadOrder(cluster, coordinator, "descending", " ORDER BY c ASC", 1, 2, 3, 4);

            // Explicit orderings that agree with the clustering order
            assertSerialReadOrder(cluster, coordinator, "ascending", " ORDER BY c ASC", 1, 2, 3, 4);
            assertSerialReadOrder(cluster, coordinator, "descending", " ORDER BY c DESC", 4, 3, 2, 1);

            // An empty result carries no partition, so no direction is consulted
            int missingKey = unwrittenNonReplicaKey(cluster, "ascending", coordinator);
            assertForwardedReadIsEmpty(cluster, coordinator, "ascending", "k = " + missingKey, "");
            assertForwardedReadIsEmpty(cluster, coordinator, "descending", "k = " + missingKey, " ORDER BY c ASC");
            // A partition that exists, sliced so that it selects no rows
            assertForwardedReadIsEmpty(cluster, coordinator, "ascending", "k = " + READ_KEY + " AND c > 100", " ORDER BY c DESC");

            // Paging does more than reorder: the next page's boundary comes from these rows as they
            // stream past, so a page handed back ascending repeats or skips rows
            assertPagedSerialReadOrder(cluster, coordinator, "ascending", " ORDER BY c DESC", 4, 3, 2, 1);
            assertPagedSerialReadOrder(cluster, coordinator, "descending", " ORDER BY c ASC", 1, 2, 3, 4);
        }
    }

    /**
     * Asserts the forwarded SERIAL read returns the partition in the order the same query returns it at a
     * non-serial consistency, which does not forward.
     */
    private void assertSerialReadOrder(Cluster cluster, int coordinator, String table, String ordering, int... expected)
    {
        String cql = withKeyspace("SELECT c FROM %s." + table + " WHERE k = " + READ_KEY) + ordering;
        assertForwardedReadMatches(cluster, coordinator, cql, expectedRows(expected));
    }

    private void assertForwardedReadIsEmpty(Cluster cluster, int coordinator, String table, String predicate, String ordering)
    {
        String cql = withKeyspace("SELECT c FROM %s." + table + " WHERE " + predicate) + ordering;
        assertForwardedReadMatches(cluster, coordinator, cql, new Object[0][]);
    }

    /**
     * Checks the forwarded result against the ordinary read path. The forward count is asserted per query,
     * since one assertion at the end would be satisfied by the control reads alone.
     */
    private void assertForwardedReadMatches(Cluster cluster, int coordinator, String cql, Object[][] expectedRows)
    {
        int beforeReference = consensusReadForwards.get();
        assertRowsInOrder(cql, ConsistencyLevel.ALL, cluster, coordinator, expectedRows);
        assertEquals("A non-serial read should not have been forwarded: " + cql,
                     beforeReference, consensusReadForwards.get());

        for (ConsistencyLevel serial : new ConsistencyLevel[]{ ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL })
        {
            int before = consensusReadForwards.get();
            assertRowsInOrder(cql, serial, cluster, coordinator, expectedRows);
            assertTrue('"' + cql + "\" at " + serial + " should have been forwarded to a replica coordinator",
                       consensusReadForwards.get() > before);
        }
    }

    /** The same read paged in twos, so the direction has to hold within a page and across the boundary. */
    private void assertPagedSerialReadOrder(Cluster cluster, int coordinator, String table, String ordering, int... expected)
    {
        String cql = withKeyspace("SELECT c FROM %s." + table + " WHERE k = " + READ_KEY) + ordering;
        Object[][] expectedRows = expectedRows(expected);

        for (ConsistencyLevel consistencyLevel : new ConsistencyLevel[]{ ConsistencyLevel.ALL, ConsistencyLevel.SERIAL })
        {
            Object[][] actual = Iterators.toArray(cluster.coordinator(coordinator).executeWithPaging(cql, consistencyLevel, 2),
                                                  Object[].class);
            try
            {
                AssertUtils.assertRows(actual, expectedRows);
            }
            catch (AssertionError e)
            {
                throw new AssertionError('"' + cql + "\" paged at " + consistencyLevel + ": " + e.getMessage(), e);
            }
        }
    }

    private static Object[][] expectedRows(int... expected)
    {
        Object[][] expectedRows = new Object[expected.length][];
        for (int i = 0; i < expected.length; i++)
            expectedRows[i] = AssertUtils.row(expected[i]);
        return expectedRows;
    }

    private void assertRowsInOrder(String cql, ConsistencyLevel consistencyLevel, Cluster cluster, int coordinator, Object[][] expectedRows)
    {
        Object[][] actual = cluster.coordinator(coordinator).execute(cql, consistencyLevel);
        try
        {
            AssertUtils.assertRows(actual, expectedRows);
        }
        catch (AssertionError e)
        {
            throw new AssertionError('"' + cql + "\" at " + consistencyLevel + ": " + e.getMessage(), e);
        }
    }

    /**
     * With RF=3 across four nodes exactly one node is not a replica for the key, and a non-replica
     * coordinator is the only one that forwards.
     */
    private int nonReplicaCoordinator(Cluster cluster, String tableName, int key)
    {
        Set<Integer> replicaNodes = replicaNodes(cluster, tableName, key);

        for (int node = 1; node <= cluster.size(); node++)
        {
            if (!replicaNodes.contains(node))
                return node;
        }

        throw new AssertionError("Every node is a replica for key " + key + ", nothing would be forwarded: " + replicaNodes);
    }

    /**
     * A key nothing has been written to that the given coordinator is not a replica for, so that reading
     * it both forwards and comes back empty. Only {@link #READ_KEY} is ever written.
     */
    private int unwrittenNonReplicaKey(Cluster cluster, String tableName, int coordinator)
    {
        for (int key = READ_KEY + 1; key <= READ_KEY + 100; key++)
        {
            if (!replicaNodes(cluster, tableName, key).contains(coordinator))
                return key;
        }

        throw new AssertionError("Found no unwritten key that node " + coordinator + " is not a replica for");
    }

    private Set<Integer> replicaNodes(Cluster cluster, String tableName, int key)
    {
        String keyspaceName = KEYSPACE;
        String replicaEndpoints = cluster.get(1).callOnInstance(
            () -> String.join(",", StorageService.instance.getNaturalEndpointsWithPort(keyspaceName, tableName, Integer.toString(key))));

        Set<Integer> replicaNodes = new HashSet<>();
        for (String endpoint : replicaEndpoints.split(","))
        {
            // Addresses arrive as "127.0.0.3:7000" or "/127.0.0.3:7000"
            int colonIndex = endpoint.indexOf(':');
            String hostPart = colonIndex > 0 ? endpoint.substring(0, colonIndex) : endpoint;
            replicaNodes.add(Integer.parseInt(hostPart.substring(hostPart.lastIndexOf('.') + 1)));
        }

        return replicaNodes;
    }

    private void testCasForwarding(String paxosVariant, boolean useReplicaCoordinator) throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
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
                coordinatorNode = nonReplicaNode(replicaNodes);
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
            Object[][] casResult = cluster.coordinator(coordinatorNode).execute(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            // A forwarded CAS has to hand its outcome back, not just its side effect
            assertCasApplied(casResult);

            // Verify that unblocked replica nodes have the mutation tracked
            for (int replicaNode : replicaNodes) {
                if (replicaNode != blockedReplicaNode) {
                    final int nodeNum = replicaNode;
                    cluster.get(nodeNum).runOnInstance(() -> {
                        MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(
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
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(
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
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(
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
                    MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(
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

            // A CAS that does not apply hands back the row that stopped it. Left until here so the
            // tracking assertions above see only the mutation the applying CAS wrote.
            Object[][] notAppliedResult = cluster.coordinator(coordinatorNode).execute(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM);
            assertRows(notAppliedResult, row(false, 1, 1));

            // A serial read spanning partitions is refused wherever it is coordinated. Before the fix a
            // non-replica coordinator answered it with partition 1 alone, no error and no warning.
            cluster.coordinator(coordinatorNode).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (2, 2)"), ConsistencyLevel.ALL);
            String multiPartitionRead = withKeyspace("SELECT * FROM %s.tbl WHERE k IN (1, 2)");
            for (int node : new int[]{ replicaNodes[0], nonReplicaNode(replicaNodes) })
            {
                assertThatThrownBy(() -> cluster.coordinator(node).execute(multiPartitionRead, ConsistencyLevel.SERIAL))
                .describedAs("SERIAL read spanning two partitions, coordinated by node " + node)
                .hasMessageContaining("may only be requested for one partition at a time");

                // Both partitions are readable without SERIAL. Sort by key, they arrive in token order
                Object[][] bothPartitions = cluster.coordinator(node).execute(multiPartitionRead, ConsistencyLevel.ALL);
                Arrays.sort(bothPartitions, Comparator.comparingInt(partitionRow -> (int) partitionRow[0]));
                assertRows(bothPartitions, row(1, 1), row(2, 2));
            }
        }
    }

    /** The one node of four that is not a replica for key 1, and so has to forward. */
    private static int nonReplicaNode(int[] replicaNodes)
    {
        for (int node = 1; node <= 4; node++)
        {
            boolean isReplica = false;
            for (int replicaNode : replicaNodes)
            {
                if (node == replicaNode)
                {
                    isReplica = true;
                    break;
                }
            }
            if (!isReplica)
                return node;
        }
        throw new AssertionError("Expected one of the four nodes to not be a replica for key 1");
    }
}
