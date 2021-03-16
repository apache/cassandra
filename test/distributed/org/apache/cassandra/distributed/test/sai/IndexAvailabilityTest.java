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

package org.apache.cassandra.distributed.test.sai;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.base.Objects;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.waitForIndexQueryable;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexAvailabilityTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %s.%s (pk text primary key, v1 int, v2 text) " +
                                               "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX %s ON %s.%s(%s) USING 'StorageAttachedIndex'";
    
    private static Map<NodeIndex, Index.Status> expectedNodeIndexQueryability = new ConcurrentHashMap<>();
    private List<String> keyspaces;
    private List<String> indexesPerKs;

    @Test
    public void verifyIndexStatusPropagation() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            String ks1 = "ks1";
            String ks2 = "ks2";
            String ks3 = "ks3";
            String cf1 = "cf1";
            String index1 = "cf1_idx1";
            String index2 = "cf1_idx2";

            keyspaces = Arrays.asList(ks1, ks2, ks3);
            indexesPerKs = Arrays.asList(index1, index2);

            // create 1 tables per keyspace, 2 indexes per table. all indexes are queryable
            for (String ks : keyspaces)
            {
                cluster.schemaChange(String.format(CREATE_KEYSPACE, ks, 2));
                cluster.schemaChange(String.format(CREATE_TABLE, ks, cf1));
                cluster.schemaChange(String.format(CREATE_INDEX, index1, ks, cf1, "v1"));
                cluster.schemaChange(String.format(CREATE_INDEX, index2, ks, cf1, "v2"));
                waitForIndexQueryable(cluster, ks);
                cluster.forEach(node -> {
                    expectedNodeIndexQueryability.put(NodeIndex.create(ks, index1, node), Index.Status.BUILD_SUCCEEDED);
                    expectedNodeIndexQueryability.put(NodeIndex.create(ks, index2, node), Index.Status.BUILD_SUCCEEDED);
                });
            }

            // mark ks1 index1 as non-queryable on node1
            markIndexNonQueryable(cluster.get(1), ks1, cf1, index1);
            // on node2, it observes that node1 ks1.index1 is not queryable
            waitForIndexingStatus(cluster.get(2), ks1, index1, cluster.get(1), Index.Status.BUILD_FAILED);
            // other indexes or keyspaces should not be affected
            assertIndexingStatus(cluster);

            // mark ks2 index2 as non-queryable on node2
            markIndexNonQueryable(cluster.get(2), ks2, cf1, index2);
            // on node1, it observes that node2 ks2.index2 is not queryable
            waitForIndexingStatus(cluster.get(1), ks2, index2, cluster.get(2), Index.Status.BUILD_FAILED);
            // other indexes or keyspaces should not be affected
            assertIndexingStatus(cluster);

            // mark ks1 index1 as queryable on node1
            markIndexQueryable(cluster.get(1), ks1, cf1, index1);
            // on node2, it observes that node1 ks1.index1 is queryable
            waitForIndexingStatus(cluster.get(2), ks1, index1, cluster.get(1), Index.Status.BUILD_SUCCEEDED);
            // other indexes or keyspaces should not be affected
            assertIndexingStatus(cluster);

            // mark ks2 index2 as indexing on node1
            markIndexBuilding(cluster.get(1), ks2, cf1, index2);
            // on node2, it observes that node1 ks2.index2 is not queryable
            waitForIndexingStatus(cluster.get(2), ks2, index2, cluster.get(1), Index.Status.FULL_REBUILD_STARTED);
            // other indexes or keyspaces should not be affected
            assertIndexingStatus(cluster);

            // drop ks1, ks1 index1/index2 should be non queryable on all nodes
            cluster.schemaChange("DROP KEYSPACE " + ks1);
            expectedNodeIndexQueryability.keySet().forEach(k -> {
                if (k.keyspace.equals(ks1))
                    expectedNodeIndexQueryability.put(k, Index.Status.UNKNOWN);
            });
            assertIndexingStatus(cluster);

            // drop ks2 index2, there should be no ks2 index2 status on all node
            cluster.schemaChange("DROP INDEX " + ks2 + "." + index2);
            expectedNodeIndexQueryability.keySet().forEach(k -> {
                if (k.keyspace.equals(ks2) && k.index.equals(index2))
                    expectedNodeIndexQueryability.put(k, Index.Status.UNKNOWN);
            });
            assertIndexingStatus(cluster);

            // drop ks3 cf1, there should be no ks3 index1/index2 status
            cluster.schemaChange("DROP TABLE " + ks3 + "." + cf1);
            expectedNodeIndexQueryability.keySet().forEach(k -> {
                if (k.keyspace.equals(ks3))
                    expectedNodeIndexQueryability.put(k, Index.Status.UNKNOWN);
            });
            assertIndexingStatus(cluster);
        }
    }

    @Test
    public void testNonQueryableNodeN2Rf2() throws Exception
    {
        shouldSkipNonQueryableNode(2, Collections.singletonList(1), Arrays.asList(1, 2));
    }

    @Test
    public void testSkipNonQueryableNodeN3Rf3() throws Exception
    {
        shouldSkipNonQueryableNode(3, Collections.singletonList(1), Arrays.asList(1, 2), Arrays.asList(1, 2, 3));
    }

    @Test
    public void testSkipNonQueryableNodeN1Rf1() throws Exception
    {
        shouldSkipNonQueryableNode(1, Collections.singletonList(1));
    }

    private void shouldSkipNonQueryableNode(int nodes, List<Integer>... nonQueryableNodesList) throws Exception
    {
        try (Cluster cluster = init(Cluster.build(nodes)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            String table = "non_queryable_node_test_" + System.currentTimeMillis();
            cluster.schemaChange(String.format(CREATE_TABLE, KEYSPACE, table));
            cluster.schemaChange(String.format(CREATE_INDEX, "", KEYSPACE, table, "v1"));
            cluster.schemaChange(String.format(CREATE_INDEX, "", KEYSPACE, table, "v2"));
            waitForIndexQueryable(cluster, KEYSPACE);

            // create 100 rows in 1 sstable
            int rows = 100;
            for (int i = 0; i < rows; i++)
                cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s(pk, v1, v2) VALUES ('%d', 0, '0');", KEYSPACE, table, i), ConsistencyLevel.QUORUM);
            cluster.forEach(node -> node.flush(KEYSPACE));

            String numericQuery = String.format("SELECT pk FROM %s.%s WHERE v1=0", KEYSPACE, table);
            String stringQuery = String.format("SELECT pk FROM %s.%s WHERE v2='0'", KEYSPACE, table);
            String multiIndexQuery = String.format("SELECT pk FROM %s.%s WHERE v1=0 AND v2='0'", KEYSPACE, table);

            // get index name base on node id to have different non-queryable index on different nodes.
            Function<Integer, String> nodeIdToColumn = nodeId -> "v" + (nodeId % 2 + 1);
            IntFunction<String> nodeIdToIndex = nodeId -> IndexMetadata.generateDefaultIndexName(table, ColumnIdentifier.getInterned(nodeIdToColumn.apply(nodeId), false));

            for (List<Integer> nonQueryableNodes : nonQueryableNodesList)
            {
                int numericLiveReplicas = (int) (nodes - nonQueryableNodes.stream().map(nodeIdToColumn).filter(c -> c.equals("v1")).count());
                int stringLiveReplicas = (int) (nodes - nonQueryableNodes.stream().map(nodeIdToColumn).filter(c -> c.equals("v2")).count());
                int liveReplicas = nodes - nonQueryableNodes.size();

                // mark index non-queryable at once and wait for ack from remote peers
                for (int local : nonQueryableNodes)
                    markIndexNonQueryable(cluster.get(local), KEYSPACE, table, nodeIdToIndex.apply(local));

                for (int local : nonQueryableNodes)
                    for (int remote = 1; remote <= cluster.size(); remote++)
                        waitForIndexingStatus(cluster.get(remote), KEYSPACE, nodeIdToIndex.apply(local), cluster.get(local), Index.Status.BUILD_FAILED);

                // test different query types
                executeOnAllCoordinatorsAllConsistencies(cluster, numericQuery, numericLiveReplicas, rows);
                executeOnAllCoordinatorsAllConsistencies(cluster, stringQuery, stringLiveReplicas, rows);
                executeOnAllCoordinatorsAllConsistencies(cluster, multiIndexQuery, liveReplicas, rows);

                // rebuild local index at once and wait for remote ack
                for (int local : nonQueryableNodes)
                {
                    String index = nodeIdToIndex.apply(local);
                    cluster.get(local).runOnInstance(() -> ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, table, index));
                }

                for (int local : nonQueryableNodes)
                    for (int remote = 1; remote <= cluster.size(); remote++)
                        waitForIndexingStatus(cluster.get(remote), KEYSPACE, nodeIdToIndex.apply(local), cluster.get(local), Index.Status.BUILD_SUCCEEDED);

                // With cl=all, query should pass
                executeOnAllCoordinators(cluster, numericQuery, ConsistencyLevel.ALL, rows);
                executeOnAllCoordinators(cluster, stringQuery, ConsistencyLevel.ALL, rows);
                executeOnAllCoordinators(cluster, multiIndexQuery, ConsistencyLevel.ALL, rows);
            }
        }
    }

    private void executeOnAllCoordinatorsAllConsistencies(Cluster cluster, String statement, int liveReplicas, int num) throws Exception
    {
        int allReplicas = cluster.size();

        // test different consistency levels
        executeOnAllCoordinators(cluster, statement, ConsistencyLevel.ONE, liveReplicas >= 1 ? num : -1);
        if (allReplicas >= 2)
            executeOnAllCoordinators(cluster, statement, ConsistencyLevel.TWO, liveReplicas >= 2 ? num : -1);
        executeOnAllCoordinators(cluster, statement, ConsistencyLevel.ALL, liveReplicas >= allReplicas ? num : -1);
    }

    private void executeOnAllCoordinators(Cluster cluster, String query, ConsistencyLevel level, int expected) throws Exception
    {
        // test different coordinator
        for (int nodeId = 1; nodeId <= cluster.size(); nodeId++)
        {
            final int node = nodeId;
            if (expected >= 0)
                assertEquals(expected, cluster.coordinator(nodeId).execute(query, level).length);
            else
            {
                try
                {
                    cluster.coordinator(node).execute(query, level);
                }
                catch (Throwable e)
                {
                    assertTrue(e.getClass().getSimpleName().equals("ReadFailureException"));
                }
            }
        }
    }

    private void markIndexNonQueryable(IInvokableInstance node, String keyspace, String table, String indexName) throws Exception
    {
        expectedNodeIndexQueryability.put(NodeIndex.create(keyspace, indexName, node), Index.Status.BUILD_FAILED);

        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStore(table).indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.makeIndexNonQueryable(index, Index.Status.BUILD_FAILED);
        });
    }

    private void markIndexQueryable(IInvokableInstance node, String keyspace, String table, String indexName) throws Exception
    {
        expectedNodeIndexQueryability.put(NodeIndex.create(keyspace, indexName, node), Index.Status.BUILD_SUCCEEDED);

        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStore(table).indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.makeIndexNonQueryable(index, Index.Status.BUILD_SUCCEEDED);
        });
    }

    private void markIndexBuilding(IInvokableInstance node, String keyspace, String table, String indexName) throws Exception
    {
        expectedNodeIndexQueryability.put(NodeIndex.create(keyspace, indexName, node), Index.Status.FULL_REBUILD_STARTED);

        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStore(table).indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.markIndexesBuilding(Collections.singleton(index), true, false);
        });
    }

    private void assertIndexingStatus(Cluster cluster)
    {
        for (String ks : keyspaces)
        {
            for (String indexName : indexesPerKs)
            {
                assertIndexingStatus(cluster, ks, indexName);
            }
        }
    }

    private static void assertIndexingStatus(Cluster cluster, String keyspace, String indexName)
    {
        for (int nodeId = 1; nodeId <= cluster.size(); nodeId++)
        {
            for (int replica = 1; replica <= cluster.size(); replica++)
            {
                NodeIndex nodeIndex = NodeIndex.create(keyspace, indexName, cluster.get(replica));
                Index.Status expected = expectedNodeIndexQueryability.get(nodeIndex);

                assertIndexingStatus(cluster.get(nodeId), keyspace, indexName, cluster.get(replica), expected);
            }
        }
    }

    private static void assertIndexingStatus(IInvokableInstance node, String keyspaceName, String indexName, IInvokableInstance replica, Index.Status expected)
    {
        InetAddressAndPort replicaAddressAndPort = getFullAddress(replica);
        try
        {
            Index.Status actual = getNodeIndexStatus(node, keyspaceName, indexName, replicaAddressAndPort);
            String errorMessage = String.format("Failed to verify %s.%s status for replica %s on node %s, expected %s, but got %s.",
                                                keyspaceName, indexName, replica.broadcastAddress(), node.broadcastAddress(), expected, actual);
            assertEquals(errorMessage, expected, actual);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void waitForIndexingStatus(IInvokableInstance node, String keyspace, String index, IInvokableInstance replica, Index.Status status)
    {
        InetAddressAndPort replicaAddressAndPort = getFullAddress(replica);
        await().atMost(5, TimeUnit.SECONDS)
               .until(() -> node.callOnInstance(() -> getIndexStatus(keyspace, index, replicaAddressAndPort) == status).booleanValue());
    }

    private static Index.Status getNodeIndexStatus(IInvokableInstance node, String keyspaceName, String indexName, InetAddressAndPort replica)
    {
        return Index.Status.values()[node.callsOnInstance(() -> getIndexStatus(keyspaceName, indexName, replica).ordinal()).call()];
    }
    
    private static Index.Status getIndexStatus(String keyspaceName, String indexName, InetAddressAndPort replica)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            return Index.Status.UNKNOWN;

        TableMetadata table = keyspace.findIndexedTable(indexName).orElse(null);
        if (table == null)
            return Index.Status.UNKNOWN;

        SecondaryIndexManager indexManager = Keyspace.openAndGetStore(table).indexManager;
        
        return indexManager.getIndexStatus(replica, keyspaceName, indexName);
    }

    private static InetAddressAndPort getFullAddress(IInvokableInstance node)
    {
        InetAddress address = node.broadcastAddress().getAddress();
        int port = node.callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().port);
        return InetAddressAndPort.getByAddressOverrideDefaults(address, port);
    }
    
    private static class NodeIndex
    {
        private final String keyspace;
        private final String index;
        private final IInvokableInstance node;

        NodeIndex(String keyspace, String index, IInvokableInstance node)
        {
            this.keyspace = keyspace;
            this.index = index;
            this.node = node;
        }

        public static NodeIndex create(String keyspace, String index, IInvokableInstance node)
        {
            return new NodeIndex(keyspace, index, node);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeIndex that = (NodeIndex) o;
            return node.equals(that.node) &&
                   Objects.equal(keyspace, that.keyspace) &&
                   Objects.equal(index, that.index);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(keyspace, index, node);
        }
    }
}
