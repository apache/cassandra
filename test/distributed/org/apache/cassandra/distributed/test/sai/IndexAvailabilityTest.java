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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.sai.SAIUtil.waitForIndexQueryable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexAvailabilityTest extends TestBaseImpl
{
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String CREATE_TABLE = "CREATE TABLE %s.%s (pk text primary key, v1 int, v2 text) " +
                                               "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX = "CREATE CUSTOM INDEX %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    private static final Map<NodeIndex, Index.Status> expectedNodeIndexQueryability = new ConcurrentHashMap<>();
    private List<String> keyspaces;
    private List<String> indexesPerKs;

    @Test
    public void verifyIndexStatusPropagation() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("index_status_poll_interval_in_seconds", "1"))
                                           .start()))
        {
            verifyIndexStatusPropagation(cluster);
        }
    }

    @Test
    public void verifyIndexStatusPropagationMixedPatchVersion() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .withInstanceInitializer(MixedPatchVersionHelper::setVersions)
                                           .start()))
        {
            verifyIndexStatusPropagation(cluster);
        }
    }

    @Test
    public void verifyIndexStatusPropagationMixedMajorVersion() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .withInstanceInitializer(MixedMajorVersionHelper::setVersions)
                                           .start()))
        {
            verifyIndexStatusPropagation(cluster);
        }
    }

    private void verifyIndexStatusPropagation(Cluster cluster)
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
        cluster.schemaChange("DROP INDEX " + ks2 + '.' + index2);
        expectedNodeIndexQueryability.keySet().forEach(k -> {
            if (k.keyspace.equals(ks2) && k.index.equals(index2))
                expectedNodeIndexQueryability.put(k, Index.Status.UNKNOWN);
        });
        assertIndexingStatus(cluster);

        // drop ks3 cf1, there should be no ks3 index1/index2 status
        cluster.schemaChange("DROP TABLE " + ks3 + '.' + cf1);
        expectedNodeIndexQueryability.keySet().forEach(k -> {
            if (k.keyspace.equals(ks3))
                expectedNodeIndexQueryability.put(k, Index.Status.UNKNOWN);
        });
        assertIndexingStatus(cluster);
    }

    @SuppressWarnings("DataFlowIssue")
    private void markIndexNonQueryable(IInvokableInstance node, String keyspace, String table, String indexName)
    {
        expectedNodeIndexQueryability.put(NodeIndex.create(keyspace, indexName, node), Index.Status.BUILD_FAILED);

        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStore(table).indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.makeIndexNonQueryable(index, Index.Status.BUILD_FAILED);
        });
    }

    @Test
    public void testIndexExceptionsTwoIndexesOn3NodeCluster() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                .withConfig(config -> config.with(GOSSIP)
                                            .with(NETWORK)
                                            .set("index_status_poll_interval_in_seconds", "1"))
                .start()))
        {
            String ks2 = "ks2";
            String cf1 = "cf1";
            String index1 = "cf1_idx1";
            String index2 = "cf1_idx2";

            // Create keyspace, table with correct column types
            cluster.schemaChange(String.format(CREATE_KEYSPACE, ks2, 2));
            cluster.schemaChange("CREATE TABLE " + ks2 + '.' + cf1 + " (pk int PRIMARY KEY, v1 int, v2 int)");
            executeOnAllCoordinators(cluster,
                              "SELECT pk FROM " + ks2 + '.' + cf1 + " WHERE v1=0 AND v2=0 ALLOW FILTERING");
            executeOnAllCoordinators(cluster,
                               "SELECT pk FROM " + ks2 + '.' + cf1 + " WHERE v2=0 ALLOW FILTERING");
            executeOnAllCoordinators(cluster,
                               "SELECT pk FROM " + ks2 + '.' + cf1 + " WHERE v1=0 ALLOW FILTERING");

            cluster.schemaChange(String.format(CREATE_INDEX, index1, ks2, cf1, "v1"));
            cluster.schemaChange(String.format(CREATE_INDEX, index2, ks2, cf1, "v2"));
            cluster.forEach(node -> expectedNodeIndexQueryability.put(NodeIndex.create(ks2, index1, node), Index.Status.BUILD_SUCCEEDED));
            for (IInvokableInstance node : cluster.get(2, 1, 3))
                for (IInvokableInstance replica : cluster.get(1, 2, 3))
                    waitForIndexingStatus(node, ks2, index1, replica, Index.Status.BUILD_SUCCEEDED);

            // Mark only index2 as building on node3, leave index1 in BUILD_SUCCEEDED state
            markIndexBuilding(cluster.get(3), ks2, cf1, index2);
            cluster.forEach(node -> expectedNodeIndexQueryability.put(NodeIndex.create(ks2, index2, node), Index.Status.FULL_REBUILD_STARTED));
            for (IInvokableInstance node : cluster.get(1, 2, 3))
                waitForIndexingStatus(node, ks2, index2, cluster.get(3), Index.Status.FULL_REBUILD_STARTED);

            assertThatThrownBy(() ->
                    executeOnAllCoordinators(cluster,
                                       "SELECT pk FROM " + ks2 + '.' + cf1 + " WHERE v1=0 AND v2=0"))
                    .hasMessageContaining("Operation failed - received 1 responses and 1 failures: INDEX_BUILD_IN_PROGRESS");

            // Mark only index2 as failing on node2, leave index1 in BUILD_SUCCEEDED state
            markIndexBuilding(cluster.get(2), ks2, cf1, index2);
            cluster.forEach(node -> expectedNodeIndexQueryability.put(NodeIndex.create(ks2, index2, node), Index.Status.FULL_REBUILD_STARTED));
            for (IInvokableInstance node : cluster.get(1, 2, 3))
                waitForIndexingStatus(node, ks2, index2, cluster.get(2), Index.Status.FULL_REBUILD_STARTED);


            assertThatThrownBy(() ->
                    executeOnAllCoordinators(cluster,
                                      "SELECT pk FROM " + ks2 + '.' + cf1 + " WHERE v1=0 AND v2=0"))
                    .hasMessageContaining("Operation failed - received 1 responses and 1 failures: INDEX_BUILD_IN_PROGRESS");

            // Mark only index2 as failing on node1, leave index1 in BUILD_SUCCEEDED state
            markIndexNonQueryable(cluster.get(1), ks2, cf1, index2);
            cluster.forEach(node -> expectedNodeIndexQueryability.put(NodeIndex.create(ks2, index2, node), Index.Status.BUILD_FAILED));
            for (IInvokableInstance node : cluster.get(1, 2, 3)) {
                waitForIndexingStatus(node, ks2, index2, cluster.get(1), Index.Status.BUILD_FAILED);
            }

            assertThatThrownBy(() ->
                    executeOnAllCoordinators(cluster,
                                       "SELECT pk FROM " + ks2 + '.' + cf1 + " WHERE v1=0 AND v2=0"))
                    .hasMessageMatching("^Operation failed - received 0 responses and 2 failures: INDEX_NOT_AVAILABLE from .+, INDEX_BUILD_IN_PROGRESS from .+$");
        }
    }

    private void executeOnAllCoordinators(Cluster cluster, String query)
    {
        // test different coordinator
        for (int nodeId = 1; nodeId <= cluster.size(); nodeId++)
        {
            assertEquals(0, cluster.coordinator(nodeId).execute(query, ConsistencyLevel.LOCAL_QUORUM).length);
        }
    }

    @SuppressWarnings("DataFlowIssue")
    private void markIndexQueryable(IInvokableInstance node, String keyspace, String table, String indexName)
    {
        expectedNodeIndexQueryability.put(NodeIndex.create(keyspace, indexName, node), Index.Status.BUILD_SUCCEEDED);

        node.runOnInstance(() -> {
            SecondaryIndexManager sim = Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStore(table).indexManager;
            Index index = sim.getIndexByName(indexName);
            sim.makeIndexQueryable(index, Index.Status.BUILD_SUCCEEDED);
        });
    }

    @SuppressWarnings("DataFlowIssue")
    private void markIndexBuilding(IInvokableInstance node, String keyspace, String table, String indexName)
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
               .until(() -> node.callOnInstance(() -> getIndexStatus(keyspace, index, replicaAddressAndPort) == status));
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

        return IndexStatusManager.instance.getIndexStatus(replica, keyspaceName, indexName);
    }

    private static InetAddressAndPort getFullAddress(IInvokableInstance node)
    {
        InetAddress address = node.broadcastAddress().getAddress();
        int port = node.callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getPort());
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

    @Test
    public void verifyIndexStatusPropagationViaTablePolling() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                                       .set("index_status_poll_interval_in_seconds", 5)).start()))
        {
            String ks = "poll_test_ks";
            String cf = "cf1";
            String index = "cf1_poll_idx";

            cluster.schemaChange(String.format(CREATE_KEYSPACE, ks, 2));
            cluster.schemaChange(String.format(CREATE_TABLE, ks, cf));
            cluster.schemaChange(String.format(CREATE_INDEX, index, ks, cf, "v1"));
            waitForIndexQueryable(cluster, ks);

            await().atMost(15, TimeUnit.SECONDS)
                   .until(() -> cluster.get(2).callOnInstance(() -> {
                       InetAddressAndPort node1Address = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
                       int port = FBUtilities.getBroadcastAddressAndPort().getPort();
                       InetAddressAndPort node1 = InetAddressAndPort.getByAddressOverrideDefaults(node1Address.getAddress(), port);
                       return IndexStatusManager.instance.getIndexStatus(node1, ks, index) == Index.Status.BUILD_SUCCEEDED;
                   }));
        }
    }


    @Test
    public void verifyMaxSizeIndexTest() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                .withInstanceInitializer(MixedPatchVersionHelper::setVersions)
                .start()))
        {
            LogAction logs = cluster.get(1).logs();
            long mark = logs.mark();

            cluster.get(1).runOnInstance(() -> {
                Map<String, Index.Status> localStatusMap =
                        IndexStatusManager.instance.peerIndexStatus
                                .computeIfAbsent(ClusterMetadata.current().myNodeId(), k -> new HashMap<>());

                for (int ks = 0; ks < 100; ks++)
                    for (int idx = 0; idx < 200; idx++)
                        localStatusMap.put("keyspace_" + ks + ".my_table_index_name" + idx, Index.Status.BUILD_SUCCEEDED);

                IndexStatusManager.instance.propagateLocalIndexStatus("keyspace_trigger", "trigger_idx", Index.Status.BUILD_SUCCEEDED);
            });

            assertFalse(logs.watchFor(mark, "exceeds limit").getResult().isEmpty());
        }
    }


    @Test
    public void verifyMixedVersionSkipsTableWritesAndUsesGossip() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .withInstanceInitializer(MixedMajorVersionHelper::setVersions)
                                           .start()))
        {
            String ks = "mixed_ks";
            String cf = "cf1";
            String index1 = "cf1_idx1";

            cluster.schemaChange(String.format(CREATE_KEYSPACE, ks, 2));
            cluster.schemaChange(String.format(CREATE_TABLE, ks, cf));
            cluster.schemaChange(String.format(CREATE_INDEX, index1, ks, cf, "v1"));
            waitForIndexQueryable(cluster, ks);

            waitForIndexingStatus(cluster.get(2), ks, index1, cluster.get(1), Index.Status.BUILD_SUCCEEDED);

            cluster.get(1).runOnInstance(() -> {
                Map<NodeId, Map<String, Index.Status>> allStatuses = SystemDistributedKeyspace.allIndexStatuses();
                assertTrue("index_build_status table should be empty in mixed-version cluster, but has " + allStatuses.size() + " entries",
                           allStatuses.isEmpty());
            });

            markIndexNonQueryable(cluster.get(1), ks, cf, index1);
            waitForIndexingStatus(cluster.get(2), ks, index1, cluster.get(1), Index.Status.BUILD_FAILED);

            cluster.get(1).runOnInstance(() -> {
                Map<NodeId, Map<String, Index.Status>> allStatuses =
                SystemDistributedKeyspace.allIndexStatuses();
                assertTrue("index_build_status table should still be empty in mixed-version cluster",
                           allStatuses.isEmpty());
            });

            markIndexQueryable(cluster.get(1), ks, cf, index1);
            waitForIndexingStatus(cluster.get(2), ks, index1, cluster.get(1), Index.Status.BUILD_SUCCEEDED);
        }
    }

    public static class MixedMajorVersionHelper
    {
        @SuppressWarnings({ "unused", "resource" })
        static void setVersions(ClassLoader loader, int node)
        {
            if (node == 1)
                new ByteBuddy().rebase(FBUtilities.class)
                               .method(named("getReleaseVersionString"))
                               .intercept(MethodDelegation.to(MixedMajorVersionHelper.class))
                               .make()
                               .load(loader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static String getReleaseVersionString()
        {
            return "4.1.0";
        }
    }

    public static class MixedPatchVersionHelper
    {
        @SuppressWarnings({ "unused", "resource" })
        static void setVersions(ClassLoader loader, int node)
        {
            if (node == 1)
                new ByteBuddy().rebase(FBUtilities.class)
                               .method(named("getReleaseVersionString"))
                               .intercept(MethodDelegation.to(MixedPatchVersionHelper.class))
                               .make()
                               .load(loader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static String getReleaseVersionString()
        {
            return "5.0.2";
        }
    }
}
