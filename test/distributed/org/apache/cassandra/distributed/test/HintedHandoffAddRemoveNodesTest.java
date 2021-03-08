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

import org.apache.cassandra.distributed.action.GossipHelper;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.distributed.action.GossipHelper.decommission;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests around removing and adding nodes from and to a cluster while hints are still outstanding.
 */
public class HintedHandoffAddRemoveNodesTest extends TestBaseImpl
{
    /**
     * Replaces Python dtest {@code hintedhandoff_test.py:TestHintedHandoff.test_hintedhandoff_decom()}.
     */
    @Test
    public void shouldStreamHintsDuringDecommission() throws Exception
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.decom_hint_test (key int PRIMARY KEY, value int)"));
            
            cluster.get(4).shutdown().get();
            
            // Write data using the second node as the coordinator...
            populate(cluster, "decom_hint_test", 2, 0, 128, ConsistencyLevel.ONE);
            Long totalHints = countTotalHints(cluster);
            // ...and verify that we've accumulated hints intended for node 4, which is down.
            assertThat(totalHints).isGreaterThan(0);

            // Decomision node 1...
            assertEquals(4, endpointsKnownTo(cluster, 2));
            cluster.run(decommission(), 1);
            await().pollDelay(1, SECONDS).until(() -> endpointsKnownTo(cluster, 2) == 3);
            // ...and verify that all data still exists on either node 2 or 3.
            verify(cluster, "decom_hint_test", 2, 0, 128, ConsistencyLevel.ONE);
            
            // Start node 4 back up and verify that all hints were delivered.
            cluster.get(4).startup();
            await().atMost(30, SECONDS).pollDelay(3, SECONDS).until(() -> count(cluster, "decom_hint_test", 4).equals(totalHints));

            // Now decommission both nodes 2 and 3...
            cluster.run(GossipHelper.decommission(true), 2);
            cluster.run(GossipHelper.decommission(true), 3);
            await().pollDelay(1, SECONDS).until(() -> endpointsKnownTo(cluster, 4) == 1);
            // ...and verify that even if we drop below the replication factor of 2, all data has been preserved.
            verify(cluster, "decom_hint_test", 4, 0, 128, ConsistencyLevel.ONE);
        }
    }

    @Test
    public void shouldBootstrapWithHintsOutstanding() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.boot_hint_test (key int PRIMARY KEY, value int)"));

            cluster.get(3).shutdown().get();

            // Write data using the second node as the coordinator...
            populate(cluster, "boot_hint_test", 2, 0, 128, ConsistencyLevel.ONE);
            Long totalHints = countTotalHints(cluster);
            // ...and verify that we've accumulated hints intended for node 3, which is down.
            assertThat(totalHints).isGreaterThan(0);

            // Bootstrap a new/4th node into the cluster...
            bootstrapAndJoinNode(cluster);

            // ...and verify that all data is available.
            verify(cluster, "boot_hint_test", 4, 0, 128, ConsistencyLevel.ONE);

            // Finally, bring node 3 back up and verify that all hints were delivered.
            cluster.get(3).startup();
            await().atMost(30, SECONDS).pollDelay(3, SECONDS).until(() -> count(cluster, "boot_hint_test", 3).equals(totalHints));
            verify(cluster, "boot_hint_test", 3, 0, 128, ConsistencyLevel.ONE);
            verify(cluster, "boot_hint_test", 3, 0, 128, ConsistencyLevel.TWO);
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    private Long countTotalHints(Cluster cluster)
    {
        return cluster.get(2).callOnInstance(() -> StorageMetrics.totalHints.getCount());
    }

    @SuppressWarnings("SameParameterValue")
    private void populate(Cluster cluster, String table, int coordinator, int start, int count, ConsistencyLevel cl)
    {
        for (int i = start; i < start + count; i++)
            cluster.coordinator(coordinator)
                   .execute("INSERT INTO " + KEYSPACE + '.' + table + " (key, value) VALUES (?, ?)", cl, i, i);
    }

    @SuppressWarnings("SameParameterValue")
    private void verify(Cluster cluster, String table, int coordinator, int start, int count, ConsistencyLevel cl)
    {
        for (int i = start; i < start + count; i++)
        {
            Object[][] row = cluster.coordinator(coordinator)
                                    .execute("SELECT key, value FROM " + KEYSPACE + '.' + table + " WHERE key = ?", cl, i);
            assertRows(row, row(i, i));
        }
    }

    private Long count(Cluster cluster, String table, int node)
    {
        return (Long) cluster.get(node).executeInternal("SELECT COUNT(*) FROM " + KEYSPACE + '.' + table)[0][0];
    }

    private int endpointsKnownTo(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> StorageService.instance.getTokenMetadata().getAllEndpoints().size());
    }
}
