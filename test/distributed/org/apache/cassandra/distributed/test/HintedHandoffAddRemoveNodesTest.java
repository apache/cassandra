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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.awaitility.Awaitility;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.setProgressBarrierMinConsistencyLevel;
import static org.apache.cassandra.db.ConsistencyLevel.NODE_LOCAL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.TWO;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Tests around removing and adding nodes from and to a cluster while hints are still outstanding.
 */
public class HintedHandoffAddRemoveNodesTest extends TestBaseImpl
{
    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void shouldAvoidHintTransferOnDecommission() throws Exception
    {
        // This test was written with expectation that auth table is going to be empty. Since auth setup became syncrhonous for tests
        // we need to skip it now, since otherwise streaming will fail.
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.SKIP_AUTH_SETUP, "true");
             Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.set("transfer_hints_on_decommission", false)
                                                                         .set("progress_barrier_timeout", "1000ms")
                                                                         .set("progress_barrier_backoff", "100ms")
                                                                         // Just to make test pass faster
                                                                         .set("progress_barrier_min_consistency_level", NODE_LOCAL)
                                                                         .set("progress_barrier_default_consistency_level", NODE_LOCAL)
                                                                         .with(GOSSIP, NETWORK))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.decom_no_hints_test (key int PRIMARY KEY, value int)"));

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.decom_no_hints_test (key, value) VALUES (?, ?)"), ALL, 0, 0);
            long hintsBeforeShutdown = countTotalHints(cluster.get(1));
            assertThat(hintsBeforeShutdown).isEqualTo(0);
            long hintsDelivered = countHintsDelivered(cluster.get(1));
            assertThat(hintsDelivered).isEqualTo(0);

            // Shutdown node 3 so hints can be written against it.
            cluster.get(3).shutdown().get();

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.decom_no_hints_test (key, value) VALUES (?, ?)"), TWO, 0, 0);
            Awaitility.await().until(() -> countTotalHints(cluster.get(1)) > 0);
            long hintsAfterShutdown = countTotalHints(cluster.get(1));
            assertThat(hintsAfterShutdown).isEqualTo(1);

            cluster.get(2).runOnInstance(() -> setProgressBarrierMinConsistencyLevel(org.apache.cassandra.db.ConsistencyLevel.ONE));
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1), 3);
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
            long hintsDeliveredByDecom = countHintsDelivered(cluster.get(2));
            String mode = cluster.get(2).callOnInstance(() -> StorageService.instance.getOperationMode());
            assertEquals(StorageService.Mode.DECOMMISSIONED.toString(), mode);
            assertThat(hintsDeliveredByDecom).isEqualTo(0);
        }
    }

    /**
     * Replaces Python dtest {@code hintedhandoff_test.py:TestHintedHandoff.test_hintedhandoff_decom()}.
     */
    @Test
    public void shouldStreamHintsDuringDecommission() throws Exception
    {
        try (Cluster cluster = builder().withNodes(5)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    // Just to make test pass faster
                                                                    .set("progress_barrier_min_consistency_level", NODE_LOCAL)
                                                                    .set("progress_barrier_default_consistency_level", NODE_LOCAL))
                                        .withoutVNodes()
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.decom_hint_test (key int PRIMARY KEY, value int)"));
            
            cluster.get(5).shutdown().get();
            
            // Write data using the second node as the coordinator...
            populate(cluster, "decom_hint_test", 2, 0, 128, ConsistencyLevel.ONE);

            // ...and verify that we've accumulated hints intended for node 4, which is down.
            Awaitility.await().until(() -> countTotalHints(cluster.get(2)) > 0);
            long totalHints = countTotalHints(cluster.get(2));

            // Decomision node 2...
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            // ...and verify that all data still exists on either node 2 or 3.
            verify(cluster, "decom_hint_test", 1, 0, 128, ConsistencyLevel.ONE);
            
            // Start node 5 back up and verify that all hints were delivered.
            cluster.get(5).startup();
            await().atMost(30, SECONDS).pollDelay(3, SECONDS).until(() -> count(cluster, "decom_hint_test", 4) >= totalHints);

            // Now decommission both node 4
            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();
            cluster.get(4).nodetoolResult("decommission", "--force").asserts().success();
            cluster.get(5).nodetoolResult("decommission", "--force").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1), 2, 3, 4, 5);
            // ...and verify that even if we drop below the replication factor of 2, all data has been preserved.
            verify(cluster, "decom_hint_test", 1, 0, 128, ConsistencyLevel.ONE);
        }
    }

    @Test
    public void shouldBootstrapWithHintsOutstanding() throws Exception
    {
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.CONSISTENT_RANGE_MOVEMENT, "false");
             Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4, 1))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    // Just to make test pass faster
                                                                    .set("progress_barrier_min_consistency_level", NODE_LOCAL)
                                                                    .set("progress_barrier_default_consistency_level", NODE_LOCAL))
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.boot_hint_test (key int PRIMARY KEY, value int)"));

            cluster.get(3).shutdown().get();

            // Write data using the second node as the coordinator...
            populate(cluster, "boot_hint_test", 2, 0, 128, ConsistencyLevel.ONE);

            // ...and verify that we've accumulated hints intended for node 3, which is down.
            Awaitility.await().until(() -> countTotalHints(cluster.get(2)) > 0);
            long totalHints = countTotalHints(cluster.get(2));

            // Bootstrap a new/4th node into the cluster...
            bootstrapAndJoinNode(cluster);

            // ...and verify that all data is available.
            verify(cluster, "boot_hint_test", 4, 0, 128, ConsistencyLevel.ONE);

            // Finally, bring node 3 back up and verify that all hints were delivered.
            cluster.get(3).startup();
            await().atMost(30, SECONDS).pollDelay(3, SECONDS).until(() -> count(cluster, "boot_hint_test", 3) >= totalHints);
            verify(cluster, "boot_hint_test", 3, 0, 128, ConsistencyLevel.ONE);
            verify(cluster, "boot_hint_test", 3, 0, 128, ConsistencyLevel.TWO);
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    private long countTotalHints(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> StorageMetrics.totalHints.getCount());
    }

    @SuppressWarnings("Convert2MethodRef")
    private long countHintsDelivered(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> HintsServiceMetrics.hintsSucceeded.getCount());
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

    private long count(Cluster cluster, String table, int node)
    {
        return (Long) cluster.get(node).executeInternal("SELECT COUNT(*) FROM " + KEYSPACE + '.' + table)[0][0];
    }
}
