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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.reads.tracked.TrackedSummaryResponse;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertMatchingSummaryIdSpaceForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertMatchingSummaryIdSpaceForTable;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertOffsetsIsSuperSet;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertSummaryIdSpaceIsSuperSet;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.numLogReconciliations;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.row;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryForTable;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;

public class MutationTrackingReadReconciliationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingReadReconciliationTest.class);

    public static void awaitNodeLiveness(IInvokableInstance from, InetAddressAndPort node, boolean alive)
    {
        from.runOnInstance(() -> {
            for (int i=0; i<30; i++)
            {
                if (FailureDetector.instance.isAlive(node) == alive)
                    return;

                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            throw new AssertionError(node + " wasn't marked " + (alive ? "alive" : "dead"));

        });
    }

    public static void awaitNodeDead(IInvokableInstance from, IInvokableInstance node)
    {
        awaitNodeLiveness(from, InetAddressAndPort.getByAddress(node.broadcastAddress()), false);
    }

    public static void awaitNodeAlive(IInvokableInstance from, IInvokableInstance node)
    {
        awaitNodeLiveness(from, InetAddressAndPort.getByAddress(node.broadcastAddress()), true);
    }

    // TODO (expected): remove after speculation is implemented
    private static Object[][] queryWithRetries(ICoordinator coordinator, String query) throws InterruptedException
    {
        int attempt = 0;
        for (;;)
        {
            attempt++;
            try
            {
                return coordinator.execute(query, ConsistencyLevel.QUORUM);
            }
            catch (Throwable t)
            {
                if (attempt < 10 && t.getClass().getSimpleName().equals(ReadTimeoutException.class.getSimpleName()))
                {
                    Thread.sleep(2000);
                }
                else
                {
                    throw t;
                }
            }
        }
    }

    /**
     * Test a read reconciliation where the coordinator doesn't have a read response it needs to apply
     * additional mutations to
     */
    @Test
    public void testBasicReadReconciliation() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.forEach(node -> {
                node.runOnInstance(() -> {
                    KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    Assert.assertEquals(ReplicationType.tracked, ksm.params.replicationType);
                });
            });

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // insert a row at ALL, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);

            MutationSummary firstSummary = summaryForKey(cluster.get(1), keyspaceName, "tbl", 1);
            CoordinatorLogId logId = firstSummary.get(0).logId();
            Offsets firstIds = summaryIdSpace(firstSummary.get(logId));
            Assert.assertEquals(1, firstIds.offsetCount());

            cluster.get(2, 3).forEach(node -> assertMatchingSummaryIdSpaceForKey(node, keyspaceName, tableName, 1, firstSummary));

            // block messages to node 3 and perform a write at quorum
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            awaitNodeDead(cluster.get(1), cluster.get(3));

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            MutationSummary finalSummary = summaryForKey(cluster.get(1), keyspaceName, "tbl", 1);
            Assert.assertEquals(1, finalSummary.size());
            Offsets finalIds = summaryIdSpace(finalSummary.get(logId));

            Assert.assertEquals(2, finalIds.offsetCount());
            assertOffsetsIsSuperSet(finalIds, firstIds);;

            Offsets secondIds = Offsets.Immutable.difference(finalIds, firstIds);
            Assert.assertEquals(1, secondIds.offsetCount());
            Assert.assertEquals(0, Offsets.Immutable.intersection(firstIds, secondIds).offsetCount());


            // second node should have the new id, third should not
            assertMatchingSummaryIdSpaceForKey(cluster.get(2), keyspaceName, tableName, 1, finalSummary);
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), keyspaceName, tableName, 1, firstSummary);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();
            awaitNodeAlive(cluster.get(1), cluster.get(3));
            awaitNodeDead(cluster.get(1), cluster.get(2));

            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = queryWithRetries(cluster.coordinator(1), format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName));
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1)), result);

            // check that node3 has the new ids
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), keyspaceName, tableName, 1, finalSummary);
        }
    }

    /**
     * Test a read reconciliation where the coordinator needs to receive and apply mutations missing
     * from its data response
     */
    @Test
    public void testReadReconciliationApplyMutations() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.forEach(node -> {
                logger.info(">>> {}", node);
                node.runOnInstance(() -> {
                    KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    Assert.assertEquals(ReplicationType.tracked, ksm.params.replicationType);
                });
            });

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);
            MutationSummary firstSummary = summaryForKey(cluster.get(1), keyspaceName, "tbl", 1);
            CoordinatorLogId logId = firstSummary.get(0).logId();
            Offsets firstIds = summaryIdSpace(firstSummary.get(logId));

            cluster.get(2, 3).forEach(node -> {
                assertMatchingSummaryIdSpaceForKey(node, keyspaceName, tableName, 1, firstSummary);
            });

            // block messages to node 3 and perform a write at quorum
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            MutationSummary finalSummary = summaryForKey(cluster.get(1), keyspaceName, "tbl", 1);
            Assert.assertEquals(1, finalSummary.size());
            Offsets finalIds = summaryIdSpace(finalSummary.get(logId));

            Assert.assertEquals(2, finalIds.offsetCount());
            assertOffsetsIsSuperSet(finalIds, firstIds);;

            Offsets secondIds = Offsets.Immutable.difference(finalIds, firstIds);
            Assert.assertEquals(1, secondIds.offsetCount());
            Assert.assertEquals(0, Offsets.Immutable.intersection(firstIds, secondIds).offsetCount());


            // second node should have the new id, third should not
            assertMatchingSummaryIdSpaceForKey(cluster.get(2), keyspaceName, tableName, 1, finalSummary);
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), keyspaceName, tableName, 1, firstSummary);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();


            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = queryWithRetries(cluster.coordinator(3), format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName));
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1)), result);

            // check that node3 has the new ids
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), keyspaceName, tableName, 1, finalSummary);
        }
    }

    /**
     * Test a read reconciliation where the coordinator doesn't have a read response it needs to apply
     * additional mutations to
     */
    @Test
    public void testBasicRangeReadReconciliation() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.forEach(node -> {
                node.runOnInstance(() -> {
                    KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    Assert.assertEquals(ReplicationType.tracked, ksm.params.replicationType);
                });
            });

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);

            MutationSummary firstSummary = summaryForTable(cluster.get(1), keyspaceName, "tbl");
            Assert.assertEquals(1, firstSummary.size());
            CoordinatorLogId logId = firstSummary.get(0).logId();
            Offsets firstIds = summaryIdSpace(firstSummary.get(logId));
            Assert.assertEquals(1, firstIds.offsetCount());

            cluster.get(2, 3).forEach(node -> {
                assertMatchingSummaryIdSpaceForKey(node, keyspaceName, tableName, 1, firstSummary);
            });

            // block messages to node 3 and perform a write at quorum
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (2, 2, 2)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            MutationSummary finalSummary = summaryForTable(cluster.get(1), keyspaceName, tableName);
            Assert.assertEquals(1, finalSummary.size());
            Offsets finalIds = summaryIdSpace(finalSummary.get(logId));

            Assert.assertEquals(3, finalIds.offsetCount());
            assertSummaryIdSpaceIsSuperSet(finalSummary, firstSummary);

            // second node should have the new id, third should not
            assertMatchingSummaryIdSpaceForTable(cluster.get(2), keyspaceName, tableName, finalSummary);
            assertMatchingSummaryIdSpaceForTable(cluster.get(3), keyspaceName, tableName, firstSummary);

            // reverse the partition and do a read, read should include coordinator (1)'s ID and replica (3), even though (3) is missing the ID
            cluster.filters().reset();
            // cluster.filters().allVerbs().to(2).drop();
            // cluster.filters().allVerbs().from(2).drop();
            cluster.get(2).shutdown().get();
            // wait for node1 gossip to settle, shouldn't see any failures due to node1 coordinating to node2 replica

            // No reconciliation has happened yet
            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = queryWithRetries(cluster.coordinator(1), format("SELECT * FROM %s.%s", keyspaceName, tableName));
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1), row(2, 2, 2)), result);

            // Coordinator sends its missing mutations to 3 on read
            Assert.assertEquals(1, numLogReconciliations(cluster.get(1)));

            // check that node3 has the new ids
            assertMatchingSummaryIdSpaceForTable(cluster.get(3), keyspaceName, tableName, finalSummary);
        }
    }

    @Test
    public void testBasicRangeReadReconciliationApplyMutations() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));

            cluster.forEach(node -> {
                logger.info(">>> {}", node);
                node.runOnInstance(() -> {
                    KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    Assert.assertEquals(ReplicationType.tracked, ksm.params.replicationType);
                });
            });

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);

            MutationSummary firstSummary = summaryForTable(cluster.get(1), keyspaceName, "tbl");
            Assert.assertEquals(1, firstSummary.size());
            CoordinatorLogId logId = firstSummary.get(0).logId();
            Offsets firstIds = summaryIdSpace(firstSummary.get(logId));
            Assert.assertEquals(1, firstIds.offsetCount());

            cluster.get(2, 3).forEach(node -> {
                assertMatchingSummaryIdSpaceForKey(node, keyspaceName, tableName, 1, firstSummary);
            });

            // block messages to node 3 and perform a write at quorum
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (2, 2, 2)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            MutationSummary finalSummary = summaryForTable(cluster.get(1), keyspaceName, tableName);
            Assert.assertEquals(1, finalSummary.size());
            Offsets finalIds = summaryIdSpace(finalSummary.get(logId));

            Assert.assertEquals(3, finalIds.offsetCount());
            assertSummaryIdSpaceIsSuperSet(finalSummary, firstSummary);

            // second node should have the new id, third should not
            assertMatchingSummaryIdSpaceForTable(cluster.get(2), keyspaceName, tableName, finalSummary);
            assertMatchingSummaryIdSpaceForTable(cluster.get(3), keyspaceName, tableName, firstSummary);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();
            awaitNodeAlive(cluster.get(3), cluster.get(1));
            awaitNodeDead(cluster.get(3), cluster.get(2));


            Assert.assertEquals(0, numLogReconciliations(cluster.get(3)));
            Object[][] result = queryWithRetries(cluster.coordinator(3), format("SELECT * FROM %s.%s", keyspaceName, tableName));
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1), row(2, 2, 2)), result);

            // Coordinator sends its missing mutations to 3 on read
            Assert.assertEquals(1, numLogReconciliations(cluster.get(3)));

            // check that node3 has the new ids
            assertMatchingSummaryIdSpaceForTable(cluster.get(3), keyspaceName, tableName, finalSummary);
        }
    }

    /**
     * Verifies that when replicas are already in sync, the read completes without waiting for
     * or needing any MT_READ_RECONCILE_ACK messages.
     */
    @Test
    public void testInSyncReadCompletesWithZeroReconcileAcks() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "zero_ack_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, v int, primary key (k));", keyspaceName, tableName));

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 10)", keyspaceName, tableName), ConsistencyLevel.ALL);

            cluster.filters().verbs(Verb.MT_READ_RECONCILE_ACK.id).drop();

            Object[][] result = cluster.coordinator(1).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            Assert.assertArrayEquals(new Object[][]{ row(1, 10) }, result);
        }
    }

    /**
     * Verifies that when node 1 is data node and needs 1 summary node at QUORUM,
     * node 3 is missing a mutation from node 1, and an extra summary node 2 is down/unresponsive,
     * node 3 sends a per-peer ack for node 1 as soon as it pulls the mutation, allowing the read
     * to complete without waiting on node 2.
     */
    @Test
    public void testPerPeerAckWithUnresponsiveNonQuorumNode() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "per_peer_ack_unresponsive_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, v int, primary key (k));", keyspaceName, tableName));

            // Write k=1 consistently
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 10)", keyspaceName, tableName), ConsistencyLevel.ALL);

            // Isolate node 3 and write an update to node 1 at QUORUM (node 3 misses the update)
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            awaitNodeDead(cluster.get(1), cluster.get(3));

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 20)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            // Reconnect node 3, isolate node 2 (node 2 is unresponsive)
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();
            awaitNodeAlive(cluster.get(1), cluster.get(3));
            awaitNodeDead(cluster.get(1), cluster.get(2));

            // Read from node 1 at QUORUM (data node 1, summary node 3).
            // Node 3 pulls the missing mutation from node 1 and sends per-peer ack for node 1.
            Object[][] result = queryWithRetries(cluster.coordinator(1), format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName));
            Assert.assertArrayEquals(new Object[][]{ row(1, 20) }, result);
        }
    }

    /**
     * Verifies that when coordinator node 1 needs one summary node at QUORUM,
     * summary node 2 has an unreconciled mutation that node 1 is missing,
     * summary node 3 is in sync with node 1,
     * and node 1 holds the pull of the missing mutation,
     * the coordinator pivots to node 3 and completes the read.
     * Also verifies that subsequent reads succeed after replica 2 restarts.
     */
    @Test
    public void testEagerPivotToReadySummaryNode() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("read_request_timeout", "4000ms")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "eager_pivot_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, v int, primary key (k)) WITH speculative_retry = 'ALWAYS';", keyspaceName, tableName));

            // Write consistently across all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 10)", keyspaceName, tableName), ConsistencyLevel.ALL);

            // Prevent nodes 1 and 3 from receiving node 2's mutation or learning about it via broadcast
            cluster.filters().verbs(Verb.MUTATION_REQ.id).from(2).to(1, 3).drop();
            IMessageFilters.Filter blockBroadcastFrom2 = cluster.filters().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).from(2).drop();

            boolean timedOut = false;
            try
            {
                cluster.coordinator(2).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 20)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            }
            catch (Exception e)
            {
                timedOut = true;
            }
            Assert.assertTrue("Expected write timeout on node 2", timedOut);

            // Verify node 2 has the new mutation locally, while nodes 1 and 3 do not
            Assert.assertEquals(10, (int) cluster.get(1).executeInternal(format("SELECT v FROM %s.%s WHERE k=1", keyspaceName, tableName))[0][0]);
            Assert.assertEquals(20, (int) cluster.get(2).executeInternal(format("SELECT v FROM %s.%s WHERE k=1", keyspaceName, tableName))[0][0]);
            Assert.assertEquals(10, (int) cluster.get(3).executeInternal(format("SELECT v FROM %s.%s WHERE k=1", keyspaceName, tableName))[0][0]);

            // Set up latches to hold the mutation pull from node 1
            CountDownLatch pullParked = new CountDownLatch(1);
            CountDownLatch pullRelease = new CountDownLatch(1);

            // Hold pull requests from node 1 (to any replica)
            cluster.filters().verbs(Verb.MT_PULL_MUTATIONS_REQ.id).from(1).messagesMatching((from, to, msg) -> {
                pullParked.countDown();
                Uninterruptibles.awaitUninterruptibly(pullRelease);
                return false;
            }).drop();

            // Ensure node 3's MT_SUMMARY_RSP does not arrive before node 1 has initiated the pull for node 2
            cluster.filters().verbs(Verb.MT_SUMMARY_RSP.id).from(3).to(1).messagesMatching((from, to, msg) -> {
                Uninterruptibles.awaitUninterruptibly(pullParked);
                return false;
            }).drop();

            try
            {
                Object[][] result = cluster.coordinator(1).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
                Assert.assertArrayEquals(new Object[][]{ row(1, 10) }, result);

                pullRelease.countDown();
                blockBroadcastFrom2.off();

                cluster.get(2).shutdown().get();
                cluster.get(2).startup();
                awaitNodeAlive(cluster.get(1), cluster.get(2));

                Object[][] postRestartResult = cluster.coordinator(1).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
                Assert.assertNotNull(postRestartResult);
            }
            finally
            {
                pullRelease.countDown();
                blockBroadcastFrom2.off();
            }
        }
    }

    /**
     * Verifies that an un-upgraded summary node (which does not send the MT_PER_PEER_RECONCILE_ACK flag)
     * completes reads with an upgraded data node.
     */
    @Test
    public void testMixedVersionSummaryNodeInteroperability() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "mixed_version_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, v int, primary key (k));", keyspaceName, tableName));

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 100)", keyspaceName, tableName), ConsistencyLevel.ALL);

            // Isolate node 2 and write an update to node 1 at QUORUM (node 2 misses the update)
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();
            awaitNodeDead(cluster.get(1), cluster.get(2));

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 200)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            // Reconnect node 2, isolate node 3 (node 3 is unresponsive)
            cluster.filters().reset();
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            awaitNodeAlive(cluster.get(1), cluster.get(2));
            awaitNodeDead(cluster.get(1), cluster.get(3));

            // Strip MT_PER_PEER_RECONCILE_ACK on node 1 to simulate an un-upgraded summary node
            cluster.get(1).runOnInstance(() -> {
                MessagingService.instance().inboundSink.add(msg -> {
                    if (msg.verb() == Verb.MT_SUMMARY_RSP
                        && msg.header.hasFlag(MessageFlag.MT_PER_PEER_RECONCILE_ACK))
                    {
                        @SuppressWarnings("unchecked")
                        Message<TrackedSummaryResponse> typedMsg =
                            (Message<TrackedSummaryResponse>) msg;
                        Message.Builder<TrackedSummaryResponse> builder =
                            Message.builder(typedMsg).withFlags(0);
                        for (MessageFlag flag : MessageFlag.values())
                        {
                            if (flag != MessageFlag.MT_PER_PEER_RECONCILE_ACK && msg.header.hasFlag(flag))
                                builder.withFlag(flag);
                        }
                        try
                        {
                            ((IVerbHandler) Verb.MT_SUMMARY_RSP.handler()).doVerb(builder.build());
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
                        return false;
                    }
                    return true;
                });
            });

            // Strip the MT_PER_PEER_RECONCILE_ACK flag from inbound MT_SUMMARY_RSP on node 2 (simulating un-upgraded data node)
            cluster.get(2).runOnInstance(() -> {
                MessagingService.instance().inboundSink.add(msg -> {
                    if (msg.verb() == Verb.MT_SUMMARY_RSP
                        && msg.header.hasFlag(MessageFlag.MT_PER_PEER_RECONCILE_ACK))
                    {
                        @SuppressWarnings("unchecked")
                        Message<TrackedSummaryResponse> typedMsg =
                            (Message<TrackedSummaryResponse>) msg;
                        Message.Builder<TrackedSummaryResponse> builder =
                            Message.builder(typedMsg).withFlags(0);
                        for (MessageFlag flag : MessageFlag.values())
                        {
                            if (flag != MessageFlag.MT_PER_PEER_RECONCILE_ACK && msg.header.hasFlag(flag))
                                builder.withFlag(flag);
                        }
                        try
                        {
                            ((IVerbHandler) Verb.MT_SUMMARY_RSP.handler()).doVerb(builder.build());
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
                        return false;
                    }
                    return true;
                });
            });

            // Read at QUORUM from node 1.
            // Node 2 pulls missing mutation from node 1 and falls back to legacy ack mode.
            // Node 1 accepts the legacy ack and completes the read.
            Object[][] result = queryWithRetries(cluster.coordinator(1), format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName));
            Assert.assertArrayEquals(new Object[][]{ row(1, 200) }, result);
        }
    }


}
