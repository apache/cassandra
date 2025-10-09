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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for PaxosCommitAndPrepare with mutation tracking enabled.
 *
 * When mutation tracking is enabled, PaxosCommitAndPrepare takes a different path:
 * instead of embedding the commit in the prepare message, it performs a synchronous
 * commit followed by a separate prepare. This test verifies that mutation tracking
 * works correctly in this scenario.
 */
public class MutationTrackingCommitAndPrepareTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingCommitAndPrepareTest.class);

    private static final String INSERT_CQL = "INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (1, 1)";
    private static final String INSERT_CQL_2 = "INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (1, 2)";
    private static final String CONDITIONAL_INSERT_CQL = INSERT_CQL + " IF NOT EXISTS";

    /**
     * Test 1: Basic CommitAndPrepare path with mutation tracking.
     *
     * This test forces the CommitAndPrepare path by blocking the commit phase of the first CAS,
     * leaving an uncommitted ballot. The second CAS will then find this uncommitted ballot
     * during prepare and trigger CommitAndPrepare. With mutation tracking enabled, this should
     * take the sequential commit-then-prepare path.
     */
    @Test
    public void testCommitAndPrepareBasicPath() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("paxos_variant", "v2_without_linearizable_reads"))
                                      .start())
        {
            String keyspaceName = KEYSPACE;

            // Create tracked keyspace with RF=3
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // Verify keyspace is tracked
            cluster.get(1).runOnInstance(() ->
            {
                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
                logger.info("Verified keyspace {} is tracked", keyspaceName);
            });

            // Strategy: Block PAXOS_COMMIT_REQ on the first CAS to prevent commit completion,
            // leaving an uncommitted ballot. The second CAS will then trigger CommitAndPrepare.
            // This follows the same pattern as PaxosRepairTest.
            cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

            logger.info("Executing first CAS to create uncommitted ballot");
            // First CAS - will complete prepare and propose but not commit (due to filter)
            // This will leave an uncommitted accepted proposal
            try
            {
                cluster.coordinator(1).execute(CONDITIONAL_INSERT_CQL,
                                             ConsistencyLevel.SERIAL,
                                             ConsistencyLevel.QUORUM);
                logger.info("First CAS completed (may have timed out on commit)");
            }
            catch (Exception e)
            {
                logger.info("First CAS threw exception (expected): {}", e.getMessage());
                // Expected - commit will timeout due to dropped messages
            }

            // Reset filters so the next CAS can proceed normally
            cluster.filters().reset();
            logger.info("Reset filters");

            logger.info("Executing second CAS - should trigger CommitAndPrepare");
            // Second CAS - should find the uncommitted ballot and trigger CommitAndPrepare
            Object[][] result = cluster.coordinator(1).execute(INSERT_CQL_2 + " IF NOT EXISTS",
                                                              ConsistencyLevel.SERIAL,
                                                              ConsistencyLevel.QUORUM);

            // CAS should succeed
            assertTrue("Second CAS operation should succeed", result.length > 0);
            logger.info("Second CAS succeeded");

            // Verify mutation tracking shows correct state on coordinator (node 1)
            cluster.get(1).runOnInstance(() ->
            {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(dk, table.id, false);

                logger.info("Node 1 mutation summary: {} coordinator logs", summary.size());

                // Should have at least one coordinator log entry
                assertTrue("Should have at least one coordinator log", summary.size() >= 1);

                CoordinatorLogId logId = getOnlyLogId(summary);
                Offsets offsets = summaryIdSpace(summary.get(logId));

                logger.info("Node 1 has {} mutation offsets", offsets.offsetCount());
                // Should have mutations from the CommitAndPrepare
                assertTrue("Should have at least one mutation offset", offsets.offsetCount() >= 1);
            });

            // Wait for reconciliation
            // TODO: Replace Thread.sleep with Awaitility for condition-based waiting
            Thread.sleep(2000);

            // Verify all nodes eventually see the mutation
            for (int i = 1; i <= 3; i++)
            {
                int nodeId = i;
                cluster.get(nodeId).runOnInstance(() ->
                {
                    TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                    DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(dk, table.id, true);

                    logger.info("Node {} mutation summary size: {}", nodeId, summary.size());
                    // All nodes should eventually have the mutation
                    assertTrue("Node " + nodeId + " should have mutation in summary", summary.size() > 0);
                });
            }

            // Verify data is readable
            result = cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1",
                                                   ConsistencyLevel.QUORUM);
            assertEquals("Should have one row", 1, result.length);
            assertEquals("Key should be 1", 1, result[0][0]);
            logger.info("Final value: {}", result[0][1]);
        }
    }

    /**
     * Test 2: CommitAndPrepare with witness (transient) replicas.
     *
     * This test verifies that with witness replicas (transient replication), the commit
     * in CommitAndPrepare only goes to full replicas, and the witness eventually gets
     * the mutation through reconciliation.
     */
    @Test
    public void testCommitAndPrepareWithWitnessReplicas() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("transient_replication_enabled", "true")
                                                            .set("paxos_variant", "v2_without_linearizable_reads"))
                                      .start())
        {
            String keyspaceName = KEYSPACE;

            // Create tracked keyspace with RF='3/1' (2 full replicas + 1 witness/transient)
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // Verify keyspace configuration
            cluster.get(1).runOnInstance(() ->
            {
                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
                logger.info("Verified keyspace {} is tracked with transient replication", keyspaceName);
            });

            // Similar strategy: block PAXOS_COMMIT_REQ to create uncommitted ballot
            cluster.filters().verbs(Verb.PAXOS_COMMIT_REQ.id).drop();

            logger.info("Executing first CAS to create uncommitted ballot");
            // First CAS - will complete prepare and propose but timeout on commit
            try
            {
                cluster.coordinator(1).execute(CONDITIONAL_INSERT_CQL,
                                             ConsistencyLevel.SERIAL,
                                             ConsistencyLevel.QUORUM);
                logger.info("First CAS completed (may have timed out)");
            }
            catch (Exception e)
            {
                logger.info("First CAS threw exception (expected): {}", e.getMessage());
                // Expected - commit will timeout
            }

            // Wait for async operations
            // TODO: Replace Thread.sleep with Awaitility for condition-based waiting
            Thread.sleep(1000);

            // Reset filters
            cluster.filters().reset();
            logger.info("Reset filters");

            logger.info("Executing second CAS - should trigger CommitAndPrepare");
            // Second CAS should find uncommitted ballot and trigger CommitAndPrepare
            Object[][] result = cluster.coordinator(1).execute(INSERT_CQL_2 + " IF NOT EXISTS",
                                                              ConsistencyLevel.SERIAL,
                                                              ConsistencyLevel.QUORUM);

            assertTrue("Second CAS should succeed", result.length > 0);
            logger.info("Second CAS succeeded");

            // Verify mutation tracking on full replicas
            // With RF=3/1, we have 2 full replicas and 1 transient
            // The commit should only go to full replicas
            for (int i = 1; i <= 3; i++)
            {
                int nodeId = i;
                cluster.get(nodeId).runOnInstance(() ->
                {
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(
                        Util.dk(1),
                        ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id,
                        true);
                    logger.info("Node {} mutation summary size: {}", nodeId, summary.size());
                    // All nodes should eventually have mutation tracking info (including witness through reconciliation)
                    assertTrue("Node " + nodeId + " should have mutation info", summary.size() > 0);
                });
            }

            // Wait for reconciliation
            // TODO: Replace Thread.sleep with Awaitility for condition-based waiting
            Thread.sleep(2000);

            // Verify data consistency across all nodes (including witness)
            // The data should be on full replicas, witness may not have it depending on when we check
            int rowsFound = 0;
            String selectCql = withKeyspace("SELECT * FROM %s.tbl WHERE k = 1");
            for (int i = 1; i <= 3; i++)
            {
                Object[][] nodeResult = cluster.get(i).executeInternal(selectCql);
                logger.info("Node {} has {} rows", i, nodeResult.length);
                assertTrue("Each node should have 0-1 rows", nodeResult.length == 0 || nodeResult.length == 1);
                rowsFound += nodeResult.length;
            }
            logger.info("Total rows found across nodes: {}", rowsFound);
            // At least 2 full replicas should have the row
            assertTrue("At least 2 instances should have the row", rowsFound >= 2);

            // Verify data is readable with QUORUM (which requires full replicas)
            result = cluster.coordinator(1).execute(selectCql, ConsistencyLevel.QUORUM);
            assertEquals("Should have one row", 1, result.length);
            assertEquals("Key should be 1", 1, result[0][0]);
            logger.info("Final value from QUORUM read: {}", result[0][1]);
        }
    }
}
