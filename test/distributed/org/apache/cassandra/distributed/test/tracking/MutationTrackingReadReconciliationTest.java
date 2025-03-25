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

import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.*;

public class MutationTrackingReadReconciliationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingReadReconciliationTest.class);

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
                                                            .set("mutation_tracking_enabled", "true")
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

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);

            MutationSummary finalSummary = summaryForKey(cluster.get(1), keyspaceName, "tbl", 1);
            Assert.assertEquals(1, finalSummary.size());
            Offsets finalIds = summaryIdSpace(finalSummary.get(logId));

            Assert.assertEquals(2, finalIds.offsetCount());
            assertOffsetsIsSuperSet(finalIds, firstIds);;

            Offsets secondIds = Offsets.difference(finalIds, firstIds);
            Assert.assertEquals(1, secondIds.offsetCount());
            Assert.assertEquals(0, Offsets.intersection(firstIds, secondIds).offsetCount());


            // second node should have the new id, third should not
            assertMatchingSummaryIdSpaceForKey(cluster.get(2), keyspaceName, tableName, 1, finalSummary);
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), keyspaceName, tableName, 1, firstSummary);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();


            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = cluster.coordinator(1).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
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
                                                            .set("mutation_tracking_enabled", "true")
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

            Offsets secondIds = Offsets.difference(finalIds, firstIds);
            Assert.assertEquals(1, secondIds.offsetCount());
            Assert.assertEquals(0, Offsets.intersection(firstIds, secondIds).offsetCount());


            // second node should have the new id, third should not
            assertMatchingSummaryIdSpaceForKey(cluster.get(2), keyspaceName, tableName, 1, finalSummary);
            assertMatchingSummaryIdSpaceForKey(cluster.get(3), keyspaceName, tableName, 1, firstSummary);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();


            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = cluster.coordinator(3).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
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
                                                            .set("mutation_tracking_enabled", "true")
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
            Object[][] result = cluster.coordinator(1).execute(format("SELECT * FROM %s.%s", keyspaceName, tableName), ConsistencyLevel.QUORUM);
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
                                                            .set("mutation_tracking_enabled", "true")
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


            Assert.assertEquals(0, numLogReconciliations(cluster.get(3)));
            Object[][] result = cluster.coordinator(3).execute(format("SELECT * FROM %s.%s", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1), row(2, 2, 2)), result);

            // Coordinator sends its missing mutations to 3 on read
            Assert.assertEquals(1, numLogReconciliations(cluster.get(3)));

            // check that node3 has the new ids
            assertMatchingSummaryIdSpaceForTable(cluster.get(3), keyspaceName, tableName, finalSummary);
        }
    }
}
