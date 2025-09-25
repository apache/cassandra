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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO This test would be a lot faster if it had a shared cluster
public class MutationTrackingTest extends TestBaseImpl
{
    private static final String INSERT_FMT = "INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (%d, %d)";
    private static final String INSERT_CQL = String.format(INSERT_FMT, 1, 1);
    private static final String CONDITIONAL_INSERT_CQL = INSERT_CQL + " IF NOT EXISTS";

    private static final String BATCH_INSERT_FMT = "BEGIN %s BATCH%n"
                                                 + "  %s%n"
                                                 + "  %s%n"
                                                 + "APPLY BATCH";

    @Test
    public void testBasicWritePath() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);
                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                assertEquals(1, summaryIds.offsetCount());
            });
        }
    }

    @Test
    public void testWitnessPaxosV1Reads() throws Throwable
    {
        testWitnessPaxosReads("v1");
    }

    @Test
    public void testWitnessPaxosV2Reads() throws Throwable
    {
        testWitnessPaxosReads("v2");
    }

    private void testWitnessPaxosReads(String paxosVariant) throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                            .set("transient_replication_enabled", "true")
                                                            .set("paxos_variant", paxosVariant))
                                      .start())
        {
            String keyspaceName = KEYSPACE;
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // TODO shouldn't be necessary to mess with marking things in Gossip but there is no read speculation
            // so the read fails because it routes to a node that is blocked
            cluster.filters().allVerbs().to(3).drop().on();
            cluster.filters().allVerbs().from(3).drop().on();
            for (int i = 1; i < 3; i++)
                cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.3"), Double.MAX_VALUE));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            // Two nodes should know about the mutation
            for (int i = 1; i <= 2; i++)
                cluster.get(i).runOnInstance(() -> {
                    MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(Util.dk(1), ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, true);
                    assertEquals(1, summary.size());
                });

            // Filter should stop the witness from getting the mutation so we can test pushing the mutation summary to the witness
            cluster.get(3).runOnInstance(() -> {
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(Util.dk(1), ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, true);
                assertEquals(0, summary.size());
            });

            int rowsFound = 0;
            String singlePartitionSelectCQL = withKeyspace("SELECT * FROM %s.tbl WHERE k = 1");
            for (IInvokableInstance instance : cluster)
            {
                Object[][] result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0-1 rows", result.length == 0 || result.length == 1);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 2, rowsFound);

            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop().on();
            cluster.filters().allVerbs().from(2).drop().on();
            cluster.get(1).runOnInstance(() -> Gossiper.runInGossipStageBlocking(() -> {
                InetAddressAndPort endpoint = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
                Gossiper.instance.realMarkAlive(endpoint, Gossiper.instance.getEndpointStateForEndpoint(endpoint));
            }));
            for (int i = 1; i < 4; i++)
                if (i != 2)
                    cluster.get(i).runOnInstance(() -> Gossiper.instance.convict(InetAddressAndPort.getByNameUnchecked("127.0.0.2"), Double.MAX_VALUE));

            Object[][] result = cluster.coordinator(1).execute(singlePartitionSelectCQL, ConsistencyLevel.SERIAL);
            assertEquals(1, result.length);
            assertEquals(1, result[0][0]);
            assertEquals(1, result[0][1]);

            // The read at SERIAL should propagate the mutation to the witness
            cluster.get(3).runOnInstance(() -> {
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(Util.dk(1), ColumnFamilyStore.getIfExists(keyspaceName, "tbl").metadata.id, true);
                assertEquals(1, summary.size());
            });
        }
    }

    @Ignore("Unlogged batches not supported with mutation tracking yet")
    @Test
    public void testWitnessUnloggedBatchSkippedPath() throws Throwable
    {
        testWitnessBatchWrites(false);
    }

    @Ignore("Logged batches not supported with mutation tracking yet")
    @Test
    public void testWitnessLoggedBatchSkippedPath() throws Throwable
    {
        testWitnessBatchWrites(true);
    }

    private void testWitnessBatchWrites(boolean logged) throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                                          .set("transient_replication_enabled", "true"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            String insertCql = String.format(BATCH_INSERT_FMT, logged ? "" : "UNLOGGED", String.format(INSERT_FMT, KEYSPACE, 1, 1), String.format(INSERT_FMT, KEYSPACE, 2, 2));
            cluster.coordinator(1).execute(insertCql, ConsistencyLevel.ALL);

            // Only two instances should have the row
            int rowsFound = 0;
            String singlePartitionSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            for (IInvokableInstance instance : cluster)
            {
                Object[][] result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0 or 2 rows", result.length == 0 || result.length == 2);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 4, rowsFound);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);

                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                assertEquals(1, summaryIds.offsetCount());
            });

            Object[][] result = cluster.coordinator(1).execute(singlePartitionSelectCQL, ConsistencyLevel.ALL);
            assertEquals(2, result.length);
            String partitionRangeSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            result = cluster.coordinator(1).execute(partitionRangeSelectCQL, ConsistencyLevel.ALL);
            assertEquals(2, result.length);

            // Read time reconciliation should not propagate the row to the witness node
            rowsFound = 0;
            for (IInvokableInstance instance : cluster)
            {
                result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0 or 2 rows", result.length == 0 || result.length == 2);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 4, rowsFound);
        }
    }

    @Test
    public void testWitnessHintSkippedPath() throws Throwable
    {

    }

    @Test
    public void testWitnessSerialPaxosV1WritesSkipped() throws Throwable
    {
        testWitnessWrites(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, "v1");
    }

    @Test
    public void testWitnessSerialPaxosV2WritesSkipped() throws Throwable
    {
        testWitnessWrites(CONDITIONAL_INSERT_CQL, ConsistencyLevel.SERIAL, "v2");
    }

    @Test
    public void testNonSerialWitnessWrites() throws Throwable
    {
        testWitnessWrites(INSERT_CQL, ConsistencyLevel.ALL, null);
    }

    private void testWitnessWrites(String insertCql, ConsistencyLevel cl, String paxosVariant) throws Throwable
    {
        String paxosVariantFinal = paxosVariant == null ? "v1" : paxosVariant;
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)
                                                                          .set("transient_replication_enabled", "true")
                                                                          .set("paxos_variant", paxosVariantFinal))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': '3/1'} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            cluster.coordinator(1).execute(insertCql, cl, ConsistencyLevel.QUORUM);

            // Only two instances should have the row
            int rowsFound = 0;
            String singlePartitionSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            for (IInvokableInstance instance : cluster)
            {
                Object[][] result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0-1 rows", result.length == 0 || result.length == 1);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 2, rowsFound);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);

                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                assertEquals(1, summaryIds.offsetCount());
            });

            Object[][] result = cluster.coordinator(1).execute(singlePartitionSelectCQL, ConsistencyLevel.ALL);
            assertEquals(1, result.length);
            String partitionRangeSelectCQL = withKeyspace("SELECT * FROM %s.tbl");
            result = cluster.coordinator(1).execute(partitionRangeSelectCQL, ConsistencyLevel.ALL);
            assertEquals(1, result.length);

            // Read time reconciliation should not propagate the row to the witness node
            rowsFound = 0;
            for (IInvokableInstance instance : cluster)
            {
                result = instance.executeInternal(singlePartitionSelectCQL);
                assertTrue("Each node should have 0-1 rows", result.length == 0 || result.length == 1);
                rowsFound += result.length;
            }
            assertEquals("Only two instances should have the row", 2, rowsFound);
        }
    }

    @Test
    public void testHintsNotWrittenOnFailedWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // block messages to node 3
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            UUID node3HostId = cluster.get(3).callOnInstance(() -> StorageService.instance.getLocalHostUUID());
            long hints = cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount());

            // confirm no hints for node 3
            cluster.get(1).runOnInstance(() -> assertEquals(0, HintsService.instance.getTotalHintsSize(node3HostId)));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            // wait for write timeout
            Thread.sleep(5000);

            // TODO: confirm hints aren't written
            cluster.get(1).runOnInstance(() -> {
                assertEquals(hints, StorageMetrics.totalHints.getCount());
            });
        }
    }

    @Test
    public void testFailedMutationRedelivery() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP)
                                                                          .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = KEYSPACE;

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // block writes to node 3
            cluster.filters().verbs(Verb.MUTATION_REQ.id).to(3).drop();

            // pause reconciler temporarily
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().pauseActiveReconciler());

            // issue a write - should fail on node 3
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            Thread.sleep(1000); // wait for write timeout

            cluster.get(1).runOnInstance(() ->
            {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);
                Assert.assertEquals(1, summary.get(logId).unreconciled.offsetCount());
                Assert.assertEquals(0, summary.get(logId).reconciled.offsetCount());
            });

            // resume the reconciler
            cluster.get(1).runOnInstance(() -> MutationTrackingService.instance().resumeActiveReconciler());
            Thread.sleep(1000); // wait for reconiciler to do its job

            cluster.get(1).runOnInstance(() ->
            {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);
                Assert.assertEquals(0, summary.get(logId).unreconciled.offsetCount());
                Assert.assertEquals(1, summary.get(logId).reconciled.offsetCount());
            });
        }
    }
}
