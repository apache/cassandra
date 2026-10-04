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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.MutationExceededMaxSizeException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.listeners.MetadataSnapshotListener;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.utils.Clock.Global;
import org.apache.cassandra.utils.NoSpamLogger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the D1 fix: {@link MetadataSnapshots.SystemKeyspaceMetadataSnapshots#storeSnapshot} writes the whole
 * serialised {@link ClusterMetadata} as a single mutation, and that mutation is rejected with
 * {@link MutationExceededMaxSizeException} once it exceeds {@code max_mutation_size}. This test verifies:
 * <ul>
 *     <li>a small snapshot stores cleanly, with no warning-threshold side effects</li>
 *     <li>an oversized snapshot fails with {@link MutationExceededMaxSizeException}, and the attempted size and
 *     failure are still recorded via {@link TCMMetrics}</li>
 *     <li>{@link MetadataSnapshotListener#notify} swallows that failure (so the log-processing thread survives)
 *     while still incrementing {@link TCMMetrics#snapshotStoreFailures}</li>
 * </ul>
 * test/conf/cassandra.yaml sets {@code commitlog_segment_size: 5MiB}, which makes
 * {@code max_mutation_size = commitlog_segment_size / 2 = 2,621,440} bytes for this test JVM. A one
 * partition-key-int + one regular-text-column table serialises at roughly 527 bytes as part of a
 * {@link ClusterMetadata}, so 5,000 such tables (~2.6MB) reliably clears that limit.
 */
public class MetadataSnapshotSizeWarningTest
{
    private static final String KEYSPACE = "metadata_snapshot_size_warning_test_ks";

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    private static DistributedSchema schemaWithTables(int tableCount)
    {
        List<TableMetadata> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++)
        {
            tables.add(TableMetadata.builder(KEYSPACE, "t" + i)
                                     .addPartitionKeyColumn("k", Int32Type.instance)
                                     .addRegularColumn("v", UTF8Type.instance)
                                     .build());
        }
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(tables));
        return new DistributedSchema(Keyspaces.of(ksm));
    }

    private static void truncateSnapshotsTable()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME);
        if (cfs != null)
            cfs.truncateBlockingWithoutSnapshot();
    }

    @Test
    public void smallSnapshotStoresCleanlyWithNoWarningOrFailure()
    {
        truncateSnapshotsTable();
        try
        {
            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata small = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(2), Murmur3Partitioner.instance, schemaWithTables(10));
            snapshots.storeSnapshot(small);

            assertEquals(small, snapshots.getSnapshot(small.epoch));
            assertEquals("A small snapshot must not increment the failure counter",
                         failuresBefore, TCMMetrics.instance.snapshotStoreFailures.getCount());
            assertTrue("A small snapshot must be well under the warning threshold",
                       TCMMetrics.instance.lastSnapshotSize.getValue() < DatabaseDescriptor.getMaxMutationSize() * 0.75);
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    @Test
    public void oversizeSnapshotFailsWithMutationExceededMaxSizeExceptionAndIsRecorded()
    {
        truncateSnapshotsTable();
        try
        {
            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata oversize = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(3), Murmur3Partitioner.instance, schemaWithTables(5000));

            try
            {
                snapshots.storeSnapshot(oversize);
                fail("Expected storeSnapshot to fail once the serialised size exceeds max_mutation_size");
            }
            catch (RuntimeException e)
            {
                Throwable cause = e instanceof MutationExceededMaxSizeException ? e : e.getCause();
                assertTrue("Expected MutationExceededMaxSizeException, got " + e,
                           cause instanceof MutationExceededMaxSizeException);
            }

            assertTrue("The attempted serialised size must be recorded even though the store failed",
                       TCMMetrics.instance.lastSnapshotSize.getValue() > DatabaseDescriptor.getMaxMutationSize() * 0.75);
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    @Test
    public void listenerSwallowsOversizeFailureButCountsIt()
    {
        truncateSnapshotsTable();
        try
        {
            MetadataSnapshots realSnapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            StubClusterMetadataService service = StubClusterMetadataService.builder(Murmur3Partitioner.instance)
                                                                            .withSnapshots(realSnapshots)
                                                                            .build();
            ClusterMetadataService.unsetInstance();
            ClusterMetadataService.setInstance(service);

            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            ClusterMetadata oversize = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(4), Murmur3Partitioner.instance, schemaWithTables(5000));
            Entry entry = new Entry(Entry.Id.NONE, oversize.epoch, new ForceSnapshot(oversize));
            ClusterMetadata previous = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
            Transformation.Result result = entry.transform.execute(previous);

            MetadataSnapshotListener listener = new MetadataSnapshotListener();
            // Must not throw: the listener is required to swallow storeSnapshot failures so the log-processing
            // thread survives a snapshot that is too large to store.
            listener.notify(entry, result);

            assertEquals("The listener must increment snapshotStoreFailures on a caught storeSnapshot failure",
                         failuresBefore + 1, TCMMetrics.instance.snapshotStoreFailures.getCount());
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    /**
     * Covers the CASSANDRA-21664 size warning: once the serialised snapshot reaches 75% of max_mutation_size,
     * {@link MetadataSnapshots.SystemKeyspaceMetadataSnapshots#storeSnapshot} logs a rate-limited WARN so operators
     * get notice before writes start failing. This test verifies that warning actually fires when a snapshot lands
     * in the [75%, 100%) band — large enough to trigger the warning but still small enough to store successfully.
     */
    @Test
    public void warningBandSnapshotStoresSuccessfullyAndLogsWarning() throws IOException
    {
        truncateSnapshotsTable();
        Logger logger = null;
        ListAppender<ILoggingEvent> listAppender = null;
        try
        {
            // Advance NoSpamLogger's clock past the 5-minute suppression window to ensure the warning is
            // not suppressed by previous test runs. The two existing oversize tests (which exceed 100% of
            // max_mutation_size) also trigger this same warning before throwing, so without advancing the
            // clock, running them before this test would suppress the warning and fail the assertion.
            NoSpamLogger.unsafeSetClock(() -> Global.nanoTime() + TimeUnit.MINUTES.toNanos(10));

            // Attach a ListAppender to capture log output from MetadataSnapshots
            logger = (Logger) org.slf4j.LoggerFactory.getLogger(MetadataSnapshots.class);
            listAppender = new ListAppender<>();
            listAppender.start();
            logger.addAppender(listAppender);

            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            // Calculate table count to land in the warning band [75%, 100%) of max_mutation_size. The
            // per-table serialised cost is measured directly rather than assumed: a hardcoded 527-byte
            // estimate held for the plain _jdkNN test variants but was ~39% low against the latest_jdkNN
            // variants (different dependency versions), pushing the snapshot past 100% of the limit and
            // into the oversize-throw path instead of the warning-band success path this test exercises.
            // Two measurements at different table counts isolate the per-table slope from the fixed
            // overhead of the rest of ClusterMetadata (schema keyspace, node states, epoch, ...), so the
            // estimate self-corrects under any JDK, dependency profile, or future metadata format change.
            int maxMutationSize = DatabaseDescriptor.getMaxMutationSize();
            int calibrationLow = 200;
            int calibrationHigh = 1000;
            long sizeAtLow = MetadataSnapshots.toBytes(ClusterMetadataTestHelper.minimalForTesting(
                Epoch.create(1), Murmur3Partitioner.instance, schemaWithTables(calibrationLow))).remaining();
            long sizeAtHigh = MetadataSnapshots.toBytes(ClusterMetadataTestHelper.minimalForTesting(
                Epoch.create(1), Murmur3Partitioner.instance, schemaWithTables(calibrationHigh))).remaining();
            double bytesPerTable = (sizeAtHigh - sizeAtLow) / (double) (calibrationHigh - calibrationLow);
            double overhead = sizeAtLow - calibrationLow * bytesPerTable;

            double targetFraction = 0.80; // 80% of max_mutation_size
            int tableCount = (int) ((maxMutationSize * targetFraction - overhead) / bytesPerTable);

            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata warningBand = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(5), Murmur3Partitioner.instance, schemaWithTables(tableCount));

            // Must not throw: the snapshot is below max_mutation_size, so the store should succeed
            snapshots.storeSnapshot(warningBand);

            // Verify the snapshot stored successfully and can be retrieved
            assertEquals(warningBand, snapshots.getSnapshot(warningBand.epoch));

            // Verify the failure counter was not incremented (unlike test 2, this store succeeded)
            assertEquals("A warning-band snapshot must store successfully and not increment the failure counter",
                         failuresBefore, TCMMetrics.instance.snapshotStoreFailures.getCount());

            // Guard: verify the serialised size actually landed in the warning band [75%, 100%)
            long recordedSize = TCMMetrics.instance.lastSnapshotSize.getValue();
            double lowerBound = maxMutationSize * 0.75;
            assertTrue("Serialised size " + recordedSize + " must be at least 75% of max_mutation_size " + maxMutationSize,
                       recordedSize >= lowerBound);
            assertTrue("Serialised size " + recordedSize + " must be below max_mutation_size " + maxMutationSize + " (test setup error if not)",
                       recordedSize < maxMutationSize);

            // Assert the WARN log was emitted
            boolean foundWarning = listAppender.list.stream()
                                                     .anyMatch(event -> event.getLevel() == Level.WARN
                                                                        && event.getFormattedMessage().contains("Serialised cluster metadata snapshot")
                                                                        && event.getFormattedMessage().contains("of the max_mutation_size limit"));
            assertTrue("Expected a WARN log about snapshot size approaching max_mutation_size, but none was found. " +
                       "Captured log events: " + listAppender.list.size(), foundWarning);
        }
        finally
        {
            if (logger != null && listAppender != null)
                logger.detachAppender(listAppender);
            NoSpamLogger.unsafeSetClock(Global::nanoTime);
            truncateSnapshotsTable();
        }
    }
}
